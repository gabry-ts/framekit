import type { CSVReadOptions } from '../../types/options';
import { DType } from '../../types/dtype';
import { IOError, ParseError } from '../../errors';

const DEFAULT_NULL_VALUES = ['', 'null', 'NULL', 'NA', 'N/A', 'NaN', 'nan', 'None', 'none'];

export interface StreamCSVOptions extends CSVReadOptions {
  chunkSize?: number | undefined;
}

interface SchemaInfo {
  header: string[];
  delimiter: string;
  nullValues: Set<string>;
  inferredTypes: Record<string, DType>;
}

/**
 * Stream a CSV file yielding DataFrames of at most `chunkSize` rows each.
 * Uses Node.js readable streams to keep memory bounded.
 */
export async function* streamCSVFile(
  filePath: string,
  options: StreamCSVOptions = {},
): AsyncIterable<{ header: string[]; rawColumns: Record<string, (string | null)[]>; inferredTypes: Record<string, DType> }> {
  const chunkSize = options.chunkSize ?? 10000;
  const fs = await import('fs');
  const { createReadStream } = fs;

  let stream: ReturnType<typeof createReadStream>;
  try {
    stream = createReadStream(filePath, { encoding: (options.encoding ?? 'utf-8') as BufferEncoding });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    throw new IOError(`Failed to open CSV file '${filePath}': ${message}`);
  }

  const hasHeader = options.hasHeader !== false;
  const nullValues = new Set(options.nullValues ?? DEFAULT_NULL_VALUES);
  const comment = options.comment;
  const skipRows = options.skipRows ?? 0;

  let schema: SchemaInfo | null = null;
  let buffer = '';
  let inQuotes = false;
  let pendingLines: string[] = [];
  let linesSkipped = 0;
  let headerConsumed = false;
  let chunkRows: Record<string, (string | null)[]> = {};
  let rowCount = 0;
  let totalRowsEmitted = 0;
  const nRows = options.nRows;

  function parseLine(line: string, delimiter: string): string[] {
    const fields: string[] = [];
    let current = '';
    let quoted = false;
    let i = 0;

    while (i < line.length) {
      const ch = line[i]!;
      if (quoted) {
        if (ch === '"') {
          if (i + 1 < line.length && line[i + 1] === '"') {
            current += '"';
            i += 2;
          } else {
            quoted = false;
            i++;
          }
        } else {
          current += ch;
          i++;
        }
      } else {
        if (ch === '"' && current.length === 0) {
          quoted = true;
          i++;
        } else if (line.startsWith(delimiter, i)) {
          fields.push(current);
          current = '';
          i += delimiter.length;
        } else {
          current += ch;
          i++;
        }
      }
    }

    if (quoted) {
      throw new ParseError('Unterminated quoted field in CSV');
    }

    fields.push(current);
    return fields;
  }

  function detectDelimiter(lines: string[]): string {
    const candidates = [',', ';', '\t', '|'];
    let bestDelimiter = ',';
    let bestScore = -1;

    for (const delim of candidates) {
      const counts = lines.map((line) => {
        let count = 0;
        let q = false;
        for (let i = 0; i < line.length; i++) {
          const ch = line[i]!;
          if (ch === '"') q = !q;
          else if (!q && line.startsWith(delim, i)) count++;
        }
        return count;
      });

      if (counts.length === 0) continue;
      const avg = counts.reduce((a, b) => a + b, 0) / counts.length;
      if (avg === 0) continue;
      const allSame = counts.every((c) => c === counts[0]);
      const score = allSame ? avg * 2 : avg;
      if (score > bestScore) {
        bestScore = score;
        bestDelimiter = delim;
      }
    }

    return bestDelimiter;
  }

  function inferTypes(
    columns: Record<string, (string | null)[]>,
    header: string[],
    opts: CSVReadOptions,
  ): Record<string, DType> {
    const types: Record<string, DType> = {};
    const parseNumbers = opts.parseNumbers !== false;
    const parseDates = opts.parseDates !== false;

    for (const name of header) {
      if (opts.dtypes && name in opts.dtypes) {
        types[name] = opts.dtypes[name]!;
        continue;
      }

      const values = columns[name]!;
      const sample = values.slice(0, 100).filter((v): v is string => v !== null);

      if (sample.length === 0) {
        types[name] = DType.Utf8;
        continue;
      }

      if (parseNumbers && sample.every(isNumericString)) {
        types[name] = sample.every(isIntegerString) ? DType.Int32 : DType.Float64;
        continue;
      }

      if (sample.every(isBooleanString)) {
        types[name] = DType.Boolean;
        continue;
      }

      if (parseDates && sample.every(isDateString)) {
        types[name] = DType.Date;
        continue;
      }

      types[name] = DType.Utf8;
    }

    return types;
  }

  function initChunkRows(header: string[]): Record<string, (string | null)[]> {
    const cols: Record<string, (string | null)[]> = {};
    for (const name of header) {
      cols[name] = [];
    }
    return cols;
  }

  function addRowToChunk(fields: string[], schema: SchemaInfo): void {
    for (let i = 0; i < schema.header.length; i++) {
      const name = schema.header[i]!;
      const raw = i < fields.length ? fields[i]! : '';
      const value = schema.nullValues.has(raw) ? null : raw;
      chunkRows[name]!.push(value);
    }
    rowCount++;
  }

  let parsedHeader: string[] | null = null;
  let detectedDelimiter: string | null = null;

  function processLine(line: string): { header: string[]; rawColumns: Record<string, (string | null)[]>; inferredTypes: Record<string, DType> } | null {
    // Filter comments
    if (comment && line.trimStart().startsWith(comment)) {
      return null;
    }

    // Skip rows
    if (linesSkipped < skipRows) {
      linesSkipped++;
      return null;
    }

    // Phase 1: parse header from first line
    if (!headerConsumed) {
      detectedDelimiter = options.delimiter ?? detectDelimiter([line]);
      if (options.header) {
        parsedHeader = options.header;
        // This line is data, keep it in pendingLines
        pendingLines.push(line);
      } else if (hasHeader) {
        parsedHeader = parseLine(line, detectedDelimiter).map((h) => h.trim());
        // Header line consumed, don't add to pendingLines
      } else {
        const firstFields = parseLine(line, detectedDelimiter);
        parsedHeader = firstFields.map((_, i) => `column_${i}`);
        pendingLines.push(line);
      }
      headerConsumed = true;
      return null;
    }

    // Phase 2: collect data lines until we can infer types, then finalize schema
    if (!schema) {
      pendingLines.push(line);

      // We have at least one data line now — infer types and finalize schema
      const delimiter = detectedDelimiter!;
      const header = parsedHeader!;
      const tempCols = initChunkRows(header);
      for (const pl of pendingLines) {
        const fields = parseLine(pl, delimiter);
        for (let i = 0; i < header.length; i++) {
          const name = header[i]!;
          const raw = i < fields.length ? fields[i]! : '';
          const value = nullValues.has(raw) ? null : raw;
          tempCols[name]!.push(value);
        }
      }

      const inferredTypes = inferTypes(tempCols, header, options);
      schema = { header, delimiter, nullValues, inferredTypes };
      chunkRows = initChunkRows(header);

      // Add pending lines to chunk
      for (const pl of pendingLines) {
        const fields = parseLine(pl, delimiter);
        addRowToChunk(fields, schema);
      }
      pendingLines = [];

      // Check if chunk is full
      if (rowCount >= chunkSize) {
        const result = { header: schema.header, rawColumns: chunkRows, inferredTypes: schema.inferredTypes };
        chunkRows = initChunkRows(schema.header);
        totalRowsEmitted += rowCount;
        rowCount = 0;
        return result;
      }

      return null;
    }

    // Normal processing with established schema
    const fields = parseLine(line, schema.delimiter);
    addRowToChunk(fields, schema);

    if (rowCount >= chunkSize) {
      const result = { header: schema.header, rawColumns: chunkRows, inferredTypes: schema.inferredTypes };
      chunkRows = initChunkRows(schema.header);
      totalRowsEmitted += rowCount;
      rowCount = 0;
      return result;
    }

    return null;
  }

  // Process the stream
  for await (const rawChunk of stream as AsyncIterable<string>) {
    buffer += rawChunk;

    // Extract complete lines from buffer
    let lineStart = 0;
    for (let i = 0; i < buffer.length; i++) {
      const ch = buffer[i]!;
      if (ch === '"') {
        inQuotes = !inQuotes;
      } else if (!inQuotes && (ch === '\n' || ch === '\r')) {
        const line = buffer.slice(lineStart, i);
        if (ch === '\r' && i + 1 < buffer.length && buffer[i + 1] === '\n') {
          i++; // skip \n after \r
        }
        lineStart = i + 1;

        if (line.length > 0) {
          // Check nRows limit
          if (nRows !== undefined && totalRowsEmitted + rowCount >= nRows) {
            break;
          }

          const result = processLine(line);
          if (result) {
            yield result;
            // Check nRows limit after yielding
            if (nRows !== undefined && totalRowsEmitted >= nRows) {
              stream.destroy();
              return;
            }
          }
        }
      }
    }

    buffer = buffer.slice(lineStart);

    // Check nRows limit
    if (nRows !== undefined && totalRowsEmitted >= nRows) {
      stream.destroy();
      return;
    }
  }

  // Process remaining buffer
  if (buffer.length > 0 && !(nRows !== undefined && totalRowsEmitted >= nRows)) {
    if (!comment || !buffer.trimStart().startsWith(comment)) {
      processLine(buffer);
    }
  }

  // Yield final chunk if any rows remain
  // TS narrows schema to never in generators after yield; use assertion
  const finalSchema = schema as SchemaInfo | null;
  if (rowCount > 0 && finalSchema !== null) {
    // Apply nRows limit to final chunk
    if (nRows !== undefined) {
      const remaining = nRows - totalRowsEmitted;
      if (remaining <= 0) return;
      if (remaining < rowCount) {
        for (const name of finalSchema.header) {
          chunkRows[name] = chunkRows[name]!.slice(0, remaining);
        }
      }
    }
    yield { header: finalSchema.header, rawColumns: chunkRows, inferredTypes: finalSchema.inferredTypes };
  }
}

function isNumericString(s: string): boolean {
  if (s.length === 0) return false;
  const n = Number(s);
  return !Number.isNaN(n) && s.trim().length > 0;
}

function isIntegerString(s: string): boolean {
  if (!isNumericString(s)) return false;
  const n = Number(s);
  return Number.isInteger(n) && !s.includes('.') && !s.includes('e') && !s.includes('E');
}

function isBooleanString(s: string): boolean {
  const lower = s.toLowerCase();
  return lower === 'true' || lower === 'false';
}

const ISO_DATE_RE = /^\d{4}-\d{2}-\d{2}(T\d{2}:\d{2}(:\d{2})?(\.\d+)?(Z|[+-]\d{2}:?\d{2})?)?$/;

function isDateString(s: string): boolean {
  if (!ISO_DATE_RE.test(s)) return false;
  const d = new Date(s);
  return !Number.isNaN(d.getTime());
}
