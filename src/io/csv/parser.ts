import type { CSVReadOptions } from '../../types/options';
import { DType } from '../../types/dtype';

export interface ParsedColumns {
  header: string[];
  columns: Record<string, (string | null)[]>;
  inferredTypes: Record<string, DType>;
}

const DEFAULT_NULL_VALUES = ['', 'null', 'NULL', 'NA', 'N/A', 'NaN', 'nan', 'None', 'none'];

// Char codes for hot-loop comparisons
const CH_QUOTE = 0x22;    // "
const CH_LF = 0x0a;       // \n
const CH_CR = 0x0d;       // \r
const CH_COMMA = 0x2c;    // ,
const CH_SEMI = 0x3b;     // ;
const CH_TAB = 0x09;      // \t
const CH_PIPE = 0x7c;     // |

/**
 * Single-pass RFC 4180 CSV parser with auto-detection of delimiter and column types.
 */
export function parseCSV(content: string, options: CSVReadOptions = {}): ParsedColumns {
  if (content.length === 0) {
    return { header: [], columns: {}, inferredTypes: {} };
  }

  const comment = options.comment;
  const hasHeader = options.hasHeader !== false;
  const nullValues = new Set(options.nullValues ?? DEFAULT_NULL_VALUES);
  const skipRows = options.skipRows ?? 0;

  // We need to detect the delimiter from the first few lines
  const delimiter = options.delimiter ?? detectDelimiterFast(content);
  const delimCode = delimiter.charCodeAt(0);
  const delimLen = delimiter.length;
  const multiCharDelim = delimLen > 1;

  // First pass on header line: find end of header, parse header fields
  let pos = 0;

  // Skip rows if needed
  let skipped = 0;
  while (skipped < skipRows && pos < content.length) {
    pos = skipLine(content, pos);
    skipped++;
  }

  // Skip comment lines before header
  if (comment) {
    while (pos < content.length) {
      let lineStart = pos;
      while (lineStart < content.length) {
        const c = content.charCodeAt(lineStart);
        if (c !== 0x20 && c !== CH_TAB) break;
        lineStart++;
      }
      if (lineStart < content.length && content.startsWith(comment, lineStart)) {
        pos = skipLine(content, pos);
      } else {
        break;
      }
    }
  }

  // Parse header line
  let header: string[];
  if (options.header) {
    header = options.header;
    if (hasHeader) {
      // Skip the header line in the content
      pos = skipLine(content, pos);
    }
  } else if (hasHeader) {
    const headerResult = parseLineFields(content, pos, delimCode, delimLen, multiCharDelim, delimiter);
    header = headerResult.fields.map((h) => h.trim());
    pos = headerResult.nextPos;
  } else {
    // No header — peek at first line to count fields
    const peek = parseLineFields(content, pos, delimCode, delimLen, multiCharDelim, delimiter);
    header = peek.fields.map((_, i) => `column_${i}`);
    // Don't advance pos — first line is data
  }

  if (header.length === 0) {
    return { header: [], columns: {}, inferredTypes: {} };
  }

  // Determine selected columns
  const selectedColumns = options.columns ? new Set(options.columns) : null;
  const activeHeader: string[] = [];
  const colIndices: number[] = []; // which field indices to keep
  for (let i = 0; i < header.length; i++) {
    const name = header[i]!;
    if (selectedColumns && !selectedColumns.has(name)) continue;
    activeHeader.push(name);
    colIndices.push(i);
  }

  // Pre-allocate column arrays with size estimate
  const estimatedRows = Math.max(1, Math.floor(content.length / 40));
  const columns: Record<string, (string | null)[]> = {};
  for (const name of activeHeader) {
    const arr: (string | null)[] = [];
    // V8 hint: pre-allocate backing store
    if (estimatedRows > 1000) {
      arr.length = estimatedRows;
      arr.length = 0;
    }
    columns[name] = arr;
  }

  // Limit rows
  const maxRows = options.nRows ?? Infinity;
  let rowCount = 0;

  // Single-pass data parsing — write directly to column arrays
  const numActiveCols = activeHeader.length;
  const allColumns = numActiveCols === header.length; // no column filtering

  while (pos < content.length && rowCount < maxRows) {
    // Skip comment lines
    if (comment) {
      let lineStart = pos;
      // Skip leading whitespace for comment detection
      while (lineStart < content.length) {
        const c = content.charCodeAt(lineStart);
        if (c !== 0x20 && c !== CH_TAB) break;
        lineStart++;
      }
      if (lineStart < content.length && content.startsWith(comment, lineStart)) {
        pos = skipLine(content, pos);
        continue;
      }
    }

    // Check for empty trailing line
    if (content.charCodeAt(pos) === CH_LF || content.charCodeAt(pos) === CH_CR) {
      pos = skipLine(content, pos);
      continue;
    }

    // Parse one row directly into column arrays
    if (allColumns) {
      pos = parseRowDirect(content, pos, delimCode, delimLen, multiCharDelim, delimiter, columns, activeHeader, numActiveCols, nullValues);
    } else {
      pos = parseRowFiltered(content, pos, delimCode, delimLen, multiCharDelim, delimiter, columns, activeHeader, colIndices, numActiveCols, nullValues);
    }
    rowCount++;
  }

  // Infer types by sampling first 100 rows
  const inferredTypes = inferColumnTypes(columns, activeHeader, options);

  return { header: activeHeader, columns, inferredTypes };
}

/** Skip to the start of the next line, returns position after newline. */
function skipLine(content: string, pos: number): number {
  let inQuotes = false;
  while (pos < content.length) {
    const ch = content.charCodeAt(pos);
    if (ch === CH_QUOTE) {
      inQuotes = !inQuotes;
    } else if (!inQuotes) {
      if (ch === CH_CR) {
        pos++;
        if (pos < content.length && content.charCodeAt(pos) === CH_LF) pos++;
        return pos;
      }
      if (ch === CH_LF) {
        return pos + 1;
      }
    }
    pos++;
  }
  return pos;
}

interface LineResult {
  fields: string[];
  nextPos: number;
}

/** Parse a single line into field strings. Used for header parsing. */
function parseLineFields(
  content: string,
  pos: number,
  delimCode: number,
  delimLen: number,
  multiCharDelim: boolean,
  delimiter: string,
): LineResult {
  const fields: string[] = [];
  const len = content.length;

  while (pos <= len) {
    // At end of content, or at end of line: push empty final field was handled by delimiter
    if (pos >= len) {
      fields.push('');
      break;
    }

    const ch = content.charCodeAt(pos);

    // End of line
    if (ch === CH_CR || ch === CH_LF) {
      fields.push('');
      if (ch === CH_CR && pos + 1 < len && content.charCodeAt(pos + 1) === CH_LF) {
        pos += 2;
      } else {
        pos += 1;
      }
      return { fields, nextPos: pos };
    }

    // Quoted field
    if (ch === CH_QUOTE) {
      pos++; // skip opening quote
      const fieldStart = pos;
      let hasEscape = false;

      while (pos < len) {
        const c = content.charCodeAt(pos);
        if (c === CH_QUOTE) {
          if (pos + 1 < len && content.charCodeAt(pos + 1) === CH_QUOTE) {
            hasEscape = true;
            pos += 2;
          } else {
            // End of quoted field
            break;
          }
        } else {
          pos++;
        }
      }

      let value: string;
      if (hasEscape) {
        value = content.substring(fieldStart, pos).replace(/""/g, '"');
      } else {
        value = content.substring(fieldStart, pos);
      }
      fields.push(value);

      if (pos < len) pos++; // skip closing quote

      // After quoted field: expect delimiter or end of line
      if (pos < len) {
        const next = content.charCodeAt(pos);
        if (next === CH_CR || next === CH_LF) {
          if (next === CH_CR && pos + 1 < len && content.charCodeAt(pos + 1) === CH_LF) {
            pos += 2;
          } else {
            pos += 1;
          }
          return { fields, nextPos: pos };
        }
        if (multiCharDelim ? content.startsWith(delimiter, pos) : next === delimCode) {
          pos += delimLen;
        }
      }
    } else {
      // Unquoted field — use substring
      const fieldStart = pos;
      while (pos < len) {
        const c = content.charCodeAt(pos);
        if (c === CH_CR || c === CH_LF) break;
        if (multiCharDelim ? content.startsWith(delimiter, pos) : c === delimCode) break;
        pos++;
      }
      fields.push(content.substring(fieldStart, pos));

      if (pos >= len) {
        return { fields, nextPos: pos };
      }

      const c = content.charCodeAt(pos);
      if (c === CH_CR || c === CH_LF) {
        if (c === CH_CR && pos + 1 < len && content.charCodeAt(pos + 1) === CH_LF) {
          pos += 2;
        } else {
          pos += 1;
        }
        return { fields, nextPos: pos };
      }
      // delimiter
      pos += delimLen;
    }
  }

  return { fields, nextPos: pos };
}

/** Parse a row and write directly into column arrays — no column filtering. */
function parseRowDirect(
  content: string,
  pos: number,
  delimCode: number,
  delimLen: number,
  multiCharDelim: boolean,
  delimiter: string,
  columns: Record<string, (string | null)[]>,
  activeHeader: string[],
  numCols: number,
  nullValues: Set<string>,
): number {
  const len = content.length;
  let colIdx = 0;

  while (pos < len && colIdx <= numCols) {
    const ch = content.charCodeAt(pos);

    // End of line
    if (ch === CH_CR || ch === CH_LF) {
      // Pad remaining columns with null
      while (colIdx < numCols) {
        columns[activeHeader[colIdx]!]!.push(null);
        colIdx++;
      }
      if (ch === CH_CR && pos + 1 < len && content.charCodeAt(pos + 1) === CH_LF) {
        return pos + 2;
      }
      return pos + 1;
    }

    if (colIdx >= numCols) {
      // Extra columns — skip to end of line
      return skipToEOL(content, pos);
    }

    // Quoted field
    if (ch === CH_QUOTE) {
      pos++; // skip opening quote
      const fieldStart = pos;
      let hasEscape = false;

      while (pos < len) {
        const c = content.charCodeAt(pos);
        if (c === CH_QUOTE) {
          if (pos + 1 < len && content.charCodeAt(pos + 1) === CH_QUOTE) {
            hasEscape = true;
            pos += 2;
          } else {
            break;
          }
        } else {
          pos++;
        }
      }

      let value: string;
      if (hasEscape) {
        value = content.substring(fieldStart, pos).replace(/""/g, '"');
      } else {
        value = content.substring(fieldStart, pos);
      }

      if (pos < len) pos++; // skip closing quote

      const colArr = columns[activeHeader[colIdx]!]!;
      colArr.push(nullValues.has(value) ? null : value);
      colIdx++;

      // Skip delimiter
      if (pos < len) {
        const next = content.charCodeAt(pos);
        if (next === CH_CR || next === CH_LF) continue;
        if (multiCharDelim ? content.startsWith(delimiter, pos) : next === delimCode) {
          pos += delimLen;
        }
      }
    } else {
      // Unquoted field
      const fieldStart = pos;
      while (pos < len) {
        const c = content.charCodeAt(pos);
        if (c === CH_CR || c === CH_LF) break;
        if (multiCharDelim ? content.startsWith(delimiter, pos) : c === delimCode) break;
        pos++;
      }

      const value = content.substring(fieldStart, pos);
      const colArr = columns[activeHeader[colIdx]!]!;
      colArr.push(nullValues.has(value) ? null : value);
      colIdx++;

      if (pos >= len) break;

      const c = content.charCodeAt(pos);
      if (c === CH_CR || c === CH_LF) continue;
      pos += delimLen; // skip delimiter
    }
  }

  // Pad remaining columns if line ended early
  while (colIdx < numCols) {
    columns[activeHeader[colIdx]!]!.push(null);
    colIdx++;
  }

  return pos;
}

/** Parse a row with column filtering — only extract selected column indices. */
function parseRowFiltered(
  content: string,
  pos: number,
  delimCode: number,
  delimLen: number,
  multiCharDelim: boolean,
  delimiter: string,
  columns: Record<string, (string | null)[]>,
  activeHeader: string[],
  colIndices: number[],
  numActiveCols: number,
  nullValues: Set<string>,
): number {
  const len = content.length;
  let fieldIdx = 0;
  let activeIdx = 0;
  const nextWanted = colIndices[0] ?? -1;
  let currentWanted = nextWanted;

  while (pos < len) {
    const ch = content.charCodeAt(pos);

    // End of line
    if (ch === CH_CR || ch === CH_LF) {
      while (activeIdx < numActiveCols) {
        columns[activeHeader[activeIdx]!]!.push(null);
        activeIdx++;
      }
      if (ch === CH_CR && pos + 1 < len && content.charCodeAt(pos + 1) === CH_LF) {
        return pos + 2;
      }
      return pos + 1;
    }

    const wanted = fieldIdx === currentWanted;

    // Quoted field
    if (ch === CH_QUOTE) {
      pos++; // skip opening quote
      if (wanted) {
        const fieldStart = pos;
        let hasEscape = false;
        while (pos < len) {
          const c = content.charCodeAt(pos);
          if (c === CH_QUOTE) {
            if (pos + 1 < len && content.charCodeAt(pos + 1) === CH_QUOTE) {
              hasEscape = true;
              pos += 2;
            } else {
              break;
            }
          } else {
            pos++;
          }
        }
        let value: string;
        if (hasEscape) {
          value = content.substring(fieldStart, pos).replace(/""/g, '"');
        } else {
          value = content.substring(fieldStart, pos);
        }
        if (pos < len) pos++; // skip closing quote
        columns[activeHeader[activeIdx]!]!.push(nullValues.has(value) ? null : value);
        activeIdx++;
        currentWanted = activeIdx < numActiveCols ? colIndices[activeIdx]! : -1;
      } else {
        // Skip quoted field
        while (pos < len) {
          const c = content.charCodeAt(pos);
          if (c === CH_QUOTE) {
            if (pos + 1 < len && content.charCodeAt(pos + 1) === CH_QUOTE) {
              pos += 2;
            } else {
              pos++;
              break;
            }
          } else {
            pos++;
          }
        }
      }
    } else {
      // Unquoted field
      const fieldStart = pos;
      while (pos < len) {
        const c = content.charCodeAt(pos);
        if (c === CH_CR || c === CH_LF) break;
        if (multiCharDelim ? content.startsWith(delimiter, pos) : c === delimCode) break;
        pos++;
      }

      if (wanted) {
        const value = content.substring(fieldStart, pos);
        columns[activeHeader[activeIdx]!]!.push(nullValues.has(value) ? null : value);
        activeIdx++;
        currentWanted = activeIdx < numActiveCols ? colIndices[activeIdx]! : -1;
      }
    }

    fieldIdx++;

    if (pos >= len) break;
    const c = content.charCodeAt(pos);
    if (c === CH_CR || c === CH_LF) continue;
    if (multiCharDelim ? content.startsWith(delimiter, pos) : c === delimCode) {
      pos += delimLen;
    }
  }

  // Pad remaining columns
  while (activeIdx < numActiveCols) {
    columns[activeHeader[activeIdx]!]!.push(null);
    activeIdx++;
  }

  return pos;
}

/** Skip to end of line, handling quotes. Returns position after newline. */
function skipToEOL(content: string, pos: number): number {
  let inQuotes = false;
  const len = content.length;
  while (pos < len) {
    const ch = content.charCodeAt(pos);
    if (ch === CH_QUOTE) {
      inQuotes = !inQuotes;
    } else if (!inQuotes) {
      if (ch === CH_CR) {
        pos++;
        if (pos < len && content.charCodeAt(pos) === CH_LF) pos++;
        return pos;
      }
      if (ch === CH_LF) {
        return pos + 1;
      }
    }
    pos++;
  }
  return pos;
}

/**
 * Auto-detect delimiter from first N lines by scoring candidates.
 * Fast version: scans raw content instead of pre-split lines.
 */
function detectDelimiterFast(content: string): string {
  const candidates = [CH_COMMA, CH_SEMI, CH_TAB, CH_PIPE];
  const candidateStrs = [',', ';', '\t', '|'];

  // Gather counts for first ~10 lines
  const maxLines = 10;
  let bestDelimiter = ',';
  let bestScore = -1;

  for (let ci = 0; ci < candidates.length; ci++) {
    const delimCode = candidates[ci]!;
    const counts: number[] = [];
    let pos = 0;
    let lineIdx = 0;

    while (pos < content.length && lineIdx < maxLines) {
      let count = 0;
      let inQuotes = false;
      while (pos < content.length) {
        const ch = content.charCodeAt(pos);
        if (ch === CH_QUOTE) {
          inQuotes = !inQuotes;
        } else if (!inQuotes) {
          if (ch === delimCode) count++;
          if (ch === CH_CR || ch === CH_LF) {
            if (ch === CH_CR && pos + 1 < content.length && content.charCodeAt(pos + 1) === CH_LF) {
              pos++;
            }
            pos++;
            break;
          }
        }
        pos++;
      }
      counts.push(count);
      lineIdx++;
    }

    if (counts.length === 0) continue;
    const avg = counts.reduce((a, b) => a + b, 0) / counts.length;
    if (avg === 0) continue;

    const allSame = counts.every((c) => c === counts[0]);
    const score = allSame ? avg * 2 : avg;

    if (score > bestScore) {
      bestScore = score;
      bestDelimiter = candidateStrs[ci]!;
    }
  }

  return bestDelimiter;
}

/**
 * Infer column types by sampling values.
 */
function inferColumnTypes(
  columns: Record<string, (string | null)[]>,
  header: string[],
  options: CSVReadOptions,
): Record<string, DType> {
  const types: Record<string, DType> = {};
  const parseNumbers = options.parseNumbers !== false;
  const parseDates = options.parseDates !== false;

  for (const name of header) {
    // Check if user specified dtype
    if (options.dtypes && name in options.dtypes) {
      types[name] = options.dtypes[name]!;
      continue;
    }

    const values = columns[name]!;
    const sample = values.slice(0, 100).filter((v): v is string => v !== null);

    if (sample.length === 0) {
      types[name] = DType.Utf8;
      continue;
    }

    // Try number
    if (parseNumbers && sample.every(isNumericString)) {
      types[name] = sample.every(isIntegerString) ? DType.Int32 : DType.Float64;
      continue;
    }

    // Try boolean
    if (sample.every(isBooleanString)) {
      types[name] = DType.Boolean;
      continue;
    }

    // Try date
    if (parseDates && sample.every(isDateString)) {
      types[name] = DType.Date;
      continue;
    }

    types[name] = DType.Utf8;
  }

  return types;
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
