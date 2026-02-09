import type { ParquetReadOptions } from '../../types/options';
import { DType } from '../../types/dtype';
import { IOError } from '../../errors';

export interface ParsedParquetData {
  header: string[];
  columns: Record<string, unknown[]>;
  inferredTypes: Record<string, DType>;
}

/**
 * Map Parquet Arrow data types to FrameKit DType.
 * parquet-wasm exposes Apache Arrow record batches — we inspect Arrow field types.
 */
// eslint-disable-next-line @typescript-eslint/no-explicit-any
function arrowTypeToDType(field: any): DType {
  // Arrow type objects from parquet-wasm / arrow-js have a typeId or toString()
  // We inspect the type's toString() representation for robust matching
  // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment, @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unsafe-call
  const typeStr: string = field.type?.toString?.() ?? String(field.type);
  const lower = typeStr.toLowerCase();

  if (lower.includes('int32') || lower.includes('int16') || lower.includes('int8')) {
    return DType.Int32;
  }
  if (lower.includes('int64') || lower.includes('int') || lower.includes('uint')) {
    // No Int64Column exists — map to Float64 for numeric compatibility
    return DType.Float64;
  }
  if (lower.includes('float') || lower.includes('double') || lower.includes('decimal')) {
    return DType.Float64;
  }
  if (lower.includes('utf8') || lower.includes('string') || lower.includes('largestring') || lower.includes('largeutf8')) {
    return DType.Utf8;
  }
  if (lower === 'bool' || lower.includes('boolean')) {
    return DType.Boolean;
  }
  if (lower.includes('date') || lower.includes('timestamp')) {
    return DType.Date;
  }

  // Fallback to Utf8 for unknown types
  return DType.Utf8;
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
function extractColumnValues(column: any, dtype: DType, length: number): unknown[] {
  const values: unknown[] = [];
  for (let i = 0; i < length; i++) {
    // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unsafe-call
    if (column.isValid(i) === false) {
      values.push(null);
      continue;
    }
    // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-unsafe-assignment
    let val = column.get(i);

    // Convert BigInt to number for numeric dtypes
    if (typeof val === 'bigint') {
      val = Number(val);
    }

    if (dtype === DType.Date && typeof val === 'number') {
      val = new Date(val);
    }

    values.push(val as unknown);
  }
  return values;
}

export async function readParquetFile(
  filePath: string,
  options: ParquetReadOptions = {},
): Promise<ParsedParquetData> {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  let parquetWasm: any; // eslint-disable-line @typescript-eslint/no-unsafe-assignment
  try {
    const moduleName = 'parquet-wasm';
    // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment
    parquetWasm = await import(moduleName);
  } catch {
    throw new IOError(
      'parquet-wasm is required to read Parquet files but is not installed. Run: npm install parquet-wasm',
    );
  }

  try {
    const fs = await import('fs/promises');
    const buffer = await fs.readFile(filePath);
    const uint8 = new Uint8Array(buffer.buffer, buffer.byteOffset, buffer.byteLength);

    // Read the Parquet file into an Arrow table
    // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-unsafe-assignment
    const arrowTable = parquetWasm.readParquet(uint8);

    // Get schema from the Arrow table
    // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unsafe-assignment
    const schema = arrowTable.schema;
    // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unsafe-assignment
    const numFields: number = schema.numFields ?? schema.fields?.length ?? 0;
    // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unsafe-assignment
    const numRows: number = arrowTable.numRows ?? 0;

    // Collect all field names and types
    const allFields: Array<{ name: string; dtype: DType; index: number }> = [];
    for (let i = 0; i < numFields; i++) {
      // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-unsafe-assignment
      const field = schema.field(i);
      // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access
      const name = String(field.name);
      const dtype = arrowTypeToDType(field);
      allFields.push({ name, dtype, index: i });
    }

    // Apply column projection if specified
    const selectedFields = options.columns
      ? allFields.filter((f) => options.columns!.includes(f.name))
      : allFields;

    const header: string[] = [];
    const columns: Record<string, unknown[]> = {};
    const inferredTypes: Record<string, DType> = {};

    for (const field of selectedFields) {
      header.push(field.name);
      inferredTypes[field.name] = field.dtype;

      // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-unsafe-assignment
      const arrowCol = arrowTable.getChildAt(field.index);
      if (arrowCol) {
        columns[field.name] = extractColumnValues(arrowCol, field.dtype, numRows);
      } else {
        // Column not accessible — fill with nulls
        columns[field.name] = new Array<null>(numRows).fill(null);
      }
    }

    return { header, columns, inferredTypes };
  } catch (err) {
    if (err instanceof IOError) throw err;
    const message = err instanceof Error ? err.message : String(err);
    throw new IOError(`Failed to read Parquet file '${filePath}': ${message}`);
  }
}
