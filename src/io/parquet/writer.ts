import type { ParquetWriteOptions } from '../../types/options';
import { DType } from '../../types/dtype';
import { IOError } from '../../errors';

export type ParquetCompression = 'snappy' | 'gzip' | 'zstd' | 'none';

// eslint-disable-next-line @typescript-eslint/no-explicit-any
function dtypeToArrowType(arrow: any, dtype: DType): any {
  switch (dtype) {
    case DType.Float64:
      // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-unsafe-return
      return new arrow.Float64();
    case DType.Int32:
      // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-unsafe-return
      return new arrow.Int32();
    case DType.Utf8:
      // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-unsafe-return
      return new arrow.Utf8();
    case DType.Boolean:
      // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-unsafe-return
      return new arrow.Bool();
    case DType.Date:
    case DType.DateTime:
      // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-unsafe-return
      return new arrow.DateMillisecond();
    default:
      // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-unsafe-return
      return new arrow.Utf8();
  }
}

function coerceValues(values: unknown[], dtype: DType): unknown[] {
  switch (dtype) {
    case DType.Float64:
    case DType.Int32:
      return values.map((v) => (v === null || v === undefined ? null : Number(v)));
    case DType.Utf8:
      return values.map((v) => {
        if (v === null || v === undefined) return null;
        if (typeof v === 'string') return v;
        if (typeof v === 'number' || typeof v === 'boolean' || typeof v === 'bigint') return String(v);
        return typeof v === 'object' ? JSON.stringify(v) : String(v as string);
      });
    case DType.Boolean:
      return values.map((v) => (v === null || v === undefined ? null : Boolean(v)));
    case DType.Date:
    case DType.DateTime:
      return values.map((v) => {
        if (v === null || v === undefined) return null;
        if (v instanceof Date) return v.getTime();
        if (typeof v === 'number') return v;
        return null;
      });
    default:
      return values.map((v) => {
        if (v === null || v === undefined) return null;
        if (typeof v === 'string') return v;
        if (typeof v === 'number' || typeof v === 'boolean' || typeof v === 'bigint') {
          return String(v);
        }
        // eslint-disable-next-line @typescript-eslint/no-base-to-string
        return typeof v === 'object' ? JSON.stringify(v) : String(v as string);
      });
  }
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
function resolveCompression(parquetWasm: any, compression: ParquetCompression): any {
  // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unsafe-assignment
  const Compression = parquetWasm.Compression;
  if (!Compression) return compression.toUpperCase();
  switch (compression) {
    case 'snappy':
      // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unsafe-return
      return Compression.SNAPPY;
    case 'gzip':
      // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unsafe-return
      return Compression.GZIP;
    case 'zstd':
      // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unsafe-return
      return Compression.ZSTD;
    case 'none':
      // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unsafe-return
      return Compression.UNCOMPRESSED;
  }
}

export async function writeParquetFile(
  filePath: string,
  header: string[],
  columns: Record<string, { values: unknown[]; dtype: DType }>,
  options: ParquetWriteOptions = {},
): Promise<void> {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  let parquetWasm: any;
  try {
    const moduleName = 'parquet-wasm';
    // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment
    parquetWasm = await import(moduleName);
  } catch {
    throw new IOError(
      'parquet-wasm is required to write Parquet files but is not installed. Run: npm install parquet-wasm',
    );
  }

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  let arrow: any;
  try {
    const arrowModule = 'apache-arrow';
    // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment
    arrow = await import(arrowModule);
  } catch {
    throw new IOError(
      'apache-arrow is required to write Parquet files but is not installed. Run: npm install apache-arrow',
    );
  }

  try {
    // Build Arrow fields and column data using apache-arrow
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const fields: any[] = [];
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const arrowColumns: Record<string, any> = {};

    for (const name of header) {
      const col = columns[name]!;
      // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment
      const arrowType = dtypeToArrowType(arrow, col.dtype);
      // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unsafe-call
      fields.push(new arrow.Field(name, arrowType, true));
      const coerced = coerceValues(col.values, col.dtype);
      // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-unsafe-assignment
      arrowColumns[name] = arrow.vectorFromArray(coerced, arrowType);
    }

    // Create Arrow schema and table
    // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-unsafe-assignment
    const schema = new arrow.Schema(fields);
    // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-unsafe-assignment
    const arrowTable = new arrow.Table(schema, arrowColumns);

    // Serialize to Arrow IPC stream format
    // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-unsafe-assignment
    const ipcBytes: Uint8Array = arrow.tableToIPC(arrowTable, 'stream');

    // Convert IPC stream to parquet-wasm Table
    // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-unsafe-assignment
    const wasmTable = parquetWasm.Table.fromIPCStream(ipcBytes);

    // Build writer properties
    const compression = options.compression ?? 'snappy';
    // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment
    const resolvedCompression = resolveCompression(parquetWasm, compression);

    // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-unsafe-assignment
    let builder = new parquetWasm.WriterPropertiesBuilder();
    // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-unsafe-assignment
    builder = builder.setCompression(resolvedCompression);

    if (options.rowGroupSize !== undefined) {
      // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-unsafe-assignment
      builder = builder.setMaxRowGroupSize(options.rowGroupSize);
    }

    // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-unsafe-assignment
    const writerProperties = builder.build();

    // Write to Parquet bytes
    // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-unsafe-assignment
    const parquetBytes: Uint8Array = parquetWasm.writeParquet(wasmTable, writerProperties);

    // Write bytes to file
    const fs = await import('fs/promises');
    await fs.writeFile(filePath, parquetBytes);
  } catch (err) {
    if (err instanceof IOError) throw err;
    const message = err instanceof Error ? err.message : String(err);
    throw new IOError(`Failed to write Parquet file '${filePath}': ${message}`);
  }
}
