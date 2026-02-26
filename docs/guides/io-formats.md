# I/O Formats

FrameKit supports reading and writing data in seven formats. Some formats are built-in, while others require installing an optional peer dependency.

## Format Support Matrix

| Format | Read | Write | Streaming | Peer Dependency |
|---|---|---|---|---|
| CSV | `fromCSV` | `toCSV` | `streamCSV`, `scanCSV` | None (built-in) |
| JSON | `fromJSON` | `toJSON` | -- | None (built-in) |
| NDJSON | `fromJSON` | `toNDJSON` | `streamNDJSON`, `scanNDJSON` | None (built-in) |
| Arrow | `fromArrow` | `toArrow` | -- | `apache-arrow` |
| Arrow IPC | `fromArrowIPC` | `toArrowIPC` | -- | `apache-arrow` |
| Excel | `fromExcel` | `toExcel` | -- | `xlsx` |
| Parquet | `fromParquet` | `toParquet` | -- | `parquet-wasm` |
| SQL | -- | `toSQL` | -- | None (built-in) |

---

## CSV

CSV is the most common tabular data format. FrameKit includes a built-in RFC 4180 parser with automatic delimiter detection, type inference, and streaming support.

### Reading CSV

```ts
import { DataFrame } from 'framekit-js';

// From a file
const df = await DataFrame.fromCSV('./data.csv');

// From a string
const csv = `name,age,active
Alice,30,true
Bob,25,false`;
const df2 = DataFrame.fromCSV(csv);
```

#### Read Options (`CSVReadOptions`)

| Option | Type | Default | Description |
|---|---|---|---|
| `delimiter` | `string` | Auto-detected | Field separator (`,`, `;`, `\t`, `\|` are auto-detected) |
| `encoding` | `string` | `'utf-8'` | File encoding |
| `hasHeader` | `boolean` | `true` | Whether the first row is a header |
| `header` | `string[]` | -- | Override column names (first row treated as data) |
| `dtypes` | `Record<string, DType>` | -- | Override inferred types for specific columns |
| `parseNumbers` | `boolean` | `true` | Attempt to parse numeric strings as numbers |
| `parseDates` | `boolean` | `true` | Attempt to parse ISO 8601 date strings as Date objects |
| `nRows` | `number` | -- | Maximum number of data rows to read |
| `skipRows` | `number` | `0` | Number of rows to skip before the header |
| `columns` | `string[]` | -- | Read only these columns (others are ignored) |
| `nullValues` | `string[]` | `['', 'null', 'NULL', 'NA', 'N/A', 'NaN', 'nan', 'None', 'none']` | Strings treated as null |
| `comment` | `string` | -- | Lines starting with this string are ignored |

```ts
const df = await DataFrame.fromCSV('./european-data.csv', {
  delimiter: ';',
  skipRows: 2,
  nRows: 1000,
  columns: ['id', 'name', 'value'],
  dtypes: { id: DType.Utf8 },  // Force id to stay as string
  nullValues: ['', 'NA', '-'],
  comment: '#',
});
```

### Writing CSV

```ts
// To a string
const csvString = df.toCSV();

// To a file
await df.toCSV('./output.csv');

// To a writable stream
const stream = fs.createWriteStream('./output.csv');
await df.toCSV(stream);
```

#### Write Options (`CSVWriteOptions`)

| Option | Type | Default | Description |
|---|---|---|---|
| `delimiter` | `string` | `','` | Field separator |
| `quoteStyle` | `'always' \| 'necessary' \| 'never'` | `'necessary'` | When to quote fields |
| `nullValue` | `string` | `''` | String to write for null values |
| `header` | `boolean` | `true` | Whether to include the header row |
| `bom` | `boolean` | `false` | Prepend a UTF-8 BOM (useful for Excel compatibility) |

```ts
await df.toCSV('./output.tsv', {
  delimiter: '\t',
  quoteStyle: 'always',
  nullValue: 'NA',
  bom: true,
});
```

### Streaming CSV

For files too large to fit in memory, use `streamCSV` to process data in chunks.

```ts
for await (const chunk of DataFrame.streamCSV('./huge-file.csv', { chunkSize: 50000 })) {
  // chunk is a DataFrame with up to 50,000 rows
  const filtered = chunk.filter(col<number>('value').gt(100));
  await filtered.toCSV('./filtered.csv', { header: false }); // append mode
}
```

The `scanCSV` method returns a `LazyFrame` that streams the file on `collect()`:

```ts
const result = await DataFrame.scanCSV('./huge-file.csv')
  .filter(col<number>('value').gt(100))
  .select('id', 'value')
  .collect();
```

---

## JSON and NDJSON

### Reading JSON

```ts
// From a file
const df = await DataFrame.fromJSON('./data.json');

// From a string (array of objects)
const df2 = DataFrame.fromJSON('[{"a": 1}, {"a": 2}]');
```

The `path` option extracts a nested array from a JSON structure:

```ts
// If the file contains { "results": { "data": [...] } }
const df = await DataFrame.fromJSON('./response.json', { path: 'results.data' });
```

### Writing JSON

```ts
// To a string (array of objects)
const jsonString = df.toJSON();

// Pretty-printed
const pretty = df.toJSON({ pretty: true });

// To a file
await df.toJSON('./output.json');
```

### NDJSON (Newline-Delimited JSON)

NDJSON stores one JSON object per line. It is streamable and suitable for log data and append-only workloads.

```ts
// Write NDJSON to string
const ndjson = df.toNDJSON();

// Write NDJSON to file
await df.toNDJSON('./output.ndjson');
```

### Streaming NDJSON

```ts
for await (const chunk of DataFrame.streamNDJSON('./logs.ndjson', { chunkSize: 10000 })) {
  // Process each chunk
}

// Or use scanNDJSON for a lazy approach
const result = await DataFrame.scanNDJSON('./logs.ndjson')
  .filter(col<string>('level').eq('error'))
  .collect();
```

---

## Arrow and Arrow IPC

Apache Arrow is a columnar in-memory format designed for zero-copy data sharing between systems. FrameKit supports both Arrow Table objects and the Arrow IPC (Inter-Process Communication) binary format.

**Peer dependency:** `apache-arrow`

```bash
npm install apache-arrow
```

### Working with Arrow Tables

```ts
import { DataFrame } from 'framekit-js';

// Convert a DataFrame to an Arrow Table
const arrowTable = await df.toArrow();

// Create a DataFrame from an Arrow Table
const df2 = DataFrame.fromArrow(arrowTable);
```

### Arrow IPC (Binary Serialization)

Arrow IPC serializes an Arrow Table to a compact binary format, suitable for network transfer or file storage.

```ts
// Serialize to Arrow IPC bytes
const bytes: Uint8Array = await df.toArrowIPC();

// Deserialize from Arrow IPC bytes
const df2 = await DataFrame.fromArrowIPC(bytes);
```

Arrow IPC is useful for exchanging data between Node.js processes, sending data over HTTP, or interoperating with other Arrow-compatible systems (Python/pandas, Rust, Java).

---

## Excel

FrameKit reads and writes `.xlsx` files using the `xlsx` peer dependency.

**Peer dependency:** `xlsx`

```bash
npm install xlsx
```

### Reading Excel

```ts
import { DataFrame } from 'framekit-js';

// Read the first sheet
const df = await DataFrame.fromExcel('./report.xlsx');

// Read a specific sheet by name
const df2 = await DataFrame.fromExcel('./report.xlsx', { sheet: 'Q4 Results' });

// Read a specific sheet by index (0-based)
const df3 = await DataFrame.fromExcel('./report.xlsx', { sheet: 0 });
```

#### Read Options (`ExcelReadOptions`)

| Option | Type | Default | Description |
|---|---|---|---|
| `sheet` | `string \| number` | First sheet | Sheet name or 0-based index |
| `hasHeader` | `boolean` | `true` | Whether the first row contains column names |
| `range` | `string` | -- | Cell range to read (e.g., `'A1:D100'`) |
| `dtypes` | `Record<string, DType>` | -- | Override inferred types for specific columns |

```ts
const df = await DataFrame.fromExcel('./data.xlsx', {
  sheet: 'Sales',
  range: 'B2:F500',
  hasHeader: true,
  dtypes: { 'Order ID': DType.Utf8 },
});
```

### Writing Excel

```ts
await df.toExcel('./output.xlsx');
```

#### Write Options (`ExcelWriteOptions`)

| Option | Type | Default | Description |
|---|---|---|---|
| `sheet` | `string` | -- | Sheet name |
| `autoFilter` | `boolean` | -- | Enable auto-filter on the header row |
| `freezePanes` | `{ row: number; col: number }` | -- | Freeze rows and columns (e.g., `{ row: 1, col: 0 }` freezes the header) |
| `columnWidths` | `Record<string, number>` | -- | Set column widths by column name |
| `startCell` | `string` | -- | Top-left cell for output (e.g., `'B3'`) |

```ts
await df.toExcel('./report.xlsx', {
  sheet: 'Summary',
  autoFilter: true,
  freezePanes: { row: 1, col: 0 },
  columnWidths: { name: 25, description: 50 },
  startCell: 'A1',
});
```

---

## Parquet

Apache Parquet is a columnar storage format optimized for analytics workloads. It supports compression and is widely used in data lake architectures.

**Peer dependency:** `parquet-wasm`

```bash
npm install parquet-wasm
```

### Reading Parquet

```ts
import { DataFrame } from 'framekit-js';

// Read all columns
const df = await DataFrame.fromParquet('./data.parquet');

// Read specific columns (column pruning)
const df2 = await DataFrame.fromParquet('./data.parquet', {
  columns: ['id', 'name', 'value'],
});
```

#### Read Options (`ParquetReadOptions`)

| Option | Type | Default | Description |
|---|---|---|---|
| `columns` | `string[]` | -- | Read only these columns (others are skipped) |

### Writing Parquet

```ts
await df.toParquet('./output.parquet');
```

#### Write Options (`ParquetWriteOptions`)

| Option | Type | Default | Description |
|---|---|---|---|
| `compression` | `'snappy' \| 'gzip' \| 'zstd' \| 'none'` | -- | Compression algorithm |
| `rowGroupSize` | `number` | -- | Number of rows per row group |

```ts
await df.toParquet('./output.parquet', {
  compression: 'zstd',
  rowGroupSize: 100000,
});
```

---

## SQL Export

FrameKit can generate SQL `INSERT` statements from a DataFrame. This is useful for seeding databases, generating migration data, or building test fixtures.

Note: This is a write-only format. FrameKit does not read from SQL databases.

```ts
import { DataFrame } from 'framekit-js';

const df = DataFrame.fromRows([
  { id: 1, name: 'Alice', active: true },
  { id: 2, name: "Bob's", active: false },
  { id: 3, name: 'Carol', active: true },
]);

const sql = df.toSQL('users');
```

Output:

```sql
INSERT INTO "users" ("id", "name", "active") VALUES
(1, 'Alice', TRUE),
(2, 'Bob''s', FALSE),
(3, 'Carol', TRUE);
```

#### Options (`SQLWriteOptions`)

| Option | Type | Default | Description |
|---|---|---|---|
| `batchSize` | `number` | `1000` | Maximum rows per INSERT statement |

For large DataFrames, the output is split into multiple INSERT statements:

```ts
const sql = df.toSQL('events', { batchSize: 500 });
// Produces one INSERT per 500 rows
```

Values are properly escaped: strings have single quotes escaped, identifiers are double-quoted, dates are serialized as ISO 8601, nulls become `NULL`, and non-finite numbers become `NULL`.

---

## Custom Readers and Writers

FrameKit provides an extension mechanism for registering custom file format handlers. This lets you use `DataFrame.fromFile()` and `df.toFile()` with any file extension.

### Registering a custom reader

```ts
import { DataFrame } from 'framekit-js';

DataFrame.registerReader('msgpack', async (source: Buffer) => {
  const { unpack } = await import('msgpackr');
  const rows = unpack(source) as Record<string, unknown>[];
  return DataFrame.fromRows(rows);
});

// Now you can read .msgpack files
const df = await DataFrame.fromFile('./data.msgpack');
```

### Registering a custom writer

```ts
DataFrame.registerWriter('msgpack', async (df: DataFrame, path: string) => {
  const { pack } = await import('msgpackr');
  const fs = await import('fs/promises');
  const buffer = pack(df.toArray());
  await fs.writeFile(path, buffer);
});

// Now you can write .msgpack files
await df.toFile('./output.msgpack');
```

### Using `fromFile` and `toFile`

These methods determine the format from the file extension and dispatch to the registered reader or writer. Built-in formats do not need registration -- custom formats do.

```ts
// Reads using the registered reader for .msgpack
const df = await DataFrame.fromFile<{ id: number; name: string }>('./data.msgpack');

// Writes using the registered writer for .msgpack
await df.toFile('./output.msgpack');
```

If no reader or writer is registered for a given extension, FrameKit throws an `IOError` with a helpful message suggesting you call `registerReader` or `registerWriter`.

---

## Gotchas and Notes

1. **Peer dependencies are optional.** FrameKit does not bundle `apache-arrow`, `xlsx`, or `parquet-wasm`. Install only the ones you need. If you call a method that requires a missing dependency, FrameKit throws an `IOError` with installation instructions.

2. **CSV auto-detection is heuristic.** The delimiter is detected by scoring candidates (`,`, `;`, `\t`, `|`) against the first 10 lines. If your file uses an unusual delimiter, specify it explicitly with the `delimiter` option.

3. **CSV type inference samples 100 rows.** The parser examines the first 100 data rows to infer column types. If a column contains mixed types beyond the sample window, the inferred type may be incorrect. Use the `dtypes` option to override.

4. **`fromCSV` with a string argument is synchronous.** When you pass a CSV string directly, the result is returned synchronously. When you pass a file path, it returns a Promise.

5. **`toCSV` overloading.** `toCSV()` with no arguments returns a string. `toCSV(filePath)` writes to a file and returns a Promise. `toCSV(stream)` writes to a Node.js Writable stream and returns a Promise.

6. **`streamCSV` and `streamNDJSON` are async generators.** Use `for await...of` to iterate over chunks. Each chunk is an independent DataFrame.

7. **`scanCSV` and `scanNDJSON` load all data on `collect()`.** They stream the file in chunks to avoid a single large allocation, but the final DataFrame resides in memory. For true streaming, use `streamCSV` / `streamNDJSON` and process chunks individually.

8. **Excel range format.** The `range` option uses standard Excel cell references like `'A1:D100'`. If not specified, the entire used range of the sheet is read.

9. **Parquet column pruning.** When reading Parquet files, always specify the `columns` option if you only need a subset of columns. This can significantly reduce I/O and memory usage because Parquet stores data columnar.

10. **SQL output is standard SQL.** The generated INSERT statements use ANSI SQL syntax. They should work with PostgreSQL, MySQL, SQLite, and most other SQL databases. Identifiers are double-quoted for safety.

11. **Default null values in CSV.** The following strings are treated as null by default: `''`, `'null'`, `'NULL'`, `'NA'`, `'N/A'`, `'NaN'`, `'nan'`, `'None'`, `'none'`. Override with the `nullValues` option if your data uses different sentinel values.
