# DataFrame API Reference

Complete reference for the `DataFrame<S>` class. `S` is a generic row-shape type that flows through all transformations for full TypeScript inference.

```ts
import { DataFrame } from 'framekit-js';
```

---

## Construction

### `DataFrame.fromRows<S>(rows: object[]): DataFrame<S>`

Create a DataFrame from an array of row objects. Column names and types are inferred from the first row.

```ts
const df = DataFrame.fromRows([
  { name: 'Alice', age: 30 },
  { name: 'Bob', age: 25 },
]);
```

### `DataFrame.fromColumns<S>(data: Record<string, unknown[]>): DataFrame<S>`

Create a DataFrame from a column-oriented record. Each key is a column name and each value is an array of column values. All arrays must have the same length.

```ts
const df = DataFrame.fromColumns({
  name: ['Alice', 'Bob'],
  age: [30, 25],
});
```

### `DataFrame.empty<S>(): DataFrame<S>`

Create an empty DataFrame with no rows and no columns.

```ts
const df = DataFrame.empty();
// df.length === 0, df.columns === []
```

### `DataFrame.range<S>(name: string, start: number, end: number, step?: number): DataFrame<S>`

Create a single-column DataFrame containing a numeric range from `start` (inclusive) to `end` (exclusive), incrementing by `step` (default `1`).

| Parameter | Type | Description |
|-----------|------|-------------|
| `name` | `string` | Column name |
| `start` | `number` | Start value (inclusive) |
| `end` | `number` | End value (exclusive) |
| `step` | `number` | Increment (default `1`) |

```ts
const df = DataFrame.range('i', 0, 5);
// i: [0, 1, 2, 3, 4]
```

### `DataFrame.linspace<S>(name: string, start: number, end: number, count: number): DataFrame<S>`

Create a single-column DataFrame containing `count` evenly spaced values between `start` and `end` (both inclusive).

| Parameter | Type | Description |
|-----------|------|-------------|
| `name` | `string` | Column name |
| `start` | `number` | Start value (inclusive) |
| `end` | `number` | End value (inclusive) |
| `count` | `number` | Number of values to generate |

```ts
const df = DataFrame.linspace('x', 0, 1, 5);
// x: [0, 0.25, 0.5, 0.75, 1]
```

### `DataFrame.concat(...frames: DataFrame[]): DataFrame`

Concatenate multiple DataFrames vertically (row-wise). All frames must share the same column names.

```ts
const combined = DataFrame.concat(dfA, dfB, dfC);
```

---

## Accessors

### `shape: [number, number]`

Returns a tuple of `[rows, columns]`.

```ts
df.shape; // [100, 5]
```

### `columns: string[]`

Returns an array of column names in order.

```ts
df.columns; // ['name', 'age', 'city']
```

### `length: number`

Returns the number of rows.

```ts
df.length; // 100
```

### `col<K extends keyof S>(name: K): Series<S[K]>`

Retrieve a single column as a `Series`. The return type is inferred from the row shape.

```ts
const ages: Series<number> = df.col('age');
```

### `row(index: number): S`

Get a single row by zero-based index. Negative indices count from the end.

```ts
df.row(0);  // { name: 'Alice', age: 30 }
df.row(-1); // last row
```

### `toArray(): S[]`

Materialize all rows into a plain JavaScript array of objects.

```ts
const rows = df.toArray();
```

### `[Symbol.iterator]()`

DataFrame is iterable. You can use `for...of` to iterate over rows.

```ts
for (const row of df) {
  console.log(row.name);
}
```

---

## Selection

### `select<K extends keyof S>(...columns: K[]): DataFrame<Pick<S, K>>`

Return a new DataFrame containing only the specified columns. Column order matches the argument order.

```ts
const slim = df.select('name', 'age');
```

### `drop<K extends keyof S>(...columns: K[]): DataFrame<Omit<S, K>>`

Return a new DataFrame with the specified columns removed.

```ts
const noAge = df.drop('age');
```

### `head(n?: number): DataFrame<S>`

Return the first `n` rows. Default is `5`.

```ts
df.head();   // first 5 rows
df.head(10); // first 10 rows
```

### `tail(n?: number): DataFrame<S>`

Return the last `n` rows. Default is `5`.

```ts
df.tail();   // last 5 rows
df.tail(10); // last 10 rows
```

### `slice(start: number, end?: number): DataFrame<S>`

Return a subset of rows from `start` (inclusive) to `end` (exclusive). If `end` is omitted, slices to the end of the DataFrame.

```ts
df.slice(10, 20); // rows 10 through 19
df.slice(5);      // rows 5 through the end
```

---

## Filtering and Sorting

### `filter(predicate: ((row: S) => boolean) | Expr<boolean>): DataFrame<S>`

Return rows matching the predicate. Accepts either a callback function or an `Expr<boolean>`.

```ts
// Callback style
df.filter(row => row.age > 30);

// Expression style
import { col } from 'framekit-js';
df.filter(col('age').gt(30));
```

### `where(column: string, op: '=' | '!=' | '>' | '>=' | '<' | '<=', value: unknown): DataFrame<S>`

Shorthand filter using a comparison operator on a single column.

| Parameter | Type | Description |
|-----------|------|-------------|
| `column` | `string` | Column name to compare |
| `op` | `'=' \| '!=' \| '>' \| '>=' \| '<' \| '<='` | Comparison operator |
| `value` | `unknown` | Value to compare against |

```ts
df.where('age', '>=', 18);
df.where('status', '!=', 'inactive');
```

### `sortBy(columns: string | string[], order?: 'asc' | 'desc' | ('asc' | 'desc')[]): DataFrame<S>`

Sort the DataFrame by one or more columns. When multiple columns are provided, `order` can be an array specifying direction per column.

```ts
df.sortBy('age');                          // ascending by default
df.sortBy('age', 'desc');                  // descending
df.sortBy(['city', 'age'], ['asc', 'desc']); // city asc, age desc
```

### `unique(columns?: string | string[], keep?: 'first' | 'last'): DataFrame<S>`

Remove duplicate rows. When `columns` is provided, uniqueness is determined by those columns only. `keep` controls which duplicate to retain (default `'first'`).

```ts
df.unique();                        // all columns
df.unique('email');                  // unique by email
df.unique(['city', 'state'], 'last'); // keep last occurrence
```

### `sample(n: number, options?: SampleOptions): DataFrame<S>`

Return `n` randomly sampled rows. Pass `{ seed }` for reproducible results.

```ts
df.sample(10);
df.sample(5, { seed: 42 }); // reproducible
```

**SampleOptions:**

| Property | Type | Description |
|----------|------|-------------|
| `seed` | `number` | Random seed for reproducibility |

---

## Transformation

### `withColumn<K extends string, V>(name: K, valuesOrFnOrExpr: V[] | ((row: S) => V) | Expr<V>): DataFrame<S & Record<K, V>>`

Add or replace a column. Accepts an array of values, a row-level callback, or an expression. The returned DataFrame type includes the new column.

```ts
// From callback
df.withColumn('senior', row => row.age >= 65);

// From expression
df.withColumn('doubled', col('age').mul(2));

// From array
df.withColumn('index', [0, 1, 2, 3, 4]);
```

### `derive(exprs: Record<string, (row: S) => unknown>): DataFrame`

Add multiple computed columns at once. Each key is a column name and each value is a row-level function.

```ts
const enriched = df.derive({
  fullName: row => `${row.first} ${row.last}`,
  ageGroup: row => row.age >= 18 ? 'adult' : 'minor',
});
```

### `rename(mapping: Record<string, string>): DataFrame<S>`

Rename columns. Keys are current names, values are new names.

```ts
df.rename({ name: 'fullName', age: 'years' });
```

### `cast(dtypes: Partial<Record<keyof S, DType>>): DataFrame<S>`

Cast columns to different data types.

**DType values:** `'Float64'` | `'Int32'` | `'Utf8'` | `'Boolean'` | `'Date'` | `'Object'`

```ts
df.cast({ age: 'Float64', active: 'Boolean' });
```

### `apply(fn: (row: S) => S): DataFrame<S>`

Apply a function to each row, returning a new row object with the same shape.

```ts
df.apply(row => ({ ...row, name: row.name.toUpperCase() }));
```

### `assign(other: DataFrame): DataFrame`

Merge columns from another DataFrame into this one. Both DataFrames must have the same number of rows. Columns from `other` overwrite columns with the same name.

```ts
const withScores = df.assign(scoresDf);
```

### `relocate(columns: string[], options: { before?: string; after?: string }): DataFrame`

Reorder columns by moving the specified columns to a new position relative to an anchor column.

```ts
df.relocate(['age', 'city'], { after: 'id' });
df.relocate(['name'], { before: 'email' });
```

### `clone(): DataFrame<S>`

Create a shallow copy of the DataFrame.

```ts
const copy = df.clone();
```

### `reify(): DataFrame<S>`

Create a deep copy of the DataFrame, fully materializing all data. Useful for value isolation after lazy chains.

```ts
const isolated = df.reify();
```

---

## Missing Values

### `dropNull(columns?: string | string[]): DataFrame<S>`

Remove rows containing null or undefined values. When `columns` is specified, only those columns are checked.

```ts
df.dropNull();         // drop if any column is null
df.dropNull('email');  // drop only if email is null
df.dropNull(['a', 'b']);
```

### `fillNull(strategy: Record<string, unknown> | 'forward' | 'backward'): DataFrame<S>`

Fill null values. Pass a record mapping column names to fill values, or a string strategy for directional filling.

```ts
df.fillNull({ age: 0, name: 'Unknown' }); // per-column values
df.fillNull('forward');   // forward-fill (last observation carried forward)
df.fillNull('backward');  // backward-fill
```

### `impute(values: Record<string, unknown>, options?: { expand?: boolean }): DataFrame`

Fill null values with specified defaults. When `expand` is `true`, generates missing key combinations before filling.

```ts
df.impute({ sales: 0 });
df.impute({ sales: 0 }, { expand: true }); // fill structural gaps
```

---

## Grouping

### `groupBy<K extends keyof S>(...keys: K[]): GroupBy<S, K>`

Group the DataFrame by one or more columns. Returns a `GroupBy` object that supports aggregation methods.

```ts
const grouped = df.groupBy('department');
const grouped2 = df.groupBy('city', 'state');
```

### GroupBy Methods

#### `agg(specs: Record<string, AggExpr>): DataFrame`

Apply aggregation expressions to produce a summary DataFrame. Each key is the output column name and the value is an aggregation expression built from `col()`.

```ts
import { col } from 'framekit-js';

df.groupBy('department').agg({
  avgSalary: col('salary').mean(),
  headCount: col('id').count(),
  topSalary: col('salary').max(),
});
```

#### `count(): DataFrame`

Count rows per group.

```ts
df.groupBy('city').count();
```

#### `sum(column: string): DataFrame`

Sum a column per group.

```ts
df.groupBy('category').sum('revenue');
```

#### `mean(column: string): DataFrame`

Compute the mean of a column per group.

```ts
df.groupBy('region').mean('temperature');
```

#### `min(column: string): DataFrame`

Compute the minimum of a column per group.

#### `max(column: string): DataFrame`

Compute the maximum of a column per group.

#### `first(): DataFrame`

Return the first row of each group.

#### `last(): DataFrame`

Return the last row of each group.

#### `apply(fn: (group: DataFrame) => DataFrame): DataFrame`

Apply a function to each group DataFrame and concatenate the results.

```ts
df.groupBy('team').apply(group =>
  group.sortBy('score', 'desc').head(3)
);
```

#### `groups(): Map<string, DataFrame>`

Return a Map from group key (stringified) to group DataFrame.

```ts
const map = df.groupBy('city').groups();
for (const [key, group] of map) {
  console.log(key, group.length);
}
```

#### `nGroups(): number`

Return the number of distinct groups.

```ts
df.groupBy('city').nGroups(); // 12
```

---

## Joining

### `join<R>(other: DataFrame<R>, on, how?, options?): DataFrame`

Join two DataFrames.

| Parameter | Type | Description |
|-----------|------|-------------|
| `other` | `DataFrame<R>` | Right-side DataFrame |
| `on` | `string \| string[] \| { left: string \| string[]; right: string \| string[] }` | Join key(s). A string or array for shared column names, or an object for different column names on each side. |
| `how` | `'inner' \| 'left' \| 'right' \| 'outer' \| 'cross' \| 'semi' \| 'anti'` | Join type (default `'inner'`) |
| `options` | `{ suffix?: string }` | Suffix appended to disambiguate overlapping column names (default `'_right'`) |

**Join types:**

| Type | Description |
|------|-------------|
| `inner` | Only matching rows from both sides |
| `left` | All rows from left, matching from right |
| `right` | All rows from right, matching from left |
| `outer` | All rows from both sides |
| `cross` | Cartesian product (ignores `on`) |
| `semi` | Left rows that have a match in right |
| `anti` | Left rows that have no match in right |

```ts
// Shared key
orders.join(customers, 'customerId', 'left');

// Different key names
orders.join(products, { left: 'productId', right: 'id' }, 'inner');

// With suffix
df.join(other, 'id', 'outer', { suffix: '_b' });
```

---

## Reshape

### `pivot(options): DataFrame`

Reshape from long to wide format.

| Option | Type | Description |
|--------|------|-------------|
| `index` | `string \| string[]` | Row identifier column(s) |
| `columns` | `string` | Column whose values become new column names |
| `values` | `string` | Column whose values populate the cells |
| `aggFunc` | `'sum' \| 'mean' \| 'count' \| 'first' \| 'last'` | Aggregation when multiple values exist (optional) |

```ts
df.pivot({
  index: 'date',
  columns: 'metric',
  values: 'value',
  aggFunc: 'sum',
});
```

### `melt(options): DataFrame`

Reshape from wide to long format (unpivot).

| Option | Type | Description |
|--------|------|-------------|
| `idVars` | `string \| string[]` | Columns to keep as identifiers |
| `valueVars` | `string \| string[]` | Columns to unpivot (default: all non-id columns) |
| `varName` | `string` | Name for the variable column (default `'variable'`) |
| `valueName` | `string` | Name for the value column (default `'value'`) |

```ts
df.melt({
  idVars: 'date',
  valueVars: ['temp', 'humidity'],
  varName: 'metric',
  valueName: 'reading',
});
```

### `explode(column: string): DataFrame`

Expand array-valued cells into individual rows. Other columns are repeated.

```ts
// { tags: ['a', 'b'], id: 1 } becomes two rows: { tags: 'a', id: 1 }, { tags: 'b', id: 1 }
df.explode('tags');
```

### `transpose(headerColumn?: string): DataFrame`

Transpose rows and columns. If `headerColumn` is provided, that column's values become the new column names.

```ts
df.transpose();
df.transpose('metric'); // use 'metric' column values as new headers
```

### `spread(column: string): DataFrame`

Expand object or array values in a column into separate columns.

```ts
// { data: { x: 1, y: 2 } } becomes { data_x: 1, data_y: 2 }
df.spread('data');
```

### `unroll(columns: string | string[], options?: { index?: string }): DataFrame`

Expand array values into rows (similar to `explode` but supports multiple columns and an optional index column).

| Parameter | Type | Description |
|-----------|------|-------------|
| `columns` | `string \| string[]` | Column(s) containing arrays to unroll |
| `options.index` | `string` | Name of an index column tracking position within each array |

```ts
df.unroll('values');
df.unroll(['xs', 'ys'], { index: 'i' });
```

---

## Set Operations

### `union(other: DataFrame): DataFrame`

Return all rows from both DataFrames, removing duplicates.

```ts
const combined = dfA.union(dfB);
```

### `intersection(other: DataFrame): DataFrame`

Return only rows that appear in both DataFrames.

```ts
const common = dfA.intersection(dfB);
```

### `difference(other: DataFrame): DataFrame`

Return rows from this DataFrame that do not appear in the other.

```ts
const onlyA = dfA.difference(dfB);
```

---

## Lazy Evaluation

### `lazy(): LazyFrame<S>`

Convert to a `LazyFrame` for deferred execution. Operations are batched and optimized before materialization.

```ts
const result = df
  .lazy()
  .filter(col('age').gt(30))
  .select('name', 'age')
  .collect(); // materializes
```

---

## Specialized

### `lookup(other: DataFrame, on: string, values?: string[]): DataFrame`

Key-based enrichment. Looks up matching rows in `other` by the `on` column and appends the specified `values` columns (or all non-key columns if omitted). Produces one output row per left row.

```ts
orders.lookup(products, 'productId', ['name', 'price']);
```

### `query(queryStr: string): DataFrame`

Filter rows using a string query expression.

```ts
df.query('age > 30 and city == "Rome"');
```

### `describe(): DataFrame`

Compute summary statistics (count, mean, std, min, 25%, 50%, 75%, max) for all numeric columns.

```ts
df.describe().print();
```

---

## Output

### `toString(options?: { maxRows?: number; maxCols?: number }): string`

Return a formatted string table representation of the DataFrame.

```ts
console.log(df.toString({ maxRows: 20 }));
```

### `print(options?: { maxRows?: number; maxCols?: number }): void`

Print the DataFrame table to the console. Equivalent to `console.log(df.toString(options))`.

```ts
df.print();
df.print({ maxRows: 50, maxCols: 10 });
```

### `info(): void`

Print column names, types, non-null counts, and memory usage to the console.

```ts
df.info();
```

### `memoryUsage(): number`

Return the estimated memory footprint in bytes.

```ts
df.memoryUsage(); // 4096
```

---

## I/O

FrameKit supports many input/output formats. See the [I/O Formats guide](../guides/io-formats.md) for full details and options.

### Reading

| Method | Description |
|--------|-------------|
| `DataFrame.fromCSV(source, options?)` | Parse CSV string or stream |
| `DataFrame.fromJSON(source, options?)` | Parse JSON string or object |
| `DataFrame.fromExcel(buffer, options?)` | Parse Excel (.xlsx) buffer |
| `DataFrame.fromParquet(buffer, options?)` | Parse Parquet buffer |
| `DataFrame.fromArrow(table)` | Import from Apache Arrow Table |
| `DataFrame.fromArrowIPC(buffer)` | Import from Arrow IPC format |
| `DataFrame.fromFile(path, options?)` | Auto-detect format from file extension |
| `DataFrame.scanCSV(path, options?)` | Lazy-scan a CSV file (streaming) |
| `DataFrame.scanNDJSON(path, options?)` | Lazy-scan an NDJSON file (streaming) |
| `DataFrame.streamCSV(source, options?)` | Stream CSV rows |
| `DataFrame.streamNDJSON(source, options?)` | Stream NDJSON rows |
| `DataFrame.registerReader(ext, reader)` | Register a custom reader for a file extension |

### Writing

| Method | Description |
|--------|-------------|
| `df.toCSV(options?)` | Serialize to CSV string |
| `df.toJSON(options?)` | Serialize to JSON string |
| `df.toNDJSON(options?)` | Serialize to newline-delimited JSON |
| `df.toExcel(options?)` | Serialize to Excel buffer |
| `df.toParquet(options?)` | Serialize to Parquet buffer |
| `df.toArrow()` | Export as Apache Arrow Table |
| `df.toArrowIPC()` | Export as Arrow IPC buffer |
| `df.toSQL(tableName, options?)` | Generate SQL INSERT statements |
| `df.toFile(path, options?)` | Auto-detect format from file extension |
| `DataFrame.registerWriter(ext, writer)` | Register a custom writer for a file extension |
