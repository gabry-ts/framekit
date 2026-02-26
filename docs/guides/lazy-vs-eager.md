# Lazy vs Eager Execution

FrameKit supports two execution models: **eager** and **lazy**. Understanding when to use each model is essential for writing efficient data pipelines.

## Eager Execution

In eager mode, every operation materializes its result immediately. When you call `.filter()`, `.select()`, or `.sortBy()`, the engine executes the transformation on the spot and returns a new `DataFrame` containing the result.

```ts
import { DataFrame, col } from 'framekit-js';

const df = DataFrame.fromRows([
  { name: 'Alice', age: 30, dept: 'eng' },
  { name: 'Bob', age: 25, dept: 'eng' },
  { name: 'Carol', age: 35, dept: 'sales' },
]);

// Each call executes immediately and allocates a new DataFrame
const engineers = df.filter(col<number>('age').gt(26));
const names = engineers.select('name', 'age');
const sorted = names.sortBy('age', true);
```

Eager execution is straightforward: you see the result at every step, which makes it suitable for interactive exploration, debugging, and small datasets where overhead is negligible.

**When to use eager mode:**

- Exploratory data analysis in a REPL or notebook
- Small to medium datasets (thousands to low hundreds of thousands of rows)
- When you need to inspect intermediate results
- One-off scripts where readability matters more than performance

## Lazy Execution

Lazy mode defers all computation. Instead of executing operations immediately, FrameKit builds an internal **query plan** -- a tree of logical nodes that describes _what_ to compute, not _how_ or _when_. The plan is only executed when you explicitly call `.collect()` or `.sink()`.

```ts
import { DataFrame, col } from 'framekit-js';

const df = DataFrame.fromRows([
  { name: 'Alice', age: 30, dept: 'eng' },
  { name: 'Bob', age: 25, dept: 'eng' },
  { name: 'Carol', age: 35, dept: 'sales' },
]);

// Nothing executes yet -- this builds a plan
const plan = df
  .lazy()
  .filter(col<number>('age').gt(26))
  .select('name', 'age')
  .sort('age', true);

// The plan executes here
const result = await plan.collect();
```

The key advantage of lazy execution is that the optimizer can rewrite the plan before it runs, reducing the total amount of work.

**When to use lazy mode:**

- Multi-step ETL pipelines
- Large datasets where unnecessary columns or rows should be pruned early
- When you want the optimizer to rearrange your operations for better performance
- Streaming or scanning from disk (see below)

## The Query Optimizer

When you call `.collect()` or `.explain()`, FrameKit's optimizer applies three rewriting passes to the query plan:

### 1. Predicate Pushdown

Filters are pushed as close to the data source as possible. If you filter _after_ a sort or a select, the optimizer moves the filter _before_ those operations so that fewer rows flow through the pipeline.

```ts
const plan = df
  .lazy()
  .sort('age')
  .select('name', 'age')
  .filter(col<number>('age').gt(30));

// The optimizer rewrites this so the filter runs before the sort and select
```

Predicate pushdown is safe across `select`, `project`, and `sort` nodes. It is **not** pushed below `groupBy` nodes because that would change the semantics of the aggregation.

### 2. Filter Merging

When the plan contains consecutive filter nodes, the optimizer merges them into a single filter using a logical AND. This avoids scanning the data multiple times.

```ts
const plan = df
  .lazy()
  .filter(col<number>('age').gt(20))
  .filter(col<string>('dept').eq('eng'));

// Becomes a single filter: age > 20 AND dept = 'eng'
```

### 3. Projection Pruning

The optimizer tracks which columns each node actually needs and propagates that information downward. Columns that are never used by any downstream operation are dropped at the scan level, reducing memory and processing time.

```ts
const plan = df
  .lazy()
  .select('name', 'age')
  .filter(col<number>('age').gt(30));

// The scan node only reads 'name' and 'age', ignoring 'dept' entirely
```

## Inspecting Plans with `explain()`

The `explain()` method returns a string representation of both the original and optimized plans. This is invaluable for understanding what the optimizer is doing and for debugging performance issues.

```ts
import { DataFrame, col } from 'framekit-js';

const df = DataFrame.fromRows([
  { name: 'Alice', age: 30, dept: 'eng' },
  { name: 'Bob', age: 25, dept: 'eng' },
  { name: 'Carol', age: 35, dept: 'sales' },
]);

const plan = df
  .lazy()
  .sort('age')
  .select('name', 'age')
  .filter(col<number>('age').gt(30));

console.log(plan.explain());
```

Output (illustrative):

```
ORIGINAL:
FILTER [col(age) > 30]
  SELECT [name, age]
    SORT [age ASC]
      SCAN [id=0]

OPTIMIZED:
SELECT [name, age]
  SORT [age ASC]
    FILTER [col(age) > 30]
      SCAN [id=0, cols=age, name]
```

Notice how the filter moved below the sort, and the scan now only reads the two needed columns.

## `collect()` vs `sink()`

There are two ways to materialize a lazy plan:

### `collect(): Promise<DataFrame>`

Collects the full result into memory as a `DataFrame`. Use this when you need to continue working with the result in code.

```ts
const result = await df
  .lazy()
  .filter(col<number>('age').gt(25))
  .collect();

console.log(result.toArray());
```

### `sink(filePath: string): Promise<void>`

Materializes the result and writes it directly to a file. The format is determined by the file extension. Supported sink formats are `.csv`, `.tsv`, `.ndjson`, and `.jsonl`.

```ts
await df
  .lazy()
  .filter(col<number>('age').gt(25))
  .select('name', 'age')
  .sink('./output.csv');
```

## Streaming and Scanning

For files that are too large to fit in memory, FrameKit provides streaming iterators and scan-based lazy frames.

### `streamCSV` and `streamNDJSON`

These are async generators that yield `DataFrame` chunks. Each chunk contains at most `chunkSize` rows (default: 10,000). You process data one chunk at a time, keeping memory bounded.

```ts
for await (const chunk of DataFrame.streamCSV('./large-file.csv', { chunkSize: 5000 })) {
  // chunk is a DataFrame with up to 5000 rows
  const filtered = chunk.filter(col<number>('value').gt(100));
  console.log(filtered.shape);
}
```

### `scanCSV` and `scanNDJSON`

These return a `LazyFrame` backed by a streaming reader. When you call `.collect()`, the file is streamed in chunks and concatenated into a single `DataFrame`. This lets you combine lazy query planning with streaming I/O.

```ts
const result = await DataFrame.scanCSV<{ id: number; value: number }>('./large-file.csv')
  .filter(col<number>('value').gt(100))
  .select('id', 'value')
  .collect();
```

## Available LazyFrame Operations

The `LazyFrame` API supports the following operations:

| Method | Description |
|---|---|
| `filter(predicate)` | Filter rows by expression |
| `select(...columns)` | Select columns by name |
| `project(...exprs)` | Project using expressions |
| `sort(by, descending?)` | Sort by a column |
| `sortBy(by, descending?)` | Alias for `sort` |
| `limit(n)` / `head(n)` | Take the first N rows |
| `distinct(subset?)` / `unique(subset?)` | Remove duplicate rows |
| `groupBy(...keys).agg(...exprs)` | Group and aggregate |
| `explain()` | Print the query plan |
| `collect()` | Execute and return a DataFrame |
| `sink(filePath)` | Execute and write to file |

## Gotchas

1. **`collect()` returns a Promise.** Even though the current implementation is synchronous under the hood, the API is async to allow future streaming and parallel execution. Always `await` the result.

2. **Not all DataFrame operations are available on LazyFrame.** Methods like `withColumn`, `derive`, `join`, `pivot`, `melt`, and others are only available in eager mode. If you need them, call `.collect()` first, perform the operation, then optionally call `.lazy()` again.

3. **Filters are not pushed below `groupBy`.** This is intentional: filtering before a group-by changes which rows are included in the aggregation, which would silently alter the result.

4. **`sink()` only supports CSV, TSV, NDJSON, and JSONL.** If you need to write to Parquet, Excel, or Arrow, call `.collect()` and then use the appropriate `toParquet()`, `toExcel()`, or `toArrow()` method.

5. **`scanCSV` and `scanNDJSON` load all data into memory on `collect()`.** They stream the file to avoid a single large allocation, but the final `DataFrame` still resides in memory. For true out-of-core processing, use `streamCSV` / `streamNDJSON` and process chunks individually.

6. **Plan optimization is best-effort.** The optimizer applies a fixed set of rewriting rules. It does not perform cost-based optimization or reorder joins. For complex plans, inspect the output of `explain()` to verify that the optimizer is doing what you expect.
