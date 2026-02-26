# Null Handling

FrameKit treats `null` as the canonical representation of missing data. This guide explains how nulls propagate through expressions, how aggregations handle them, and what tools are available for cleaning and imputing missing values.

## Null Philosophy

In FrameKit, `null` means "this value is absent." It is not the same as `0`, `""`, `false`, or `undefined`. The library enforces explicit null semantics:

- Nulls propagate through arithmetic and comparison expressions.
- Aggregations skip null values by default.
- You must explicitly choose how to handle nulls -- FrameKit will not silently drop or fill them.

This approach prevents a common class of bugs where missing data is silently converted to a zero or empty string, producing incorrect results without any warning.

## Null Propagation in Expressions

### Arithmetic

Any arithmetic operation involving a null operand produces null. This is consistent with SQL's three-valued logic and with IEEE semantics for "unknown" values.

```ts
import { DataFrame, col } from 'framekit-js';

const df = DataFrame.fromRows([
  { a: 10, b: 5 },
  { a: null, b: 3 },
  { a: 7, b: null },
]);

const result = df.withColumn('sum', col<number>('a').add(col<number>('b')));
// Row 0: sum = 15
// Row 1: sum = null  (null + 3 = null)
// Row 2: sum = null  (7 + null = null)
```

This applies to all arithmetic operators: `add`, `sub`, `mul`, `div`, `mod`, and `pow`.

### Comparisons

Comparing a null to any value (including another null) produces null, not `true` or `false`. This means that a filter expression involving null will exclude rows where the compared column is null.

```ts
const df = DataFrame.fromRows([
  { score: 90 },
  { score: null },
  { score: 70 },
]);

const passing = df.filter(col<number>('score').gt(75));
// Only row 0 (score=90) passes.
// Row 1 is excluded because null > 75 evaluates to null, which is falsy.
```

This is an important distinction from JavaScript's native behavior, where `null > 75` evaluates to `false`. FrameKit's approach is safer because it makes the absence of data explicit rather than silently treating it as a valid comparison.

### Logical Operators

The `and`, `or`, and `not` operators follow SQL-style three-valued logic:

- `null AND true` = `null`
- `null AND false` = `false`
- `null OR true` = `true`
- `null OR false` = `null`
- `NOT null` = `null`

## Null Handling in Aggregations

Aggregation functions skip null values by default. This means they compute their result over the non-null values only.

```ts
import { DataFrame, col } from 'framekit-js';

const df = DataFrame.fromRows([
  { group: 'A', value: 10 },
  { group: 'A', value: null },
  { group: 'A', value: 30 },
  { group: 'B', value: null },
]);

const result = df.groupBy('group').agg(
  col('value').sum().as('total'),
  col('value').mean().as('avg'),
  col('value').count().as('n'),
);

// group A: total = 40, avg = 20, n = 2
// group B: total = 0, avg = null (no non-null values), n = 0
```

The following aggregations skip nulls: `sum`, `mean`, `min`, `max`, `std`, `count`, `countDistinct`, `mode`, `first`, `last`, and `op.corr`.

Note that `count` counts non-null values. If you need the total number of rows regardless of nulls, use `count` on a column that has no nulls, or derive a literal column.

## Filtering Nulls with `dropNull()`

The `dropNull()` method removes rows that contain null values. You can target specific columns or apply it to all columns.

```ts
import { DataFrame } from 'framekit-js';

const df = DataFrame.fromRows([
  { id: 1, name: 'Alice', score: 90 },
  { id: 2, name: null,    score: 85 },
  { id: 3, name: 'Carol', score: null },
  { id: 4, name: 'Dave',  score: 75 },
]);

// Drop rows where ANY column is null
const clean = df.dropNull();
// Rows 1 and 4 remain (id=1, id=4)

// Drop rows where 'score' is null specifically
const hasScore = df.dropNull('score');
// Rows 1, 2, and 4 remain

// Drop rows where 'name' OR 'score' is null
const complete = df.dropNull(['name', 'score']);
// Rows 1 and 4 remain
```

## Filling Nulls with `fillNull()`

The `fillNull()` method replaces null values. It supports three strategies:

### Strategy 1: Fill with specific values per column

Pass an object mapping column names to replacement values.

```ts
const df = DataFrame.fromRows([
  { name: 'Alice', age: 30, dept: null },
  { name: null,    age: null, dept: 'eng' },
]);

const filled = df.fillNull({ name: 'Unknown', age: 0, dept: 'unassigned' });
// Row 0: { name: 'Alice', age: 30, dept: 'unassigned' }
// Row 1: { name: 'Unknown', age: 0, dept: 'eng' }
```

### Strategy 2: Forward fill

Replace each null with the last non-null value that appeared before it in the same column. This is useful for time series data where a value should "carry forward" until it changes.

```ts
const df = DataFrame.fromRows([
  { date: '2024-01-01', price: 100 },
  { date: '2024-01-02', price: null },
  { date: '2024-01-03', price: null },
  { date: '2024-01-04', price: 110 },
]);

const filled = df.fillNull('forward');
// Row 1: price = 100 (carried from row 0)
// Row 2: price = 100 (carried from row 0)
// Row 3: price = 110
```

### Strategy 3: Backward fill

Replace each null with the next non-null value that appears after it. This is the reverse of forward fill.

```ts
const filled = df.fillNull('backward');
// Row 0: price = 100
// Row 1: price = 110 (taken from row 3)
// Row 2: price = 110 (taken from row 3)
// Row 3: price = 110
```

## Advanced Imputation with `impute()`

The `impute()` method provides more flexible null replacement. It takes a mapping of column names to fill values, where the fill value can be either a literal or a function that receives the column data and computes a replacement.

### Simple imputation with a constant

```ts
import { DataFrame } from 'framekit-js';

const df = DataFrame.fromRows([
  { id: 1, score: 90 },
  { id: 2, score: null },
  { id: 3, score: 85 },
]);

const imputed = df.impute({ score: 0 });
// Row 1: score = 0
```

### Imputation with a computed value

Pass a function to compute the fill value from the existing data. The function receives an object mapping column names to arrays of all values (including nulls).

```ts
const imputed = df.impute({
  score: (data) => {
    const values = data['score'].filter((v): v is number => v !== null);
    return values.reduce((a, b) => a + b, 0) / values.length; // mean
  },
});
// Row 1: score = 87.5 (mean of 90 and 85)
```

### Imputation with expansion

The `expand` option generates the Cartesian product of the specified key columns, filling in missing combinations with null (which are then imputed). This is useful for panel data where you expect every combination of keys to exist.

```ts
const df = DataFrame.fromRows([
  { region: 'North', year: 2023, sales: 100 },
  { region: 'South', year: 2024, sales: 200 },
]);

const expanded = df.impute({ sales: 0 }, { expand: ['region', 'year'] });
// Creates all 4 combinations (North/South x 2023/2024)
// Missing combinations get sales = 0
```

## Null Expressions: `coalesce`, `fillNull`, `isNull`, `isNotNull`

These expression-level functions let you handle nulls inside column derivations and projections.

### `coalesce(...alternatives)`

Returns the first non-null value from a list of expressions. This is equivalent to SQL's `COALESCE` function.

```ts
import { DataFrame, col, lit } from 'framekit-js';

const df = DataFrame.fromRows([
  { primary: null, backup: 'fallback-A' },
  { primary: 'value-B', backup: 'fallback-B' },
  { primary: null, backup: null },
]);

const result = df.withColumn(
  'resolved',
  col<string>('primary').coalesce(col<string>('backup'), lit('default')),
);
// Row 0: resolved = 'fallback-A'
// Row 1: resolved = 'value-B'
// Row 2: resolved = 'default'
```

### `fillNull(value)`

Expression-level `fillNull` replaces nulls in a single expression with a specified value or expression. It is equivalent to `coalesce` with a single fallback.

```ts
const result = df.withColumn(
  'safe_score',
  col<number>('score').fillNull(0),
);
```

### `isNull()` and `isNotNull()`

These return boolean expressions that test whether a value is null.

```ts
const df = DataFrame.fromRows([
  { name: 'Alice', email: 'alice@example.com' },
  { name: 'Bob',   email: null },
]);

// Filter to rows where email is missing
const noEmail = df.filter(col('email').isNull());

// Add a boolean column indicating completeness
const withFlag = df.withColumn('has_email', col('email').isNotNull());
```

## Gotchas and Edge Cases

1. **`null` is not `undefined`.** FrameKit normalizes `undefined` values to `null` during DataFrame construction. If your source data contains `undefined`, it will be treated as `null` internally.

2. **Forward fill does not fill leading nulls.** If the first value in a column is null, `fillNull('forward')` cannot fill it because there is no preceding value. The null remains. Use `fillNull('backward')` or `impute()` if you need to handle leading nulls.

3. **Backward fill does not fill trailing nulls.** Symmetrically, if the last value is null, backward fill cannot resolve it.

4. **`count` skips nulls.** If you call `col('x').count()` in an aggregation, it counts only non-null values. This differs from some libraries where `count()` counts all rows. To count all rows in a group, use `count` on a non-nullable column (such as a primary key).

5. **Aggregation of an all-null group.** When every value in a group is null, aggregations return edge-case values: `sum` returns `0`, `mean` returns `null`, `min` and `max` return `null`, and `count` returns `0`.

6. **Null equality.** The expression `col('x').eq(null)` evaluates to `null` for every row (not `true` for null rows). Use `col('x').isNull()` to test for null values.

7. **`impute()` preserves non-null values.** The impute function only replaces values that are strictly `null`. Existing non-null values, even if they are `0` or `""`, are left unchanged.

8. **`dropNull()` with no arguments checks all columns.** If any column in a row contains null, the entire row is removed. This can be aggressive on wide DataFrames. Prefer specifying the columns you care about.
