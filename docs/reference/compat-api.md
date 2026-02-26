# Compat API Reference

The compat layer provides an Arquero-style verb interface for users familiar with tidyverse or Arquero patterns. All functions are stateless and return new DataFrames.

```ts
import { derive, rollup, fold, orderby, op, all, not, range, desc } from 'framekit-js/compat';
```

---

## Verbs

### `derive(df: DataFrame, exprs: Record<string, (row: any) => unknown>): DataFrame`

Add computed columns to a DataFrame. Each key in `exprs` is the new column name, and each value is a row-level function.

| Parameter | Type | Description |
|-----------|------|-------------|
| `df` | `DataFrame` | Input DataFrame |
| `exprs` | `Record<string, (row: any) => unknown>` | Map of column names to row-level compute functions |

**Returns:** `DataFrame` with the new columns appended.

```ts
// FrameKit compat
const result = derive(df, {
  total: (d) => d.price * d.qty,
  discounted: (d) => d.price * 0.9,
});
```

**Arquero equivalent:**

```ts
// Arquero
const result = dt.derive({
  total: (d) => d.price * d.qty,
  discounted: (d) => d.price * 0.9,
});
```

---

### `rollup(input: DataFrame | GroupBy, exprs: Record<string, (row: any) => unknown>): DataFrame`

Compute aggregate summaries. Works on both a plain DataFrame (whole-table aggregation) and a `GroupBy` object (per-group aggregation).

| Parameter | Type | Description |
|-----------|------|-------------|
| `input` | `DataFrame \| GroupBy` | Input DataFrame or GroupBy |
| `exprs` | `Record<string, (row: any) => unknown>` | Map of output column names to aggregate functions using `op` helpers |

**Returns:** `DataFrame` with one row per group (or one row total for ungrouped input).

```ts
// FrameKit compat - whole table
const summary = rollup(df, {
  totalRevenue: (d) => op.sum(d.revenue),
  avgPrice: (d) => op.mean(d.price),
});

// FrameKit compat - grouped
const perCity = rollup(df.groupBy('city'), {
  population: (d) => op.sum(d.pop),
  maxTemp: (d) => op.max(d.temp),
});
```

**Arquero equivalent:**

```ts
// Arquero - whole table
const summary = dt.rollup({
  totalRevenue: (d) => op.sum(d.revenue),
  avgPrice: (d) => op.mean(d.price),
});

// Arquero - grouped
const perCity = dt.groupby('city').rollup({
  population: (d) => op.sum(d.pop),
  maxTemp: (d) => op.max(d.temp),
});
```

---

### `fold(df: DataFrame, columns: string[], options?: { as?: [string, string] }): DataFrame`

Unpivot (melt) specified columns into key-value rows. Similar to `DataFrame.melt()` but follows the Arquero `fold` API.

| Parameter | Type | Description |
|-----------|------|-------------|
| `df` | `DataFrame` | Input DataFrame |
| `columns` | `string[]` | Column names to fold into rows |
| `options.as` | `[string, string]` | Names for the key and value columns (default `['key', 'value']`) |

**Returns:** `DataFrame` in long format.

```ts
// FrameKit compat
const long = fold(df, ['q1', 'q2', 'q3', 'q4'], { as: ['quarter', 'sales'] });
```

**Arquero equivalent:**

```ts
// Arquero
const long = dt.fold(['q1', 'q2', 'q3', 'q4'], { as: ['quarter', 'sales'] });
```

---

### `orderby(df: DataFrame, specs: SortSpec[]): DataFrame`

Sort a DataFrame by one or more columns. Each spec is either a column name string (ascending) or a `desc()` wrapper (descending).

| Parameter | Type | Description |
|-----------|------|-------------|
| `df` | `DataFrame` | Input DataFrame |
| `specs` | `SortSpec[]` | Array of column names or `desc()` wrapped names |

**`SortSpec`** is `string | DescSpec`.

**Returns:** Sorted `DataFrame`.

```ts
// FrameKit compat
const sorted = orderby(df, ['city', desc('population')]);
```

**Arquero equivalent:**

```ts
// Arquero
const sorted = dt.orderby('city', aq.desc('population'));
```

---

## Helpers

### `all(): AllSelector`

Select all columns. Typically used in verb contexts where you want to operate on every column.

```ts
derive(df, all(), (d) => d.value * 2);
```

### `not(...columns: string[]): NotSelector`

Select all columns except the specified ones.

```ts
not('id', 'timestamp');
```

### `range(start: string, end: string): RangeSelector`

Select a contiguous range of columns from `start` to `end` (inclusive), based on column order.

```ts
range('jan', 'dec');
```

### `desc(column: string): DescSpec`

Mark a column for descending sort order. Used with `orderby`.

```ts
orderby(df, [desc('score'), 'name']);
```

---

## Aggregation Operators (op)

Row-level aggregation helpers used inside `rollup` expressions. Each function receives an array-like column accessor and computes the aggregate over the group.

### `op.sum(values: any): number`

Sum of all values.

```ts
rollup(df, { total: (d) => op.sum(d.revenue) });
```

### `op.mean(values: any): number`

Arithmetic mean.

```ts
rollup(df, { avg: (d) => op.mean(d.score) });
```

### `op.count(values: any): number`

Count of non-null values.

```ts
rollup(df, { n: (d) => op.count(d.id) });
```

### `op.min(values: any): number`

Minimum value.

```ts
rollup(df, { lowest: (d) => op.min(d.price) });
```

### `op.max(values: any): number`

Maximum value.

```ts
rollup(df, { highest: (d) => op.max(d.price) });
```

### `op.distinct(values: any): number`

Count of distinct values.

```ts
rollup(df, { uniqueCities: (d) => op.distinct(d.city) });
```

### `op.first(values: any): any`

First value in the group.

```ts
rollup(df, { earliest: (d) => op.first(d.date) });
```

### `op.last(values: any): any`

Last value in the group.

```ts
rollup(df, { latest: (d) => op.last(d.date) });
```

---

## Full Example

A complete pipeline using compat verbs alongside standard DataFrame methods:

```ts
import { DataFrame } from 'framekit-js';
import { derive, rollup, orderby, op, desc } from 'framekit-js/compat';

const sales = DataFrame.fromRows([
  { store: 'A', product: 'X', price: 10, qty: 5 },
  { store: 'A', product: 'Y', price: 20, qty: 3 },
  { store: 'B', product: 'X', price: 10, qty: 8 },
  { store: 'B', product: 'Y', price: 20, qty: 1 },
]);

// Step 1: compute revenue per row
const withRevenue = derive(sales, {
  revenue: (d) => d.price * d.qty,
});

// Step 2: aggregate by store
const byStore = rollup(withRevenue.groupBy('store'), {
  totalRevenue: (d) => op.sum(d.revenue),
  itemsSold: (d) => op.sum(d.qty),
});

// Step 3: sort by revenue descending
const ranked = orderby(byStore, [desc('totalRevenue')]);

ranked.print();
// store  totalRevenue  itemsSold
// B      100           9
// A      110           8
```
