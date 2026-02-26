# Window Functions

Window functions compute values across a set of related rows without collapsing them into a single output row. Unlike `groupBy().agg()`, which produces one row per group, window functions add a new column while preserving every row of the original DataFrame.

FrameKit provides window functions as chainable methods on expressions. They fall into four categories: ranking, cumulative, offset, and rolling.

## Concepts

A window function operates on a **frame** of rows. The frame can be:

- **The entire DataFrame** -- the function sees all rows.
- **A partition** -- the function sees only rows that share the same value(s) in one or more grouping columns, defined with `.over()`.
- **An ordered frame** -- the function processes rows in a specific order, defined with `.orderBy()`.

These modifiers can be combined: `.over('dept').orderBy('date')` means "within each department, ordered by date."

### Comparison to SQL

FrameKit's window functions map directly to SQL's `OVER()` clause:

| SQL | FrameKit |
|---|---|
| `RANK() OVER (ORDER BY salary DESC)` | `col('salary').rank().orderBy('salary', 'desc')` |
| `SUM(amount) OVER (PARTITION BY dept)` | `col('amount').cumSum().over('dept')` |
| `ROW_NUMBER() OVER (PARTITION BY dept ORDER BY hire_date)` | `col('hire_date').rowNumber().over('dept').orderBy('hire_date')` |
| `LAG(price, 1) OVER (ORDER BY date)` | `col('price').shift(1).orderBy('date')` |

## Ranking Functions

Ranking functions assign a position to each row based on the values in a column. All ranking functions are 1-based.

### `rank()`

Standard competition ranking. Tied values receive the same rank, and the next rank after a tie skips ahead. For example, if two rows tie at rank 2, the next row gets rank 4.

### `denseRank()`

Like `rank()`, but without gaps. If two rows tie at rank 2, the next row gets rank 3.

### `rowNumber()`

Assigns a unique sequential number to each row. Ties are broken arbitrarily (by original row order).

### `percentRank()`

Returns `(rank - 1) / (n - 1)`, where `n` is the total number of rows. The result ranges from 0.0 to 1.0. A single-row result returns 0.

### `ntile(n)`

Distributes rows into `n` roughly equal buckets, numbered 1 through `n`.

```ts
import { DataFrame, col } from 'framekit-js';

const df = DataFrame.fromRows([
  { name: 'Alice', salary: 120000 },
  { name: 'Bob',   salary: 95000 },
  { name: 'Carol', salary: 120000 },
  { name: 'Dave',  salary: 80000 },
  { name: 'Eve',   salary: 150000 },
]);

const ranked = df
  .withColumn('rank',         col('salary').rank())
  .withColumn('dense_rank',   col('salary').denseRank())
  .withColumn('row_num',      col('salary').rowNumber())
  .withColumn('pct_rank',     col('salary').percentRank())
  .withColumn('quartile',     col('salary').ntile(4));

// Results (sorted by salary for readability):
// Dave:  salary=80000,  rank=1, dense_rank=1, row_num=1, pct_rank=0.00, quartile=1
// Bob:   salary=95000,  rank=2, dense_rank=2, row_num=2, pct_rank=0.25, quartile=1
// Alice: salary=120000, rank=3, dense_rank=3, row_num=3, pct_rank=0.50, quartile=2
// Carol: salary=120000, rank=3, dense_rank=3, row_num=4, pct_rank=0.50, quartile=3
// Eve:   salary=150000, rank=5, dense_rank=4, row_num=5, pct_rank=1.00, quartile=4
```

## Cumulative Functions

Cumulative functions compute a running aggregate over all rows seen so far, respecting the current row order.

### `cumSum()`

Running total. Null values are skipped (the sum continues from the last non-null value).

### `cumMax()` and `cumMin()`

Running maximum and minimum. Returns `null` until the first non-null value is encountered.

### `cumProd()`

Running product, starting at 1.

### `cumCount()`

Running count of non-null values.

```ts
import { DataFrame, col } from 'framekit-js';

const df = DataFrame.fromRows([
  { month: 'Jan', revenue: 1000 },
  { month: 'Feb', revenue: 1500 },
  { month: 'Mar', revenue: null },
  { month: 'Apr', revenue: 2000 },
]);

const cumulative = df
  .withColumn('ytd_revenue', col('revenue').cumSum())
  .withColumn('best_month',  col('revenue').cumMax())
  .withColumn('months_seen', col('revenue').cumCount());

// Jan: ytd=1000, best=1000, months_seen=1
// Feb: ytd=2500, best=1500, months_seen=2
// Mar: ytd=2500, best=1500, months_seen=2  (null skipped)
// Apr: ytd=4500, best=2000, months_seen=3
```

## Offset Functions

Offset functions access values from other rows relative to the current row.

### `shift(n)`

Shifts values by `n` positions. A positive `n` looks backward (like SQL `LAG`), a negative `n` looks forward (like SQL `LEAD`). Positions that fall outside the DataFrame are filled with `null`.

### `diff(n?)`

Computes the difference between the current value and the value `n` rows back. Default offset is 1. Returns `null` for the first `n` rows and when either value is null.

### `pctChange(n?)`

Computes the percentage change from `n` rows back: `(current - previous) / previous`. Default offset is 1. Returns `null` when the previous value is null or zero.

```ts
import { DataFrame, col } from 'framekit-js';

const df = DataFrame.fromRows([
  { date: '2024-01-01', price: 100 },
  { date: '2024-01-02', price: 105 },
  { date: '2024-01-03', price: 102 },
  { date: '2024-01-04', price: 110 },
]);

const analysis = df
  .withColumn('prev_price', col('price').shift(1))
  .withColumn('daily_change', col('price').diff())
  .withColumn('daily_return', col('price').pctChange());

// 2024-01-01: prev=null,  change=null,  return=null
// 2024-01-02: prev=100,   change=5,     return=0.05
// 2024-01-03: prev=105,   change=-3,    return=-0.0286
// 2024-01-04: prev=102,   change=8,     return=0.0784
```

## Rolling Functions

Rolling functions compute an aggregate over a sliding window of `n` consecutive rows. The first `n - 1` rows return `null` because the window is not yet full.

### `rollingMean(n)`

Moving average over the last `n` rows. Null values within the window are excluded from the calculation (the mean is computed over the non-null values only).

### `rollingSum(n)`

Moving sum over the last `n` rows. Nulls are treated as zero.

### `rollingStd(n)`

Moving sample standard deviation (ddof=1, matching pandas behavior). Requires at least 2 non-null values in the window; returns `null` otherwise.

### `rollingMin(n)` and `rollingMax(n)`

Moving minimum and maximum over the last `n` rows.

```ts
import { DataFrame, col } from 'framekit-js';

const df = DataFrame.fromRows([
  { day: 1, temp: 22 },
  { day: 2, temp: 25 },
  { day: 3, temp: 21 },
  { day: 4, temp: 28 },
  { day: 5, temp: 24 },
  { day: 6, temp: 30 },
  { day: 7, temp: 27 },
]);

const smoothed = df
  .withColumn('avg_3d',  col('temp').rollingMean(3))
  .withColumn('high_3d', col('temp').rollingMax(3))
  .withColumn('low_3d',  col('temp').rollingMin(3));

// day 1: avg_3d=null,  high_3d=null,  low_3d=null   (window not full)
// day 2: avg_3d=null,  high_3d=null,  low_3d=null   (window not full)
// day 3: avg_3d=22.67, high_3d=25,    low_3d=21
// day 4: avg_3d=24.67, high_3d=28,    low_3d=21
// day 5: avg_3d=24.33, high_3d=28,    low_3d=21
// day 6: avg_3d=27.33, high_3d=30,    low_3d=24
// day 7: avg_3d=27.00, high_3d=30,    low_3d=24
```

## Exponential Weighted Moving Average

### `ewm(alpha)`

Computes an exponentially weighted moving average with smoothing factor `alpha` (between 0 and 1). Higher alpha values give more weight to recent observations.

The formula is: `EWMA_t = alpha * value_t + (1 - alpha) * EWMA_{t-1}`. The first non-null value initializes the EWMA. Null values are skipped (the EWMA carries forward unchanged).

```ts
import { DataFrame, col } from 'framekit-js';

const df = DataFrame.fromRows([
  { day: 1, signal: 10 },
  { day: 2, signal: 12 },
  { day: 3, signal: 11 },
  { day: 4, signal: 15 },
  { day: 5, signal: 13 },
]);

const smoothed = df.withColumn('ewma', col('signal').ewm(0.3));

// day 1: ewma = 10.000  (first value initializes)
// day 2: ewma = 10.600  (0.3 * 12 + 0.7 * 10)
// day 3: ewma = 10.720  (0.3 * 11 + 0.7 * 10.6)
// day 4: ewma = 12.004  (0.3 * 15 + 0.7 * 10.72)
// day 5: ewma = 12.303  (0.3 * 13 + 0.7 * 12.004)
```

## Partitioning with `over()`

The `.over(...columns)` modifier partitions the data by one or more columns before applying the window function. Each partition is processed independently, as if it were a separate DataFrame.

```ts
import { DataFrame, col } from 'framekit-js';

const df = DataFrame.fromRows([
  { dept: 'eng',   name: 'Alice', salary: 120000 },
  { dept: 'eng',   name: 'Bob',   salary: 95000 },
  { dept: 'eng',   name: 'Carol', salary: 110000 },
  { dept: 'sales', name: 'Dave',  salary: 80000 },
  { dept: 'sales', name: 'Eve',   salary: 90000 },
]);

const withRank = df.withColumn(
  'dept_rank',
  col('salary').rank().over('dept'),
);

// eng:   Alice=3, Bob=1, Carol=2  (ranked within eng)
// sales: Dave=1, Eve=2            (ranked within sales)
```

You can partition by multiple columns:

```ts
const result = df.withColumn(
  'team_cumsum',
  col('amount').cumSum().over('dept', 'region'),
);
```

## Ordering with `orderBy()`

The `.orderBy(column, direction?)` modifier sorts the data before applying the window function. This is essential for functions where row order matters, such as `cumSum`, `shift`, `diff`, and ranking.

The `direction` parameter accepts `'asc'` (default) or `'desc'`.

```ts
import { DataFrame, col } from 'framekit-js';

const df = DataFrame.fromRows([
  { dept: 'eng', name: 'Alice', hire_date: '2020-01-15' },
  { dept: 'eng', name: 'Bob',   hire_date: '2019-06-01' },
  { dept: 'eng', name: 'Carol', hire_date: '2021-03-10' },
]);

const withSeniority = df.withColumn(
  'seniority_rank',
  col('hire_date').rowNumber().orderBy('hire_date', 'asc'),
);

// Bob=1 (earliest hire), Alice=2, Carol=3
```

### Combining `over()` and `orderBy()`

When both modifiers are used, the data is first partitioned by `over()`, then each partition is sorted by `orderBy()`.

```ts
import { DataFrame, col } from 'framekit-js';

const df = DataFrame.fromRows([
  { dept: 'eng',   quarter: 'Q1', revenue: 100 },
  { dept: 'eng',   quarter: 'Q2', revenue: 150 },
  { dept: 'eng',   quarter: 'Q3', revenue: 130 },
  { dept: 'sales', quarter: 'Q1', revenue: 200 },
  { dept: 'sales', quarter: 'Q2', revenue: 180 },
  { dept: 'sales', quarter: 'Q3', revenue: 220 },
]);

const withYTD = df.withColumn(
  'ytd_revenue',
  col('revenue').cumSum().over('dept').orderBy('quarter'),
);

// eng:   Q1=100, Q2=250, Q3=380
// sales: Q1=200, Q2=380, Q3=600
```

## Real-World Example: Sales Analytics Dashboard

This example demonstrates combining multiple window functions to build a comprehensive analytics view.

```ts
import { DataFrame, col } from 'framekit-js';

const sales = DataFrame.fromRows([
  { store: 'NYC', date: '2024-01-01', amount: 500 },
  { store: 'NYC', date: '2024-01-02', amount: 600 },
  { store: 'NYC', date: '2024-01-03', amount: 550 },
  { store: 'NYC', date: '2024-01-04', amount: 700 },
  { store: 'LA',  date: '2024-01-01', amount: 400 },
  { store: 'LA',  date: '2024-01-02', amount: 450 },
  { store: 'LA',  date: '2024-01-03', amount: 420 },
  { store: 'LA',  date: '2024-01-04', amount: 500 },
]);

const dashboard = sales
  .withColumn('daily_rank',    col('amount').rank().over('store'))
  .withColumn('running_total', col('amount').cumSum().over('store').orderBy('date'))
  .withColumn('prev_day',      col('amount').shift(1).over('store').orderBy('date'))
  .withColumn('day_over_day',  col('amount').pctChange().over('store').orderBy('date'))
  .withColumn('moving_avg_3d', col('amount').rollingMean(3).over('store').orderBy('date'));
```

## Gotchas

1. **Ranking functions sort by the source column's values, not by row position.** Calling `col('salary').rank()` ranks by salary value. If you want to rank by a different column, call `rank()` on that column instead.

2. **`orderBy()` applies to ranking expressions by setting their sort direction.** For ranking functions, `orderBy('col', 'desc')` is equivalent to asking for a descending rank. For sequential functions (cumSum, shift, rolling), `orderBy()` physically reorders the rows before computation.

3. **Rolling functions return `null` for the first `n - 1` rows.** The window must be fully populated before a value is produced. There is no "partial window" mode.

4. **`rollingStd` uses sample standard deviation (ddof=1).** This matches pandas' default behavior. If the window contains fewer than 2 non-null values, the result is `null`.

5. **`shift(1)` is lag, `shift(-1)` is lead.** A positive offset looks backward in the data; a negative offset looks forward.

6. **Null handling in cumulative functions.** Cumulative functions skip nulls. `cumSum` does not reset to zero when it encounters a null -- it continues accumulating from the last known sum. `cumCount` only increments for non-null values.

7. **`over()` creates independent partitions.** Each partition is processed as if it were a completely separate DataFrame. Window sizes, cumulative sums, and ranks all reset at partition boundaries.

8. **Nulls sort last in ranking functions.** When ranking or ordering, null values are placed after all non-null values regardless of sort direction.

9. **`ewm` carries forward on nulls.** When a null value is encountered, the EWMA is not updated -- it simply repeats the previous EWMA value. This is different from skipping the row entirely.
