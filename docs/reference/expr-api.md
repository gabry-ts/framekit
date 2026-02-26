# Expression API Reference

Expressions are composable, type-safe building blocks used in `filter`, `withColumn`, `groupBy().agg()`, and other DataFrame operations. They describe computations without executing them immediately, enabling optimization and type inference.

```ts
import { col, lit, when } from 'framekit-js';
```

---

## Factories

### `col<T>(name: string): Expr<T>`

Create an expression that references a column by name. The generic parameter `T` represents the column's value type.

```ts
const age = col<number>('age');
const name = col<string>('name');
```

### `lit<T>(value: T): Expr<T>`

Create an expression representing a literal constant value.

```ts
const threshold = lit(100);
const label = lit('active');
```

---

## Arithmetic

All arithmetic methods accept `Expr<number> | number` and return `Expr<number>`.

### `add(other: Expr<number> | number): Expr<number>`

Addition.

### `sub(other: Expr<number> | number): Expr<number>`

Subtraction.

### `mul(other: Expr<number> | number): Expr<number>`

Multiplication.

### `div(other: Expr<number> | number): Expr<number>`

Division.

### `mod(other: Expr<number> | number): Expr<number>`

Modulo (remainder).

### `pow(other: Expr<number> | number): Expr<number>`

Exponentiation.

```ts
// revenue = price * quantity
col('price').mul(col('quantity'));

// percentage = (value / total) * 100
col('value').div(col('total')).mul(100);

// area = side.pow(2)
col('side').pow(2);
```

---

## Comparison

All comparison methods accept `Expr<T> | T` and return `Expr<boolean>`.

### `eq(other: Expr<T> | T): Expr<boolean>`

Equal to.

### `neq(other: Expr<T> | T): Expr<boolean>`

Not equal to.

### `gt(other: Expr<T> | T): Expr<boolean>`

Greater than.

### `gte(other: Expr<T> | T): Expr<boolean>`

Greater than or equal to.

### `lt(other: Expr<T> | T): Expr<boolean>`

Less than.

### `lte(other: Expr<T> | T): Expr<boolean>`

Less than or equal to.

```ts
col('age').gte(18);
col('status').eq('active');
col('score').gt(col('threshold'));
```

---

## Logical

### `and(other: Expr<boolean> | boolean): Expr<boolean>`

Logical AND.

### `or(other: Expr<boolean> | boolean): Expr<boolean>`

Logical OR.

### `not(): Expr<boolean>`

Logical NOT (negation).

```ts
col('age').gte(18).and(col('verified').eq(true));
col('banned').not();
col('premium').or(col('trial'));
```

---

## Null Handling

### `coalesce(...others: Array<Expr<T> | T>): Expr<T>`

Return the first non-null value among this expression and the provided alternatives.

```ts
col('nickname').coalesce(col('firstName'), lit('Anonymous'));
```

### `fillNull(value: Expr<T> | T): Expr<T>`

Replace null values with the given value.

```ts
col('score').fillNull(0);
```

### `isNull(): Expr<boolean>`

Return `true` where the value is null.

```ts
df.filter(col('email').isNull());
```

### `isNotNull(): Expr<boolean>`

Return `true` where the value is not null.

```ts
df.filter(col('email').isNotNull());
```

---

## Conditional

Build if/else chains using `when`, `then`, and `otherwise`.

### `when(condition: Expr<boolean>): WhenBuilder`

Start a conditional expression.

### `WhenBuilder.then<T>(value: Expr<T> | T): ThenBuilder<T>`

Specify the value to return when the condition is true.

### `ThenBuilder.when(condition: Expr<boolean>): ChainedWhenBuilder`

Chain an additional condition (else-if).

### `ThenBuilder.otherwise(value: Expr<T> | T): Expr<T>`

Specify the fallback value when no conditions match.

```ts
// Simple if/else
when(col('age').gte(18))
  .then('adult')
  .otherwise('minor');

// Chained conditions
when(col('score').gte(90))
  .then('A')
  .when(col('score').gte(80))
  .then('B')
  .when(col('score').gte(70))
  .then('C')
  .otherwise('F');
```

---

## Aggregations

Aggregation expressions are used inside `groupBy().agg()`. Each returns a specific `AggExpr` subtype.

### `sum(): SumAggExpr`

Sum of values in the group.

### `mean(): MeanAggExpr`

Arithmetic mean of values in the group.

### `count(): CountAggExpr`

Count of non-null values in the group.

### `countDistinct(): CountDistinctAggExpr`

Count of distinct values in the group.

### `min(): MinAggExpr`

Minimum value in the group.

### `max(): MaxAggExpr`

Maximum value in the group.

### `std(): StdAggExpr`

Standard deviation of values in the group.

### `first(): FirstAggExpr`

First value in the group.

### `last(): LastAggExpr`

Last value in the group.

### `list(): ListAggExpr`

Collect all group values into an array.

### `mode(): ModeAggExpr`

Most frequent value in the group.

### `op.corr(colA: Expr<number>, colB: Expr<number>): AggExpr`

Pearson correlation coefficient between two columns across the group.

```ts
import { col, op } from 'framekit-js';

df.groupBy('department').agg({
  totalSalary: col('salary').sum(),
  avgSalary: col('salary').mean(),
  headCount: col('id').count(),
  uniqueRoles: col('role').countDistinct(),
  salarySpread: col('salary').std(),
  allNames: col('name').list(),
  topRole: col('role').mode(),
});

// Cross-column correlation
df.groupBy().agg({
  corr: op.corr(col('x'), col('y')),
});
```

---

## String Accessor

Access string operations via the `.str` accessor on a string-typed expression. All methods return `Expr<string>` unless noted.

### `col('x').str.toLowerCase(): Expr<string>`

Convert to lowercase.

### `col('x').str.toUpperCase(): Expr<string>`

Convert to uppercase.

### `col('x').str.trim(): Expr<string>`

Remove leading and trailing whitespace.

### `col('x').str.contains(pattern: string): Expr<boolean>`

Return `true` if the string contains the pattern.

### `col('x').str.startsWith(prefix: string): Expr<boolean>`

Return `true` if the string starts with the prefix.

### `col('x').str.endsWith(suffix: string): Expr<boolean>`

Return `true` if the string ends with the suffix.

### `col('x').str.replace(pattern: string, replacement: string): Expr<string>`

Replace the first occurrence of `pattern` with `replacement`.

### `col('x').str.slice(start: number, end?: number): Expr<string>`

Extract a substring from `start` to `end`.

### `col('x').str.length(): Expr<number>`

Return the length of the string.

### `col('x').str.concat(...others: Array<Expr<string> | string>): Expr<string>`

Concatenate with other strings or string expressions.

```ts
df.withColumn('lower', col('name').str.toLowerCase());
df.filter(col('email').str.contains('@company.com'));
df.withColumn('initials',
  col('first').str.slice(0, 1).str.concat(col('last').str.slice(0, 1))
);
```

---

## Date Accessor

Access date/time operations via the `.dt` accessor on a date-typed expression. Extraction methods return `Expr<number>`.

### `col('x').dt.year(): Expr<number>`

Extract the year component.

### `col('x').dt.month(): Expr<number>`

Extract the month (1--12).

### `col('x').dt.day(): Expr<number>`

Extract the day of the month (1--31).

### `col('x').dt.hour(): Expr<number>`

Extract the hour (0--23).

### `col('x').dt.minute(): Expr<number>`

Extract the minute (0--59).

### `col('x').dt.second(): Expr<number>`

Extract the second (0--59).

### `col('x').dt.dayOfWeek(): Expr<number>`

Day of the week (0 = Sunday, 6 = Saturday).

### `col('x').dt.dayOfYear(): Expr<number>`

Day of the year (1--366).

### `col('x').dt.weekNumber(): Expr<number>`

ISO week number.

### `col('x').dt.quarter(): Expr<number>`

Quarter of the year (1--4).

### `col('x').dt.timestamp(): Expr<number>`

Unix timestamp in milliseconds.

### `col('x').dt.truncate(unit: 'year' | 'month' | 'week' | 'day' | 'hour' | 'minute'): Expr<Date>`

Truncate the date to the given unit.

### `col('x').dt.diff(other: Expr<Date>, unit: 'days' | 'hours' | 'minutes' | 'seconds' | 'milliseconds'): Expr<number>`

Compute the difference between two dates in the specified unit.

```ts
df.withColumn('year', col('createdAt').dt.year());
df.withColumn('quarter', col('date').dt.quarter());
df.withColumn('daysSince',
  col('endDate').dt.diff(col('startDate'), 'days')
);
df.withColumn('monthStart', col('date').dt.truncate('month'));
```

---

## Window Functions

Window functions compute values relative to a set of rows. Use `.over()` to define the partition and `.orderBy()` to define the ordering.

### Ranking

#### `rank(): Expr<number>`

Rank with gaps (1, 2, 2, 4).

#### `denseRank(): Expr<number>`

Rank without gaps (1, 2, 2, 3).

#### `rowNumber(): Expr<number>`

Sequential row number starting at 1.

#### `percentRank(): Expr<number>`

Percent rank in the range [0, 1].

#### `ntile(n: number): Expr<number>`

Distribute rows into `n` approximately equal buckets (1 through `n`).

```ts
df.withColumn('rank',
  col('score').rank().over('department').orderBy('score', 'desc')
);
df.withColumn('quartile', col('salary').ntile(4).over('department'));
```

### Cumulative

#### `cumSum(): Expr<number>`

Running sum.

#### `cumMax(): Expr<number>`

Running maximum.

#### `cumMin(): Expr<number>`

Running minimum.

#### `cumProd(): Expr<number>`

Running product.

#### `cumCount(): Expr<number>`

Running count of non-null values.

```ts
df.withColumn('runningTotal', col('revenue').cumSum().over('region'));
```

### Offset and Difference

#### `shift(offset: number): Expr<T>`

Shift values by `offset` positions. Positive shifts down, negative shifts up. Missing positions are filled with null.

#### `diff(offset?: number): Expr<number>`

Difference between the current value and the value `offset` rows before (default `1`).

#### `pctChange(offset?: number): Expr<number>`

Percentage change between the current value and the value `offset` rows before (default `1`).

```ts
df.withColumn('prevSales', col('sales').shift(1));
df.withColumn('salesDelta', col('sales').diff());
df.withColumn('growthPct', col('revenue').pctChange());
```

### Rolling

#### `rollingMean(windowSize: number): Expr<number>`

Rolling (moving) average over a window of `windowSize` rows.

#### `rollingSum(windowSize: number): Expr<number>`

Rolling sum.

#### `rollingStd(windowSize: number): Expr<number>`

Rolling standard deviation.

#### `rollingMin(windowSize: number): Expr<number>`

Rolling minimum.

#### `rollingMax(windowSize: number): Expr<number>`

Rolling maximum.

```ts
df.withColumn('ma7', col('price').rollingMean(7));
df.withColumn('volatility', col('returns').rollingStd(30));
```

### Exponential Weighting

#### `ewm(alpha: number): Expr<number>`

Exponentially weighted moving average with smoothing factor `alpha` (between 0 and 1).

```ts
df.withColumn('ema', col('price').ewm(0.3));
```

### Partitioning and Ordering

#### `over(...partitionCols: string[]): Expr<T>`

Define the partition columns for a window function. Rows with the same values in the partition columns form a window.

#### `orderBy(column: string, direction?: 'asc' | 'desc'): Expr<T>`

Define the ordering within each window partition.

```ts
df.withColumn('deptRank',
  col('salary').rank().over('department').orderBy('salary', 'desc')
);
```

---

## Naming

### `as(name: string): NamedExpr<T>`

Assign an output name to an expression. Used when the expression result needs a specific column name.

```ts
col('price').mul(col('qty')).as('revenue');
```
