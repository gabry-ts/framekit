# Cookbook: Ecommerce Metrics

This cookbook walks through a realistic ecommerce analytics workflow using FrameKit.
Starting from raw order data, we progressively build up revenue calculations, daily
aggregations, product rankings, customer cohort analysis, moving averages,
month-over-month growth, and pivot tables.

---

## 1. Load order data

Every analysis starts with data. Here we construct a small but representative order
dataset using `DataFrame.fromRows`. Each row represents a single order line with a
date, product SKU, customer ID, quantity, and unit price.

```ts
import { DataFrame, col, lit, when } from 'framekit';

const orders = DataFrame.fromRows([
  { date: '2026-01-05', product: 'Widget A', customer: 'c001', qty: 2, price: 15.00 },
  { date: '2026-01-05', product: 'Widget B', customer: 'c002', qty: 1, price: 45.00 },
  { date: '2026-01-06', product: 'Widget A', customer: 'c003', qty: 5, price: 15.00 },
  { date: '2026-01-06', product: 'Widget C', customer: 'c001', qty: 1, price: 120.00 },
  { date: '2026-01-07', product: 'Widget B', customer: 'c004', qty: 3, price: 45.00 },
  { date: '2026-01-07', product: 'Widget A', customer: 'c002', qty: 1, price: 15.00 },
  { date: '2026-01-08', product: 'Widget C', customer: 'c005', qty: 2, price: 120.00 },
  { date: '2026-01-08', product: 'Widget A', customer: 'c003', qty: 4, price: 15.00 },
  { date: '2026-01-09', product: 'Widget B', customer: 'c001', qty: 2, price: 45.00 },
  { date: '2026-01-09', product: 'Widget C', customer: 'c004', qty: 1, price: 120.00 },
  { date: '2026-01-10', product: 'Widget A', customer: 'c005', qty: 3, price: 15.00 },
  { date: '2026-01-10', product: 'Widget B', customer: 'c002', qty: 1, price: 45.00 },
  { date: '2026-02-01', product: 'Widget A', customer: 'c001', qty: 6, price: 15.00 },
  { date: '2026-02-01', product: 'Widget C', customer: 'c003', qty: 1, price: 120.00 },
  { date: '2026-02-02', product: 'Widget B', customer: 'c004', qty: 2, price: 45.00 },
  { date: '2026-02-02', product: 'Widget A', customer: 'c005', qty: 2, price: 15.00 },
  { date: '2026-02-03', product: 'Widget C', customer: 'c002', qty: 1, price: 120.00 },
  { date: '2026-02-03', product: 'Widget B', customer: 'c001', qty: 3, price: 45.00 },
]);
```

---

## 2. Calculate revenue per order line

Before we can aggregate revenue, we need it as a column. `withColumn` combined with
expression arithmetic multiplies quantity by unit price for each row. The expression
`col('qty').mul(col('price'))` is evaluated element-wise without pulling data into
JavaScript objects, keeping things fast.

```ts
const withRevenue = orders.withColumn(
  'revenue',
  col<number>('qty').mul(col<number>('price')),
);
// Each row now has a `revenue` field: qty * price
// e.g. { date: '2026-01-05', product: 'Widget A', customer: 'c001', qty: 2, price: 15, revenue: 30 }
```

---

## 3. Daily revenue and order count

Group by date and compute two aggregations at once: total revenue (sum) and the
number of order lines (count). This gives us a time-series view of the business.

```ts
const dailyMetrics = withRevenue
  .groupBy('date')
  .agg({
    total_revenue: col('revenue').sum(),
    order_count: col('revenue').count(),
    avg_order_value: col('revenue').mean(),
  })
  .sortBy('date', 'asc');
// Result: one row per date with total_revenue, order_count, and avg_order_value
```

---

## 4. Product performance ranking

To find which products generate the most revenue, group by product, sum revenue,
then rank them. The `rank()` window function assigns a 1-based rank after sorting
by total revenue in descending order.

```ts
const productRevenue = withRevenue
  .groupBy('product')
  .agg({
    total_revenue: col('revenue').sum(),
    units_sold: col('qty').sum(),
    order_count: col('revenue').count(),
  });

const ranked = productRevenue.withColumn(
  'revenue_rank',
  col('total_revenue').rank().orderBy('total_revenue', 'desc'),
);
// Widget C likely ranks first due to the higher unit price
```

---

## 5. Customer cohort analysis

Cohort analysis groups customers by their first purchase date, then checks how many
return in subsequent periods. We first find each customer's earliest order date using
`groupBy` with `first` after sorting, then join it back to the full order set.

```ts
// Step 1: Find each customer's first purchase date (cohort)
const firstPurchase = orders
  .sortBy('date', 'asc')
  .groupBy('customer')
  .agg({
    cohort_date: col('date').first(),
  });

// Step 2: Join cohort date back to the orders
const withCohort = withRevenue.join(firstPurchase, 'customer', 'left');

// Step 3: Extract month from both dates for cohort grouping
const cohortAnalysis = withCohort
  .withColumn('order_month', (row: any) => (row.date as string).slice(0, 7))
  .withColumn('cohort_month', (row: any) => (row.cohort_date as string).slice(0, 7));

// Step 4: Count distinct customers per cohort_month x order_month
const cohortCounts = cohortAnalysis
  .groupBy('cohort_month', 'order_month')
  .agg({
    unique_customers: col('customer').countDistinct(),
    cohort_revenue: col('revenue').sum(),
  })
  .sortBy(['cohort_month', 'order_month'], ['asc', 'asc']);
// Shows how many distinct customers from each cohort made purchases in each month
```

---

## 6. Moving average of daily revenue

A rolling mean smooths out day-to-day noise and reveals trends. Here we apply a
3-day moving average to the daily revenue series. The first two rows will be null
because the window needs at least 3 data points.

```ts
const dailySorted = dailyMetrics.sortBy('date', 'asc');

const withMovingAvg = dailySorted.withColumn(
  'revenue_3d_ma',
  col('total_revenue').rollingMean(3),
);
// The 3-day moving average smooths the daily revenue curve.
// Row 0 and 1 are null; row 2 onward contains the mean of the current + 2 prior days.
```

You can also compute a rolling sum or rolling standard deviation the same way:

```ts
const withRollingStats = dailySorted
  .withColumn('revenue_3d_sum', col('total_revenue').rollingSum(3))
  .withColumn('revenue_3d_std', col('total_revenue').rollingStd(3));
```

---

## 7. Month-over-month growth

`pctChange` computes the fractional change between the current and a previous value.
To get month-over-month growth, we first aggregate revenue by month, then apply
`pctChange(1)` which compares each row to the one directly before it.

```ts
const monthlyRevenue = withRevenue
  .withColumn('month', (row: any) => (row.date as string).slice(0, 7))
  .groupBy('month')
  .agg({
    monthly_revenue: col('revenue').sum(),
    monthly_orders: col('revenue').count(),
  })
  .sortBy('month', 'asc');

const withGrowth = monthlyRevenue.withColumn(
  'mom_growth',
  col('monthly_revenue').pctChange(1),
);
// First row has null (no previous month), subsequent rows show fractional growth.
// A value of 0.25 means 25% growth versus the prior month.
```

---

## 8. Pivot table: product x month revenue matrix

Pivot tables reshape long-form data into a cross-tabulation matrix. Here we create
a table where each row is a product, each column is a month, and cells contain
the summed revenue.

```ts
const ordersByProductMonth = withRevenue
  .withColumn('month', (row: any) => (row.date as string).slice(0, 7));

const revenueMatrix = ordersByProductMonth.pivot({
  index: 'product',
  columns: 'month',
  values: 'revenue',
  aggFunc: 'sum',
});
// Result:
// | product   | 2026-01 | 2026-02 |
// |-----------|---------|---------|
// | Widget A  |     195 |     120 |
// | Widget B  |     270 |     225 |
// | Widget C  |     360 |     240 |
```

You can swap `aggFunc` to `'count'` for order counts or `'mean'` for average
order value per product per month.

```ts
const orderCountMatrix = ordersByProductMonth.pivot({
  index: 'product',
  columns: 'month',
  values: 'revenue',
  aggFunc: 'count',
});
```

---

## Putting it all together

The progression above demonstrates a typical ecommerce analytics flow:

1. **Load** raw transactional data with `fromRows`
2. **Derive** computed columns with `withColumn` and expressions
3. **Aggregate** along dimensions with `groupBy().agg()`
4. **Rank** products or customers with window functions
5. **Analyze cohorts** by joining back first-purchase dates
6. **Smooth** time-series data with `rollingMean`
7. **Measure growth** with `pctChange`
8. **Reshape** data into cross-tabs with `pivot`

Each step produces a new immutable DataFrame, so you can branch your analysis at
any point without mutating prior results.
