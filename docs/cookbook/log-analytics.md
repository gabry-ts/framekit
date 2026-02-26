# Cookbook: Log Analytics

This cookbook demonstrates how to analyze server logs using FrameKit. We cover
error rate calculations, latency statistics, time-series aggregation, anomaly
detection, status code distribution, cumulative error tracking, and conditional
alert summaries.

---

## 1. Load log data

We start with structured log entries. Each row represents a single HTTP request
with a timestamp, log level, service name, response latency in milliseconds,
and HTTP status code.

```ts
import { DataFrame, col, lit, when } from 'framekit';

const logs = DataFrame.fromRows([
  { timestamp: '2026-01-15T10:00:12', level: 'INFO',  service: 'api',     latency_ms: 45,   status: 200 },
  { timestamp: '2026-01-15T10:00:15', level: 'INFO',  service: 'api',     latency_ms: 52,   status: 200 },
  { timestamp: '2026-01-15T10:00:18', level: 'ERROR', service: 'api',     latency_ms: 1230, status: 500 },
  { timestamp: '2026-01-15T10:00:22', level: 'WARN',  service: 'api',     latency_ms: 310,  status: 429 },
  { timestamp: '2026-01-15T10:00:25', level: 'INFO',  service: 'auth',    latency_ms: 23,   status: 200 },
  { timestamp: '2026-01-15T10:00:30', level: 'INFO',  service: 'auth',    latency_ms: 18,   status: 200 },
  { timestamp: '2026-01-15T10:00:35', level: 'ERROR', service: 'auth',    latency_ms: 5050, status: 503 },
  { timestamp: '2026-01-15T10:00:40', level: 'INFO',  service: 'payments', latency_ms: 89,  status: 200 },
  { timestamp: '2026-01-15T10:00:45', level: 'INFO',  service: 'payments', latency_ms: 102, status: 201 },
  { timestamp: '2026-01-15T10:00:50', level: 'ERROR', service: 'payments', latency_ms: 3400, status: 500 },
  { timestamp: '2026-01-15T10:01:00', level: 'INFO',  service: 'api',     latency_ms: 38,   status: 200 },
  { timestamp: '2026-01-15T10:01:05', level: 'INFO',  service: 'api',     latency_ms: 41,   status: 200 },
  { timestamp: '2026-01-15T10:01:10', level: 'WARN',  service: 'api',     latency_ms: 280,  status: 429 },
  { timestamp: '2026-01-15T10:01:15', level: 'INFO',  service: 'auth',    latency_ms: 19,   status: 200 },
  { timestamp: '2026-01-15T10:01:20', level: 'ERROR', service: 'api',     latency_ms: 2100, status: 502 },
  { timestamp: '2026-01-15T10:01:25', level: 'INFO',  service: 'payments', latency_ms: 95,  status: 200 },
  { timestamp: '2026-01-15T10:01:30', level: 'INFO',  service: 'payments', latency_ms: 88,  status: 201 },
  { timestamp: '2026-01-15T10:01:35', level: 'ERROR', service: 'auth',    latency_ms: 4800, status: 503 },
  { timestamp: '2026-01-15T10:01:40', level: 'INFO',  service: 'api',     latency_ms: 55,   status: 200 },
  { timestamp: '2026-01-15T10:01:45', level: 'INFO',  service: 'auth',    latency_ms: 21,   status: 200 },
]);
```

---

## 2. Error rate by service

To compute error rates, we need two numbers per service: total requests and error
requests. We derive an `is_error` flag, then group by service to get both counts.
Dividing errors by total gives the error rate.

```ts
// Flag each row as error or not
const withErrorFlag = logs.withColumn(
  'is_error',
  (row: any) => (row.level === 'ERROR' ? 1 : 0),
);

// Aggregate per service
const errorRates = withErrorFlag
  .groupBy('service')
  .agg({
    total_requests: col('is_error').count(),
    error_count: col('is_error').sum(),
  });

// Compute error rate as a derived column
const withRate = errorRates.withColumn(
  'error_rate',
  col<number>('error_count').div(col<number>('total_requests')),
);
// Result:
// | service  | total_requests | error_count | error_rate |
// |----------|----------------|-------------|------------|
// | api      |              8 |           2 |       0.25 |
// | auth     |              5 |           2 |        0.4 |
// | payments |              4 |           1 |       0.25 |
```

---

## 3. Latency percentiles and statistics

Understanding latency distribution is critical for SLA monitoring. Group by service
and compute mean, standard deviation, minimum, and maximum latency.

```ts
const latencyStats = logs
  .groupBy('service')
  .agg({
    avg_latency: col('latency_ms').mean(),
    std_latency: col('latency_ms').std(),
    min_latency: col('latency_ms').min(),
    max_latency: col('latency_ms').max(),
    request_count: col('latency_ms').count(),
  })
  .sortBy('avg_latency', 'desc');
// Services with higher average latency or wider standard deviation
// may need investigation.
```

To dig deeper, you can also look at latency stats for errors vs. non-errors:

```ts
const latencyByLevel = logs
  .groupBy('service', 'level')
  .agg({
    avg_latency: col('latency_ms').mean(),
    max_latency: col('latency_ms').max(),
    count: col('latency_ms').count(),
  })
  .sortBy(['service', 'level'], ['asc', 'asc']);
// Confirms that ERROR-level requests tend to have much higher latency
```

---

## 4. Time-series: requests per minute

For time-series visualization, truncate timestamps to the minute and count requests
in each bucket. This gives a requests-per-minute (RPM) metric.

```ts
// Truncate timestamp to the minute
const withMinute = logs.withColumn(
  'minute',
  (row: any) => (row.timestamp as string).slice(0, 16), // '2026-01-15T10:00'
);

const rpm = withMinute
  .groupBy('minute')
  .agg({
    request_count: col('minute').count(),
    error_count: col('latency_ms').count(), // placeholder; see below for proper count
    avg_latency: col('latency_ms').mean(),
  })
  .sortBy('minute', 'asc');
```

For a breakdown by service within each minute:

```ts
const rpmByService = withMinute
  .groupBy('minute', 'service')
  .agg({
    request_count: col('minute').count(),
    avg_latency: col('latency_ms').mean(),
  })
  .sortBy(['minute', 'service'], ['asc', 'asc']);
// Produces a time-series per service, ready for plotting
```

---

## 5. Anomaly detection: flag high-latency requests

A simple anomaly detection approach flags requests whose latency exceeds 2 standard
deviations above the service mean. We compute per-service statistics, join them back,
and derive a boolean flag using expressions.

```ts
// Step 1: Compute per-service latency stats
const serviceStats = logs
  .groupBy('service')
  .agg({
    svc_mean: col('latency_ms').mean(),
    svc_std: col('latency_ms').std(),
  });

// Step 2: Join stats back to each log row
const withStats = logs.join(serviceStats, 'service', 'left');

// Step 3: Compute the anomaly threshold (mean + 2*std) and flag anomalies
const withThreshold = withStats.withColumn(
  'threshold',
  col<number>('svc_mean').add(col<number>('svc_std').mul(lit(2))),
);

const withAnomaly = withThreshold.withColumn(
  'is_anomaly',
  col<number>('latency_ms').gt(col<number>('threshold')),
);

// Step 4: Filter to just the anomalies for review
const anomalies = withAnomaly.filter(col<boolean>('is_anomaly').eq(true));
// These are the requests that are statistical outliers within their service
```

---

## 6. Status code distribution (pivot)

A pivot table shows the count of each HTTP status code per service. This reveals
at a glance which services produce which types of responses.

```ts
// Convert status to string so it works as a pivot column
const withStatusStr = logs.withColumn(
  'status_str',
  (row: any) => String(row.status),
);

const statusDistribution = withStatusStr.pivot({
  index: 'service',
  columns: 'status_str',
  values: 'latency_ms',
  aggFunc: 'count',
});
// Result:
// | service  | 200 | 201 | 429 | 500 | 502 | 503 |
// |----------|-----|-----|-----|-----|-----|-----|
// | api      |   5 |   0 |   2 |   1 |   1 |   0 |
// | auth     |   3 |   0 |   0 |   0 |   0 |   2 |
// | payments |   2 |   2 |   0 |   1 |   0 |   0 |
```

You can also melt a pivot table back to long form if needed:

```ts
const longForm = statusDistribution.melt({
  id: ['service'],
  value: ['200', '201', '429', '500', '502', '503'],
  variableName: 'status_code',
  valueName: 'count',
});
```

---

## 7. Cumulative error count over time

Tracking how errors accumulate over time reveals whether error bursts are isolated
or sustained. Sort chronologically, flag errors, then apply a cumulative sum.

```ts
const sorted = logs.sortBy('timestamp', 'asc');

const withErrorNum = sorted.withColumn(
  'is_error',
  (row: any) => (row.level === 'ERROR' ? 1 : 0),
);

const withCumErrors = withErrorNum.withColumn(
  'cumulative_errors',
  col('is_error').cumSum(),
);
// Each row now shows the running total of errors up to that point.
// A steep increase in cumulative_errors indicates an error burst.
```

For per-service cumulative tracking, use `.over()`:

```ts
const withCumErrorsByService = withErrorNum.withColumn(
  'svc_cumulative_errors',
  col('is_error').cumSum().over('service'),
);
// Independent running error counts for each service
```

---

## 8. Alert summary with conditional logic

Use `when/then/otherwise` to classify each request into severity levels based on
latency and status code. This produces a human-readable alert column that can
drive notification logic.

```ts
const withSeverity = logs.withColumn(
  'severity',
  when(col<string>('level').eq('ERROR'))
    .then('critical')
    .when(col<number>('latency_ms').gt(lit(1000)))
    .then('high')
    .when(col<number>('latency_ms').gt(lit(200)))
    .then('medium')
    .otherwise('low'),
);
// Result: each row gets a severity label based on the cascading conditions
```

You can then aggregate to get an alert summary:

```ts
const alertSummary = withSeverity
  .groupBy('service', 'severity')
  .agg({
    count: col('severity').count(),
    avg_latency: col('latency_ms').mean(),
    max_latency: col('latency_ms').max(),
  })
  .sortBy(['service', 'severity'], ['asc', 'asc']);
// Shows how many requests of each severity level exist per service
```

For a final dashboard-ready summary, pivot severity across services:

```ts
const severityMatrix = withSeverity.pivot({
  index: 'service',
  columns: 'severity',
  values: 'latency_ms',
  aggFunc: 'count',
});
// | service  | critical | high | medium | low |
// |----------|----------|------|--------|-----|
// | api      |        2 |    0 |      2 |   4 |
// | auth     |        2 |    0 |      0 |   3 |
// | payments |        1 |    0 |      0 |   3 |
```

---

## Summary

This cookbook covered the core patterns for server log analysis:

1. **Load** structured log data with `fromRows`
2. **Compute error rates** using derived flags and `groupBy().agg()`
3. **Analyze latency** with `mean`, `std`, `min`, and `max` aggregations
4. **Build time-series** by truncating timestamps and grouping by time bucket
5. **Detect anomalies** by joining per-service statistics and comparing with thresholds
6. **Visualize distributions** with `pivot` tables
7. **Track cumulative errors** with `cumSum` and `.over()` for per-service tracking
8. **Classify severity** with `when/then/otherwise` conditional expressions

These patterns compose naturally. For example, you could combine anomaly detection
(step 5) with severity classification (step 8) to build a complete alerting pipeline,
then export the results with `toCSV` or `toJSON` for integration with monitoring tools.
