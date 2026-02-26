# Cookbook: Event Pipeline

This cookbook builds a realistic event and clickstream processing pipeline using
FrameKit. Starting from raw user events, we parse and filter, sessionize by time
gaps, compute session-level metrics, run funnel analysis, explode nested fields,
and export the results.

---

## 1. Load raw events

We start with a set of user interaction events. Each event has a user ID, event
type, a Unix-epoch timestamp (in milliseconds), a page path, and an optional
array of tags for categorization.

```ts
import { DataFrame, col, lit } from 'framekit';

const events = DataFrame.fromRows([
  { user: 'u1', event: 'page_view',   ts: 1706100000000, page: '/',          tags: ['organic', 'mobile'] },
  { user: 'u1', event: 'page_view',   ts: 1706100060000, page: '/products',  tags: ['organic', 'mobile'] },
  { user: 'u1', event: 'add_to_cart', ts: 1706100120000, page: '/products/1', tags: ['organic'] },
  { user: 'u1', event: 'checkout',    ts: 1706100180000, page: '/checkout',   tags: ['organic'] },
  { user: 'u1', event: 'purchase',    ts: 1706100240000, page: '/confirm',    tags: ['organic'] },
  { user: 'u1', event: 'page_view',   ts: 1706105000000, page: '/',          tags: ['direct', 'desktop'] },
  { user: 'u1', event: 'page_view',   ts: 1706105060000, page: '/about',     tags: ['direct', 'desktop'] },
  { user: 'u2', event: 'page_view',   ts: 1706100000000, page: '/',          tags: ['paid', 'mobile'] },
  { user: 'u2', event: 'page_view',   ts: 1706100030000, page: '/products',  tags: ['paid', 'mobile'] },
  { user: 'u2', event: 'add_to_cart', ts: 1706100090000, page: '/products/2', tags: ['paid'] },
  { user: 'u2', event: 'page_view',   ts: 1706108000000, page: '/',          tags: ['organic', 'desktop'] },
  { user: 'u3', event: 'page_view',   ts: 1706100000000, page: '/',          tags: ['organic', 'desktop'] },
  { user: 'u3', event: 'page_view',   ts: 1706100045000, page: '/pricing',   tags: ['organic', 'desktop'] },
  { user: 'u3', event: 'page_view',   ts: 1706100090000, page: '/signup',    tags: ['organic'] },
  { user: 'u3', event: 'signup',      ts: 1706100150000, page: '/welcome',   tags: ['organic'] },
]);
```

---

## 2. Parse and filter events

Before sessionizing, filter to only the event types you care about and extract
derived fields. Here we keep page views and conversion events, and parse the
page path to get the top-level section.

```ts
// Keep only actionable event types
const actionEvents = events.filter(
  col<string>('event').neq('signup'),
);

// Extract the top-level page section from the path
const withSection = actionEvents.withColumn(
  'section',
  (row: any) => {
    const parts = (row.page as string).split('/').filter(Boolean);
    return parts.length > 0 ? parts[0] : 'home';
  },
);
// Each row now has a `section` field: 'home', 'products', 'checkout', etc.
```

You can also filter by multiple conditions using `.and()`:

```ts
const mobilePageViews = events.filter(
  col<string>('event').eq('page_view').and(col<string>('page').neq('/')),
);
```

---

## 3. Sessionize: assign session IDs based on time gaps

Sessionization groups a stream of events into sessions. The standard approach:
sort by user and timestamp, compute the time gap between consecutive events,
and start a new session whenever the gap exceeds a threshold (here, 30 minutes).

We use `shift(1)` to get the previous timestamp and `diff(1)` to compute deltas.
The `.over('user')` modifier ensures these window functions operate within each
user's events independently.

```ts
const SESSION_GAP_MS = 30 * 60 * 1000; // 30 minutes

// Sort by user then timestamp so shift/diff operate in chronological order per user
const sorted = events.sortBy(['user', 'ts'], ['asc', 'asc']);

// Compute the time gap to the previous event within the same user
const withGap = sorted.withColumn(
  'time_gap',
  col('ts').diff(1).over('user'),
);

// Flag rows where a new session starts (first event or gap > threshold)
const withSessionFlag = withGap.withColumn(
  'new_session',
  (row: any) => row.time_gap === null || (row.time_gap as number) > SESSION_GAP_MS ? 1 : 0,
);

// Build session IDs by taking a cumulative sum of the new_session flag per user
const sessionized = withSessionFlag.withColumn(
  'session_id',
  col('new_session').cumSum().over('user'),
);

// Create a composite session key combining user and session number
const withSessionKey = sessionized.withColumn(
  'session_key',
  (row: any) => `${row.user}_s${row.session_id}`,
);
```

---

## 4. Compute session metrics

With sessions assigned, aggregate to get per-session statistics: event count,
session duration, and the first and last pages visited.

```ts
const sessionMetrics = withSessionKey
  .groupBy('user', 'session_key')
  .agg({
    event_count: col('ts').count(),
    session_start: col('ts').min(),
    session_end: col('ts').max(),
    entry_page: col('page').first(),
    exit_page: col('page').last(),
  });

// Compute session duration in seconds
const withDuration = sessionMetrics.withColumn(
  'duration_sec',
  col<number>('session_end').sub(col<number>('session_start')).div(lit(1000)),
);
// Result: one row per session with event_count, duration, entry/exit pages
```

You can then look at overall session statistics:

```ts
const sessionSummary = withDuration
  .groupBy('user')
  .agg({
    total_sessions: col('session_key').count(),
    avg_events_per_session: col('event_count').mean(),
    avg_duration_sec: col('duration_sec').mean(),
  });
```

---

## 5. Funnel analysis

Funnel analysis counts how many distinct users reached each step of a conversion
flow. We filter the full event set for each funnel step, then count distinct users.

```ts
const funnelSteps = ['page_view', 'add_to_cart', 'checkout', 'purchase'];

// Count distinct users who performed each event type
const funnelData = events
  .groupBy('event')
  .agg({
    unique_users: col('user').countDistinct(),
  });
// Filter to only the funnel steps and sort by the natural funnel order
```

For a more detailed approach, you can verify that users completed steps in order:

```ts
// Users who viewed a page
const viewers = events.filter(col<string>('event').eq('page_view'));
const viewerCount = viewers
  .groupBy('event')
  .agg({ users: col('user').countDistinct() });

// Users who added to cart
const carters = events.filter(col<string>('event').eq('add_to_cart'));
const carterCount = carters
  .groupBy('event')
  .agg({ users: col('user').countDistinct() });

// Users who checked out
const checkouters = events.filter(col<string>('event').eq('checkout'));
const checkoutCount = checkouters
  .groupBy('event')
  .agg({ users: col('user').countDistinct() });

// Users who purchased
const purchasers = events.filter(col<string>('event').eq('purchase'));
const purchaseCount = purchasers
  .groupBy('event')
  .agg({ users: col('user').countDistinct() });

// Combine into a single funnel summary
const funnel = DataFrame.fromRows([
  { step: 'page_view',   step_order: 1, unique_users: viewerCount.row(0).users },
  { step: 'add_to_cart', step_order: 2, unique_users: carterCount.row(0).users },
  { step: 'checkout',    step_order: 3, unique_users: checkoutCount.row(0).users },
  { step: 'purchase',    step_order: 4, unique_users: purchaseCount.row(0).users },
]).sortBy('step_order', 'asc');
```

---

## 6. Explode nested tags

Many event systems attach arrays of tags or categories to each event. The `unroll`
method (FrameKit's explode) turns each array element into its own row, which lets
you group and count by individual tag values.

```ts
// Explode the tags array: each tag becomes its own row
const exploded = events.unroll('tags');
// Original row with tags: ['organic', 'mobile'] becomes two rows,
// one with tags='organic' and one with tags='mobile'.

// Count events per tag
const tagCounts = exploded
  .groupBy('tags')
  .agg({
    event_count: col('tags').count(),
    unique_users: col('user').countDistinct(),
  })
  .sortBy('event_count', 'desc');
```

If you want to keep track of the position within the original array, pass the
`index` option:

```ts
const explodedWithIndex = events.unroll('tags', { index: 'tag_position' });
// Each row now has a `tag_position` field (0, 1, ...) indicating where
// the tag appeared in the original array.
```

You can also use `spread` to pivot array elements into separate columns:

```ts
const widened = events.spread('tags', {
  name: (colName, key) => `tag_${String(key)}`,
});
// Creates columns tag_0, tag_1, etc. with the array values spread across them.
```

---

## 7. Export results

Once the analysis is complete, export to CSV or JSON for downstream consumption.
FrameKit supports both in-memory string output and writing directly to files.

```ts
// Export session metrics to CSV string
const csvString = withDuration.toCSV();
console.log(csvString);

// Write directly to a file
await withDuration.toCSV('/tmp/session_metrics.csv');

// Export tag counts to JSON (array of objects)
const jsonString = tagCounts.toJSON();
console.log(jsonString);

// Write JSON to a file
await tagCounts.toJSON('/tmp/tag_counts.json');
```

For large datasets, you can stream results instead of buffering in memory:

```ts
import { createWriteStream } from 'fs';

const stream = createWriteStream('/tmp/large_export.csv');
await withDuration.toCSV(stream);
```

---

## Pipeline summary

The event pipeline pattern demonstrated above covers the core operations needed
for clickstream and behavioral analytics:

1. **Load** raw events with `fromRows` (or `fromCSV` / `fromJSON` for real data)
2. **Filter** to relevant events with `filter` and comparison expressions
3. **Sessionize** using `shift`, `diff`, and `cumSum` with `.over()` partitioning
4. **Aggregate** session-level metrics with `groupBy().agg()`
5. **Analyze funnels** by filtering each step and counting distinct users
6. **Explode** nested arrays with `unroll` for tag-level analysis
7. **Export** results with `toCSV` or `toJSON`

Each transformation produces a new DataFrame, so intermediate results are always
available for inspection or branching into alternative analyses.
