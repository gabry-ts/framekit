# Migrating from Arquero to FrameKit

This guide maps common Arquero APIs to FrameKit core and `framekit-js/compat` equivalents.

## API Mapping

| Arquero                    | FrameKit Core                             | FrameKit Compat                  |
| -------------------------- | ----------------------------------------- | -------------------------------- |
| `aq.table(rows)`           | `DataFrame.fromRows(rows)`                | same as core                     |
| `table.params(...)`        | use JS variables/closures                 | same as core                     |
| `derive({...})`            | `df.withColumn(...)` / `df.derive({...})` | `derive(df, {...})`              |
| `filter(...)`              | `df.filter(...)`                          | same as core                     |
| `groupby(...).rollup(...)` | `df.groupBy(...).agg({...})`              | `rollup(df.groupBy(...), {...})` |
| `join(...)`                | `df.join(...)`                            | same as core                     |
| `fold(...)`                | `df.melt(...)`                            | `fold(df, [...])`                |
| `orderby(...)`             | `df.sortBy(...)`                          | `orderby(df, [...])`             |

## Side-by-Side Examples

### Table Creation

```ts
// Arquero
const t = aq.table([{ a: 1 }, { a: 2 }]);

// FrameKit
const df = DataFrame.fromRows([{ a: 1 }, { a: 2 }]);
```

### Filter

```ts
// Arquero
t.filter((d) => d.a > 1);

// FrameKit
df.filter((d) => (d.a as number) > 1);
```

### Derive

```ts
// Arquero
t.derive({ b: (d) => d.a * 2 });

// FrameKit core
df.withColumn('b', (d) => (d.a as number) * 2);

// FrameKit compat
derive(df, { b: (d) => (d.a as number) * 2 });
```

### GroupBy + Aggregate

```ts
// Arquero
t.groupby('k').rollup({ total: (d) => op.sum(d.v) });

// FrameKit core
df.groupBy('k').agg({ total: col('v').sum() });

// FrameKit compat
rollup(df.groupBy('k'), { total: (d) => op.sum(d.v!) });
```

### Join

```ts
// Arquero
left.join_left(right, 'id');

// FrameKit
left.join(right, 'id', 'left');
```

### Reshape

```ts
// Arquero
t.fold(['x', 'y'], { as: ['metric', 'value'] });

// FrameKit core
df.melt({ idVars: ['id'], valueVars: ['x', 'y'], varName: 'metric', valueName: 'value' });

// FrameKit compat
fold(df, ['x', 'y'], { as: ['metric', 'value'] });
```

### I/O

```ts
const csv = await DataFrame.fromCSV('./data.csv');
await csv.toParquet('./data.parquet');
```

## Semantic Differences and Gotchas

- FrameKit null semantics are explicit (`null` is preserved and propagated in many ops).
- `op.rowNumber()` in FrameKit is 0-based in derive/withColumn context.
- `lookup` keeps one output row per left row (first matching key wins on duplicates).
