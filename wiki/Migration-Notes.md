# Migration Notes

For teams moving from Arquero-like APIs, FrameKit includes `framekit/compat` to reduce migration friction.

## Key Mapping

- `table.derive(...)` -> `derive(df, ...)` or `df.withColumn(...)`
- `groupby(...).rollup(...)` -> `df.groupBy(...).agg(...)` or `rollup(...)`
- `fold(...)` -> `df.melt(...)` or `fold(...)`
- `orderby(...)` -> `df.sortBy(...)` or `orderby(...)`

## Full Guide

See `docs/guides/migration-arquero.md` for side-by-side examples.
