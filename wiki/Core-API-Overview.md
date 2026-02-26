# Core API Overview

## DataFrame Core

- Selection and shape: `select`, `drop`, `head`, `tail`, `slice`
- Row filtering and sort: `filter`, `where`, `sortBy`, `unique`, `sample`
- Group and aggregate: `groupBy().agg(...)`, shorthand aggregations
- Joins: inner, left, right, outer, cross, semi, anti
- Reshape: `pivot`, `melt`, `explode`, `spread`, `unroll`, `transpose`

## Utility Methods

- `assign`
- `relocate`
- `lookup`
- `reify`
- `derive`
- `apply`
- `impute`

## Compat Entry Point

Use `framekit/compat` for migration-friendly verbs:

- `derive`
- `rollup`
- `fold`
- `orderby`
