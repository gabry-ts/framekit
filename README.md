# FrameKit

[![Bench Smoke](https://github.com/gabry-ts/framekit/actions/workflows/bench-smoke.yml/badge.svg)](https://github.com/gabry-ts/framekit/actions/workflows/bench-smoke.yml)
[![Bench Nightly](https://github.com/gabry-ts/framekit/actions/workflows/bench-nightly.yml/badge.svg)](https://github.com/gabry-ts/framekit/actions/workflows/bench-nightly.yml)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)
[![GitHub stars](https://img.shields.io/github/stars/gabry-ts/framekit?style=social)](https://github.com/gabry-ts/framekit/stargazers)
[![GitHub forks](https://img.shields.io/github/forks/gabry-ts/framekit?style=social)](https://github.com/gabry-ts/framekit/network/members)
[![GitHub issues](https://img.shields.io/github/issues/gabry-ts/framekit)](https://github.com/gabry-ts/framekit/issues)
[![GitHub last commit](https://img.shields.io/github/last-commit/gabry-ts/framekit)](https://github.com/gabry-ts/framekit/commits/main)
[![npm publish](https://img.shields.io/badge/npm-coming_soon-blue)](https://www.npmjs.com/)

FrameKit is a TypeScript-first DataFrame engine for Node.js.
It focuses on three things that matter in real analytics projects: clear APIs, predictable null behavior, and production-ready data I/O.

## Table of Contents

- [Why FrameKit](#why-framekit)
- [Feature Overview](#feature-overview)
- [Install](#install)
- [60-Second Example](#60-second-example)
- [Eager and Lazy APIs](#eager-and-lazy-apis)
- [I/O Matrix](#io-matrix)
- [Core and Compat APIs](#core-and-compat-apis)
- [Documentation and Wiki](#documentation-and-wiki)
- [Benchmarks and Quality](#benchmarks-and-quality)
- [Project Standards](#project-standards)
- [License](#license)

## Why FrameKit

- Strong TypeScript ergonomics for both row-level and expression-level transformations.
- Shared semantics across eager and lazy execution.
- Practical data engineering surface: joins, reshape, aggregations, window ops, and streaming formats.
- Compatibility layer (`framekit/compat`) to ease migration from Arquero-style pipelines.

## Feature Overview

| Area               | Highlights                                                                  |
| ------------------ | --------------------------------------------------------------------------- |
| DataFrame core     | `select`, `drop`, `filter`, `sortBy`, `groupBy`, `join`, `unique`, `sample` |
| Expressions        | arithmetic, comparison, logical, null helpers, string/date accessors        |
| Aggregations       | `sum`, `mean`, `count`, `min`, `max`, `std`, `corr`, list/mode, first/last  |
| Reshape            | `pivot`, `melt`, `explode`, `spread`, `unroll`, `transpose`, `concat`       |
| Utility transforms | `assign`, `relocate`, `lookup`, `reify`, `impute`, `derive`, `apply`        |
| Lazy execution     | query plans, optimizer passes, explain support, eager executor              |
| I/O                | CSV, JSON, NDJSON, Arrow, Excel, Parquet, SQL export                        |

## Install

```bash
npm install framekit
```

## 60-Second Example

```ts
import { DataFrame, col, op } from 'framekit';

const sales = DataFrame.fromRows([
  { region: 'EU', product: 'A', qty: 2, price: 10 },
  { region: 'EU', product: 'B', qty: 1, price: 25 },
  { region: 'US', product: 'A', qty: 4, price: 10 },
  { region: 'US', product: 'B', qty: 3, price: 25 },
]);

const result = sales
  .withColumn('revenue', col<number>('qty').mul(col<number>('price')))
  .groupBy('region')
  .agg({
    total_revenue: col('revenue').sum(),
    avg_revenue: col('revenue').mean(),
  })
  .sortBy('total_revenue', 'desc');

const quality = sales.groupBy().agg({
  qty_price_corr: op.corr(col('qty'), col('price')),
});

console.log(result.toArray());
console.log(quality.toArray());
```

## Eager and Lazy APIs

```ts
import { DataFrame, col } from 'framekit';

const source = DataFrame.fromRows([
  { city: 'Rome', temp: 26 },
  { city: 'Milan', temp: 31 },
  { city: 'Turin', temp: 28 },
]);

const eager = source.filter(col<number>('temp').gt(27));

const lazy = source.lazy().filter(col<number>('temp').gt(27)).select('city', 'temp').collect();
```

## I/O Matrix

| Format             | Read | Write |
| ------------------ | ---- | ----- |
| CSV                | Yes  | Yes   |
| JSON               | Yes  | Yes   |
| NDJSON (streaming) | Yes  | Yes   |
| Arrow IPC          | Yes  | Yes   |
| Excel              | Yes  | Yes   |
| Parquet            | Yes  | Yes   |
| SQL (INSERT text)  | No   | Yes   |

## Core and Compat APIs

- `framekit`: native API (`DataFrame`, expressions, lazy plans, I/O).
- `framekit/compat`: migration-friendly verbs (`derive`, `rollup`, `fold`, `orderby`) and helpers (`all`, `not`, `range`, `desc`).

Start here for migration examples: `docs/guides/migration-arquero.md`.

## Documentation and Wiki

- Getting started: `docs/getting-started/quickstart.md`
- Guides: `docs/guides/`
- Cookbook examples: `docs/cookbook/`
- API references: `docs/reference/`
- GitHub Wiki: `https://github.com/gabry-ts/framekit/wiki`

Wiki pages are sourced from `wiki/` and can be synced through `.github/workflows/wiki-sync.yml`.

## Benchmarks and Quality

- Benchmark compare runners and outputs live under `tests/benchmarks/` and `benchmarks/results/`.
- CI includes smoke and nightly benchmark workflows.
- Regression checks are available in the benchmark harness.

Benchmark outputs are directional and environment-sensitive; validate on your own hardware before drawing hard conclusions.

## Project Standards

- Changelog: `CHANGELOG.md`
- Contributing guide: `CONTRIBUTING.md`
- Security policy: `SECURITY.md`
- Code of conduct: `CODE_OF_CONDUCT.md`

## License

MIT - see `LICENSE`.
