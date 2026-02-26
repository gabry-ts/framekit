# FrameKit

[![CI](https://github.com/gabry-ts/framekit-js/actions/workflows/ci.yml/badge.svg)](https://github.com/gabry-ts/framekit-js/actions/workflows/ci.yml)
[![Bench Smoke](https://github.com/gabry-ts/framekit-js/actions/workflows/bench-smoke.yml/badge.svg)](https://github.com/gabry-ts/framekit-js/actions/workflows/bench-smoke.yml)
[![Bench Nightly](https://github.com/gabry-ts/framekit-js/actions/workflows/bench-nightly.yml/badge.svg)](https://github.com/gabry-ts/framekit-js/actions/workflows/bench-nightly.yml)
![npm](https://img.shields.io/npm/v/framekit-js)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)
![TypeScript](https://img.shields.io/badge/TypeScript-first-blue?logo=typescript&logoColor=white)
![Node.js](https://img.shields.io/badge/node-%3E%3D18-brightgreen?logo=node.js&logoColor=white)
[![GitHub stars](https://img.shields.io/github/stars/gabry-ts/framekit-js?style=social)](https://github.com/gabry-ts/framekit-js/stargazers)
[![GitHub forks](https://img.shields.io/github/forks/gabry-ts/framekit-js?style=social)](https://github.com/gabry-ts/framekit-js/network/members)
[![GitHub issues](https://img.shields.io/github/issues/gabry-ts/framekit-js)](https://github.com/gabry-ts/framekit-js/issues)
[![GitHub last commit](https://img.shields.io/github/last-commit/gabry-ts/framekit-js)](https://github.com/gabry-ts/framekit-js/commits/main)

> DataFrames for TypeScript that don't make you wish you were using Python.
>
> FrameKit is a TypeScript-first DataFrame engine for Node.js. Strong types, predictable null semantics, lazy and eager execution, and production-grade I/O — without the "just use pandas" compromise.

## Table of Contents

- [Why FrameKit](#why-framekit)
- [Feature Overview](#feature-overview)
- [Install](#install)
- [60-Second Example](#60-second-example)
- [Eager and Lazy APIs](#eager-and-lazy-apis)
- [I/O Matrix](#io-matrix)
- [Core and Compat APIs](#core-and-compat-apis)
- [Documentation](#documentation)
- [Comparison](#comparison)
- [Benchmarks and Quality](#benchmarks-and-quality)
- [Project Standards](#project-standards)
- [License](#license)

## Why FrameKit

- Strong TypeScript ergonomics for both row-level and expression-level transformations.
- Shared semantics across eager and lazy execution.
- Practical data engineering surface: joins, reshape, aggregations, window ops, and streaming formats.
- Compatibility layer (`framekit-js/compat`) to ease migration from Arquero-style pipelines.

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
npm install framekit-js
```

## 60-Second Example

```ts
import { DataFrame, col, op } from 'framekit-js';

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

Expected output:

```
// result.toArray()
[
  { region: 'US', total_revenue: 115, avg_revenue: 57.5 },
  { region: 'EU', total_revenue: 45,  avg_revenue: 22.5 }
]

// quality.toArray()
[
  { qty_price_corr: 0.7071067811865476 }
]
```

## Eager and Lazy APIs

```ts
import { DataFrame, col } from 'framekit-js';

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

- `framekit-js`: native API (`DataFrame`, expressions, lazy plans, I/O).
- `framekit-js/compat`: migration-friendly verbs (`derive`, `rollup`, `fold`, `orderby`) and helpers (`all`, `not`, `range`, `desc`).

Start here for migration examples: `docs/guides/migration-arquero.md`.

## Documentation

- Getting started: `docs/getting-started/quickstart.md`
- Guides: `docs/guides/`
- Cookbook examples: `docs/cookbook/`
- API references: `docs/reference/`

## Comparison

|                    | FrameKit | Arquero | Danfo.js | Polars (Node) |
| ------------------ | -------- | ------- | -------- | ------------- |
| TypeScript-first   | Yes      | No      | Partial  | No (Rust FFI) |
| Lazy execution     | Yes      | No      | No       | Yes           |
| Expression API     | Yes      | No      | No       | Yes           |
| Null semantics     | Explicit | Implicit| Implicit | Explicit      |
| I/O formats        | 7        | 2       | 4        | 6             |
| Browser support    | Yes      | Yes     | Yes      | No            |
| Zero native deps   | Yes      | Yes     | No       | No            |

## Benchmarks and Quality

- Benchmark compare runners and outputs live under `tests/benchmarks/` and `benchmarks/results/`.
- CI includes smoke and nightly benchmark workflows.
- Regression checks are available in the benchmark harness.

### Latest Benchmark Snapshot (FrameKit vs Arquero)

Current checked-in snapshot (`BENCH_ROWS=50000`, `BENCH_ITERS=10`, `BENCH_WARMUP=3`):

| Operation | FrameKit Median (ms) | Arquero Median (ms) | Relative (FrameKit/Arquero) | Source                                    |
| --------- | -------------------: | ------------------: | --------------------------: | ----------------------------------------- |
| Filter    |              26.9228 |             36.6342 |                       0.73x | `benchmarks/results/compare-filter.json`  |
| Sort      |              25.5620 |             50.6992 |                       0.50x | `benchmarks/results/compare-sort.json`    |
| GroupBy   |               3.9674 |              5.0675 |                       0.78x | `benchmarks/results/compare-groupby.json` |
| Join      |              20.4444 |             61.5447 |                       0.33x | `benchmarks/results/compare-join.json`    |
| Reshape   |              17.2577 |             65.6563 |                       0.26x | `benchmarks/results/compare-reshape.json` |
| Window    |              22.2838 |             64.1068 |                       0.35x | `benchmarks/results/compare-window.json`  |

### Run Benchmarks Locally

```bash
# Fast smoke check
npm run bench:smoke

# Full compare suite
npm run bench:full
```

For human-readable per-operation reports, see `benchmarks/results/*.md`.

Benchmark outputs are directional and environment-sensitive; validate on your own hardware and runtime before drawing hard conclusions.

## Project Standards

- Changelog: `CHANGELOG.md`
- Contributing guide: `CONTRIBUTING.md`
- Security policy: `SECURITY.md`
- Code of conduct: `CODE_OF_CONDUCT.md`

## License

MIT - see `LICENSE`.
