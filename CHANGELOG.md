# Changelog

All notable changes to this project are documented in this file.
This project adheres to [Semantic Versioning](https://semver.org/).

## 0.1.0

Initial public release.

### DataFrame Core

- `DataFrame.fromRows` and `DataFrame.fromColumns` constructors.
- Selection and shape: `select`, `drop`, `rename`, `cast`, `head`, `tail`, `slice`.
- Row operations: `filter`, `where`, `sortBy`, `unique`, `sample`.
- Column mutation: `withColumn`, `derive`, `apply`, `assign`.
- Group and aggregate: `groupBy().agg(...)` with full aggregation spec.
- Joins: `inner`, `left`, `right`, `outer`, `cross`, `semi`, `anti`.
- Reshape: `pivot`, `melt`, `explode`, `spread`, `unroll`, `transpose`, `concat`.
- Set operations: `union`, `intersection`, `difference`.
- Utility: `relocate`, `lookup`, `reify`, `impute`.
- Pretty-print with `print()` and `toString()`.

### Expression System

- `col(name)` and `lit(value)` factories.
- Arithmetic: `add`, `sub`, `mul`, `div`, `mod`, `pow`.
- Comparison: `eq`, `neq`, `gt`, `gte`, `lt`, `lte`.
- Logical: `and`, `or`, `not`.
- Null helpers: `coalesce`, `fillNull`, `isNull`, `isNotNull`.
- Conditional: `when(...).then(...).otherwise(...)`.
- Aggregations: `sum`, `mean`, `count`, `countDistinct`, `min`, `max`, `std`, `first`, `last`, `list`, `mode`.
- Cross-column aggregation: `op.corr(col('x'), col('y'))`.
- String accessor: `col('name').str.upper()`, `.lower()`, `.contains()`, `.slice()`, etc.
- Date accessor: `col('ts').dt.year()`, `.month()`, `.day()`, `.hour()`, etc.

### Window Functions

- Ranking: `rank`, `denseRank`, `rowNumber`, `percentRank`, `ntile`.
- Cumulative: `cumSum`, `cumMax`, `cumMin`, `cumProd`, `cumCount`.
- Offset: `shift`, `diff`, `pctChange`.
- Partitioned and ordered window expressions.

### Lazy Execution Engine

- `df.lazy()` for deferred query plans.
- `LazyFrame` with `filter`, `select`, `groupBy`, `join`, `sortBy`, `limit`, `distinct`.
- `LazyGroupBy` with `agg`.
- Query optimizer with predicate pushdown and projection pruning.
- `explain()` for plan inspection.
- `collect()` to materialize results.

### I/O

- CSV: `fromCSV` / `toCSV` with delimiter, header, and quoting options.
- JSON: `fromJSON` / `toJSON` with pretty-print support.
- NDJSON: streaming read/write for large files.
- Arrow IPC: `fromArrow` / `toArrow` via optional `apache-arrow` peer dependency.
- Excel: `fromExcel` / `toExcel` via optional `exceljs` peer dependency.
- Parquet: `fromParquet` / `toParquet` via optional `parquet-wasm` peer dependency.
- SQL: `toSQL` for generating INSERT statements.

### Storage Layer

- Typed column storage: `Float64Column`, `Int32Column`, `Utf8Column`, `BooleanColumn`, `DateColumn`, `ObjectColumn`.
- `BitArray` for efficient boolean storage.
- `Series` as the public column API.

### Compatibility Layer

- `framekit-js/compat` entrypoint for Arquero-style migration.
- Verbs: `derive`, `rollup`, `fold`, `orderby`.
- Helpers: `all`, `not`, `range`, `desc`.
- Row-level aggregation helpers: `op.sum`, `op.mean`, `op.count`, `op.min`, `op.max`, `op.distinct`, `op.first`, `op.last`.

### CLI

- `framekit` CLI for quick data inspection from the terminal.

### Browser Support

- `framekit-js/browser` entrypoint with I/O excluded for smaller bundles.

### Error Handling

- Structured error hierarchy: `FrameKitError`, `ColumnNotFoundError`, `TypeMismatchError`, `ShapeMismatchError`, `ParseError`, `IOError`.
- Consistent `ErrorCode` enum for programmatic error handling.
