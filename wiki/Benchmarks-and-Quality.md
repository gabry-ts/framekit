# Benchmarks and Quality

## Benchmarks

- Compare suites are under `tests/benchmarks/`.
- Output artifacts are in `benchmarks/results/`.
- CI includes smoke and nightly benchmark workflows.

### Latest Snapshot (FrameKit vs Arquero)

Checked-in snapshot (`BENCH_ROWS=50000`, `BENCH_ITERS=10`, `BENCH_WARMUP=3`):

| Operation | FrameKit Median (ms) | Arquero Median (ms) | Relative (FrameKit/Arquero) |
| --------- | -------------------: | ------------------: | --------------------------: |
| Filter    |              11.6839 |              4.4141 |                       2.65x |
| Sort      |              66.5706 |             30.2187 |                       2.20x |
| GroupBy   |               8.1293 |              9.3622 |                       0.87x |
| Join      |              66.2457 |             25.6667 |                       2.58x |
| Reshape   |              61.2699 |             47.5455 |                       1.29x |
| Window    |              51.3322 |             59.3355 |                       0.87x |

Raw sources:

- `benchmarks/results/compare-filter.json`
- `benchmarks/results/compare-sort.json`
- `benchmarks/results/compare-groupby.json`
- `benchmarks/results/compare-join.json`
- `benchmarks/results/compare-reshape.json`
- `benchmarks/results/compare-window.json`

### Reproducing Results

```bash
npm run bench:smoke
npm run bench:full
```

## Quality Gates

- Type checking
- Unit and integration tests
- Protected main branch with required checks and review

## Interpretation

Benchmark results are directional and environment-dependent. Re-run on target hardware and runtime before using them as strict production SLAs.
