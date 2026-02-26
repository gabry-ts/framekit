# Benchmarks and Quality

## Benchmarks

- Compare suites are under `tests/benchmarks/`.
- Output artifacts are in `benchmarks/results/`.
- CI includes smoke and nightly benchmark workflows.

## Quality Gates

- Type checking
- Unit and integration tests
- Protected main branch with required checks and review

## Interpretation

Benchmark results are directional and environment-dependent. Re-run on target hardware and runtime before using them as strict production SLAs.
