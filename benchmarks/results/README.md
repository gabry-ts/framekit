# Benchmark Results

This folder stores generated benchmark outputs from compare suites.

## Latest Snapshot (FrameKit vs Arquero)

| Operation | FrameKit Median (ms) | Arquero Median (ms) | Relative (FrameKit/Arquero) | JSON                   | Markdown             |
| --------- | -------------------: | ------------------: | --------------------------: | ---------------------- | -------------------- |
| Filter    |              11.6839 |              4.4141 |                       2.65x | `compare-filter.json`  | `compare-filter.md`  |
| Sort      |              66.5706 |             30.2187 |                       2.20x | `compare-sort.json`    | `compare-sort.md`    |
| GroupBy   |               8.1293 |              9.3622 |                       0.87x | `compare-groupby.json` | `compare-groupby.md` |
| Join      |              66.2457 |             25.6667 |                       2.58x | `compare-join.json`    | `compare-join.md`    |
| Reshape   |              61.2699 |             47.5455 |                       1.29x | `compare-reshape.json` | `compare-reshape.md` |
| Window    |              51.3322 |             59.3355 |                       0.87x | `compare-window.json`  | `compare-window.md`  |

## Re-run

```bash
npm run bench:smoke
npm run bench:full
```

Benchmark numbers are environment-sensitive and should be interpreted as directional.
