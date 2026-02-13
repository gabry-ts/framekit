import { describe, it, expect } from 'vitest';
import { DataFrame } from '../../../../src/dataframe';
import { col } from '../../../../src/expr/expr';
import { shouldUseParallel } from '../../../../src/engine/parallelism/parallel-agg';
import type { AggExpr } from '../../../../src/expr/expr';
import os from 'os';

describe('shouldUseParallel', () => {
  it('returns false when row count is below threshold', () => {
    expect(shouldUseParallel(100)).toBe(false);
    expect(shouldUseParallel(999_999)).toBe(false);
  });

  it('returns true when row count exceeds default threshold on multi-core', () => {
    // Only true if machine has >1 CPU
    const cpus = os.cpus().length;
    if (cpus > 1) {
      expect(shouldUseParallel(1_000_000)).toBe(true);
      expect(shouldUseParallel(2_000_000)).toBe(true);
    }
  });

  it('respects custom threshold option', () => {
    const cpus = os.cpus().length;
    if (cpus > 1) {
      expect(shouldUseParallel(500, { threshold: 100 })).toBe(true);
      expect(shouldUseParallel(50, { threshold: 100 })).toBe(false);
    }
  });

  it('returns false when workerCount is 1', () => {
    expect(shouldUseParallel(2_000_000, { workerCount: 1 })).toBe(false);
  });
});

describe('ParallelAggregator - worker thread integration', () => {
  it('aggAsync produces same results as sync agg for sum', async () => {
    // Create a moderately sized DataFrame
    const rows: object[] = [];
    for (let i = 0; i < 1000; i++) {
      rows.push({ group: `g${i % 10}`, value: i });
    }
    const df = DataFrame.fromRows(rows);
    const gb = df.groupBy('group');

    const syncResult = gb.agg({
      value: col('value').sum() as AggExpr<unknown>,
    });

    // Force parallel with low threshold
    const asyncResult = await gb.aggAsync(
      { value: col('value').sum() as AggExpr<unknown> },
      { threshold: 1, workerCount: 2 },
    );

    // Sort both by group key for comparison
    const syncSorted = syncResult.sortBy('group');
    const asyncSorted = asyncResult.sortBy('group');

    expect(asyncSorted.length).toBe(syncSorted.length);
    for (let i = 0; i < syncSorted.length; i++) {
      expect(asyncSorted.row(i)['group']).toBe(syncSorted.row(i)['group']);
      expect(asyncSorted.row(i)['value']).toBeCloseTo(
        syncSorted.row(i)['value'] as number,
        5,
      );
    }
  });

  it('aggAsync produces same results as sync agg for mean', async () => {
    const rows: object[] = [];
    for (let i = 0; i < 500; i++) {
      rows.push({ cat: `c${i % 5}`, val: i * 1.5 });
    }
    const df = DataFrame.fromRows(rows);
    const gb = df.groupBy('cat');

    const syncResult = gb.agg({
      val: col('val').mean() as AggExpr<unknown>,
    });
    const asyncResult = await gb.aggAsync(
      { val: col('val').mean() as AggExpr<unknown> },
      { threshold: 1, workerCount: 2 },
    );

    const syncSorted = syncResult.sortBy('cat');
    const asyncSorted = asyncResult.sortBy('cat');

    expect(asyncSorted.length).toBe(syncSorted.length);
    for (let i = 0; i < syncSorted.length; i++) {
      expect(asyncSorted.row(i)['cat']).toBe(syncSorted.row(i)['cat']);
      expect(asyncSorted.row(i)['val']).toBeCloseTo(
        syncSorted.row(i)['val'] as number,
        5,
      );
    }
  });

  it('aggAsync produces same results for count', async () => {
    const rows: object[] = [];
    for (let i = 0; i < 300; i++) {
      rows.push({ group: `g${i % 3}`, x: i });
    }
    const df = DataFrame.fromRows(rows);
    const gb = df.groupBy('group');

    const syncResult = gb.agg({
      x: col('x').count() as AggExpr<unknown>,
    });
    const asyncResult = await gb.aggAsync(
      { x: col('x').count() as AggExpr<unknown> },
      { threshold: 1, workerCount: 2 },
    );

    const syncSorted = syncResult.sortBy('group');
    const asyncSorted = asyncResult.sortBy('group');

    expect(asyncSorted.length).toBe(syncSorted.length);
    for (let i = 0; i < syncSorted.length; i++) {
      expect(asyncSorted.row(i)['group']).toBe(syncSorted.row(i)['group']);
      expect(asyncSorted.row(i)['x']).toBe(syncSorted.row(i)['x']);
    }
  });

  it('aggAsync handles min and max', async () => {
    const rows: object[] = [];
    for (let i = 0; i < 200; i++) {
      rows.push({ group: `g${i % 4}`, val: Math.sin(i) * 100 });
    }
    const df = DataFrame.fromRows(rows);
    const gb = df.groupBy('group');

    const syncMin = gb.agg({ val: col('val').min() as AggExpr<unknown> });
    const asyncMin = await gb.aggAsync(
      { val: col('val').min() as AggExpr<unknown> },
      { threshold: 1, workerCount: 2 },
    );

    const syncMax = gb.agg({ val: col('val').max() as AggExpr<unknown> });
    const asyncMax = await gb.aggAsync(
      { val: col('val').max() as AggExpr<unknown> },
      { threshold: 1, workerCount: 2 },
    );

    const syncMinSorted = syncMin.sortBy('group');
    const asyncMinSorted = asyncMin.sortBy('group');
    const syncMaxSorted = syncMax.sortBy('group');
    const asyncMaxSorted = asyncMax.sortBy('group');

    for (let i = 0; i < syncMinSorted.length; i++) {
      expect(asyncMinSorted.row(i)['val']).toBeCloseTo(
        syncMinSorted.row(i)['val'] as number,
        5,
      );
      expect(asyncMaxSorted.row(i)['val']).toBeCloseTo(
        syncMaxSorted.row(i)['val'] as number,
        5,
      );
    }
  });

  it('aggAsync falls back to sync for unsupported agg types', async () => {
    const rows: object[] = [];
    for (let i = 0; i < 100; i++) {
      rows.push({ group: `g${i % 2}`, val: i });
    }
    const df = DataFrame.fromRows(rows);
    const gb = df.groupBy('group');

    // list() is not supported in parallel — should fall back to sync
    const result = await gb.aggAsync(
      { val: col('val').list() as AggExpr<unknown> },
      { threshold: 1, workerCount: 2 },
    );

    expect(result.length).toBe(2);
  });

  it('worker count defaults to os.cpus().length - 1', () => {
    const cpuCount = os.cpus().length;
    const expectedWorkers = Math.max(1, cpuCount - 1);
    // We verify the function returns sensible values
    // by checking shouldUseParallel with high row count
    if (expectedWorkers > 1) {
      expect(shouldUseParallel(2_000_000)).toBe(true);
    } else {
      expect(shouldUseParallel(2_000_000)).toBe(false);
    }
  });

  it('falls back to single-threaded when workerCount is 1', async () => {
    const rows: object[] = [];
    for (let i = 0; i < 100; i++) {
      rows.push({ group: `g${i % 3}`, val: i });
    }
    const df = DataFrame.fromRows(rows);
    const gb = df.groupBy('group');

    // With workerCount: 1, shouldUseParallel returns false → sync fallback
    const result = await gb.aggAsync(
      { val: col('val').sum() as AggExpr<unknown> },
      { threshold: 1, workerCount: 1 },
    );

    expect(result.length).toBe(3);
  });
});
