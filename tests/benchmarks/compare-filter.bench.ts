import { describe, it, expect } from 'vitest';
import { col } from '../../src';
import { numericDataset } from './data/generators';
import { maybeLoadArquero, runCase, writeComparisonResults } from './runner';

describe('benchmark compare filter', () => {
  it('runs FrameKit vs Arquero filter benchmark', async () => {
    const rows = Number(process.env.BENCH_ROWS ?? '100000');
    const warmup = Number(process.env.BENCH_WARMUP ?? '1');
    const iterations = Number(process.env.BENCH_ITERS ?? '5');
    const df = numericDataset(rows);
    const rowsData = df.toArray() as Record<string, unknown>[];

    const framekit = await runCase(
      'framekit-filter',
      () => {
        return df.filter(col('amount').gt(5000));
      },
      warmup,
      iterations,
    );

    const aq = await maybeLoadArquero();
    let arquero: Awaited<ReturnType<typeof runCase>> | undefined;
    if (aq) {
      const table = aq.from(rowsData) as {
        filter: (fn: (d: Record<string, unknown>) => boolean) => unknown;
      };
      arquero = await runCase(
        'arquero-filter',
        () => {
          return table.filter((d) => (d.amount as number) > 5000);
        },
        warmup,
        iterations,
      );
    }

    const result = {
      benchmark: 'Filter',
      framekit,
      ...(arquero
        ? {
            arquero,
            relative: framekit.medianMs / arquero.medianMs,
          }
        : {}),
    };

    await writeComparisonResults('compare-filter', result);

    expect(framekit.medianMs).toBeGreaterThan(0);
  }, 120_000);
});
