import { describe, it, expect } from 'vitest';
import { col } from '../../src';
import { numericDataset } from './data/generators';
import { maybeLoadArquero, runCase, writeComparisonResults } from './runner';

declare const op: {
  sum: (value: unknown) => unknown;
};

describe('benchmark compare groupby', () => {
  it('runs FrameKit vs Arquero groupby benchmark', async () => {
    const rows = Number(process.env.BENCH_ROWS ?? '100000');
    const warmup = Number(process.env.BENCH_WARMUP ?? '1');
    const iterations = Number(process.env.BENCH_ITERS ?? '5');
    const df = numericDataset(rows);
    const rowsData = df.toArray() as Record<string, unknown>[];

    const framekit = await runCase(
      'framekit-groupby',
      () => {
        return df.groupBy('group').agg({ total: col('amount').sum() });
      },
      warmup,
      iterations,
    );

    const aq = await maybeLoadArquero();
    let arquero: Awaited<ReturnType<typeof runCase>> | undefined;
    if (aq) {
      const table = aq.from(rowsData) as {
        groupby: (key: string) => {
          rollup: (obj: Record<string, unknown>) => unknown;
        };
      };
      arquero = await runCase(
        'arquero-groupby',
        () => {
          return table.groupby('group').rollup({
            total: (d: Record<string, unknown>) => op.sum(d.amount as unknown),
          });
        },
        warmup,
        iterations,
      );
    }

    const result = {
      benchmark: 'GroupBy',
      framekit,
      ...(arquero ? { arquero, relative: framekit.medianMs / arquero.medianMs } : {}),
    };

    await writeComparisonResults('compare-groupby', result);
    expect(framekit.medianMs).toBeGreaterThan(0);
  }, 120_000);
});
