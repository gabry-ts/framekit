import { describe, it, expect } from 'vitest';
import { col } from '../../src';
import { numericDataset } from './data/generators';
import { maybeLoadArquero, runCase, writeComparisonResults } from './runner';

declare const op: {
  row_number: () => unknown;
};

describe('benchmark compare window', () => {
  it('runs FrameKit vs Arquero window benchmark', async () => {
    const rows = Number(process.env.BENCH_ROWS ?? '100000');
    const warmup = Number(process.env.BENCH_WARMUP ?? '1');
    const iterations = Number(process.env.BENCH_ITERS ?? '5');
    const df = numericDataset(rows);
    const rowsData = df.toArray() as Record<string, unknown>[];

    const framekit = await runCase(
      'framekit-window',
      () => {
        return df.withColumn('rn', col('amount').rowNumber());
      },
      warmup,
      iterations,
    );

    const aq = await maybeLoadArquero();
    let arquero: Awaited<ReturnType<typeof runCase>> | undefined;
    if (aq) {
      const table = aq.from(rowsData) as {
        derive: (obj: Record<string, unknown>) => unknown;
        orderby: (...cols: string[]) => {
          derive: (obj: Record<string, unknown>) => unknown;
        };
      };
      arquero = await runCase(
        'arquero-window',
        () => {
          return table.orderby('amount').derive({ rn: () => op.row_number() });
        },
        warmup,
        iterations,
      );
    }

    const result = {
      benchmark: 'Window',
      framekit,
      ...(arquero ? { arquero, relative: framekit.medianMs / arquero.medianMs } : {}),
    };

    await writeComparisonResults('compare-window', result);
    expect(framekit.medianMs).toBeGreaterThan(0);
  }, 120_000);
});
