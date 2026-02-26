import { describe, it, expect } from 'vitest';
import { numericDataset } from './data/generators';
import { maybeLoadArquero, runCase, writeComparisonResults } from './runner';

describe('benchmark compare sort', () => {
  it('runs FrameKit vs Arquero sort benchmark', async () => {
    const rows = Number(process.env.BENCH_ROWS ?? '100000');
    const warmup = Number(process.env.BENCH_WARMUP ?? '1');
    const iterations = Number(process.env.BENCH_ITERS ?? '5');
    const df = numericDataset(rows);
    const rowsData = df.toArray() as Record<string, unknown>[];

    const framekit = await runCase(
      'framekit-sort',
      () => {
        return df.sortBy(['group', 'amount']);
      },
      warmup,
      iterations,
    );

    const aq = await maybeLoadArquero();
    let arquero: Awaited<ReturnType<typeof runCase>> | undefined;
    if (aq) {
      const table = aq.from(rowsData) as {
        orderby: (...cols: string[]) => unknown;
      };
      arquero = await runCase(
        'arquero-sort',
        () => {
          return table.orderby('group', 'amount');
        },
        warmup,
        iterations,
      );
    }

    const result = {
      benchmark: 'Sort',
      framekit,
      ...(arquero ? { arquero, relative: framekit.medianMs / arquero.medianMs } : {}),
    };

    await writeComparisonResults('compare-sort', result);
    expect(framekit.medianMs).toBeGreaterThan(0);
  }, 120_000);
});
