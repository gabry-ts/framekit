import { describe, it, expect } from 'vitest';
import { numericDataset } from './data/generators';
import { maybeLoadArquero, runCase, writeComparisonResults } from './runner';

describe('benchmark compare join', () => {
  it('runs FrameKit vs Arquero join benchmark', async () => {
    const rows = Number(process.env.BENCH_ROWS ?? '50000');
    const warmup = Number(process.env.BENCH_WARMUP ?? '1');
    const iterations = Number(process.env.BENCH_ITERS ?? '5');
    const left = numericDataset(rows, 21);
    const right = numericDataset(Math.floor(rows / 2), 22).rename({
      amount: 'amount_r',
      score: 'score_r',
    });
    const leftRows = left.toArray() as Record<string, unknown>[];
    const rightRows = right.toArray() as Record<string, unknown>[];

    const framekit = await runCase(
      'framekit-join',
      () => {
        return left.join(right, 'id', 'left');
      },
      warmup,
      iterations,
    );

    const aq = await maybeLoadArquero();
    let arquero: Awaited<ReturnType<typeof runCase>> | undefined;
    if (aq) {
      const leftTable = aq.from(leftRows) as {
        join_left: (other: unknown, key: string) => unknown;
      };
      const rightTable = aq.from(rightRows);
      arquero = await runCase(
        'arquero-join',
        () => {
          return leftTable.join_left(rightTable, 'id');
        },
        warmup,
        iterations,
      );
    }

    const result = {
      benchmark: 'Join',
      framekit,
      ...(arquero ? { arquero, relative: framekit.medianMs / arquero.medianMs } : {}),
    };

    await writeComparisonResults('compare-join', result);
    expect(framekit.medianMs).toBeGreaterThan(0);
  }, 120_000);
});
