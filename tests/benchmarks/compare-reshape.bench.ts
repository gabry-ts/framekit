import { describe, it, expect } from 'vitest';
import { mixedDataset } from './data/generators';
import { maybeLoadArquero, runCase, writeComparisonResults } from './runner';

describe('benchmark compare reshape', () => {
  it('runs FrameKit vs Arquero reshape benchmark', async () => {
    const rows = Number(process.env.BENCH_ROWS ?? '50000');
    const warmup = Number(process.env.BENCH_WARMUP ?? '1');
    const iterations = Number(process.env.BENCH_ITERS ?? '5');
    const df = mixedDataset(rows);
    const rowsData = df.toArray() as Record<string, unknown>[];

    const framekit = await runCase(
      'framekit-reshape',
      () => {
        return df.melt({
          idVars: ['id', 'region'],
          valueVars: ['revenue', 'orders'],
          varName: 'metric',
          valueName: 'value',
        });
      },
      warmup,
      iterations,
    );

    const aq = await maybeLoadArquero();
    let arquero: Awaited<ReturnType<typeof runCase>> | undefined;
    if (aq) {
      const table = aq.from(rowsData) as {
        fold: (cols: string[], opts?: Record<string, unknown>) => unknown;
      };
      arquero = await runCase(
        'arquero-reshape',
        () => {
          return table.fold(['revenue', 'orders'], { as: ['metric', 'value'] });
        },
        warmup,
        iterations,
      );
    }

    const result = {
      benchmark: 'Reshape',
      framekit,
      ...(arquero ? { arquero, relative: framekit.medianMs / arquero.medianMs } : {}),
    };

    await writeComparisonResults('compare-reshape', result);
    expect(framekit.medianMs).toBeGreaterThan(0);
  }, 120_000);
});
