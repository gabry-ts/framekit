import { mkdir, writeFile } from 'node:fs/promises';
import path from 'node:path';

export interface BenchmarkResult {
  name: string;
  samplesMs: number[];
  medianMs: number;
  p95Ms: number;
}

export interface ComparisonResult {
  benchmark: string;
  framekit: BenchmarkResult;
  arquero?: BenchmarkResult;
  relative?: number;
}

function percentile(sorted: number[], p: number): number {
  if (sorted.length === 0) return 0;
  const idx = Math.min(sorted.length - 1, Math.max(0, Math.ceil((p / 100) * sorted.length) - 1));
  return sorted[idx]!;
}

export async function runCase(
  name: string,
  fn: () => unknown | Promise<unknown>,
  warmup = 2,
  iterations = 10,
): Promise<BenchmarkResult> {
  const consume = (value: unknown): void => {
    if (value == null) return;

    if (typeof value === 'number' || typeof value === 'string' || typeof value === 'boolean') {
      return;
    }

    if (Array.isArray(value)) {
      let checksum = 0;
      for (let i = 0; i < value.length; i++) {
        checksum += i;
      }
      if (checksum < 0) {
        throw new Error('unreachable');
      }
      return;
    }

    if (typeof value === 'object' && value !== null) {
      const candidate = value as {
        toArray?: () => unknown[];
        objects?: () => Iterable<unknown>;
      };

      if (typeof candidate.toArray === 'function') {
        const arr = candidate.toArray();
        let checksum = 0;
        for (let i = 0; i < arr.length; i++) {
          checksum += i;
        }
        if (checksum < 0) {
          throw new Error('unreachable');
        }
        return;
      }

      if (typeof candidate.objects === 'function') {
        let count = 0;
        for (const _row of candidate.objects()) {
          count++;
        }
        if (count < 0) {
          throw new Error('unreachable');
        }
      }
    }
  };

  for (let i = 0; i < warmup; i++) {
    const out = await fn();
    consume(out);
  }

  const samplesMs: number[] = [];
  for (let i = 0; i < iterations; i++) {
    const start = process.hrtime.bigint();
    const out = await fn();
    consume(out);
    const end = process.hrtime.bigint();
    samplesMs.push(Number(end - start) / 1_000_000);
  }

  const sorted = [...samplesMs].sort((a, b) => a - b);
  return {
    name,
    samplesMs,
    medianMs: percentile(sorted, 50),
    p95Ms: percentile(sorted, 95),
  };
}

export async function maybeLoadArquero(): Promise<null | {
  from: (rows: Record<string, unknown>[]) => unknown;
  op: Record<string, (...args: unknown[]) => unknown>;
}> {
  try {
    const moduleName = 'arquero';
    const mod = (await import(moduleName)) as {
      default?: unknown;
      from?: unknown;
      op?: unknown;
    };
    const candidate = (mod.default ?? mod) as { from?: unknown; op?: unknown };
    if (typeof candidate.from === 'function' && candidate.op && typeof candidate.op === 'object') {
      return {
        from: candidate.from as (rows: Record<string, unknown>[]) => unknown,
        op: candidate.op as Record<string, (...args: unknown[]) => unknown>,
      };
    }
    return null;
  } catch {
    return null;
  }
}

export async function writeComparisonResults(
  fileSlug: string,
  result: ComparisonResult,
): Promise<void> {
  const outDir = path.resolve('benchmarks/results');
  await mkdir(outDir, { recursive: true });

  const jsonPath = path.join(outDir, `${fileSlug}.json`);
  const mdPath = path.join(outDir, `${fileSlug}.md`);

  await writeFile(jsonPath, `${JSON.stringify(result, null, 2)}\n`, 'utf-8');

  const lines = [
    `# ${result.benchmark}`,
    '',
    '| Engine | Median (ms) | p95 (ms) |',
    '|---|---:|---:|',
    `| FrameKit | ${result.framekit.medianMs.toFixed(3)} | ${result.framekit.p95Ms.toFixed(3)} |`,
  ];

  if (result.arquero) {
    lines.push(
      `| Arquero | ${result.arquero.medianMs.toFixed(3)} | ${result.arquero.p95Ms.toFixed(3)} |`,
    );
  }

  if (typeof result.relative === 'number') {
    const pct = (result.relative * 100).toFixed(2);
    lines.push('');
    lines.push(`Relative median (FrameKit / Arquero): ${pct}%`);
  }

  await writeFile(mdPath, `${lines.join('\n')}\n`, 'utf-8');
}
