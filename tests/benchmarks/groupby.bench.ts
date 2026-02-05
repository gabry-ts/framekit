import { bench, describe } from 'vitest';
import { DataFrame, col } from '../../src';

function generateGroupByDF(nRows: number, nGroups: number): DataFrame<Record<string, unknown>> {
  const rows: Record<string, unknown>[] = [];
  for (let i = 0; i < nRows; i++) {
    rows.push({
      group: `g_${String(i % nGroups)}`,
      value: Math.random() * 1000,
    });
  }
  return DataFrame.fromRows(rows);
}

const df1M_100 = generateGroupByDF(1_000_000, 100);
const df1M_10K = generateGroupByDF(1_000_000, 10_000);

describe('groupBy + sum', () => {
  bench('1M rows, 100 groups', () => {
    df1M_100.groupBy('group').agg({ total: col('value').sum() });
  });

  bench('1M rows, 10K groups', () => {
    df1M_10K.groupBy('group').agg({ total: col('value').sum() });
  });
});
