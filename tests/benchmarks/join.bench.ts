import { bench, describe } from 'vitest';
import { DataFrame } from '../../src';

function generateJoinDF(nRows: number, keyRange: number): DataFrame<Record<string, unknown>> {
  const rows: Record<string, unknown>[] = [];
  for (let i = 0; i < nRows; i++) {
    rows.push({
      key: Math.floor(Math.random() * keyRange),
      value: Math.random() * 1000,
    });
  }
  return DataFrame.fromRows(rows);
}

const dfLeft = generateJoinDF(1_000_000, 100_000);
const dfRight = generateJoinDF(100_000, 100_000);

describe('inner hash join', () => {
  bench('1M x 100K rows', () => {
    dfLeft.join(dfRight, 'key', 'inner');
  });
});
