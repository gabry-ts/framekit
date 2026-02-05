import { bench, describe } from 'vitest';
import { col } from '../../src/index';
import { generateLargeDF } from './helpers';

const df100k = generateLargeDF(100_000, 6);

describe('filter', () => {
  bench('predicate filter 100K rows', () => {
    df100k.filter((row: Record<string, unknown>) => (row['value'] as number) > 500);
  });

  bench('expression filter 100K rows', () => {
    df100k.filter(col('value').gt(500));
  });
});
