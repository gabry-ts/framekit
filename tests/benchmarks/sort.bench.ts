import { bench, describe } from 'vitest';
import { generateLargeDF } from './helpers';

const df100k = generateLargeDF(100_000, 6);

describe('sort', () => {
  bench('single-column sort 100K rows', () => {
    df100k.sortBy('value');
  });

  bench('multi-column sort 100K rows', () => {
    df100k.sortBy(['category', 'value']);
  });
});
