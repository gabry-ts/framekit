import { bench, describe } from 'vitest';
import { DataFrame } from '../../src/index';
import { generateCSVString } from './helpers';

const csv100k = generateCSVString(100_000);
const csv1m = generateCSVString(1_000_000);

describe('CSV parsing', () => {
  bench('parse 100K rows', async () => {
    await DataFrame.fromCSV(csv100k, { parse: 'string' });
  });

  bench('parse 1M rows', async () => {
    await DataFrame.fromCSV(csv1m, { parse: 'string' });
  });
});
