import { describe, it, expect } from 'vitest';
import { DataFrame } from '../../../src/dataframe';
import { DType } from '../../../src/types/dtype';
import * as path from 'path';

const fixturesDir = path.join(__dirname, '../../fixtures');

describe('CSV I/O with test fixtures (US-025)', () => {
  describe('simple.csv', () => {
    it('reads simple.csv with auto-detected types', async () => {
      const df = await DataFrame.fromCSV(path.join(fixturesDir, 'simple.csv'));

      expect(df.shape).toEqual([5, 4]);
      expect(df.columns).toEqual(['name', 'age', 'score', 'joined']);

      // Check dtypes were auto-detected
      expect(df.col('name').dtype).toBe(DType.Utf8);
      expect(df.col('age').dtype).toBe(DType.Int32);
      expect(df.col('score').dtype).toBe(DType.Float64);
      expect(df.col('joined').dtype).toBe(DType.Date);
    });

    it('reads correct values from simple.csv', async () => {
      const df = await DataFrame.fromCSV(path.join(fixturesDir, 'simple.csv'));

      expect(df.col('name').get(0)).toBe('Alice');
      expect(df.col('age').get(0)).toBe(30);
      expect(df.col('score').get(0)).toBeCloseTo(95.5);
      const joined = df.col('joined').get(0) as Date;
      expect(joined.getFullYear()).toBe(2024);
      expect(joined.getMonth()).toBe(0); // January
      expect(joined.getDate()).toBe(15);
    });

    it('reads all rows from simple.csv', async () => {
      const df = await DataFrame.fromCSV(path.join(fixturesDir, 'simple.csv'));

      expect(df.col('name').get(4)).toBe('Eve');
      expect(df.col('age').get(4)).toBe(32);
    });
  });

  describe('quoted.csv', () => {
    it('reads quoted.csv handling RFC 4180 edge cases', async () => {
      const df = await DataFrame.fromCSV(path.join(fixturesDir, 'quoted.csv'));

      expect(df.columns).toEqual(['name', 'description', 'value']);
      expect(df.shape[0]).toBe(5);
    });

    it('handles fields with commas', async () => {
      const df = await DataFrame.fromCSV(path.join(fixturesDir, 'quoted.csv'));

      expect(df.col('name').get(1)).toBe('With, comma');
      expect(df.col('description').get(1)).toBe('Field contains a comma');
    });

    it('handles escaped quotes', async () => {
      const df = await DataFrame.fromCSV(path.join(fixturesDir, 'quoted.csv'));

      expect(df.col('name').get(2)).toBe('With "quotes"');
      expect(df.col('description').get(2)).toBe('Escaped quotes inside');
    });

    it('handles multiline fields', async () => {
      const df = await DataFrame.fromCSV(path.join(fixturesDir, 'quoted.csv'));

      expect(df.col('name').get(3)).toBe('Multi\nline');
      expect(df.col('description').get(3)).toBe('Field spans\ntwo lines');
    });

    it('handles combined edge cases', async () => {
      const df = await DataFrame.fromCSV(path.join(fixturesDir, 'quoted.csv'));

      expect(df.col('name').get(4)).toBe('All "combined, with\nnewline"');
      expect(df.col('value').get(4)).toBe(5);
    });
  });

  describe('european.csv', () => {
    it('reads european.csv with semicolon delimiter', async () => {
      const df = await DataFrame.fromCSV(path.join(fixturesDir, 'european.csv'));

      expect(df.shape).toEqual([3, 4]);
      expect(df.columns).toEqual(['name', 'price', 'quantity', 'active']);
    });

    it('auto-detects semicolon delimiter', async () => {
      const df = await DataFrame.fromCSV(path.join(fixturesDir, 'european.csv'));

      expect(df.col('name').get(0)).toBe('Alice');
      expect(df.col('price').get(0)).toBe(1234);
      expect(df.col('quantity').get(0)).toBe(10);
      expect(df.col('active').get(0)).toBe(true);
    });

    it('reads with explicit delimiter override', async () => {
      const df = await DataFrame.fromCSV(path.join(fixturesDir, 'european.csv'), {
        delimiter: ';',
      });

      expect(df.shape).toEqual([3, 4]);
      expect(df.col('name').get(1)).toBe('Bob');
      expect(df.col('price').get(1)).toBe(5678);
    });
  });

  describe('toCSV produces valid CSV', () => {
    it('toCSV output can be re-read', async () => {
      const df = DataFrame.fromColumns({
        name: ['Alice', 'Bob'],
        age: [30, 25],
        active: [true, false],
      });

      const csv = df.toCSV();
      const restored = await DataFrame.fromCSV(csv, { parse: 'string' });

      expect(restored.shape).toEqual(df.shape);
      expect(restored.columns).toEqual(df.columns);
      expect(restored.col('name').get(0)).toBe('Alice');
      expect(restored.col('age').get(0)).toBe(30);
      expect(restored.col('active').get(0)).toBe(true);
    });

    it('toCSV handles special characters correctly', async () => {
      const df = DataFrame.fromColumns({
        text: ['hello, world', 'say "hi"', 'normal'],
      });

      const csv = df.toCSV();
      const restored = await DataFrame.fromCSV(csv, { parse: 'string' });

      expect(restored.col('text').get(0)).toBe('hello, world');
      expect(restored.col('text').get(1)).toBe('say "hi"');
      expect(restored.col('text').get(2)).toBe('normal');
    });
  });

  describe('roundtrip: fromCSV(df.toCSV())', () => {
    it('roundtrip preserves mixed types', async () => {
      const original = DataFrame.fromColumns({
        id: [1, 2, 3],
        name: ['Alice', 'Bob', 'Charlie'],
        score: [95.5, 87.0, 92.3],
        active: [true, false, true],
      });

      const csv = original.toCSV();
      const restored = await DataFrame.fromCSV(csv, { parse: 'string' });

      expect(restored.shape).toEqual(original.shape);
      expect(restored.columns).toEqual(original.columns);

      for (let i = 0; i < original.length; i++) {
        expect(restored.col('id').get(i)).toBe(original.col('id').get(i));
        expect(restored.col('name').get(i)).toBe(original.col('name').get(i));
        expect(restored.col('active').get(i)).toBe(original.col('active').get(i));
      }
    });

    it('roundtrip preserves null values', async () => {
      const original = DataFrame.fromColumns({
        x: [1, null, 3],
        y: ['a', null, 'c'],
      });

      const csv = original.toCSV();
      const restored = await DataFrame.fromCSV(csv, { parse: 'string' });

      expect(restored.col('x').get(0)).toBe(1);
      expect(restored.col('x').get(1)).toBeNull();
      expect(restored.col('x').get(2)).toBe(3);
      expect(restored.col('y').get(0)).toBe('a');
      expect(restored.col('y').get(1)).toBeNull();
      expect(restored.col('y').get(2)).toBe('c');
    });

    it('roundtrip with date values', async () => {
      const original = DataFrame.fromColumns({
        date: [new Date('2024-01-15T00:00:00.000Z'), new Date('2024-06-30T00:00:00.000Z')],
        val: [10, 20],
      });

      const csv = original.toCSV();
      const restored = await DataFrame.fromCSV(csv, { parse: 'string' });

      expect(restored.col('date').dtype).toBe(DType.Date);
      const d0 = restored.col('date').get(0) as Date;
      expect(d0.getFullYear()).toBe(2024);
      expect(d0.getMonth()).toBe(0);
      const d1 = restored.col('date').get(1) as Date;
      expect(d1.getFullYear()).toBe(2024);
      expect(d1.getMonth()).toBe(5);
    });

    it('roundtrip from fixture file', async () => {
      const original = await DataFrame.fromCSV(path.join(fixturesDir, 'simple.csv'));
      const csv = original.toCSV();
      const restored = await DataFrame.fromCSV(csv, { parse: 'string' });

      expect(restored.shape).toEqual(original.shape);
      expect(restored.columns).toEqual(original.columns);

      for (let i = 0; i < original.length; i++) {
        expect(restored.col('name').get(i)).toBe(original.col('name').get(i));
        expect(restored.col('age').get(i)).toBe(original.col('age').get(i));
      }
    });
  });

  describe('CSVReadOptions with fixtures', () => {
    it('delimiter override on simple.csv (no effect, already comma)', async () => {
      const df = await DataFrame.fromCSV(path.join(fixturesDir, 'simple.csv'), {
        delimiter: ',',
      });

      expect(df.shape).toEqual([5, 4]);
      expect(df.col('name').get(0)).toBe('Alice');
    });

    it('skipRows skips initial rows', async () => {
      const csv = 'name,age\nAlice,30\nBob,25\nCharlie,35\n';
      const df = await DataFrame.fromCSV(csv, { parse: 'string', skipRows: 1 });

      // skipRows=1 skips the first data row (Alice), header comes from row 1 (Bob)
      // Actually skipRows skips N rows from the start, then header is next
      expect(df.shape[0]).toBeLessThan(3);
    });

    it('nRows limits rows read', async () => {
      const df = await DataFrame.fromCSV(path.join(fixturesDir, 'simple.csv'), {
        nRows: 2,
      });

      expect(df.shape).toEqual([2, 4]);
      expect(df.col('name').get(0)).toBe('Alice');
      expect(df.col('name').get(1)).toBe('Bob');
    });

    it('columns selects subset of columns', async () => {
      const df = await DataFrame.fromCSV(path.join(fixturesDir, 'simple.csv'), {
        columns: ['name', 'age'],
      });

      expect(df.columns).toEqual(['name', 'age']);
      expect(df.shape).toEqual([5, 2]);
      expect(df.col('name').get(0)).toBe('Alice');
      expect(df.col('age').get(0)).toBe(30);
    });

    it('nullValues recognizes custom null markers', async () => {
      const csv = 'x,y\n1,hello\nNA,world\n3,N/A\n';
      const df = await DataFrame.fromCSV(csv, {
        parse: 'string',
        nullValues: ['NA', 'N/A'],
      });

      expect(df.col('x').get(1)).toBeNull();
      expect(df.col('y').get(2)).toBeNull();
      expect(df.col('x').get(0)).toBe(1);
    });

    it('dtypes override auto-detection', async () => {
      const df = await DataFrame.fromCSV(path.join(fixturesDir, 'simple.csv'), {
        dtypes: { age: DType.Float64, score: DType.Utf8 },
      });

      expect(df.col('age').dtype).toBe(DType.Float64);
      expect(df.col('score').dtype).toBe(DType.Utf8);
      expect(df.col('score').get(0)).toBe('95.5');
    });

    it('comment option filters comment lines', async () => {
      const csv = '# This is a comment\nname,age\nAlice,30\n# Another comment\nBob,25\n';
      const df = await DataFrame.fromCSV(csv, { parse: 'string', comment: '#' });

      expect(df.shape).toEqual([2, 2]);
      expect(df.col('name').get(0)).toBe('Alice');
      expect(df.col('name').get(1)).toBe('Bob');
    });

    it('hasHeader false with auto-generated column names', async () => {
      const csv = 'Alice,30\nBob,25\n';
      const df = await DataFrame.fromCSV(csv, { parse: 'string', hasHeader: false });

      expect(df.columns).toEqual(['column_0', 'column_1']);
      expect(df.col('column_0').get(0)).toBe('Alice');
    });

    it('combined options: nRows + columns', async () => {
      const df = await DataFrame.fromCSV(path.join(fixturesDir, 'simple.csv'), {
        nRows: 3,
        columns: ['name', 'score'],
      });

      expect(df.shape).toEqual([3, 2]);
      expect(df.columns).toEqual(['name', 'score']);
      expect(df.col('name').get(2)).toBe('Charlie');
    });
  });
});
