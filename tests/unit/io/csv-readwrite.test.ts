import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { DataFrame } from '../../../src/dataframe';
import { DType } from '../../../src/types/dtype';
import * as fs from 'fs/promises';
import * as path from 'path';
import * as os from 'os';

describe('DataFrame CSV reader/writer (US-018)', () => {
  let tmpDir: string;

  beforeAll(async () => {
    tmpDir = await fs.mkdtemp(path.join(os.tmpdir(), 'framekit-csv-'));
  });

  afterAll(async () => {
    await fs.rm(tmpDir, { recursive: true, force: true });
  });

  describe('fromCSV with string', () => {
    it('parses CSV string with parse option', async () => {
      const csv = 'name,age,active\nAlice,30,true\nBob,25,false\n';
      const df = await DataFrame.fromCSV(csv, { parse: 'string' });

      expect(df.shape).toEqual([2, 3]);
      expect(df.columns).toEqual(['name', 'age', 'active']);
      expect(df.col('name').get(0)).toBe('Alice');
      expect(df.col('age').get(0)).toBe(30);
      expect(df.col('active').get(0)).toBe(true);
    });

    it('handles empty CSV string', async () => {
      const df = await DataFrame.fromCSV('', { parse: 'string' });
      expect(df.shape).toEqual([0, 0]);
    });

    it('supports CSVReadOptions: delimiter', async () => {
      const csv = 'name;age\nAlice;30\nBob;25\n';
      const df = await DataFrame.fromCSV(csv, { parse: 'string', delimiter: ';' });

      expect(df.shape).toEqual([2, 2]);
      expect(df.col('name').get(0)).toBe('Alice');
      expect(df.col('age').get(0)).toBe(30);
    });

    it('supports CSVReadOptions: hasHeader false', async () => {
      const csv = 'Alice,30\nBob,25\n';
      const df = await DataFrame.fromCSV(csv, { parse: 'string', hasHeader: false });

      expect(df.columns).toEqual(['column_0', 'column_1']);
      expect(df.shape).toEqual([2, 2]);
    });

    it('supports CSVReadOptions: custom header', async () => {
      const csv = 'Alice,30\nBob,25\n';
      const df = await DataFrame.fromCSV(csv, {
        parse: 'string',
        hasHeader: false,
        header: ['name', 'age'],
      });

      expect(df.columns).toEqual(['name', 'age']);
      expect(df.col('name').get(0)).toBe('Alice');
    });

    it('supports CSVReadOptions: dtypes override', async () => {
      const csv = 'id,value\n1,100\n2,200\n';
      const df = await DataFrame.fromCSV(csv, {
        parse: 'string',
        dtypes: { id: DType.Utf8 },
      });

      expect(df.col('id').dtype).toBe(DType.Utf8);
      expect(df.col('id').get(0)).toBe('1');
      expect(df.col('value').dtype).toBe(DType.Int32);
    });

    it('supports CSVReadOptions: nRows', async () => {
      const csv = 'x\n1\n2\n3\n4\n5\n';
      const df = await DataFrame.fromCSV(csv, { parse: 'string', nRows: 3 });

      expect(df.shape).toEqual([3, 1]);
      expect(df.col('x').get(2)).toBe(3);
    });

    it('supports CSVReadOptions: columns selection', async () => {
      const csv = 'a,b,c\n1,2,3\n4,5,6\n';
      const df = await DataFrame.fromCSV(csv, { parse: 'string', columns: ['a', 'c'] });

      expect(df.columns).toEqual(['a', 'c']);
      expect(df.shape).toEqual([2, 2]);
    });

    it('auto-detects date columns', async () => {
      const csv = 'date,val\n2024-01-15,10\n2024-02-20,20\n';
      const df = await DataFrame.fromCSV(csv, { parse: 'string' });

      expect(df.col('date').dtype).toBe(DType.Date);
      const d = df.col('date').get(0) as Date;
      expect(d.getFullYear()).toBe(2024);
    });

    it('handles null values', async () => {
      const csv = 'x,y\n1,hello\n,\n3,world\n';
      const df = await DataFrame.fromCSV(csv, { parse: 'string' });

      expect(df.col('x').get(1)).toBeNull();
      expect(df.col('y').get(1)).toBeNull();
    });
  });

  describe('fromCSV with file path', () => {
    it('reads CSV from file', async () => {
      const filePath = path.join(tmpDir, 'test.csv');
      await fs.writeFile(filePath, 'name,score\nAlice,95\nBob,87\n', 'utf-8');

      const df = await DataFrame.fromCSV(filePath);

      expect(df.shape).toEqual([2, 2]);
      expect(df.col('name').get(0)).toBe('Alice');
      expect(df.col('score').get(0)).toBe(95);
    });

    it('throws IOError for non-existent file', async () => {
      await expect(
        DataFrame.fromCSV(path.join(tmpDir, 'nonexistent.csv')),
      ).rejects.toThrow('Failed to read CSV file');
    });
  });

  describe('toCSV', () => {
    it('returns CSV string with header', () => {
      const df = DataFrame.fromColumns({ name: ['Alice', 'Bob'], age: [30, 25] });
      const csv = df.toCSV();

      expect(csv).toBe('name,age\nAlice,30\nBob,25\n');
    });

    it('handles null values with default empty string', () => {
      const df = DataFrame.fromColumns({ x: [1, null, 3] });
      const csv = df.toCSV();

      expect(csv).toBe('x\n1\n\n3\n');
    });

    it('supports custom delimiter', () => {
      const df = DataFrame.fromColumns({ a: [1, 2], b: [3, 4] });
      const csv = df.toCSV({ delimiter: ';' });

      expect(csv).toBe('a;b\n1;3\n2;4\n');
    });

    it('supports quoteStyle always', () => {
      const df = DataFrame.fromColumns({ x: ['hello', 'world'] });
      const csv = df.toCSV({ quoteStyle: 'always' });

      expect(csv).toBe('"x"\n"hello"\n"world"\n');
    });

    it('supports quoteStyle never', () => {
      const df = DataFrame.fromColumns({ x: ['hello,world'] });
      const csv = df.toCSV({ quoteStyle: 'never' });

      expect(csv).toBe('x\nhello,world\n');
    });

    it('quotes fields with delimiters by default (necessary)', () => {
      const df = DataFrame.fromColumns({ x: ['hello,world', 'foo'] });
      const csv = df.toCSV();

      expect(csv).toBe('x\n"hello,world"\nfoo\n');
    });

    it('escapes quotes in quoted fields', () => {
      const df = DataFrame.fromColumns({ x: ['say "hi"'] });
      const csv = df.toCSV();

      expect(csv).toBe('x\n"say ""hi"""\n');
    });

    it('supports custom nullValue', () => {
      const df = DataFrame.fromColumns({ x: [1, null, 3] });
      const csv = df.toCSV({ nullValue: 'NA' });

      expect(csv).toBe('x\n1\nNA\n3\n');
    });

    it('supports header: false', () => {
      const df = DataFrame.fromColumns({ x: [1, 2], y: [3, 4] });
      const csv = df.toCSV({ header: false });

      expect(csv).toBe('1,3\n2,4\n');
    });

    it('supports BOM', () => {
      const df = DataFrame.fromColumns({ x: [1] });
      const csv = df.toCSV({ bom: true });

      expect(csv.startsWith('\ufeff')).toBe(true);
    });

    it('writes CSV to file', async () => {
      const df = DataFrame.fromColumns({ a: [1, 2], b: ['x', 'y'] });
      const filePath = path.join(tmpDir, 'output.csv');

      await df.toCSV(filePath);

      const content = await fs.readFile(filePath, 'utf-8');
      expect(content).toBe('a,b\n1,x\n2,y\n');
    });

    it('writes CSV to file with options', async () => {
      const df = DataFrame.fromColumns({ a: [1], b: [2] });
      const filePath = path.join(tmpDir, 'output2.csv');

      await df.toCSV(filePath, { delimiter: '\t' });

      const content = await fs.readFile(filePath, 'utf-8');
      expect(content).toBe('a\tb\n1\t2\n');
    });

    it('serializes Date values as ISO strings', () => {
      const df = DataFrame.fromColumns({ d: [new Date('2024-01-15T00:00:00.000Z')] });
      const csv = df.toCSV();

      expect(csv).toContain('2024-01-15T00:00:00.000Z');
    });

    it('serializes boolean values', () => {
      const df = DataFrame.fromColumns({ active: [true, false] });
      const csv = df.toCSV();

      expect(csv).toBe('active\ntrue\nfalse\n');
    });
  });

  describe('roundtrip', () => {
    it('fromCSV(df.toCSV()) produces equivalent DataFrame', async () => {
      const original = DataFrame.fromColumns({
        name: ['Alice', 'Bob', 'Charlie'],
        age: [30, 25, 35],
        active: [true, false, true],
      });

      const csv = original.toCSV();
      const restored = await DataFrame.fromCSV(csv, { parse: 'string' });

      expect(restored.shape).toEqual(original.shape);
      expect(restored.columns).toEqual(original.columns);

      for (let i = 0; i < original.length; i++) {
        expect(restored.col('name').get(i)).toBe(original.col('name').get(i));
        expect(restored.col('age').get(i)).toBe(original.col('age').get(i));
        expect(restored.col('active').get(i)).toBe(original.col('active').get(i));
      }
    });

    it('roundtrip with nulls', async () => {
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

    it('roundtrip via file', async () => {
      const original = DataFrame.fromColumns({
        id: [1, 2, 3],
        value: ['x', 'y', 'z'],
      });

      const filePath = path.join(tmpDir, 'roundtrip.csv');
      await original.toCSV(filePath);
      const restored = await DataFrame.fromCSV(filePath);

      expect(restored.shape).toEqual(original.shape);
      expect(restored.col('id').get(0)).toBe(1);
      expect(restored.col('value').get(2)).toBe('z');
    });
  });
});
