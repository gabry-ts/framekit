import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { DataFrame } from '../../../src/dataframe';
import * as fs from 'fs/promises';
import * as path from 'path';
import * as os from 'os';

describe('Parquet I/O (US-077)', () => {
  let tmpDir: string;
  let parquetAvailable = false;

  beforeAll(async () => {
    tmpDir = await fs.mkdtemp(path.join(os.tmpdir(), 'framekit-parquet-io-'));
    try {
      await import('parquet-wasm');
      await import('apache-arrow');
      parquetAvailable = true;
    } catch {
      parquetAvailable = false;
    }
  });

  afterAll(async () => {
    await fs.rm(tmpDir, { recursive: true, force: true });
  });

  it('toParquet writes a valid Parquet file', async () => {
    if (!parquetAvailable) return;

    const df = DataFrame.fromRows([
      { id: 1, name: 'Alice', score: 95.5 },
      { id: 2, name: 'Bob', score: 87.0 },
      { id: 3, name: 'Charlie', score: 72.3 },
    ]);

    const outPath = path.join(tmpDir, 'write-valid.parquet');
    await df.toParquet(outPath);

    const stat = await fs.stat(outPath);
    expect(stat.size).toBeGreaterThan(0);

    // Parquet files start with magic bytes 'PAR1'
    const buf = await fs.readFile(outPath);
    const magic = String.fromCharCode(buf[0]!, buf[1]!, buf[2]!, buf[3]!);
    expect(magic).toBe('PAR1');
  });

  it('fromParquet reads data with correct types', async () => {
    if (!parquetAvailable) return;

    const df = DataFrame.fromRows([
      { num: 3.14, str: 'hello', flag: true, count: 42 },
      { num: 2.71, str: 'world', flag: false, count: 7 },
    ]);

    const outPath = path.join(tmpDir, 'types.parquet');
    await df.toParquet(outPath);

    const df2 = await DataFrame.fromParquet(outPath);
    expect(df2.length).toBe(2);

    // Numeric values preserved
    const numSeries = df2.col('num');
    expect(numSeries.get(0)).toBeCloseTo(3.14, 5);
    expect(numSeries.get(1)).toBeCloseTo(2.71, 5);

    // String values preserved
    const strSeries = df2.col('str');
    expect(strSeries.get(0)).toBe('hello');
    expect(strSeries.get(1)).toBe('world');

    // Boolean values preserved
    const flagSeries = df2.col('flag');
    expect(flagSeries.get(0)).toBe(true);
    expect(flagSeries.get(1)).toBe(false);

    // Integer values preserved as numbers
    const countSeries = df2.col('count');
    expect(countSeries.get(0)).toBe(42);
    expect(countSeries.get(1)).toBe(7);
  });

  it('Parquet roundtrip: toParquet then fromParquet produces equal DataFrame', async () => {
    if (!parquetAvailable) return;

    const original = DataFrame.fromRows([
      { a: 1, b: 'x', c: true },
      { a: 2, b: 'y', c: false },
      { a: 3, b: 'z', c: true },
    ]);

    const outPath = path.join(tmpDir, 'roundtrip.parquet');
    await original.toParquet(outPath);
    const restored = await DataFrame.fromParquet(outPath);

    expect(restored.length).toBe(original.length);
    expect([...restored.columns].sort()).toEqual([...original.columns].sort());

    // Verify each cell value matches
    for (const colName of original.columns) {
      const origCol = original.col(colName);
      const restCol = restored.col(colName);
      for (let i = 0; i < original.length; i++) {
        const origVal = origCol.get(i);
        const restVal = restCol.get(i);
        if (typeof origVal === 'number') {
          expect(restVal).toBeCloseTo(origVal, 10);
        } else {
          expect(restVal).toEqual(origVal);
        }
      }
    }
  });

  it('column selection option on fromParquet', async () => {
    if (!parquetAvailable) return;

    const df = DataFrame.fromRows([
      { x: 1, y: 'a', z: true },
      { x: 2, y: 'b', z: false },
    ]);

    const outPath = path.join(tmpDir, 'col-select.parquet');
    await df.toParquet(outPath);

    // Read only a subset of columns
    const df2 = await DataFrame.fromParquet(outPath, { columns: ['x', 'z'] });
    expect(df2.columns.length).toBe(2);
    expect([...df2.columns].sort()).toEqual(['x', 'z']);
    expect(df2.length).toBe(2);

    // Verify values of selected columns
    expect(df2.col('x').get(0)).toBe(1);
    expect(df2.col('x').get(1)).toBe(2);
    expect(df2.col('z').get(0)).toBe(true);
    expect(df2.col('z').get(1)).toBe(false);
  });

  it('compression option: snappy (default)', async () => {
    if (!parquetAvailable) return;

    const df = DataFrame.fromRows([
      { val: 1, text: 'hello' },
      { val: 2, text: 'world' },
    ]);

    const outPath = path.join(tmpDir, 'snappy.parquet');
    await df.toParquet(outPath); // snappy is default

    const stat = await fs.stat(outPath);
    expect(stat.size).toBeGreaterThan(0);

    // Verify roundtrip with snappy
    const df2 = await DataFrame.fromParquet(outPath);
    expect(df2.length).toBe(2);
    expect(df2.col('val').get(0)).toBe(1);
    expect(df2.col('text').get(0)).toBe('hello');
  });

  it('compression option: gzip', async () => {
    if (!parquetAvailable) return;

    const df = DataFrame.fromRows([
      { val: 10, text: 'foo' },
      { val: 20, text: 'bar' },
    ]);

    const outPath = path.join(tmpDir, 'gzip.parquet');
    await df.toParquet(outPath, { compression: 'gzip' });

    const stat = await fs.stat(outPath);
    expect(stat.size).toBeGreaterThan(0);

    // Verify roundtrip with gzip
    const df2 = await DataFrame.fromParquet(outPath);
    expect(df2.length).toBe(2);
    expect(df2.col('val').get(0)).toBe(10);
    expect(df2.col('text').get(0)).toBe('foo');
  });

  it('null value handling in Parquet', async () => {
    if (!parquetAvailable) return;

    const df = DataFrame.fromColumns({
      a: [1, null, 3, null, 5],
      b: ['x', null, 'z', null, 'w'],
      c: [true, false, null, true, null],
    });

    const outPath = path.join(tmpDir, 'nulls.parquet');
    await df.toParquet(outPath);

    const df2 = await DataFrame.fromParquet(outPath);
    expect(df2.length).toBe(5);

    // Numeric nulls
    const aSeries = df2.col('a');
    expect(aSeries.get(0)).toBe(1);
    expect(aSeries.get(1)).toBeNull();
    expect(aSeries.get(2)).toBe(3);
    expect(aSeries.get(3)).toBeNull();
    expect(aSeries.get(4)).toBe(5);

    // String nulls
    const bSeries = df2.col('b');
    expect(bSeries.get(0)).toBe('x');
    expect(bSeries.get(1)).toBeNull();
    expect(bSeries.get(2)).toBe('z');
    expect(bSeries.get(3)).toBeNull();
    expect(bSeries.get(4)).toBe('w');

    // Boolean nulls
    const cSeries = df2.col('c');
    expect(cSeries.get(0)).toBe(true);
    expect(cSeries.get(1)).toBe(false);
    expect(cSeries.get(2)).toBeNull();
    expect(cSeries.get(3)).toBe(true);
    expect(cSeries.get(4)).toBeNull();
  });

  it('roundtrip with larger dataset', async () => {
    if (!parquetAvailable) return;

    const rows = Array.from({ length: 200 }, (_, i) => ({
      id: i,
      label: `row_${String(i)}`,
      value: Math.random() * 100,
      active: i % 2 === 0,
    }));
    const df = DataFrame.fromRows(rows);

    const outPath = path.join(tmpDir, 'large.parquet');
    await df.toParquet(outPath, { rowGroupSize: 50 });

    const df2 = await DataFrame.fromParquet(outPath);
    expect(df2.length).toBe(200);
    expect([...df2.columns].sort()).toEqual(['active', 'id', 'label', 'value']);

    // Spot-check a few values
    expect(df2.col('id').get(0)).toBe(0);
    expect(df2.col('id').get(199)).toBe(199);
    expect(df2.col('label').get(5)).toBe('row_5');
    expect(df2.col('active').get(0)).toBe(true);
    expect(df2.col('active').get(1)).toBe(false);
  });
});
