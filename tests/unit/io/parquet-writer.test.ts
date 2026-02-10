import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { DataFrame } from '../../../src/dataframe';
import * as fs from 'fs/promises';
import * as path from 'path';
import * as os from 'os';

describe('DataFrame.toParquet (US-071)', () => {
  let tmpDir: string;
  let parquetAvailable = false;

  beforeAll(async () => {
    tmpDir = await fs.mkdtemp(path.join(os.tmpdir(), 'framekit-parquet-'));
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

  it('writes a basic DataFrame to Parquet and reads it back', async () => {
    if (!parquetAvailable) return;

    const df = DataFrame.fromRows([
      { name: 'Alice', age: 30, active: true },
      { name: 'Bob', age: 25, active: false },
      { name: 'Charlie', age: 35, active: true },
    ]);

    const outPath = path.join(tmpDir, 'basic.parquet');
    await df.toParquet(outPath);

    // Verify file exists and is non-empty
    const stat = await fs.stat(outPath);
    expect(stat.size).toBeGreaterThan(0);

    // Read it back
    const df2 = await DataFrame.fromParquet(outPath);
    expect(df2.length).toBe(3);
    expect(df2.columns.length).toBe(3);

    // Verify column names exist
    const colNames = [...df2.columns].sort();
    expect(colNames).toEqual(['active', 'age', 'name']);
  });

  it('writes with snappy compression by default', async () => {
    if (!parquetAvailable) return;

    const df = DataFrame.fromRows([
      { x: 1, y: 'a' },
      { x: 2, y: 'b' },
    ]);

    const outPath = path.join(tmpDir, 'snappy.parquet');
    await df.toParquet(outPath);

    const stat = await fs.stat(outPath);
    expect(stat.size).toBeGreaterThan(0);
  });

  it('supports compression options: gzip', async () => {
    if (!parquetAvailable) return;

    const df = DataFrame.fromRows([
      { x: 1, y: 'hello' },
      { x: 2, y: 'world' },
    ]);

    const outPath = path.join(tmpDir, 'gzip.parquet');
    await df.toParquet(outPath, { compression: 'gzip' });

    const stat = await fs.stat(outPath);
    expect(stat.size).toBeGreaterThan(0);

    // Read back to verify
    const df2 = await DataFrame.fromParquet(outPath);
    expect(df2.length).toBe(2);
  });

  it('supports compression option: none', async () => {
    if (!parquetAvailable) return;

    const df = DataFrame.fromRows([
      { x: 1 },
      { x: 2 },
    ]);

    const outPath = path.join(tmpDir, 'uncompressed.parquet');
    await df.toParquet(outPath, { compression: 'none' });

    const stat = await fs.stat(outPath);
    expect(stat.size).toBeGreaterThan(0);
  });

  it('supports rowGroupSize option', async () => {
    if (!parquetAvailable) return;

    const rows = Array.from({ length: 100 }, (_, i) => ({ id: i, val: `row_${String(i)}` }));
    const df = DataFrame.fromRows(rows);

    const outPath = path.join(tmpDir, 'rowgroup.parquet');
    await df.toParquet(outPath, { rowGroupSize: 10 });

    const stat = await fs.stat(outPath);
    expect(stat.size).toBeGreaterThan(0);

    const df2 = await DataFrame.fromParquet(outPath);
    expect(df2.length).toBe(100);
  });

  it('correctly maps DType to Parquet types', async () => {
    if (!parquetAvailable) return;

    const df = DataFrame.fromRows([
      { num: 3.14, str: 'hello', flag: true, count: 42 },
      { num: 2.71, str: 'world', flag: false, count: 7 },
    ]);

    const outPath = path.join(tmpDir, 'dtypes.parquet');
    await df.toParquet(outPath);

    const df2 = await DataFrame.fromParquet(outPath);
    expect(df2.length).toBe(2);

    // Verify numeric values round-trip
    const numSeries = df2.col('num');
    expect(numSeries.get(0)).toBeCloseTo(3.14, 5);
    expect(numSeries.get(1)).toBeCloseTo(2.71, 5);

    // Verify string values round-trip
    const strSeries = df2.col('str');
    expect(strSeries.get(0)).toBe('hello');
    expect(strSeries.get(1)).toBe('world');
  });

  it('handles null values correctly', async () => {
    if (!parquetAvailable) return;

    const df = DataFrame.fromColumns({
      a: [1, null, 3],
      b: ['x', 'y', null],
    });

    const outPath = path.join(tmpDir, 'nulls.parquet');
    await df.toParquet(outPath);

    const df2 = await DataFrame.fromParquet(outPath);
    expect(df2.length).toBe(3);

    const aSeries = df2.col('a');
    expect(aSeries.get(0)).toBe(1);
    expect(aSeries.get(1)).toBeNull();
    expect(aSeries.get(2)).toBe(3);

    const bSeries = df2.col('b');
    expect(bSeries.get(0)).toBe('x');
    expect(bSeries.get(1)).toBe('y');
    expect(bSeries.get(2)).toBeNull();
  });

  it('has toParquet method on DataFrame', () => {
    expect(typeof DataFrame.prototype.toParquet).toBe('function');
  });
});
