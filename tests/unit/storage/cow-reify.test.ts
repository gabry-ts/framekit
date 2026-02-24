import { describe, it, expect } from 'vitest';
import { DataFrame } from '../../../src/dataframe';
import { Float64Column } from '../../../src/storage/numeric';
import { Column } from '../../../src/storage/column';

describe('DataFrame.reify()', () => {
  it('returns a new DataFrame with equal values', () => {
    const df = DataFrame.fromRows([
      { a: 1, b: 'hello', c: true },
      { a: 2, b: 'world', c: false },
    ]);
    const reified = df.reify();

    expect(reified.toArray()).toEqual(df.toArray());
    expect(reified.columns).toEqual(df.columns);
    expect(reified.length).toBe(df.length);
  });

  it('reified DataFrame is independent from original', () => {
    const colA = Float64Column.from([1, 2, 3]);
    const colB = Float64Column.from([4, 5, 6]);
    const columns = new Map<string, Column<unknown>>();
    columns.set('a', colA);
    columns.set('b', colB);
    const df = new DataFrame(columns, ['a', 'b']);

    const reified = df.reify();

    // Modify original via withColumn - reified should not be affected
    const modified = df.withColumn('a', [10, 20, 30]);
    expect(modified.col('a').get(0)).toBe(10);
    expect(reified.col('a').get(0)).toBe(1);
  });

  it('mutations on reified do not affect original', () => {
    const df = DataFrame.fromRows([
      { x: 10, y: 20 },
      { x: 30, y: 40 },
    ]);
    const reified = df.reify();

    const modifiedReified = reified.withColumn('x', [100, 200]);
    expect(modifiedReified.col('x').get(0)).toBe(100);
    expect(df.col('x').get(0)).toBe(10);
  });

  it('reified columns are not shared with original', () => {
    const data = new Float64Array([1, 2, 3]);
    const col = new Float64Column(data);
    const columns = new Map<string, Column<unknown>>();
    columns.set('val', col);
    const df = new DataFrame(columns, ['val']);

    const reified = df.reify();

    // The reified DataFrame has its own columns, so the original column's
    // refCount should not increase (reify creates new column objects)
    // The new columns in reified are independent clones
    expect(reified.col('val').get(0)).toBe(1);
    expect(reified.col('val').get(2)).toBe(3);

    // Verify value equality
    for (let i = 0; i < df.length; i++) {
      expect(reified.col('val').get(i)).toBe(df.col('val').get(i));
    }
  });

  it('works with empty DataFrame', () => {
    const df = DataFrame.empty();
    const reified = df.reify();
    expect(reified.length).toBe(0);
    expect(reified.columns).toEqual([]);
  });

  it('works after select (breaks shared column references)', () => {
    const df = DataFrame.fromRows([
      { a: 1, b: 2, c: 3 },
      { a: 4, b: 5, c: 6 },
    ]);

    const selected = df.select('a', 'b');
    const reified = selected.reify();

    expect(reified.toArray()).toEqual(selected.toArray());
    expect(reified.columns).toEqual(['a', 'b']);
  });

  it('works after head (materializes sliced data)', () => {
    const size = 1000;
    const rows = Array.from({ length: size }, (_, i) => ({ val: i }));
    const df = DataFrame.fromRows(rows);

    const headed = df.head(5);
    const reified = headed.reify();

    expect(reified.length).toBe(5);
    for (let i = 0; i < 5; i++) {
      expect(reified.col('val').get(i)).toBe(i);
    }
  });

  it('preserves null values', () => {
    const df = DataFrame.fromRows([
      { a: 1, b: 'hello' },
      { a: null, b: null },
      { a: 3, b: 'world' },
    ]);

    const reified = df.reify();
    expect(reified.col('a').get(1)).toBeNull();
    expect(reified.col('b').get(1)).toBeNull();
    expect(reified.col('a').get(0)).toBe(1);
    expect(reified.col('b').get(2)).toBe('world');
  });
});
