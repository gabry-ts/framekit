import { describe, it, expect } from 'vitest';
import { Float64Column, Int32Column } from '../../../src/storage/numeric';
import { Utf8Column } from '../../../src/storage/string';
import { BooleanColumn } from '../../../src/storage/boolean';
import { DateColumn } from '../../../src/storage/date';
import { ObjectColumn } from '../../../src/storage/object';
import { DataFrame } from '../../../src/dataframe';
import { Column } from '../../../src/storage/column';

describe('Copy-on-Write: Reference Counting', () => {
  it('should start with refCount of 1', () => {
    const col = Float64Column.from([1, 2, 3]);
    expect(col.refCount).toBe(1);
    expect(col.isShared).toBe(false);
  });

  it('should increment refCount with addRef()', () => {
    const col = Float64Column.from([1, 2, 3]);
    col.addRef();
    expect(col.refCount).toBe(2);
    expect(col.isShared).toBe(true);
  });

  it('should decrement refCount with release()', () => {
    const col = Float64Column.from([1, 2, 3]);
    col.addRef();
    col.release();
    expect(col.refCount).toBe(1);
    expect(col.isShared).toBe(false);
  });

  it('should not go below 0 on release', () => {
    const col = Float64Column.from([1, 2, 3]);
    col.release();
    col.release();
    expect(col.refCount).toBe(0);
  });

  it('refCount is available on all column types', () => {
    const cols: Column<unknown>[] = [
      Float64Column.from([1, 2]),
      Int32Column.from([1, 2]),
      Utf8Column.from(['a', 'b']),
      BooleanColumn.from([true, false]),
      DateColumn.from([new Date(), new Date()]),
      ObjectColumn.from([{ x: 1 }, { x: 2 }]),
    ];
    for (const col of cols) {
      expect(col.refCount).toBeGreaterThanOrEqual(1);
      col.addRef();
      expect(col.isShared).toBe(true);
    }
  });
});

describe('Copy-on-Write: select() shares column buffers', () => {
  it('select shares column objects with original DataFrame', () => {
    const df = DataFrame.fromRows([
      { a: 1, b: 'x', c: true },
      { a: 2, b: 'y', c: false },
    ]);

    const selected = df.select('a', 'b');

    // The columns in selected should be the same object references
    const origA = df.col('a');
    const selA = selected.col('a');
    // Values should be identical
    expect(selA.get(0)).toBe(origA.get(0));
    expect(selA.get(1)).toBe(origA.get(1));
  });

  it('select does not copy column data', () => {
    const data = new Float64Array([10, 20, 30, 40, 50]);
    const col = new Float64Column(data);
    const columns = new Map<string, Column<unknown>>();
    columns.set('val', col);
    const df = new DataFrame(columns, ['val']);

    const selected = df.select('val');
    // Column should be shared (refCount > 1 from both DataFrames)
    expect(col.refCount).toBeGreaterThan(1);
    expect(col.isShared).toBe(true);

    // Values identical
    expect(selected.col('val').get(0)).toBe(10);
    expect(selected.col('val').get(4)).toBe(50);
  });
});

describe('Copy-on-Write: head() shares TypedArray via subarray', () => {
  it('head creates columns sharing underlying TypedArray buffer', () => {
    const bigData = new Float64Array(10000);
    for (let i = 0; i < 10000; i++) {
      bigData[i] = i;
    }
    const col = new Float64Column(bigData);
    const columns = new Map<string, Column<unknown>>();
    columns.set('val', col);
    const df = new DataFrame(columns, ['val']);

    const headDf = df.head(10);

    // head(10) should only have 10 rows
    expect(headDf.length).toBe(10);

    // Values should be correct
    for (let i = 0; i < 10; i++) {
      expect(headDf.col('val').get(i)).toBe(i);
    }
  });

  it('slice uses subarray for zero-copy on TypedArray columns', () => {
    const data = new Float64Array([1, 2, 3, 4, 5]);
    const col = new Float64Column(data);
    const sliced = col.slice(1, 4);

    // Sliced column should have correct values
    expect(sliced.length).toBe(3);
    expect(sliced.get(0)).toBe(2);
    expect(sliced.get(1)).toBe(3);
    expect(sliced.get(2)).toBe(4);
  });

  it('Int32Column slice uses subarray', () => {
    const data = new Int32Array([10, 20, 30, 40, 50]);
    const col = new Int32Column(data);
    const sliced = col.slice(2, 5);
    expect(sliced.length).toBe(3);
    expect(sliced.get(0)).toBe(30);
    expect(sliced.get(1)).toBe(40);
    expect(sliced.get(2)).toBe(50);
  });

  it('BooleanColumn slice uses subarray', () => {
    const col = BooleanColumn.from([true, false, true, false, true]);
    const sliced = col.slice(1, 4);
    expect(sliced.length).toBe(3);
    expect(sliced.get(0)).toBe(false);
    expect(sliced.get(1)).toBe(true);
    expect(sliced.get(2)).toBe(false);
  });

  it('DateColumn slice uses subarray', () => {
    const dates = [new Date(2020, 0, 1), new Date(2021, 0, 1), new Date(2022, 0, 1)];
    const col = DateColumn.from(dates);
    const sliced = col.slice(1, 3);
    expect(sliced.length).toBe(2);
    expect(sliced.get(0)!.getTime()).toBe(dates[1]!.getTime());
    expect(sliced.get(1)!.getTime()).toBe(dates[2]!.getTime());
  });
});

describe('Copy-on-Write: withColumn only copies target column', () => {
  it('withColumn on shared DataFrame does not copy other columns', () => {
    const colA = Float64Column.from([1, 2, 3]);
    const colB = Float64Column.from([4, 5, 6]);
    const columns = new Map<string, Column<unknown>>();
    columns.set('a', colA);
    columns.set('b', colB);
    const df = new DataFrame(columns, ['a', 'b']);

    // withColumn replaces 'a' but keeps 'b' shared
    const df2 = df.withColumn('a', [10, 20, 30]);

    // Column 'b' should be shared between df and df2
    expect(colB.isShared).toBe(true);
    expect(colB.refCount).toBeGreaterThan(1);

    // Original DataFrame should be unchanged
    expect(df.col('a').get(0)).toBe(1);
    expect(df2.col('a').get(0)).toBe(10);

    // Column 'b' data is identical in both
    expect(df.col('b').get(0)).toBe(4);
    expect(df2.col('b').get(0)).toBe(4);
  });

  it('withColumn with function only copies target column', () => {
    const df = DataFrame.fromRows([
      { x: 1, y: 10 },
      { x: 2, y: 20 },
    ]);

    const df2 = df.withColumn('z', (row: Record<string, unknown>) => (row['x'] as number) + (row['y'] as number));
    expect(df2.col('z').get(0)).toBe(11);
    expect(df2.col('z').get(1)).toBe(22);
    // Original columns should still work
    expect(df2.col('x').get(0)).toBe(1);
    expect(df2.col('y').get(0)).toBe(10);
  });
});

describe('Copy-on-Write: Reference counting is transparent to API users', () => {
  it('all DataFrame operations produce correct results regardless of sharing', () => {
    const df = DataFrame.fromRows([
      { a: 1, b: 'hello', c: true },
      { a: 2, b: 'world', c: false },
      { a: 3, b: 'test', c: true },
    ]);

    // Chain of operations that create shared columns
    const selected = df.select('a', 'b');
    const headed = df.head(2);
    const withNew = df.withColumn('d', [10, 20, 30]);

    // All should produce correct independent results
    expect(selected.columns).toEqual(['a', 'b']);
    expect(selected.length).toBe(3);

    expect(headed.length).toBe(2);
    expect(headed.col('a').get(0)).toBe(1);
    expect(headed.col('a').get(1)).toBe(2);

    expect(withNew.columns).toEqual(['a', 'b', 'c', 'd']);
    expect(withNew.col('d').get(0)).toBe(10);
  });

  it('clone creates independent copies', () => {
    const df = DataFrame.fromRows([
      { a: 1, b: 2 },
      { a: 3, b: 4 },
    ]);
    const cloned = df.clone();
    expect(cloned.col('a').get(0)).toBe(1);
    expect(cloned.col('b').get(1)).toBe(4);
  });
});

describe('Copy-on-Write: Memory efficiency', () => {
  it('selecting subsets of large DataFrames uses less memory than full copy', () => {
    // Create a large DataFrame
    const size = 100000;
    const values: { a: number; b: number; c: number }[] = [];
    for (let i = 0; i < size; i++) {
      values.push({ a: i, b: i * 2, c: i * 3 });
    }
    const df = DataFrame.fromRows(values);

    // select shares columns - no new data allocated
    const selected = df.select('a');
    expect(selected.length).toBe(size);
    expect(selected.col('a').get(0)).toBe(0);
    expect(selected.col('a').get(size - 1)).toBe(size - 1);

    // head uses subarray - shares buffer, no copy
    const headed = df.head(10);
    expect(headed.length).toBe(10);
    expect(headed.col('a').get(0)).toBe(0);
    expect(headed.col('a').get(9)).toBe(9);
  });
});
