import { describe, it, expect } from 'vitest';
import { DataFrame } from '../../../src/dataframe';
import { ShapeMismatchError } from '../../../src/errors';

describe('transpose', () => {
  it('should swap rows and columns', () => {
    const df = DataFrame.fromRows([
      { name: 'Alice', age: 30, score: 85 },
      { name: 'Bob', age: 25, score: 90 },
    ]);

    const result = df.transpose('name');
    expect(result.columns).toEqual(['column', 'Alice', 'Bob']);
    expect(result.length).toBe(2); // age and score rows
    expect(result.col('column').toArray()).toEqual(['age', 'score']);
    expect(result.col('Alice').toArray()).toEqual([30, 85]);
    expect(result.col('Bob').toArray()).toEqual([25, 90]);
  });

  it('should use row indices as headers when no headerColumn specified', () => {
    const df = DataFrame.fromRows([
      { a: 1, b: 2 },
      { a: 3, b: 4 },
    ]);

    const result = df.transpose();
    expect(result.columns).toEqual(['column', '0', '1']);
    expect(result.col('column').toArray()).toEqual(['a', 'b']);
    expect(result.col('0').toArray()).toEqual([1, 2]);
    expect(result.col('1').toArray()).toEqual([3, 4]);
  });

  it('should handle single row', () => {
    const df = DataFrame.fromRows([{ x: 10, y: 20, z: 30 }]);
    const result = df.transpose();
    expect(result.columns).toEqual(['column', '0']);
    expect(result.col('column').toArray()).toEqual(['x', 'y', 'z']);
    expect(result.col('0').toArray()).toEqual([10, 20, 30]);
  });

  it('should handle single column', () => {
    const df = DataFrame.fromRows([{ a: 1 }, { a: 2 }, { a: 3 }]);
    const result = df.transpose();
    expect(result.columns).toEqual(['column', '0', '1', '2']);
    expect(result.col('column').toArray()).toEqual(['a']);
    expect(result.col('0').toArray()).toEqual([1]);
    expect(result.col('1').toArray()).toEqual([2]);
    expect(result.col('2').toArray()).toEqual([3]);
  });

  it('should handle null values in header column', () => {
    const df = DataFrame.fromRows([
      { name: 'Alice', val: 1 },
      { name: null, val: 2 },
    ]);

    const result = df.transpose('name');
    expect(result.columns).toEqual(['column', 'Alice', 'null']);
    expect(result.col('Alice').toArray()).toEqual([1]);
    expect(result.col('null').toArray()).toEqual([2]);
  });

  it('should handle empty DataFrame', () => {
    const df = DataFrame.empty();
    const result = df.transpose();
    expect(result.length).toBe(0);
    expect(result.columns.length).toBe(0);
  });
});

describe('DataFrame.concat', () => {
  it('should stack DataFrames vertically with matching columns', () => {
    const df1 = DataFrame.fromRows([
      { a: 1, b: 'x' },
      { a: 2, b: 'y' },
    ]);
    const df2 = DataFrame.fromRows([
      { a: 3, b: 'z' },
    ]);

    const result = DataFrame.concat(df1, df2);
    expect(result.length).toBe(3);
    expect(result.col('a').toArray()).toEqual([1, 2, 3]);
    expect(result.col('b').toArray()).toEqual(['x', 'y', 'z']);
  });

  it('should concat three DataFrames', () => {
    const df1 = DataFrame.fromRows([{ x: 1 }]);
    const df2 = DataFrame.fromRows([{ x: 2 }]);
    const df3 = DataFrame.fromRows([{ x: 3 }]);

    const result = DataFrame.concat(df1, df2, df3);
    expect(result.length).toBe(3);
    expect(result.col('x').toArray()).toEqual([1, 2, 3]);
  });

  it('should fill null for missing columns', () => {
    const df1 = DataFrame.fromRows([{ a: 1, b: 2 }]);
    const df2 = DataFrame.fromRows([{ a: 3, c: 4 }]);

    const result = DataFrame.concat(df1, df2);
    expect(result.columns).toEqual(['a', 'b', 'c']);
    expect(result.length).toBe(2);
    expect(result.col('a').toArray()).toEqual([1, 3]);
    expect(result.col('b').toArray()).toEqual([2, null]);
    expect(result.col('c').toArray()).toEqual([null, 4]);
  });

  it('should throw on incompatible column types', () => {
    const df1 = DataFrame.fromRows([{ a: 1 }]);
    const df2 = DataFrame.fromRows([{ a: 'hello' }]);

    expect(() => DataFrame.concat(df1, df2)).toThrow(ShapeMismatchError);
  });

  it('should handle single DataFrame', () => {
    const df = DataFrame.fromRows([{ x: 1, y: 2 }]);
    const result = DataFrame.concat(df);
    expect(result.length).toBe(1);
    expect(result.col('x').toArray()).toEqual([1]);
  });

  it('should preserve column order from first DataFrame', () => {
    const df1 = DataFrame.fromRows([{ b: 1, a: 2 }]);
    const df2 = DataFrame.fromRows([{ a: 3, b: 4 }]);

    const result = DataFrame.concat(df1, df2);
    expect(result.columns).toEqual(['b', 'a']);
  });

  it('should handle null values in concatenated data', () => {
    const df1 = DataFrame.fromRows([{ a: 1, b: null }]);
    const df2 = DataFrame.fromRows([{ a: null, b: 2 }]);

    const result = DataFrame.concat(df1, df2);
    expect(result.col('a').toArray()).toEqual([1, null]);
    expect(result.col('b').toArray()).toEqual([null, 2]);
  });

  it('should handle empty DataFrames in concat', () => {
    const df1 = DataFrame.fromRows([{ a: 1 }]);
    const df2 = DataFrame.empty();

    const result = DataFrame.concat(df1, df2);
    expect(result.length).toBe(1);
    expect(result.col('a').toArray()).toEqual([1]);
  });
});
