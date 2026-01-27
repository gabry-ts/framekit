import { describe, it, expect } from 'vitest';
import { DataFrame, Series, Utf8Column } from '../../../src';

describe('Series.valueCounts', () => {
  it('returns DataFrame with value and count columns sorted by count desc', () => {
    const df = DataFrame.fromColumns({
      fruit: ['apple', 'banana', 'apple', 'cherry', 'banana', 'apple'],
    });
    const result = df.col('fruit').valueCounts();

    expect(result.columns).toEqual(['value', 'count']);
    expect(result.length).toBe(3);

    const rows = result.toArray();
    // apple appears 3 times, banana 2, cherry 1
    expect(rows[0]).toEqual({ value: 'apple', count: 3 });
    expect(rows[1]).toEqual({ value: 'banana', count: 2 });
    expect(rows[2]).toEqual({ value: 'cherry', count: 1 });
  });

  it('handles numeric series', () => {
    const df = DataFrame.fromColumns({
      nums: [1, 2, 2, 3, 3, 3],
    });
    const result = df.col('nums').valueCounts();

    expect(result.length).toBe(3);
    const rows = result.toArray();
    expect(rows[0]).toEqual({ value: 3, count: 3 });
    expect(rows[1]).toEqual({ value: 2, count: 2 });
    expect(rows[2]).toEqual({ value: 1, count: 1 });
  });

  it('handles nulls in series', () => {
    const col = Utf8Column.from(['a', null, 'a', null, 'b']);
    const series = new Series<string>('x', col);
    const result = series.valueCounts();

    expect(result.length).toBe(3);
    const rows = result.toArray();
    // a=2, null=2, b=1 — a and null both have count 2, order among ties is insertion order
    expect(rows[0]!.count).toBe(2);
    expect(rows[1]!.count).toBe(2);
    expect(rows[2]).toEqual({ value: 'b', count: 1 });
  });
});

describe('DataFrame.explode', () => {
  it('expands array column into multiple rows', () => {
    const df = DataFrame.fromColumns({
      name: ['Alice', 'Bob'],
      tags: [['a', 'b'], ['c']],
    });
    const result = df.explode('tags');

    expect(result.length).toBe(3);
    expect(result.toArray()).toEqual([
      { name: 'Alice', tags: 'a' },
      { name: 'Alice', tags: 'b' },
      { name: 'Bob', tags: 'c' },
    ]);
  });

  it('handles null arrays by producing a row with null value', () => {
    const df = DataFrame.fromColumns({
      name: ['Alice', 'Bob'],
      tags: [['a', 'b'], null],
    });
    const result = df.explode('tags');

    expect(result.length).toBe(3);
    const rows = result.toArray();
    expect(rows[0]).toEqual({ name: 'Alice', tags: 'a' });
    expect(rows[1]).toEqual({ name: 'Alice', tags: 'b' });
    expect(rows[2]).toEqual({ name: 'Bob', tags: null });
  });

  it('handles empty arrays by removing the row', () => {
    const df = DataFrame.fromColumns({
      name: ['Alice', 'Bob', 'Carol'],
      tags: [['a'], [], ['c']],
    });
    const result = df.explode('tags');

    expect(result.length).toBe(2);
    const rows = result.toArray();
    expect(rows[0]).toEqual({ name: 'Alice', tags: 'a' });
    expect(rows[1]).toEqual({ name: 'Carol', tags: 'c' });
  });

  it('throws on non-existent column', () => {
    const df = DataFrame.fromColumns({ a: [1, 2] });
    expect(() => df.explode('nonexistent')).toThrow();
  });
});
