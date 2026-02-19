import { describe, it, expect } from 'vitest';
import { DataFrame } from '../../../src/index';
import { rollup } from '../../../src/compat/verbs';
import { op } from '../../../src/compat/helpers';

describe('compat rollup', () => {
  it('produces single-row aggregation result', () => {
    const df = DataFrame.fromColumns({ amount: [10, 20, 30] });
    const result = rollup(df, { total: (d) => op.sum(d.amount!) });

    expect(result.length).toBe(1);
    expect(result.columns).toEqual(['total']);
    expect(result.col('total').toArray()).toEqual([60]);
  });

  it('supports multiple aggregation columns', () => {
    const df = DataFrame.fromColumns({ x: [1, 2, 3, 4] });
    const result = rollup(df, {
      total: (d) => op.sum(d.x!),
      avg: (d) => op.mean(d.x!),
      n: (d) => op.count(d.x!),
      lo: (d) => op.min(d.x!),
      hi: (d) => op.max(d.x!),
    });

    expect(result.length).toBe(1);
    expect(result.col('total').toArray()).toEqual([10]);
    expect(result.col('avg').toArray()).toEqual([2.5]);
    expect(result.col('n').toArray()).toEqual([4]);
    expect(result.col('lo').toArray()).toEqual([1]);
    expect(result.col('hi').toArray()).toEqual([4]);
  });

  it('works with grouped DataFrames', () => {
    const df = DataFrame.fromColumns({
      category: ['a', 'a', 'b', 'b'],
      amount: [10, 20, 30, 40],
    });
    const grouped = df.groupBy('category');
    const result = rollup(grouped, { total: (d) => op.sum(d.amount!) });

    expect(result.length).toBe(2);
    expect(result.columns).toEqual(['category', 'total']);

    // Sort by category to ensure deterministic assertion
    const rows = result.toArray() as Array<{ category: string; total: number }>;
    rows.sort((a, b) => a.category.localeCompare(b.category));
    expect(rows).toEqual([
      { category: 'a', total: 30 },
      { category: 'b', total: 70 },
    ]);
  });

  it('handles null values consistently', () => {
    const df = DataFrame.fromColumns({ x: [1, null, 3, null, 5] });
    const result = rollup(df, {
      total: (d) => op.sum(d.x!),
      avg: (d) => op.mean(d.x!),
      n: (d) => op.count(d.x!),
    });

    expect(result.col('total').toArray()).toEqual([9]);
    expect(result.col('avg').toArray()).toEqual([3]);
    expect(result.col('n').toArray()).toEqual([3]);
  });

  it('handles all-null columns', () => {
    const df = DataFrame.fromColumns({ x: [null, null, null] });
    const result = rollup(df, {
      total: (d) => op.sum(d.x!),
      avg: (d) => op.mean(d.x!),
      lo: (d) => op.min(d.x!),
    });

    expect(result.col('total').toArray()).toEqual([0]);
    expect(result.col('avg').toArray()).toEqual([null]);
    expect(result.col('lo').toArray()).toEqual([null]);
  });

  it('grouped rollup with multiple keys', () => {
    const df = DataFrame.fromColumns({
      region: ['east', 'east', 'west', 'west'],
      type: ['a', 'b', 'a', 'b'],
      value: [10, 20, 30, 40],
    });
    const grouped = df.groupBy('region', 'type');
    const result = rollup(grouped, { total: (d) => op.sum(d.value!) });

    expect(result.length).toBe(4);
    expect(result.columns).toEqual(['region', 'type', 'total']);
  });

  it('deterministic output column order', () => {
    const df = DataFrame.fromColumns({ a: [1, 2], b: [3, 4] });
    const result = rollup(df, {
      z_sum: (d) => op.sum(d.a!),
      a_sum: (d) => op.sum(d.b!),
    });

    expect(result.columns).toEqual(['z_sum', 'a_sum']);
  });

  it('works with empty DataFrame', () => {
    const df = DataFrame.fromColumns({ x: [] as number[] });
    const result = rollup(df, { total: (d) => op.sum(d.x!) });

    expect(result.length).toBe(1);
    expect(result.col('total').toArray()).toEqual([0]);
  });

  it('grouped rollup with nulls in values', () => {
    const df = DataFrame.fromColumns({
      cat: ['a', 'a', 'b', 'b'],
      val: [10, null, null, 40],
    });
    const grouped = df.groupBy('cat');
    const result = rollup(grouped, {
      total: (d) => op.sum(d.val!),
      avg: (d) => op.mean(d.val!),
    });

    const rows = result.toArray() as Array<{ cat: string; total: number; avg: number | null }>;
    rows.sort((a, b) => a.cat.localeCompare(b.cat));
    expect(rows).toEqual([
      { cat: 'a', total: 10, avg: 10 },
      { cat: 'b', total: 40, avg: 40 },
    ]);
  });
});
