import { describe, it, expect } from 'vitest';
import { DataFrame } from '../../../src/index';

describe('DataFrame.apply()', () => {
  it('applies function to each row returning new DataFrame', () => {
    const df = DataFrame.fromRows<{ name: string; age: number }>([
      { name: 'Alice', age: 30 },
      { name: 'Bob', age: 25 },
    ]);

    const result = df.apply((row) => ({
      name: row.name.toUpperCase(),
      age: row.age + 1,
    }));

    expect(result.length).toBe(2);
    expect(result.row(0)).toEqual({ name: 'ALICE', age: 31 });
    expect(result.row(1)).toEqual({ name: 'BOB', age: 26 });
  });

  it('preserves schema type S', () => {
    const df = DataFrame.fromRows<{ x: number; y: number }>([
      { x: 1, y: 2 },
      { x: 3, y: 4 },
    ]);

    const result = df.apply((row) => ({
      x: row.x * 2,
      y: row.y * 2,
    }));

    expect(result.columns).toEqual(['x', 'y']);
    expect(result.row(0)).toEqual({ x: 2, y: 4 });
    expect(result.row(1)).toEqual({ x: 6, y: 8 });
  });

  it('handles empty DataFrames correctly', () => {
    const df = DataFrame.fromRows<{ a: number }>([]);
    const result = df.apply((row) => ({ a: row.a + 1 }));
    expect(result.length).toBe(0);
  });
});

describe('GroupBy.apply()', () => {
  it('applies function to each group returning combined DataFrame', () => {
    const df = DataFrame.fromRows<{ group: string; value: number }>([
      { group: 'A', value: 1 },
      { group: 'A', value: 2 },
      { group: 'B', value: 10 },
      { group: 'B', value: 20 },
    ]);

    const result = df.groupBy('group').apply((group) => {
      // Double all values within each group
      return group.apply((row) => ({
        group: row.group,
        value: row.value * 2,
      }));
    });

    expect(result.length).toBe(4);

    // Collect results by group
    const rows = result.toArray();
    const groupA = rows.filter((r) => r.group === 'A');
    const groupB = rows.filter((r) => r.group === 'B');

    expect(groupA.map((r) => r.value).sort((a, b) => a - b)).toEqual([2, 4]);
    expect(groupB.map((r) => r.value).sort((a, b) => a - b)).toEqual([20, 40]);
  });

  it('handles single-element groups', () => {
    const df = DataFrame.fromRows<{ group: string; value: number }>([
      { group: 'A', value: 1 },
      { group: 'B', value: 2 },
    ]);
    const result = df.groupBy('group').apply((group) => group);
    expect(result.length).toBe(2);
  });
});
