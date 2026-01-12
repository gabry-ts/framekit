import { describe, it, expect } from 'vitest';
import { DataFrame } from '../../src/dataframe';

type Row = { name: string; age: number; active: boolean };

function makeDF(): DataFrame<Row> {
  return DataFrame.fromRows<Row>([
    { name: 'Alice', age: 30, active: true },
    { name: 'Bob', age: 25, active: false },
    { name: 'Charlie', age: 35, active: true },
  ]);
}

describe('DataFrame iterator and rows access (US-010)', () => {
  describe('Symbol.iterator', () => {
    it('supports for...of iteration', () => {
      const df = makeDF();
      const rows: Row[] = [];
      for (const row of df) {
        rows.push(row);
      }
      expect(rows).toHaveLength(3);
      expect(rows[0]).toEqual({ name: 'Alice', age: 30, active: true });
      expect(rows[1]).toEqual({ name: 'Bob', age: 25, active: false });
      expect(rows[2]).toEqual({ name: 'Charlie', age: 35, active: true });
    });

    it('yields plain objects matching schema S', () => {
      const df = makeDF();
      const [first] = df;
      expect(first).toBeDefined();
      expect(typeof first!.name).toBe('string');
      expect(typeof first!.age).toBe('number');
      expect(typeof first!.active).toBe('boolean');
    });

    it('works with spread operator', () => {
      const df = makeDF();
      const rows = [...df];
      expect(rows).toHaveLength(3);
    });

    it('works with Array.from', () => {
      const df = makeDF();
      const rows = Array.from(df);
      expect(rows).toHaveLength(3);
      expect(rows[0]).toEqual({ name: 'Alice', age: 30, active: true });
    });

    it('handles empty DataFrame', () => {
      const df = DataFrame.empty();
      const rows = [...df];
      expect(rows).toHaveLength(0);
    });
  });

  describe('rows()', () => {
    it('returns an iterator over row objects', () => {
      const df = makeDF();
      const iter = df.rows();
      const first = iter.next();
      expect(first.done).toBe(false);
      expect(first.value).toEqual({ name: 'Alice', age: 30, active: true });

      const second = iter.next();
      expect(second.done).toBe(false);
      expect(second.value).toEqual({ name: 'Bob', age: 25, active: false });

      const third = iter.next();
      expect(third.done).toBe(false);
      expect(third.value).toEqual({ name: 'Charlie', age: 35, active: true });

      const done = iter.next();
      expect(done.done).toBe(true);
    });

    it('returns independent iterators on multiple calls', () => {
      const df = makeDF();
      const iter1 = df.rows();
      const iter2 = df.rows();
      iter1.next(); // advance iter1
      const second = iter2.next(); // iter2 still at start
      expect(second.value).toEqual({ name: 'Alice', age: 30, active: true });
    });
  });

  describe('row() out-of-bounds', () => {
    it('throws descriptive error for positive out-of-bounds index', () => {
      const df = makeDF();
      expect(() => df.row(3)).toThrow(/out of bounds/i);
      expect(() => df.row(100)).toThrow(/out of bounds/i);
    });

    it('throws descriptive error for negative index', () => {
      const df = makeDF();
      expect(() => df.row(-1)).toThrow(/out of bounds/i);
    });

    it('includes row count in error message', () => {
      const df = makeDF();
      expect(() => df.row(5)).toThrow(/3 rows/);
    });
  });

  describe('toArray()', () => {
    it('returns S[] array of all row objects', () => {
      const df = makeDF();
      const arr = df.toArray();
      expect(arr).toHaveLength(3);
      expect(arr[0]).toEqual({ name: 'Alice', age: 30, active: true });
      expect(arr[1]).toEqual({ name: 'Bob', age: 25, active: false });
      expect(arr[2]).toEqual({ name: 'Charlie', age: 35, active: true });
    });

    it('returns a plain array', () => {
      const df = makeDF();
      const arr = df.toArray();
      expect(Array.isArray(arr)).toBe(true);
    });

    it('returns empty array for empty DataFrame', () => {
      const df = DataFrame.empty();
      expect(df.toArray()).toEqual([]);
    });

    it('returns independent copies on multiple calls', () => {
      const df = makeDF();
      const arr1 = df.toArray();
      const arr2 = df.toArray();
      expect(arr1).toEqual(arr2);
      expect(arr1).not.toBe(arr2);
    });
  });
});
