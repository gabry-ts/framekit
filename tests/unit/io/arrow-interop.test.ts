/* eslint-disable @typescript-eslint/no-unsafe-assignment, @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unsafe-call */
import { describe, it, expect, beforeAll } from 'vitest';
import { DataFrame } from '../../../src/dataframe';
import { IOError } from '../../../src/errors';

let arrowAvailable = false;
// eslint-disable-next-line @typescript-eslint/no-explicit-any
let arrow: any;

beforeAll(async () => {
  try {
    arrow = await import('apache-arrow');
    arrowAvailable = true;
  } catch {
    arrowAvailable = false;
  }
});

describe('Arrow interop (US-072)', () => {
  describe('toArrow', () => {
    it('should convert a DataFrame with numeric columns to Arrow Table', async () => {
      if (!arrowAvailable) return;

      const df = DataFrame.fromRows([
        { x: 1, y: 10 },
        { x: 2, y: 20 },
        { x: 3, y: 30 },
      ]);

      const table = await df.toArrow();

      expect(table.numRows).toBe(3);
      expect(table.numCols).toBe(2);
      expect(table.schema.fields.map((f: { name: string }) => f.name)).toEqual(['x', 'y']);
    });

    it('should convert string columns to Utf8 vectors', async () => {
      if (!arrowAvailable) return;

      const df = DataFrame.fromRows([
        { name: 'alice' },
        { name: 'bob' },
        { name: 'charlie' },
      ]);

      const table = await df.toArrow();

      const col = table.getChild('name');
      expect(col.get(0)).toBe('alice');
      expect(col.get(1)).toBe('bob');
      expect(col.get(2)).toBe('charlie');
    });

    it('should handle null values', async () => {
      if (!arrowAvailable) return;

      const df = DataFrame.fromColumns({ val: [1, null, 3] });

      const table = await df.toArrow();

      const col = table.getChild('val');
      expect(col.get(0)).toBe(1);
      expect(col.get(1)).toBeNull();
      expect(col.get(2)).toBe(3);
    });
  });

  describe('fromArrow', () => {
    it('should create a DataFrame from an Arrow Table', () => {
      if (!arrowAvailable) return;

      const table = arrow.tableFromArrays({
        x: new Float64Array([1, 2, 3]),
        y: new Float64Array([10, 20, 30]),
      });

      const df = DataFrame.fromArrow(table);

      expect(df.shape).toEqual([3, 2]);
      expect(df.columns).toEqual(['x', 'y']);
      expect(df.col('x').toArray()).toEqual([1, 2, 3]);
      expect(df.col('y').toArray()).toEqual([10, 20, 30]);
    });

    it('should handle Utf8 vectors', () => {
      if (!arrowAvailable) return;

      const table = arrow.tableFromArrays({
        name: ['alice', 'bob', 'charlie'],
      });

      const df = DataFrame.fromArrow(table);

      expect(df.col('name').toArray()).toEqual(['alice', 'bob', 'charlie']);
    });

    it('should handle Int32 vectors', () => {
      if (!arrowAvailable) return;

      const table = arrow.tableFromArrays({
        count: new Int32Array([10, 20, 30]),
      });

      const df = DataFrame.fromArrow(table);

      expect(df.col('count').toArray()).toEqual([10, 20, 30]);
    });

    it('should throw IOError for invalid input', () => {
      expect(() => DataFrame.fromArrow(null)).toThrow(IOError);
      expect(() => DataFrame.fromArrow({})).toThrow(IOError);
    });
  });

  describe('round-trip', () => {
    it('should round-trip numeric data through Arrow', async () => {
      if (!arrowAvailable) return;

      const original = DataFrame.fromRows([
        { a: 1.1, b: 10 },
        { a: 2.2, b: 20 },
        { a: 3.3, b: 30 },
      ]);

      const table = await original.toArrow();
      const restored = DataFrame.fromArrow(table);

      expect(restored.shape).toEqual(original.shape);
      expect(restored.columns).toEqual(original.columns);
    });

    it('should round-trip string data through Arrow', async () => {
      if (!arrowAvailable) return;

      const original = DataFrame.fromRows([
        { name: 'alice' },
        { name: 'bob' },
      ]);

      const table = await original.toArrow();
      const restored = DataFrame.fromArrow(table);

      expect(restored.col('name').toArray()).toEqual(['alice', 'bob']);
    });
  });
});
