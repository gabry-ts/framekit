import { describe, it, expect } from 'vitest';
import { Series } from '../../src/series';
import { Float64Column } from '../../src/storage/numeric';
import { Utf8Column } from '../../src/storage/string';

describe('Series numeric utility methods (US-087)', () => {
  describe('between()', () => {
    it('returns true for values in range (inclusive)', () => {
      const s = new Series<number>('x', Float64Column.from([1, 5, 10, 15, 20]));
      const result = s.between(5, 15);
      expect(result.toArray()).toEqual([false, true, true, true, false]);
    });

    it('handles null values', () => {
      const s = new Series<number>('x', Float64Column.from([1, null, 10, null, 20]));
      const result = s.between(5, 15);
      expect(result.toArray()).toEqual([false, null, true, null, false]);
    });

    it('handles empty series', () => {
      const s = new Series<number>('x', Float64Column.from([]));
      const result = s.between(0, 10);
      expect(result.toArray()).toEqual([]);
    });

    it('throws on non-numeric series', () => {
      const s = new Series<string>('x', Utf8Column.from(['a', 'b']));
      expect(() => (s as unknown as Series<number>).between(0, 10)).toThrow();
    });
  });

  describe('cumSum()', () => {
    it('returns running sum', () => {
      const s = new Series<number>('x', Float64Column.from([1, 2, 3, 4]));
      const result = s.cumSum();
      expect(result.toArray()).toEqual([1, 3, 6, 10]);
    });

    it('skips nulls in accumulation', () => {
      const s = new Series<number>('x', Float64Column.from([1, null, 3, 4]));
      const result = s.cumSum();
      expect(result.toArray()).toEqual([1, null, 4, 8]);
    });

    it('handles empty series', () => {
      const s = new Series<number>('x', Float64Column.from([]));
      expect(s.cumSum().toArray()).toEqual([]);
    });

    it('throws on non-numeric series', () => {
      const s = new Series<string>('x', Utf8Column.from(['a']));
      expect(() => (s as unknown as Series<number>).cumSum()).toThrow();
    });
  });

  describe('abs()', () => {
    it('returns absolute values', () => {
      const s = new Series<number>('x', Float64Column.from([-3, -1, 0, 2, 5]));
      const result = s.abs();
      expect(result.toArray()).toEqual([3, 1, 0, 2, 5]);
    });

    it('handles null values', () => {
      const s = new Series<number>('x', Float64Column.from([-3, null, 5]));
      const result = s.abs();
      expect(result.toArray()).toEqual([3, null, 5]);
    });

    it('handles empty series', () => {
      const s = new Series<number>('x', Float64Column.from([]));
      expect(s.abs().toArray()).toEqual([]);
    });

    it('throws on non-numeric series', () => {
      const s = new Series<string>('x', Utf8Column.from(['a']));
      expect(() => (s as unknown as Series<number>).abs()).toThrow();
    });
  });

  describe('round()', () => {
    it('rounds to specified decimal places', () => {
      const s = new Series<number>('x', Float64Column.from([1.456, 2.789, 3.123]));
      const result = s.round(2);
      expect(result.toArray()).toEqual([1.46, 2.79, 3.12]);
    });

    it('defaults to 0 decimal places', () => {
      const s = new Series<number>('x', Float64Column.from([1.5, 2.3, 3.7]));
      const result = s.round();
      expect(result.toArray()).toEqual([2, 2, 4]);
    });

    it('handles null values', () => {
      const s = new Series<number>('x', Float64Column.from([1.456, null, 3.789]));
      const result = s.round(1);
      expect(result.toArray()).toEqual([1.5, null, 3.8]);
    });

    it('handles empty series', () => {
      const s = new Series<number>('x', Float64Column.from([]));
      expect(s.round(2).toArray()).toEqual([]);
    });

    it('throws on non-numeric series', () => {
      const s = new Series<string>('x', Utf8Column.from(['a']));
      expect(() => (s as unknown as Series<number>).round()).toThrow();
    });
  });
});
