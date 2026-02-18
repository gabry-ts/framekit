import { describe, it, expect } from 'vitest';
import { DataFrame, col, lit } from '../../../src/index';

type NullRow = {
  name: string;
  nickname: string;
  amount: number;
};

const df = DataFrame.fromRows<NullRow>([
  { name: 'Alice', nickname: 'Ali', amount: 100 },
  { name: null as unknown as string, nickname: 'Bobby', amount: null as unknown as number },
  { name: 'Charlie', nickname: null as unknown as string, amount: 300 },
  { name: null as unknown as string, nickname: null as unknown as string, amount: null as unknown as number },
  { name: 'Eve', nickname: 'Evie', amount: 500 },
]);

describe('Null handling expressions', () => {
  describe('coalesce()', () => {
    it('returns first non-null value from multiple expressions', () => {
      const result = col<string>('name')
        .coalesce(col<string>('nickname'), lit('Anonymous'))
        .evaluate(df);
      expect(result.toArray()).toEqual(['Alice', 'Bobby', 'Charlie', 'Anonymous', 'Eve']);
    });

    it('returns null when all values are null', () => {
      const result = col<string>('name')
        .coalesce(col<string>('nickname'))
        .evaluate(df);
      // Row 3 has null for both name and nickname
      expect(result.get(3)).toBeNull();
    });

    it('picks the first non-null from first expr', () => {
      const result = col<string>('name')
        .coalesce(lit('fallback'))
        .evaluate(df);
      expect(result.toArray()).toEqual(['Alice', 'fallback', 'Charlie', 'fallback', 'Eve']);
    });
  });

  describe('fillNull()', () => {
    it('replaces nulls with literal value via expression', () => {
      const result = col<number>('amount')
        .fillNull(lit(0))
        .evaluate(df);
      expect(result.toArray()).toEqual([100, 0, 300, 0, 500]);
    });

    it('replaces nulls with raw value', () => {
      const result = col<string>('name')
        .fillNull('Unknown')
        .evaluate(df);
      expect(result.toArray()).toEqual(['Alice', 'Unknown', 'Charlie', 'Unknown', 'Eve']);
    });

    it('keeps non-null values unchanged', () => {
      const result = col<number>('amount')
        .fillNull(lit(-1))
        .evaluate(df);
      expect(result.get(0)).toBe(100);
      expect(result.get(2)).toBe(300);
      expect(result.get(4)).toBe(500);
    });
  });

  describe('isNull()', () => {
    it('returns boolean expression for null check', () => {
      const result = col('amount').isNull().evaluate(df);
      expect(result.toArray()).toEqual([false, true, false, true, false]);
    });

    it('works on string column', () => {
      const result = col('name').isNull().evaluate(df);
      expect(result.toArray()).toEqual([false, true, false, true, false]);
    });
  });

  describe('isNotNull()', () => {
    it('returns boolean expression for non-null check', () => {
      const result = col('amount').isNotNull().evaluate(df);
      expect(result.toArray()).toEqual([true, false, true, false, true]);
    });

    it('works on string column', () => {
      const result = col('name').isNotNull().evaluate(df);
      expect(result.toArray()).toEqual([true, false, true, false, true]);
    });
  });

  describe('lazy mode', () => {
    it('coalesce works in lazy mode via withColumn', () => {
      const result = df
        .withColumn('display', col<string>('name').coalesce(col<string>('nickname'), lit('Anonymous')));
      expect(result.col('display').toArray()).toEqual(['Alice', 'Bobby', 'Charlie', 'Anonymous', 'Eve']);
    });

    it('fillNull works in lazy mode via withColumn', () => {
      const result = df
        .withColumn('filled', col<number>('amount').fillNull(lit(0)));
      expect(result.col('filled').toArray()).toEqual([100, 0, 300, 0, 500]);
    });

    it('isNull works in filter expression', () => {
      const result = df.filter(col('amount').isNull());
      expect(result.length).toBe(2);
    });

    it('isNotNull works in filter expression', () => {
      const result = df.filter(col('amount').isNotNull());
      expect(result.length).toBe(3);
      expect(result.col('amount').toArray()).toEqual([100, 300, 500]);
    });

    it('works with LazyFrame collect', async () => {
      const lazy = df.lazy();
      const result = await lazy
        .filter(col('amount').isNotNull())
        .collect();
      expect(result.length).toBe(3);
      expect(result.col('amount').toArray()).toEqual([100, 300, 500]);
    });
  });

  describe('negative differences', () => {
    it('coalesce with all non-null returns first value', () => {
      const result = col<number>('amount')
        .coalesce(lit(999))
        .evaluate(df);
      expect(result.get(0)).toBe(100);
      expect(result.get(2)).toBe(300);
    });
  });
});
