import { describe, it, expect } from 'vitest';
import { DataFrame, col, lit } from '../../src/index';

type OrderRow = {
  region: string;
  product: string;
  amount: number;
  price: number;
  quantity: number;
};

describe('DataFrame expression-based operations', () => {
  const df = DataFrame.fromRows<OrderRow>([
    { region: 'Turin', product: 'A', amount: 100, price: 10, quantity: 5 },
    { region: 'Milan', product: 'B', amount: 600, price: 20, quantity: 10 },
    { region: 'Turin', product: 'C', amount: 800, price: 15, quantity: 8 },
    { region: 'Rome', product: 'D', amount: 200, price: 30, quantity: 3 },
    { region: 'Turin', product: 'E', amount: 50, price: 5, quantity: 2 },
  ]);

  describe('filter with expression', () => {
    it('filters rows with simple comparison', () => {
      const result = df.filter(col<number>('amount').gt(100));
      expect(result.length).toBe(3);
      const amounts = result.col('amount').toArray();
      expect(amounts).toEqual([600, 800, 200]);
    });

    it('filters rows with combined conditions', () => {
      const result = df.filter(
        col<string>('region').eq('Turin').and(col<number>('amount').gt(500)),
      );
      expect(result.length).toBe(1);
      expect(result.row(0)).toEqual({
        region: 'Turin',
        product: 'C',
        amount: 800,
        price: 15,
        quantity: 8,
      });
    });

    it('returns empty DataFrame when no rows match', () => {
      const result = df.filter(col<number>('amount').gt(10000));
      expect(result.length).toBe(0);
      expect(result.columns).toEqual(['region', 'product', 'amount', 'price', 'quantity']);
    });

    it('still supports predicate function filter', () => {
      const result = df.filter((row) => row.amount > 100);
      expect(result.length).toBe(3);
    });
  });

  describe('withColumn with expression', () => {
    it('adds computed column using expressions', () => {
      const result = df.withColumn('revenue', col<number>('price').mul(col<number>('quantity')));
      expect(result.columns).toContain('revenue');
      const revenues = result.col('revenue' as never).toArray();
      expect(revenues).toEqual([50, 200, 120, 90, 10]);
    });

    it('adds column using arithmetic with literal', () => {
      const result = df.withColumn('doubled', col<number>('amount').mul(2));
      const doubled = result.col('doubled' as never).toArray();
      expect(doubled).toEqual([200, 1200, 1600, 400, 100]);
    });

    it('replaces existing column with expression', () => {
      const result = df.withColumn('amount', col<number>('amount').add(lit(1000)));
      expect(result.columns.length).toBe(df.columns.length);
      expect(result.col('amount').toArray()).toEqual([1100, 1600, 1800, 1200, 1050]);
    });

    it('still supports array-based withColumn', () => {
      const result = df.withColumn('flag', [true, false, true, false, true]);
      expect(result.col('flag' as never).toArray()).toEqual([true, false, true, false, true]);
    });

    it('still supports function-based withColumn', () => {
      const result = df.withColumn('label', (row) => `${row.region}-${row.product}`);
      expect(result.col('label' as never).get(0)).toBe('Turin-A');
    });
  });

  describe('where shorthand', () => {
    it('filters with >= operator', () => {
      const result = df.where('amount', '>=', 200);
      expect(result.length).toBe(3);
      const amounts = result.col('amount').toArray();
      expect(amounts).toEqual([600, 800, 200]);
    });

    it('filters with = operator on strings', () => {
      const result = df.where('region', '=', 'Turin');
      expect(result.length).toBe(3);
    });

    it('filters with != operator', () => {
      const result = df.where('region', '!=', 'Turin');
      expect(result.length).toBe(2);
    });

    it('filters with < operator', () => {
      const result = df.where('amount', '<', 200);
      expect(result.length).toBe(2);
    });

    it('filters with <= operator', () => {
      const result = df.where('amount', '<=', 200);
      expect(result.length).toBe(3);
    });

    it('filters with > operator', () => {
      const result = df.where('amount', '>', 200);
      expect(result.length).toBe(2);
    });
  });
});
