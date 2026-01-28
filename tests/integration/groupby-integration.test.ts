import { describe, it, expect } from 'vitest';
import * as path from 'path';
import { DataFrame, col } from '../../src';

const fixturesDir = path.join(__dirname, '../fixtures');

describe('GroupBy Integration Tests (US-044)', () => {
  let df: DataFrame<{
    date: Date;
    region: string | null;
    product: string;
    amount: number;
    quantity: number;
  }>;

  // Load sales.csv once before tests
  const loadDf = async () => {
    if (!df) {
      df = await DataFrame.fromCSV(path.join(fixturesDir, 'sales.csv'));
    }
    return df;
  };

  describe('fixture sanity', () => {
    it('should load sales.csv with expected shape', async () => {
      const frame = await loadDf();
      expect(frame.shape).toEqual([24, 5]);
      expect(frame.columns).toEqual(['date', 'region', 'product', 'amount', 'quantity']);
    });
  });

  describe('single-key groupBy with sum, mean, count', () => {
    it('should group by product and compute sum', async () => {
      const frame = await loadDf();
      const result = frame.groupBy('product').agg({
        total_amount: col('amount').sum(),
      });
      expect(result.length).toBe(3); // Widget, Gadget, Gizmo

      // Verify all products are present
      const products = result.col('product').toArray();
      expect(products.sort()).toEqual(['Gadget', 'Gizmo', 'Widget']);
    });

    it('should group by product and compute mean', async () => {
      const frame = await loadDf();
      const result = frame.groupBy('product').agg({
        avg_amount: col('amount').mean(),
      });
      expect(result.length).toBe(3);

      // Each group mean should be a reasonable number
      for (let i = 0; i < result.length; i++) {
        const val = result.col('avg_amount').get(i) as number;
        expect(val).toBeGreaterThan(0);
        expect(val).toBeLessThan(500);
      }
    });

    it('should group by product and compute count', async () => {
      const frame = await loadDf();
      const result = frame.groupBy('product').agg({
        n: col('amount').count(),
      });
      expect(result.length).toBe(3);

      // Total rows should sum to 24
      let total = 0;
      for (let i = 0; i < result.length; i++) {
        total += result.col('n').get(i) as number;
      }
      expect(total).toBe(24);
    });
  });

  describe('multi-key groupBy with multiple aggregations', () => {
    it('should group by region and product with multiple agg expressions', async () => {
      const frame = await loadDf();
      const result = frame.groupBy('region', 'product').agg({
        total_amount: col('amount').sum(),
        avg_quantity: col('quantity').mean(),
        n: col('amount').count(),
      });

      // Result should have 5 columns: region, product, total_amount, avg_quantity, n
      expect(result.columns).toEqual(['region', 'product', 'total_amount', 'avg_quantity', 'n']);

      // Every row should have positive values
      for (let i = 0; i < result.length; i++) {
        expect(result.col('total_amount').get(i) as number).toBeGreaterThan(0);
        expect(result.col('avg_quantity').get(i) as number).toBeGreaterThan(0);
        expect(result.col('n').get(i) as number).toBeGreaterThanOrEqual(1);
      }
    });

    it('should produce correct aggregations for known group', async () => {
      const frame = await loadDf();
      const result = frame.groupBy('region', 'product').agg({
        total_amount: col('amount').sum(),
        count: col('amount').count(),
      });

      // Find East+Widget group: rows with region=East, product=Widget
      // From CSV: 150.50, 175.25, 160.00, 195.00 = 680.75, count=4
      let eastWidgetIdx = -1;
      for (let i = 0; i < result.length; i++) {
        if (result.col('region').get(i) === 'East' && result.col('product').get(i) === 'Widget') {
          eastWidgetIdx = i;
          break;
        }
      }
      expect(eastWidgetIdx).not.toBe(-1);
      expect(result.col('total_amount').get(eastWidgetIdx) as number).toBeCloseTo(680.75, 1);
      expect(result.col('count').get(eastWidgetIdx)).toBe(4);
    });
  });

  describe('shorthand aggregations', () => {
    it('groupBy().count() should return row counts per group', async () => {
      const frame = await loadDf();
      const result = frame.groupBy('product').count();

      expect(result.length).toBe(3);
      expect(result.columns).toContain('product');
      expect(result.columns).toContain('count');

      // Total count across groups should equal total rows
      let total = 0;
      for (let i = 0; i < result.length; i++) {
        total += result.col('count').get(i) as number;
      }
      expect(total).toBe(24);
    });

    it("groupBy().sum('amount') should return sum per group", async () => {
      const frame = await loadDf();
      const result = frame.groupBy('region').sum('amount');

      // 4 regions: East, West, North, null
      expect(result.length).toBe(4);
      expect(result.columns).toContain('region');
      expect(result.columns).toContain('amount');

      // All sums should be positive
      for (let i = 0; i < result.length; i++) {
        const val = result.col('amount').get(i) as number;
        expect(val).toBeGreaterThan(0);
      }
    });

    it("groupBy().mean('quantity') should return mean per group", async () => {
      const frame = await loadDf();
      const result = frame.groupBy('product').mean('quantity');

      expect(result.length).toBe(3);
      for (let i = 0; i < result.length; i++) {
        const val = result.col('quantity').get(i) as number;
        expect(val).toBeGreaterThan(0);
      }
    });
  });

  describe('groupBy with null values in key columns', () => {
    it('should create a group for null region values', async () => {
      const frame = await loadDf();
      const result = frame.groupBy('region').count();

      // Should have 4 groups: East, West, North, null
      expect(result.length).toBe(4);

      // Find the null group
      let nullIdx = -1;
      for (let i = 0; i < result.length; i++) {
        if (result.col('region').get(i) === null) {
          nullIdx = i;
          break;
        }
      }
      expect(nullIdx).not.toBe(-1);
      // 3 rows have null region in the CSV
      expect(result.col('count').get(nullIdx)).toBe(3);
    });

    it('should aggregate correctly within the null group', async () => {
      const frame = await loadDf();
      const result = frame.groupBy('region').agg({
        total_amount: col('amount').sum(),
      });

      let nullIdx = -1;
      for (let i = 0; i < result.length; i++) {
        if (result.col('region').get(i) === null) {
          nullIdx = i;
          break;
        }
      }
      expect(nullIdx).not.toBe(-1);
      // Null region rows: 180.00 + 140.00 + 205.00 = 525.00
      expect(result.col('total_amount').get(nullIdx) as number).toBeCloseTo(525.0, 1);
    });
  });

  describe('result schema validation', () => {
    it('should have correct dtypes for key and aggregation columns', async () => {
      const frame = await loadDf();
      const result = frame.groupBy('product').agg({
        total: col('amount').sum(),
        avg: col('amount').mean(),
        n: col('amount').count(),
      });

      // Key column should be utf8
      expect(result.col('product').dtype).toBe('utf8');
      // Aggregation columns should be f64
      expect(result.col('total').dtype).toBe('f64');
      expect(result.col('avg').dtype).toBe('f64');
      expect(result.col('n').dtype).toBe('f64');
    });

    it('should have key columns first, then aggregation columns', async () => {
      const frame = await loadDf();
      const result = frame.groupBy('region', 'product').agg({
        s: col('amount').sum(),
      });
      expect(result.columns[0]).toBe('region');
      expect(result.columns[1]).toBe('product');
      expect(result.columns[2]).toBe('s');
    });
  });
});
