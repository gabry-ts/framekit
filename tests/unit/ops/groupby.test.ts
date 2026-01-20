import { describe, it, expect } from 'vitest';
import { DataFrame, GroupBy, col } from '../../../src';

describe('GroupBy', () => {
  const data = [
    { region: 'East', product: 'A', amount: 100 },
    { region: 'West', product: 'B', amount: 200 },
    { region: 'East', product: 'B', amount: 150 },
    { region: 'West', product: 'A', amount: 300 },
    { region: 'East', product: 'A', amount: 250 },
  ];

  type Row = typeof data[0];
  const df = DataFrame.fromRows<Row>(data);

  describe('single-key groupBy', () => {
    it('returns a GroupBy instance', () => {
      const gb = df.groupBy('region');
      expect(gb).toBeInstanceOf(GroupBy);
    });

    it('nGroups returns number of distinct groups', () => {
      const gb = df.groupBy('region');
      expect(gb.nGroups()).toBe(2);
    });

    it('keys returns the group key columns', () => {
      const gb = df.groupBy('region');
      expect(gb.keys).toEqual(['region']);
    });

    it('groups returns Map of sub-DataFrames', () => {
      const gb = df.groupBy('region');
      const grps = gb.groups();
      expect(grps.size).toBe(2);

      // Find East group
      let eastDf: DataFrame | undefined;
      let westDf: DataFrame | undefined;
      for (const [, subDf] of grps) {
        const firstRegion = subDf.row(0).region;
        if (firstRegion === 'East') eastDf = subDf;
        if (firstRegion === 'West') westDf = subDf;
      }

      expect(eastDf).toBeDefined();
      expect(eastDf!.length).toBe(3);
      expect(westDf).toBeDefined();
      expect(westDf!.length).toBe(2);
    });

    it('sub-DataFrames preserve all columns', () => {
      const gb = df.groupBy('region');
      const grps = gb.groups();
      for (const [, subDf] of grps) {
        expect(subDf.columns).toEqual(df.columns);
      }
    });

    it('sub-DataFrames contain correct rows', () => {
      const gb = df.groupBy('region');
      const grps = gb.groups();

      for (const [, subDf] of grps) {
        const rows = subDf.toArray();
        const region = rows[0]!.region;
        for (const row of rows) {
          expect(row.region).toBe(region);
        }
      }
    });
  });

  describe('multi-key groupBy', () => {
    it('supports grouping by multiple columns', () => {
      const gb = df.groupBy('region', 'product');
      expect(gb.nGroups()).toBe(4); // East-A, East-B, West-A, West-B
    });

    it('keys returns all group key columns', () => {
      const gb = df.groupBy('region', 'product');
      expect(gb.keys).toEqual(['region', 'product']);
    });

    it('groups returns correct sub-DataFrames', () => {
      const gb = df.groupBy('region', 'product');
      const grps = gb.groups();
      expect(grps.size).toBe(4);

      for (const [, subDf] of grps) {
        const rows = subDf.toArray();
        const region = rows[0]!.region;
        const product = rows[0]!.product;
        for (const row of rows) {
          expect(row.region).toBe(region);
          expect(row.product).toBe(product);
        }
      }
    });

    it('East-A group has 2 rows', () => {
      const gb = df.groupBy('region', 'product');
      const grps = gb.groups();

      let eastACount = 0;
      for (const [, subDf] of grps) {
        if (subDf.row(0).region === 'East' && subDf.row(0).product === 'A') {
          eastACount = subDf.length;
        }
      }
      expect(eastACount).toBe(2);
    });
  });

  describe('edge cases', () => {
    it('throws ColumnNotFoundError for invalid column', () => {
      expect(() => df.groupBy('invalid' as never)).toThrow();
    });

    it('groupBy on single-row DataFrame', () => {
      const singleDf = DataFrame.fromRows<Row>([data[0]!]);
      const gb = singleDf.groupBy('region');
      expect(gb.nGroups()).toBe(1);
    });

    it('groupBy where every row is unique', () => {
      const gb = df.groupBy('amount' as never);
      expect(gb.nGroups()).toBe(5);
    });

    it('groupBy with null values in key', () => {
      type NullRow = { region: string | null; amount: number };
      const nullDf = DataFrame.fromRows<NullRow>([
        { region: 'East', amount: 100 },
        { region: null, amount: 200 },
        { region: 'East', amount: 150 },
        { region: null, amount: 300 },
      ]);
      const gb = nullDf.groupBy('region');
      expect(gb.nGroups()).toBe(2); // East and null
    });

    it('groupMap is accessible', () => {
      const gb = df.groupBy('region');
      const map = gb.groupMap;
      expect(map).toBeInstanceOf(Map);
      let totalRows = 0;
      for (const [, indices] of map) {
        totalRows += indices.length;
      }
      expect(totalRows).toBe(df.length);
    });

    it('dataframe getter returns source DataFrame', () => {
      const gb = df.groupBy('region');
      expect(gb.dataframe).toBe(df);
    });
  });

  describe('agg() with expression-based aggregations', () => {
    it('computes sum aggregation per group', () => {
      const result = df.groupBy('region').agg({
        total: col('amount').sum(),
      });
      expect(result.length).toBe(2);
      expect(result.columns).toEqual(['region', 'total']);

      const rows = result.toArray();
      const eastRow = rows.find((r) => r['region'] === 'East');
      const westRow = rows.find((r) => r['region'] === 'West');
      expect(eastRow!['total']).toBe(500); // 100 + 150 + 250
      expect(westRow!['total']).toBe(500); // 200 + 300
    });

    it('computes multiple aggregations per group', () => {
      const result = df.groupBy('region').agg({
        total: col('amount').sum(),
        avg: col('amount').mean(),
      });
      expect(result.length).toBe(2);
      expect(result.columns).toEqual(['region', 'total', 'avg']);

      const rows = result.toArray();
      const eastRow = rows.find((r) => r['region'] === 'East');
      expect(eastRow!['total']).toBe(500);
      expect(eastRow!['avg']).toBeCloseTo(500 / 3);
    });

    it('supports min and max aggregations', () => {
      const result = df.groupBy('region').agg({
        min_amt: col('amount').min(),
        max_amt: col('amount').max(),
      });
      const rows = result.toArray();
      const eastRow = rows.find((r) => r['region'] === 'East');
      expect(eastRow!['min_amt']).toBe(100);
      expect(eastRow!['max_amt']).toBe(250);
    });

    it('supports count aggregation', () => {
      const result = df.groupBy('region').agg({
        n: col('amount').count(),
      });
      const rows = result.toArray();
      const eastRow = rows.find((r) => r['region'] === 'East');
      const westRow = rows.find((r) => r['region'] === 'West');
      expect(eastRow!['n']).toBe(3);
      expect(westRow!['n']).toBe(2);
    });

    it('supports first and last aggregations', () => {
      const result = df.groupBy('region').agg({
        first_amt: col('amount').first(),
        last_amt: col('amount').last(),
      });
      const rows = result.toArray();
      const eastRow = rows.find((r) => r['region'] === 'East');
      expect(eastRow!['first_amt']).toBe(100);
      expect(eastRow!['last_amt']).toBe(250);
    });

    it('result has one row per group', () => {
      const result = df.groupBy('region', 'product').agg({
        total: col('amount').sum(),
      });
      expect(result.length).toBe(4); // East-A, East-B, West-A, West-B
      expect(result.columns).toEqual(['region', 'product', 'total']);
    });

    it('group key columns appear first, then aggregation columns', () => {
      const result = df.groupBy('region').agg({
        z_total: col('amount').sum(),
        a_mean: col('amount').mean(),
      });
      expect(result.columns).toEqual(['region', 'z_total', 'a_mean']);
    });

    it('string shorthand: agg with string method names', () => {
      const result = df.groupBy('region').agg({
        amount: 'sum',
      });
      const rows = result.toArray();
      const eastRow = rows.find((r) => r['region'] === 'East');
      expect(eastRow!['amount']).toBe(500);
    });

    it('string shorthand: supports mean, min, max, count', () => {
      const result = df.groupBy('region').agg({
        amount: 'mean',
      });
      const rows = result.toArray();
      const eastRow = rows.find((r) => r['region'] === 'East');
      expect(eastRow!['amount']).toBeCloseTo(500 / 3);
    });

    it('string shorthand: supports first and last', () => {
      const result = df.groupBy('region').agg({
        amount: 'first',
      });
      const rows = result.toArray();
      const eastRow = rows.find((r) => r['region'] === 'East');
      expect(eastRow!['amount']).toBe(100);
    });

    it('handles null values in aggregation columns', () => {
      type NullRow = { region: string; amount: number | null };
      const nullDf = DataFrame.fromRows<NullRow>([
        { region: 'East', amount: 100 },
        { region: 'East', amount: null },
        { region: 'East', amount: 200 },
        { region: 'West', amount: null },
        { region: 'West', amount: 300 },
      ]);
      const result = nullDf.groupBy('region').agg({
        total: col('amount').sum(),
        avg: col('amount').mean(),
        n: col('amount').count(),
      });
      const rows = result.toArray();
      const eastRow = rows.find((r) => r['region'] === 'East');
      const westRow = rows.find((r) => r['region'] === 'West');
      expect(eastRow!['total']).toBe(300);
      expect(eastRow!['avg']).toBe(150);
      expect(eastRow!['n']).toBe(2);
      expect(westRow!['total']).toBe(300);
      expect(westRow!['n']).toBe(1);
    });

    it('throws on unknown string shorthand', () => {
      expect(() =>
        df.groupBy('region').agg({ amount: 'invalid' }),
      ).toThrow('Unknown aggregation method');
    });

    it('std aggregation works', () => {
      const result = df.groupBy('region').agg({
        amount_std: col('amount').std(),
      });
      const rows = result.toArray();
      const eastRow = rows.find((r) => r['region'] === 'East');
      // East amounts: 100, 150, 250 -> mean=500/3, std = sample std dev
      expect(eastRow!['amount_std']).toBeGreaterThan(0);
    });

    it('mixed expression and shorthand in same agg call', () => {
      const result = df.groupBy('region').agg({
        total: col('amount').sum(),
        amount: 'mean',
      });
      expect(result.columns).toEqual(['region', 'total', 'amount']);
      expect(result.length).toBe(2);
    });
  });

  describe('shorthand methods', () => {
    it('count() returns region + count columns', () => {
      const result = df.groupBy('region').count();
      expect(result.columns).toEqual(['region', 'count']);
      expect(result.length).toBe(2);

      const rows = result.toArray();
      const eastRow = rows.find((r) => r['region'] === 'East');
      const westRow = rows.find((r) => r['region'] === 'West');
      expect(eastRow!['count']).toBe(3);
      expect(westRow!['count']).toBe(2);
    });

    it('sum(column) returns region + sum of column', () => {
      const result = df.groupBy('region').sum('amount');
      expect(result.columns).toEqual(['region', 'amount']);
      expect(result.length).toBe(2);

      const rows = result.toArray();
      const eastRow = rows.find((r) => r['region'] === 'East');
      const westRow = rows.find((r) => r['region'] === 'West');
      expect(eastRow!['amount']).toBe(500);
      expect(westRow!['amount']).toBe(500);
    });

    it('mean(column) returns region + mean of column', () => {
      const result = df.groupBy('region').mean('amount');
      expect(result.columns).toEqual(['region', 'amount']);

      const rows = result.toArray();
      const eastRow = rows.find((r) => r['region'] === 'East');
      const westRow = rows.find((r) => r['region'] === 'West');
      expect(eastRow!['amount']).toBeCloseTo(500 / 3);
      expect(westRow!['amount']).toBe(250);
    });

    it('min(column) returns region + min of column', () => {
      const result = df.groupBy('region').min('amount');
      expect(result.columns).toEqual(['region', 'amount']);

      const rows = result.toArray();
      const eastRow = rows.find((r) => r['region'] === 'East');
      const westRow = rows.find((r) => r['region'] === 'West');
      expect(eastRow!['amount']).toBe(100);
      expect(westRow!['amount']).toBe(200);
    });

    it('max(column) returns region + max of column', () => {
      const result = df.groupBy('region').max('amount');
      expect(result.columns).toEqual(['region', 'amount']);

      const rows = result.toArray();
      const eastRow = rows.find((r) => r['region'] === 'East');
      const westRow = rows.find((r) => r['region'] === 'West');
      expect(eastRow!['amount']).toBe(250);
      expect(westRow!['amount']).toBe(300);
    });

    it('first() returns first row per group for all non-key columns', () => {
      const result = df.groupBy('region').first();
      expect(result.length).toBe(2);
      expect(result.columns).toEqual(['region', 'product', 'amount']);

      const rows = result.toArray();
      const eastRow = rows.find((r) => r['region'] === 'East');
      const westRow = rows.find((r) => r['region'] === 'West');
      expect(eastRow!['product']).toBe('A');
      expect(eastRow!['amount']).toBe(100);
      expect(westRow!['product']).toBe('B');
      expect(westRow!['amount']).toBe(200);
    });

    it('last() returns last row per group for all non-key columns', () => {
      const result = df.groupBy('region').last();
      expect(result.length).toBe(2);
      expect(result.columns).toEqual(['region', 'product', 'amount']);

      const rows = result.toArray();
      const eastRow = rows.find((r) => r['region'] === 'East');
      const westRow = rows.find((r) => r['region'] === 'West');
      expect(eastRow!['product']).toBe('A');
      expect(eastRow!['amount']).toBe(250);
      expect(westRow!['product']).toBe('A');
      expect(westRow!['amount']).toBe(300);
    });

    it('count() with multi-key groupBy', () => {
      const result = df.groupBy('region', 'product').count();
      expect(result.columns).toEqual(['region', 'product', 'count']);
      expect(result.length).toBe(4);

      const rows = result.toArray();
      const eastA = rows.find((r) => r['region'] === 'East' && r['product'] === 'A');
      expect(eastA!['count']).toBe(2);
    });

    it('sum() with nulls skips null values', () => {
      type NullRow = { region: string; amount: number | null };
      const nullDf = DataFrame.fromRows<NullRow>([
        { region: 'East', amount: 100 },
        { region: 'East', amount: null },
        { region: 'East', amount: 200 },
      ]);
      const result = nullDf.groupBy('region').sum('amount');
      const rows = result.toArray();
      expect(rows[0]!['amount']).toBe(300);
    });
  });
});
