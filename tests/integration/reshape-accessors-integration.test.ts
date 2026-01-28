import { describe, it, expect } from 'vitest';
import { DataFrame, col, when } from '../../src';

/**
 * Helper to create DataFrames from row objects for integration tests.
 */
function rows<S extends Record<string, unknown>>(data: S[]): DataFrame<S> {
  return DataFrame.fromRows(data);
}

describe('Pivot Integration Tests (US-046)', () => {
  const salesData = () =>
    rows([
      { product: 'Widget', region: 'East', amount: 100 },
      { product: 'Widget', region: 'West', amount: 150 },
      { product: 'Widget', region: 'East', amount: 200 },
      { product: 'Gadget', region: 'East', amount: 80 },
      { product: 'Gadget', region: 'West', amount: 120 },
      { product: 'Gadget', region: 'West', amount: 60 },
    ]);

  it('should pivot long-to-wide with sum aggregation', () => {
    const result = salesData().pivot({
      index: 'product',
      columns: 'region',
      values: 'amount',
      aggFunc: 'sum',
    });

    // Should have one row per product
    expect(result.length).toBe(2);

    // Columns: product + one per unique region
    expect(result.columns).toContain('product');
    expect(result.columns).toContain('East');
    expect(result.columns).toContain('West');

    // Verify aggregated values
    for (let i = 0; i < result.length; i++) {
      const product = result.col('product').get(i);
      if (product === 'Widget') {
        expect(result.col('East').get(i)).toBe(300); // 100+200
        expect(result.col('West').get(i)).toBe(150);
      } else if (product === 'Gadget') {
        expect(result.col('East').get(i)).toBe(80);
        expect(result.col('West').get(i)).toBe(180); // 120+60
      }
    }
  });

  it('should pivot with count aggregation', () => {
    const result = salesData().pivot({
      index: 'product',
      columns: 'region',
      values: 'amount',
      aggFunc: 'count',
    });

    expect(result.length).toBe(2);

    for (let i = 0; i < result.length; i++) {
      const product = result.col('product').get(i);
      if (product === 'Widget') {
        expect(result.col('East').get(i)).toBe(2); // two East entries
        expect(result.col('West').get(i)).toBe(1);
      } else if (product === 'Gadget') {
        expect(result.col('East').get(i)).toBe(1);
        expect(result.col('West').get(i)).toBe(2); // two West entries
      }
    }
  });
});

describe('Melt Integration Tests (US-046)', () => {
  const wideData = () =>
    rows([
      { date: '2024-01', widget: 100, gadget: 80 },
      { date: '2024-02', widget: 150, gadget: 90 },
    ]);

  it('should melt wide-to-long', () => {
    const result = wideData().melt({
      idVars: 'date',
      valueVars: ['widget', 'gadget'],
      varName: 'product',
      valueName: 'amount',
    });

    // 2 rows * 2 value vars = 4 rows
    expect(result.length).toBe(4);
    expect(result.columns).toEqual(['date', 'product', 'amount']);

    const products = result.col('product').toArray();
    expect(products.filter((p) => p === 'widget').length).toBe(2);
    expect(products.filter((p) => p === 'gadget').length).toBe(2);
  });

  it('should roundtrip pivot then melt', () => {
    const original = rows([
      { product: 'Widget', region: 'East', amount: 100 },
      { product: 'Widget', region: 'West', amount: 150 },
      { product: 'Gadget', region: 'East', amount: 80 },
    ]);

    // Pivot to wide
    const wide = original.pivot({
      index: 'product',
      columns: 'region',
      values: 'amount',
      aggFunc: 'first',
    });

    expect(wide.length).toBe(2);
    expect(wide.columns).toContain('East');
    expect(wide.columns).toContain('West');

    // Melt back to long
    const long = wide.melt({
      idVars: 'product',
      valueVars: ['East', 'West'],
      varName: 'region',
      valueName: 'amount',
    });

    // 2 products * 2 regions = 4 rows (one will be null since Gadget/West didn't exist)
    expect(long.length).toBe(4);
    expect(long.columns).toEqual(['product', 'region', 'amount']);

    // Verify Widget/East value survived roundtrip
    for (let i = 0; i < long.length; i++) {
      if (
        long.col('product').get(i) === 'Widget' &&
        long.col('region').get(i) === 'East'
      ) {
        expect(long.col('amount').get(i)).toBe(100);
      }
    }
  });
});

describe('Transpose Integration Tests (US-046)', () => {
  it('should transpose a small DataFrame', () => {
    const data = rows([
      { name: 'Alice', score: 90, grade: 'A' },
      { name: 'Bob', score: 85, grade: 'B' },
    ]);

    const result = data.transpose('name');

    // Original has 3 columns (name, score, grade); name is used as header
    // Remaining columns (score, grade) become rows
    expect(result.length).toBe(2); // score row and grade row

    // Should have column named 'column' + one per row (Alice, Bob)
    expect(result.columns).toContain('column');
    expect(result.columns).toContain('Alice');
    expect(result.columns).toContain('Bob');

    // Find the 'score' row
    for (let i = 0; i < result.length; i++) {
      if (result.col('column').get(i) === 'score') {
        // Values should be stringified since transpose mixes types
        const aliceScore = result.col('Alice').get(i);
        const bobScore = result.col('Bob').get(i);
        // Scores are either numbers or string representations
        expect(Number(aliceScore)).toBe(90);
        expect(Number(bobScore)).toBe(85);
      }
    }
  });
});

describe('StringAccessor Integration Tests (US-046)', () => {
  const nameData = () =>
    rows([
      { id: 1, name: 'Alice Johnson' },
      { id: 2, name: 'Bob Smith' },
      { id: 3, name: 'Charlie Brown' },
      { id: 4, name: null },
    ]);

  it('should convert to lower case', () => {
    const result = nameData().col('name').str.toLowerCase();

    expect(result.get(0)).toBe('alice johnson');
    expect(result.get(1)).toBe('bob smith');
    expect(result.get(2)).toBe('charlie brown');
    expect(result.get(3)).toBeNull();
  });

  it('should check contains', () => {
    const result = nameData().col('name').str.contains('son');

    expect(result.get(0)).toBe(true); // Johnson
    expect(result.get(1)).toBe(false); // Smith
    expect(result.get(2)).toBe(false); // Brown
    expect(result.get(3)).toBeNull(); // null
  });

  it('should replace substrings', () => {
    const result = nameData().col('name').str.replace(' ', '_');

    expect(result.get(0)).toBe('Alice_Johnson');
    expect(result.get(1)).toBe('Bob_Smith');
    expect(result.get(2)).toBe('Charlie_Brown');
    expect(result.get(3)).toBeNull();
  });
});

describe('DateAccessor Integration Tests (US-046)', () => {
  const dateData = () =>
    rows([
      { id: 1, ts: new Date(2024, 0, 15) }, // Jan 15, 2024
      { id: 2, ts: new Date(2024, 5, 20) }, // Jun 20, 2024
      { id: 3, ts: new Date(2024, 11, 25) }, // Dec 25, 2024
    ]);

  it('should extract year', () => {
    const years = dateData().col('ts').dt.year();
    expect(years.get(0)).toBe(2024);
    expect(years.get(1)).toBe(2024);
    expect(years.get(2)).toBe(2024);
  });

  it('should extract month (1-indexed)', () => {
    const months = dateData().col('ts').dt.month();
    expect(months.get(0)).toBe(1); // January
    expect(months.get(1)).toBe(6); // June
    expect(months.get(2)).toBe(12); // December
  });

  it('should extract day', () => {
    const days = dateData().col('ts').dt.day();
    expect(days.get(0)).toBe(15);
    expect(days.get(1)).toBe(20);
    expect(days.get(2)).toBe(25);
  });
});

describe('When/Then/Otherwise Integration Tests (US-046)', () => {
  const salesData = () =>
    rows([
      { product: 'Widget', region: 'East', amount: 50 },
      { product: 'Gadget', region: 'West', amount: 150 },
      { product: 'Widget', region: 'East', amount: 200 },
      { product: 'Gadget', region: 'East', amount: 30 },
    ]);

  it('should apply simple when/then/otherwise in withColumn', () => {
    const df = salesData();

    const result = df.withColumn(
      'tier',
      when(col('amount').gt(100))
        .then('high')
        .otherwise('low'),
    );

    expect(result.columns).toContain('tier');
    expect(result.length).toBe(4);

    // amount 50 → low, 150 → high, 200 → high, 30 → low
    expect(result.col('tier').get(0)).toBe('low');
    expect(result.col('tier').get(1)).toBe('high');
    expect(result.col('tier').get(2)).toBe('high');
    expect(result.col('tier').get(3)).toBe('low');
  });

  it('should apply chained when/then conditions', () => {
    const df = salesData();

    const result = df.withColumn(
      'category',
      when(col('amount').gt(150))
        .then('premium')
        .when(col('amount').gt(50))
        .then('standard')
        .otherwise('budget'),
    );

    // 50 → budget, 150 → standard, 200 → premium, 30 → budget
    expect(result.col('category').get(0)).toBe('budget');
    expect(result.col('category').get(1)).toBe('standard');
    expect(result.col('category').get(2)).toBe('premium');
    expect(result.col('category').get(3)).toBe('budget');
  });
});
