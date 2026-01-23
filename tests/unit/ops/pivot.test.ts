import { describe, it, expect } from 'vitest';
import { DataFrame } from '../../../src/dataframe';

describe('pivot', () => {
  const salesData = DataFrame.fromRows([
    { region: 'North', quarter: 'Q1', revenue: 100 },
    { region: 'North', quarter: 'Q2', revenue: 150 },
    { region: 'South', quarter: 'Q1', revenue: 200 },
    { region: 'South', quarter: 'Q2', revenue: 250 },
    { region: 'East', quarter: 'Q1', revenue: 300 },
  ]);

  it('creates wide DataFrame from long format', () => {
    const result = salesData.pivot({ index: 'region', columns: 'quarter', values: 'revenue' });
    expect(result.columns).toEqual(['region', 'Q1', 'Q2']);
    expect(result.length).toBe(3);
  });

  it('fills null for missing data', () => {
    const result = salesData.pivot({ index: 'region', columns: 'quarter', values: 'revenue' });
    // East has Q1 but no Q2
    const eastRow = result.toArray().find((r) => r['region'] === 'East');
    expect(eastRow!['Q1']).toBe(300);
    expect(eastRow!['Q2']).toBeNull();
  });

  it('uses first as default aggFunc', () => {
    const df = DataFrame.fromRows([
      { region: 'North', quarter: 'Q1', revenue: 100 },
      { region: 'North', quarter: 'Q1', revenue: 999 },
    ]);
    const result = df.pivot({ index: 'region', columns: 'quarter', values: 'revenue' });
    const row = result.toArray()[0]!;
    expect(row['Q1']).toBe(100); // first value
  });

  it('supports sum aggFunc', () => {
    const df = DataFrame.fromRows([
      { region: 'North', quarter: 'Q1', revenue: 100 },
      { region: 'North', quarter: 'Q1', revenue: 50 },
      { region: 'North', quarter: 'Q2', revenue: 200 },
    ]);
    const result = df.pivot({ index: 'region', columns: 'quarter', values: 'revenue', aggFunc: 'sum' });
    const row = result.toArray()[0]!;
    expect(row['Q1']).toBe(150);
    expect(row['Q2']).toBe(200);
  });

  it('supports mean aggFunc', () => {
    const df = DataFrame.fromRows([
      { region: 'North', quarter: 'Q1', revenue: 100 },
      { region: 'North', quarter: 'Q1', revenue: 200 },
    ]);
    const result = df.pivot({ index: 'region', columns: 'quarter', values: 'revenue', aggFunc: 'mean' });
    const row = result.toArray()[0]!;
    expect(row['Q1']).toBe(150);
  });

  it('supports count aggFunc', () => {
    const df = DataFrame.fromRows([
      { region: 'North', quarter: 'Q1', revenue: 100 },
      { region: 'North', quarter: 'Q1', revenue: 200 },
      { region: 'North', quarter: 'Q2', revenue: 300 },
    ]);
    const result = df.pivot({ index: 'region', columns: 'quarter', values: 'revenue', aggFunc: 'count' });
    const row = result.toArray()[0]!;
    expect(row['Q1']).toBe(2);
    expect(row['Q2']).toBe(1);
  });

  it('supports last aggFunc', () => {
    const df = DataFrame.fromRows([
      { region: 'North', quarter: 'Q1', revenue: 100 },
      { region: 'North', quarter: 'Q1', revenue: 999 },
    ]);
    const result = df.pivot({ index: 'region', columns: 'quarter', values: 'revenue', aggFunc: 'last' });
    const row = result.toArray()[0]!;
    expect(row['Q1']).toBe(999);
  });

  it('supports multi-column index', () => {
    const df = DataFrame.fromRows([
      { region: 'North', year: 2024, quarter: 'Q1', revenue: 100 },
      { region: 'North', year: 2024, quarter: 'Q2', revenue: 150 },
      { region: 'North', year: 2025, quarter: 'Q1', revenue: 200 },
    ]);
    const result = df.pivot({ index: ['region', 'year'], columns: 'quarter', values: 'revenue' });
    expect(result.columns).toEqual(['region', 'year', 'Q1', 'Q2']);
    expect(result.length).toBe(2);
  });

  it('preserves index column order', () => {
    const result = salesData.pivot({ index: 'region', columns: 'quarter', values: 'revenue' });
    const regions = result.col('region').toArray();
    expect(regions).toEqual(['North', 'South', 'East']);
  });

  it('preserves pivot column order', () => {
    const result = salesData.pivot({ index: 'region', columns: 'quarter', values: 'revenue' });
    expect(result.columns[1]).toBe('Q1');
    expect(result.columns[2]).toBe('Q2');
  });

  it('throws on missing column', () => {
    expect(() => salesData.pivot({ index: 'nonexistent', columns: 'quarter', values: 'revenue' }))
      .toThrow();
  });

  it('handles single row per cell without aggregation', () => {
    const result = salesData.pivot({ index: 'region', columns: 'quarter', values: 'revenue' });
    const northRow = result.toArray().find((r) => r['region'] === 'North');
    expect(northRow!['Q1']).toBe(100);
    expect(northRow!['Q2']).toBe(150);
    const southRow = result.toArray().find((r) => r['region'] === 'South');
    expect(southRow!['Q1']).toBe(200);
    expect(southRow!['Q2']).toBe(250);
  });

  it('handles string values', () => {
    const df = DataFrame.fromRows([
      { id: 1, attr: 'color', val: 'red' },
      { id: 1, attr: 'size', val: 'large' },
      { id: 2, attr: 'color', val: 'blue' },
    ]);
    const result = df.pivot({ index: 'id', columns: 'attr', values: 'val' });
    expect(result.columns).toEqual(['id', 'color', 'size']);
    const rows = result.toArray();
    expect(rows[0]!['color']).toBe('red');
    expect(rows[0]!['size']).toBe('large');
    expect(rows[1]!['color']).toBe('blue');
    expect(rows[1]!['size']).toBeNull();
  });
});
