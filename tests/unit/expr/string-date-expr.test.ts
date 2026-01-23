import { describe, it, expect } from 'vitest';
import { DataFrame, col } from '../../../src/index';
import '../../../src/expr/expr-accessors';

describe('StringExprAccessor', () => {
  const df = DataFrame.fromRows([
    { name: 'Alice', city: 'New York' },
    { name: 'Bob', city: 'Los Angeles' },
    { name: null, city: 'Chicago' },
    { name: 'Dave', city: null },
  ]);

  it('toLowerCase()', () => {
    const result = df.withColumn('lower', col<string>('name').str.toLowerCase());
    expect(result.col('lower').get(0)).toBe('alice');
    expect(result.col('lower').get(1)).toBe('bob');
    expect(result.col('lower').get(2)).toBeNull();
    expect(result.col('lower').get(3)).toBe('dave');
  });

  it('toUpperCase()', () => {
    const result = df.withColumn('upper', col<string>('name').str.toUpperCase());
    expect(result.col('upper').get(0)).toBe('ALICE');
    expect(result.col('upper').get(1)).toBe('BOB');
  });

  it('trim()', () => {
    const df2 = DataFrame.fromRows([
      { val: '  hello  ' },
      { val: 'world' },
      { val: null },
    ]);
    const result = df2.withColumn('trimmed', col<string>('val').str.trim());
    expect(result.col('trimmed').get(0)).toBe('hello');
    expect(result.col('trimmed').get(1)).toBe('world');
    expect(result.col('trimmed').get(2)).toBeNull();
  });

  it('contains()', () => {
    const result = df.withColumn('hasY', col<string>('city').str.contains('York'));
    expect(result.col('hasY').get(0)).toBe(true);
    expect(result.col('hasY').get(1)).toBe(false);
    expect(result.col('hasY').get(3)).toBeNull();
  });

  it('startsWith()', () => {
    const result = df.withColumn('startsNew', col<string>('city').str.startsWith('New'));
    expect(result.col('startsNew').get(0)).toBe(true);
    expect(result.col('startsNew').get(1)).toBe(false);
  });

  it('endsWith()', () => {
    const result = df.withColumn('endsLes', col<string>('city').str.endsWith('les'));
    expect(result.col('endsLes').get(1)).toBe(true);
    expect(result.col('endsLes').get(2)).toBe(false);
  });

  it('replace()', () => {
    const result = df.withColumn('replaced', col<string>('city').str.replace('New', 'Old'));
    expect(result.col('replaced').get(0)).toBe('Old York');
    expect(result.col('replaced').get(1)).toBe('Los Angeles');
    expect(result.col('replaced').get(3)).toBeNull();
  });

  it('slice()', () => {
    const result = df.withColumn('sliced', col<string>('name').str.slice(0, 3));
    expect(result.col('sliced').get(0)).toBe('Ali');
    expect(result.col('sliced').get(1)).toBe('Bob');
    expect(result.col('sliced').get(2)).toBeNull();
  });

  it('length()', () => {
    const result = df.withColumn('len', col<string>('name').str.length());
    expect(result.col('len').get(0)).toBe(5);
    expect(result.col('len').get(1)).toBe(3);
    expect(result.col('len').get(2)).toBeNull();
  });

  it('concat() with literal separator', () => {
    const result = df.withColumn(
      'full',
      col<string>('name').str.concat(' from ', col<string>('city')),
    );
    expect(result.col('full').get(0)).toBe('Alice from New York');
    expect(result.col('full').get(1)).toBe('Bob from Los Angeles');
    // null propagation — either side null → result null
    expect(result.col('full').get(2)).toBeNull();
    expect(result.col('full').get(3)).toBeNull();
  });

  it('concat() with multiple parts', () => {
    const df2 = DataFrame.fromRows([
      { first: 'John', last: 'Doe' },
      { first: 'Jane', last: 'Smith' },
    ]);
    const result = df2.withColumn(
      'full',
      col<string>('first').str.concat(' ', col<string>('last')),
    );
    expect(result.col('full').get(0)).toBe('John Doe');
    expect(result.col('full').get(1)).toBe('Jane Smith');
  });

  it('works with filter()', () => {
    const result = df.filter(col<string>('city').str.contains('Angeles'));
    expect(result.length).toBe(1);
    expect(result.col('name').get(0)).toBe('Bob');
  });

  it('chains string operations', () => {
    const result = df.withColumn(
      'processed',
      col<string>('name').str.toUpperCase(),
    );
    const result2 = result.withColumn(
      'sliced',
      col<string>('processed').str.slice(0, 2),
    );
    expect(result2.col('sliced').get(0)).toBe('AL');
    expect(result2.col('sliced').get(1)).toBe('BO');
  });
});

describe('DateExprAccessor', () => {
  const df = DataFrame.fromRows([
    { date: new Date('2024-03-15T10:30:45'), label: 'a' },
    { date: new Date('2024-06-20T14:15:30'), label: 'b' },
    { date: null, label: 'c' },
    { date: new Date('2024-12-25T00:00:00'), label: 'd' },
  ]);

  it('year()', () => {
    const result = df.withColumn('y', col<Date>('date').dt.year());
    expect(result.col('y').get(0)).toBe(2024);
    expect(result.col('y').get(2)).toBeNull();
  });

  it('month()', () => {
    const result = df.withColumn('m', col<Date>('date').dt.month());
    expect(result.col('m').get(0)).toBe(3);
    expect(result.col('m').get(1)).toBe(6);
    expect(result.col('m').get(3)).toBe(12);
  });

  it('day()', () => {
    const result = df.withColumn('d', col<Date>('date').dt.day());
    expect(result.col('d').get(0)).toBe(15);
    expect(result.col('d').get(3)).toBe(25);
  });

  it('hour()', () => {
    const result = df.withColumn('h', col<Date>('date').dt.hour());
    expect(result.col('h').get(0)).toBe(10);
    expect(result.col('h').get(1)).toBe(14);
  });

  it('minute()', () => {
    const result = df.withColumn('min', col<Date>('date').dt.minute());
    expect(result.col('min').get(0)).toBe(30);
  });

  it('second()', () => {
    const result = df.withColumn('sec', col<Date>('date').dt.second());
    expect(result.col('sec').get(0)).toBe(45);
  });

  it('dayOfWeek()', () => {
    const result = df.withColumn('dow', col<Date>('date').dt.dayOfWeek());
    // 2024-03-15 is Friday = 5
    expect(result.col('dow').get(0)).toBe(5);
  });

  it('quarter()', () => {
    const result = df.withColumn('q', col<Date>('date').dt.quarter());
    expect(result.col('q').get(0)).toBe(1);  // March = Q1
    expect(result.col('q').get(1)).toBe(2);  // June = Q2
    expect(result.col('q').get(3)).toBe(4);  // December = Q4
  });

  it('timestamp()', () => {
    const result = df.withColumn('ts', col<Date>('date').dt.timestamp());
    expect(result.col('ts').get(0)).toBe(new Date('2024-03-15T10:30:45').getTime());
    expect(result.col('ts').get(2)).toBeNull();
  });

  it('truncate()', () => {
    const result = df.withColumn('trunc', col<Date>('date').dt.truncate('day'));
    const truncated = result.col('trunc').get(0) as Date;
    expect(truncated.getHours()).toBe(0);
    expect(truncated.getMinutes()).toBe(0);
    expect(truncated.getSeconds()).toBe(0);
    expect(truncated.getDate()).toBe(15);
  });

  it('diff() in days', () => {
    const df2 = DataFrame.fromRows([
      { start: new Date('2024-01-01'), end: new Date('2024-01-11') },
      { start: new Date('2024-06-15'), end: new Date('2024-06-15') },
      { start: null, end: new Date('2024-01-01') },
    ]);
    const result = df2.withColumn(
      'days',
      col<Date>('end').dt.diff(col<Date>('start'), 'days'),
    );
    expect(result.col('days').get(0)).toBe(10);
    expect(result.col('days').get(1)).toBe(0);
    expect(result.col('days').get(2)).toBeNull();
  });

  it('diff() in hours', () => {
    const df2 = DataFrame.fromRows([
      { start: new Date('2024-01-01T00:00:00'), end: new Date('2024-01-01T06:00:00') },
    ]);
    const result = df2.withColumn(
      'hours',
      col<Date>('end').dt.diff(col<Date>('start'), 'hours'),
    );
    expect(result.col('hours').get(0)).toBe(6);
  });

  it('diff() negative result', () => {
    const df2 = DataFrame.fromRows([
      { start: new Date('2024-01-11'), end: new Date('2024-01-01') },
    ]);
    const result = df2.withColumn(
      'days',
      col<Date>('end').dt.diff(col<Date>('start'), 'days'),
    );
    expect(result.col('days').get(0)).toBe(-10);
  });

  it('works with filter()', () => {
    const result = df.filter(col<Date>('date').dt.month().gt(6));
    expect(result.length).toBe(1);
    expect(result.col('label').get(0)).toBe('d');
  });

  it('works with withColumn chaining', () => {
    const result = df
      .withColumn('y', col<Date>('date').dt.year())
      .withColumn('m', col<Date>('date').dt.month());
    expect(result.col('y').get(0)).toBe(2024);
    expect(result.col('m').get(0)).toBe(3);
  });

  it('null propagation across all component methods', () => {
    const result = df.withColumn('y', col<Date>('date').dt.year());
    expect(result.col('y').get(2)).toBeNull();
  });
});
