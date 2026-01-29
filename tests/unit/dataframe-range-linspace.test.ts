import { describe, it, expect } from 'vitest';
import { DataFrame, ErrorCode, FrameKitError } from '../../src';

describe('DataFrame.range', () => {
  it('creates a DataFrame with column containing 0..99', () => {
    const df = DataFrame.range('i', 0, 100);
    expect(df.length).toBe(100);
    expect(df.columns).toEqual(['i']);
    expect(df.col('i').get(0)).toBe(0);
    expect(df.col('i').get(99)).toBe(99);
  });

  it('creates a DataFrame with step=2', () => {
    const df = DataFrame.range('i', 0, 100, 2);
    expect(df.length).toBe(50);
    expect(df.col('i').get(0)).toBe(0);
    expect(df.col('i').get(1)).toBe(2);
    expect(df.col('i').get(49)).toBe(98);
  });

  it('throws on step=0', () => {
    expect(() => DataFrame.range('i', 0, 100, 0)).toThrow(FrameKitError);
    try {
      DataFrame.range('i', 0, 100, 0);
    } catch (e) {
      expect((e as FrameKitError).code).toBe(ErrorCode.INVALID_OPERATION);
    }
  });

  it('throws when start >= end', () => {
    expect(() => DataFrame.range('i', 100, 0)).toThrow(FrameKitError);
    expect(() => DataFrame.range('i', 5, 5)).toThrow(FrameKitError);
    try {
      DataFrame.range('i', 100, 0);
    } catch (e) {
      expect((e as FrameKitError).code).toBe(ErrorCode.INVALID_OPERATION);
    }
  });
});

describe('DataFrame.linspace', () => {
  it('creates 100 evenly spaced values from 0 to 1', () => {
    const df = DataFrame.linspace('x', 0, 1, 100);
    expect(df.length).toBe(100);
    expect(df.columns).toEqual(['x']);
    expect(df.col('x').get(0)).toBe(0);
    expect(df.col('x').get(99)).toBeCloseTo(1, 10);
  });

  it('creates 5 evenly spaced values from 0 to 4', () => {
    const df = DataFrame.linspace('x', 0, 4, 5);
    expect(df.length).toBe(5);
    expect(df.col('x').get(0)).toBe(0);
    expect(df.col('x').get(1)).toBe(1);
    expect(df.col('x').get(2)).toBe(2);
    expect(df.col('x').get(3)).toBe(3);
    expect(df.col('x').get(4)).toBe(4);
  });

  it('throws when count < 2', () => {
    expect(() => DataFrame.linspace('x', 0, 1, 1)).toThrow(FrameKitError);
    try {
      DataFrame.linspace('x', 0, 1, 1);
    } catch (e) {
      expect((e as FrameKitError).code).toBe(ErrorCode.INVALID_OPERATION);
    }
  });
});
