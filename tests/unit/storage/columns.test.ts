import { describe, it, expect } from 'vitest';
import { Int32Column } from '../../../src/storage/numeric';
import { Utf8Column } from '../../../src/storage/string';
import { BooleanColumn } from '../../../src/storage/boolean';
import { DateColumn } from '../../../src/storage/date';
import { DType } from '../../../src/types/dtype';
import { Column } from '../../../src/storage/column';

describe('Int32Column', () => {
  it('should have correct dtype', () => {
    const col = Int32Column.from([1, 2, 3]);
    expect(col.dtype).toBe(DType.Int32);
  });

  it('should be instance of Column', () => {
    const col = Int32Column.from([1]);
    expect(col).toBeInstanceOf(Column);
  });

  it('should handle nulls', () => {
    const col = Int32Column.from([1, null, 3]);
    expect(col.length).toBe(3);
    expect(col.nullCount).toBe(1);
    expect(col.get(0)).toBe(1);
    expect(col.get(1)).toBeNull();
    expect(col.get(2)).toBe(3);
  });

  it('should truncate floats to integers', () => {
    const col = Int32Column.from([1.9, 2.5]);
    expect(col.get(0)).toBe(1);
    expect(col.get(1)).toBe(2);
  });

  it('sum/mean/min/max skip nulls', () => {
    const col = Int32Column.from([10, null, 30]);
    expect(col.sum()).toBe(40);
    expect(col.mean()).toBe(20);
    expect(col.min()).toBe(10);
    expect(col.max()).toBe(30);
  });

  it('mean/min/max return null for all-null', () => {
    const col = Int32Column.from([null, null]);
    expect(col.mean()).toBeNull();
    expect(col.min()).toBeNull();
    expect(col.max()).toBeNull();
  });

  it('slice preserves nulls', () => {
    const col = Int32Column.from([1, null, 3, 4]);
    const sliced = col.slice(0, 3);
    expect(sliced.length).toBe(3);
    expect(sliced.get(1)).toBeNull();
  });

  it('clone creates independent copy', () => {
    const col = Int32Column.from([1, null, 3]);
    const cloned = col.clone();
    expect(cloned.length).toBe(3);
    expect(cloned.get(1)).toBeNull();
  });

  it('take selects by indices', () => {
    const col = Int32Column.from([10, 20, 30, 40]);
    const taken = col.take(new Int32Array([0, 3]));
    expect(taken.length).toBe(2);
    expect(taken.get(0)).toBe(10);
    expect(taken.get(1)).toBe(40);
  });

  it('from empty array', () => {
    const col = Int32Column.from([]);
    expect(col.length).toBe(0);
    expect(col.nullCount).toBe(0);
  });

  it('throws on out-of-bounds', () => {
    const col = Int32Column.from([1]);
    expect(() => col.get(-1)).toThrow();
    expect(() => col.get(1)).toThrow();
  });

  it('filter by boolean mask', () => {
    const col = Int32Column.from([10, 20, 30, 40]);
    const mask = BooleanColumn.from([true, false, true, false]);
    const filtered = col.filter(mask);
    expect(filtered.length).toBe(2);
    expect(filtered.get(0)).toBe(10);
    expect(filtered.get(1)).toBe(30);
  });

  it('filter preserves nulls', () => {
    const col = Int32Column.from([1, null, 3]);
    const mask = BooleanColumn.from([true, true, false]);
    const filtered = col.filter(mask);
    expect(filtered.length).toBe(2);
    expect(filtered.get(0)).toBe(1);
    expect(filtered.get(1)).toBeNull();
  });

  it('filter throws on mask length mismatch', () => {
    const col = Int32Column.from([1, 2]);
    const mask = BooleanColumn.from([true]);
    expect(() => col.filter(mask)).toThrow();
  });
});

describe('Utf8Column', () => {
  it('should have correct dtype', () => {
    const col = Utf8Column.from(['a', 'b']);
    expect(col.dtype).toBe(DType.Utf8);
  });

  it('should be instance of Column', () => {
    const col = Utf8Column.from(['a']);
    expect(col).toBeInstanceOf(Column);
  });

  it('should handle nulls', () => {
    const col = Utf8Column.from(['hello', null, 'world']);
    expect(col.length).toBe(3);
    expect(col.nullCount).toBe(1);
    expect(col.get(0)).toBe('hello');
    expect(col.get(1)).toBeNull();
    expect(col.get(2)).toBe('world');
  });

  it('slice preserves nulls', () => {
    const col = Utf8Column.from(['a', null, 'c', 'd']);
    const sliced = col.slice(0, 3);
    expect(sliced.length).toBe(3);
    expect(sliced.get(1)).toBeNull();
    expect(sliced.get(2)).toBe('c');
  });

  it('clone creates independent copy', () => {
    const col = Utf8Column.from(['x', null, 'z']);
    const cloned = col.clone();
    expect(cloned.length).toBe(3);
    expect(cloned.get(0)).toBe('x');
    expect(cloned.get(1)).toBeNull();
  });

  it('take selects by indices', () => {
    const col = Utf8Column.from(['a', 'b', 'c', 'd']);
    const taken = col.take(new Int32Array([1, 3]));
    expect(taken.length).toBe(2);
    expect(taken.get(0)).toBe('b');
    expect(taken.get(1)).toBe('d');
  });

  it('from empty array', () => {
    const col = Utf8Column.from([]);
    expect(col.length).toBe(0);
  });

  it('throws on out-of-bounds', () => {
    const col = Utf8Column.from(['a']);
    expect(() => col.get(-1)).toThrow();
    expect(() => col.get(1)).toThrow();
  });

  it('filter by boolean mask', () => {
    const col = Utf8Column.from(['a', 'b', 'c', 'd']);
    const mask = BooleanColumn.from([false, true, false, true]);
    const filtered = col.filter(mask);
    expect(filtered.length).toBe(2);
    expect(filtered.get(0)).toBe('b');
    expect(filtered.get(1)).toBe('d');
  });

  it('filter preserves nulls', () => {
    const col = Utf8Column.from(['a', null, 'c']);
    const mask = BooleanColumn.from([true, true, false]);
    const filtered = col.filter(mask);
    expect(filtered.length).toBe(2);
    expect(filtered.get(0)).toBe('a');
    expect(filtered.get(1)).toBeNull();
  });

  it('filter returns empty on all-false mask', () => {
    const col = Utf8Column.from(['a', 'b']);
    const mask = BooleanColumn.from([false, false]);
    const filtered = col.filter(mask);
    expect(filtered.length).toBe(0);
  });

  it('handles empty strings', () => {
    const col = Utf8Column.from(['', 'hello', '']);
    expect(col.length).toBe(3);
    expect(col.nullCount).toBe(0);
    expect(col.get(0)).toBe('');
    expect(col.get(2)).toBe('');
  });
});

describe('BooleanColumn', () => {
  it('should have correct dtype', () => {
    const col = BooleanColumn.from([true, false]);
    expect(col.dtype).toBe(DType.Boolean);
  });

  it('should be instance of Column', () => {
    const col = BooleanColumn.from([true]);
    expect(col).toBeInstanceOf(Column);
  });

  it('should handle nulls', () => {
    const col = BooleanColumn.from([true, null, false]);
    expect(col.length).toBe(3);
    expect(col.nullCount).toBe(1);
    expect(col.get(0)).toBe(true);
    expect(col.get(1)).toBeNull();
    expect(col.get(2)).toBe(false);
  });

  it('slice preserves nulls', () => {
    const col = BooleanColumn.from([true, null, false, true]);
    const sliced = col.slice(1, 3);
    expect(sliced.length).toBe(2);
    expect(sliced.get(0)).toBeNull();
    expect(sliced.get(1)).toBe(false);
  });

  it('clone creates independent copy', () => {
    const col = BooleanColumn.from([true, null, false]);
    const cloned = col.clone();
    expect(cloned.length).toBe(3);
    expect(cloned.get(0)).toBe(true);
    expect(cloned.get(1)).toBeNull();
  });

  it('take selects by indices', () => {
    const col = BooleanColumn.from([true, false, true, false]);
    const taken = col.take(new Int32Array([0, 2]));
    expect(taken.length).toBe(2);
    expect(taken.get(0)).toBe(true);
    expect(taken.get(1)).toBe(true);
  });

  it('from empty array', () => {
    const col = BooleanColumn.from([]);
    expect(col.length).toBe(0);
  });

  it('throws on out-of-bounds', () => {
    const col = BooleanColumn.from([true]);
    expect(() => col.get(-1)).toThrow();
    expect(() => col.get(1)).toThrow();
  });

  it('filter by boolean mask', () => {
    const col = BooleanColumn.from([true, false, true, false]);
    const mask = BooleanColumn.from([true, true, false, false]);
    const filtered = col.filter(mask);
    expect(filtered.length).toBe(2);
    expect(filtered.get(0)).toBe(true);
    expect(filtered.get(1)).toBe(false);
  });

  it('filter preserves nulls', () => {
    const col = BooleanColumn.from([true, null, false]);
    const mask = BooleanColumn.from([true, true, true]);
    const filtered = col.filter(mask);
    expect(filtered.length).toBe(3);
    expect(filtered.get(1)).toBeNull();
  });

  it('all-null column', () => {
    const col = BooleanColumn.from([null, null, null]);
    expect(col.length).toBe(3);
    expect(col.nullCount).toBe(3);
  });
});

describe('DateColumn', () => {
  const d1 = new Date('2024-01-01');
  const d2 = new Date('2024-06-15');
  const d3 = new Date('2024-12-31');

  it('should have correct dtype', () => {
    const col = DateColumn.from([d1, d2]);
    expect(col.dtype).toBe(DType.Date);
  });

  it('should be instance of Column', () => {
    const col = DateColumn.from([d1]);
    expect(col).toBeInstanceOf(Column);
  });

  it('should return Date objects from get()', () => {
    const col = DateColumn.from([d1, d2]);
    const result = col.get(0);
    expect(result).toBeInstanceOf(Date);
    expect(result!.getTime()).toBe(d1.getTime());
  });

  it('should handle nulls', () => {
    const col = DateColumn.from([d1, null, d3]);
    expect(col.length).toBe(3);
    expect(col.nullCount).toBe(1);
    expect(col.get(0)!.getTime()).toBe(d1.getTime());
    expect(col.get(1)).toBeNull();
    expect(col.get(2)!.getTime()).toBe(d3.getTime());
  });

  it('stores epoch-ms internally', () => {
    const col = DateColumn.from([d1, d2]);
    const retrieved1 = col.get(0)!;
    const retrieved2 = col.get(1)!;
    expect(retrieved1.getTime()).toBe(d1.getTime());
    expect(retrieved2.getTime()).toBe(d2.getTime());
    // Verify returned Date is a new instance (not the original)
    expect(retrieved1).not.toBe(d1);
  });

  it('slice preserves nulls', () => {
    const col = DateColumn.from([d1, null, d3, d2]);
    const sliced = col.slice(0, 3);
    expect(sliced.length).toBe(3);
    expect(sliced.get(1)).toBeNull();
    expect(sliced.get(2)!.getTime()).toBe(d3.getTime());
  });

  it('clone creates independent copy', () => {
    const col = DateColumn.from([d1, null, d3]);
    const cloned = col.clone();
    expect(cloned.length).toBe(3);
    expect(cloned.get(0)!.getTime()).toBe(d1.getTime());
    expect(cloned.get(1)).toBeNull();
  });

  it('take selects by indices', () => {
    const col = DateColumn.from([d1, d2, d3]);
    const taken = col.take(new Int32Array([0, 2]));
    expect(taken.length).toBe(2);
    expect(taken.get(0)!.getTime()).toBe(d1.getTime());
    expect(taken.get(1)!.getTime()).toBe(d3.getTime());
  });

  it('from empty array', () => {
    const col = DateColumn.from([]);
    expect(col.length).toBe(0);
  });

  it('throws on out-of-bounds', () => {
    const col = DateColumn.from([d1]);
    expect(() => col.get(-1)).toThrow();
    expect(() => col.get(1)).toThrow();
  });

  it('filter by boolean mask', () => {
    const col = DateColumn.from([d1, d2, d3]);
    const mask = BooleanColumn.from([true, false, true]);
    const filtered = col.filter(mask);
    expect(filtered.length).toBe(2);
    expect(filtered.get(0)!.getTime()).toBe(d1.getTime());
    expect(filtered.get(1)!.getTime()).toBe(d3.getTime());
  });

  it('filter preserves nulls', () => {
    const col = DateColumn.from([d1, null, d3]);
    const mask = BooleanColumn.from([false, true, true]);
    const filtered = col.filter(mask);
    expect(filtered.length).toBe(2);
    expect(filtered.get(0)).toBeNull();
    expect(filtered.get(1)!.getTime()).toBe(d3.getTime());
  });

  it('all-null column', () => {
    const col = DateColumn.from([null, null]);
    expect(col.length).toBe(2);
    expect(col.nullCount).toBe(2);
    expect(col.get(0)).toBeNull();
    expect(col.get(1)).toBeNull();
  });
});
