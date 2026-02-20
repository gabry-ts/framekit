import { describe, it, expect } from 'vitest';
import { DataFrame } from '../../../src/index';
import { all, not, range, desc, resolveSelector } from '../../../src/compat/helpers';

describe('compat selectors', () => {
  const df = DataFrame.fromColumns({
    a: [1, 2, 3],
    b: [4, 5, 6],
    c: [7, 8, 9],
    d: [10, 11, 12],
  });
  const cols = df.columns;

  describe('all()', () => {
    it('returns all column names', () => {
      expect(resolveSelector(cols, all())).toEqual(['a', 'b', 'c', 'd']);
    });

    it('returns empty array for empty DataFrame', () => {
      const empty = DataFrame.fromColumns({});
      expect(resolveSelector(empty.columns, all())).toEqual([]);
    });
  });

  describe('not()', () => {
    it('excludes specified columns', () => {
      expect(resolveSelector(cols, not('b', 'd'))).toEqual(['a', 'c']);
    });

    it('excludes a single column', () => {
      expect(resolveSelector(cols, not('a'))).toEqual(['b', 'c', 'd']);
    });

    it('returns all columns when excluding none', () => {
      expect(resolveSelector(cols, not())).toEqual(['a', 'b', 'c', 'd']);
    });

    it('throws FrameKitError for non-existent column', () => {
      expect(() => resolveSelector(cols, not('x'))).toThrow('Column \'x\' not found');
    });

    it('throws with available columns in message', () => {
      expect(() => resolveSelector(cols, not('missing'))).toThrow(
        'Available columns: [a, b, c, d]',
      );
    });
  });

  describe('range()', () => {
    it('selects columns in range inclusive', () => {
      expect(resolveSelector(cols, range('b', 'd'))).toEqual(['b', 'c', 'd']);
    });

    it('selects single column when start equals end', () => {
      expect(resolveSelector(cols, range('b', 'b'))).toEqual(['b']);
    });

    it('works with reversed start/end order', () => {
      expect(resolveSelector(cols, range('d', 'b'))).toEqual(['b', 'c', 'd']);
    });

    it('selects from first to last', () => {
      expect(resolveSelector(cols, range('a', 'd'))).toEqual(['a', 'b', 'c', 'd']);
    });

    it('throws for non-existent start column', () => {
      expect(() => resolveSelector(cols, range('x', 'b'))).toThrow(
        "Range start column 'x' not found",
      );
    });

    it('throws for non-existent end column', () => {
      expect(() => resolveSelector(cols, range('a', 'z'))).toThrow(
        "Range end column 'z' not found",
      );
    });
  });

  describe('desc()', () => {
    it('creates a DescSpec', () => {
      const spec = desc('price');
      expect(spec).toEqual({ kind: 'desc', column: 'price' });
    });
  });

  describe('selector type narrowing', () => {
    it('all() has kind "all"', () => {
      expect(all().kind).toBe('all');
    });

    it('not() has kind "not"', () => {
      expect(not('a').kind).toBe('not');
      expect(not('a').columns).toEqual(['a']);
    });

    it('range() has kind "range"', () => {
      const r = range('a', 'c');
      expect(r.kind).toBe('range');
      expect(r.start).toBe('a');
      expect(r.end).toBe('c');
    });
  });
});
