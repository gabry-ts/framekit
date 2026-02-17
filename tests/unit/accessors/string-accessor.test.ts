import { describe, it, expect } from 'vitest';
import { Series } from '../../../src/series';
import { Utf8Column } from '../../../src/storage/string';
import { Float64Column } from '../../../src/storage/numeric';
import { StringAccessor } from '../../../src/accessors/string-accessor';

function strSeries(values: (string | null)[]): Series<string> {
  return new Series<string>('test', Utf8Column.from(values));
}

describe('StringAccessor', () => {
  describe('construction', () => {
    it('should be accessible via .str property on string Series', () => {
      const s = strSeries(['hello']);
      expect(s.str).toBeInstanceOf(StringAccessor);
    });

    it('should throw on non-string Series', () => {
      const s = new Series<number>('num', Float64Column.from([1, 2]));
      expect(() => s.str).toThrow('Utf8');
    });
  });

  describe('toLowerCase', () => {
    it('should convert to lower case', () => {
      const result = strSeries(['HELLO', 'World', 'foo']).str.toLowerCase();
      expect(result.toArray()).toEqual(['hello', 'world', 'foo']);
    });

    it('should handle nulls', () => {
      const result = strSeries(['HELLO', null, 'FOO']).str.toLowerCase();
      expect(result.toArray()).toEqual(['hello', null, 'foo']);
    });
  });

  describe('toUpperCase', () => {
    it('should convert to upper case', () => {
      const result = strSeries(['hello', 'World']).str.toUpperCase();
      expect(result.toArray()).toEqual(['HELLO', 'WORLD']);
    });

    it('should handle nulls', () => {
      const result = strSeries([null, 'hello']).str.toUpperCase();
      expect(result.toArray()).toEqual([null, 'HELLO']);
    });
  });

  describe('trim', () => {
    it('should trim whitespace', () => {
      const result = strSeries(['  hello  ', '\tworld\n', 'foo']).str.trim();
      expect(result.toArray()).toEqual(['hello', 'world', 'foo']);
    });

    it('should handle nulls', () => {
      const result = strSeries(['  hi  ', null]).str.trim();
      expect(result.toArray()).toEqual(['hi', null]);
    });
  });

  describe('startsWith', () => {
    it('should check prefix', () => {
      const result = strSeries(['hello', 'world', 'help']).str.startsWith('hel');
      expect(result.toArray()).toEqual([true, false, true]);
    });

    it('should handle nulls', () => {
      const result = strSeries(['hello', null]).str.startsWith('hel');
      expect(result.toArray()).toEqual([true, null]);
    });
  });

  describe('endsWith', () => {
    it('should check suffix', () => {
      const result = strSeries(['hello', 'world', 'mango']).str.endsWith('lo');
      expect(result.toArray()).toEqual([true, false, false]);
    });

    it('should handle nulls', () => {
      const result = strSeries([null, 'hello']).str.endsWith('lo');
      expect(result.toArray()).toEqual([null, true]);
    });
  });

  describe('contains', () => {
    it('should check containment', () => {
      const result = strSeries(['hello world', 'foo bar', 'baz']).str.contains('oo');
      expect(result.toArray()).toEqual([false, true, false]);
    });

    it('should handle nulls', () => {
      const result = strSeries(['hello', null, 'foo']).str.contains('ell');
      expect(result.toArray()).toEqual([true, null, false]);
    });
  });

  describe('replace', () => {
    it('should replace occurrences', () => {
      const result = strSeries(['hello world', 'foo bar']).str.replace('o', '0');
      expect(result.toArray()).toEqual(['hell0 w0rld', 'f00 bar']);
    });

    it('should handle nulls', () => {
      const result = strSeries(['hello', null]).str.replace('l', 'r');
      expect(result.toArray()).toEqual(['herro', null]);
    });
  });

  describe('split', () => {
    it('should split strings', () => {
      const result = strSeries(['a,b,c', 'x,y']).str.split(',');
      expect(result.get(0)).toEqual(['a', 'b', 'c']);
      expect(result.get(1)).toEqual(['x', 'y']);
    });

    it('should handle nulls', () => {
      const result = strSeries(['a,b', null, 'c']).str.split(',');
      expect(result.get(0)).toEqual(['a', 'b']);
      expect(result.get(1)).toBeNull();
      expect(result.get(2)).toEqual(['c']);
    });
  });

  describe('slice', () => {
    it('should slice strings with start and end', () => {
      const result = strSeries(['hello', 'world']).str.slice(1, 3);
      expect(result.toArray()).toEqual(['el', 'or']);
    });

    it('should slice with only start', () => {
      const result = strSeries(['hello', 'world']).str.slice(2);
      expect(result.toArray()).toEqual(['llo', 'rld']);
    });

    it('should handle nulls', () => {
      const result = strSeries(['hello', null]).str.slice(0, 2);
      expect(result.toArray()).toEqual(['he', null]);
    });
  });

  describe('length', () => {
    it('should return string lengths', () => {
      const result = strSeries(['hello', 'hi', '']).str.length();
      expect(result.toArray()).toEqual([5, 2, 0]);
    });

    it('should handle nulls', () => {
      const result = strSeries(['hello', null, 'hi']).str.length();
      expect(result.toArray()).toEqual([5, null, 2]);
    });
  });

  describe('padStart', () => {
    it('should pad start of strings', () => {
      const result = strSeries(['hi', 'hello']).str.padStart(5, '0');
      expect(result.toArray()).toEqual(['000hi', 'hello']);
    });

    it('should handle nulls', () => {
      const result = strSeries(['hi', null]).str.padStart(4, '*');
      expect(result.toArray()).toEqual(['**hi', null]);
    });
  });

  describe('padEnd', () => {
    it('should pad end of strings', () => {
      const result = strSeries(['hi', 'hello']).str.padEnd(5, '0');
      expect(result.toArray()).toEqual(['hi000', 'hello']);
    });

    it('should handle nulls', () => {
      const result = strSeries([null, 'hi']).str.padEnd(4, '-');
      expect(result.toArray()).toEqual([null, 'hi--']);
    });
  });

  describe('extract', () => {
    it('should extract first capture group from regex', () => {
      const result = strSeries(['abc123', 'def456', 'ghi789']).str.extract(/([0-9]+)/);
      expect(result.toArray()).toEqual(['123', '456', '789']);
    });

    it('should return null for non-matching strings', () => {
      const result = strSeries(['abc123', 'nodigits', 'xyz789']).str.extract(/([0-9]+)/);
      expect(result.toArray()).toEqual(['123', null, '789']);
    });

    it('should handle null values', () => {
      const result = strSeries(['abc123', null, 'xyz789']).str.extract(/([0-9]+)/);
      expect(result.toArray()).toEqual(['123', null, '789']);
    });

    it('should throw if pattern has no capture group', () => {
      expect(() => strSeries(['abc']).str.extract(/[0-9]+/)).toThrow('capture group');
    });

    it('should not throw for escaped parentheses', () => {
      expect(() => strSeries(['abc(1)']).str.extract(/\([0-9]+\)/)).toThrow('capture group');
    });

    it('should extract first capture group only', () => {
      const result = strSeries(['2024-01-15']).str.extract(/(\d{4})-(\d{2})-(\d{2})/);
      expect(result.toArray()).toEqual(['2024']);
    });
  });

  describe('chaining', () => {
    it('should support method chaining', () => {
      const result = strSeries(['  Hello World  ', '  Foo  '])
        .str.trim()
        .str.toLowerCase()
        .str.replace(' ', '_');
      expect(result.toArray()).toEqual(['hello_world', 'foo']);
    });
  });
});
