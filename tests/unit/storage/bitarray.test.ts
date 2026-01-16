import { describe, it, expect } from 'vitest';
import { BitArray } from '../../../src/storage/bitarray';

describe('BitArray', () => {
  describe('constructor', () => {
    it('creates a BitArray with all false by default', () => {
      const bits = new BitArray(10);
      expect(bits.length).toBe(10);
      for (let i = 0; i < 10; i++) {
        expect(bits.get(i)).toBe(false);
      }
    });

    it('creates a BitArray with all true when initialValue is true', () => {
      const bits = new BitArray(10, true);
      expect(bits.length).toBe(10);
      for (let i = 0; i < 10; i++) {
        expect(bits.get(i)).toBe(true);
      }
    });

    it('creates an empty BitArray', () => {
      const bits = new BitArray(0);
      expect(bits.length).toBe(0);
      expect(bits.countOnes()).toBe(0);
      expect(bits.countZeros()).toBe(0);
    });

    it('throws on negative length', () => {
      expect(() => new BitArray(-1)).toThrow('non-negative');
    });

    it('creates a single-bit BitArray (false)', () => {
      const bits = new BitArray(1);
      expect(bits.length).toBe(1);
      expect(bits.get(0)).toBe(false);
      expect(bits.countOnes()).toBe(0);
      expect(bits.countZeros()).toBe(1);
    });

    it('creates a single-bit BitArray (true)', () => {
      const bits = new BitArray(1, true);
      expect(bits.length).toBe(1);
      expect(bits.get(0)).toBe(true);
      expect(bits.countOnes()).toBe(1);
      expect(bits.countZeros()).toBe(0);
    });
  });

  describe('get/set', () => {
    it('sets and gets individual bits', () => {
      const bits = new BitArray(16);
      bits.set(0, true);
      bits.set(7, true);
      bits.set(15, true);
      expect(bits.get(0)).toBe(true);
      expect(bits.get(1)).toBe(false);
      expect(bits.get(7)).toBe(true);
      expect(bits.get(15)).toBe(true);
    });

    it('can unset bits', () => {
      const bits = new BitArray(8, true);
      bits.set(3, false);
      expect(bits.get(3)).toBe(false);
      expect(bits.get(2)).toBe(true);
    });

    it('handles single-bit toggle', () => {
      const bits = new BitArray(1);
      expect(bits.get(0)).toBe(false);
      bits.set(0, true);
      expect(bits.get(0)).toBe(true);
      bits.set(0, false);
      expect(bits.get(0)).toBe(false);
    });

    it('sets bits across byte boundaries', () => {
      const bits = new BitArray(20);
      bits.set(7, true); // last bit of byte 0
      bits.set(8, true); // first bit of byte 1
      bits.set(15, true); // last bit of byte 1
      bits.set(16, true); // first bit of byte 2
      expect(bits.get(7)).toBe(true);
      expect(bits.get(8)).toBe(true);
      expect(bits.get(15)).toBe(true);
      expect(bits.get(16)).toBe(true);
      expect(bits.get(6)).toBe(false);
      expect(bits.get(9)).toBe(false);
    });

    it('throws on out-of-bounds get', () => {
      const bits = new BitArray(8);
      expect(() => bits.get(8)).toThrow('out of bounds');
      expect(() => bits.get(-1)).toThrow('out of bounds');
    });

    it('throws on out-of-bounds set', () => {
      const bits = new BitArray(8);
      expect(() => bits.set(8, true)).toThrow('out of bounds');
      expect(() => bits.set(-1, true)).toThrow('out of bounds');
    });

    it('throws on get/set for empty BitArray', () => {
      const bits = new BitArray(0);
      expect(() => bits.get(0)).toThrow('out of bounds');
      expect(() => bits.set(0, true)).toThrow('out of bounds');
    });
  });

  describe('countOnes / countZeros', () => {
    it('counts ones correctly', () => {
      const bits = new BitArray(10);
      bits.set(0, true);
      bits.set(5, true);
      bits.set(9, true);
      expect(bits.countOnes()).toBe(3);
      expect(bits.countZeros()).toBe(7);
    });

    it('all true', () => {
      const bits = new BitArray(8, true);
      expect(bits.countOnes()).toBe(8);
      expect(bits.countZeros()).toBe(0);
    });

    it('all false', () => {
      const bits = new BitArray(8);
      expect(bits.countOnes()).toBe(0);
      expect(bits.countZeros()).toBe(8);
    });

    it('counts correctly for large arrays', () => {
      const bits = new BitArray(1000);
      for (let i = 0; i < 1000; i += 2) {
        bits.set(i, true);
      }
      expect(bits.countOnes()).toBe(500);
      expect(bits.countZeros()).toBe(500);
    });

    it('counts correctly for non-byte-aligned length', () => {
      const bits = new BitArray(11, true);
      expect(bits.countOnes()).toBe(11);
      bits.set(10, false);
      expect(bits.countOnes()).toBe(10);
      expect(bits.countZeros()).toBe(1);
    });
  });

  describe('bitwise operations', () => {
    it('and() returns correct result', () => {
      const a = new BitArray(8);
      const b = new BitArray(8);
      a.set(0, true);
      a.set(1, true);
      b.set(1, true);
      b.set(2, true);
      const result = a.and(b);
      expect(result.get(0)).toBe(false);
      expect(result.get(1)).toBe(true);
      expect(result.get(2)).toBe(false);
    });

    it('or() returns correct result', () => {
      const a = new BitArray(8);
      const b = new BitArray(8);
      a.set(0, true);
      b.set(1, true);
      const result = a.or(b);
      expect(result.get(0)).toBe(true);
      expect(result.get(1)).toBe(true);
      expect(result.get(2)).toBe(false);
    });

    it('not() returns correct result', () => {
      const bits = new BitArray(4);
      bits.set(0, true);
      bits.set(2, true);
      const result = bits.not();
      expect(result.get(0)).toBe(false);
      expect(result.get(1)).toBe(true);
      expect(result.get(2)).toBe(false);
      expect(result.get(3)).toBe(true);
    });

    it('and() throws on length mismatch', () => {
      const a = new BitArray(8);
      const b = new BitArray(16);
      expect(() => a.and(b)).toThrow('length mismatch');
    });

    it('or() throws on length mismatch', () => {
      const a = new BitArray(8);
      const b = new BitArray(16);
      expect(() => a.or(b)).toThrow('length mismatch');
    });

    it('and/or/not work on empty arrays', () => {
      const a = new BitArray(0);
      const b = new BitArray(0);
      expect(a.and(b).length).toBe(0);
      expect(a.or(b).length).toBe(0);
      expect(a.not().length).toBe(0);
    });

    it('not() of not() returns original values', () => {
      const bits = new BitArray(16);
      bits.set(3, true);
      bits.set(7, true);
      bits.set(12, true);
      const doubleNot = bits.not().not();
      for (let i = 0; i < 16; i++) {
        expect(doubleNot.get(i)).toBe(bits.get(i));
      }
    });

    it('bitwise ops on large arrays', () => {
      const a = new BitArray(256, true);
      const b = new BitArray(256);
      for (let i = 0; i < 256; i += 3) {
        b.set(i, true);
      }
      const andResult = a.and(b);
      const orResult = a.or(b);
      expect(andResult.countOnes()).toBe(b.countOnes());
      expect(orResult.countOnes()).toBe(256);
    });
  });

  describe('clone', () => {
    it('creates an independent copy', () => {
      const bits = new BitArray(8);
      bits.set(3, true);
      const cloned = bits.clone();
      expect(cloned.get(3)).toBe(true);
      cloned.set(3, false);
      expect(bits.get(3)).toBe(true);
      expect(cloned.get(3)).toBe(false);
    });

    it('clones empty BitArray', () => {
      const bits = new BitArray(0);
      const cloned = bits.clone();
      expect(cloned.length).toBe(0);
    });

    it('clones large BitArray', () => {
      const bits = new BitArray(500, true);
      bits.set(250, false);
      const cloned = bits.clone();
      expect(cloned.length).toBe(500);
      expect(cloned.get(250)).toBe(false);
      expect(cloned.countOnes()).toBe(499);
    });
  });

  describe('length property', () => {
    it('returns the total number of bits', () => {
      expect(new BitArray(0).length).toBe(0);
      expect(new BitArray(1).length).toBe(1);
      expect(new BitArray(100).length).toBe(100);
    });
  });
});
