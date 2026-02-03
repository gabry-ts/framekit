import { describe, it, expect } from 'vitest';
import { DataFrame, col } from '../../../src';

describe('Window ranking functions', () => {
  const df = DataFrame.fromRows([
    { name: 'Alice', amount: 100 },
    { name: 'Bob', amount: 200 },
    { name: 'Charlie', amount: 200 },
    { name: 'Dave', amount: 300 },
    { name: 'Eve', amount: 100 },
  ]);

  describe('rank()', () => {
    it('should compute 1-based rank with gaps after ties', () => {
      const result = df.withColumn('r', col('amount').rank());
      const ranks = [];
      for (let i = 0; i < result.length; i++) {
        ranks.push(result.col('r').get(i));
      }
      // amounts: [100, 200, 200, 300, 100]
      // sorted: 100(0), 100(4), 200(1), 200(2), 300(3)
      // ranks:  1,       1,      3,      3,      5
      // by original index: [1, 3, 3, 5, 1]
      expect(ranks).toEqual([1, 3, 3, 5, 1]);
    });
  });

  describe('denseRank()', () => {
    it('should compute dense rank without gaps', () => {
      const result = df.withColumn('r', col('amount').denseRank());
      const ranks = [];
      for (let i = 0; i < result.length; i++) {
        ranks.push(result.col('r').get(i));
      }
      // sorted: 100(0), 100(4), 200(1), 200(2), 300(3)
      // dense ranks: 1, 1, 2, 2, 3
      // by original index: [1, 2, 2, 3, 1]
      expect(ranks).toEqual([1, 2, 2, 3, 1]);
    });
  });

  describe('rowNumber()', () => {
    it('should assign sequential 1-based row numbers by sort order', () => {
      const result = df.withColumn('r', col('amount').rowNumber());
      const ranks = [];
      for (let i = 0; i < result.length; i++) {
        ranks.push(result.col('r').get(i));
      }
      // sorted: 100(idx0), 100(idx4), 200(idx1), 200(idx2), 300(idx3)
      // row numbers: 1, 2, 3, 4, 5
      // by original index: idx0→1, idx1→3, idx2→4, idx3→5, idx4→2
      expect(ranks).toEqual([1, 3, 4, 5, 2]);
    });
  });

  describe('percentRank()', () => {
    it('should compute percent rank in [0, 1]', () => {
      const result = df.withColumn('r', col('amount').percentRank());
      const ranks = [];
      for (let i = 0; i < result.length; i++) {
        ranks.push(result.col('r').get(i));
      }
      // rank: [1, 3, 3, 5, 1]
      // percentRank = (rank - 1) / (n - 1) = (rank - 1) / 4
      // idx0: (1-1)/4=0, idx1: (3-1)/4=0.5, idx2: (3-1)/4=0.5, idx3: (5-1)/4=1, idx4: (1-1)/4=0
      expect(ranks).toEqual([0, 0.5, 0.5, 1, 0]);
    });

    it('should return 0 for single-element', () => {
      const single = DataFrame.fromRows([{ amount: 42 }]);
      const result = single.withColumn('r', col('amount').percentRank());
      expect(result.col('r').get(0)).toBe(0);
    });
  });

  describe('ntile()', () => {
    it('should distribute rows into N buckets', () => {
      const result = df.withColumn('r', col('amount').ntile(2));
      const ranks = [];
      for (let i = 0; i < result.length; i++) {
        ranks.push(result.col('r').get(i));
      }
      // sorted: 100(0), 100(4), 200(1), 200(2), 300(3)
      // ntile(2) for 5 rows: first 3 get bucket 1, last 2 get bucket 2
      // floor(i*2/5)+1: i=0→1, i=1→1, i=2→1, i=3→2, i=4→2
      // mapped back: idx0→1, idx4→1, idx1→1, idx2→2, idx3→2
      // by original index: [1, 1, 2, 2, 1]
      expect(ranks).toEqual([1, 1, 2, 2, 1]);
    });

    it('should handle ntile(4) for quartiles', () => {
      const df8 = DataFrame.fromRows([
        { v: 1 }, { v: 2 }, { v: 3 }, { v: 4 },
        { v: 5 }, { v: 6 }, { v: 7 }, { v: 8 },
      ]);
      const result = df8.withColumn('q', col('v').ntile(4));
      const buckets = [];
      for (let i = 0; i < result.length; i++) {
        buckets.push(result.col('q').get(i));
      }
      // Already sorted, so floor(i*4/8)+1 = floor(i/2)+1
      // i=0→1, i=1→1, i=2→2, i=3→2, i=4→3, i=5→3, i=6→4, i=7→4
      expect(buckets).toEqual([1, 1, 2, 2, 3, 3, 4, 4]);
    });
  });

  describe('with null values', () => {
    it('should handle nulls (sorted last)', () => {
      const dfNull = DataFrame.fromRows([
        { amount: 100 },
        { amount: null },
        { amount: 200 },
      ]);
      const result = dfNull.withColumn('r', col('amount').rank());
      const ranks = [];
      for (let i = 0; i < result.length; i++) {
        ranks.push(result.col('r').get(i));
      }
      // sorted: 100(0), 200(2), null(1)
      // rank: 1, 2, 3
      // by original index: [1, 3, 2]
      expect(ranks).toEqual([1, 3, 2]);
    });
  });

  describe('with string values', () => {
    it('should rank strings alphabetically', () => {
      const dfStr = DataFrame.fromRows([
        { name: 'Charlie' },
        { name: 'Alice' },
        { name: 'Bob' },
      ]);
      const result = dfStr.withColumn('r', col('name').rank());
      const ranks = [];
      for (let i = 0; i < result.length; i++) {
        ranks.push(result.col('r').get(i));
      }
      // sorted: Alice(1), Bob(2), Charlie(0)
      // rank: 1, 2, 3
      // by original index: [3, 1, 2]
      expect(ranks).toEqual([3, 1, 2]);
    });
  });
});

describe('Cumulative window functions', () => {
  const df = DataFrame.fromRows([
    { amount: 10 },
    { amount: 20 },
    { amount: 30 },
    { amount: 40 },
    { amount: 50 },
  ]);

  function getValues(result: DataFrame, colName: string): (number | null)[] {
    const vals: (number | null)[] = [];
    for (let i = 0; i < result.length; i++) {
      vals.push(result.col(colName).get(i) as number | null);
    }
    return vals;
  }

  describe('cumSum()', () => {
    it('should compute running sum', () => {
      const result = df.withColumn('cs', col('amount').cumSum());
      expect(getValues(result, 'cs')).toEqual([10, 30, 60, 100, 150]);
    });

    it('should skip nulls in running sum', () => {
      const dfNull = DataFrame.fromRows([
        { amount: 10 },
        { amount: null },
        { amount: 30 },
      ]);
      const result = dfNull.withColumn('cs', col('amount').cumSum());
      expect(getValues(result, 'cs')).toEqual([10, 10, 40]);
    });
  });

  describe('cumMax()', () => {
    it('should compute running maximum', () => {
      const dfMixed = DataFrame.fromRows([
        { amount: 30 },
        { amount: 10 },
        { amount: 50 },
        { amount: 20 },
      ]);
      const result = dfMixed.withColumn('cm', col('amount').cumMax());
      expect(getValues(result, 'cm')).toEqual([30, 30, 50, 50]);
    });

    it('should skip nulls in running max', () => {
      const dfNull = DataFrame.fromRows([
        { amount: null },
        { amount: 10 },
        { amount: 5 },
      ]);
      const result = dfNull.withColumn('cm', col('amount').cumMax());
      expect(getValues(result, 'cm')).toEqual([null, 10, 10]);
    });
  });

  describe('cumMin()', () => {
    it('should compute running minimum', () => {
      const dfMixed = DataFrame.fromRows([
        { amount: 30 },
        { amount: 10 },
        { amount: 50 },
        { amount: 20 },
      ]);
      const result = dfMixed.withColumn('cm', col('amount').cumMin());
      expect(getValues(result, 'cm')).toEqual([30, 10, 10, 10]);
    });

    it('should skip nulls in running min', () => {
      const dfNull = DataFrame.fromRows([
        { amount: null },
        { amount: 10 },
        { amount: 5 },
      ]);
      const result = dfNull.withColumn('cm', col('amount').cumMin());
      expect(getValues(result, 'cm')).toEqual([null, 10, 5]);
    });
  });

  describe('cumProd()', () => {
    it('should compute running product', () => {
      const result = DataFrame.fromRows([
        { amount: 2 },
        { amount: 3 },
        { amount: 4 },
      ]).withColumn('cp', col('amount').cumProd());
      expect(getValues(result, 'cp')).toEqual([2, 6, 24]);
    });

    it('should skip nulls in running product', () => {
      const dfNull = DataFrame.fromRows([
        { amount: 2 },
        { amount: null },
        { amount: 5 },
      ]);
      const result = dfNull.withColumn('cp', col('amount').cumProd());
      expect(getValues(result, 'cp')).toEqual([2, 2, 10]);
    });
  });

  describe('cumCount()', () => {
    it('should compute running count excluding nulls', () => {
      const result = df.withColumn('cc', col('amount').cumCount());
      expect(getValues(result, 'cc')).toEqual([1, 2, 3, 4, 5]);
    });

    it('should skip nulls in running count', () => {
      const dfNull = DataFrame.fromRows([
        { amount: 10 },
        { amount: null },
        { amount: 30 },
        { amount: null },
        { amount: 50 },
      ]);
      const result = dfNull.withColumn('cc', col('amount').cumCount());
      expect(getValues(result, 'cc')).toEqual([1, 1, 2, 2, 3]);
    });
  });
});
