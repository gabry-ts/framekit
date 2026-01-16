import { describe, it, expect } from 'vitest';
import { DataFrame, col, lit, NamedExpr } from '../../../src/index';

type TestRow = {
  x: number;
  y: number;
  name: string;
  active: boolean;
};

const testDf = DataFrame.fromRows<TestRow>([
  { x: 10, y: 2, name: 'a', active: true },
  { x: 20, y: 5, name: 'b', active: false },
  { x: 30, y: 3, name: 'c', active: true },
  { x: 40, y: 8, name: 'd', active: false },
  { x: 50, y: 1, name: 'e', active: true },
]);

type NullRow = {
  a: number;
  b: number;
};

const nullDf = DataFrame.fromRows<NullRow>([
  { a: 1, b: 10 },
  { a: null as unknown as number, b: 20 },
  { a: 3, b: null as unknown as number },
  { a: null as unknown as number, b: null as unknown as number },
  { a: 5, b: 50 },
]);

describe('Expression engine', () => {
  // ── col() ──
  describe('col()', () => {
    it('references the correct column values', () => {
      const expr = col<number>('x');
      const result = expr.evaluate(testDf);
      expect(result.toArray()).toEqual([10, 20, 30, 40, 50]);
    });

    it('references string column', () => {
      const result = col<string>('name').evaluate(testDf);
      expect(result.toArray()).toEqual(['a', 'b', 'c', 'd', 'e']);
    });

    it('references boolean column', () => {
      const result = col<boolean>('active').evaluate(testDf);
      expect(result.toArray()).toEqual([true, false, true, false, true]);
    });

    it('throws on missing column', () => {
      const expr = col('nonexistent');
      expect(() => expr.evaluate(testDf)).toThrow();
    });

    it('reports dependencies correctly', () => {
      const expr = col('x');
      expect(expr.dependencies).toEqual(['x']);
    });
  });

  // ── lit() ──
  describe('lit()', () => {
    it('wraps numeric scalar', () => {
      const result = lit(42).evaluate(testDf);
      expect(result.length).toBe(5);
      expect(result.toArray()).toEqual([42, 42, 42, 42, 42]);
    });

    it('wraps string scalar', () => {
      const result = lit('hello').evaluate(testDf);
      expect(result.toArray()).toEqual(['hello', 'hello', 'hello', 'hello', 'hello']);
    });

    it('wraps boolean scalar', () => {
      const result = lit(true).evaluate(testDf);
      expect(result.toArray()).toEqual([true, true, true, true, true]);
    });

    it('has no dependencies', () => {
      expect(lit(42).dependencies).toEqual([]);
    });
  });

  // ── Arithmetic ──
  describe('arithmetic operations', () => {
    it('add two columns', () => {
      const result = col<number>('x').add(col<number>('y')).evaluate(testDf);
      expect(result.toArray()).toEqual([12, 25, 33, 48, 51]);
    });

    it('add column and scalar', () => {
      const result = col<number>('x').add(100).evaluate(testDf);
      expect(result.toArray()).toEqual([110, 120, 130, 140, 150]);
    });

    it('sub', () => {
      const result = col<number>('x').sub(col<number>('y')).evaluate(testDf);
      expect(result.toArray()).toEqual([8, 15, 27, 32, 49]);
    });

    it('mul', () => {
      const result = col<number>('x').mul(col<number>('y')).evaluate(testDf);
      expect(result.toArray()).toEqual([20, 100, 90, 320, 50]);
    });

    it('div', () => {
      const result = col<number>('x').div(col<number>('y')).evaluate(testDf);
      expect(result.toArray()).toEqual([5, 4, 10, 5, 50]);
    });

    it('mod', () => {
      const result = col<number>('x').mod(3).evaluate(testDf);
      expect(result.toArray()).toEqual([1, 2, 0, 1, 2]);
    });

    it('pow', () => {
      const result = col<number>('y').pow(2).evaluate(testDf);
      expect(result.toArray()).toEqual([4, 25, 9, 64, 1]);
    });

    it('chained arithmetic', () => {
      // (x + y) * 2
      const result = col<number>('x').add(col<number>('y')).mul(2).evaluate(testDf);
      expect(result.toArray()).toEqual([24, 50, 66, 96, 102]);
    });

    it('null propagation in arithmetic', () => {
      const result = col<number>('a').add(col<number>('b')).evaluate(nullDf);
      expect(result.toArray()).toEqual([11, null, null, null, 55]);
    });

    it('deduplicates dependencies', () => {
      const expr = col<number>('x').add(col<number>('x'));
      expect(expr.dependencies).toEqual(['x']);
    });

    it('lists dependencies from both sides', () => {
      const expr = col<number>('x').mul(col<number>('y'));
      expect(expr.dependencies.sort()).toEqual(['x', 'y']);
    });
  });

  // ── Comparison ──
  describe('comparison operations', () => {
    it('eq with scalar', () => {
      const result = col<number>('x').eq(30).evaluate(testDf);
      expect(result.toArray()).toEqual([false, false, true, false, false]);
    });

    it('neq with scalar', () => {
      const result = col<number>('x').neq(30).evaluate(testDf);
      expect(result.toArray()).toEqual([true, true, false, true, true]);
    });

    it('gt', () => {
      const result = col<number>('x').gt(25).evaluate(testDf);
      expect(result.toArray()).toEqual([false, false, true, true, true]);
    });

    it('gte', () => {
      const result = col<number>('x').gte(30).evaluate(testDf);
      expect(result.toArray()).toEqual([false, false, true, true, true]);
    });

    it('lt', () => {
      const result = col<number>('x').lt(30).evaluate(testDf);
      expect(result.toArray()).toEqual([true, true, false, false, false]);
    });

    it('lte', () => {
      const result = col<number>('x').lte(30).evaluate(testDf);
      expect(result.toArray()).toEqual([true, true, true, false, false]);
    });

    it('eq with string column', () => {
      const result = col<string>('name').eq('c').evaluate(testDf);
      expect(result.toArray()).toEqual([false, false, true, false, false]);
    });

    it('eq comparing two columns', () => {
      const df = DataFrame.fromRows([
        { a: 1, b: 1 },
        { a: 2, b: 3 },
        { a: 3, b: 3 },
      ]);
      const result = col<number>('a').eq(col<number>('b')).evaluate(df);
      expect(result.toArray()).toEqual([true, false, true]);
    });

    it('null propagation in comparison', () => {
      const result = col<number>('a').gt(2).evaluate(nullDf);
      expect(result.toArray()).toEqual([false, null, true, null, true]);
    });
  });

  // ── Logical ──
  describe('logical operations', () => {
    it('and combines two boolean expressions', () => {
      const result = col<number>('x')
        .gt(15)
        .and(col<number>('x').lt(45))
        .evaluate(testDf);
      expect(result.toArray()).toEqual([false, true, true, true, false]);
    });

    it('or combines two boolean expressions', () => {
      const result = col<number>('x')
        .lt(15)
        .or(col<number>('x').gt(45))
        .evaluate(testDf);
      expect(result.toArray()).toEqual([true, false, false, false, true]);
    });

    it('not negates a boolean expression', () => {
      const result = col<boolean>('active').not().evaluate(testDf);
      expect(result.toArray()).toEqual([false, true, false, true, false]);
    });

    it('and with literal boolean', () => {
      const result = col<boolean>('active').and(false).evaluate(testDf);
      expect(result.toArray()).toEqual([false, false, false, false, false]);
    });

    it('or with literal boolean', () => {
      const result = col<boolean>('active').or(true).evaluate(testDf);
      expect(result.toArray()).toEqual([true, true, true, true, true]);
    });

    it('complex logical chain: (x > 20) and (y < 5 or active)', () => {
      const result = col<number>('x')
        .gt(20)
        .and(col<number>('y').lt(5).or(col<boolean>('active')))
        .evaluate(testDf);
      // x>20: [f,f,t,t,t], y<5: [t,f,t,f,t], active: [t,f,t,f,t]
      // y<5 or active: [t,f,t,f,t]
      // and: [f,f,t,f,t]
      expect(result.toArray()).toEqual([false, false, true, false, true]);
    });

    it('null propagation in logical and', () => {
      // a > 2 has nulls at index 1,3
      const result = col<number>('a').gt(2).and(col<number>('b').gt(15)).evaluate(nullDf);
      // a>2: [f, null, t, null, t], b>15: [f, t, null, null, t]
      // and: [f, null, null, null, t]
      expect(result.toArray()).toEqual([false, null, null, null, true]);
    });

    it('not with null propagation', () => {
      const result = col<number>('a').gt(2).not().evaluate(nullDf);
      // a>2: [f, null, t, null, t] → not: [t, null, f, null, f]
      expect(result.toArray()).toEqual([true, null, false, null, false]);
    });
  });

  // ── NamedExpr / as() ──
  describe('as() aliasing', () => {
    it('creates NamedExpr with correct name', () => {
      const named = col<number>('x').add(1).as('x_plus_1');
      expect(named).toBeInstanceOf(NamedExpr);
      expect(named.name).toBe('x_plus_1');
    });

    it('NamedExpr preserves dependencies', () => {
      const named = col<number>('x').mul(col<number>('y')).as('product');
      expect(named.dependencies.sort()).toEqual(['x', 'y']);
    });
  });

  // ── Expression-based filter on DataFrame ──
  describe('df.filter() with expression', () => {
    it('filters with simple comparison', () => {
      const result = testDf.filter(col<number>('x').gt(25));
      expect(result.length).toBe(3);
      expect(result.col('x').toArray()).toEqual([30, 40, 50]);
    });

    it('filters with combined expression', () => {
      const result = testDf.filter(
        col<string>('name').eq('a').or(col<string>('name').eq('e')),
      );
      expect(result.length).toBe(2);
      expect(result.col('name').toArray()).toEqual(['a', 'e']);
    });

    it('expression filter matches predicate filter', () => {
      const exprResult = testDf.filter(col<number>('x').gte(30));
      const predResult = testDf.filter((row) => row.x >= 30);
      expect(exprResult.toArray()).toEqual(predResult.toArray());
    });

    it('returns empty df when no match', () => {
      const result = testDf.filter(col<number>('x').gt(999));
      expect(result.length).toBe(0);
      expect(result.columns).toEqual(testDf.columns);
    });
  });

  // ── Expression-based withColumn on DataFrame ──
  describe('df.withColumn() with expression', () => {
    it('adds computed column', () => {
      const result = testDf.withColumn('sum', col<number>('x').add(col<number>('y')));
      expect(result.col('sum' as never).toArray()).toEqual([12, 25, 33, 48, 51]);
    });

    it('replaces column via expression', () => {
      const result = testDf.withColumn('x', col<number>('x').mul(10));
      expect(result.col('x').toArray()).toEqual([100, 200, 300, 400, 500]);
      expect(result.columns.length).toBe(testDf.columns.length);
    });

    it('adds column using lit()', () => {
      const result = testDf.withColumn('const', lit(99));
      expect(result.col('const' as never).toArray()).toEqual([99, 99, 99, 99, 99]);
    });
  });

  // ── where() shorthand ──
  describe('where() shorthand', () => {
    it('= operator', () => {
      const result = testDf.where('name', '=', 'c');
      expect(result.length).toBe(1);
      expect(result.row(0).name).toBe('c');
    });

    it('!= operator', () => {
      const result = testDf.where('active', '!=', false);
      expect(result.length).toBe(3);
    });

    it('> operator', () => {
      const result = testDf.where('x', '>', 30);
      expect(result.length).toBe(2);
    });

    it('>= operator', () => {
      const result = testDf.where('x', '>=', 30);
      expect(result.length).toBe(3);
    });

    it('< operator', () => {
      const result = testDf.where('y', '<', 3);
      expect(result.length).toBe(2);
    });

    it('<= operator', () => {
      const result = testDf.where('y', '<=', 3);
      expect(result.length).toBe(3);
    });

    it('where matches equivalent filter expression', () => {
      const whereResult = testDf.where('x', '>=', 30);
      const filterResult = testDf.filter(col<number>('x').gte(30));
      expect(whereResult.toArray()).toEqual(filterResult.toArray());
    });
  });
});
