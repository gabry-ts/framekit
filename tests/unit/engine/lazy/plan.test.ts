import { describe, it, expect } from 'vitest';
import { DataFrame } from '../../../../src/dataframe';
import { col } from '../../../../src/expr/expr';
import type { PlanNode } from '../../../../src/engine/lazy/plan';
import { explainPlan } from '../../../../src/engine/lazy/plan';

describe('LazyFrame', () => {
  const df = DataFrame.fromRows<{ a: number; b: string }>([
    { a: 1, b: 'x' },
    { a: 2, b: 'y' },
    { a: 3, b: 'z' },
  ]);

  it('df.lazy() creates a LazyFrame with a scan node', () => {
    const lf = df.lazy();
    expect(lf._plan.type).toBe('scan');
  });

  it('explain() returns string representation', () => {
    const lf = df.lazy();
    const plan = lf.explain();
    expect(plan).toContain('SCAN');
  });

  it('collect() returns the source DataFrame', async () => {
    const lf = df.lazy();
    const result = await lf.collect();
    expect(result.shape).toEqual([3, 2]);
    expect(result.columns).toEqual(['a', 'b']);
  });

  it('builds filter plan node', () => {
    const lf = df.lazy().filter(col<number>('a').gt(1));
    expect(lf._plan.type).toBe('filter');
    expect(lf.explain()).toContain('FILTER');
    expect(lf.explain()).toContain('SCAN');
  });

  it('builds select plan node', () => {
    const lf = df.lazy().select('a');
    expect(lf._plan.type).toBe('select');
    expect(lf.explain()).toContain('SELECT [a]');
  });

  it('builds sort plan node', () => {
    const lf = df.lazy().sort('a', true);
    expect(lf._plan.type).toBe('sort');
    expect(lf.explain()).toContain('SORT [a DESC]');
  });

  it('builds limit plan node', () => {
    const lf = df.lazy().limit(10);
    expect(lf._plan.type).toBe('limit');
    expect(lf.explain()).toContain('LIMIT [10]');
  });

  it('builds distinct plan node', () => {
    const lf = df.lazy().distinct(['a']);
    expect(lf._plan.type).toBe('distinct');
    expect(lf.explain()).toContain('DISTINCT [a]');
  });

  it('chains multiple operations', () => {
    const lf = df.lazy().filter(col<number>('a').gt(1)).select('a').limit(5);
    expect(lf._plan.type).toBe('limit');
    const plan = lf.explain();
    expect(plan).toContain('LIMIT');
    expect(plan).toContain('SELECT');
    expect(plan).toContain('FILTER');
    expect(plan).toContain('SCAN');
  });

  it('groupBy(...keys).agg(specs) appends groupby node', () => {
    const lf = df.lazy().groupBy('b').agg(col<number>('a'));
    expect(lf._plan.type).toBe('groupby');
    const plan = lf.explain();
    expect(plan).toContain('GROUPBY [keys: b; aggs: a]');
    expect(plan).toContain('SCAN');
  });

  it('groupBy with multiple keys', () => {
    const lf = df.lazy().groupBy('a', 'b').agg(col<number>('a'));
    expect(lf._plan.type).toBe('groupby');
    expect(lf.explain()).toContain('GROUPBY [keys: a, b; aggs: a]');
  });

  it('sortBy(column, order) appends sort node', () => {
    const lf = df.lazy().sortBy('a', true);
    expect(lf._plan.type).toBe('sort');
    expect(lf.explain()).toContain('SORT [a DESC]');
  });

  it('sortBy defaults to ascending', () => {
    const lf = df.lazy().sortBy('a');
    expect(lf.explain()).toContain('SORT [a ASC]');
  });

  it('head(n) appends limit node', () => {
    const lf = df.lazy().head(5);
    expect(lf._plan.type).toBe('limit');
    expect(lf.explain()).toContain('LIMIT [5]');
  });

  it('unique() appends distinct node', () => {
    const lf = df.lazy().unique();
    expect(lf._plan.type).toBe('distinct');
    expect(lf.explain()).toContain('DISTINCT');
  });

  it('unique(subset) appends distinct node with subset', () => {
    const lf = df.lazy().unique(['a']);
    expect(lf._plan.type).toBe('distinct');
    expect(lf.explain()).toContain('DISTINCT [a]');
  });

  it('all methods return new LazyFrame (immutable plan building)', () => {
    const lf = df.lazy();
    const filtered = lf.filter(col<number>('a').gt(1));
    const selected = lf.select('a');
    const grouped = lf.groupBy('b').agg(col<number>('a'));
    const sorted = lf.sortBy('a');
    const headed = lf.head(2);
    const uniqued = lf.unique();

    expect(lf._plan.type).toBe('scan');
    expect(filtered._plan.type).toBe('filter');
    expect(selected._plan.type).toBe('select');
    expect(grouped._plan.type).toBe('groupby');
    expect(sorted._plan.type).toBe('sort');
    expect(headed._plan.type).toBe('limit');
    expect(uniqued._plan.type).toBe('distinct');
  });

  it('no data processing happens until collect()', () => {
    const lf = df
      .lazy()
      .filter(col<number>('a').gt(1))
      .groupBy('b')
      .agg(col<number>('a').sum())
      .sortBy('b')
      .head(10)
      .unique();
    const result = lf.collect();
    expect(result).toBeInstanceOf(Promise);
  });
});

describe('explainPlan', () => {
  it('explains join node', () => {
    const node: PlanNode = {
      type: 'join',
      left: { type: 'scan', id: 0 },
      right: { type: 'scan', id: 1 },
      on: 'id',
      how: 'inner',
    };
    const result = explainPlan(node);
    expect(result).toContain('JOIN [inner]');
    expect(result).toContain('SCAN');
  });

  it('explains project node', () => {
    const node: PlanNode = {
      type: 'project',
      input: { type: 'scan', id: 0 },
      exprs: [col<number>('a'), col<string>('b')],
    };
    const result = explainPlan(node);
    expect(result).toContain('PROJECT [a, b]');
  });

  it('explains groupby node', () => {
    const node: PlanNode = {
      type: 'groupby',
      input: { type: 'scan', id: 0 },
      keys: ['a'],
      aggs: [col<number>('b')],
    };
    const result = explainPlan(node);
    expect(result).toContain('GROUPBY [keys: a; aggs: b]');
  });
});

describe('explain() plan visualization', () => {
  const df = DataFrame.fromRows<{ amount: number; category: string; status: string }>([
    { amount: 100, category: 'A', status: 'active' },
    { amount: 200, category: 'B', status: 'inactive' },
    { amount: 300, category: 'A', status: 'active' },
  ]);

  it('filter node shows predicate expression', () => {
    const plan = df.lazy().filter(col<number>('amount').gt(100)).explain();
    expect(plan).toContain('FILTER [(amount > 100)]');
  });

  it('filter with complex predicate shows full expression', () => {
    const pred = col<number>('amount').gt(100).and(col<string>('status').eq('active'));
    const plan = df.lazy().filter(pred).explain();
    expect(plan).toContain('FILTER [((amount > 100) AND (status == "active"))]');
  });

  it('groupBy shows keys and aggregation specs', () => {
    const plan = df.lazy().groupBy('category').agg(col<number>('amount').sum()).explain();
    expect(plan).toContain('GROUPBY [keys: category; aggs: sum(amount)]');
  });

  it('select node shows column list', () => {
    const plan = df.lazy().select('amount', 'category').explain();
    expect(plan).toContain('SELECT [amount, category]');
  });

  it('sort node shows column and direction', () => {
    const plan = df.lazy().sort('amount', true).explain();
    expect(plan).toContain('SORT [amount DESC]');
  });

  it('scan node shows id', () => {
    const plan = df.lazy().explain();
    expect(plan).toMatch(/SCAN \[id=\d+\]/);
  });

  it('shows both original and optimized plans', () => {
    const plan = df.lazy().filter(col<number>('amount').gt(100)).select('amount').explain();
    expect(plan).toContain('ORIGINAL:');
    expect(plan).toContain('OPTIMIZED:');
  });

  it('plan uses indentation showing nesting', () => {
    const plan = df.lazy().filter(col<number>('amount').gt(100)).select('amount').explain();
    const lines = plan.split('\n');
    const selectLine = lines.find(l => l.includes('SELECT'));
    const filterLine = lines.find(l => l.includes('FILTER'));
    expect(selectLine).toBeDefined();
    expect(filterLine).toBeDefined();
    // child nodes should have more indentation than parent
    const selectIndent = selectLine!.search(/\S/);
    const filterIndent = filterLine!.search(/\S/);
    expect(filterIndent).toBeGreaterThanOrEqual(selectIndent);
  });

  it('distinct node with subset', () => {
    const plan = df.lazy().distinct(['amount']).explain();
    expect(plan).toContain('DISTINCT [amount]');
  });

  it('limit node shows count', () => {
    const plan = df.lazy().limit(5).explain();
    expect(plan).toContain('LIMIT [5]');
  });

  it('project node shows expression details', () => {
    const plan = df.lazy().project(col<number>('amount').mul(2)).explain();
    expect(plan).toContain('PROJECT [(amount * 2)]');
  });
});
