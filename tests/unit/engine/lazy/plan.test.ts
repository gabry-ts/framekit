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
    expect(result).toContain('PROJECT [2 expr(s)]');
  });

  it('explains groupby node', () => {
    const node: PlanNode = {
      type: 'groupby',
      input: { type: 'scan', id: 0 },
      keys: ['a'],
      aggs: [col<number>('b')],
    };
    const result = explainPlan(node);
    expect(result).toContain('GROUPBY [a]');
  });
});
