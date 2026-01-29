import type { DataFrame } from './dataframe';
import type { Expr } from './expr/expr';

// eslint-disable-next-line @typescript-eslint/no-explicit-any
type AnyExpr = Expr<any>;
import type { PlanNode } from './engine/lazy/plan';
import { createScanNode, explainPlan } from './engine/lazy/plan';

export class LazyFrame<S extends Record<string, unknown> = Record<string, unknown>> {
  /** @internal */
  readonly _source: DataFrame<S>;
  /** @internal */
  readonly _plan: PlanNode;

  constructor(source: DataFrame<S>, plan: PlanNode) {
    this._source = source;
    this._plan = plan;
  }

  filter(predicate: AnyExpr): LazyFrame<S> {
    return new LazyFrame<S>(this._source, {
      type: 'filter',
      input: this._plan,
      predicate,
    });
  }

  select(...columns: (string & keyof S)[]): LazyFrame<S> {
    return new LazyFrame<S>(this._source, {
      type: 'select',
      input: this._plan,
      columns,
    });
  }

  project(...exprs: AnyExpr[]): LazyFrame<S> {
    return new LazyFrame<S>(this._source, {
      type: 'project',
      input: this._plan,
      exprs,
    });
  }

  sort(by: string & keyof S, descending = false): LazyFrame<S> {
    return new LazyFrame<S>(this._source, {
      type: 'sort',
      input: this._plan,
      by,
      descending,
    });
  }

  limit(n: number): LazyFrame<S> {
    return new LazyFrame<S>(this._source, {
      type: 'limit',
      input: this._plan,
      n,
    });
  }

  distinct(subset?: (string & keyof S)[]): LazyFrame<S> {
    return new LazyFrame<S>(this._source, {
      type: 'distinct',
      input: this._plan,
      subset,
    });
  }

  explain(): string {
    return explainPlan(this._plan);
  }

  collect(): Promise<DataFrame<S>> {
    return Promise.resolve(this._source);
  }
}

export function createLazyFrame<S extends Record<string, unknown>>(
  source: DataFrame<S>,
): LazyFrame<S> {
  return new LazyFrame<S>(source, createScanNode());
}
