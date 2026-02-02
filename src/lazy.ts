import type { DataFrame } from './dataframe';
import type { Expr } from './expr/expr';

// eslint-disable-next-line @typescript-eslint/no-explicit-any
type AnyExpr = Expr<any>;
import type { PlanNode } from './engine/lazy/plan';
import { createScanNode, explainPlan } from './engine/lazy/plan';
import { execute } from './engine/lazy/executor';
import { optimize } from './engine/lazy/optimizer';

export class LazyGroupBy<S extends Record<string, unknown> = Record<string, unknown>> {
  /** @internal */
  readonly _source: DataFrame<S>;
  /** @internal */
  readonly _plan: PlanNode;
  /** @internal */
  readonly _keys: readonly string[];

  constructor(source: DataFrame<S>, plan: PlanNode, keys: readonly string[]) {
    this._source = source;
    this._plan = plan;
    this._keys = keys;
  }

  agg(...specs: AnyExpr[]): LazyFrame<S> {
    return new LazyFrame<S>(this._source, {
      type: 'groupby',
      input: this._plan,
      keys: [...this._keys],
      aggs: specs,
    });
  }
}

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

  sortBy(by: string & keyof S, descending = false): LazyFrame<S> {
    return this.sort(by, descending);
  }

  limit(n: number): LazyFrame<S> {
    return new LazyFrame<S>(this._source, {
      type: 'limit',
      input: this._plan,
      n,
    });
  }

  head(n: number): LazyFrame<S> {
    return this.limit(n);
  }

  distinct(subset?: (string & keyof S)[]): LazyFrame<S> {
    return new LazyFrame<S>(this._source, {
      type: 'distinct',
      input: this._plan,
      subset,
    });
  }

  unique(subset?: (string & keyof S)[]): LazyFrame<S> {
    return this.distinct(subset);
  }

  groupBy(...keys: (string & keyof S)[]): LazyGroupBy<S> {
    return new LazyGroupBy<S>(this._source, this._plan, keys);
  }

  explain(): string {
    const original = explainPlan(this._plan);
    const optimizedPlan = optimize(this._plan);
    const optimized = explainPlan(optimizedPlan);
    return `ORIGINAL:\n${original}\n\nOPTIMIZED:\n${optimized}`;
  }

  collect(): Promise<DataFrame<S>> {
    return Promise.resolve(execute(this._plan, this._source));
  }

  async sink(filePath: string): Promise<void> {
    const df = await this.collect();
    const ext = filePath.toLowerCase();
    if (ext.endsWith('.csv') || ext.endsWith('.tsv')) {
      await df.toCSV(filePath);
    } else if (ext.endsWith('.ndjson') || ext.endsWith('.jsonl')) {
      await df.toNDJSON(filePath);
    } else {
      throw new Error(`Unsupported sink format for '${filePath}'. Supported: .csv, .tsv, .ndjson, .jsonl`);
    }
  }
}

export function createLazyFrame<S extends Record<string, unknown>>(
  source: DataFrame<S>,
): LazyFrame<S> {
  return new LazyFrame<S>(source, createScanNode());
}
