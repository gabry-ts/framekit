import type { DataFrame } from '../dataframe';
import { Series } from '../series';
import { Expr, LiteralExpr } from './expr';
import type { Column } from '../storage/column';
import { Float64Column } from '../storage/numeric';
import { Utf8Column } from '../storage/string';
import { BooleanColumn } from '../storage/boolean';
import { DateColumn } from '../storage/date';
import { DType } from '../types/dtype';

function detectValueDType(value: unknown): DType {
  if (typeof value === 'number') return DType.Float64;
  if (typeof value === 'string') return DType.Utf8;
  if (typeof value === 'boolean') return DType.Boolean;
  if (value instanceof Date) return DType.Date;
  return DType.Float64;
}

function buildColumnForValues(dtype: DType, values: unknown[]): Column<unknown> {
  switch (dtype) {
    case DType.Float64:
      return Float64Column.from(values as (number | null)[]);
    case DType.Utf8:
      return Utf8Column.from(values as (string | null)[]);
    case DType.Boolean:
      return BooleanColumn.from(values as (boolean | null)[]);
    case DType.Date:
      return DateColumn.from(values as (Date | null)[]);
    default:
      return Float64Column.from(values as (number | null)[]);
  }
}

interface WhenThenClause<T> {
  condition: Expr<boolean>;
  value: Expr<T>;
}

class WhenExpr<T> extends Expr<T> {
  private readonly _clauses: WhenThenClause<T>[];
  private readonly _otherwise: Expr<T> | null;

  constructor(clauses: WhenThenClause<T>[], otherwise: Expr<T> | null) {
    super();
    this._clauses = clauses;
    this._otherwise = otherwise;
  }

  get dependencies(): string[] {
    const deps = new Set<string>();
    for (const clause of this._clauses) {
      for (const d of clause.condition.dependencies) deps.add(d);
      for (const d of clause.value.dependencies) deps.add(d);
    }
    if (this._otherwise) {
      for (const d of this._otherwise.dependencies) deps.add(d);
    }
    return [...deps];
  }

  toString(): string {
    const parts = this._clauses.map(c => `WHEN ${c.condition.toString()} THEN ${c.value.toString()}`);
    if (this._otherwise) {
      parts.push(`ELSE ${this._otherwise.toString()}`);
    }
    return `CASE ${parts.join(' ')} END`;
  }

  evaluate(df: DataFrame): Series<T> {
    const len = df.length;
    const conditionResults = this._clauses.map((c) => c.condition.evaluate(df));
    const valueResults = this._clauses.map((c) => c.value.evaluate(df));
    const otherwiseResult = this._otherwise ? this._otherwise.evaluate(df) : null;

    const results: (T | null)[] = [];
    let detectedDType: DType | null = null;

    for (let i = 0; i < len; i++) {
      let matched = false;
      for (let j = 0; j < this._clauses.length; j++) {
        const cond = conditionResults[j]!.get(i);
        if (cond === true) {
          const val = valueResults[j]!.get(i);
          results.push(val);
          if (detectedDType === null && val !== null) {
            detectedDType = detectValueDType(val);
          }
          matched = true;
          break;
        }
      }
      if (!matched) {
        if (otherwiseResult) {
          const val = otherwiseResult.get(i);
          results.push(val);
          if (detectedDType === null && val !== null) {
            detectedDType = detectValueDType(val);
          }
        } else {
          results.push(null);
        }
      }
    }

    const dtype = detectedDType ?? DType.Float64;
    const col = buildColumnForValues(dtype, results as unknown[]);
    return new Series<T>('', col as Column<T>);
  }
}

export class WhenBuilder {
  private readonly _condition: Expr<boolean>;

  constructor(condition: Expr<boolean>) {
    this._condition = condition;
  }

  then<T>(value: Expr<T> | T): ThenBuilder<T> {
    const expr = value instanceof Expr ? value : new LiteralExpr<T>(value);
    return new ThenBuilder<T>([{ condition: this._condition, value: expr }]);
  }
}

export class ThenBuilder<T> {
  private readonly _clauses: WhenThenClause<T>[];

  constructor(clauses: WhenThenClause<T>[]) {
    this._clauses = clauses;
  }

  when(condition: Expr<boolean>): ChainedWhenBuilder<T> {
    return new ChainedWhenBuilder<T>(this._clauses, condition);
  }

  otherwise(value: Expr<T> | T): Expr<T> {
    const expr = value instanceof Expr ? value : new LiteralExpr<T>(value);
    return new WhenExpr<T>(this._clauses, expr);
  }
}

class ChainedWhenBuilder<T> {
  private readonly _clauses: WhenThenClause<T>[];
  private readonly _condition: Expr<boolean>;

  constructor(clauses: WhenThenClause<T>[], condition: Expr<boolean>) {
    this._clauses = clauses;
    this._condition = condition;
  }

  then(value: Expr<T> | T): ThenBuilder<T> {
    const expr = value instanceof Expr ? value : new LiteralExpr<T>(value);
    return new ThenBuilder<T>([...this._clauses, { condition: this._condition, value: expr }]);
  }
}

export function when(condition: Expr<boolean>): WhenBuilder {
  return new WhenBuilder(condition);
}
