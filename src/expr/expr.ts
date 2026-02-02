import { DType } from '../types/dtype';
import type { DataFrame } from '../dataframe';
import { Series } from '../series';
import type { Column } from '../storage/column';
import { Float64Column } from '../storage/numeric';
import { Utf8Column } from '../storage/string';
import { BooleanColumn } from '../storage/boolean';
import { DateColumn } from '../storage/date';

export abstract class Expr<T> {
  abstract evaluate(df: DataFrame): Series<T>;
  abstract get dependencies(): string[];
  abstract toString(): string;

  as(name: string): NamedExpr<T> {
    return new NamedExpr<T>(this, name);
  }

  // Arithmetic
  add(other: Expr<number> | number): Expr<number> {
    return new ArithmeticExpr(this as unknown as Expr<number>, wrapNum(other), 'add');
  }

  sub(other: Expr<number> | number): Expr<number> {
    return new ArithmeticExpr(this as unknown as Expr<number>, wrapNum(other), 'sub');
  }

  mul(other: Expr<number> | number): Expr<number> {
    return new ArithmeticExpr(this as unknown as Expr<number>, wrapNum(other), 'mul');
  }

  div(other: Expr<number> | number): Expr<number> {
    return new ArithmeticExpr(this as unknown as Expr<number>, wrapNum(other), 'div');
  }

  mod(other: Expr<number> | number): Expr<number> {
    return new ArithmeticExpr(this as unknown as Expr<number>, wrapNum(other), 'mod');
  }

  pow(other: Expr<number> | number): Expr<number> {
    return new ArithmeticExpr(this as unknown as Expr<number>, wrapNum(other), 'pow');
  }

  // Comparison
  eq(other: Expr<T> | T): Expr<boolean> {
    return new ComparisonExpr<T>(this, wrap<T>(other), 'eq');
  }

  neq(other: Expr<T> | T): Expr<boolean> {
    return new ComparisonExpr<T>(this, wrap<T>(other), 'neq');
  }

  gt(other: Expr<T> | T): Expr<boolean> {
    return new ComparisonExpr<T>(this, wrap<T>(other), 'gt');
  }

  gte(other: Expr<T> | T): Expr<boolean> {
    return new ComparisonExpr<T>(this, wrap<T>(other), 'gte');
  }

  lt(other: Expr<T> | T): Expr<boolean> {
    return new ComparisonExpr<T>(this, wrap<T>(other), 'lt');
  }

  lte(other: Expr<T> | T): Expr<boolean> {
    return new ComparisonExpr<T>(this, wrap<T>(other), 'lte');
  }

  // Logical
  and(other: Expr<boolean> | boolean): Expr<boolean> {
    return new LogicalExpr(this as unknown as Expr<boolean>, wrapBool(other), 'and');
  }

  or(other: Expr<boolean> | boolean): Expr<boolean> {
    return new LogicalExpr(this as unknown as Expr<boolean>, wrapBool(other), 'or');
  }

  not(): Expr<boolean> {
    return new NotExpr(this as unknown as Expr<boolean>);
  }

  // ── Aggregation (returns AggExpr for use in groupBy().agg()) ──

  private _aggColumnName(): string {
    const deps = this.dependencies;
    if (deps.length === 0) {
      throw new Error('Aggregation requires a column reference');
    }
    return deps[0]!;
  }

  sum(): SumAggExpr {
    return new SumAggExpr(this._aggColumnName());
  }

  mean(): MeanAggExpr {
    return new MeanAggExpr(this._aggColumnName());
  }

  count(): CountAggExpr {
    return new CountAggExpr(this._aggColumnName());
  }

  countDistinct(): CountDistinctAggExpr {
    return new CountDistinctAggExpr(this._aggColumnName());
  }

  min(): MinAggExpr {
    return new MinAggExpr(this._aggColumnName());
  }

  max(): MaxAggExpr {
    return new MaxAggExpr(this._aggColumnName());
  }

  std(): StdAggExpr {
    return new StdAggExpr(this._aggColumnName());
  }

  first(): FirstAggExpr<T> {
    return new FirstAggExpr<T>(this._aggColumnName());
  }

  last(): LastAggExpr<T> {
    return new LastAggExpr<T>(this._aggColumnName());
  }

  list(): ListAggExpr<T> {
    return new ListAggExpr<T>(this._aggColumnName());
  }

  mode(): ModeAggExpr<T> {
    return new ModeAggExpr<T>(this._aggColumnName());
  }
}

export class NamedExpr<T> {
  readonly expr: Expr<T>;
  readonly name: string;

  constructor(expr: Expr<T>, name: string) {
    this.expr = expr;
    this.name = name;
  }

  get dependencies(): string[] {
    return this.expr.dependencies;
  }

  toString(): string {
    return `${this.expr.toString()} AS ${this.name}`;
  }
}

// ── Helpers ──

function wrap<T>(value: Expr<T> | T): Expr<T> {
  if (value instanceof Expr) return value;
  return new LiteralExpr<T>(value);
}

function wrapNum(value: Expr<number> | number): Expr<number> {
  if (value instanceof Expr) return value;
  return new LiteralExpr<number>(value);
}

function wrapBool(value: Expr<boolean> | boolean): Expr<boolean> {
  if (value instanceof Expr) return value;
  return new LiteralExpr<boolean>(value);
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

function detectLiteralDType(value: unknown): DType {
  if (typeof value === 'number') return DType.Float64;
  if (typeof value === 'string') return DType.Utf8;
  if (typeof value === 'boolean') return DType.Boolean;
  if (value instanceof Date) return DType.Date;
  return DType.Float64;
}

// ── Literal Expression ──

export class LiteralExpr<T> extends Expr<T> {
  private readonly _value: T;

  constructor(value: T) {
    super();
    this._value = value;
  }

  get dependencies(): string[] {
    return [];
  }

  toString(): string {
    if (typeof this._value === 'string') return `"${this._value}"`;
    if (this._value instanceof Date) return this._value.toISOString();
    return String(this._value);
  }

  evaluate(df: DataFrame): Series<T> {
    const len = df.length;
    const values = new Array<T>(len).fill(this._value) as (T | null)[];
    const dtype = detectLiteralDType(this._value);
    const col = buildColumnForValues(dtype, values as unknown[]);
    return new Series<T>('literal', col as Column<T>);
  }
}

// ── Column Expression ──

export class ColumnExpr<T> extends Expr<T> {
  private readonly _name: string;

  constructor(name: string) {
    super();
    this._name = name;
  }

  get dependencies(): string[] {
    return [this._name];
  }

  toString(): string {
    return this._name;
  }

  evaluate(df: DataFrame): Series<T> {
    return df.col(this._name) as unknown as Series<T>;
  }
}

// ── Arithmetic Expression ──

type ArithOp = 'add' | 'sub' | 'mul' | 'div' | 'mod' | 'pow';

const ARITH_OP_SYMBOLS: Record<ArithOp, string> = {
  add: '+', sub: '-', mul: '*', div: '/', mod: '%', pow: '**',
};

class ArithmeticExpr extends Expr<number> {
  private readonly _left: Expr<number>;
  private readonly _right: Expr<number>;
  private readonly _op: ArithOp;

  constructor(left: Expr<number>, right: Expr<number>, op: ArithOp) {
    super();
    this._left = left;
    this._right = right;
    this._op = op;
  }

  get dependencies(): string[] {
    return [...new Set([...this._left.dependencies, ...this._right.dependencies])];
  }

  toString(): string {
    return `(${this._left.toString()} ${ARITH_OP_SYMBOLS[this._op]} ${this._right.toString()})`;
  }

  evaluate(df: DataFrame): Series<number> {
    const leftSeries = this._left.evaluate(df);
    const rightSeries = this._right.evaluate(df);
    const len = leftSeries.length;
    const results: (number | null)[] = [];

    for (let i = 0; i < len; i++) {
      const a = leftSeries.get(i);
      const b = rightSeries.get(i);
      if (a === null || b === null) {
        results.push(null);
      } else {
        results.push(applyArithOp(a, b, this._op));
      }
    }

    return new Series<number>('', Float64Column.from(results));
  }
}

function applyArithOp(a: number, b: number, op: ArithOp): number {
  switch (op) {
    case 'add': return a + b;
    case 'sub': return a - b;
    case 'mul': return a * b;
    case 'div': return a / b;
    case 'mod': return a % b;
    case 'pow': return Math.pow(a, b);
  }
}

// ── Comparison Expression ──

type CmpOp = 'eq' | 'neq' | 'gt' | 'gte' | 'lt' | 'lte';

const CMP_OP_SYMBOLS: Record<CmpOp, string> = {
  eq: '==', neq: '!=', gt: '>', gte: '>=', lt: '<', lte: '<=',
};

class ComparisonExpr<T> extends Expr<boolean> {
  private readonly _left: Expr<T>;
  private readonly _right: Expr<T>;
  private readonly _op: CmpOp;

  constructor(left: Expr<T>, right: Expr<T>, op: CmpOp) {
    super();
    this._left = left;
    this._right = right;
    this._op = op;
  }

  get dependencies(): string[] {
    return [...new Set([...this._left.dependencies, ...this._right.dependencies])];
  }

  toString(): string {
    return `(${this._left.toString()} ${CMP_OP_SYMBOLS[this._op]} ${this._right.toString()})`;
  }

  evaluate(df: DataFrame): Series<boolean> {
    const leftSeries = this._left.evaluate(df);
    const rightSeries = this._right.evaluate(df);
    const len = leftSeries.length;
    const results: (boolean | null)[] = [];

    for (let i = 0; i < len; i++) {
      const a = leftSeries.get(i);
      const b = rightSeries.get(i);
      if (a === null || b === null) {
        results.push(null);
      } else {
        results.push(applyCmpOp(a, b, this._op));
      }
    }

    return new Series<boolean>('', BooleanColumn.from(results));
  }
}

function applyCmpOp<T>(a: T, b: T, op: CmpOp): boolean {
  switch (op) {
    case 'eq': return a === b;
    case 'neq': return a !== b;
    case 'gt': return (a as number) > (b as number);
    case 'gte': return (a as number) >= (b as number);
    case 'lt': return (a as number) < (b as number);
    case 'lte': return (a as number) <= (b as number);
  }
}

// ── Logical Expressions ──

type LogicOp = 'and' | 'or';

class LogicalExpr extends Expr<boolean> {
  private readonly _left: Expr<boolean>;
  private readonly _right: Expr<boolean>;
  private readonly _op: LogicOp;

  constructor(left: Expr<boolean>, right: Expr<boolean>, op: LogicOp) {
    super();
    this._left = left;
    this._right = right;
    this._op = op;
  }

  get dependencies(): string[] {
    return [...new Set([...this._left.dependencies, ...this._right.dependencies])];
  }

  toString(): string {
    return `(${this._left.toString()} ${this._op.toUpperCase()} ${this._right.toString()})`;
  }

  evaluate(df: DataFrame): Series<boolean> {
    const leftSeries = this._left.evaluate(df);
    const rightSeries = this._right.evaluate(df);
    const len = leftSeries.length;
    const results: (boolean | null)[] = [];

    for (let i = 0; i < len; i++) {
      const a = leftSeries.get(i);
      const b = rightSeries.get(i);
      if (a === null || b === null) {
        results.push(null);
      } else if (this._op === 'and') {
        results.push(a && b);
      } else {
        results.push(a || b);
      }
    }

    return new Series<boolean>('', BooleanColumn.from(results));
  }
}

class NotExpr extends Expr<boolean> {
  private readonly _inner: Expr<boolean>;

  constructor(inner: Expr<boolean>) {
    super();
    this._inner = inner;
  }

  get dependencies(): string[] {
    return this._inner.dependencies;
  }

  toString(): string {
    return `NOT ${this._inner.toString()}`;
  }

  evaluate(df: DataFrame): Series<boolean> {
    const innerSeries = this._inner.evaluate(df);
    const len = innerSeries.length;
    const results: (boolean | null)[] = [];

    for (let i = 0; i < len; i++) {
      const v = innerSeries.get(i);
      results.push(v === null ? null : !v);
    }

    return new Series<boolean>('', BooleanColumn.from(results));
  }
}

// ── Public factory functions ──

export function col<T = unknown>(name: string): Expr<T> {
  return new ColumnExpr<T>(name);
}

export function lit<T>(value: T): Expr<T> {
  return new LiteralExpr<T>(value);
}

// ── Aggregate Expressions ──

function toComparableKey(v: unknown): string {
  if (v instanceof Date) return `\0date${v.getTime()}`;
  if (typeof v === 'string') return `\0str${v}`;
  if (typeof v === 'number') return `\0num${v}`;
  if (typeof v === 'boolean') return `\0bool${v}`;
  return `\0other${String(v)}`;
}

export abstract class AggExpr<T> extends Expr<T> {
  protected readonly _columnName: string;
  protected abstract readonly _aggName: string;

  constructor(columnName: string) {
    super();
    this._columnName = columnName;
  }

  get dependencies(): string[] {
    return [this._columnName];
  }

  toString(): string {
    return `${this._aggName}(${this._columnName})`;
  }

  evaluate(df: DataFrame): Series<T> {
    const series = df.col(this._columnName);
    const result = this.evaluateColumn(series.column);
    const values = [result] as (T | null)[];
    const col = Float64Column.from(values as unknown as (number | null)[]);
    return new Series<T>('', col as unknown as Column<T>);
  }

  abstract evaluateColumn(column: Column<unknown>): T | null;
}

export class SumAggExpr extends AggExpr<number> {
  protected readonly _aggName = 'sum';
  evaluateColumn(column: Column<unknown>): number {
    let total = 0;
    for (let i = 0; i < column.length; i++) {
      const v = column.get(i);
      if (v !== null && typeof v === 'number') {
        total += v;
      }
    }
    return total;
  }
}

export class MeanAggExpr extends AggExpr<number> {
  protected readonly _aggName = 'mean';
  evaluateColumn(column: Column<unknown>): number | null {
    let total = 0;
    let count = 0;
    for (let i = 0; i < column.length; i++) {
      const v = column.get(i);
      if (v !== null && typeof v === 'number') {
        total += v;
        count++;
      }
    }
    return count === 0 ? null : total / count;
  }
}

export class CountAggExpr extends AggExpr<number> {
  protected readonly _aggName = 'count';
  evaluateColumn(column: Column<unknown>): number {
    let count = 0;
    for (let i = 0; i < column.length; i++) {
      if (column.get(i) !== null) {
        count++;
      }
    }
    return count;
  }
}

export class CountDistinctAggExpr extends AggExpr<number> {
  protected readonly _aggName = 'count_distinct';
  evaluateColumn(column: Column<unknown>): number {
    const seen = new Set<string>();
    for (let i = 0; i < column.length; i++) {
      const v = column.get(i);
      if (v !== null) {
        seen.add(toComparableKey(v));
      }
    }
    return seen.size;
  }
}

export class MinAggExpr extends AggExpr<number> {
  protected readonly _aggName = 'min';
  evaluateColumn(column: Column<unknown>): number | null {
    let result: number | null = null;
    for (let i = 0; i < column.length; i++) {
      const v = column.get(i);
      if (v !== null && typeof v === 'number') {
        if (result === null || v < result) {
          result = v;
        }
      }
    }
    return result;
  }
}

export class MaxAggExpr extends AggExpr<number> {
  protected readonly _aggName = 'max';
  evaluateColumn(column: Column<unknown>): number | null {
    let result: number | null = null;
    for (let i = 0; i < column.length; i++) {
      const v = column.get(i);
      if (v !== null && typeof v === 'number') {
        if (result === null || v > result) {
          result = v;
        }
      }
    }
    return result;
  }
}

export class StdAggExpr extends AggExpr<number> {
  protected readonly _aggName = 'std';
  evaluateColumn(column: Column<unknown>): number | null {
    const values: number[] = [];
    for (let i = 0; i < column.length; i++) {
      const v = column.get(i);
      if (v !== null && typeof v === 'number') {
        values.push(v);
      }
    }
    if (values.length < 2) return null;
    const mean = values.reduce((a, b) => a + b, 0) / values.length;
    const sumSqDiff = values.reduce((acc, v) => acc + (v - mean) ** 2, 0);
    return Math.sqrt(sumSqDiff / (values.length - 1));
  }
}

export class FirstAggExpr<T> extends AggExpr<T> {
  protected readonly _aggName = 'first';
  evaluateColumn(column: Column<unknown>): T | null {
    for (let i = 0; i < column.length; i++) {
      const v = column.get(i);
      if (v !== null) {
        return v as T;
      }
    }
    return null;
  }
}

export class LastAggExpr<T> extends AggExpr<T> {
  protected readonly _aggName = 'last';
  evaluateColumn(column: Column<unknown>): T | null {
    for (let i = column.length - 1; i >= 0; i--) {
      const v = column.get(i);
      if (v !== null) {
        return v as T;
      }
    }
    return null;
  }
}

export class ListAggExpr<T> extends AggExpr<T[]> {
  protected readonly _aggName = 'list';
  evaluateColumn(column: Column<unknown>): T[] {
    const result: T[] = [];
    for (let i = 0; i < column.length; i++) {
      const v = column.get(i);
      if (v !== null) {
        result.push(v as T);
      }
    }
    return result;
  }
}

export class ModeAggExpr<T> extends AggExpr<T> {
  protected readonly _aggName = 'mode';
  evaluateColumn(column: Column<unknown>): T | null {
    const counts = new Map<string, { value: T; count: number }>();
    for (let i = 0; i < column.length; i++) {
      const v = column.get(i);
      if (v !== null) {
        const key = toComparableKey(v);
        const entry = counts.get(key);
        if (entry) {
          entry.count++;
        } else {
          counts.set(key, { value: v as T, count: 1 });
        }
      }
    }
    let best: T | null = null;
    let bestCount = 0;
    for (const entry of counts.values()) {
      if (entry.count > bestCount) {
        best = entry.value;
        bestCount = entry.count;
      }
    }
    return best;
  }
}
