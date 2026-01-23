import type { DataFrame } from '../dataframe';
import { Series } from '../series';
import { Expr } from './expr';
import { Float64Column } from '../storage/numeric';
import { DateColumn } from '../storage/date';
import type { TimeUnit } from '../accessors/date-accessor';

type DateComponentOp =
  | 'year'
  | 'month'
  | 'day'
  | 'hour'
  | 'minute'
  | 'second'
  | 'dayOfWeek'
  | 'dayOfYear'
  | 'weekNumber'
  | 'quarter'
  | 'timestamp';

function extractDateComponent(date: Date, op: DateComponentOp): number {
  switch (op) {
    case 'year': return date.getFullYear();
    case 'month': return date.getMonth() + 1;
    case 'day': return date.getDate();
    case 'hour': return date.getHours();
    case 'minute': return date.getMinutes();
    case 'second': return date.getSeconds();
    case 'dayOfWeek': return date.getDay();
    case 'dayOfYear': {
      const start = new Date(date.getFullYear(), 0, 0);
      const diff = date.getTime() - start.getTime();
      const oneDay = 1000 * 60 * 60 * 24;
      return Math.floor(diff / oneDay);
    }
    case 'weekNumber': {
      const target = new Date(date.getTime());
      target.setHours(0, 0, 0, 0);
      target.setDate(target.getDate() + 3 - ((target.getDay() + 6) % 7));
      const jan4 = new Date(target.getFullYear(), 0, 4);
      const dayDiff = (target.getTime() - jan4.getTime()) / (1000 * 60 * 60 * 24);
      return 1 + Math.round((dayDiff - 3 + ((jan4.getDay() + 6) % 7)) / 7);
    }
    case 'quarter': return Math.floor(date.getMonth() / 3) + 1;
    case 'timestamp': return date.getTime();
  }
}

class DateComponentExpr extends Expr<number> {
  private readonly _inner: Expr<Date>;
  private readonly _op: DateComponentOp;

  constructor(inner: Expr<Date>, op: DateComponentOp) {
    super();
    this._inner = inner;
    this._op = op;
  }

  get dependencies(): string[] {
    return this._inner.dependencies;
  }

  evaluate(df: DataFrame): Series<number> {
    const series = this._inner.evaluate(df);
    const results: (number | null)[] = [];
    for (let i = 0; i < series.length; i++) {
      const val = series.get(i);
      results.push(val === null ? null : extractDateComponent(val, this._op));
    }
    return new Series<number>('', Float64Column.from(results));
  }
}

class DateTruncateExpr extends Expr<Date> {
  private readonly _inner: Expr<Date>;
  private readonly _unit: TimeUnit;

  constructor(inner: Expr<Date>, unit: TimeUnit) {
    super();
    this._inner = inner;
    this._unit = unit;
  }

  get dependencies(): string[] {
    return this._inner.dependencies;
  }

  evaluate(df: DataFrame): Series<Date> {
    const series = this._inner.evaluate(df);
    const results: (Date | null)[] = [];
    for (let i = 0; i < series.length; i++) {
      const val = series.get(i);
      if (val === null) {
        results.push(null);
      } else {
        results.push(truncateDate(val, this._unit));
      }
    }
    return new Series<Date>('', DateColumn.from(results));
  }
}

type DateDiffUnit = 'days' | 'hours' | 'minutes' | 'seconds' | 'milliseconds';

class DateDiffExpr extends Expr<number> {
  private readonly _left: Expr<Date>;
  private readonly _right: Expr<Date>;
  private readonly _unit: DateDiffUnit;

  constructor(left: Expr<Date>, right: Expr<Date>, unit: DateDiffUnit) {
    super();
    this._left = left;
    this._right = right;
    this._unit = unit;
  }

  get dependencies(): string[] {
    return [...new Set([...this._left.dependencies, ...this._right.dependencies])];
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
        const diffMs = a.getTime() - b.getTime();
        results.push(convertMsToDiffUnit(diffMs, this._unit));
      }
    }
    return new Series<number>('', Float64Column.from(results));
  }
}

function convertMsToDiffUnit(ms: number, unit: DateDiffUnit): number {
  switch (unit) {
    case 'milliseconds': return ms;
    case 'seconds': return ms / 1000;
    case 'minutes': return ms / (1000 * 60);
    case 'hours': return ms / (1000 * 60 * 60);
    case 'days': return ms / (1000 * 60 * 60 * 24);
  }
}

function truncateDate(date: Date, unit: TimeUnit): Date {
  switch (unit) {
    case 'year':
      return new Date(date.getFullYear(), 0, 1);
    case 'month':
      return new Date(date.getFullYear(), date.getMonth(), 1);
    case 'day':
      return new Date(date.getFullYear(), date.getMonth(), date.getDate());
    case 'hour':
      return new Date(date.getFullYear(), date.getMonth(), date.getDate(), date.getHours());
    case 'minute':
      return new Date(date.getFullYear(), date.getMonth(), date.getDate(), date.getHours(), date.getMinutes());
    case 'second':
      return new Date(date.getFullYear(), date.getMonth(), date.getDate(), date.getHours(), date.getMinutes(), date.getSeconds());
  }
}

export class DateExprAccessor {
  private readonly _expr: Expr<Date>;

  constructor(expr: Expr<Date>) {
    this._expr = expr;
  }

  year(): Expr<number> {
    return new DateComponentExpr(this._expr, 'year');
  }

  month(): Expr<number> {
    return new DateComponentExpr(this._expr, 'month');
  }

  day(): Expr<number> {
    return new DateComponentExpr(this._expr, 'day');
  }

  hour(): Expr<number> {
    return new DateComponentExpr(this._expr, 'hour');
  }

  minute(): Expr<number> {
    return new DateComponentExpr(this._expr, 'minute');
  }

  second(): Expr<number> {
    return new DateComponentExpr(this._expr, 'second');
  }

  dayOfWeek(): Expr<number> {
    return new DateComponentExpr(this._expr, 'dayOfWeek');
  }

  dayOfYear(): Expr<number> {
    return new DateComponentExpr(this._expr, 'dayOfYear');
  }

  weekNumber(): Expr<number> {
    return new DateComponentExpr(this._expr, 'weekNumber');
  }

  quarter(): Expr<number> {
    return new DateComponentExpr(this._expr, 'quarter');
  }

  timestamp(): Expr<number> {
    return new DateComponentExpr(this._expr, 'timestamp');
  }

  truncate(unit: TimeUnit): Expr<Date> {
    return new DateTruncateExpr(this._expr, unit);
  }

  diff(other: Expr<Date>, unit: DateDiffUnit = 'days'): Expr<number> {
    return new DateDiffExpr(this._expr, other, unit);
  }
}
