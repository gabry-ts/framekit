import type { DataFrame } from '../dataframe';
import { Series } from '../series';
import { Expr } from './expr';
import { Utf8Column } from '../storage/string';
import { BooleanColumn } from '../storage/boolean';
import { Float64Column } from '../storage/numeric';

type StringUnaryOp =
  | 'toLowerCase'
  | 'toUpperCase'
  | 'trim';

class StringUnaryExpr extends Expr<string> {
  private readonly _inner: Expr<string>;
  private readonly _op: StringUnaryOp;

  constructor(inner: Expr<string>, op: StringUnaryOp) {
    super();
    this._inner = inner;
    this._op = op;
  }

  get dependencies(): string[] {
    return this._inner.dependencies;
  }

  toString(): string {
    return `${this._inner.toString()}.str.${this._op}()`;
  }

  evaluate(df: DataFrame): Series<string> {
    const series = this._inner.evaluate(df);
    const results: (string | null)[] = [];
    for (let i = 0; i < series.length; i++) {
      const val = series.get(i);
      if (val === null) {
        results.push(null);
      } else {
        switch (this._op) {
          case 'toLowerCase': results.push(val.toLowerCase()); break;
          case 'toUpperCase': results.push(val.toUpperCase()); break;
          case 'trim': results.push(val.trim()); break;
        }
      }
    }
    return new Series<string>('', Utf8Column.from(results));
  }
}

class StringContainsExpr extends Expr<boolean> {
  private readonly _inner: Expr<string>;
  private readonly _pattern: string;

  constructor(inner: Expr<string>, pattern: string) {
    super();
    this._inner = inner;
    this._pattern = pattern;
  }

  get dependencies(): string[] {
    return this._inner.dependencies;
  }

  toString(): string {
    return `${this._inner.toString()}.str.contains("${this._pattern}")`;
  }

  evaluate(df: DataFrame): Series<boolean> {
    const series = this._inner.evaluate(df);
    const results: (boolean | null)[] = [];
    for (let i = 0; i < series.length; i++) {
      const val = series.get(i);
      results.push(val === null ? null : val.includes(this._pattern));
    }
    return new Series<boolean>('', BooleanColumn.from(results));
  }
}

class StringStartsWithExpr extends Expr<boolean> {
  private readonly _inner: Expr<string>;
  private readonly _prefix: string;

  constructor(inner: Expr<string>, prefix: string) {
    super();
    this._inner = inner;
    this._prefix = prefix;
  }

  get dependencies(): string[] {
    return this._inner.dependencies;
  }

  toString(): string {
    return `${this._inner.toString()}.str.startsWith("${this._prefix}")`;
  }

  evaluate(df: DataFrame): Series<boolean> {
    const series = this._inner.evaluate(df);
    const results: (boolean | null)[] = [];
    for (let i = 0; i < series.length; i++) {
      const val = series.get(i);
      results.push(val === null ? null : val.startsWith(this._prefix));
    }
    return new Series<boolean>('', BooleanColumn.from(results));
  }
}

class StringEndsWithExpr extends Expr<boolean> {
  private readonly _inner: Expr<string>;
  private readonly _suffix: string;

  constructor(inner: Expr<string>, suffix: string) {
    super();
    this._inner = inner;
    this._suffix = suffix;
  }

  get dependencies(): string[] {
    return this._inner.dependencies;
  }

  toString(): string {
    return `${this._inner.toString()}.str.endsWith("${this._suffix}")`;
  }

  evaluate(df: DataFrame): Series<boolean> {
    const series = this._inner.evaluate(df);
    const results: (boolean | null)[] = [];
    for (let i = 0; i < series.length; i++) {
      const val = series.get(i);
      results.push(val === null ? null : val.endsWith(this._suffix));
    }
    return new Series<boolean>('', BooleanColumn.from(results));
  }
}

class StringReplaceExpr extends Expr<string> {
  private readonly _inner: Expr<string>;
  private readonly _pattern: string;
  private readonly _replacement: string;

  constructor(inner: Expr<string>, pattern: string, replacement: string) {
    super();
    this._inner = inner;
    this._pattern = pattern;
    this._replacement = replacement;
  }

  get dependencies(): string[] {
    return this._inner.dependencies;
  }

  toString(): string {
    return `${this._inner.toString()}.str.replace("${this._pattern}", "${this._replacement}")`;
  }

  evaluate(df: DataFrame): Series<string> {
    const series = this._inner.evaluate(df);
    const results: (string | null)[] = [];
    for (let i = 0; i < series.length; i++) {
      const val = series.get(i);
      results.push(val === null ? null : val.replaceAll(this._pattern, this._replacement));
    }
    return new Series<string>('', Utf8Column.from(results));
  }
}

class StringSliceExpr extends Expr<string> {
  private readonly _inner: Expr<string>;
  private readonly _start: number;
  private readonly _end: number | undefined;

  constructor(inner: Expr<string>, start: number, end?: number) {
    super();
    this._inner = inner;
    this._start = start;
    this._end = end;
  }

  get dependencies(): string[] {
    return this._inner.dependencies;
  }

  toString(): string {
    return `${this._inner.toString()}.str.slice(${this._start}${this._end !== undefined ? `, ${this._end}` : ''})`;
  }

  evaluate(df: DataFrame): Series<string> {
    const series = this._inner.evaluate(df);
    const results: (string | null)[] = [];
    for (let i = 0; i < series.length; i++) {
      const val = series.get(i);
      results.push(val === null ? null : val.slice(this._start, this._end));
    }
    return new Series<string>('', Utf8Column.from(results));
  }
}

class StringLengthExpr extends Expr<number> {
  private readonly _inner: Expr<string>;

  constructor(inner: Expr<string>) {
    super();
    this._inner = inner;
  }

  get dependencies(): string[] {
    return this._inner.dependencies;
  }

  toString(): string {
    return `${this._inner.toString()}.str.length()`;
  }

  evaluate(df: DataFrame): Series<number> {
    const series = this._inner.evaluate(df);
    const results: (number | null)[] = [];
    for (let i = 0; i < series.length; i++) {
      const val = series.get(i);
      results.push(val === null ? null : val.length);
    }
    return new Series<number>('', Float64Column.from(results));
  }
}

class StringConcatExpr extends Expr<string> {
  private readonly _parts: (Expr<string> | string)[];

  constructor(parts: (Expr<string> | string)[]) {
    super();
    this._parts = parts;
  }

  get dependencies(): string[] {
    const deps = new Set<string>();
    for (const part of this._parts) {
      if (part instanceof Expr) {
        for (const d of part.dependencies) deps.add(d);
      }
    }
    return [...deps];
  }

  toString(): string {
    const parts = this._parts.map(p => p instanceof Expr ? p.toString() : `"${p}"`);
    return `concat(${parts.join(', ')})`;
  }

  evaluate(df: DataFrame): Series<string> {
    const len = df.length;
    const evaluatedParts = this._parts.map((part) => {
      if (part instanceof Expr) return part.evaluate(df);
      return part;
    });

    const results: (string | null)[] = [];
    for (let i = 0; i < len; i++) {
      let hasNull = false;
      let result = '';
      for (const part of evaluatedParts) {
        if (typeof part === 'string') {
          result += part;
        } else {
          const val = part.get(i);
          if (val === null) {
            hasNull = true;
            break;
          }
          result += val;
        }
      }
      results.push(hasNull ? null : result);
    }
    return new Series<string>('', Utf8Column.from(results));
  }
}

export class StringExprAccessor {
  private readonly _expr: Expr<string>;

  constructor(expr: Expr<string>) {
    this._expr = expr;
  }

  toLowerCase(): Expr<string> {
    return new StringUnaryExpr(this._expr, 'toLowerCase');
  }

  toUpperCase(): Expr<string> {
    return new StringUnaryExpr(this._expr, 'toUpperCase');
  }

  trim(): Expr<string> {
    return new StringUnaryExpr(this._expr, 'trim');
  }

  contains(pattern: string): Expr<boolean> {
    return new StringContainsExpr(this._expr, pattern);
  }

  startsWith(prefix: string): Expr<boolean> {
    return new StringStartsWithExpr(this._expr, prefix);
  }

  endsWith(suffix: string): Expr<boolean> {
    return new StringEndsWithExpr(this._expr, suffix);
  }

  replace(pattern: string, replacement: string): Expr<string> {
    return new StringReplaceExpr(this._expr, pattern, replacement);
  }

  slice(start: number, end?: number): Expr<string> {
    return new StringSliceExpr(this._expr, start, end);
  }

  length(): Expr<number> {
    return new StringLengthExpr(this._expr);
  }

  concat(...parts: (Expr<string> | string)[]): Expr<string> {
    return new StringConcatExpr([this._expr as Expr<string> | string, ...parts]);
  }
}
