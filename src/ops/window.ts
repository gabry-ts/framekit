import type { DataFrame } from '../dataframe';
import { Expr } from '../expr/expr';
import { Series } from '../series';
import { Float64Column } from '../storage/numeric';

// ── Window Ranking Expression Base ──

abstract class WindowRankingExpr extends Expr<number> {
  protected readonly _source: Expr<unknown>;

  constructor(source: Expr<unknown>) {
    super();
    this._source = source;
  }

  get dependencies(): string[] {
    return this._source.dependencies;
  }
}

// ── rank(): 1-based, ties get same rank, gaps after ──

export class WindowRankExpr extends WindowRankingExpr {
  toString(): string {
    return `rank(${this._source.toString()})`;
  }

  evaluate(df: DataFrame): Series<number> {
    const series = this._source.evaluate(df);
    const len = series.length;

    // Build (value, originalIndex) pairs, treating nulls as largest
    const indexed: { value: unknown; idx: number }[] = [];
    for (let i = 0; i < len; i++) {
      indexed.push({ value: series.get(i), idx: i });
    }

    // Sort ascending (nulls last)
    indexed.sort((a, b) => compareValues(a.value, b.value));

    const ranks = new Array<number | null>(len);
    let rank = 1;
    for (let i = 0; i < indexed.length; i++) {
      if (i > 0 && compareValues(indexed[i]!.value, indexed[i - 1]!.value) !== 0) {
        rank = i + 1;
      }
      ranks[indexed[i]!.idx] = rank;
    }

    return new Series<number>('rank', Float64Column.from(ranks));
  }
}

// ── denseRank(): no gaps ──

export class WindowDenseRankExpr extends WindowRankingExpr {
  toString(): string {
    return `dense_rank(${this._source.toString()})`;
  }

  evaluate(df: DataFrame): Series<number> {
    const series = this._source.evaluate(df);
    const len = series.length;

    const indexed: { value: unknown; idx: number }[] = [];
    for (let i = 0; i < len; i++) {
      indexed.push({ value: series.get(i), idx: i });
    }

    indexed.sort((a, b) => compareValues(a.value, b.value));

    const ranks = new Array<number | null>(len);
    let rank = 1;
    for (let i = 0; i < indexed.length; i++) {
      if (i > 0 && compareValues(indexed[i]!.value, indexed[i - 1]!.value) !== 0) {
        rank++;
      }
      ranks[indexed[i]!.idx] = rank;
    }

    return new Series<number>('dense_rank', Float64Column.from(ranks));
  }
}

// ── rowNumber(): sequential 1-based ──

export class WindowRowNumberExpr extends WindowRankingExpr {
  toString(): string {
    return `row_number(${this._source.toString()})`;
  }

  evaluate(df: DataFrame): Series<number> {
    const series = this._source.evaluate(df);
    const len = series.length;

    const indexed: { value: unknown; idx: number }[] = [];
    for (let i = 0; i < len; i++) {
      indexed.push({ value: series.get(i), idx: i });
    }

    indexed.sort((a, b) => compareValues(a.value, b.value));

    const ranks = new Array<number | null>(len);
    for (let i = 0; i < indexed.length; i++) {
      ranks[indexed[i]!.idx] = i + 1;
    }

    return new Series<number>('row_number', Float64Column.from(ranks));
  }
}

// ── percentRank(): (rank - 1) / (n - 1), range [0, 1] ──

export class WindowPercentRankExpr extends WindowRankingExpr {
  toString(): string {
    return `percent_rank(${this._source.toString()})`;
  }

  evaluate(df: DataFrame): Series<number> {
    const series = this._source.evaluate(df);
    const len = series.length;

    if (len <= 1) {
      const results: (number | null)[] = new Array<number | null>(len).fill(0);
      return new Series<number>('percent_rank', Float64Column.from(results));
    }

    const indexed: { value: unknown; idx: number }[] = [];
    for (let i = 0; i < len; i++) {
      indexed.push({ value: series.get(i), idx: i });
    }

    indexed.sort((a, b) => compareValues(a.value, b.value));

    const ranks = new Array<number | null>(len);
    let rank = 1;
    for (let i = 0; i < indexed.length; i++) {
      if (i > 0 && compareValues(indexed[i]!.value, indexed[i - 1]!.value) !== 0) {
        rank = i + 1;
      }
      ranks[indexed[i]!.idx] = (rank - 1) / (len - 1);
    }

    return new Series<number>('percent_rank', Float64Column.from(ranks));
  }
}

// ── ntile(n): distribute rows into n roughly-equal buckets ──

export class WindowNtileExpr extends WindowRankingExpr {
  private readonly _n: number;

  constructor(source: Expr<unknown>, n: number) {
    super(source);
    this._n = n;
  }

  toString(): string {
    return `ntile(${this._source.toString()}, ${this._n})`;
  }

  evaluate(df: DataFrame): Series<number> {
    const series = this._source.evaluate(df);
    const len = series.length;

    const indexed: { value: unknown; idx: number }[] = [];
    for (let i = 0; i < len; i++) {
      indexed.push({ value: series.get(i), idx: i });
    }

    indexed.sort((a, b) => compareValues(a.value, b.value));

    const results = new Array<number | null>(len);
    for (let i = 0; i < indexed.length; i++) {
      results[indexed[i]!.idx] = Math.floor((i * this._n) / len) + 1;
    }

    return new Series<number>('ntile', Float64Column.from(results));
  }
}

// ── Helper: compare values for sorting (nulls last) ──

function compareValues(a: unknown, b: unknown): number {
  if (a === null && b === null) return 0;
  if (a === null) return 1;
  if (b === null) return -1;

  if (typeof a === 'number' && typeof b === 'number') {
    return a - b;
  }
  if (typeof a === 'string' && typeof b === 'string') {
    return a < b ? -1 : a > b ? 1 : 0;
  }
  if (a instanceof Date && b instanceof Date) {
    return a.getTime() - b.getTime();
  }
  // fallback: convert to string with type narrowing
  const sa = typeof a === 'string' ? a : typeof a === 'number' ? `${a}` : typeof a === 'boolean' ? `${a}` : 'object';
  const sb = typeof b === 'string' ? b : typeof b === 'number' ? `${b}` : typeof b === 'boolean' ? `${b}` : 'object';
  return sa < sb ? -1 : sa > sb ? 1 : 0;
}

// ── Cumulative Window Expression Base ──

abstract class CumulativeExpr extends Expr<number> {
  protected readonly _source: Expr<unknown>;

  constructor(source: Expr<unknown>) {
    super();
    this._source = source;
  }

  get dependencies(): string[] {
    return this._source.dependencies;
  }
}

// ── cumSum(): running sum ──

export class CumSumExpr extends CumulativeExpr {
  toString(): string {
    return `cumSum(${this._source.toString()})`;
  }

  evaluate(df: DataFrame): Series<number> {
    const series = this._source.evaluate(df);
    const len = series.length;
    const results = new Array<number | null>(len);
    let sum = 0;

    for (let i = 0; i < len; i++) {
      const v = series.get(i);
      if (v !== null && typeof v === 'number') {
        sum += v;
      }
      results[i] = sum;
    }

    return new Series<number>('cumSum', Float64Column.from(results));
  }
}

// ── cumMax(): running maximum ──

export class CumMaxExpr extends CumulativeExpr {
  toString(): string {
    return `cumMax(${this._source.toString()})`;
  }

  evaluate(df: DataFrame): Series<number> {
    const series = this._source.evaluate(df);
    const len = series.length;
    const results = new Array<number | null>(len);
    let max: number | null = null;

    for (let i = 0; i < len; i++) {
      const v = series.get(i);
      if (v !== null && typeof v === 'number') {
        max = max === null ? v : Math.max(max, v);
      }
      results[i] = max;
    }

    return new Series<number>('cumMax', Float64Column.from(results));
  }
}

// ── cumMin(): running minimum ──

export class CumMinExpr extends CumulativeExpr {
  toString(): string {
    return `cumMin(${this._source.toString()})`;
  }

  evaluate(df: DataFrame): Series<number> {
    const series = this._source.evaluate(df);
    const len = series.length;
    const results = new Array<number | null>(len);
    let min: number | null = null;

    for (let i = 0; i < len; i++) {
      const v = series.get(i);
      if (v !== null && typeof v === 'number') {
        min = min === null ? v : Math.min(min, v);
      }
      results[i] = min;
    }

    return new Series<number>('cumMin', Float64Column.from(results));
  }
}

// ── cumProd(): running product ──

export class CumProdExpr extends CumulativeExpr {
  toString(): string {
    return `cumProd(${this._source.toString()})`;
  }

  evaluate(df: DataFrame): Series<number> {
    const series = this._source.evaluate(df);
    const len = series.length;
    const results = new Array<number | null>(len);
    let prod = 1;

    for (let i = 0; i < len; i++) {
      const v = series.get(i);
      if (v !== null && typeof v === 'number') {
        prod *= v;
      }
      results[i] = prod;
    }

    return new Series<number>('cumProd', Float64Column.from(results));
  }
}

// ── cumCount(): running count (excluding nulls) ──

export class CumCountExpr extends CumulativeExpr {
  toString(): string {
    return `cumCount(${this._source.toString()})`;
  }

  evaluate(df: DataFrame): Series<number> {
    const series = this._source.evaluate(df);
    const len = series.length;
    const results = new Array<number | null>(len);
    let count = 0;

    for (let i = 0; i < len; i++) {
      const v = series.get(i);
      if (v !== null) {
        count++;
      }
      results[i] = count;
    }

    return new Series<number>('cumCount', Float64Column.from(results));
  }
}

// ── Module augmentation: add methods to Expr ──

declare module '../expr/expr' {
  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  interface Expr<T> {
    rank(): Expr<number>;
    denseRank(): Expr<number>;
    rowNumber(): Expr<number>;
    percentRank(): Expr<number>;
    ntile(n: number): Expr<number>;
    cumSum(): Expr<number>;
    cumMax(): Expr<number>;
    cumMin(): Expr<number>;
    cumProd(): Expr<number>;
    cumCount(): Expr<number>;
  }
}

Expr.prototype.rank = function (this: Expr<unknown>): Expr<number> {
  return new WindowRankExpr(this);
};

Expr.prototype.denseRank = function (this: Expr<unknown>): Expr<number> {
  return new WindowDenseRankExpr(this);
};

Expr.prototype.rowNumber = function (this: Expr<unknown>): Expr<number> {
  return new WindowRowNumberExpr(this);
};

Expr.prototype.percentRank = function (this: Expr<unknown>): Expr<number> {
  return new WindowPercentRankExpr(this);
};

Expr.prototype.ntile = function (this: Expr<unknown>, n: number): Expr<number> {
  return new WindowNtileExpr(this, n);
};

Expr.prototype.cumSum = function (this: Expr<unknown>): Expr<number> {
  return new CumSumExpr(this);
};

Expr.prototype.cumMax = function (this: Expr<unknown>): Expr<number> {
  return new CumMaxExpr(this);
};

Expr.prototype.cumMin = function (this: Expr<unknown>): Expr<number> {
  return new CumMinExpr(this);
};

Expr.prototype.cumProd = function (this: Expr<unknown>): Expr<number> {
  return new CumProdExpr(this);
};

Expr.prototype.cumCount = function (this: Expr<unknown>): Expr<number> {
  return new CumCountExpr(this);
};
