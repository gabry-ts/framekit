import { DataFrame } from '../dataframe';
import { Expr } from '../expr/expr';
import { Series } from '../series';
import { Column } from '../storage/column';
import { Float64Column } from '../storage/numeric';

// ── Window Ranking Expression Base ──

abstract class WindowRankingExpr extends Expr<number> {
  protected readonly _source: Expr<unknown>;
  protected readonly _descending: boolean;

  constructor(source: Expr<unknown>, descending = false) {
    super();
    this._source = source;
    this._descending = descending;
  }

  get dependencies(): string[] {
    return this._source.dependencies;
  }

  protected _sortCompare(a: unknown, b: unknown): number {
    return this._descending ? -compareValues(a, b) : compareValues(a, b);
  }

  abstract withDescending(desc: boolean): WindowRankingExpr;
}

// ── rank(): 1-based, ties get same rank, gaps after ──

export class WindowRankExpr extends WindowRankingExpr {
  toString(): string {
    return `rank(${this._source.toString()})`;
  }

  withDescending(desc: boolean): WindowRankExpr {
    return new WindowRankExpr(this._source, desc);
  }

  evaluate(df: DataFrame): Series<number> {
    const series = this._source.evaluate(df);
    const len = series.length;

    const indexed: { value: unknown; idx: number }[] = [];
    for (let i = 0; i < len; i++) {
      indexed.push({ value: series.get(i), idx: i });
    }

    indexed.sort((a, b) => this._sortCompare(a.value, b.value));

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

  withDescending(desc: boolean): WindowDenseRankExpr {
    return new WindowDenseRankExpr(this._source, desc);
  }

  evaluate(df: DataFrame): Series<number> {
    const series = this._source.evaluate(df);
    const len = series.length;

    const indexed: { value: unknown; idx: number }[] = [];
    for (let i = 0; i < len; i++) {
      indexed.push({ value: series.get(i), idx: i });
    }

    indexed.sort((a, b) => this._sortCompare(a.value, b.value));

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

  withDescending(desc: boolean): WindowRowNumberExpr {
    return new WindowRowNumberExpr(this._source, desc);
  }

  evaluate(df: DataFrame): Series<number> {
    const series = this._source.evaluate(df);
    const len = series.length;

    const indexed: { value: unknown; idx: number }[] = [];
    for (let i = 0; i < len; i++) {
      indexed.push({ value: series.get(i), idx: i });
    }

    indexed.sort((a, b) => this._sortCompare(a.value, b.value));

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

  withDescending(desc: boolean): WindowPercentRankExpr {
    return new WindowPercentRankExpr(this._source, desc);
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

    indexed.sort((a, b) => this._sortCompare(a.value, b.value));

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

  constructor(source: Expr<unknown>, n: number, descending = false) {
    super(source, descending);
    this._n = n;
  }

  toString(): string {
    return `ntile(${this._source.toString()}, ${this._n})`;
  }

  withDescending(desc: boolean): WindowNtileExpr {
    return new WindowNtileExpr(this._source, this._n, desc);
  }

  evaluate(df: DataFrame): Series<number> {
    const series = this._source.evaluate(df);
    const len = series.length;

    const indexed: { value: unknown; idx: number }[] = [];
    for (let i = 0; i < len; i++) {
      indexed.push({ value: series.get(i), idx: i });
    }

    indexed.sort((a, b) => this._sortCompare(a.value, b.value));

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

// ── Offset Window Expression Base ──

abstract class OffsetExpr extends Expr<number> {
  protected readonly _source: Expr<unknown>;
  protected readonly _offset: number;

  constructor(source: Expr<unknown>, offset: number) {
    super();
    this._source = source;
    this._offset = offset;
  }

  get dependencies(): string[] {
    return this._source.dependencies;
  }
}

// ── shift(n): lag (positive) / lead (negative) ──

export class ShiftExpr extends OffsetExpr {
  toString(): string {
    return `shift(${this._source.toString()}, ${this._offset})`;
  }

  evaluate(df: DataFrame): Series<number> {
    const series = this._source.evaluate(df);
    const len = series.length;
    const results = new Array<number | null>(len);

    for (let i = 0; i < len; i++) {
      const srcIdx = i - this._offset;
      if (srcIdx < 0 || srcIdx >= len) {
        results[i] = null;
      } else {
        const v = series.get(srcIdx);
        results[i] = v !== null && typeof v === 'number' ? v : null;
      }
    }

    return new Series<number>('shift', Float64Column.from(results));
  }
}

// ── diff(n): difference with n rows back ──

export class DiffExpr extends OffsetExpr {
  toString(): string {
    return `diff(${this._source.toString()}, ${this._offset})`;
  }

  evaluate(df: DataFrame): Series<number> {
    const series = this._source.evaluate(df);
    const len = series.length;
    const results = new Array<number | null>(len);

    for (let i = 0; i < len; i++) {
      const prevIdx = i - this._offset;
      if (prevIdx < 0 || prevIdx >= len) {
        results[i] = null;
      } else {
        const curr = series.get(i);
        const prev = series.get(prevIdx);
        if (curr !== null && typeof curr === 'number' && prev !== null && typeof prev === 'number') {
          results[i] = curr - prev;
        } else {
          results[i] = null;
        }
      }
    }

    return new Series<number>('diff', Float64Column.from(results));
  }
}

// ── pctChange(n): percentage change from n rows back ──

export class PctChangeExpr extends OffsetExpr {
  toString(): string {
    return `pctChange(${this._source.toString()}, ${this._offset})`;
  }

  evaluate(df: DataFrame): Series<number> {
    const series = this._source.evaluate(df);
    const len = series.length;
    const results = new Array<number | null>(len);

    for (let i = 0; i < len; i++) {
      const prevIdx = i - this._offset;
      if (prevIdx < 0 || prevIdx >= len) {
        results[i] = null;
      } else {
        const curr = series.get(i);
        const prev = series.get(prevIdx);
        if (curr !== null && typeof curr === 'number' && prev !== null && typeof prev === 'number' && prev !== 0) {
          results[i] = (curr - prev) / prev;
        } else {
          results[i] = null;
        }
      }
    }

    return new Series<number>('pctChange', Float64Column.from(results));
  }
}

// ── Rolling Window Expression Base ──

abstract class RollingExpr extends Expr<number> {
  protected readonly _source: Expr<unknown>;
  protected readonly _windowSize: number;

  constructor(source: Expr<unknown>, windowSize: number) {
    super();
    this._source = source;
    this._windowSize = windowSize;
  }

  get dependencies(): string[] {
    return this._source.dependencies;
  }

  protected _getNumericValues(df: DataFrame): (number | null)[] {
    const series = this._source.evaluate(df);
    const len = series.length;
    const values: (number | null)[] = [];
    for (let i = 0; i < len; i++) {
      const v = series.get(i);
      values.push(v !== null && typeof v === 'number' ? v : null);
    }
    return values;
  }
}

// ── rollingMean(n): n-row moving average ──

export class RollingMeanExpr extends RollingExpr {
  toString(): string {
    return `rollingMean(${this._source.toString()}, ${this._windowSize})`;
  }

  evaluate(df: DataFrame): Series<number> {
    const values = this._getNumericValues(df);
    const len = values.length;
    const results = new Array<number | null>(len);

    for (let i = 0; i < len; i++) {
      if (i < this._windowSize - 1) {
        results[i] = null;
      } else {
        let sum = 0;
        let count = 0;
        for (let j = i - this._windowSize + 1; j <= i; j++) {
          const v = values[j]!;
          if (v !== null) {
            sum += v;
            count++;
          }
        }
        results[i] = count > 0 ? sum / count : null;
      }
    }

    return new Series<number>('rollingMean', Float64Column.from(results));
  }
}

// ── rollingSum(n): n-row moving sum ──

export class RollingSumExpr extends RollingExpr {
  toString(): string {
    return `rollingSum(${this._source.toString()}, ${this._windowSize})`;
  }

  evaluate(df: DataFrame): Series<number> {
    const values = this._getNumericValues(df);
    const len = values.length;
    const results = new Array<number | null>(len);

    for (let i = 0; i < len; i++) {
      if (i < this._windowSize - 1) {
        results[i] = null;
      } else {
        let sum = 0;
        for (let j = i - this._windowSize + 1; j <= i; j++) {
          const v = values[j]!;
          if (v !== null) {
            sum += v;
          }
        }
        results[i] = sum;
      }
    }

    return new Series<number>('rollingSum', Float64Column.from(results));
  }
}

// ── rollingStd(n): n-row moving standard deviation (population) ──

export class RollingStdExpr extends RollingExpr {
  toString(): string {
    return `rollingStd(${this._source.toString()}, ${this._windowSize})`;
  }

  evaluate(df: DataFrame): Series<number> {
    const values = this._getNumericValues(df);
    const len = values.length;
    const results = new Array<number | null>(len);

    for (let i = 0; i < len; i++) {
      if (i < this._windowSize - 1) {
        results[i] = null;
      } else {
        const windowVals: number[] = [];
        for (let j = i - this._windowSize + 1; j <= i; j++) {
          const v = values[j]!;
          if (v !== null) {
            windowVals.push(v);
          }
        }
        if (windowVals.length < 2) {
          results[i] = null;
        } else {
          let sum = 0;
          for (const v of windowVals) sum += v;
          const mean = sum / windowVals.length;
          let sqDiffSum = 0;
          for (const v of windowVals) sqDiffSum += (v - mean) * (v - mean);
          // sample std (ddof=1) like pandas
          results[i] = Math.sqrt(sqDiffSum / (windowVals.length - 1));
        }
      }
    }

    return new Series<number>('rollingStd', Float64Column.from(results));
  }
}

// ── rollingMin(n): n-row moving minimum ──

export class RollingMinExpr extends RollingExpr {
  toString(): string {
    return `rollingMin(${this._source.toString()}, ${this._windowSize})`;
  }

  evaluate(df: DataFrame): Series<number> {
    const values = this._getNumericValues(df);
    const len = values.length;
    const results = new Array<number | null>(len);

    for (let i = 0; i < len; i++) {
      if (i < this._windowSize - 1) {
        results[i] = null;
      } else {
        let min: number | null = null;
        for (let j = i - this._windowSize + 1; j <= i; j++) {
          const v = values[j]!;
          if (v !== null) {
            min = min === null ? v : Math.min(min, v);
          }
        }
        results[i] = min;
      }
    }

    return new Series<number>('rollingMin', Float64Column.from(results));
  }
}

// ── rollingMax(n): n-row moving maximum ──

export class RollingMaxExpr extends RollingExpr {
  toString(): string {
    return `rollingMax(${this._source.toString()}, ${this._windowSize})`;
  }

  evaluate(df: DataFrame): Series<number> {
    const values = this._getNumericValues(df);
    const len = values.length;
    const results = new Array<number | null>(len);

    for (let i = 0; i < len; i++) {
      if (i < this._windowSize - 1) {
        results[i] = null;
      } else {
        let max: number | null = null;
        for (let j = i - this._windowSize + 1; j <= i; j++) {
          const v = values[j]!;
          if (v !== null) {
            max = max === null ? v : Math.max(max, v);
          }
        }
        results[i] = max;
      }
    }

    return new Series<number>('rollingMax', Float64Column.from(results));
  }
}

// ── ewm(alpha): exponential weighted moving average ──

export class EwmExpr extends Expr<number> {
  private readonly _source: Expr<unknown>;
  private readonly _alpha: number;

  constructor(source: Expr<unknown>, alpha: number) {
    super();
    this._source = source;
    this._alpha = alpha;
  }

  get dependencies(): string[] {
    return this._source.dependencies;
  }

  toString(): string {
    return `ewm(${this._source.toString()}, ${this._alpha})`;
  }

  evaluate(df: DataFrame): Series<number> {
    const series = this._source.evaluate(df);
    const len = series.length;
    const results = new Array<number | null>(len);
    let ewma: number | null = null;

    for (let i = 0; i < len; i++) {
      const v = series.get(i);
      if (v !== null && typeof v === 'number') {
        if (ewma === null) {
          ewma = v;
        } else {
          ewma = this._alpha * v + (1 - this._alpha) * ewma;
        }
        results[i] = ewma;
      } else {
        results[i] = ewma;
      }
    }

    return new Series<number>('ewm', Float64Column.from(results));
  }
}

// ── Partitioned Window Expression (.over()) ──

export class PartitionedWindowExpr extends Expr<number> {
  readonly _inner: Expr<number>;
  private readonly _partitionCols: string[];

  constructor(inner: Expr<number>, partitionCols: string[]) {
    super();
    this._inner = inner;
    this._partitionCols = partitionCols;
  }

  get dependencies(): string[] {
    return [...new Set([...this._inner.dependencies, ...this._partitionCols])];
  }

  toString(): string {
    return `${this._inner.toString()}.over(${this._partitionCols.join(', ')})`;
  }

  evaluate(df: DataFrame): Series<number> {
    const len = df.length;
    const results = new Array<number | null>(len);

    // Build partition map: serialized-key → row indices
    const partitionMap = new Map<string, number[]>();
    const partCols: Column<unknown>[] = this._partitionCols.map((name) => df.col(name).column);

    for (let i = 0; i < len; i++) {
      const key = serializeKey(partCols, i);
      const group = partitionMap.get(key);
      if (group) {
        group.push(i);
      } else {
        partitionMap.set(key, [i]);
      }
    }

    // For each partition, build a sub-DataFrame, evaluate inner expr, scatter results back
    const columnOrder = df.columns;
    for (const indices of partitionMap.values()) {
      const int32Indices = new Int32Array(indices);
      const newColumns = new Map<string, Column<unknown>>();
      for (const name of columnOrder) {
        newColumns.set(name, df.col(name).column.take(int32Indices));
      }
      const subDf = new DataFrame(newColumns, columnOrder);

      const subResult = this._inner.evaluate(subDf);
      for (let i = 0; i < indices.length; i++) {
        results[indices[i]!] = subResult.get(i);
      }
    }

    return new Series<number>('over', Float64Column.from(results));
  }
}

function serializeKey(columns: Column<unknown>[], index: number): string {
  const parts: string[] = [];
  for (const column of columns) {
    const v = column.get(index);
    if (v === null) {
      parts.push('\0null');
    } else if (v instanceof Date) {
      parts.push(`\0d${v.getTime()}`);
    } else if (typeof v === 'number' || typeof v === 'string' || typeof v === 'boolean') {
      parts.push(`\0${typeof v}${String(v)}`);
    } else {
      parts.push(`\0obj${JSON.stringify(v)}`);
    }
  }
  return parts.join('\x01');
}

// ── Ordered Window Expression (.orderBy()) ──

export class OrderedWindowExpr extends Expr<number> {
  private readonly _inner: Expr<number>;
  private readonly _orderCol: string;
  private readonly _direction: 'asc' | 'desc';

  constructor(inner: Expr<number>, orderCol: string, direction: 'asc' | 'desc' = 'asc') {
    super();
    this._inner = inner;
    this._orderCol = orderCol;
    this._direction = direction;
  }

  get dependencies(): string[] {
    return [...new Set([...this._inner.dependencies, this._orderCol])];
  }

  toString(): string {
    return `${this._inner.toString()}.orderBy(${this._orderCol}, ${this._direction})`;
  }

  evaluate(df: DataFrame): Series<number> {
    const len = df.length;
    const descending = this._direction === 'desc';

    // Apply ranking direction to ranking expressions in the tree
    const directedInner = applyRankingDirection(this._inner, descending);

    // Pre-sort DF by orderBy column (affects sequential functions like cumSum, shift, rolling)
    const orderCol = df.col(this._orderCol).column;
    const indices: number[] = [];
    for (let i = 0; i < len; i++) indices.push(i);

    const dir = descending ? -1 : 1;
    indices.sort((a, b) => dir * compareValues(orderCol.get(a), orderCol.get(b)));

    // Create a reordered DataFrame
    const int32Indices = new Int32Array(indices);
    const columnOrder = df.columns;
    const newColumns = new Map<string, Column<unknown>>();
    for (const name of columnOrder) {
      newColumns.set(name, df.col(name).column.take(int32Indices));
    }
    const sortedDf = new DataFrame(newColumns, columnOrder);

    // Evaluate on the sorted DataFrame
    const sortedResult = directedInner.evaluate(sortedDf);

    // Map results back to original order
    const results = new Array<number | null>(len);
    for (let i = 0; i < len; i++) {
      results[indices[i]!] = sortedResult.get(i);
    }

    return new Series<number>('orderBy', Float64Column.from(results));
  }
}

function applyRankingDirection(expr: Expr<number>, descending: boolean): Expr<number> {
  if (expr instanceof WindowRankingExpr) {
    return expr.withDescending(descending);
  }
  if (expr instanceof PartitionedWindowExpr) {
    const directedInner = applyRankingDirection(expr._inner, descending);
    return new PartitionedWindowExpr(directedInner, expr['_partitionCols']);
  }
  return expr;
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
    shift(offset: number): Expr<number>;
    diff(offset?: number): Expr<number>;
    pctChange(offset?: number): Expr<number>;
    rollingMean(windowSize: number): Expr<number>;
    rollingSum(windowSize: number): Expr<number>;
    rollingStd(windowSize: number): Expr<number>;
    rollingMin(windowSize: number): Expr<number>;
    rollingMax(windowSize: number): Expr<number>;
    ewm(alpha: number): Expr<number>;
    over(...partitionCols: string[]): Expr<number>;
    orderBy(column: string, direction?: 'asc' | 'desc'): Expr<number>;
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

Expr.prototype.shift = function (this: Expr<unknown>, offset: number): Expr<number> {
  return new ShiftExpr(this, offset);
};

Expr.prototype.diff = function (this: Expr<unknown>, offset = 1): Expr<number> {
  return new DiffExpr(this, offset);
};

Expr.prototype.pctChange = function (this: Expr<unknown>, offset = 1): Expr<number> {
  return new PctChangeExpr(this, offset);
};

Expr.prototype.rollingMean = function (this: Expr<unknown>, windowSize: number): Expr<number> {
  return new RollingMeanExpr(this, windowSize);
};

Expr.prototype.rollingSum = function (this: Expr<unknown>, windowSize: number): Expr<number> {
  return new RollingSumExpr(this, windowSize);
};

Expr.prototype.rollingStd = function (this: Expr<unknown>, windowSize: number): Expr<number> {
  return new RollingStdExpr(this, windowSize);
};

Expr.prototype.rollingMin = function (this: Expr<unknown>, windowSize: number): Expr<number> {
  return new RollingMinExpr(this, windowSize);
};

Expr.prototype.rollingMax = function (this: Expr<unknown>, windowSize: number): Expr<number> {
  return new RollingMaxExpr(this, windowSize);
};

Expr.prototype.ewm = function (this: Expr<unknown>, alpha: number): Expr<number> {
  return new EwmExpr(this, alpha);
};

Expr.prototype.over = function (this: Expr<number>, ...partitionCols: string[]): Expr<number> {
  return new PartitionedWindowExpr(this, partitionCols);
};

Expr.prototype.orderBy = function (
  this: Expr<number>,
  column: string,
  direction: 'asc' | 'desc' = 'asc',
): Expr<number> {
  return new OrderedWindowExpr(this, column, direction);
};
