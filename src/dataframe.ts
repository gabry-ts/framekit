import { DType } from './types/dtype';
import type {
  CSVReadOptions,
  CSVWriteOptions,
  JSONReadOptions,
  JSONWriteOptions,
  PrintOptions,
  SampleOptions,
  ExcelReadOptions,
  ExcelWriteOptions,
  ParquetReadOptions,
  ParquetWriteOptions,
  SQLWriteOptions,
} from './types/options';
import {
  ColumnNotFoundError,
  ErrorCode,
  FrameKitError,
  IOError,
  ShapeMismatchError,
} from './errors';
import { Column } from './storage/column';
import { Float64Column, Int32Column } from './storage/numeric';
import { Utf8Column } from './storage/string';
import { BooleanColumn } from './storage/boolean';
import { DateColumn } from './storage/date';
import { ObjectColumn } from './storage/object';
import { BitArray } from './storage/bitarray';
import { Series, _registerDataFrameFactory } from './series';
import { Expr, col, ComparisonExpr, ColumnExpr, LiteralExpr } from './expr/expr';
import { parseCSV } from './io/csv/parser';
import { writeCSV } from './io/csv/writer';
import { streamCSVFile } from './engine/streaming/scanner';
import type { StreamCSVOptions } from './engine/streaming/scanner';
import { streamNDJSONFile } from './engine/streaming/ndjson-scanner';
import type { StreamNDJSONOptions } from './engine/streaming/ndjson-scanner';
import { writeJSON, writeNDJSON } from './io/json/writer';
import { readExcelFile } from './io/excel/reader';
import { readParquetFile } from './io/parquet/reader';
import { writeExcelFile } from './io/excel/writer';
import { writeParquetFile } from './io/parquet/writer';
import { toArrowTable } from './io/arrow/to-arrow';
import { fromArrowTable } from './io/arrow/from-arrow';
import { GroupBy } from './ops/groupby';
import { hashJoin } from './ops/join';
import type { JoinType, JoinOnMapping, JoinOptions } from './ops/join';
import { lookup as lookupImpl } from './ops/lookup';
import { spread as spreadImpl } from './ops/spread';
import type { SpreadOptions } from './ops/spread';
import { unroll as unrollImpl } from './ops/unroll';
import type { UnrollOptions } from './ops/unroll';
import { impute as imputeImpl } from './ops/impute';
import type { ImputeOptions, ImputeValue } from './ops/impute';
import { pivot } from './ops/pivot';
import type { PivotOptions } from './ops/pivot';
import { melt } from './ops/melt';
import type { MeltOptions } from './ops/melt';
import { transpose, concat } from './ops/reshape';
import { union, intersection, difference } from './ops/setops';
import { writeSQL } from './io/sql/writer';
import type { LazyFrame } from './lazy';
import { createLazyFrame } from './lazy';
import { executeQuery } from './ops/query';

// eslint-disable-next-line @typescript-eslint/no-explicit-any
export type ReaderFn = (source: string | Buffer, options?: any) => Promise<DataFrame>;
// eslint-disable-next-line @typescript-eslint/no-explicit-any
export type WriterFn = (df: DataFrame, path: string, options?: any) => Promise<void>;

export class DataFrame<S extends Record<string, unknown> = Record<string, unknown>> {
  private static readonly _readers = new Map<string, ReaderFn>();
  private static readonly _writers = new Map<string, WriterFn>();

  private readonly _columns: Map<string, Column<unknown>>;
  private readonly _columnOrder: string[];

  constructor(columns: Map<string, Column<unknown>>, columnOrder: string[]) {
    this._columns = columns;
    this._columnOrder = columnOrder;
    for (const col of columns.values()) {
      col.addRef();
    }
  }

  get shape(): [number, number] {
    return [this.length, this._columnOrder.length];
  }

  get columns(): string[] {
    return [...this._columnOrder];
  }

  get dtypes(): Record<string, DType> {
    const result: Record<string, DType> = {};
    for (const name of this._columnOrder) {
      result[name] = this._columns.get(name)!.dtype;
    }
    return result;
  }

  get length(): number {
    if (this._columnOrder.length === 0) return 0;
    return this._columns.get(this._columnOrder[0]!)!.length;
  }

  col<K extends string & keyof S>(name: K): Series<S[K]> {
    const column = this._columns.get(name);
    if (!column) {
      throw new ColumnNotFoundError(name, this._columnOrder);
    }
    return new Series<S[K]>(name, column as Column<S[K]>);
  }

  row(index: number): S {
    if (index < 0 || index >= this.length) {
      throw new FrameKitError(
        ErrorCode.INVALID_OPERATION,
        `Row index ${index} out of bounds for DataFrame with ${this.length} rows`,
      );
    }
    const result: Record<string, unknown> = {};
    for (const name of this._columnOrder) {
      result[name] = this._columns.get(name)!.get(index);
    }
    return result as S;
  }

  *[Symbol.iterator](): Iterator<S> {
    for (let i = 0; i < this.length; i++) {
      yield this.row(i);
    }
  }

  rows(): Iterator<S> {
    return this[Symbol.iterator]();
  }

  toArray(): S[] {
    const result: S[] = [];
    for (let i = 0; i < this.length; i++) {
      result.push(this.row(i));
    }
    return result;
  }

  clone(): DataFrame<S> {
    const clonedColumns = new Map<string, Column<unknown>>();
    for (const [name, col] of this._columns) {
      clonedColumns.set(name, col.clone());
    }
    return new DataFrame<S>(clonedColumns, [...this._columnOrder]);
  }

  reify(): DataFrame<S> {
    return this.clone();
  }

  lazy(): LazyFrame<S> {
    return createLazyFrame(this);
  }

  select<K extends string & keyof S>(...columns: K[]): DataFrame<Pick<S, K>> {
    for (const name of columns) {
      if (!this._columns.has(name)) {
        throw new ColumnNotFoundError(name, this._columnOrder);
      }
    }
    const newColumns = new Map<string, Column<unknown>>();
    for (const name of columns) {
      newColumns.set(name, this._columns.get(name)!);
    }
    return new DataFrame<Pick<S, K>>(newColumns, [...columns]);
  }

  drop<K extends string & keyof S>(...columns: K[]): DataFrame<Omit<S, K>> {
    for (const name of columns) {
      if (!this._columns.has(name)) {
        throw new ColumnNotFoundError(name, this._columnOrder);
      }
    }
    const dropSet = new Set<string>(columns);
    const newColumns = new Map<string, Column<unknown>>();
    const newOrder: string[] = [];
    for (const name of this._columnOrder) {
      if (!dropSet.has(name)) {
        newColumns.set(name, this._columns.get(name)!);
        newOrder.push(name);
      }
    }
    return new DataFrame<Omit<S, K>>(newColumns, newOrder);
  }

  head(n = 5): DataFrame<S> {
    return this.slice(0, Math.min(n, this.length));
  }

  tail(n = 5): DataFrame<S> {
    const start = Math.max(0, this.length - n);
    return this.slice(start, this.length);
  }

  slice(start: number, end?: number): DataFrame<S> {
    const resolvedEnd = end === undefined ? this.length : Math.min(end, this.length);
    const resolvedStart = Math.max(0, start);
    const newColumns = new Map<string, Column<unknown>>();
    for (const name of this._columnOrder) {
      newColumns.set(name, this._columns.get(name)!.slice(resolvedStart, resolvedEnd));
    }
    return new DataFrame<S>(newColumns, [...this._columnOrder]);
  }

  withColumn<K extends string, V>(
    name: K,
    valuesOrFnOrExpr: V[] | ((row: S) => V) | Expr<V>,
  ): DataFrame<S & Record<K, V>> {
    type Result = S & Record<K, V>;

    if (valuesOrFnOrExpr instanceof Expr) {
      const series = valuesOrFnOrExpr.evaluate(this as DataFrame);
      const newColumns = new Map<string, Column<unknown>>(this._columns);
      newColumns.set(name, series.column);

      const newOrder = this._columnOrder.includes(name)
        ? [...this._columnOrder]
        : [...this._columnOrder, name];

      return new DataFrame<Result>(newColumns, newOrder);
    }

    let columnValues: V[];

    if (typeof valuesOrFnOrExpr === 'function') {
      const fn = valuesOrFnOrExpr as (row: S) => V;
      columnValues = [];
      for (let i = 0; i < this.length; i++) {
        columnValues.push(fn(this.row(i)));
      }
    } else {
      columnValues = valuesOrFnOrExpr;
      if (columnValues.length !== this.length) {
        throw new ShapeMismatchError(
          `Column '${name}' has length ${columnValues.length}, expected ${this.length}`,
        );
      }
    }

    const dtype = detectDType(columnValues as unknown[]);
    const newCol = buildColumn(dtype, columnValues as unknown[]);

    const newColumns = new Map<string, Column<unknown>>(this._columns);
    newColumns.set(name, newCol);

    const newOrder = this._columnOrder.includes(name)
      ? [...this._columnOrder]
      : [...this._columnOrder, name];

    return new DataFrame<Result>(newColumns, newOrder);
  }

  assign(other: DataFrame): DataFrame {
    if (other.length !== this.length && other.columns.length > 0 && this._columnOrder.length > 0) {
      throw new ShapeMismatchError(
        `Cannot assign DataFrame with ${other.length} rows to DataFrame with ${this.length} rows`,
      );
    }

    const newColumns = new Map<string, Column<unknown>>(this._columns);
    const newOrder = [...this._columnOrder];

    for (const name of other.columns) {
      newColumns.set(name, other._columns.get(name)!);
      if (!this._columnOrder.includes(name)) {
        newOrder.push(name);
      }
    }

    return new DataFrame(newColumns, newOrder);
  }

  derive(exprs: Record<string, (row: S) => unknown>): DataFrame {
    // eslint-disable-next-line @typescript-eslint/no-this-alias
    let result: DataFrame = this;
    for (const [name, fn] of Object.entries(exprs)) {
      if (this.length === 0) {
        result = result.withColumn(name, []);
        continue;
      }

      const first = fn(this.row(0));
      if (first instanceof Expr) {
        result = result.withColumn(name, first.evaluate(this as DataFrame).toArray());
        continue;
      }

      const values: unknown[] = [first];
      for (let i = 1; i < this.length; i++) {
        values.push(fn(this.row(i)));
      }
      result = result.withColumn(name, values);
    }
    return result;
  }

  lookup(other: DataFrame, on: string, values?: string[]): DataFrame {
    return lookupImpl(this, other, on, values);
  }

  spread(column: string, options?: SpreadOptions): DataFrame {
    return spreadImpl(this, column, options);
  }

  unroll(columns: string | string[], options?: UnrollOptions): DataFrame {
    return unrollImpl(this, columns, options);
  }

  impute(values: Record<string, ImputeValue>, options?: ImputeOptions): DataFrame {
    return imputeImpl(this as DataFrame<Record<string, unknown>>, values, options);
  }

  relocate(columns: string[], options: { before?: string; after?: string }): DataFrame {
    const { before, after } = options;

    if (before !== undefined && after !== undefined) {
      throw new FrameKitError(
        ErrorCode.INVALID_OPERATION,
        'Cannot specify both "before" and "after" in relocate',
      );
    }
    if (before === undefined && after === undefined) {
      throw new FrameKitError(
        ErrorCode.INVALID_OPERATION,
        'Must specify either "before" or "after" in relocate',
      );
    }

    // Validate source columns exist
    for (const col of columns) {
      if (!this._columns.has(col)) {
        throw new ColumnNotFoundError(col, this._columnOrder);
      }
    }

    // Validate anchor column exists
    const anchor = (before ?? after)!;
    if (!this._columns.has(anchor)) {
      throw new ColumnNotFoundError(anchor, this._columnOrder);
    }

    // Build new order: remove source columns, then insert at anchor position
    const remaining = this._columnOrder.filter((c) => !columns.includes(c));
    const anchorIdx = remaining.indexOf(anchor);
    const insertIdx = before !== undefined ? anchorIdx : anchorIdx + 1;

    const newOrder = [...remaining.slice(0, insertIdx), ...columns, ...remaining.slice(insertIdx)];

    return new DataFrame(new Map(this._columns), newOrder);
  }

  rename(mapping: Record<string, string>): DataFrame<S> {
    // Validate that all source columns exist
    for (const oldName of Object.keys(mapping)) {
      if (!this._columns.has(oldName)) {
        throw new ColumnNotFoundError(oldName, this._columnOrder);
      }
    }

    const newColumns = new Map<string, Column<unknown>>();
    const newOrder: string[] = [];

    for (const name of this._columnOrder) {
      const newName = mapping[name] ?? name;
      newColumns.set(newName, this._columns.get(name)!);
      newOrder.push(newName);
    }

    return new DataFrame<S>(newColumns, newOrder);
  }

  filter(predicateOrExpr: ((row: S) => boolean) | Expr<boolean>): DataFrame<S> {
    if (predicateOrExpr instanceof Expr) {
      const maybeCmp = predicateOrExpr as unknown as ComparisonExpr<unknown>;
      const cmpAny = maybeCmp as unknown as {
        _left?: Expr<unknown>;
        _right?: Expr<unknown>;
        _op?: 'eq' | 'neq' | 'gt' | 'gte' | 'lt' | 'lte';
      };

      if (
        cmpAny._op !== undefined &&
        cmpAny._left instanceof ColumnExpr &&
        cmpAny._right instanceof LiteralExpr
      ) {
        const columnName = cmpAny._left.dependencies[0]!;
        const literal = (cmpAny._right as unknown as { _value: unknown })._value;
        const source = this._columns.get(columnName);
        if (source) {
          // Fast path: interned Utf8Column with eq/neq — integer comparison on dictionary indices
          if (
            source instanceof Utf8Column &&
            source.isInterned &&
            typeof literal === 'string' &&
            (cmpAny._op === 'eq' || cmpAny._op === 'neq')
          ) {
            const interned = source.internedStorage!;
            const targetIdx = interned.dictionary.indexOf(literal);
            const isEq = cmpAny._op === 'eq';

            if (isEq && targetIdx === -1) {
              return this.slice(0, 0);
            }

            const internedIndices = interned.indices;
            const n = this.length;

            if (source.allValid) {
              // No nulls — pure integer scan
              let count = 0;
              if (isEq) {
                for (let i = 0; i < n; i++) {
                  if (internedIndices[i] === targetIdx) count++;
                }
              } else {
                if (targetIdx === -1) {
                  // neq with value not in dictionary: all match
                  return this;
                }
                for (let i = 0; i < n; i++) {
                  if (internedIndices[i] !== targetIdx) count++;
                }
              }

              if (count === n) return this;
              if (count === 0) return this.slice(0, 0);

              const indices = new Int32Array(count);
              let pos = 0;
              if (isEq) {
                for (let i = 0; i < n; i++) {
                  if (internedIndices[i] === targetIdx) indices[pos++] = i;
                }
              } else {
                for (let i = 0; i < n; i++) {
                  if (internedIndices[i] !== targetIdx) indices[pos++] = i;
                }
              }
              return this._takeByInt32Indices(indices);
            } else {
              // Has nulls — check null mask
              const nullMask = source.nullMask;
              let count = 0;
              if (isEq) {
                for (let i = 0; i < n; i++) {
                  if (nullMask.getUnsafe(i) && internedIndices[i] === targetIdx) count++;
                }
              } else {
                for (let i = 0; i < n; i++) {
                  if (nullMask.getUnsafe(i) && internedIndices[i] !== targetIdx) count++;
                }
              }

              if (count === n) return this;
              if (count === 0) return this.slice(0, 0);

              const indices = new Int32Array(count);
              let pos = 0;
              if (isEq) {
                for (let i = 0; i < n; i++) {
                  if (nullMask.getUnsafe(i) && internedIndices[i] === targetIdx) indices[pos++] = i;
                }
              } else {
                for (let i = 0; i < n; i++) {
                  if (nullMask.getUnsafe(i) && internedIndices[i] !== targetIdx) indices[pos++] = i;
                }
              }
              return this._takeByInt32Indices(indices);
            }
          }

          // Generic fast path for other ops
          let count = 0;
          for (let i = 0; i < this.length; i++) {
            const v = source.get(i);
            if (v === null || literal === null) continue;
            let keep = false;
            switch (cmpAny._op) {
              case 'eq':
                keep = v === literal;
                break;
              case 'neq':
                keep = v !== literal;
                break;
              case 'gt':
                keep = (v as number) > (literal as number);
                break;
              case 'gte':
                keep = (v as number) >= (literal as number);
                break;
              case 'lt':
                keep = (v as number) < (literal as number);
                break;
              case 'lte':
                keep = (v as number) <= (literal as number);
                break;
            }
            if (keep) count++;
          }

          if (count === this.length) {
            return this;
          }
          if (count === 0) {
            return this.slice(0, 0);
          }

          const indices = new Int32Array(count);
          let pos = 0;
          for (let i = 0; i < this.length; i++) {
            const v = source.get(i);
            if (v === null || literal === null) continue;
            let keep = false;
            switch (cmpAny._op) {
              case 'eq':
                keep = v === literal;
                break;
              case 'neq':
                keep = v !== literal;
                break;
              case 'gt':
                keep = (v as number) > (literal as number);
                break;
              case 'gte':
                keep = (v as number) >= (literal as number);
                break;
              case 'lt':
                keep = (v as number) < (literal as number);
                break;
              case 'lte':
                keep = (v as number) <= (literal as number);
                break;
            }
            if (keep) indices[pos++] = i;
          }
          return this._takeByInt32Indices(indices);
        }
      }

      const boolSeries = predicateOrExpr.evaluate(this as DataFrame);
      let count = 0;
      for (let i = 0; i < this.length; i++) {
        if (boolSeries.get(i) === true) {
          count++;
        }
      }
      if (count === this.length) {
        return this;
      }
      if (count === 0) {
        return this.slice(0, 0);
      }
      const indices = new Int32Array(count);
      let pos = 0;
      for (let i = 0; i < this.length; i++) {
        if (boolSeries.get(i) === true) {
          indices[pos++] = i;
        }
      }
      return this._takeByInt32Indices(indices);
    }

    const predicate = predicateOrExpr;
    let count = 0;
    for (let i = 0; i < this.length; i++) {
      if (predicate(this.row(i))) {
        count++;
      }
    }
    if (count === this.length) {
      return this;
    }
    if (count === 0) {
      return this.slice(0, 0);
    }
    const indices = new Int32Array(count);
    let pos = 0;
    for (let i = 0; i < this.length; i++) {
      if (predicate(this.row(i))) {
        indices[pos++] = i;
      }
    }
    return this._takeByInt32Indices(indices);
  }

  apply(fn: (row: S) => S): DataFrame<S> {
    if (this.length === 0) {
      return new DataFrame<S>(new Map(), [...this._columnOrder]);
    }
    const rows: S[] = [];
    for (let i = 0; i < this.length; i++) {
      rows.push(fn(this.row(i)));
    }
    return DataFrame.fromRows<S>(rows);
  }

  where(column: string, op: '=' | '!=' | '>' | '>=' | '<' | '<=', value: unknown): DataFrame<S> {
    const colExpr = col(column);
    const litValue = value;
    let expr: Expr<boolean>;

    switch (op) {
      case '=':
        expr = colExpr.eq(litValue);
        break;
      case '!=':
        expr = colExpr.neq(litValue);
        break;
      case '>':
        expr = colExpr.gt(litValue as never);
        break;
      case '>=':
        expr = colExpr.gte(litValue as never);
        break;
      case '<':
        expr = colExpr.lt(litValue as never);
        break;
      case '<=':
        expr = colExpr.lte(litValue as never);
        break;
    }

    return this.filter(expr);
  }

  sortBy(columns: string | string[], order?: 'asc' | 'desc' | ('asc' | 'desc')[]): DataFrame<S> {
    const cols = Array.isArray(columns) ? columns : [columns];
    const orders = Array.isArray(order) ? order : cols.map(() => order ?? 'asc');

    for (const name of cols) {
      if (!this._columns.has(name)) {
        throw new ColumnNotFoundError(name, this._columnOrder);
      }
    }

    // Fast path: single-column sort for primitive/date values
    if (cols.length === 1) {
      const sortCol = this._columns.get(cols[0]!)!;
      const desc = orders[0] === 'desc';

      // Interned Utf8 fast path: sort dictionary once, then sort by rank integer
      if (sortCol instanceof Utf8Column && sortCol.isInterned) {
        const interned = sortCol.internedStorage!;
        const dictLen = interned.dictionary.length;

        // Sort dictionary entries to get rank mapping
        const sortedDictIndices = Array.from({ length: dictLen }, (_, i) => i);
        sortedDictIndices.sort((a, b) => {
          const sa = interned.dictionary[a]!;
          const sb = interned.dictionary[b]!;
          const cmp = sa < sb ? -1 : sa > sb ? 1 : 0;
          return desc ? -cmp : cmp;
        });

        // Build rank array: rank[dictIdx] = sortPosition
        const rank = new Uint32Array(dictLen);
        for (let r = 0; r < dictLen; r++) {
          rank[sortedDictIndices[r]!] = r;
        }

        const n = this.length;
        const internedIndices = interned.indices;
        const indices = Array.from({ length: n }, (_, i) => i);

        if (sortCol.allValid) {
          indices.sort((a, b) => rank[internedIndices[a]!]! - rank[internedIndices[b]!]!);
        } else {
          const nullMask = sortCol.nullMask;
          indices.sort((a, b) => {
            const aValid = nullMask.getUnsafe(a);
            const bValid = nullMask.getUnsafe(b);
            if (!aValid && !bValid) return 0;
            if (!aValid) return 1;
            if (!bValid) return -1;
            return rank[internedIndices[a]!]! - rank[internedIndices[b]!]!;
          });
        }

        const int32Indices = new Int32Array(indices);
        const newColumns = new Map<string, Column<unknown>>();
        for (const name of this._columnOrder) {
          newColumns.set(name, this._columns.get(name)!.take(int32Indices));
        }
        return new DataFrame<S>(newColumns, [...this._columnOrder]);
      }

      const vals: unknown[] = new Array(this.length);
      for (let i = 0; i < this.length; i++) vals[i] = sortCol.get(i);

      const indices = Array.from({ length: this.length }, (_, i) => i);
      indices.sort((a, b) => {
        const va = vals[a]!;
        const vb = vals[b]!;
        const aIsNull = va === null || va === undefined;
        const bIsNull = vb === null || vb === undefined;
        if (aIsNull && bIsNull) return 0;
        if (aIsNull) return 1;
        if (bIsNull) return -1;

        let cmp = 0;
        if (typeof va === 'number' && typeof vb === 'number') {
          cmp = va - vb;
        } else if (typeof va === 'string' && typeof vb === 'string') {
          cmp = va < vb ? -1 : va > vb ? 1 : 0;
        } else if (typeof va === 'boolean' && typeof vb === 'boolean') {
          cmp = (va ? 1 : 0) - (vb ? 1 : 0);
        } else if (va instanceof Date && vb instanceof Date) {
          cmp = va.getTime() - vb.getTime();
        }
        return desc ? -cmp : cmp;
      });

      const int32Indices = new Int32Array(indices);
      const newColumns = new Map<string, Column<unknown>>();
      for (const name of this._columnOrder) {
        newColumns.set(name, this._columns.get(name)!.take(int32Indices));
      }
      return new DataFrame<S>(newColumns, [...this._columnOrder]);
    }

    if (
      cols.length === 2 &&
      this._columns.get(cols[0]!)!.dtype === DType.Utf8 &&
      (this._columns.get(cols[1]!)!.dtype === DType.Float64 ||
        this._columns.get(cols[1]!)!.dtype === DType.Int32)
    ) {
      const c0 = this._columns.get(cols[0]!)!;
      const c1 = this._columns.get(cols[1]!)!;
      const v0: (string | null)[] = new Array<string | null>(this.length);
      const v1: (number | null)[] = new Array<number | null>(this.length);
      for (let i = 0; i < this.length; i++) {
        v0[i] = c0.get(i) as string | null;
        v1[i] = c1.get(i) as number | null;
      }

      const d0 = orders[0] === 'desc';
      const d1 = orders[1] === 'desc';
      const indices = Array.from({ length: this.length }, (_, i) => i);
      indices.sort((a, b) => {
        const a0 = v0[a];
        const b0 = v0[b];
        const a0Null = a0 === null;
        const b0Null = b0 === null;
        if (!a0Null || !b0Null) {
          if (a0Null) return 1;
          if (b0Null) return -1;
          const cmp0 = a0! < b0! ? -1 : a0! > b0! ? 1 : 0;
          if (cmp0 !== 0) return d0 ? -cmp0 : cmp0;
        }

        const a1 = v1[a];
        const b1 = v1[b];
        const a1Null = a1 === null;
        const b1Null = b1 === null;
        if (a1Null && b1Null) return 0;
        if (a1Null) return 1;
        if (b1Null) return -1;
        const cmp1 = a1! - b1!;
        return d1 ? -cmp1 : cmp1;
      });

      const int32Indices = new Int32Array(indices);
      return this._takeByInt32Indices(int32Indices);
    }

    // Build arrays of raw values for each sort column (avoids repeated get calls)
    const sortValues: unknown[][] = cols.map((name) => {
      const col = this._columns.get(name)!;
      const vals: unknown[] = new Array(this.length);
      for (let i = 0; i < this.length; i++) {
        vals[i] = col.get(i);
      }
      return vals;
    });

    // Create index array and sort it
    const indices = Array.from({ length: this.length }, (_, i) => i);
    indices.sort((a, b) => {
      for (let c = 0; c < cols.length; c++) {
        const va = sortValues[c]![a]!;
        const vb = sortValues[c]![b]!;
        const aIsNull = va === null || va === undefined;
        const bIsNull = vb === null || vb === undefined;

        if (aIsNull && bIsNull) continue;
        if (aIsNull) return 1; // nulls last
        if (bIsNull) return -1;

        let cmp = 0;
        if (va instanceof Date && vb instanceof Date) {
          cmp = va.getTime() - vb.getTime();
        } else if (typeof va === 'string' && typeof vb === 'string') {
          cmp = va < vb ? -1 : va > vb ? 1 : 0;
        } else if (typeof va === 'number' && typeof vb === 'number') {
          cmp = va - vb;
        } else if (typeof va === 'boolean' && typeof vb === 'boolean') {
          cmp = (va ? 1 : 0) - (vb ? 1 : 0);
        }

        if (cmp !== 0) {
          return orders[c] === 'desc' ? -cmp : cmp;
        }
      }
      return 0;
    });

    const int32Indices = new Int32Array(indices);
    const newColumns = new Map<string, Column<unknown>>();
    for (const name of this._columnOrder) {
      newColumns.set(name, this._columns.get(name)!.take(int32Indices));
    }
    return new DataFrame<S>(newColumns, [...this._columnOrder]);
  }

  unique(columns?: string | string[], keep: 'first' | 'last' = 'first'): DataFrame<S> {
    const cols =
      columns === undefined ? this._columnOrder : Array.isArray(columns) ? columns : [columns];

    for (const name of cols) {
      if (!this._columns.has(name)) {
        throw new ColumnNotFoundError(name, this._columnOrder);
      }
    }

    const seen = new Set<string>();
    const indices: number[] = [];

    if (keep === 'first') {
      for (let i = 0; i < this.length; i++) {
        const key = this._rowKey(i, cols);
        if (!seen.has(key)) {
          seen.add(key);
          indices.push(i);
        }
      }
    } else {
      // keep === 'last': scan forward but only keep last occurrence
      const lastIndex = new Map<string, number>();
      const order: string[] = [];
      for (let i = 0; i < this.length; i++) {
        const key = this._rowKey(i, cols);
        if (!lastIndex.has(key)) {
          order.push(key);
        }
        lastIndex.set(key, i);
      }
      for (const key of order) {
        indices.push(lastIndex.get(key)!);
      }
    }

    return this._takeByIndices(indices);
  }

  sample(n: number, options?: SampleOptions): DataFrame<S> {
    if (this.length === 0) {
      return this.clone();
    }

    let count: number;
    if (n >= 1) {
      count = Math.min(Math.floor(n), this.length);
    } else if (n > 0 && n < 1) {
      count = Math.max(1, Math.round(n * this.length));
    } else {
      throw new FrameKitError(
        ErrorCode.INVALID_OPERATION,
        `sample size must be positive, got ${n}`,
      );
    }

    const rng = options?.seed !== undefined ? seededRandom(options.seed) : Math.random;

    // Fisher-Yates shuffle on index array, pick first `count` elements
    const indices = Array.from({ length: this.length }, (_, i) => i);
    for (let i = indices.length - 1; i > 0; i--) {
      const j = Math.floor(rng() * (i + 1));
      const tmp = indices[i]!;
      indices[i] = indices[j]!;
      indices[j] = tmp;
    }

    return this._takeByIndices(indices.slice(0, count));
  }

  groupBy<K extends string & keyof S>(...keys: K[]): GroupBy<S, K> {
    return new GroupBy<S, K>(this, keys);
  }

  join<R extends Record<string, unknown>>(
    other: DataFrame<R>,
    on: string | string[] | JoinOnMapping,
    how: JoinType = 'inner',
    options?: JoinOptions,
  ): DataFrame<Record<string, unknown>> {
    return hashJoin(this, other, on, how, options);
  }

  pivot(options: PivotOptions): DataFrame<Record<string, unknown>> {
    return pivot(this as DataFrame<Record<string, unknown>>, options);
  }

  melt(options: MeltOptions): DataFrame<Record<string, unknown>> {
    return melt(this as DataFrame<Record<string, unknown>>, options);
  }

  explode(column: string): DataFrame<Record<string, unknown>> {
    if (!this._columns.has(column)) {
      throw new ColumnNotFoundError(column, this._columnOrder);
    }

    const explodeCol = this._columns.get(column)!;
    const otherColNames = this._columnOrder.filter((n) => n !== column);

    // Build result arrays
    const resultArrays: Record<string, unknown[]> = {};
    for (const name of this._columnOrder) {
      resultArrays[name] = [];
    }

    for (let i = 0; i < this.length; i++) {
      const val = explodeCol.get(i);

      if (val === null) {
        // null arrays produce a row with null value
        resultArrays[column]!.push(null);
        for (const name of otherColNames) {
          resultArrays[name]!.push(this._columns.get(name)!.get(i));
        }
      } else if (Array.isArray(val)) {
        if (val.length === 0) {
          // empty arrays remove the row
          continue;
        }
        for (const item of val) {
          resultArrays[column]!.push(item === undefined ? null : item);
          for (const name of otherColNames) {
            resultArrays[name]!.push(this._columns.get(name)!.get(i));
          }
        }
      } else {
        // Non-array values are kept as-is (single row)
        resultArrays[column]!.push(val);
        for (const name of otherColNames) {
          resultArrays[name]!.push(this._columns.get(name)!.get(i));
        }
      }
    }

    return DataFrame.fromColumns<Record<string, unknown>>(resultArrays);
  }

  transpose(headerColumn?: string): DataFrame<Record<string, unknown>> {
    return transpose(this as DataFrame<Record<string, unknown>>, headerColumn);
  }

  static concat(
    ...frames: DataFrame<Record<string, unknown>>[]
  ): DataFrame<Record<string, unknown>> {
    return concat(...frames);
  }

  private _rowKey(index: number, cols: string[]): string {
    const parts: string[] = [];
    for (const name of cols) {
      const v = this._columns.get(name)!.get(index);
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

  private _takeByIndices(indices: number[]): DataFrame<S> {
    const int32Indices = new Int32Array(indices);
    return this._takeByInt32Indices(int32Indices);
  }

  private _takeByInt32Indices(int32Indices: Int32Array): DataFrame<S> {
    const newColumns = new Map<string, Column<unknown>>();
    for (const name of this._columnOrder) {
      newColumns.set(name, this._columns.get(name)!.take(int32Indices));
    }
    return new DataFrame<S>(newColumns, [...this._columnOrder]);
  }

  dropNull(columns?: string | string[]): DataFrame<S> {
    const cols =
      columns === undefined ? this._columnOrder : Array.isArray(columns) ? columns : [columns];

    for (const name of cols) {
      if (!this._columns.has(name)) {
        throw new ColumnNotFoundError(name, this._columnOrder);
      }
    }

    const indices: number[] = [];
    for (let i = 0; i < this.length; i++) {
      let hasNull = false;
      for (const name of cols) {
        if (this._columns.get(name)!.get(i) === null) {
          hasNull = true;
          break;
        }
      }
      if (!hasNull) {
        indices.push(i);
      }
    }

    return this._takeByIndices(indices);
  }

  fillNull(strategy: Record<string, unknown> | 'forward' | 'backward'): DataFrame<S> {
    if (typeof strategy === 'string') {
      return this._fillNullDirectional(strategy);
    }

    // fillNull({ col: value }) - fill specific columns with values
    for (const colName of Object.keys(strategy)) {
      if (!this._columns.has(colName)) {
        throw new ColumnNotFoundError(colName, this._columnOrder);
      }
    }

    const newColumns = new Map<string, Column<unknown>>();
    for (const name of this._columnOrder) {
      const col = this._columns.get(name)!;
      const fillValue = strategy[name];
      if (fillValue !== undefined) {
        const values: unknown[] = [];
        for (let i = 0; i < col.length; i++) {
          const v = col.get(i);
          values.push(v === null ? fillValue : v);
        }
        newColumns.set(name, buildColumn(col.dtype, values));
      } else {
        newColumns.set(name, col);
      }
    }

    return new DataFrame<S>(newColumns, [...this._columnOrder]);
  }

  private _fillNullDirectional(direction: 'forward' | 'backward'): DataFrame<S> {
    const newColumns = new Map<string, Column<unknown>>();

    for (const name of this._columnOrder) {
      const col = this._columns.get(name)!;
      if (col.nullCount === 0) {
        newColumns.set(name, col);
        continue;
      }

      const values: unknown[] = [];
      for (let i = 0; i < col.length; i++) {
        values.push(col.get(i));
      }

      if (direction === 'forward') {
        for (let i = 1; i < values.length; i++) {
          if (values[i] === null && values[i - 1] !== null) {
            values[i] = values[i - 1];
          }
        }
      } else {
        for (let i = values.length - 2; i >= 0; i--) {
          if (values[i] === null && values[i + 1] !== null) {
            values[i] = values[i + 1];
          }
        }
      }

      newColumns.set(name, buildColumn(col.dtype, values));
    }

    return new DataFrame<S>(newColumns, [...this._columnOrder]);
  }

  cast(dtypes: Partial<Record<string & keyof S, DType>>): DataFrame<S> {
    for (const colName of Object.keys(dtypes)) {
      if (!this._columns.has(colName)) {
        throw new ColumnNotFoundError(colName, this._columnOrder);
      }
    }

    const newColumns = new Map<string, Column<unknown>>();
    for (const name of this._columnOrder) {
      const targetDType = (dtypes as Record<string, DType>)[name];
      if (targetDType) {
        const series = new Series<unknown>(name, this._columns.get(name)!);
        const casted = series.cast(targetDType);
        newColumns.set(name, casted.column);
      } else {
        newColumns.set(name, this._columns.get(name)!);
      }
    }

    return new DataFrame<S>(newColumns, [...this._columnOrder]);
  }

  toString(options?: PrintOptions): string {
    const maxRows = options?.maxRows ?? 10;
    const maxCols = options?.maxCols ?? 10;
    const [nRows, nCols] = this.shape;

    if (nCols === 0 || nRows === 0) {
      return `Empty DataFrame\n0 rows x ${nCols} columns`;
    }

    // Determine which columns to show
    const allCols = this._columnOrder;
    let displayCols: string[];
    if (allCols.length > maxCols) {
      const half = Math.floor(maxCols / 2);
      displayCols = [...allCols.slice(0, half), '...', ...allCols.slice(allCols.length - half)];
    } else {
      displayCols = [...allCols];
    }

    // Determine which rows to show
    let rowIndices: number[];
    if (nRows > maxRows) {
      const half = Math.floor(maxRows / 2);
      rowIndices = [
        ...Array.from({ length: half }, (_, i) => i),
        -1, // separator
        ...Array.from({ length: half }, (_, i) => nRows - half + i),
      ];
    } else {
      rowIndices = Array.from({ length: nRows }, (_, i) => i);
    }

    // Build cell values for each display column
    const formatValue = (v: unknown): string => {
      if (v === null) return 'null';
      if (v instanceof Date) return v.toISOString();
      if (typeof v === 'number' || typeof v === 'string' || typeof v === 'boolean') {
        return String(v);
      }
      return JSON.stringify(v);
    };

    // Build table data: header + rows
    const headerRow = ['', ...displayCols]; // first cell is row index header
    const dataRows: string[][] = [];

    for (const idx of rowIndices) {
      if (idx === -1) {
        dataRows.push(headerRow.map(() => '...'));
        continue;
      }
      const cells: string[] = [String(idx)];
      for (const colName of displayCols) {
        if (colName === '...') {
          cells.push('...');
        } else {
          cells.push(formatValue(this._columns.get(colName)!.get(idx)));
        }
      }
      dataRows.push(cells);
    }

    // Calculate column widths
    const colWidths: number[] = headerRow.map((h, ci) => {
      let maxW = h.length;
      for (const row of dataRows) {
        maxW = Math.max(maxW, row[ci]!.length);
      }
      return maxW;
    });

    // Build formatted table
    const pad = (s: string, w: number, ci: number): string => {
      // Right-align the index column (first), left-align the rest
      if (ci === 0) return s.padStart(w);
      return s.padEnd(w);
    };

    const sep = '─';
    const lines: string[] = [];

    // Header
    const headerLine =
      '│ ' + headerRow.map((h, ci) => pad(h, colWidths[ci]!, ci)).join(' │ ') + ' │';
    const topBorder = '┌─' + colWidths.map((w) => sep.repeat(w)).join('─┬─') + '─┐';
    const headerSep = '├─' + colWidths.map((w) => sep.repeat(w)).join('─┼─') + '─┤';
    const bottomBorder = '└─' + colWidths.map((w) => sep.repeat(w)).join('─┴─') + '─┘';

    lines.push(topBorder);
    lines.push(headerLine);
    lines.push(headerSep);

    for (const row of dataRows) {
      lines.push('│ ' + row.map((cell, ci) => pad(cell, colWidths[ci]!, ci)).join(' │ ') + ' │');
    }

    lines.push(bottomBorder);
    lines.push(`${nRows} rows x ${nCols} columns`);

    return lines.join('\n');
  }

  print(options?: PrintOptions): void {
    console.log(this.toString(options));
  }

  describe(): DataFrame {
    const statNames = ['count', 'mean', 'std', 'min', 'max'];
    const resultColumns: Record<string, unknown[]> = { stat: statNames };

    for (const name of this._columnOrder) {
      const colObj = this._columns.get(name)!;
      const dtype = colObj.dtype;

      if (dtype === DType.Float64 || dtype === DType.Int32) {
        const series = new Series<number>(name, colObj as Column<number>);
        const count = series.length - series.nullCount;
        const mean = series.mean();
        const std = series.std();
        const min = series.min();
        const max = series.max();
        resultColumns[name] = [count, mean, std, min, max];
      }
    }

    return DataFrame.fromColumns(resultColumns);
  }

  memoryUsage(): number {
    let total = 0;
    for (const name of this._columnOrder) {
      total += this._columns.get(name)!.estimatedMemoryBytes();
    }
    return total;
  }

  info(): void {
    const [nRows, nCols] = this.shape;
    const lines: string[] = [];

    lines.push(`DataFrame: ${nRows} rows x ${nCols} columns`);
    lines.push('');

    // Column info header
    const colNameWidth = Math.max(6, ...this._columnOrder.map((n) => n.length));
    const header = `${'Column'.padEnd(colNameWidth)}  ${'DType'.padEnd(10)}  ${'Null Count'.padEnd(10)}  Memory`;
    lines.push(header);
    lines.push('─'.repeat(header.length));

    let totalMemory = 0;
    for (const name of this._columnOrder) {
      const colObj = this._columns.get(name)!;
      const dtype = colObj.dtype;
      const nullCount = colObj.nullCount;
      const mem = colObj.estimatedMemoryBytes();
      totalMemory += mem;
      lines.push(
        `${name.padEnd(colNameWidth)}  ${dtype.padEnd(10)}  ${String(nullCount).padEnd(10)}  ${formatBytes(mem)}`,
      );
    }

    lines.push('─'.repeat(header.length));
    lines.push(`Total memory: ${formatBytes(totalMemory)}`);

    console.log(lines.join('\n'));
  }

  static fromColumns<S extends Record<string, unknown> = Record<string, unknown>>(
    data: Record<string, unknown[]>,
  ): DataFrame<S> {
    const keys = Object.keys(data);
    if (keys.length === 0) {
      return DataFrame.empty<S>();
    }

    // Validate all columns have same length
    const firstKey = keys[0]!;
    const rowCount = data[firstKey]!.length;
    for (const key of keys) {
      if (data[key]!.length !== rowCount) {
        throw new ShapeMismatchError(
          `Column '${key}' has length ${data[key]!.length}, expected ${rowCount}`,
        );
      }
    }

    const columns = new Map<string, Column<unknown>>();
    for (const key of keys) {
      const values = data[key]!;
      const dtype = detectDType(values);
      const col = buildColumn(dtype, values);
      columns.set(key, col);
    }

    return new DataFrame<S>(columns, keys);
  }

  static fromRows<S extends Record<string, unknown> = Record<string, unknown>>(
    rows: object[],
  ): DataFrame<S> {
    if (rows.length === 0) {
      return DataFrame.empty<S>();
    }

    // Collect all column names from all rows (preserving order from first row)
    const columnSet = new Set<string>();
    for (const row of rows) {
      for (const key of Object.keys(row)) {
        columnSet.add(key);
      }
    }
    const columnNames = [...columnSet];

    // Build arrays per column
    const data: Record<string, unknown[]> = {};
    for (const name of columnNames) {
      data[name] = [];
    }
    for (const row of rows) {
      const r = row as Record<string, unknown>;
      for (const name of columnNames) {
        const value = name in r ? r[name] : null;
        data[name]!.push(value === undefined ? null : value);
      }
    }

    return DataFrame.fromColumns<S>(data);
  }

  static empty<S extends Record<string, unknown> = Record<string, unknown>>(): DataFrame<S> {
    return new DataFrame<S>(new Map(), []);
  }

  static range<S extends Record<string, unknown> = Record<string, unknown>>(
    name: string,
    start: number,
    end: number,
    step = 1,
  ): DataFrame<S> {
    if (step === 0) {
      throw new FrameKitError(ErrorCode.INVALID_OPERATION, 'step must not be zero');
    }
    if (start >= end) {
      throw new FrameKitError(
        ErrorCode.INVALID_OPERATION,
        `start (${start}) must be less than end (${end})`,
      );
    }
    const length = Math.ceil((end - start) / step);
    const data = new Float64Array(length);
    for (let i = 0; i < length; i++) {
      data[i] = start + i * step;
    }
    const mask = new BitArray(length, true);
    const col = new Float64Column(data, mask);
    const columns = new Map<string, Column<unknown>>();
    columns.set(name, col as unknown as Column<unknown>);
    return new DataFrame<S>(columns, [name]);
  }

  static linspace<S extends Record<string, unknown> = Record<string, unknown>>(
    name: string,
    start: number,
    end: number,
    count: number,
  ): DataFrame<S> {
    if (count < 2) {
      throw new FrameKitError(ErrorCode.INVALID_OPERATION, 'count must be at least 2');
    }
    const data = new Float64Array(count);
    const step = (end - start) / (count - 1);
    for (let i = 0; i < count; i++) {
      data[i] = start + i * step;
    }
    const mask = new BitArray(count, true);
    const col = new Float64Column(data, mask);
    const columns = new Map<string, Column<unknown>>();
    columns.set(name, col as unknown as Column<unknown>);
    return new DataFrame<S>(columns, [name]);
  }

  static async fromCSV<S extends Record<string, unknown> = Record<string, unknown>>(
    input: string | import('stream').Readable,
    options: CSVReadOptions & { parse?: 'string' } = {},
  ): Promise<DataFrame<S>> {
    let content: string;

    if (typeof input !== 'string') {
      // Node.js ReadableStream
      try {
        const chunks: Buffer[] = [];
        for await (const chunk of input as AsyncIterable<Buffer>) {
          chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk as unknown as string));
        }
        content = Buffer.concat(chunks).toString((options.encoding ?? 'utf-8') as BufferEncoding);
      } catch (err) {
        if (err instanceof IOError) throw err;
        const message = err instanceof Error ? err.message : String(err);
        throw new IOError(`Failed to read CSV from stream: ${message}`);
      }
    } else if (options.parse === 'string') {
      content = input;
    } else if (input.startsWith('http://') || input.startsWith('https://')) {
      // URL — use native fetch()
      try {
        const response = await fetch(input);
        if (!response.ok) {
          throw new IOError(
            `Failed to fetch CSV from '${input}': HTTP ${String(response.status)} ${response.statusText}`,
          );
        }
        content = await response.text();
      } catch (err) {
        if (err instanceof IOError) throw err;
        const message = err instanceof Error ? err.message : String(err);
        throw new IOError(`Failed to fetch CSV from '${input}': ${message}`);
      }
    } else {
      // Treat input as a file path
      try {
        const fs = await import('fs/promises');
        content = await fs.readFile(input, (options.encoding ?? 'utf-8') as BufferEncoding);
      } catch (err) {
        if (err instanceof IOError) throw err;
        const message = err instanceof Error ? err.message : String(err);
        throw new IOError(`Failed to read CSV file '${input}': ${message}`);
      }
    }

    const parsed = parseCSV(content, options);
    return buildDataFrameFromParsed<S>(parsed.header, parsed.columns, parsed.inferredTypes);
  }

  static async *streamCSV<S extends Record<string, unknown> = Record<string, unknown>>(
    path: string,
    options: StreamCSVOptions = {},
  ): AsyncIterable<DataFrame<S>> {
    for await (const chunk of streamCSVFile(path, options)) {
      yield buildDataFrameFromParsed<S>(chunk.header, chunk.rawColumns, chunk.inferredTypes);
    }
  }

  static scanCSV<S extends Record<string, unknown> = Record<string, unknown>>(
    path: string,
    options: StreamCSVOptions = {},
  ): LazyFrame<S> {
    // Create a LazyFrame that, on collect(), streams and concatenates all chunks
    const placeholder = DataFrame.empty<S>();
    const lazy = createLazyFrame(placeholder);
    // Override collect to use streaming
    const originalCollect = lazy.collect.bind(lazy);
    void originalCollect; // suppress unused
    lazy.collect = async (): Promise<DataFrame<S>> => {
      const chunks: DataFrame<S>[] = [];
      for await (const chunk of DataFrame.streamCSV<S>(path, options)) {
        chunks.push(chunk);
      }
      if (chunks.length === 0) return DataFrame.empty<S>();
      if (chunks.length === 1) return chunks[0]!;
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      return concat(...(chunks as DataFrame<any>[])) as DataFrame<S>;
    };
    return lazy;
  }

  static async *streamNDJSON<S extends Record<string, unknown> = Record<string, unknown>>(
    path: string,
    options: StreamNDJSONOptions = {},
  ): AsyncIterable<DataFrame<S>> {
    for await (const chunk of streamNDJSONFile(path, options)) {
      yield DataFrame.fromRows<S>(chunk);
    }
  }

  static scanNDJSON<S extends Record<string, unknown> = Record<string, unknown>>(
    path: string,
    options: StreamNDJSONOptions = {},
  ): LazyFrame<S> {
    const placeholder = DataFrame.empty<S>();
    const lazy = createLazyFrame(placeholder);
    const originalCollect = lazy.collect.bind(lazy);
    void originalCollect; // suppress unused
    lazy.collect = async (): Promise<DataFrame<S>> => {
      const chunks: DataFrame<S>[] = [];
      for await (const chunk of DataFrame.streamNDJSON<S>(path, options)) {
        chunks.push(chunk);
      }
      if (chunks.length === 0) return DataFrame.empty<S>();
      if (chunks.length === 1) return chunks[0]!;
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      return concat(...(chunks as DataFrame<any>[])) as DataFrame<S>;
    };
    return lazy;
  }

  toCSV(options?: CSVWriteOptions): string;
  toCSV(filePath: string, options?: CSVWriteOptions): Promise<void>;
  toCSV(stream: import('stream').Writable, options?: CSVWriteOptions): Promise<void>;
  toCSV(
    filePathOrOptions?: string | CSVWriteOptions | import('stream').Writable,
    maybeOptions?: CSVWriteOptions,
  ): string | Promise<void> {
    let filePath: string | undefined;
    let writable: import('stream').Writable | undefined;
    let options: CSVWriteOptions;

    if (typeof filePathOrOptions === 'string') {
      filePath = filePathOrOptions;
      options = maybeOptions ?? {};
    } else if (
      filePathOrOptions != null &&
      typeof filePathOrOptions === 'object' &&
      'write' in filePathOrOptions &&
      typeof filePathOrOptions.write === 'function'
    ) {
      // Node.js Writable stream
      writable = filePathOrOptions;
      options = maybeOptions ?? {};
    } else {
      options = (filePathOrOptions ?? {}) as CSVWriteOptions;
    }

    const { header, rows } = this._extractRows();
    const csvString = writeCSV(header, rows, options);

    if (writable) {
      const stream = writable;
      return new Promise<void>((resolve, reject) => {
        let settled = false;
        const fail = (err: Error) => {
          if (settled) return;
          settled = true;
          reject(new IOError(`Failed to write CSV to stream: ${err.message}`));
        };
        stream.once('error', fail);
        stream.write(csvString, 'utf-8', (err?: Error | null) => {
          if (err) {
            fail(err);
          } else {
            if (settled) return;
            settled = true;
            stream.removeListener('error', fail);
            resolve();
          }
        });
      });
    }

    if (filePath) {
      return import('fs/promises').then((fs) =>
        fs.writeFile(filePath, csvString, 'utf-8').catch((err: unknown) => {
          const message = err instanceof Error ? err.message : String(err);
          throw new IOError(`Failed to write CSV file '${filePath}': ${message}`);
        }),
      );
    }

    return csvString;
  }

  static async fromJSON<S extends Record<string, unknown> = Record<string, unknown>>(
    input: string,
    options: JSONReadOptions & { parse?: 'string' } = {},
  ): Promise<DataFrame<S>> {
    let content: string;

    if (options.parse === 'string') {
      content = input;
    } else {
      try {
        const fs = await import('fs/promises');
        content = await fs.readFile(input, 'utf-8');
      } catch (err) {
        if (err instanceof IOError) throw err;
        const message = err instanceof Error ? err.message : String(err);
        throw new IOError(`Failed to read JSON file '${input}': ${message}`);
      }
    }

    let parsed: unknown = JSON.parse(content);

    // Navigate to nested path if specified (e.g. 'results.items')
    if (options.path) {
      const parts = options.path.split('.');
      for (const part of parts) {
        if (parsed !== null && typeof parsed === 'object' && !Array.isArray(parsed)) {
          parsed = (parsed as Record<string, unknown>)[part];
        } else {
          throw new IOError(`JSON path '${options.path}' not found: '${part}' is not an object`);
        }
      }
    }

    if (!Array.isArray(parsed)) {
      throw new IOError('JSON content must be an array of objects');
    }

    return DataFrame.fromRows<S>(parsed as object[]);
  }

  toJSON(options?: JSONWriteOptions): string;
  toJSON(filePath: string, options?: JSONWriteOptions): Promise<void>;
  toJSON(
    filePathOrOptions?: string | JSONWriteOptions,
    maybeOptions?: JSONWriteOptions,
  ): string | Promise<void> {
    let filePath: string | undefined;
    let options: JSONWriteOptions;

    if (typeof filePathOrOptions === 'string') {
      filePath = filePathOrOptions;
      options = maybeOptions ?? {};
    } else {
      options = filePathOrOptions ?? {};
    }

    const { header, rows } = this._extractRows();
    const jsonString = writeJSON(header, rows, options);

    if (filePath) {
      return import('fs/promises').then((fs) =>
        fs.writeFile(filePath, jsonString, 'utf-8').catch((err: unknown) => {
          const message = err instanceof Error ? err.message : String(err);
          throw new IOError(`Failed to write JSON file '${filePath}': ${message}`);
        }),
      );
    }

    return jsonString;
  }

  static async fromNDJSON<S extends Record<string, unknown> = Record<string, unknown>>(
    input: string,
    options: { parse?: 'string' } = {},
  ): Promise<DataFrame<S>> {
    let content: string;

    if (options.parse === 'string') {
      content = input;
    } else {
      try {
        const fs = await import('fs/promises');
        content = await fs.readFile(input, 'utf-8');
      } catch (err) {
        if (err instanceof IOError) throw err;
        const message = err instanceof Error ? err.message : String(err);
        throw new IOError(`Failed to read NDJSON file '${input}': ${message}`);
      }
    }

    const lines = content.split('\n').filter((line) => line.trim() !== '');
    const rows: object[] = lines.map((line) => JSON.parse(line) as object);

    return DataFrame.fromRows<S>(rows);
  }

  static async fromExcel<S extends Record<string, unknown> = Record<string, unknown>>(
    filePath: string,
    options: ExcelReadOptions = {},
  ): Promise<DataFrame<S>> {
    const parsed = await readExcelFile(filePath, options);

    if (parsed.header.length === 0) {
      return DataFrame.empty<S>();
    }

    const columns = new Map<string, Column<unknown>>();
    for (const name of parsed.header) {
      const dtype = parsed.inferredTypes[name] ?? DType.Float64;
      const values = parsed.columns[name]!;
      columns.set(name, buildColumn(dtype, values));
    }

    return new DataFrame<S>(columns, [...parsed.header]);
  }

  static async fromParquet<S extends Record<string, unknown> = Record<string, unknown>>(
    filePath: string,
    options: ParquetReadOptions = {},
  ): Promise<DataFrame<S>> {
    const parsed = await readParquetFile(filePath, options);

    if (parsed.header.length === 0) {
      return DataFrame.empty<S>();
    }

    const columns = new Map<string, Column<unknown>>();
    for (const name of parsed.header) {
      const dtype = parsed.inferredTypes[name] ?? DType.Float64;
      const values = parsed.columns[name]!;
      columns.set(name, buildColumn(dtype, values));
    }

    return new DataFrame<S>(columns, [...parsed.header]);
  }

  async toExcel(filePath: string, options: ExcelWriteOptions = {}): Promise<void> {
    const { header, rows } = this._extractRows();
    await writeExcelFile(filePath, header, rows, options);
  }

  async toParquet(filePath: string, options: ParquetWriteOptions = {}): Promise<void> {
    const header = this._columnOrder;
    const columns: Record<string, { values: unknown[]; dtype: DType }> = {};
    for (const name of header) {
      const col = this._columns.get(name)!;
      const values: unknown[] = [];
      for (let i = 0; i < col.length; i++) {
        values.push(col.get(i));
      }
      columns[name] = { values, dtype: col.dtype };
    }
    await writeParquetFile(filePath, header, columns, options);
  }

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  async toArrow(): Promise<any> {
    return toArrowTable({
      columnOrder: this._columnOrder,
      getColumnValues: (name: string) => {
        const col = this._columns.get(name)!;
        const values: unknown[] = [];
        for (let i = 0; i < col.length; i++) {
          values.push(col.get(i));
        }
        return { values, dtype: col.dtype };
      },
    });
  }

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  static fromArrow<S extends Record<string, unknown> = Record<string, unknown>>(
    table: any,
  ): DataFrame<S> {
    const parsed = fromArrowTable(table);

    if (parsed.header.length === 0) {
      return DataFrame.empty<S>();
    }

    const columns = new Map<string, Column<unknown>>();
    for (const name of parsed.header) {
      const dtype = parsed.inferredTypes[name] ?? DType.Float64;
      const values = parsed.columns[name]!;
      columns.set(name, buildColumn(dtype, values));
    }

    return new DataFrame<S>(columns, [...parsed.header]);
  }

  async toArrowIPC(): Promise<Uint8Array> {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    let arrow: any;
    try {
      const moduleName = 'apache-arrow';
      // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment
      arrow = await import(moduleName);
    } catch {
      throw new IOError(
        'apache-arrow is required for Arrow IPC serialization but is not installed. Run: npm install apache-arrow',
      );
    }

    // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment
    const table = await this.toArrow();
    // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unsafe-call
    return arrow.tableToIPC(table, 'stream') as Uint8Array;
  }

  static async fromArrowIPC<S extends Record<string, unknown> = Record<string, unknown>>(
    buffer: Uint8Array,
  ): Promise<DataFrame<S>> {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    let arrow: any;
    try {
      const moduleName = 'apache-arrow';
      // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment
      arrow = await import(moduleName);
    } catch {
      throw new IOError(
        'apache-arrow is required for Arrow IPC deserialization but is not installed. Run: npm install apache-arrow',
      );
    }

    // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-unsafe-assignment
    const table = arrow.tableFromIPC(buffer);
    return DataFrame.fromArrow<S>(table);
  }

  toNDJSON(): string;
  toNDJSON(filePath: string): Promise<void>;
  toNDJSON(filePath?: string): string | Promise<void> {
    const { header, rows } = this._extractRows();
    const ndjsonString = writeNDJSON(header, rows);

    if (filePath) {
      return import('fs/promises').then((fs) =>
        fs.writeFile(filePath, ndjsonString, 'utf-8').catch((err: unknown) => {
          const message = err instanceof Error ? err.message : String(err);
          throw new IOError(`Failed to write NDJSON file '${filePath}': ${message}`);
        }),
      );
    }

    return ndjsonString;
  }

  toSQL(tableName: string, options?: SQLWriteOptions): string {
    const { header, rows } = this._extractRows();
    return writeSQL(tableName, header, rows, options);
  }

  private _extractRows(): { header: string[]; rows: unknown[][] } {
    const header = this._columnOrder;
    const rows: unknown[][] = [];
    for (let i = 0; i < this.length; i++) {
      const row: unknown[] = [];
      for (const name of this._columnOrder) {
        row.push(this._columns.get(name)!.get(i));
      }
      rows.push(row);
    }
    return { header, rows };
  }

  union(other: DataFrame<Record<string, unknown>>): DataFrame<Record<string, unknown>> {
    return union(this, other);
  }

  intersection(other: DataFrame<Record<string, unknown>>): DataFrame<Record<string, unknown>> {
    return intersection(this, other);
  }

  difference(other: DataFrame<Record<string, unknown>>): DataFrame<Record<string, unknown>> {
    return difference(this, other);
  }

  query(queryStr: string): DataFrame<Record<string, unknown>> {
    return executeQuery(this as DataFrame, queryStr);
  }

  static registerReader(extension: string, readerFn: ReaderFn): void {
    const ext = extension.startsWith('.') ? extension.slice(1) : extension;
    DataFrame._readers.set(ext.toLowerCase(), readerFn);
  }

  static registerWriter(extension: string, writerFn: WriterFn): void {
    const ext = extension.startsWith('.') ? extension.slice(1) : extension;
    DataFrame._writers.set(ext.toLowerCase(), writerFn);
  }

  static async fromFile<S extends Record<string, unknown> = Record<string, unknown>>(
    filePath: string,
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    options?: any,
  ): Promise<DataFrame<S>> {
    const ext = filePath.split('.').pop()?.toLowerCase();
    if (!ext) {
      throw new IOError(`Cannot determine file extension from path: '${filePath}'`);
    }
    const reader = DataFrame._readers.get(ext);
    if (!reader) {
      throw new IOError(
        `No reader registered for extension '.${ext}'. Use DataFrame.registerReader('${ext}', readerFn) to register one.`,
      );
    }
    const fs = await import('fs/promises');
    let source: Buffer;
    try {
      source = await fs.readFile(filePath);
    } catch (err: unknown) {
      const message = err instanceof Error ? err.message : String(err);
      throw new IOError(`Failed to read file '${filePath}': ${message}`);
    }
    // eslint-disable-next-line @typescript-eslint/no-unsafe-argument
    return reader(source, options) as Promise<DataFrame<S>>;
  }

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  async toFile(filePath: string, options?: any): Promise<void> {
    const ext = filePath.split('.').pop()?.toLowerCase();
    if (!ext) {
      throw new IOError(`Cannot determine file extension from path: '${filePath}'`);
    }
    const writer = DataFrame._writers.get(ext);
    if (!writer) {
      throw new IOError(
        `No writer registered for extension '.${ext}'. Use DataFrame.registerWriter('${ext}', writerFn) to register one.`,
      );
    }
    // eslint-disable-next-line @typescript-eslint/no-unsafe-argument
    await writer(this as DataFrame, filePath, options);
  }
}

function seededRandom(seed: number): () => number {
  // Simple mulberry32 PRNG
  let s = seed | 0;
  return () => {
    s = (s + 0x6d2b79f5) | 0;
    let t = Math.imul(s ^ (s >>> 15), 1 | s);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

function buildDataFrameFromParsed<S extends Record<string, unknown>>(
  header: string[],
  rawColumns: Record<string, (string | null)[]>,
  inferredTypes: Record<string, DType>,
): DataFrame<S> {
  if (header.length === 0) {
    return DataFrame.empty<S>();
  }

  const columns = new Map<string, Column<unknown>>();
  for (const name of header) {
    const dtype = inferredTypes[name] ?? DType.Utf8;
    const rawValues = rawColumns[name]!;
    const typedValues = convertColumnValues(rawValues, dtype);
    columns.set(name, buildColumn(dtype, typedValues));
  }

  return new DataFrame<S>(columns, [...header]);
}

function convertColumnValues(values: (string | null)[], dtype: DType): unknown[] {
  switch (dtype) {
    case DType.Float64:
    case DType.Int32:
      return values.map((v) => (v === null ? null : Number(v)));
    case DType.Boolean:
      return values.map((v) => {
        if (v === null) return null;
        return v.toLowerCase() === 'true';
      });
    case DType.Date:
      return values.map((v) => (v === null ? null : new Date(v)));
    case DType.Utf8:
    default:
      return values;
  }
}

function detectDType(values: unknown[]): DType {
  for (const v of values) {
    if (v === null || v === undefined) continue;
    if (typeof v === 'number') return DType.Float64;
    if (typeof v === 'string') return DType.Utf8;
    if (typeof v === 'boolean') return DType.Boolean;
    if (v instanceof Date) return DType.Date;
    if (Array.isArray(v) || typeof v === 'object') return DType.Object;
  }
  // Default to Float64 if all nulls
  return DType.Float64;
}

function buildColumn(dtype: DType, values: unknown[]): Column<unknown> {
  switch (dtype) {
    case DType.Float64:
      return Float64Column.from(values as (number | null)[]);
    case DType.Int32:
      return Int32Column.from(values as (number | null)[]);
    case DType.Utf8:
      return Utf8Column.from(values as (string | null)[]);
    case DType.Boolean:
      return BooleanColumn.from(values as (boolean | null)[]);
    case DType.Date:
      return DateColumn.from(values as (Date | null)[]);
    case DType.Object:
      return ObjectColumn.from(values);
    default:
      throw new FrameKitError(
        ErrorCode.INVALID_OPERATION,
        `Unsupported dtype for column construction: ${dtype}`,
      );
  }
}

function formatBytes(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  if (bytes < 1024 * 1024 * 1024) return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
  return `${(bytes / (1024 * 1024 * 1024)).toFixed(1)} GB`;
}

// Register DataFrame factory for Series.valueCounts() to avoid circular dependency
_registerDataFrameFactory((columns, columnOrder) => new DataFrame(columns, columnOrder));
