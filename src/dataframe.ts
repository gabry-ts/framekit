import { DType } from './types/dtype';
import { ColumnNotFoundError, ErrorCode, FrameKitError, ShapeMismatchError } from './errors';
import { Column } from './storage/column';
import { Float64Column, Int32Column } from './storage/numeric';
import { Utf8Column } from './storage/string';
import { BooleanColumn } from './storage/boolean';
import { DateColumn } from './storage/date';
import { Series } from './series';

export class DataFrame<S extends Record<string, unknown> = Record<string, unknown>> {
  private readonly _columns: Map<string, Column<unknown>>;
  private readonly _columnOrder: string[];

  constructor(columns: Map<string, Column<unknown>>, columnOrder: string[]) {
    this._columns = columns;
    this._columnOrder = columnOrder;
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
}

function detectDType(values: unknown[]): DType {
  for (const v of values) {
    if (v === null || v === undefined) continue;
    if (typeof v === 'number') return DType.Float64;
    if (typeof v === 'string') return DType.Utf8;
    if (typeof v === 'boolean') return DType.Boolean;
    if (v instanceof Date) return DType.Date;
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
    default:
      throw new FrameKitError(
        ErrorCode.INVALID_OPERATION,
        `Unsupported dtype for column construction: ${dtype}`,
      );
  }
}
