import { DType } from './types/dtype';
import { ErrorCode, FrameKitError, TypeMismatchError } from './errors';
import { Column } from './storage/column';
import { Float64Column, Int32Column } from './storage/numeric';
import { Utf8Column } from './storage/string';
import { BooleanColumn } from './storage/boolean';
import { DateColumn } from './storage/date';
import { ObjectColumn } from './storage/object';
import { StringAccessor } from './accessors/string-accessor';
import { DateAccessor } from './accessors/date-accessor';
import type { DataFrame } from './dataframe';

// Factory registration to avoid circular dependency
type DataFrameFactory = (columns: Map<string, Column<unknown>>, columnOrder: string[]) => DataFrame;
let _createDataFrame: DataFrameFactory | undefined;

/** @internal */
export function _registerDataFrameFactory(factory: DataFrameFactory): void {
  _createDataFrame = factory;
}

export class Series<T> {
  private readonly _name: string;
  private readonly _column: Column<T>;

  constructor(name: string, column: Column<T>) {
    this._name = name;
    this._column = column;
  }

  get name(): string {
    return this._name;
  }

  get dtype(): DType {
    return this._column.dtype;
  }

  get length(): number {
    return this._column.length;
  }

  get nullCount(): number {
    return this._column.nullCount;
  }

  get column(): Column<T> {
    return this._column;
  }

  get str(): StringAccessor {
    return new StringAccessor(this as unknown as Series<string>);
  }

  get dt(): DateAccessor {
    return new DateAccessor(this as unknown as Series<Date>);
  }

  get(index: number): T | null {
    return this._column.get(index);
  }

  toArray(): (T | null)[] {
    const result: (T | null)[] = [];
    for (let i = 0; i < this._column.length; i++) {
      result.push(this._column.get(i));
    }
    return result;
  }

  // Numeric methods — only valid when T extends number
  sum(): number {
    const col = this._asNumericColumn();
    return col.sum();
  }

  mean(): number | null {
    const col = this._asNumericColumn();
    return col.mean();
  }

  min(): number | null {
    const col = this._asNumericColumn();
    return col.min();
  }

  max(): number | null {
    const col = this._asNumericColumn();
    return col.max();
  }

  std(): number | null {
    const col = this._asNumericColumn();
    const m = col.mean();
    if (m === null) return null;
    const validCount = col.length - col.nullCount;
    if (validCount < 2) return null;
    let sumSqDiff = 0;
    for (let i = 0; i < col.length; i++) {
      const val = col.get(i);
      if (val !== null) {
        const diff = val - m;
        sumSqDiff += diff * diff;
      }
    }
    return Math.sqrt(sumSqDiff / (validCount - 1));
  }

  median(): number | null {
    const col = this._asNumericColumn();
    const values: number[] = [];
    for (let i = 0; i < col.length; i++) {
      const val = col.get(i);
      if (val !== null) {
        values.push(val);
      }
    }
    if (values.length === 0) return null;
    values.sort((a, b) => a - b);
    const mid = Math.floor(values.length / 2);
    if (values.length % 2 === 0) {
      return (values[mid - 1]! + values[mid]!) / 2;
    }
    return values[mid]!;
  }

  between(low: number, high: number): Series<boolean> {
    const col = this._asNumericColumn();
    const results: (boolean | null)[] = [];
    for (let i = 0; i < col.length; i++) {
      const val = col.get(i);
      if (val === null) {
        results.push(null);
      } else {
        results.push(val >= low && val <= high);
      }
    }
    return new Series<boolean>(this._name, BooleanColumn.from(results));
  }

  cumSum(): Series<number> {
    const col = this._asNumericColumn();
    const results: (number | null)[] = [];
    let running = 0;
    for (let i = 0; i < col.length; i++) {
      const val = col.get(i);
      if (val === null) {
        results.push(null);
      } else {
        running += val;
        results.push(running);
      }
    }
    return new Series<number>(this._name, Float64Column.from(results));
  }

  abs(): Series<number> {
    const col = this._asNumericColumn();
    const results: (number | null)[] = [];
    for (let i = 0; i < col.length; i++) {
      const val = col.get(i);
      if (val === null) {
        results.push(null);
      } else {
        results.push(Math.abs(val));
      }
    }
    return new Series<number>(this._name, Float64Column.from(results));
  }

  round(decimals = 0): Series<number> {
    const col = this._asNumericColumn();
    const factor = Math.pow(10, decimals);
    const results: (number | null)[] = [];
    for (let i = 0; i < col.length; i++) {
      const val = col.get(i);
      if (val === null) {
        results.push(null);
      } else {
        results.push(Math.round(val * factor) / factor);
      }
    }
    return new Series<number>(this._name, Float64Column.from(results));
  }

  // Comparison methods
  eq(value: T): Series<boolean> {
    return this._compareScalar(value, (a, b) => a === b);
  }

  neq(value: T): Series<boolean> {
    return this._compareScalar(value, (a, b) => a !== b);
  }

  gt(value: T): Series<boolean> {
    return this._compareScalar(value, (a, b) => (a as number) > (b as number));
  }

  gte(value: T): Series<boolean> {
    return this._compareScalar(value, (a, b) => (a as number) >= (b as number));
  }

  lt(value: T): Series<boolean> {
    return this._compareScalar(value, (a, b) => (a as number) < (b as number));
  }

  lte(value: T): Series<boolean> {
    return this._compareScalar(value, (a, b) => (a as number) <= (b as number));
  }

  isIn(values: T[]): Series<boolean> {
    const set = new Set(values.map((v) => this._toComparable(v)));
    const results: (boolean | null)[] = [];
    for (let i = 0; i < this._column.length; i++) {
      const val = this._column.get(i);
      if (val === null) {
        results.push(null);
      } else {
        results.push(set.has(this._toComparable(val)));
      }
    }
    return new Series<boolean>(this._name, BooleanColumn.from(results));
  }

  isNull(): Series<boolean> {
    const results: (boolean | null)[] = [];
    for (let i = 0; i < this._column.length; i++) {
      results.push(this._column.get(i) === null);
    }
    return new Series<boolean>(this._name, BooleanColumn.from(results));
  }

  isNotNull(): Series<boolean> {
    const results: (boolean | null)[] = [];
    for (let i = 0; i < this._column.length; i++) {
      results.push(this._column.get(i) !== null);
    }
    return new Series<boolean>(this._name, BooleanColumn.from(results));
  }

  fillNull(value: T): Series<T> {
    const values = this.toArray();
    const filled = values.map((v) => (v === null ? value : v));
    return new Series<T>(this._name, this._buildColumn(filled) as Column<T>);
  }

  unique(): Series<T> {
    const seen = new Set<string>();
    const values: (T | null)[] = [];
    for (let i = 0; i < this._column.length; i++) {
      const val = this._column.get(i);
      const key = val === null ? '__null__' : String(this._toComparable(val));
      if (!seen.has(key)) {
        seen.add(key);
        values.push(val);
      }
    }
    return new Series<T>(this._name, this._buildColumn(values) as Column<T>);
  }

  nUnique(): number {
    const seen = new Set<string>();
    for (let i = 0; i < this._column.length; i++) {
      const val = this._column.get(i);
      const key = val === null ? '__null__' : String(this._toComparable(val));
      seen.add(key);
    }
    return seen.size;
  }

  valueCounts(): DataFrame<{ value: T; count: number }> {
    if (!_createDataFrame) {
      throw new FrameKitError(ErrorCode.INVALID_OPERATION, 'DataFrame factory not registered');
    }

    const counts = new Map<string, { value: T | null; count: number }>();
    const order: string[] = [];

    for (let i = 0; i < this._column.length; i++) {
      const val = this._column.get(i);
      const key = val === null ? '__null__' : String(this._toComparable(val));
      const entry = counts.get(key);
      if (entry) {
        entry.count++;
      } else {
        counts.set(key, { value: val, count: 1 });
        order.push(key);
      }
    }

    // Sort by count descending
    order.sort((a, b) => counts.get(b)!.count - counts.get(a)!.count);

    const values: (T | null)[] = [];
    const countValues: number[] = [];
    for (const key of order) {
      const entry = counts.get(key)!;
      values.push(entry.value);
      countValues.push(entry.count);
    }

    const valueCol = this._buildColumn(values);
    const countCol = Float64Column.from(countValues);

    const columns = new Map<string, Column<unknown>>();
    columns.set('value', valueCol);
    columns.set('count', countCol);

    return _createDataFrame(columns, ['value', 'count']) as DataFrame<{ value: T; count: number }>;
  }

  cast<U>(dtype: DType): Series<U> {
    const values = this.toArray();
    const converted = values.map((v) => {
      if (v === null) return null;
      return this._castValue(v, dtype);
    });
    const col = buildColumnFromDType(dtype, converted);
    return new Series<U>(this._name, col as Column<U>);
  }

  apply<U>(fn: (value: T | null) => U | null): Series<U> {
    const results: (U | null)[] = [];
    for (let i = 0; i < this._column.length; i++) {
      results.push(fn(this._column.get(i)));
    }
    // Detect dtype from first non-null result
    let resultDType = DType.Float64;
    for (const r of results) {
      if (r !== null) {
        if (typeof r === 'string') resultDType = DType.Utf8;
        else if (typeof r === 'boolean') resultDType = DType.Boolean;
        else if (r instanceof Date) resultDType = DType.Date;
        else resultDType = DType.Float64;
        break;
      }
    }
    const col = buildColumnFromDType(resultDType, results);
    return new Series<U>(this._name, col as Column<U>);
  }

  private _asNumericColumn(): Float64Column | Int32Column {
    if (this._column instanceof Float64Column) return this._column;
    if (this._column instanceof Int32Column) return this._column;
    throw new TypeMismatchError(
      `Cannot perform numeric operation on Series with dtype '${this.dtype}'`,
    );
  }

  private _compareScalar(
    value: T,
    predicate: (a: T, b: T) => boolean,
  ): Series<boolean> {
    const results: (boolean | null)[] = [];
    for (let i = 0; i < this._column.length; i++) {
      const val = this._column.get(i);
      if (val === null) {
        results.push(null);
      } else {
        results.push(predicate(val, value));
      }
    }
    return new Series<boolean>(this._name, BooleanColumn.from(results));
  }

  private _toComparable(value: T): string | number | boolean {
    if (value instanceof Date) return value.getTime();
    return value as unknown as string | number | boolean;
  }

  private _buildColumn(values: (T | null)[]): Column<unknown> {
    switch (this._column.dtype) {
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
          `Unsupported dtype: ${this._column.dtype}`,
        );
    }
  }

  private _castValue(value: T, targetDType: DType): unknown {
    switch (targetDType) {
      case DType.Float64:
      case DType.Int32:
        if (typeof value === 'number') return value;
        if (typeof value === 'string') return Number(value);
        if (typeof value === 'boolean') return value ? 1 : 0;
        if (value instanceof Date) return value.getTime();
        return Number(value);
      case DType.Utf8:
        if (value instanceof Date) return value.toISOString();
        return String(value);
      case DType.Boolean:
        return Boolean(value);
      case DType.Date:
        if (value instanceof Date) return value;
        if (typeof value === 'number') return new Date(value);
        if (typeof value === 'string') return new Date(value);
        return new Date(String(value));
      default:
        throw new TypeMismatchError(
          `Cannot cast to dtype '${targetDType}'`,
        );
    }
  }
}

function buildColumnFromDType(
  dtype: DType,
  values: (unknown)[],
): Column<unknown> {
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
