import type { DataFrame } from '../dataframe';
import { DType } from '../types/dtype';
import { Column } from '../storage/column';
import { Float64Column, Int32Column } from '../storage/numeric';
import { Utf8Column } from '../storage/string';
import { BooleanColumn } from '../storage/boolean';
import { DateColumn } from '../storage/date';
import { ColumnNotFoundError } from '../errors';

type DataFrameConstructor = new <S extends Record<string, unknown>>(
  columns: Map<string, Column<unknown>>,
  columnOrder: string[],
) => DataFrame<S>;

export type PivotAggFunc = 'sum' | 'mean' | 'count' | 'first' | 'last';

export interface PivotOptions {
  index: string | string[];
  columns: string;
  values: string;
  aggFunc?: PivotAggFunc;
}

function aggregate(values: unknown[], aggFunc: PivotAggFunc): unknown {
  switch (aggFunc) {
    case 'first':
      return values.length > 0 ? values[0]! : null;
    case 'last':
      return values.length > 0 ? values[values.length - 1]! : null;
    case 'count':
      return values.length;
    case 'sum': {
      let total = 0;
      for (const v of values) {
        if (typeof v === 'number') total += v;
      }
      return total;
    }
    case 'mean': {
      let total = 0;
      let count = 0;
      for (const v of values) {
        if (typeof v === 'number') {
          total += v;
          count++;
        }
      }
      return count > 0 ? total / count : null;
    }
  }
}

function serializeKey(df: DataFrame<Record<string, unknown>>, index: number, indexCols: string[]): string {
  const parts: string[] = [];
  for (const name of indexCols) {
    const v = df.col(name).get(index);
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

function buildColumnFromValues(dtype: DType, values: unknown[]): Column<unknown> {
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
      return Float64Column.from(values as (number | null)[]);
  }
}

export function pivot(
  df: DataFrame<Record<string, unknown>>,
  options: PivotOptions,
): DataFrame<Record<string, unknown>> {
  const indexCols = Array.isArray(options.index) ? options.index : [options.index];
  const columnsCol = options.columns;
  const valuesCol = options.values;
  const aggFunc = options.aggFunc ?? 'first';

  // Validate columns exist
  for (const col of [...indexCols, columnsCol, valuesCol]) {
    if (!df.columns.includes(col)) {
      throw new ColumnNotFoundError(col, df.columns);
    }
  }

  // Collect unique index keys (preserving order)
  const indexKeyOrder: string[] = [];
  const indexKeyToRowIndices = new Map<string, number[]>();

  for (let i = 0; i < df.length; i++) {
    const key = serializeKey(df, i, indexCols);
    if (!indexKeyToRowIndices.has(key)) {
      indexKeyOrder.push(key);
      indexKeyToRowIndices.set(key, []);
    }
    indexKeyToRowIndices.get(key)!.push(i);
  }

  // Collect unique pivot column values (preserving order)
  const pivotColValues: string[] = [];
  const pivotColSet = new Set<string>();
  const pivotColumnSeries = df.col(columnsCol);

  for (let i = 0; i < df.length; i++) {
    const v = pivotColumnSeries.get(i);
    let str: string;
    if (v === null) {
      str = 'null';
    } else if (typeof v === 'string' || typeof v === 'number' || typeof v === 'boolean') {
      str = String(v);
    } else if (v instanceof Date) {
      str = v.toISOString();
    } else {
      str = JSON.stringify(v);
    }
    if (!pivotColSet.has(str)) {
      pivotColSet.add(str);
      pivotColValues.push(str);
    }
  }

  // Build data: for each (indexKey, pivotValue) pair, collect values
  const cellData = new Map<string, unknown[]>(); // key: indexKey + '\x02' + pivotValue
  for (let i = 0; i < df.length; i++) {
    const indexKey = serializeKey(df, i, indexCols);
    const pivotVal = pivotColumnSeries.get(i);
    let pivotStr: string;
    if (pivotVal === null) {
      pivotStr = 'null';
    } else if (typeof pivotVal === 'string' || typeof pivotVal === 'number' || typeof pivotVal === 'boolean') {
      pivotStr = String(pivotVal);
    } else if (pivotVal instanceof Date) {
      pivotStr = pivotVal.toISOString();
    } else {
      pivotStr = JSON.stringify(pivotVal);
    }
    const cellKey = indexKey + '\x02' + pivotStr;

    if (!cellData.has(cellKey)) {
      cellData.set(cellKey, []);
    }
    cellData.get(cellKey)!.push(df.col(valuesCol).get(i));
  }

  // Determine value column dtype for result
  const valuesDtype = df.col(valuesCol).column.dtype;
  const resultDtype = aggFunc === 'count' ? DType.Float64 : valuesDtype;

  // Build result columns
  const resultColumns = new Map<string, Column<unknown>>();
  const columnOrder: string[] = [];
  // Index columns
  for (const idxCol of indexCols) {
    const vals: unknown[] = [];
    const sourceCol = df.col(idxCol);
    for (const key of indexKeyOrder) {
      const firstRow = indexKeyToRowIndices.get(key)![0]!;
      vals.push(sourceCol.get(firstRow));
    }
    resultColumns.set(idxCol, buildColumnFromValues(sourceCol.column.dtype, vals));
    columnOrder.push(idxCol);
  }

  // Pivot value columns
  for (const pivotVal of pivotColValues) {
    const vals: unknown[] = [];
    for (const indexKey of indexKeyOrder) {
      const cellKey = indexKey + '\x02' + pivotVal;
      const cellValues = cellData.get(cellKey);
      if (cellValues && cellValues.length > 0) {
        vals.push(aggregate(cellValues, aggFunc));
      } else {
        vals.push(null);
      }
    }
    resultColumns.set(pivotVal, buildColumnFromValues(resultDtype, vals));
    columnOrder.push(pivotVal);
  }

  const Ctor = df.constructor as DataFrameConstructor;
  return new Ctor<Record<string, unknown>>(resultColumns, columnOrder);
}
