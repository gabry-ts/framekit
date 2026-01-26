import type { DataFrame } from '../dataframe';
import { DType } from '../types/dtype';
import { Column } from '../storage/column';
import { Float64Column, Int32Column } from '../storage/numeric';
import { Utf8Column } from '../storage/string';
import { BooleanColumn } from '../storage/boolean';
import { DateColumn } from '../storage/date';
import { ShapeMismatchError } from '../errors';

type DataFrameConstructor = new <S extends Record<string, unknown>>(
  columns: Map<string, Column<unknown>>,
  columnOrder: string[],
) => DataFrame<S>;

function detectDType(values: unknown[]): DType {
  for (const v of values) {
    if (v === null || v === undefined) continue;
    if (typeof v === 'number') return DType.Float64;
    if (typeof v === 'string') return DType.Utf8;
    if (typeof v === 'boolean') return DType.Boolean;
    if (v instanceof Date) return DType.Date;
  }
  return DType.Float64;
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

function serializeValue(v: unknown): string {
  if (v === null || v === undefined) return 'null';
  if (v instanceof Date) return v.toISOString();
  if (typeof v === 'number' || typeof v === 'string' || typeof v === 'boolean') {
    return String(v);
  }
  return JSON.stringify(v);
}

export function transpose(
  df: DataFrame<Record<string, unknown>>,
  headerColumn?: string,
): DataFrame<Record<string, unknown>> {
  const Ctor = df.constructor as DataFrameConstructor;
  const nRows = df.length;
  const cols = df.columns;

  if (cols.length === 0 || nRows === 0) {
    return new Ctor<Record<string, unknown>>(new Map(), []);
  }

  // Determine new column headers from a column or row indices
  let newHeaders: string[];
  let sourceColumns: string[];

  if (headerColumn) {
    // Use specified column values as new headers
    const headerSeries = df.col(headerColumn);
    newHeaders = [];
    for (let i = 0; i < nRows; i++) {
      newHeaders.push(serializeValue(headerSeries.get(i)));
    }
    sourceColumns = cols.filter((c) => c !== headerColumn);
  } else {
    // Use row indices as new column headers
    newHeaders = [];
    for (let i = 0; i < nRows; i++) {
      newHeaders.push(String(i));
    }
    sourceColumns = cols;
  }

  // First column is the original column names
  const resultColumns = new Map<string, Column<unknown>>();
  const columnOrder: string[] = ['column'];

  resultColumns.set('column', Utf8Column.from(sourceColumns));

  // Each row becomes a column
  for (let rowIdx = 0; rowIdx < nRows; rowIdx++) {
    const header = newHeaders[rowIdx]!;
    const values: unknown[] = [];
    for (const colName of sourceColumns) {
      values.push(df.col(colName).get(rowIdx));
    }
    const dtype = detectDType(values);
    resultColumns.set(header, buildColumnFromValues(dtype, values));
    columnOrder.push(header);
  }

  return new Ctor<Record<string, unknown>>(resultColumns, columnOrder);
}

export function concat(
  ...frames: DataFrame<Record<string, unknown>>[]
): DataFrame<Record<string, unknown>> {
  if (frames.length === 0) {
    throw new ShapeMismatchError('concat requires at least one DataFrame');
  }

  const first = frames[0]!;
  const Ctor = first.constructor as DataFrameConstructor;

  if (frames.length === 1) {
    return first;
  }

  // Gather all unique column names preserving order of first appearance
  const allColumns: string[] = [];
  const columnSet = new Set<string>();
  for (const frame of frames) {
    for (const col of frame.columns) {
      if (!columnSet.has(col)) {
        columnSet.add(col);
        allColumns.push(col);
      }
    }
  }

  // Validate compatible types for overlapping columns
  const dtypeMap = new Map<string, DType>();
  for (const frame of frames) {
    const dtypes = frame.dtypes;
    for (const col of frame.columns) {
      const existing = dtypeMap.get(col);
      const current = dtypes[col]!;
      if (existing !== undefined && existing !== current) {
        throw new ShapeMismatchError(
          `Column '${col}' has incompatible types: ${existing} vs ${current}`,
        );
      }
      dtypeMap.set(col, current);
    }
  }

  // Build concatenated columns
  const totalRows = frames.reduce((sum, f) => sum + f.length, 0);
  const resultColumns = new Map<string, Column<unknown>>();

  for (const colName of allColumns) {
    const values: unknown[] = new Array(totalRows);
    let offset = 0;
    for (const frame of frames) {
      const frameLen = frame.length;
      if (frame.columns.includes(colName)) {
        const series = frame.col(colName);
        for (let i = 0; i < frameLen; i++) {
          values[offset + i] = series.get(i);
        }
      } else {
        // Fill with null for frames missing this column
        for (let i = 0; i < frameLen; i++) {
          values[offset + i] = null;
        }
      }
      offset += frameLen;
    }

    const dtype = dtypeMap.get(colName) ?? detectDType(values);
    resultColumns.set(colName, buildColumnFromValues(dtype, values));
  }

  return new Ctor<Record<string, unknown>>(resultColumns, allColumns);
}
