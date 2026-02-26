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

function serializeKey(columns: Column<unknown>[], index: number): string | null {
  const parts: string[] = [];
  for (const column of columns) {
    const v = column.get(index);
    if (v === null) {
      return null; // null keys never match
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

function normalizeSingleKey(value: unknown): unknown | null {
  if (value === null) return null;
  if (value instanceof Date) return `\0d${value.getTime()}`;
  if (typeof value === 'number' || typeof value === 'string' || typeof value === 'boolean') {
    return value;
  }
  return `\0obj${JSON.stringify(value)}`;
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

export type JoinType = 'inner' | 'left' | 'right' | 'outer' | 'cross' | 'semi' | 'anti';

export interface JoinOnMapping {
  left: string | string[];
  right: string | string[];
}

export interface JoinOptions {
  suffix?: string;
}

function resolveKeys(on: string | string[] | JoinOnMapping): {
  leftKeys: string[];
  rightKeys: string[];
} {
  if (typeof on === 'string') {
    return { leftKeys: [on], rightKeys: [on] };
  }
  if (Array.isArray(on)) {
    return { leftKeys: on, rightKeys: on };
  }
  const leftKeys = Array.isArray(on.left) ? on.left : [on.left];
  const rightKeys = Array.isArray(on.right) ? on.right : [on.right];
  if (leftKeys.length !== rightKeys.length) {
    throw new Error('Left and right key arrays must have the same length');
  }
  return { leftKeys, rightKeys };
}

function crossJoin<L extends Record<string, unknown>, R extends Record<string, unknown>>(
  left: DataFrame<L>,
  right: DataFrame<R>,
  suffix: string,
): DataFrame<Record<string, unknown>> {
  const resultLength = left.length * right.length;
  const resultColumns = new Map<string, Column<unknown>>();
  const columnOrder: string[] = [];

  // Determine right column renames for duplicates
  const rightColRenames = new Map<string, string>();
  for (const rc of right.columns) {
    if (left.columns.includes(rc)) {
      rightColRenames.set(rc, `${rc}${suffix}`);
    } else {
      rightColRenames.set(rc, rc);
    }
  }

  // Left columns: each left row repeated right.length times
  for (const colName of left.columns) {
    const srcCol = left.col(colName).column;
    const values: unknown[] = new Array(resultLength);
    for (let li = 0; li < left.length; li++) {
      const v = srcCol.get(li);
      for (let ri = 0; ri < right.length; ri++) {
        values[li * right.length + ri] = v;
      }
    }
    resultColumns.set(colName, buildColumnFromValues(srcCol.dtype, values));
    columnOrder.push(colName);
  }

  // Right columns: cycle through right rows for each left row
  for (const colName of right.columns) {
    const srcCol = right.col(colName).column;
    const outputName = rightColRenames.get(colName)!;
    const values: unknown[] = new Array(resultLength);
    for (let li = 0; li < left.length; li++) {
      for (let ri = 0; ri < right.length; ri++) {
        values[li * right.length + ri] = srcCol.get(ri);
      }
    }
    resultColumns.set(outputName, buildColumnFromValues(srcCol.dtype, values));
    columnOrder.push(outputName);
  }

  const Ctor = left.constructor as DataFrameConstructor;
  return new Ctor<Record<string, unknown>>(resultColumns, columnOrder);
}

function semiAntiJoin<L extends Record<string, unknown>, R extends Record<string, unknown>>(
  left: DataFrame<L>,
  right: DataFrame<R>,
  on: string | string[] | JoinOnMapping,
  anti: boolean,
): DataFrame<Record<string, unknown>> {
  const { leftKeys, rightKeys } = resolveKeys(on);

  for (const key of leftKeys) {
    if (!left.columns.includes(key)) {
      throw new ColumnNotFoundError(key, left.columns);
    }
  }
  for (const key of rightKeys) {
    if (!right.columns.includes(key)) {
      throw new ColumnNotFoundError(key, right.columns);
    }
  }

  // Build hash set from right side keys
  const rightKeyCols = rightKeys.map((k) => right.col(k).column);
  const leftKeyCols = leftKeys.map((k) => left.col(k).column);

  const rightKeySet = new Set<unknown>();
  if (rightKeyCols.length === 1) {
    const rightKeyCol = rightKeyCols[0]!;
    for (let i = 0; i < right.length; i++) {
      const key = normalizeSingleKey(rightKeyCol.get(i));
      if (key !== null) rightKeySet.add(key);
    }
  } else {
    for (let i = 0; i < right.length; i++) {
      const keyStr = serializeKey(rightKeyCols, i);
      if (keyStr !== null) rightKeySet.add(keyStr);
    }
  }

  // Filter left rows by existence (semi) or non-existence (anti) in right key set
  const matchedIndices: number[] = [];
  if (leftKeyCols.length === 1) {
    const leftKeyCol = leftKeyCols[0]!;
    for (let i = 0; i < left.length; i++) {
      const key = normalizeSingleKey(leftKeyCol.get(i));
      const hasMatch = key !== null && rightKeySet.has(key);
      if (anti ? !hasMatch : hasMatch) {
        matchedIndices.push(i);
      }
    }
  } else {
    for (let i = 0; i < left.length; i++) {
      const keyStr = serializeKey(leftKeyCols, i);
      const hasMatch = keyStr !== null && rightKeySet.has(keyStr);
      if (anti ? !hasMatch : hasMatch) {
        matchedIndices.push(i);
      }
    }
  }

  // Build result from left columns only (no right columns added)
  const resultColumns = new Map<string, Column<unknown>>();
  const columnOrder: string[] = [];

  for (const colName of left.columns) {
    const srcCol = left.col(colName).column;
    const values: unknown[] = new Array(matchedIndices.length);
    for (let i = 0; i < matchedIndices.length; i++) {
      values[i] = srcCol.get(matchedIndices[i]!);
    }
    resultColumns.set(colName, buildColumnFromValues(srcCol.dtype, values));
    columnOrder.push(colName);
  }

  const Ctor = left.constructor as DataFrameConstructor;
  return new Ctor<Record<string, unknown>>(resultColumns, columnOrder);
}

export function hashJoin<L extends Record<string, unknown>, R extends Record<string, unknown>>(
  left: DataFrame<L>,
  right: DataFrame<R>,
  on: string | string[] | JoinOnMapping,
  how: JoinType = 'inner',
  options?: JoinOptions,
): DataFrame<Record<string, unknown>> {
  const suffix = options?.suffix ?? '_right';

  // Dispatch to specialized implementations
  if (how === 'cross') {
    return crossJoin(left, right, suffix);
  }
  if (how === 'semi') {
    return semiAntiJoin(left, right, on, false);
  }
  if (how === 'anti') {
    return semiAntiJoin(left, right, on, true);
  }

  const { leftKeys, rightKeys } = resolveKeys(on);

  // Validate keys exist in both DataFrames
  for (const key of leftKeys) {
    if (!left.columns.includes(key)) {
      throw new ColumnNotFoundError(key, left.columns);
    }
  }
  for (const key of rightKeys) {
    if (!right.columns.includes(key)) {
      throw new ColumnNotFoundError(key, right.columns);
    }
  }

  // Build hash table on right side
  const rightKeyCols = rightKeys.map((k) => right.col(k).column);
  const hashTable = new Map<unknown, number | number[]>();
  if (rightKeyCols.length === 1) {
    const rightKeyCol = rightKeyCols[0]!;
    for (let i = 0; i < right.length; i++) {
      const key = normalizeSingleKey(rightKeyCol.get(i));
      if (key === null) continue;
      const bucket = hashTable.get(key);
      if (bucket === undefined) {
        hashTable.set(key, i);
      } else if (typeof bucket === 'number') {
        hashTable.set(key, [bucket, i]);
      } else {
        bucket.push(i);
      }
    }
  } else {
    for (let i = 0; i < right.length; i++) {
      const keyStr = serializeKey(rightKeyCols, i);
      if (keyStr === null) continue;
      const bucket = hashTable.get(keyStr);
      if (bucket === undefined) {
        hashTable.set(keyStr, [i]);
      } else if (typeof bucket === 'number') {
        hashTable.set(keyStr, [bucket, i]);
      } else {
        bucket.push(i);
      }
    }
  }

  // Probe phase
  const leftKeyCols = leftKeys.map((k) => left.col(k).column);
  const leftIndices: number[] = [];
  const rightIndices: (number | null)[] = [];
  const rightMatched = new Uint8Array(right.length);

  if (leftKeyCols.length === 1) {
    const leftKeyCol = leftKeyCols[0]!;
    for (let i = 0; i < left.length; i++) {
      const key = normalizeSingleKey(leftKeyCol.get(i));
      if (key === null) {
        if (how === 'left' || how === 'outer') {
          leftIndices.push(i);
          rightIndices.push(null);
        }
        continue;
      }

      const matches = hashTable.get(key);
      if (matches !== undefined) {
        if (typeof matches === 'number') {
          leftIndices.push(i);
          rightIndices.push(matches);
          if (how === 'right' || how === 'outer') {
            rightMatched[matches] = 1;
          }
        } else {
          for (let mi = 0; mi < matches.length; mi++) {
            const ri = matches[mi]!;
            leftIndices.push(i);
            rightIndices.push(ri);
            if (how === 'right' || how === 'outer') {
              rightMatched[ri] = 1;
            }
          }
        }
      } else if (how === 'left' || how === 'outer') {
        leftIndices.push(i);
        rightIndices.push(null);
      }
    }
  } else {
    for (let i = 0; i < left.length; i++) {
      const keyStr = serializeKey(leftKeyCols, i);
      if (keyStr === null) {
        if (how === 'left' || how === 'outer') {
          leftIndices.push(i);
          rightIndices.push(null);
        }
        continue;
      }

      const matches = hashTable.get(keyStr);
      if (matches !== undefined) {
        if (typeof matches === 'number') {
          leftIndices.push(i);
          rightIndices.push(matches);
          if (how === 'right' || how === 'outer') {
            rightMatched[matches] = 1;
          }
        } else {
          for (let mi = 0; mi < matches.length; mi++) {
            const ri = matches[mi]!;
            leftIndices.push(i);
            rightIndices.push(ri);
            if (how === 'right' || how === 'outer') {
              rightMatched[ri] = 1;
            }
          }
        }
      } else if (how === 'left' || how === 'outer') {
        leftIndices.push(i);
        rightIndices.push(null);
      }
    }
  }

  // For right/outer: add unmatched right rows
  if (how === 'right' || how === 'outer') {
    for (let i = 0; i < right.length; i++) {
      if (rightMatched[i] === 0) {
        leftIndices.push(-1);
        rightIndices.push(i);
      }
    }
  }

  // Build result columns
  const resultColumns = new Map<string, Column<unknown>>();
  const columnOrder: string[] = [];
  const resultLength = leftIndices.length;

  const rightNonKeyCols = right.columns.filter((c) => !rightKeys.includes(c));
  const leftNonKeyCols = left.columns.filter((c) => !leftKeys.includes(c));
  const rightColRenames = new Map<string, string>();
  for (const rc of rightNonKeyCols) {
    if (left.columns.includes(rc)) {
      rightColRenames.set(rc, `${rc}${suffix}`);
    } else {
      rightColRenames.set(rc, rc);
    }
  }

  // Key columns (use left key names as output names)
  for (let ki = 0; ki < leftKeys.length; ki++) {
    const leftKey = leftKeys[ki]!;
    const rightKey = rightKeys[ki]!;
    const leftCol = left.col(leftKey).column;
    const rightCol = right.col(rightKey).column;
    const values: unknown[] = new Array(resultLength);
    for (let i = 0; i < resultLength; i++) {
      const li = leftIndices[i]!;
      const ri = rightIndices[i]!;
      if (li >= 0) {
        values[i] = leftCol.get(li);
      } else if (ri !== null) {
        values[i] = rightCol.get(ri);
      } else {
        values[i] = null;
      }
    }
    resultColumns.set(leftKey, buildColumnFromValues(leftCol.dtype, values));
    columnOrder.push(leftKey);
  }

  // Left non-key columns
  for (const colName of leftNonKeyCols) {
    const srcCol = left.col(colName).column;
    const values: unknown[] = new Array(resultLength);
    for (let i = 0; i < resultLength; i++) {
      const li = leftIndices[i]!;
      values[i] = li >= 0 ? srcCol.get(li) : null;
    }
    resultColumns.set(colName, buildColumnFromValues(srcCol.dtype, values));
    columnOrder.push(colName);
  }

  // Right non-key columns
  for (const colName of rightNonKeyCols) {
    const srcCol = right.col(colName).column;
    const outputName = rightColRenames.get(colName)!;
    const values: unknown[] = new Array(resultLength);
    for (let i = 0; i < resultLength; i++) {
      const ri = rightIndices[i]!;
      if (ri !== null) {
        values[i] = srcCol.get(ri);
      } else {
        values[i] = null;
      }
    }
    resultColumns.set(outputName, buildColumnFromValues(srcCol.dtype, values));
    columnOrder.push(outputName);
  }

  const Ctor = left.constructor as DataFrameConstructor;
  return new Ctor<Record<string, unknown>>(resultColumns, columnOrder);
}
