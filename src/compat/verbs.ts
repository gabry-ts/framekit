/**
 * Compat verb functions — Arquero-style derive, rollup, fold, orderby, etc.
 */

import { DataFrame } from '../dataframe';
import { GroupBy } from '../ops/groupby';
import type { ExprRecord, AggExprRecord } from './types';

/**
 * Arquero-style derive: create new columns from row-level expressions.
 *
 * ```ts
 * derive(df, { total: d => d.price * d.qty, label: d => `item-${d.id}` })
 * ```
 *
 * Existing columns are preserved. New columns are appended in insertion order.
 * If a derived column name matches an existing column, the existing column is overwritten.
 */
export function derive(df: DataFrame, exprs: ExprRecord): DataFrame {
  const len = df.length;
  const exprEntries = Object.entries(exprs);

  // Pre-compute derived column values
  const derivedColumns: Record<string, unknown[]> = {};
  for (const [name] of exprEntries) {
    derivedColumns[name] = new Array(len);
  }

  for (let i = 0; i < len; i++) {
    const row = df.row(i);
    for (const [name, fn] of exprEntries) {
      derivedColumns[name]![i] = fn(row);
    }
  }

  // Build result: start with original df, then add/overwrite derived columns
  let result: DataFrame = df;
  for (const [name, values] of Object.entries(derivedColumns)) {
    result = result.withColumn(name, values);
  }

  return result;
}

/**
 * Build a column-oriented data object from a DataFrame (or subset of rows).
 * Each key maps to an array of all values in that column.
 */
function buildColumnData(
  df: DataFrame,
  indices?: number[],
): Record<string, unknown[]> {
  const data: Record<string, unknown[]> = {};
  for (const colName of df.columns) {
    const arr = df.col(colName).toArray();
    if (indices) {
      data[colName] = indices.map((i) => arr[i]!);
    } else {
      data[colName] = arr;
    }
  }
  return data;
}

/**
 * Evaluate aggregation expressions against column-oriented data.
 * Returns a record of { exprName: scalarResult }.
 */
function evalAggExprs(
  data: Record<string, unknown[]>,
  exprs: AggExprRecord,
): Record<string, unknown> {
  const result: Record<string, unknown> = {};
  for (const [name, fn] of Object.entries(exprs)) {
    result[name] = fn(data);
  }
  return result;
}

/**
 * Arquero-style rollup: aggregate columns into a single-row summary.
 *
 * Expression functions receive a column-oriented data object where each
 * property is an array of column values. Use `op.*` helpers for aggregation.
 *
 * ```ts
 * import { rollup, op } from 'framekit/compat';
 *
 * // Ungrouped: single-row result
 * rollup(df, { total: d => op.sum(d.amount) })
 *
 * // Grouped: one row per group
 * rollup(df.groupBy('category'), { total: d => op.sum(d.amount) })
 * ```
 */
export function rollup(
  input: DataFrame | GroupBy,
  exprs: AggExprRecord,
): DataFrame {
  if (input instanceof GroupBy) {
    return rollupGrouped(input, exprs);
  }
  return rollupUngrouped(input, exprs);
}

function rollupUngrouped(df: DataFrame, exprs: AggExprRecord): DataFrame {
  const data = buildColumnData(df);
  const row = evalAggExprs(data, exprs);
  // Wrap each value in an array for DataFrame.fromColumns
  const columns: Record<string, unknown[]> = {};
  for (const [name, value] of Object.entries(row)) {
    columns[name] = [value];
  }
  return DataFrame.fromColumns(columns);
}

function rollupGrouped(grouped: GroupBy, exprs: AggExprRecord): DataFrame {
  const df = grouped.dataframe;
  const keyNames = grouped.keys;
  const groupMap = grouped.groupMap;
  const exprNames = Object.keys(exprs);

  // Initialize result column arrays
  const resultColumns: Record<string, unknown[]> = {};
  for (const k of keyNames) resultColumns[k] = [];
  for (const e of exprNames) resultColumns[e] = [];

  for (const [, indices] of groupMap) {
    // Key values from first row of group
    for (const k of keyNames) {
      resultColumns[k]!.push(df.col(k).toArray()[indices[0]!]);
    }

    // Aggregate over group
    const data = buildColumnData(df, indices);
    const aggResult = evalAggExprs(data, exprs);
    for (const e of exprNames) {
      resultColumns[e]!.push(aggResult[e]);
    }
  }

  return DataFrame.fromColumns(resultColumns);
}
