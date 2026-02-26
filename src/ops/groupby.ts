import type { DataFrame } from '../dataframe';
import { DType } from '../types/dtype';
import { Column } from '../storage/column';
import { Float64Column } from '../storage/numeric';
import { Utf8Column } from '../storage/string';
import { BooleanColumn } from '../storage/boolean';
import { DateColumn } from '../storage/date';
import { ColumnNotFoundError } from '../errors';
import { AggExpr, SumAggExpr, col } from '../expr/expr';
import type { ParallelAggOptions, AggSpec } from '../engine/parallelism/types';
import { shouldUseParallel, parallelAgg } from '../engine/parallelism/parallel-agg';

type DataFrameConstructor = new <S extends Record<string, unknown>>(
  columns: Map<string, Column<unknown>>,
  columnOrder: string[],
) => DataFrame<S>;

export class GroupBy<
  S extends Record<string, unknown> = Record<string, unknown>,
  GroupKeys extends string = string,
> {
  private readonly _df: DataFrame<S>;
  private readonly _keys: GroupKeys[];
  private readonly _groupMap: Map<string, number[]>;

  constructor(df: DataFrame<S>, keys: GroupKeys[]) {
    this._df = df;
    this._keys = keys;

    // Validate keys exist
    for (const key of keys) {
      if (!df.columns.includes(key)) {
        throw new ColumnNotFoundError(key, df.columns);
      }
    }

    // Build hash map: serialized-key -> row-index-array
    this._groupMap = new Map<string, number[]>();
    const columns: Column<unknown>[] = keys.map((k) => df.col(k).column);

    // Fast path: single-key interned Utf8Column — use dictionary indices as bucket keys
    if (keys.length === 1) {
      const col = columns[0]!;
      if (col instanceof Utf8Column && col.isInterned) {
        const interned = col.internedStorage!;
        const internedIndices = interned.indices;
        const dictLen = interned.dictionary.length;
        const n = df.length;

        // Use array of arrays indexed by dictionary index
        const buckets: number[][] = new Array<number[]>(dictLen);
        for (let d = 0; d < dictLen; d++) {
          buckets[d] = [];
        }

        if (col.allValid) {
          for (let i = 0; i < n; i++) {
            buckets[internedIndices[i]!]!.push(i);
          }
        } else {
          const nullMask = col.nullMask;
          // Null group bucket
          let nullBucket: number[] | null = null;
          for (let i = 0; i < n; i++) {
            if (nullMask.getUnsafe(i)) {
              buckets[internedIndices[i]!]!.push(i);
            } else {
              if (!nullBucket) nullBucket = [];
              nullBucket.push(i);
            }
          }
          if (nullBucket) {
            this._groupMap.set('\0null', nullBucket);
          }
        }

        // Convert buckets to groupMap entries
        for (let d = 0; d < dictLen; d++) {
          const bucket = buckets[d]!;
          if (bucket.length > 0) {
            this._groupMap.set(`\0string${interned.dictionary[d]!}`, bucket);
          }
        }
        return;
      }
    }

    for (let i = 0; i < df.length; i++) {
      const keyStr = this._serializeKey(columns, i);
      const group = this._groupMap.get(keyStr);
      if (group) {
        group.push(i);
      } else {
        this._groupMap.set(keyStr, [i]);
      }
    }
  }

  get keys(): GroupKeys[] {
    return [...this._keys];
  }

  get dataframe(): DataFrame<S> {
    return this._df;
  }

  get groupMap(): Map<string, number[]> {
    return this._groupMap;
  }

  nGroups(): number {
    return this._groupMap.size;
  }

  agg(
    specs: Record<string, AggExpr<unknown> | string>,
    options?: ParallelAggOptions,
  ): DataFrame<Record<string, unknown>> {
    // Check if we should use parallel execution
    if (options || shouldUseParallel(this._df.length)) {
      const useParallel = shouldUseParallel(this._df.length, options);
      if (useParallel) {
        // Cannot return async from sync method — use aggAsync for parallel
        // But we can try synchronous parallel via a blocking pattern
        // For API compatibility, delegate to _aggSync
      }
    }
    return this._aggSync(specs);
  }

  /**
   * Async aggregation that uses worker threads for large datasets.
   * Falls back to single-threaded when workers are unavailable or data is small.
   */
  async aggAsync(
    specs: Record<string, AggExpr<unknown> | string>,
    options?: ParallelAggOptions,
  ): Promise<DataFrame<Record<string, unknown>>> {
    // Resolve specs
    const resolvedSpecs: Record<string, AggExpr<unknown>> = {};
    for (const [name, spec] of Object.entries(specs)) {
      if (typeof spec === 'string') {
        resolvedSpecs[name] = this._resolveShorthand(name, spec);
      } else {
        resolvedSpecs[name] = spec;
      }
    }

    const useParallel = shouldUseParallel(this._df.length, options);
    if (!useParallel) {
      return this._aggSync(specs);
    }

    // Convert AggExpr to worker-compatible AggSpec
    const aggSpecs: Record<string, AggSpec> = {};
    for (const [name, aggExpr] of Object.entries(resolvedSpecs)) {
      const aggType = this._aggExprToType(aggExpr);
      if (!aggType) {
        // Unknown agg type — fall back to sync
        return this._aggSync(specs);
      }
      aggSpecs[name] = {
        columnName: aggExpr.dependencies[0]!,
        aggType,
      };
    }

    // Collect source columns
    const sourceColumns = new Map<string, Column<unknown>>();
    for (const colName of this._df.columns) {
      sourceColumns.set(colName, this._df.col(colName).column);
    }

    const groupEntries = [...this._groupMap.entries()];
    const keyCols = this._keys.map((k) => this._df.col(k).column);

    const results = await parallelAgg(
      groupEntries,
      [...this._keys],
      keyCols,
      aggSpecs,
      sourceColumns,
      options,
    );

    // Build result DataFrame from parallel results
    const nGroups = results.length;
    const resultColumns = new Map<string, Column<unknown>>();
    const columnOrder: string[] = [];

    // Key columns
    for (let ki = 0; ki < this._keys.length; ki++) {
      const k = this._keys[ki]!;
      const vals = results.map((r) => r.keyValues[ki] ?? null);
      resultColumns.set(k, this._buildColumnLike(keyCols[ki]!, vals));
      columnOrder.push(k);
    }

    // Agg columns
    const aggNames = Object.keys(specs);
    for (const name of aggNames) {
      const vals = results.map((r) => r.aggValues[name] ?? null);
      resultColumns.set(name, this._inferColumn(vals, nGroups));
      columnOrder.push(name);
    }

    const Ctor = this._df.constructor as DataFrameConstructor;
    return new Ctor<Record<string, unknown>>(resultColumns, columnOrder);
  }

  private _aggSync(
    specs: Record<string, AggExpr<unknown> | string>,
  ): DataFrame<Record<string, unknown>> {
    const groupEntries = [...this._groupMap.entries()];
    const nGroups = groupEntries.length;
    const keyColumns = this._keys;
    const aggNames = Object.keys(specs);

    // Resolve string shorthands to AggExpr instances
    const resolvedSpecs: Record<string, AggExpr<unknown>> = {};
    for (const [name, spec] of Object.entries(specs)) {
      if (typeof spec === 'string') {
        resolvedSpecs[name] = this._resolveShorthand(name, spec);
      } else {
        resolvedSpecs[name] = spec;
      }
    }

    // Build key column values
    const keyValues: Map<string, unknown[]> = new Map();
    for (const k of keyColumns) {
      keyValues.set(k, []);
    }
    // Build agg column values
    const aggValues: Map<string, unknown[]> = new Map();
    for (const name of aggNames) {
      aggValues.set(name, []);
    }

    const keyCols = keyColumns.map((k) => this._df.col(k).column);

    if (aggNames.length === 1) {
      const onlyAggName = aggNames[0]!;
      const onlyAgg = resolvedSpecs[onlyAggName]!;
      if (onlyAgg instanceof SumAggExpr) {
        const sourceCol = this._df.col(onlyAgg.dependencies[0]!).column;
        for (const [, indices] of groupEntries) {
          const firstIndex = indices[0]!;
          for (let ki = 0; ki < keyColumns.length; ki++) {
            keyValues.get(keyColumns[ki]!)!.push(keyCols[ki]!.get(firstIndex));
          }

          let sum = 0;
          let hasValue = false;
          for (let ii = 0; ii < indices.length; ii++) {
            const v = sourceCol.get(indices[ii]!);
            if (typeof v === 'number') {
              sum += v;
              hasValue = true;
            }
          }
          aggValues.get(onlyAggName)!.push(hasValue ? sum : null);
        }

        const resultColumns = new Map<string, Column<unknown>>();
        const columnOrder: string[] = [];
        for (let ki = 0; ki < keyColumns.length; ki++) {
          const k = keyColumns[ki]!;
          const vals = keyValues.get(k)!;
          resultColumns.set(k, this._buildColumnLike(keyCols[ki]!, vals));
          columnOrder.push(k);
        }
        resultColumns.set(
          onlyAggName,
          Float64Column.from(aggValues.get(onlyAggName)! as (number | null)[]),
        );
        columnOrder.push(onlyAggName);

        const Ctor = this._df.constructor as DataFrameConstructor;
        return new Ctor<Record<string, unknown>>(resultColumns, columnOrder);
      }
    }

    for (const [, indices] of groupEntries) {
      // Extract key values from first row of each group
      const firstIndex = indices[0]!;
      for (let ki = 0; ki < keyColumns.length; ki++) {
        keyValues.get(keyColumns[ki]!)!.push(keyCols[ki]!.get(firstIndex));
      }

      // Build sub-columns and evaluate aggregations
      const int32Indices = new Int32Array(indices);
      const subColumns = new Map<string, Column<unknown>>();
      for (const sourceName of this._df.columns) {
        subColumns.set(sourceName, this._df.col(sourceName).column.take(int32Indices));
      }
      const Ctor = this._df.constructor as DataFrameConstructor;
      const subFrame = new Ctor<Record<string, unknown>>(subColumns, [...this._df.columns]);

      for (const name of aggNames) {
        const aggExpr = resolvedSpecs[name]!;
        const result = aggExpr.evaluateFrame(subFrame);
        aggValues.get(name)!.push(result);
      }
    }

    // Build result DataFrame
    const resultColumns = new Map<string, Column<unknown>>();
    const columnOrder: string[] = [];

    // Key columns first
    for (let ki = 0; ki < keyColumns.length; ki++) {
      const k = keyColumns[ki]!;
      const sourceCol = keyCols[ki]!;
      const vals = keyValues.get(k)!;
      resultColumns.set(k, this._buildColumnLike(sourceCol, vals));
      columnOrder.push(k);
    }

    // Agg columns
    for (const name of aggNames) {
      const vals = aggValues.get(name)!;
      resultColumns.set(name, this._inferColumn(vals, nGroups));
      columnOrder.push(name);
    }

    const Ctor = this._df.constructor as DataFrameConstructor;
    return new Ctor<Record<string, unknown>>(resultColumns, columnOrder);
  }

  private _aggExprToType(aggExpr: AggExpr<unknown>): AggSpec['aggType'] | null {
    // Map AggExpr subclass to worker-compatible string type
    const name = aggExpr.constructor.name;
    const map: Record<string, AggSpec['aggType']> = {
      SumAggExpr: 'sum',
      MeanAggExpr: 'mean',
      CountAggExpr: 'count',
      CountDistinctAggExpr: 'count_distinct',
      MinAggExpr: 'min',
      MaxAggExpr: 'max',
      StdAggExpr: 'std',
      FirstAggExpr: 'first',
      LastAggExpr: 'last',
    };
    return map[name] ?? null;
  }

  private _resolveShorthand(columnName: string, method: string): AggExpr<unknown> {
    const colExpr = col(columnName);
    switch (method) {
      case 'sum':
        return colExpr.sum() as AggExpr<unknown>;
      case 'mean':
        return colExpr.mean() as AggExpr<unknown>;
      case 'count':
        return colExpr.count() as AggExpr<unknown>;
      case 'min':
        return colExpr.min() as AggExpr<unknown>;
      case 'max':
        return colExpr.max() as AggExpr<unknown>;
      case 'std':
        return colExpr.std() as AggExpr<unknown>;
      case 'first':
        return colExpr.first() as AggExpr<unknown>;
      case 'last':
        return colExpr.last() as AggExpr<unknown>;
      default:
        throw new Error(`Unknown aggregation method: ${method}`);
    }
  }

  private _buildColumnLike(source: Column<unknown>, values: unknown[]): Column<unknown> {
    // Build a column of the same type as the source
    switch (source.dtype) {
      case DType.Float64:
        return Float64Column.from(values as (number | null)[]);
      case DType.Int32:
        return Float64Column.from(values as (number | null)[]);
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

  private _inferColumn(values: unknown[], _length: number): Column<unknown> {
    // Infer column type from first non-null value
    let sample: unknown = null;
    for (const v of values) {
      if (v !== null) {
        sample = v;
        break;
      }
    }
    if (sample === null || typeof sample === 'number') {
      return Float64Column.from(values as (number | null)[]);
    }
    if (typeof sample === 'string') {
      return Utf8Column.from(values as (string | null)[]);
    }
    if (typeof sample === 'boolean') {
      return BooleanColumn.from(values as (boolean | null)[]);
    }
    if (sample instanceof Date) {
      return DateColumn.from(values as (Date | null)[]);
    }
    // Fallback: store as Float64
    return Float64Column.from(values as (number | null)[]);
  }

  count(): DataFrame<Record<string, unknown>> {
    // Count rows per group — no column argument needed
    const groupEntries = [...this._groupMap.entries()];
    const keyCols = this._keys.map((k) => this._df.col(k).column);

    const keyValues: Map<string, unknown[]> = new Map();
    for (const k of this._keys) {
      keyValues.set(k, []);
    }
    const counts: (number | null)[] = [];

    for (const [, indices] of groupEntries) {
      const firstIndex = indices[0]!;
      for (let ki = 0; ki < this._keys.length; ki++) {
        keyValues.get(this._keys[ki]!)!.push(keyCols[ki]!.get(firstIndex));
      }
      counts.push(indices.length);
    }

    const resultColumns = new Map<string, Column<unknown>>();
    const columnOrder: string[] = [];
    for (let ki = 0; ki < this._keys.length; ki++) {
      const k = this._keys[ki]!;
      const vals = keyValues.get(k)!;
      resultColumns.set(k, this._buildColumnLike(keyCols[ki]!, vals));
      columnOrder.push(k);
    }
    resultColumns.set('count', Float64Column.from(counts));
    columnOrder.push('count');

    const Ctor = this._df.constructor as DataFrameConstructor;
    return new Ctor<Record<string, unknown>>(resultColumns, columnOrder);
  }

  sum(column: string): DataFrame<Record<string, unknown>> {
    return this.agg({ [column]: col(column).sum() as AggExpr<unknown> });
  }

  mean(column: string): DataFrame<Record<string, unknown>> {
    return this.agg({ [column]: col(column).mean() as AggExpr<unknown> });
  }

  min(column: string): DataFrame<Record<string, unknown>> {
    return this.agg({ [column]: col(column).min() as AggExpr<unknown> });
  }

  max(column: string): DataFrame<Record<string, unknown>> {
    return this.agg({ [column]: col(column).max() as AggExpr<unknown> });
  }

  first(): DataFrame<Record<string, unknown>> {
    // Return first row per group for all non-key columns
    const nonKeyColumns = this._df.columns.filter((c) => !this._keys.includes(c as GroupKeys));
    const specs: Record<string, AggExpr<unknown>> = {};
    for (const c of nonKeyColumns) {
      specs[c] = col(c).first() as AggExpr<unknown>;
    }
    return this.agg(specs);
  }

  last(): DataFrame<Record<string, unknown>> {
    // Return last row per group for all non-key columns
    const nonKeyColumns = this._df.columns.filter((c) => !this._keys.includes(c as GroupKeys));
    const specs: Record<string, AggExpr<unknown>> = {};
    for (const c of nonKeyColumns) {
      specs[c] = col(c).last() as AggExpr<unknown>;
    }
    return this.agg(specs);
  }

  apply(fn: (group: DataFrame<S>) => DataFrame<S>): DataFrame<S> {
    const results: DataFrame<S>[] = [];
    for (const [, indices] of this._groupMap) {
      const subFrame = this._buildSubFrame(indices);
      results.push(fn(subFrame));
    }
    if (results.length === 0) {
      return this._df.slice(0, 0);
    }
    // Use static concat via constructor reference to avoid circular import
    const Ctor = this._df.constructor as DataFrameConstructor & {
      concat(...frames: DataFrame<Record<string, unknown>>[]): DataFrame<Record<string, unknown>>;
    };
    return Ctor.concat(...(results as DataFrame<Record<string, unknown>>[])) as DataFrame<S>;
  }

  groups(): Map<string, DataFrame<S>> {
    const result = new Map<string, DataFrame<S>>();
    for (const [key, indices] of this._groupMap) {
      result.set(key, this._buildSubFrame(indices));
    }
    return result;
  }

  private _serializeKey(columns: Column<unknown>[], index: number): string {
    if (columns.length === 1) {
      const v = columns[0]!.get(index);
      if (v === null) return '\0null';
      if (v instanceof Date) return `\0d${v.getTime()}`;
      if (typeof v === 'number' || typeof v === 'string' || typeof v === 'boolean') {
        return `\0${typeof v}${String(v)}`;
      }
      return `\0obj${JSON.stringify(v)}`;
    }

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

  private _buildSubFrame(indices: number[]): DataFrame<S> {
    const int32Indices = new Int32Array(indices);
    const newColumns = new Map<string, Column<unknown>>();
    const columnOrder = this._df.columns;
    for (const name of columnOrder) {
      newColumns.set(name, this._df.col(name).column.take(int32Indices));
    }
    // Use the same constructor as the source DataFrame
    const Ctor = this._df.constructor as DataFrameConstructor;
    return new Ctor<S>(newColumns, columnOrder);
  }
}
