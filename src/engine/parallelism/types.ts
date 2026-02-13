/**
 * Types for worker thread communication in parallel aggregation.
 */

/** Options for parallel aggregation */
export interface ParallelAggOptions {
  /** Number of worker threads. Defaults to os.cpus().length - 1 */
  workerCount?: number | undefined;
  /** Row count threshold above which parallelism kicks in. Defaults to 1_000_000 */
  threshold?: number | undefined;
}

/** Describes an aggregation to perform */
export interface AggSpec {
  /** Source column name */
  columnName: string;
  /** Aggregation type */
  aggType: 'sum' | 'mean' | 'count' | 'count_distinct' | 'min' | 'max' | 'std' | 'first' | 'last';
}

/** Data for a single column sent to a worker */
export interface WorkerColumnData {
  /** Column name */
  name: string;
  /** Column dtype */
  dtype: 'float64' | 'int32' | 'utf8' | 'boolean' | 'date';
  /** Raw data - Float64Array buffer for numeric/date, string[] for utf8 */
  data: ArrayBuffer | string[];
  /** Null mask backing buffer (Uint8Array) */
  nullMask: ArrayBuffer | null;
  /** Column length */
  length: number;
}

/** Message sent to a worker */
export interface WorkerMessage {
  /** Column data keyed by column name */
  columns: Record<string, WorkerColumnData>;
  /** Groups to process: array of [groupIndex, rowIndices] */
  groups: [number, number[]][];
  /** Aggregation specs: output name -> agg spec */
  aggSpecs: Record<string, AggSpec>;
  /** Key column names */
  keyColumns: string[];
}

/** Result from a single group aggregation */
export interface GroupResult {
  /** Group index (for ordering) */
  groupIndex: number;
  /** Key values for this group */
  keyValues: (number | string | boolean | null)[];
  /** Aggregation results keyed by output name */
  aggValues: Record<string, number | string | boolean | null>;
}

/** Message sent back from a worker */
export interface WorkerResult {
  results: GroupResult[];
}
