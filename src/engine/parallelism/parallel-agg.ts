/**
 * Parallel aggregation engine using worker_threads.
 * Partitions groups across workers for large-scale GroupBy operations.
 */

import os from 'os';
import { DType } from '../../types/dtype';
import { Column } from '../../storage/column';
import { WORKER_CODE } from './worker-code';
import type {
  ParallelAggOptions,
  AggSpec,
  WorkerColumnData,
  WorkerMessage,
  WorkerResult,
  GroupResult,
} from './types';

/** Default row count threshold for engaging parallelism */
const DEFAULT_THRESHOLD = 1_000_000;

/** Default worker count: os.cpus().length - 1 */
const defaultWorkerCount = Math.max(1, os.cpus().length - 1);

/** Check if worker_threads is available in the current runtime */
let workerThreadsAvailable: boolean | null = null;

/** Synchronous check — optimistic on first call, cached after first parallelAgg run */
function isWorkerThreadsAvailableSync(): boolean {
  if (workerThreadsAvailable === null) {
    // Optimistic: worker_threads is available in Node >=12
    // If it fails at runtime, parallelAgg will catch and the caller falls back
    return true;
  }
  return workerThreadsAvailable;
}

/** Determine if parallel aggregation should be used */
export function shouldUseParallel(
  rowCount: number,
  options?: ParallelAggOptions,
): boolean {
  const threshold = options?.threshold ?? DEFAULT_THRESHOLD;
  if (rowCount < threshold) return false;
  if (!isWorkerThreadsAvailableSync()) return false;
  const workerCount = options?.workerCount ?? defaultWorkerCount;
  return workerCount > 1;
}

/** Serialize a Column into WorkerColumnData for transfer to workers */
function serializeColumn(name: string, column: Column<unknown>): WorkerColumnData {
  const len = column.length;

  // Build null mask from column values
  let nullMask: ArrayBuffer | null = null;
  if (column.nullCount > 0) {
    const maskBytes = new Uint8Array(Math.ceil(len / 8));
    for (let i = 0; i < len; i++) {
      if (column.get(i) !== null) {
        maskBytes[i >> 3]! |= 1 << (i & 7);
      }
    }
    nullMask = maskBytes.buffer;
  }

  switch (column.dtype) {
    case DType.Float64:
    case DType.Date: {
      const arr = new Float64Array(len);
      for (let i = 0; i < len; i++) {
        const v = column.get(i);
        if (v === null) {
          arr[i] = 0;
        } else if (v instanceof Date) {
          arr[i] = v.getTime();
        } else {
          arr[i] = v as number;
        }
      }
      return {
        name,
        dtype: column.dtype === DType.Date ? 'date' : 'float64',
        data: arr.buffer,
        nullMask,
        length: len,
      };
    }
    case DType.Int32: {
      const arr = new Int32Array(len);
      for (let i = 0; i < len; i++) {
        const v = column.get(i);
        arr[i] = v === null ? 0 : (v as number);
      }
      return {
        name,
        dtype: 'int32',
        data: arr.buffer,
        nullMask,
        length: len,
      };
    }
    case DType.Boolean: {
      const arr = new Uint8Array(len);
      for (let i = 0; i < len; i++) {
        const v = column.get(i);
        arr[i] = v === null ? 0 : (v ? 1 : 0);
      }
      return {
        name,
        dtype: 'boolean',
        data: arr.buffer,
        nullMask,
        length: len,
      };
    }
    case DType.Utf8:
    default: {
      const arr: string[] = [];
      for (let i = 0; i < len; i++) {
        const v = column.get(i);
        if (v === null) {
          arr.push('');
        } else if (typeof v === 'string') {
          arr.push(v);
        } else if (typeof v === 'number' || typeof v === 'boolean') {
          arr.push(String(v));
        } else {
          arr.push('');
        }
      }
      return {
        name,
        dtype: 'utf8',
        data: arr,
        nullMask,
        length: len,
      };
    }
  }
}

/** Partition groups across workers */
function partitionGroups(
  groupEntries: [string, number[]][],
  workerCount: number,
): [number, number[]][][] {
  const partitions: [number, number[]][][] = Array.from(
    { length: workerCount },
    () => [],
  );
  for (let i = 0; i < groupEntries.length; i++) {
    const [, indices] = groupEntries[i]!;
    partitions[i % workerCount]!.push([i, indices]);
  }
  return partitions;
}

/**
 * Run aggregation in parallel using worker threads.
 *
 * @param groupEntries Array of [serializedKey, rowIndices]
 * @param keyColumnNames Group key column names
 * @param keyColumns Key columns from the DataFrame
 * @param aggSpecs Aggregation specs: outputName -> { columnName, aggType }
 * @param sourceColumns Map of source column name -> Column
 * @param options Parallel aggregation options
 * @returns Array of GroupResult in original group order
 */
export async function parallelAgg(
  groupEntries: [string, number[]][],
  keyColumnNames: string[],
  keyColumns: Column<unknown>[],
  aggSpecs: Record<string, AggSpec>,
  sourceColumns: Map<string, Column<unknown>>,
  options?: ParallelAggOptions,
): Promise<GroupResult[]> {
  const wt = await import('worker_threads');
  const { Worker } = wt;
  workerThreadsAvailable = true;

  const workerCount = Math.min(
    options?.workerCount ?? defaultWorkerCount,
    groupEntries.length,
  );

  // Determine which columns we need to send
  const neededColumns = new Set<string>();
  for (const keyName of keyColumnNames) {
    neededColumns.add(keyName);
  }
  for (const spec of Object.values(aggSpecs)) {
    neededColumns.add(spec.columnName);
  }

  // Serialize needed columns
  const serializedColumns: Record<string, WorkerColumnData> = {};
  for (const colName of neededColumns) {
    const column = sourceColumns.get(colName);
    if (column) {
      serializedColumns[colName] = serializeColumn(colName, column);
    }
  }

  // Partition groups across workers
  const partitions = partitionGroups(groupEntries, workerCount);

  // Launch workers
  const workerPromises = partitions.map((partition) => {
    if (partition.length === 0) {
      return Promise.resolve({ results: [] } as WorkerResult);
    }

    return new Promise<WorkerResult>((resolve, reject) => {
      const worker = new Worker(WORKER_CODE, { eval: true });

      const message: WorkerMessage = {
        columns: serializedColumns,
        groups: partition,
        aggSpecs,
        keyColumns: keyColumnNames,
      };

      worker.on('message', (result: WorkerResult) => {
        void worker.terminate();
        resolve(result);
      });

      worker.on('error', (err: Error) => {
        void worker.terminate();
        reject(err);
      });

      worker.postMessage(message);
    });
  });

  const results = await Promise.all(workerPromises);

  // Collect and sort results by groupIndex
  const allResults: GroupResult[] = [];
  for (const workerResult of results) {
    allResults.push(...workerResult.results);
  }
  allResults.sort((a, b) => a.groupIndex - b.groupIndex);

  return allResults;
}
