/**
 * Compat helper functions — column selectors, sort helpers, and aggregation operators.
 */

import type { AllSelector, NotSelector, RangeSelector, DescSpec, ColumnSelector } from './types';
import { FrameKitError, ErrorCode } from '../errors';

/**
 * Create a selector that matches all columns.
 *
 * ```ts
 * resolveSelector(df, all()) // → ['a', 'b', 'c']
 * ```
 */
export function all(): AllSelector {
  return { kind: 'all' };
}

/**
 * Create a selector that excludes the specified columns.
 *
 * ```ts
 * resolveSelector(df, not('id', 'timestamp')) // → all columns except id and timestamp
 * ```
 */
export function not(...columns: string[]): NotSelector {
  return { kind: 'not', columns };
}

/**
 * Create a selector for a contiguous range of columns (inclusive).
 *
 * ```ts
 * resolveSelector(df, range('b', 'd')) // → ['b', 'c', 'd']
 * ```
 */
export function range(start: string, end: string): RangeSelector {
  return { kind: 'range', start, end };
}

/**
 * Create a descending sort specification for use with orderby.
 *
 * ```ts
 * orderby(df, [desc('price'), 'name'])
 * ```
 */
export function desc(column: string): DescSpec {
  return { kind: 'desc', column };
}

/**
 * Resolve a ColumnSelector against a list of column names, returning the
 * selected column names.
 *
 * @param columnNames - The full ordered list of column names from a DataFrame.
 * @param selector - A ColumnSelector (all, not, or range).
 * @returns The resolved column names.
 * @throws FrameKitError for invalid selectors (non-existent columns).
 */
export function resolveSelector(
  columnNames: string[],
  selector: ColumnSelector,
): string[] {
  switch (selector.kind) {
    case 'all':
      return [...columnNames];

    case 'not': {
      const available = new Set(columnNames);
      for (const col of selector.columns) {
        if (!available.has(col)) {
          throw new FrameKitError(
            ErrorCode.COLUMN_NOT_FOUND,
            `Column '${col}' not found in not() selector. Available columns: [${columnNames.join(', ')}]`,
          );
        }
      }
      const exclude = new Set(selector.columns);
      return columnNames.filter((c) => !exclude.has(c));
    }

    case 'range': {
      const startIdx = columnNames.indexOf(selector.start);
      if (startIdx === -1) {
        throw new FrameKitError(
          ErrorCode.COLUMN_NOT_FOUND,
          `Range start column '${selector.start}' not found. Available columns: [${columnNames.join(', ')}]`,
        );
      }
      const endIdx = columnNames.indexOf(selector.end);
      if (endIdx === -1) {
        throw new FrameKitError(
          ErrorCode.COLUMN_NOT_FOUND,
          `Range end column '${selector.end}' not found. Available columns: [${columnNames.join(', ')}]`,
        );
      }
      const lo = Math.min(startIdx, endIdx);
      const hi = Math.max(startIdx, endIdx);
      return columnNames.slice(lo, hi + 1);
    }
  }
}

/**
 * Aggregation operator helpers for use in rollup expressions.
 *
 * In rollup context, column values are passed as arrays. These helpers
 * aggregate arrays into scalar values.
 *
 * ```ts
 * rollup(df, { total: d => op.sum(d.amount) })
 * ```
 */
export const op = {
  /** Sum of non-null numeric values. Returns 0 for empty input. */
  sum(values: unknown[]): number {
    let total = 0;
    for (const v of values) {
      if (v != null) total += v as number;
    }
    return total;
  },

  /** Mean of non-null numeric values. Returns null if no non-null values. */
  mean(values: unknown[]): number | null {
    let total = 0;
    let count = 0;
    for (const v of values) {
      if (v != null) {
        total += v as number;
        count++;
      }
    }
    return count === 0 ? null : total / count;
  },

  /** Count of non-null values. */
  count(values: unknown[]): number {
    let n = 0;
    for (const v of values) {
      if (v != null) n++;
    }
    return n;
  },

  /** Minimum of non-null numeric values. Returns null if no non-null values. */
  min(values: unknown[]): number | null {
    let result: number | null = null;
    for (const v of values) {
      if (v != null) {
        const n = v as number;
        if (result === null || n < result) result = n;
      }
    }
    return result;
  },

  /** Maximum of non-null numeric values. Returns null if no non-null values. */
  max(values: unknown[]): number | null {
    let result: number | null = null;
    for (const v of values) {
      if (v != null) {
        const n = v as number;
        if (result === null || n > result) result = n;
      }
    }
    return result;
  },

  /** Count of distinct non-null values. */
  distinct(values: unknown[]): number {
    const seen = new Set<unknown>();
    for (const v of values) {
      if (v != null) seen.add(v);
    }
    return seen.size;
  },

  /** First non-null value, or null if all null. */
  first(values: unknown[]): unknown {
    for (const v of values) {
      if (v != null) return v;
    }
    return null;
  },

  /** Last non-null value, or null if all null. */
  last(values: unknown[]): unknown {
    for (let i = values.length - 1; i >= 0; i--) {
      if (values[i] != null) return values[i];
    }
    return null;
  },
};
