/**
 * Compat helper functions — column selectors, sort helpers, and aggregation operators.
 */

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
