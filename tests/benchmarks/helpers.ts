import { DataFrame } from '../../src/index';

/**
 * Generate a large DataFrame with mixed column types for benchmarking.
 * Columns: id (number), value (number), category (string), flag (boolean), ...extra numeric cols
 */
export function generateLargeDF(
  nRows: number,
  nCols: number,
): DataFrame<Record<string, unknown>> {
  const rows: Record<string, unknown>[] = [];
  const categories = ['alpha', 'beta', 'gamma', 'delta', 'epsilon'];

  for (let i = 0; i < nRows; i++) {
    const row: Record<string, unknown> = {
      id: i,
      value: Math.random() * 1000,
      category: categories[i % categories.length]!,
      flag: i % 2 === 0,
    };
    for (let c = 4; c < nCols; c++) {
      row[`col_${String(c)}`] = Math.random() * 100;
    }
    rows.push(row);
  }

  return DataFrame.fromRows(rows);
}

/**
 * Generate a CSV string with mixed types for CSV parsing benchmarks.
 */
export function generateCSVString(nRows: number): string {
  const categories = ['alpha', 'beta', 'gamma', 'delta', 'epsilon'];
  const lines: string[] = ['id,value,category,flag'];

  for (let i = 0; i < nRows; i++) {
    lines.push(
      `${String(i)},${String(Math.random() * 1000)},${categories[i % categories.length]!},${String(i % 2 === 0)}`,
    );
  }

  return lines.join('\n');
}
