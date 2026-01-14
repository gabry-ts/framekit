import type { CSVWriteOptions } from '../../types/options';

/**
 * Serialize rows to CSV string.
 */
export function writeCSV(
  header: string[],
  rows: unknown[][],
  options: CSVWriteOptions = {},
): string {
  const delimiter = options.delimiter ?? ',';
  const quoteStyle = options.quoteStyle ?? 'necessary';
  const nullValue = options.nullValue ?? '';
  const includeHeader = options.header !== false;
  const bom = options.bom === true;

  const lines: string[] = [];

  if (bom) {
    lines.push('\ufeff');
  }

  if (includeHeader) {
    lines.push(header.map((h) => quoteField(h, delimiter, quoteStyle)).join(delimiter));
  }

  for (const row of rows) {
    const fields = row.map((value) => {
      if (value === null || value === undefined) {
        return quoteField(nullValue, delimiter, quoteStyle);
      }
      if (value instanceof Date) {
        return quoteField(value.toISOString(), delimiter, quoteStyle);
      }
      if (typeof value === 'number' || typeof value === 'string' || typeof value === 'boolean') {
        return quoteField(String(value), delimiter, quoteStyle);
      }
      return quoteField(JSON.stringify(value), delimiter, quoteStyle);
    });
    lines.push(fields.join(delimiter));
  }

  return lines.join('\n') + '\n';
}

function quoteField(
  value: string,
  delimiter: string,
  quoteStyle: 'always' | 'necessary' | 'never',
): string {
  if (quoteStyle === 'never') {
    return value;
  }

  if (quoteStyle === 'always') {
    return '"' + value.replace(/"/g, '""') + '"';
  }

  // 'necessary': quote only if the field contains delimiter, quote, or newline
  if (
    value.includes(delimiter) ||
    value.includes('"') ||
    value.includes('\n') ||
    value.includes('\r')
  ) {
    return '"' + value.replace(/"/g, '""') + '"';
  }

  return value;
}
