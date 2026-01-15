import type { JSONWriteOptions } from '../../types/options';

/**
 * Serialize rows to JSON string (array of objects).
 */
export function writeJSON(
  header: string[],
  rows: unknown[][],
  options: JSONWriteOptions = {},
): string {
  const objects: Record<string, unknown>[] = [];
  for (const row of rows) {
    const obj: Record<string, unknown> = {};
    for (let i = 0; i < header.length; i++) {
      const value = row[i];
      if (value instanceof Date) {
        obj[header[i]!] = value.toISOString();
      } else {
        obj[header[i]!] = value ?? null;
      }
    }
    objects.push(obj);
  }

  if (options.pretty) {
    return JSON.stringify(objects, null, 2);
  }
  return JSON.stringify(objects);
}

/**
 * Serialize rows to NDJSON string (one JSON object per line).
 */
export function writeNDJSON(
  header: string[],
  rows: unknown[][],
): string {
  const lines: string[] = [];
  for (const row of rows) {
    const obj: Record<string, unknown> = {};
    for (let i = 0; i < header.length; i++) {
      const value = row[i];
      if (value instanceof Date) {
        obj[header[i]!] = value.toISOString();
      } else {
        obj[header[i]!] = value ?? null;
      }
    }
    lines.push(JSON.stringify(obj));
  }
  return lines.join('\n') + '\n';
}
