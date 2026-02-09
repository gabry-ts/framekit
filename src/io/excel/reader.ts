import type { ExcelReadOptions } from '../../types/options';
import { DType } from '../../types/dtype';
import { IOError } from '../../errors';

export interface ParsedExcelSheet {
  header: string[];
  columns: Record<string, unknown[]>;
  inferredTypes: Record<string, DType>;
}

function detectCellDType(value: unknown): DType {
  if (value === null || value === undefined) return DType.Float64;
  if (typeof value === 'number') return DType.Float64;
  if (typeof value === 'boolean') return DType.Boolean;
  if (value instanceof Date) return DType.Date;
  if (typeof value === 'string') return DType.Utf8;
  if (typeof value === 'object') return DType.Object;
  return DType.Utf8;
}

function parseCellRef(ref: string): { col: number; row: number } {
  const match = /^([A-Z]+)(\d+)$/.exec(ref.toUpperCase());
  if (!match) throw new IOError(`Invalid cell reference: '${ref}'`);
  const colStr = match[1]!;
  const rowNum = parseInt(match[2]!, 10);
  let colNum = 0;
  for (let i = 0; i < colStr.length; i++) {
    colNum = colNum * 26 + (colStr.charCodeAt(i) - 64);
  }
  return { col: colNum, row: rowNum };
}

function parseRange(range: string): { startCol: number; startRow: number; endCol: number; endRow: number } {
  const parts = range.split(':');
  if (parts.length !== 2) throw new IOError(`Invalid range format: '${range}'. Expected format like 'A1:G100'`);
  const start = parseCellRef(parts[0]!);
  const end = parseCellRef(parts[1]!);
  return { startCol: start.col, startRow: start.row, endCol: end.col, endRow: end.row };
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
function getWorksheet(workbook: any, sheet: string | number | undefined): any {
  if (sheet === undefined || sheet === 0) {
    // Default: first worksheet
    // eslint-disable-next-line @typescript-eslint/no-explicit-any, @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unsafe-assignment
    const ws = workbook.worksheets[0];
    if (!ws) throw new IOError('Workbook contains no worksheets');
    return ws;
  }
  if (typeof sheet === 'number') {
    // 0-based index
    // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unsafe-assignment
    const ws = workbook.worksheets[sheet];
    if (!ws) throw new IOError(`Worksheet at index ${String(sheet)} not found`);
    return ws;
  }
  // Sheet name
  // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-unsafe-assignment
  const ws = workbook.getWorksheet(sheet);
  if (!ws) throw new IOError(`Worksheet '${sheet}' not found`);
  return ws;
}

function normalizeCellValue(value: unknown): unknown {
  if (value === null || value === undefined) return null;
  // exceljs may return rich text objects: { richText: [...] }
  if (typeof value === 'object' && value !== null && !Array.isArray(value) && !(value instanceof Date)) {
    if ('richText' in value) {
      // eslint-disable-next-line @typescript-eslint/no-explicit-any, @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-unsafe-return
      return (value as any).richText.map((t: any) => String(t.text)).join('');
    }
    // exceljs formula result
    if ('result' in value) {
      // eslint-disable-next-line @typescript-eslint/no-explicit-any, @typescript-eslint/no-unsafe-member-access
      return (value as any).result as unknown;
    }
  }
  return value;
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
export function parseExcelWorksheet(worksheet: any, options: ExcelReadOptions = {}): ParsedExcelSheet {
  const hasHeader = options.hasHeader !== false;
  const rangeDef = options.range;

  // Determine row/col boundaries
  let startRow: number;
  let endRow: number;
  let startCol: number;
  let endCol: number;

  if (rangeDef) {
    const r = parseRange(rangeDef);
    startRow = r.startRow;
    endRow = r.endRow;
    startCol = r.startCol;
    endCol = r.endCol;
  } else {
    // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access
    startRow = 1;
    // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access
    endRow = worksheet.rowCount as number;
    startCol = 1;
    // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access
    endCol = worksheet.columnCount as number;
  }

  if (endRow < startRow || endCol < startCol) {
    return { header: [], columns: {}, inferredTypes: {} };
  }

  // Read all cell values in the range
  const rawRows: unknown[][] = [];
  for (let r = startRow; r <= endRow; r++) {
    // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-unsafe-assignment
    const row = worksheet.getRow(r);
    const rowValues: unknown[] = [];
    for (let c = startCol; c <= endCol; c++) {
      // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-unsafe-assignment
      const cell = row.getCell(c);
      // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access
      rowValues.push(normalizeCellValue(cell.value));
    }
    rawRows.push(rowValues);
  }

  // Extract header
  let header: string[];
  let dataStartIdx: number;

  if (hasHeader && rawRows.length > 0) {
    header = rawRows[0]!.map((v, i) => {
      if (v === null || v === undefined) return `column_${String(i)}`;
      if (typeof v === 'string') return v;
      if (typeof v === 'number' || typeof v === 'boolean' || typeof v === 'bigint') return String(v);
      if (v instanceof Date) return v.toISOString();
      return `column_${String(i)}`;
    });
    dataStartIdx = 1;
  } else {
    const colCount = endCol - startCol + 1;
    header = Array.from({ length: colCount }, (_, i) => `column_${String(i)}`);
    dataStartIdx = 0;
  }

  // Build columnar data
  const columns: Record<string, unknown[]> = {};
  for (const name of header) {
    columns[name] = [];
  }

  for (let i = dataStartIdx; i < rawRows.length; i++) {
    const row = rawRows[i]!;
    for (let j = 0; j < header.length; j++) {
      const colName = header[j]!;
      columns[colName]!.push(j < row.length ? (row[j] ?? null) : null);
    }
  }

  // Detect types
  const inferredTypes: Record<string, DType> = {};
  for (const name of header) {
    if (options.dtypes?.[name] !== undefined) {
      inferredTypes[name] = options.dtypes[name]!;
    } else {
      // Detect from first non-null value
      const colValues = columns[name]!;
      let detected = DType.Float64;
      for (const v of colValues) {
        if (v !== null && v !== undefined) {
          detected = detectCellDType(v);
          break;
        }
      }
      inferredTypes[name] = detected;
    }
  }

  return { header, columns, inferredTypes };
}

export async function readExcelFile(
  filePath: string,
  options: ExcelReadOptions = {},
): Promise<ParsedExcelSheet> {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  let ExcelJS: any; // eslint-disable-line @typescript-eslint/no-unsafe-assignment
  try {
    // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment
    ExcelJS = await import('exceljs');
  } catch {
    throw new IOError(
      'exceljs is required to read Excel files but is not installed. Run: npm install exceljs',
    );
  }

  try {
    // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-unsafe-assignment
    const workbook = new ExcelJS.Workbook();
    // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unsafe-call
    await workbook.xlsx.readFile(filePath);

    // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment
    const worksheet = getWorksheet(workbook, options.sheet);
    return parseExcelWorksheet(worksheet, options);
  } catch (err) {
    if (err instanceof IOError) throw err;
    const message = err instanceof Error ? err.message : String(err);
    throw new IOError(`Failed to read Excel file '${filePath}': ${message}`);
  }
}
