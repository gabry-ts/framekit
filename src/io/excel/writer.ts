import type { ExcelWriteOptions } from '../../types/options';
import { IOError } from '../../errors';

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

function columnNumberToLetter(col: number): string {
  let result = '';
  let n = col;
  while (n > 0) {
    const remainder = (n - 1) % 26;
    result = String.fromCharCode(65 + remainder) + result;
    n = Math.floor((n - 1) / 26);
  }
  return result;
}

export async function writeExcelFile(
  filePath: string,
  header: string[],
  rows: unknown[][],
  options: ExcelWriteOptions = {},
): Promise<void> {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  let ExcelJS: any; // eslint-disable-line @typescript-eslint/no-unsafe-assignment
  try {
    // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment
    ExcelJS = await import('exceljs');
  } catch {
    throw new IOError(
      'exceljs is required to write Excel files but is not installed. Run: npm install exceljs',
    );
  }

  try {
    // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-unsafe-assignment
    const workbook = new ExcelJS.Workbook();
    const sheetName = options.sheet ?? 'Sheet1';
    // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-unsafe-assignment
    const worksheet = workbook.addWorksheet(sheetName);

    // Determine start cell
    const startCell = options.startCell ?? 'A1';
    const { col: startCol, row: startRow } = parseCellRef(startCell);

    // Write header row
    for (let i = 0; i < header.length; i++) {
      const colLetter = columnNumberToLetter(startCol + i);
      const cellRef = `${colLetter}${String(startRow)}`;
      // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unsafe-call
      worksheet.getCell(cellRef).value = header[i]!;
    }

    // Write data rows
    for (let r = 0; r < rows.length; r++) {
      const row = rows[r]!;
      for (let c = 0; c < header.length; c++) {
        const colLetter = columnNumberToLetter(startCol + c);
        const cellRef = `${colLetter}${String(startRow + 1 + r)}`;
        const value = c < row.length ? row[c] : null;
        // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unsafe-call
        worksheet.getCell(cellRef).value = value === null || value === undefined ? null : value;
      }
    }

    // Apply autoFilter
    if (options.autoFilter) {
      const lastColLetter = columnNumberToLetter(startCol + header.length - 1);
      const lastRow = startRow + rows.length;
      // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access
      worksheet.autoFilter = `${columnNumberToLetter(startCol)}${String(startRow)}:${lastColLetter}${String(lastRow)}`;
    }

    // Apply freezePanes
    if (options.freezePanes) {
      const freezeRow = options.freezePanes.row;
      const freezeCol = options.freezePanes.col;
      // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access
      worksheet.views = [
        {
          state: 'frozen' as const,
          xSplit: freezeCol,
          ySplit: startRow - 1 + freezeRow,
          topLeftCell: `${columnNumberToLetter(startCol + freezeCol)}${String(startRow + freezeRow)}`,
          activeCell: `${columnNumberToLetter(startCol)}${String(startRow)}`,
        },
      ];
    }

    // Apply columnWidths
    if (options.columnWidths) {
      for (let i = 0; i < header.length; i++) {
        const colName = header[i]!;
        const width = options.columnWidths[colName];
        if (width !== undefined) {
          // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-unsafe-assignment
          const col = worksheet.getColumn(startCol + i);
          // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access
          col.width = width;
        }
      }
    }

    // Write file
    // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unsafe-call
    await workbook.xlsx.writeFile(filePath);
  } catch (err) {
    if (err instanceof IOError) throw err;
    const message = err instanceof Error ? err.message : String(err);
    throw new IOError(`Failed to write Excel file '${filePath}': ${message}`);
  }
}
