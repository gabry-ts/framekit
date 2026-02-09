import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { DataFrame } from '../../../src/dataframe';
import { DType } from '../../../src/types/dtype';
import * as fs from 'fs/promises';
import * as path from 'path';
import * as os from 'os';

describe('DataFrame.fromExcel (US-068)', () => {
  let tmpDir: string;
  let testFile: string;

  beforeAll(async () => {
    tmpDir = await fs.mkdtemp(path.join(os.tmpdir(), 'framekit-excel-'));

    // Create a test Excel file using exceljs
    const ExcelJS = await import('exceljs');
    const workbook = new ExcelJS.Workbook();

    // Sheet 1: mixed types with header
    const ws1 = workbook.addWorksheet('Data');
    ws1.addRow(['name', 'age', 'active', 'joined']);
    ws1.addRow(['Alice', 30, true, new Date('2023-01-15')]);
    ws1.addRow(['Bob', 25, false, new Date('2023-06-20')]);
    ws1.addRow(['Charlie', 35, true, new Date('2022-12-01')]);

    // Sheet 2: numbers only
    const ws2 = workbook.addWorksheet('Numbers');
    ws2.addRow(['x', 'y', 'z']);
    ws2.addRow([1, 10, 100]);
    ws2.addRow([2, 20, 200]);
    ws2.addRow([3, 30, 300]);

    // Sheet 3: with null values
    const ws3 = workbook.addWorksheet('WithNulls');
    ws3.addRow(['col_a', 'col_b']);
    ws3.addRow([1, 'hello']);
    ws3.addRow([null, 'world']);
    ws3.addRow([3, null]);

    testFile = path.join(tmpDir, 'test.xlsx');
    await workbook.xlsx.writeFile(testFile);
  });

  afterAll(async () => {
    await fs.rm(tmpDir, { recursive: true, force: true });
  });

  it('reads first sheet by default with header', async () => {
    const df = await DataFrame.fromExcel(testFile);
    expect(df.length).toBe(3);
    expect(df.columns).toEqual(['name', 'age', 'active', 'joined']);
  });

  it('auto-detects column types from cell values', async () => {
    const df = await DataFrame.fromExcel(testFile);
    expect(df.col('name').dtype).toBe(DType.Utf8);
    expect(df.col('age').dtype).toBe(DType.Float64);
    expect(df.col('active').dtype).toBe(DType.Boolean);
    expect(df.col('joined').dtype).toBe(DType.Date);
  });

  it('reads correct values', async () => {
    const df = await DataFrame.fromExcel(testFile);
    expect(df.col('name').get(0)).toBe('Alice');
    expect(df.col('age').get(1)).toBe(25);
    expect(df.col('active').get(1)).toBe(false);
  });

  it('selects sheet by name', async () => {
    const df = await DataFrame.fromExcel(testFile, { sheet: 'Numbers' });
    expect(df.columns).toEqual(['x', 'y', 'z']);
    expect(df.length).toBe(3);
    expect(df.col('x').get(0)).toBe(1);
    expect(df.col('z').get(2)).toBe(300);
  });

  it('selects sheet by index', async () => {
    const df = await DataFrame.fromExcel(testFile, { sheet: 1 });
    expect(df.columns).toEqual(['x', 'y', 'z']);
    expect(df.length).toBe(3);
  });

  it('reads range of cells', async () => {
    // Read only the first two columns and first two data rows from Numbers sheet
    const df = await DataFrame.fromExcel(testFile, { sheet: 'Numbers', range: 'A1:B3' });
    expect(df.columns).toEqual(['x', 'y']);
    expect(df.length).toBe(2);
    expect(df.col('x').get(0)).toBe(1);
    expect(df.col('y').get(1)).toBe(20);
  });

  it('reads without header when hasHeader is false', async () => {
    const df = await DataFrame.fromExcel(testFile, { sheet: 'Numbers', hasHeader: false });
    expect(df.columns[0]).toBe('column_0');
    // First row should be the header values as data
    expect(df.col('column_0').get(0)).toBe('x');
    expect(df.length).toBe(4); // header row + 3 data rows
  });

  it('handles null values', async () => {
    const df = await DataFrame.fromExcel(testFile, { sheet: 'WithNulls' });
    expect(df.length).toBe(3);
    expect(df.col('col_a').get(0)).toBe(1);
    expect(df.col('col_a').get(1)).toBeNull();
    expect(df.col('col_b').get(2)).toBeNull();
  });

  it('throws IOError for non-existent file', async () => {
    await expect(DataFrame.fromExcel('/nonexistent/file.xlsx')).rejects.toThrow(/Failed to read Excel file/);
  });

  it('throws IOError for non-existent sheet name', async () => {
    await expect(
      DataFrame.fromExcel(testFile, { sheet: 'NonExistent' }),
    ).rejects.toThrow(/Worksheet 'NonExistent' not found/);
  });

  it('throws IOError for non-existent sheet index', async () => {
    await expect(
      DataFrame.fromExcel(testFile, { sheet: 99 }),
    ).rejects.toThrow(/Worksheet at index 99 not found/);
  });

  it('respects dtypes override', async () => {
    const df = await DataFrame.fromExcel(testFile, {
      sheet: 'Numbers',
      dtypes: { x: DType.Utf8 },
    });
    expect(df.col('x').dtype).toBe(DType.Utf8);
    expect(df.col('y').dtype).toBe(DType.Float64);
  });
});
