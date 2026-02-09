import type { DType } from './dtype';

export interface CSVReadOptions {
  delimiter?: string;
  encoding?: string;
  hasHeader?: boolean;
  header?: string[];
  dtypes?: Record<string, DType>;
  parseNumbers?: boolean;
  parseDates?: boolean;
  nRows?: number;
  skipRows?: number;
  columns?: string[];
  nullValues?: string[];
  comment?: string;
}

export interface CSVWriteOptions {
  delimiter?: string;
  quoteStyle?: 'always' | 'necessary' | 'never';
  nullValue?: string;
  header?: boolean;
  bom?: boolean;
}

export interface JSONReadOptions {
  path?: string;
}

export interface JSONWriteOptions {
  pretty?: boolean;
}

export interface NDJSONReadOptions {
  chunkSize?: number;
}

export interface PrintOptions {
  maxRows?: number;
  maxCols?: number;
}

export interface SampleOptions {
  seed?: number;
}

export interface ExcelReadOptions {
  sheet?: string | number;
  hasHeader?: boolean | undefined;
  range?: string | undefined;
  dtypes?: Record<string, DType> | undefined;
}

export interface ParquetReadOptions {
  columns?: string[];
}

export interface ExcelWriteOptions {
  sheet?: string | undefined;
  autoFilter?: boolean | undefined;
  freezePanes?: { row: number; col: number } | undefined;
  columnWidths?: Record<string, number> | undefined;
  startCell?: string | undefined;
}
