import { DType } from '../types/dtype';
import { Series } from '../series';
import { Column } from '../storage/column';
import { BitArray } from '../storage/bitarray';
import { Utf8Column } from '../storage/string';
import { BooleanColumn } from '../storage/boolean';
import { Float64Column } from '../storage/numeric';
import { TypeMismatchError } from '../errors';

export class StringAccessor {
  private readonly _series: Series<string>;

  constructor(series: Series<string>) {
    if (series.dtype !== DType.Utf8) {
      throw new TypeMismatchError(
        `StringAccessor requires Series with dtype Utf8, got '${series.dtype}'`,
      );
    }
    this._series = series;
  }

  toLowerCase(): Series<string> {
    return this._mapString((s) => s.toLowerCase());
  }

  toUpperCase(): Series<string> {
    return this._mapString((s) => s.toUpperCase());
  }

  trim(): Series<string> {
    return this._mapString((s) => s.trim());
  }

  startsWith(prefix: string): Series<boolean> {
    return this._mapBoolean((s) => s.startsWith(prefix));
  }

  endsWith(suffix: string): Series<boolean> {
    return this._mapBoolean((s) => s.endsWith(suffix));
  }

  contains(pattern: string): Series<boolean> {
    return this._mapBoolean((s) => s.includes(pattern));
  }

  replace(pattern: string, replacement: string): Series<string> {
    return this._mapString((s) => s.replaceAll(pattern, replacement));
  }

  split(separator: string): Series<string[]> {
    const data: string[][] = [];
    const mask = new BitArray(this._series.length);
    for (let i = 0; i < this._series.length; i++) {
      const val = this._series.get(i);
      if (val === null) {
        data.push([]);
      } else {
        data.push(val.split(separator));
        mask.set(i, true);
      }
    }
    const col = new ListColumn(data, mask);
    return new Series<string[]>(this._series.name, col as Column<string[]>);
  }

  slice(start: number, end?: number): Series<string> {
    return this._mapString((s) => s.slice(start, end));
  }

  length(): Series<number> {
    const results: (number | null)[] = [];
    for (let i = 0; i < this._series.length; i++) {
      const val = this._series.get(i);
      if (val === null) {
        results.push(null);
      } else {
        results.push(val.length);
      }
    }
    return new Series<number>(this._series.name, Float64Column.from(results));
  }

  padStart(length: number, fillChar: string): Series<string> {
    return this._mapString((s) => s.padStart(length, fillChar));
  }

  padEnd(length: number, fillChar: string): Series<string> {
    return this._mapString((s) => s.padEnd(length, fillChar));
  }

  extract(pattern: RegExp): Series<string> {
    const source = pattern.source;
    if (!source.includes('(') || source.replace(/\\\(/g, '').indexOf('(') === -1) {
      throw new TypeMismatchError(
        'extract() pattern must contain at least one capture group',
      );
    }
    const results: (string | null)[] = [];
    for (let i = 0; i < this._series.length; i++) {
      const val = this._series.get(i);
      if (val === null) {
        results.push(null);
      } else {
        const match = val.match(pattern);
        if (match && match[1] !== undefined) {
          results.push(match[1]);
        } else {
          results.push(null);
        }
      }
    }
    return new Series<string>(this._series.name, Utf8Column.from(results));
  }

  private _mapString(fn: (value: string) => string): Series<string> {
    const results: (string | null)[] = [];
    for (let i = 0; i < this._series.length; i++) {
      const val = this._series.get(i);
      if (val === null) {
        results.push(null);
      } else {
        results.push(fn(val));
      }
    }
    return new Series<string>(this._series.name, Utf8Column.from(results));
  }

  private _mapBoolean(fn: (value: string) => boolean): Series<boolean> {
    const results: (boolean | null)[] = [];
    for (let i = 0; i < this._series.length; i++) {
      const val = this._series.get(i);
      if (val === null) {
        results.push(null);
      } else {
        results.push(fn(val));
      }
    }
    return new Series<boolean>(this._series.name, BooleanColumn.from(results));
  }
}

class ListColumn extends Column<string[]> {
  readonly dtype = DType.Utf8;
  private readonly _data: string[][];

  constructor(data: string[][], nullMask: BitArray) {
    super(data.length, nullMask);
    this._data = data;
  }

  get(index: number): string[] | null {
    if (index < 0 || index >= this._length) {
      throw new Error(`Index ${index} out of bounds for column of length ${this._length}`);
    }
    if (!this._nullMask.get(index)) return null;
    return this._data[index]!;
  }

  slice(start: number, end: number): ListColumn {
    const sliced = this._data.slice(start, end);
    const mask = new BitArray(sliced.length);
    for (let i = 0; i < sliced.length; i++) {
      mask.set(i, this._nullMask.get(start + i));
    }
    return new ListColumn(sliced, mask);
  }

  clone(): ListColumn {
    return new ListColumn(
      this._data.map((arr) => [...arr]),
      this._nullMask.clone(),
    );
  }

  filter(mask: Column<boolean>): ListColumn {
    const indices: number[] = [];
    for (let i = 0; i < mask.length; i++) {
      if (mask.get(i) === true) indices.push(i);
    }
    return this._takeByIndices(indices);
  }

  take(indices: Int32Array): ListColumn {
    const idxArray: number[] = [];
    for (let i = 0; i < indices.length; i++) {
      idxArray.push(indices[i]!);
    }
    return this._takeByIndices(idxArray);
  }

  estimatedMemoryBytes(): number {
    let bytes = this._nullMask.byteLength;
    for (let i = 0; i < this._length; i++) {
      if (this._nullMask.get(i)) {
        for (const s of this._data[i]!) {
          bytes += s.length * 2;
        }
      }
    }
    return bytes;
  }

  private _takeByIndices(indices: number[]): ListColumn {
    const data: string[][] = [];
    const mask = new BitArray(indices.length);
    for (let i = 0; i < indices.length; i++) {
      const idx = indices[i]!;
      data.push(this._data[idx]!);
      mask.set(i, this._nullMask.get(idx));
    }
    return new ListColumn(data, mask);
  }
}
