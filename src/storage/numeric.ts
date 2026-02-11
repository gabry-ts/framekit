import { DType } from '../types/dtype';
import { ErrorCode, FrameKitError } from '../errors';
import { BitArray } from './bitarray';
import { Column } from './column';

export class Float64Column extends Column<number> {
  readonly dtype = DType.Float64;
  private readonly _data: Float64Array;

  constructor(data: Float64Array, nullMask?: BitArray) {
    super(data.length, nullMask);
    this._data = data;
  }

  get(index: number): number | null {
    this._boundsCheck(index);
    if (!this._nullMask.get(index)) {
      return null;
    }
    return this._data[index]!;
  }

  slice(start: number, end: number): Float64Column {
    const sliced = this._data.subarray(start, end);
    const mask = new BitArray(sliced.length);
    for (let i = 0; i < sliced.length; i++) {
      mask.set(i, this._nullMask.get(start + i));
    }
    return new Float64Column(sliced, mask);
  }

  clone(): Float64Column {
    return new Float64Column(new Float64Array(this._data), this._nullMask.clone());
  }

  filter(mask: Column<boolean>): Float64Column {
    if (mask.length !== this._length) {
      throw new FrameKitError(
        ErrorCode.SHAPE_MISMATCH,
        `Filter mask length ${mask.length} does not match column length ${this._length}`,
      );
    }
    const indices: number[] = [];
    for (let i = 0; i < mask.length; i++) {
      if (mask.get(i) === true) {
        indices.push(i);
      }
    }
    return this._takeByIndices(indices);
  }

  take(indices: Int32Array): Float64Column {
    const idxArray: number[] = [];
    for (let i = 0; i < indices.length; i++) {
      idxArray.push(indices[i]!);
    }
    return this._takeByIndices(idxArray);
  }

  sum(): number {
    let total = 0;
    for (let i = 0; i < this._length; i++) {
      if (this._nullMask.get(i)) {
        total += this._data[i]!;
      }
    }
    return total;
  }

  mean(): number | null {
    const validCount = this._length - this.nullCount;
    if (validCount === 0) return null;
    return this.sum() / validCount;
  }

  min(): number | null {
    let result: number | null = null;
    for (let i = 0; i < this._length; i++) {
      if (this._nullMask.get(i)) {
        const val = this._data[i]!;
        if (result === null || val < result) {
          result = val;
        }
      }
    }
    return result;
  }

  max(): number | null {
    let result: number | null = null;
    for (let i = 0; i < this._length; i++) {
      if (this._nullMask.get(i)) {
        const val = this._data[i]!;
        if (result === null || val > result) {
          result = val;
        }
      }
    }
    return result;
  }

  static from(values: (number | null)[]): Float64Column {
    const data = new Float64Array(values.length);
    const mask = new BitArray(values.length);
    for (let i = 0; i < values.length; i++) {
      const v = values[i];
      if (v !== null && v !== undefined) {
        data[i] = v;
        mask.set(i, true);
      }
    }
    return new Float64Column(data, mask);
  }

  private _boundsCheck(index: number): void {
    if (index < 0 || index >= this._length) {
      throw new FrameKitError(
        ErrorCode.INVALID_OPERATION,
        `Index ${index} out of bounds for column of length ${this._length}`,
      );
    }
  }

  private _takeByIndices(indices: number[]): Float64Column {
    const data = new Float64Array(indices.length);
    const mask = new BitArray(indices.length);
    for (let i = 0; i < indices.length; i++) {
      const idx = indices[i]!;
      this._boundsCheck(idx);
      data[i] = this._data[idx]!;
      mask.set(i, this._nullMask.get(idx));
    }
    return new Float64Column(data, mask);
  }
}

export class Int32Column extends Column<number> {
  readonly dtype = DType.Int32;
  private readonly _data: Int32Array;

  constructor(data: Int32Array, nullMask?: BitArray) {
    super(data.length, nullMask);
    this._data = data;
  }

  get(index: number): number | null {
    this._boundsCheck(index);
    if (!this._nullMask.get(index)) {
      return null;
    }
    return this._data[index]!;
  }

  slice(start: number, end: number): Int32Column {
    const sliced = this._data.subarray(start, end);
    const mask = new BitArray(sliced.length);
    for (let i = 0; i < sliced.length; i++) {
      mask.set(i, this._nullMask.get(start + i));
    }
    return new Int32Column(sliced, mask);
  }

  clone(): Int32Column {
    return new Int32Column(new Int32Array(this._data), this._nullMask.clone());
  }

  filter(mask: Column<boolean>): Int32Column {
    if (mask.length !== this._length) {
      throw new FrameKitError(
        ErrorCode.SHAPE_MISMATCH,
        `Filter mask length ${mask.length} does not match column length ${this._length}`,
      );
    }
    const indices: number[] = [];
    for (let i = 0; i < mask.length; i++) {
      if (mask.get(i) === true) {
        indices.push(i);
      }
    }
    return this._takeByIndices(indices);
  }

  take(indices: Int32Array): Int32Column {
    const idxArray: number[] = [];
    for (let i = 0; i < indices.length; i++) {
      idxArray.push(indices[i]!);
    }
    return this._takeByIndices(idxArray);
  }

  sum(): number {
    let total = 0;
    for (let i = 0; i < this._length; i++) {
      if (this._nullMask.get(i)) {
        total += this._data[i]!;
      }
    }
    return total;
  }

  mean(): number | null {
    const validCount = this._length - this.nullCount;
    if (validCount === 0) return null;
    return this.sum() / validCount;
  }

  min(): number | null {
    let result: number | null = null;
    for (let i = 0; i < this._length; i++) {
      if (this._nullMask.get(i)) {
        const val = this._data[i]!;
        if (result === null || val < result) {
          result = val;
        }
      }
    }
    return result;
  }

  max(): number | null {
    let result: number | null = null;
    for (let i = 0; i < this._length; i++) {
      if (this._nullMask.get(i)) {
        const val = this._data[i]!;
        if (result === null || val > result) {
          result = val;
        }
      }
    }
    return result;
  }

  static from(values: (number | null)[]): Int32Column {
    const data = new Int32Array(values.length);
    const mask = new BitArray(values.length);
    for (let i = 0; i < values.length; i++) {
      const v = values[i];
      if (v !== null && v !== undefined) {
        data[i] = v;
        mask.set(i, true);
      }
    }
    return new Int32Column(data, mask);
  }

  private _boundsCheck(index: number): void {
    if (index < 0 || index >= this._length) {
      throw new FrameKitError(
        ErrorCode.INVALID_OPERATION,
        `Index ${index} out of bounds for column of length ${this._length}`,
      );
    }
  }

  private _takeByIndices(indices: number[]): Int32Column {
    const data = new Int32Array(indices.length);
    const mask = new BitArray(indices.length);
    for (let i = 0; i < indices.length; i++) {
      const idx = indices[i]!;
      this._boundsCheck(idx);
      data[i] = this._data[idx]!;
      mask.set(i, this._nullMask.get(idx));
    }
    return new Int32Column(data, mask);
  }
}
