import { DType } from '../types/dtype';
import { ErrorCode, FrameKitError } from '../errors';
import { BitArray } from './bitarray';
import { Column } from './column';

export class ObjectColumn extends Column<unknown> {
  readonly dtype = DType.Object;
  private readonly _data: unknown[];

  constructor(data: unknown[], nullMask?: BitArray) {
    super(data.length, nullMask);
    this._data = data;
  }

  get(index: number): unknown {
    this._boundsCheck(index);
    if (!this._nullMask.get(index)) {
      return null;
    }
    return this._data[index]!;
  }

  slice(start: number, end: number): ObjectColumn {
    const sliced = this._data.slice(start, end);
    const mask = new BitArray(sliced.length);
    for (let i = 0; i < sliced.length; i++) {
      mask.set(i, this._nullMask.get(start + i));
    }
    return new ObjectColumn(sliced, mask);
  }

  clone(): ObjectColumn {
    return new ObjectColumn([...this._data], this._nullMask.clone());
  }

  filter(mask: Column<boolean>): ObjectColumn {
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

  take(indices: Int32Array): ObjectColumn {
    const idxArray: number[] = [];
    for (let i = 0; i < indices.length; i++) {
      idxArray.push(indices[i]!);
    }
    return this._takeByIndices(idxArray);
  }

  static from(values: unknown[]): ObjectColumn {
    const data: unknown[] = new Array<unknown>(values.length);
    const mask = new BitArray(values.length);
    for (let i = 0; i < values.length; i++) {
      const v = values[i];
      if (v !== null && v !== undefined) {
        data[i] = v;
        mask.set(i, true);
      } else {
        data[i] = null;
      }
    }
    return new ObjectColumn(data, mask);
  }

  private _boundsCheck(index: number): void {
    if (index < 0 || index >= this._length) {
      throw new FrameKitError(
        ErrorCode.INVALID_OPERATION,
        `Index ${index} out of bounds for column of length ${this._length}`,
      );
    }
  }

  private _takeByIndices(indices: number[]): ObjectColumn {
    const data: unknown[] = new Array<unknown>(indices.length);
    const mask = new BitArray(indices.length);
    for (let i = 0; i < indices.length; i++) {
      const idx = indices[i]!;
      this._boundsCheck(idx);
      data[i] = this._data[idx]!;
      mask.set(i, this._nullMask.get(idx));
    }
    return new ObjectColumn(data, mask);
  }
}
