import { DType } from '../types/dtype';
import { BitArray } from './bitarray';

export abstract class Column<T> {
  abstract readonly dtype: DType;
  protected readonly _nullMask: BitArray;
  protected readonly _length: number;
  private _refCount = 1;

  constructor(length: number, nullMask?: BitArray) {
    this._length = length;
    this._nullMask = nullMask ?? new BitArray(length, true);
  }

  get length(): number {
    return this._length;
  }

  get nullCount(): number {
    return this._nullMask.countZeros();
  }

  addRef(): void {
    this._refCount++;
  }

  release(): void {
    if (this._refCount > 0) {
      this._refCount--;
    }
  }

  get refCount(): number {
    return this._refCount;
  }

  get isShared(): boolean {
    return this._refCount > 1;
  }

  abstract get(index: number): T | null;
  abstract slice(start: number, end: number): Column<T>;
  abstract clone(): Column<T>;
  abstract filter(mask: Column<boolean>): Column<T>;
  abstract take(indices: Int32Array): Column<T>;
}
