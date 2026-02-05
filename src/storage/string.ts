import { DType } from '../types/dtype';
import { ErrorCode, FrameKitError } from '../errors';
import { BitArray } from './bitarray';
import { Column } from './column';

/**
 * String interning storage for low-cardinality columns.
 * Stores unique strings in a dictionary + index array instead of string[].
 */
interface InternedStorage {
  dictionary: string[];
  indices: Uint32Array;
}

export class Utf8Column extends Column<string> {
  readonly dtype = DType.Utf8;
  private readonly _data: string[] | null;
  private readonly _interned: InternedStorage | null;

  constructor(data: string[], nullMask?: BitArray, interned?: InternedStorage) {
    super(data.length, nullMask);
    if (interned) {
      this._data = null;
      this._interned = interned;
    } else {
      this._data = data;
      this._interned = null;
    }
  }

  /** Whether this column uses interned (dictionary-encoded) storage. */
  get isInterned(): boolean {
    return this._interned !== null;
  }

  get(index: number): string | null {
    this._boundsCheck(index);
    if (!this._nullMask.get(index)) {
      return null;
    }
    if (this._interned) {
      return this._interned.dictionary[this._interned.indices[index]!]!;
    }
    return this._data![index]!;
  }

  slice(start: number, end: number): Utf8Column {
    const len = end - start;
    const mask = new BitArray(len);
    for (let i = 0; i < len; i++) {
      mask.set(i, this._nullMask.get(start + i));
    }
    if (this._interned) {
      const slicedIndices = this._interned.indices.slice(start, end);
      return new Utf8Column(new Array<string>(len), mask, {
        dictionary: this._interned.dictionary,
        indices: slicedIndices,
      });
    }
    const sliced = this._data!.slice(start, end);
    return new Utf8Column(sliced, mask);
  }

  clone(): Utf8Column {
    if (this._interned) {
      return new Utf8Column(new Array<string>(this._length), this._nullMask.clone(), {
        dictionary: [...this._interned.dictionary],
        indices: new Uint32Array(this._interned.indices),
      });
    }
    return new Utf8Column([...this._data!], this._nullMask.clone());
  }

  filter(mask: Column<boolean>): Utf8Column {
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

  take(indices: Int32Array): Utf8Column {
    const idxArray: number[] = [];
    for (let i = 0; i < indices.length; i++) {
      idxArray.push(indices[i]!);
    }
    return this._takeByIndices(idxArray);
  }

  static from(values: (string | null)[]): Utf8Column {
    const len = values.length;
    const data: string[] = new Array<string>(len);
    const mask = new BitArray(len);
    for (let i = 0; i < len; i++) {
      const v = values[i];
      if (v !== null && v !== undefined) {
        data[i] = v;
        mask.set(i, true);
      } else {
        data[i] = '';
      }
    }

    // Detect low-cardinality: if unique non-null values < 50% of total rows, use interning
    if (len > 0) {
      const uniqueSet = new Set<string>();
      let nonNullCount = 0;
      for (let i = 0; i < len; i++) {
        if (mask.get(i)) {
          uniqueSet.add(data[i]!);
          nonNullCount++;
        }
      }
      if (nonNullCount > 0 && uniqueSet.size < len * 0.5) {
        const dictionary: string[] = Array.from(uniqueSet);
        const dictMap = new Map<string, number>();
        for (let d = 0; d < dictionary.length; d++) {
          dictMap.set(dictionary[d]!, d);
        }
        const indices = new Uint32Array(len);
        for (let i = 0; i < len; i++) {
          if (mask.get(i)) {
            indices[i] = dictMap.get(data[i]!)!;
          }
        }
        return new Utf8Column(data, mask, { dictionary, indices });
      }
    }

    return new Utf8Column(data, mask);
  }

  private _boundsCheck(index: number): void {
    if (index < 0 || index >= this._length) {
      throw new FrameKitError(
        ErrorCode.INVALID_OPERATION,
        `Index ${index} out of bounds for column of length ${this._length}`,
      );
    }
  }

  private _takeByIndices(indices: number[]): Utf8Column {
    const len = indices.length;
    const mask = new BitArray(len);

    if (this._interned) {
      const newIndices = new Uint32Array(len);
      for (let i = 0; i < len; i++) {
        const idx = indices[i]!;
        this._boundsCheck(idx);
        newIndices[i] = this._interned.indices[idx]!;
        mask.set(i, this._nullMask.get(idx));
      }
      return new Utf8Column(new Array<string>(len), mask, {
        dictionary: this._interned.dictionary,
        indices: newIndices,
      });
    }

    const data: string[] = new Array<string>(len);
    for (let i = 0; i < len; i++) {
      const idx = indices[i]!;
      this._boundsCheck(idx);
      data[i] = this._data![idx]!;
      mask.set(i, this._nullMask.get(idx));
    }
    return new Utf8Column(data, mask);
  }
}
