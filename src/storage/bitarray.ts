import { ErrorCode, FrameKitError } from '../errors';

export class BitArray {
  private readonly _buffer: Uint8Array;
  private readonly _length: number;

  constructor(length: number, initialValue = false) {
    if (length < 0) {
      throw new FrameKitError(
        ErrorCode.INVALID_OPERATION,
        `BitArray length must be non-negative, got ${length}`,
      );
    }
    this._length = length;
    const byteCount = Math.ceil(length / 8);
    this._buffer = new Uint8Array(byteCount);
    if (initialValue) {
      this._buffer.fill(0xff);
    }
  }

  get length(): number {
    return this._length;
  }

  get(index: number): boolean {
    this._boundsCheck(index);
    const byteIndex = index >> 3;
    const bitIndex = index & 7;
    return (this._buffer[byteIndex]! & (1 << bitIndex)) !== 0;
  }

  set(index: number, value: boolean): void {
    this._boundsCheck(index);
    const byteIndex = index >> 3;
    const bitIndex = index & 7;
    if (value) {
      this._buffer[byteIndex] = this._buffer[byteIndex]! | (1 << bitIndex);
    } else {
      this._buffer[byteIndex] = this._buffer[byteIndex]! & ~(1 << bitIndex);
    }
  }

  countOnes(): number {
    let count = 0;
    for (let i = 0; i < this._length; i++) {
      if (this.get(i)) {
        count++;
      }
    }
    return count;
  }

  countZeros(): number {
    return this._length - this.countOnes();
  }

  and(other: BitArray): BitArray {
    this._lengthCheck(other);
    const result = new BitArray(this._length);
    for (let i = 0; i < this._buffer.length; i++) {
      result._buffer[i] = this._buffer[i]! & other._buffer[i]!;
    }
    return result;
  }

  or(other: BitArray): BitArray {
    this._lengthCheck(other);
    const result = new BitArray(this._length);
    for (let i = 0; i < this._buffer.length; i++) {
      result._buffer[i] = this._buffer[i]! | other._buffer[i]!;
    }
    return result;
  }

  not(): BitArray {
    const result = new BitArray(this._length);
    for (let i = 0; i < this._buffer.length; i++) {
      result._buffer[i] = ~this._buffer[i]! & 0xff;
    }
    return result;
  }

  private _boundsCheck(index: number): void {
    if (index < 0 || index >= this._length) {
      throw new FrameKitError(
        ErrorCode.INVALID_OPERATION,
        `BitArray index ${index} out of bounds for length ${this._length}`,
      );
    }
  }

  private _lengthCheck(other: BitArray): void {
    if (this._length !== other._length) {
      throw new FrameKitError(
        ErrorCode.SHAPE_MISMATCH,
        `BitArray length mismatch: ${this._length} vs ${other._length}`,
      );
    }
  }
}
