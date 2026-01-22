// Package tidesdb
// Copyright (C) TidesDB
//
// Original Author: Alex Gaetano Padula
//
// Licensed under the Mozilla Public License, v. 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	https://www.mozilla.org/en-US/MPL/2.0/
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

import { lib, ref, uint8PtrPtr, sizeTPtr } from './ffi';
import { checkResult } from './error';

/**
 * Iterator for traversing key-value pairs in a column family.
 */
export class Iterator {
  private _iter: Buffer | null;

  constructor(iter: Buffer) {
    this._iter = iter;
  }

  /**
   * Position the iterator at the first key.
   */
  seekToFirst(): void {
    if (!this._iter) throw new Error('Iterator has been freed');
    const result = lib.tidesdb_iter_seek_to_first(this._iter);
    checkResult(result, 'failed to seek to first');
  }

  /**
   * Position the iterator at the last key.
   */
  seekToLast(): void {
    if (!this._iter) throw new Error('Iterator has been freed');
    const result = lib.tidesdb_iter_seek_to_last(this._iter);
    checkResult(result, 'failed to seek to last');
  }

  /**
   * Position the iterator at the first key >= target key.
   */
  seek(key: Buffer): void {
    if (!this._iter) throw new Error('Iterator has been freed');
    const result = lib.tidesdb_iter_seek(this._iter, key, key.length);
    checkResult(result, 'failed to seek');
  }

  /**
   * Position the iterator at the last key <= target key.
   */
  seekForPrev(key: Buffer): void {
    if (!this._iter) throw new Error('Iterator has been freed');
    const result = lib.tidesdb_iter_seek_for_prev(this._iter, key, key.length);
    checkResult(result, 'failed to seek for prev');
  }

  /**
   * Check if the iterator is positioned at a valid entry.
   */
  isValid(): boolean {
    if (!this._iter) return false;
    return lib.tidesdb_iter_valid(this._iter) !== 0;
  }

  /**
   * Move the iterator to the next entry.
   */
  next(): void {
    if (!this._iter) throw new Error('Iterator has been freed');
    const result = lib.tidesdb_iter_next(this._iter);
    checkResult(result, 'failed to move to next');
  }

  /**
   * Move the iterator to the previous entry.
   */
  prev(): void {
    if (!this._iter) throw new Error('Iterator has been freed');
    const result = lib.tidesdb_iter_prev(this._iter);
    checkResult(result, 'failed to move to prev');
  }

  /**
   * Get the current key.
   */
  key(): Buffer {
    if (!this._iter) throw new Error('Iterator has been freed');

    const keyPtrPtr = ref.alloc(uint8PtrPtr);
    const keySizePtr = ref.alloc(sizeTPtr);

    const result = lib.tidesdb_iter_key(this._iter, keyPtrPtr, keySizePtr);
    checkResult(result, 'failed to get key');

    const keyPtr = keyPtrPtr.deref();
    const keySize = keySizePtr.deref() as unknown as number;

    return ref.reinterpret(keyPtr, keySize, 0);
  }

  /**
   * Get the current value.
   */
  value(): Buffer {
    if (!this._iter) throw new Error('Iterator has been freed');

    const valuePtrPtr = ref.alloc(uint8PtrPtr);
    const valueSizePtr = ref.alloc(sizeTPtr);

    const result = lib.tidesdb_iter_value(this._iter, valuePtrPtr, valueSizePtr);
    checkResult(result, 'failed to get value');

    const valuePtr = valuePtrPtr.deref();
    const valueSize = valueSizePtr.deref() as unknown as number;

    return ref.reinterpret(valuePtr, valueSize, 0);
  }

  /**
   * Free the iterator resources.
   */
  free(): void {
    if (this._iter) {
      lib.tidesdb_iter_free(this._iter);
      this._iter = null;
    }
  }
}
