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

import {
  koffi,
  tidesdb_iter_seek_to_first,
  tidesdb_iter_seek_to_last,
  tidesdb_iter_seek,
  tidesdb_iter_seek_for_prev,
  tidesdb_iter_valid,
  tidesdb_iter_next,
  tidesdb_iter_prev,
  tidesdb_iter_key,
  tidesdb_iter_value,
  tidesdb_iter_free,
} from './ffi';
import { checkResult } from './error';

// Opaque pointer type for iterator
type IterPtr = unknown;

/**
 * Iterator for traversing key-value pairs in a column family.
 */
export class Iterator {
  private _iter: IterPtr | null;

  constructor(iter: IterPtr) {
    this._iter = iter;
  }

  /**
   * Position the iterator at the first key.
   */
  seekToFirst(): void {
    if (!this._iter) throw new Error('Iterator has been freed');
    const result = tidesdb_iter_seek_to_first(this._iter);
    checkResult(result, 'failed to seek to first');
  }

  /**
   * Position the iterator at the last key.
   */
  seekToLast(): void {
    if (!this._iter) throw new Error('Iterator has been freed');
    const result = tidesdb_iter_seek_to_last(this._iter);
    checkResult(result, 'failed to seek to last');
  }

  /**
   * Position the iterator at the first key >= target key.
   */
  seek(key: Buffer): void {
    if (!this._iter) throw new Error('Iterator has been freed');
    const result = tidesdb_iter_seek(this._iter, key, key.length);
    checkResult(result, 'failed to seek');
  }

  /**
   * Position the iterator at the last key <= target key.
   */
  seekForPrev(key: Buffer): void {
    if (!this._iter) throw new Error('Iterator has been freed');
    const result = tidesdb_iter_seek_for_prev(this._iter, key, key.length);
    checkResult(result, 'failed to seek for prev');
  }

  /**
   * Check if the iterator is positioned at a valid entry.
   */
  isValid(): boolean {
    if (!this._iter) return false;
    return tidesdb_iter_valid(this._iter) !== 0;
  }

  /**
   * Move the iterator to the next entry.
   */
  next(): void {
    if (!this._iter) throw new Error('Iterator has been freed');
    const result = tidesdb_iter_next(this._iter);
    checkResult(result, 'failed to move to next');
  }

  /**
   * Move the iterator to the previous entry.
   */
  prev(): void {
    if (!this._iter) throw new Error('Iterator has been freed');
    const result = tidesdb_iter_prev(this._iter);
    checkResult(result, 'failed to move to prev');
  }

  /**
   * Get the current key.
   */
  key(): Buffer {
    if (!this._iter) throw new Error('Iterator has been freed');

    const keyPtrOut: unknown[] = [null];
    const keySizeOut: number[] = [0];

    const result = tidesdb_iter_key(this._iter, keyPtrOut, keySizeOut);
    checkResult(result, 'failed to get key');

    const keySize = keySizeOut[0];
    if (keySize === 0) return Buffer.alloc(0);

    // Decode the pointer to a buffer
    return Buffer.from(koffi.decode(keyPtrOut[0], 'uint8_t', keySize) as number[]);
  }

  /**
   * Get the current value.
   */
  value(): Buffer {
    if (!this._iter) throw new Error('Iterator has been freed');

    const valuePtrOut: unknown[] = [null];
    const valueSizeOut: number[] = [0];

    const result = tidesdb_iter_value(this._iter, valuePtrOut, valueSizeOut);
    checkResult(result, 'failed to get value');

    const valueSize = valueSizeOut[0];
    if (valueSize === 0) return Buffer.alloc(0);

    // Decode the pointer to a buffer
    return Buffer.from(koffi.decode(valuePtrOut[0], 'uint8_t', valueSize) as number[]);
  }

  /**
   * Free the iterator resources.
   */
  free(): void {
    if (this._iter) {
      tidesdb_iter_free(this._iter);
      this._iter = null;
    }
  }
}
