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
  tidesdb_txn_put,
  tidesdb_txn_get,
  tidesdb_txn_delete,
  tidesdb_txn_commit,
  tidesdb_txn_rollback,
  tidesdb_txn_reset,
  tidesdb_txn_free,
  tidesdb_txn_savepoint,
  tidesdb_txn_rollback_to_savepoint,
  tidesdb_txn_release_savepoint,
  tidesdb_iter_new,
} from './ffi';
import { checkResult } from './error';
import { ColumnFamily } from './column-family';
import { Iterator } from './iterator';
import { IsolationLevel } from './types';

// Opaque pointer type for transaction
type TxnPtr = unknown;

/**
 * Represents a transaction in TidesDB.
 */
export class Transaction {
  private _txn: TxnPtr | null;

  constructor(txn: TxnPtr) {
    this._txn = txn;
  }

  /**
   * Put a key-value pair into the column family.
   * @param cf Column family to write to.
   * @param key Key as Buffer.
   * @param value Value as Buffer.
   * @param ttl Unix timestamp (seconds since epoch) for expiration, or -1 for no expiration.
   */
  put(cf: ColumnFamily, key: Buffer, value: Buffer, ttl: number = -1): void {
    if (!this._txn) throw new Error('Transaction has been freed');

    const result = tidesdb_txn_put(
      this._txn,
      cf.ptr,
      key,
      key.length,
      value,
      value.length,
      ttl
    );
    checkResult(result, 'failed to put key-value pair');
  }

  /**
   * Get a value from the column family.
   * @param cf Column family to read from.
   * @param key Key as Buffer.
   * @returns Value as Buffer.
   */
  get(cf: ColumnFamily, key: Buffer): Buffer {
    if (!this._txn) throw new Error('Transaction has been freed');

    const valuePtrOut: unknown[] = [null];
    const valueSizeOut: number[] = [0];

    const result = tidesdb_txn_get(
      this._txn,
      cf.ptr,
      key,
      key.length,
      valuePtrOut,
      valueSizeOut
    );
    checkResult(result, 'failed to get value');

    const valueSize = valueSizeOut[0];
    if (valueSize === 0) return Buffer.alloc(0);

    // Decode the pointer to a buffer
    return Buffer.from(koffi.decode(valuePtrOut[0], 'uint8_t', valueSize) as number[]);
  }

  /**
   * Delete a key from the column family.
   * @param cf Column family to delete from.
   * @param key Key as Buffer.
   */
  delete(cf: ColumnFamily, key: Buffer): void {
    if (!this._txn) throw new Error('Transaction has been freed');

    const result = tidesdb_txn_delete(this._txn, cf.ptr, key, key.length);
    checkResult(result, 'failed to delete key');
  }

  /**
   * Commit the transaction.
   */
  commit(): void {
    if (!this._txn) throw new Error('Transaction has been freed');

    const result = tidesdb_txn_commit(this._txn);
    checkResult(result, 'failed to commit transaction');
  }

  /**
   * Rollback the transaction.
   */
  rollback(): void {
    if (!this._txn) throw new Error('Transaction has been freed');

    const result = tidesdb_txn_rollback(this._txn);
    checkResult(result, 'failed to rollback transaction');
  }

  /**
   * Create a savepoint within the transaction.
   * @param name Name of the savepoint.
   */
  savepoint(name: string): void {
    if (!this._txn) throw new Error('Transaction has been freed');

    const result = tidesdb_txn_savepoint(this._txn, name);
    checkResult(result, 'failed to create savepoint');
  }

  /**
   * Rollback the transaction to a savepoint.
   * @param name Name of the savepoint.
   */
  rollbackToSavepoint(name: string): void {
    if (!this._txn) throw new Error('Transaction has been freed');

    const result = tidesdb_txn_rollback_to_savepoint(this._txn, name);
    checkResult(result, 'failed to rollback to savepoint');
  }

  /**
   * Release a savepoint without rolling back.
   * @param name Name of the savepoint.
   */
  releaseSavepoint(name: string): void {
    if (!this._txn) throw new Error('Transaction has been freed');

    const result = tidesdb_txn_release_savepoint(this._txn, name);
    checkResult(result, 'failed to release savepoint');
  }

  /**
   * Create a new iterator for a column family.
   * @param cf Column family to iterate over.
   * @returns A new Iterator instance.
   */
  newIterator(cf: ColumnFamily): Iterator {
    if (!this._txn) throw new Error('Transaction has been freed');

    const iterPtrOut: unknown[] = [null];
    const result = tidesdb_iter_new(this._txn, cf.ptr, iterPtrOut);
    checkResult(result, 'failed to create iterator');

    return new Iterator(iterPtrOut[0]);
  }

  /**
   * Reset a committed or aborted transaction for reuse with a new isolation level.
   * This avoids the overhead of freeing and reallocating transaction resources.
   * @param isolation New isolation level for the reset transaction.
   */
  reset(isolation: IsolationLevel): void {
    if (!this._txn) throw new Error('Transaction has been freed');

    const result = tidesdb_txn_reset(this._txn, isolation);
    checkResult(result, 'failed to reset transaction');
  }

  /**
   * Free the transaction resources.
   */
  free(): void {
    if (this._txn) {
      tidesdb_txn_free(this._txn);
      this._txn = null;
    }
  }
}
