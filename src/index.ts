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

/**
 * # TidesDB
 *
 * TypeScript bindings for TidesDB - A high-performance embedded key-value storage engine.
 *
 * TidesDB is a fast and efficient key-value storage engine library written in C.
 * The underlying data structure is based on a log-structured merge-tree (LSM-tree).
 * This TypeScript binding provides a safe, idiomatic interface to TidesDB with full
 * support for all features.
 *
 * ## Features
 *
 * - MVCC with five isolation levels from READ UNCOMMITTED to SERIALIZABLE
 * - Column families (isolated key-value stores with independent configuration)
 * - Bidirectional iterators with forward/backward traversal and seek support
 * - TTL (time to live) support with automatic key expiration
 * - LZ4, LZ4 Fast, ZSTD, Snappy, or no compression
 * - Bloom filters with configurable false positive rates
 * - Global block CLOCK cache for hot blocks
 * - Savepoints for partial transaction rollback
 * - Six built-in comparators plus custom registration
 *
 * ## Example
 *
 * ```typescript
 * import { TidesDB, IsolationLevel } from 'tidesdb';
 *
 * // Open database
 * const db = TidesDB.open({
 *   dbPath: './mydb',
 *   numFlushThreads: 2,
 *   numCompactionThreads: 2,
 * });
 *
 * // Create a column family
 * db.createColumnFamily('my_cf');
 *
 * // Get the column family
 * const cf = db.getColumnFamily('my_cf');
 *
 * // Write data in a transaction
 * const txn = db.beginTransaction();
 * txn.put(cf, Buffer.from('key1'), Buffer.from('value1'), -1);
 * txn.put(cf, Buffer.from('key2'), Buffer.from('value2'), -1);
 * txn.commit();
 * txn.free();
 *
 * // Read data
 * const readTxn = db.beginTransaction();
 * const value = readTxn.get(cf, Buffer.from('key1'));
 * console.log('Value:', value.toString());
 *
 * // Iterate over data
 * const iter = readTxn.newIterator(cf);
 * iter.seekToFirst();
 * while (iter.isValid()) {
 *   const key = iter.key();
 *   const val = iter.value();
 *   console.log(`Key: ${key.toString()}, Value: ${val.toString()}`);
 *   iter.next();
 * }
 * iter.free();
 * readTxn.free();
 *
 * // Close database
 * db.close();
 * ```
 */

// Re-export all public types and classes
export { TidesDB, defaultConfig, defaultColumnFamilyConfig } from './tidesdb';
export { ColumnFamily } from './column-family';
export { Transaction } from './transaction';
export { Iterator } from './iterator';
export { TidesDBError, checkResult } from './error';
export {
  CompressionAlgorithm,
  SyncMode,
  LogLevel,
  IsolationLevel,
  ErrorCode,
  Config,
  ColumnFamilyConfig,
  Stats,
  CacheStats,
  CommitOp,
  CommitHookCallback,
} from './types';
