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
  lib,
  ref,
  TidesDBConfig,
  tidesdbPtrPtr,
  ColumnFamilyConfig as FFIColumnFamilyConfig,
  txnPtrPtr,
  CacheStatsStruct,
  charPtrPtr,
  intPtr,
} from './ffi';
import { checkResult, TidesDBError } from './error';
import { ColumnFamily } from './column-family';
import { Transaction } from './transaction';
import {
  Config,
  ColumnFamilyConfig,
  CacheStats,
  LogLevel,
  IsolationLevel,
  CompressionAlgorithm,
  SyncMode,
  ErrorCode,
} from './types';

/**
 * Default database configuration.
 */
export function defaultConfig(): Partial<Config> {
  return {
    numFlushThreads: 2,
    numCompactionThreads: 2,
    logLevel: LogLevel.Info,
    blockCacheSize: 64 * 1024 * 1024,
    maxOpenSSTables: 256,
  };
}

/**
 * Default column family configuration.
 */
export function defaultColumnFamilyConfig(): ColumnFamilyConfig {
  const cConfig = lib.tidesdb_default_column_family_config();
  return {
    writeBufferSize: cConfig.write_buffer_size as number,
    levelSizeRatio: cConfig.level_size_ratio as number,
    minLevels: cConfig.min_levels as number,
    dividingLevelOffset: cConfig.dividing_level_offset as number,
    klogValueThreshold: cConfig.klog_value_threshold as number,
    compressionAlgorithm: cConfig.compression_algo as CompressionAlgorithm,
    enableBloomFilter: cConfig.enable_bloom_filter !== 0,
    bloomFpr: cConfig.bloom_fpr as number,
    enableBlockIndexes: cConfig.enable_block_indexes !== 0,
    indexSampleRatio: cConfig.index_sample_ratio as number,
    blockIndexPrefixLen: cConfig.block_index_prefix_len as number,
    syncMode: cConfig.sync_mode as SyncMode,
    syncIntervalUs: cConfig.sync_interval_us as number,
    skipListMaxLevel: cConfig.skip_list_max_level as number,
    skipListProbability: cConfig.skip_list_probability as number,
    defaultIsolationLevel: cConfig.default_isolation_level as IsolationLevel,
    minDiskSpace: cConfig.min_disk_space as number,
    l1FileCountTrigger: cConfig.l1_file_count_trigger as number,
    l0QueueStallThreshold: cConfig.l0_queue_stall_threshold as number,
  };
}

/**
 * TidesDB database instance.
 */
export class TidesDB {
  private _db: Buffer | null;

  private constructor(db: Buffer) {
    this._db = db;
  }

  /**
   * Open a TidesDB database with the given configuration.
   */
  static open(config: Config): TidesDB {
    const defaults = defaultConfig();
    const mergedConfig = { ...defaults, ...config };

    const cPath = Buffer.from(mergedConfig.dbPath + '\0', 'utf8');

    const cConfig = new TidesDBConfig();
    cConfig.db_path = cPath as unknown as Buffer;
    cConfig.num_flush_threads = mergedConfig.numFlushThreads!;
    cConfig.num_compaction_threads = mergedConfig.numCompactionThreads!;
    cConfig.log_level = mergedConfig.logLevel!;
    cConfig.block_cache_size = mergedConfig.blockCacheSize!;
    cConfig.max_open_sstables = mergedConfig.maxOpenSSTables!;

    const dbPtrPtr = ref.alloc(tidesdbPtrPtr);
    const result = lib.tidesdb_open(cConfig.ref(), dbPtrPtr);
    checkResult(result, 'failed to open database');

    const dbPtr = dbPtrPtr.deref();
    return new TidesDB(dbPtr);
  }

  /**
   * Close the database.
   */
  close(): void {
    if (this._db) {
      const result = lib.tidesdb_close(this._db);
      this._db = null;
      checkResult(result, 'failed to close database');
    }
  }

  /**
   * Create a new column family with the given configuration.
   */
  createColumnFamily(name: string, config: ColumnFamilyConfig = {}): void {
    if (!this._db) throw new Error('Database has been closed');

    const defaults = defaultColumnFamilyConfig();
    const mergedConfig = { ...defaults, ...config };

    const cName = Buffer.from(name + '\0', 'utf8');

    const cConfig = new FFIColumnFamilyConfig();
    cConfig.write_buffer_size = mergedConfig.writeBufferSize!;
    cConfig.level_size_ratio = mergedConfig.levelSizeRatio!;
    cConfig.min_levels = mergedConfig.minLevels!;
    cConfig.dividing_level_offset = mergedConfig.dividingLevelOffset!;
    cConfig.klog_value_threshold = mergedConfig.klogValueThreshold!;
    cConfig.compression_algo = mergedConfig.compressionAlgorithm!;
    cConfig.enable_bloom_filter = mergedConfig.enableBloomFilter ? 1 : 0;
    cConfig.bloom_fpr = mergedConfig.bloomFpr!;
    cConfig.enable_block_indexes = mergedConfig.enableBlockIndexes ? 1 : 0;
    cConfig.index_sample_ratio = mergedConfig.indexSampleRatio!;
    cConfig.block_index_prefix_len = mergedConfig.blockIndexPrefixLen!;
    cConfig.sync_mode = mergedConfig.syncMode!;
    cConfig.sync_interval_us = mergedConfig.syncIntervalUs!;
    cConfig.skip_list_max_level = mergedConfig.skipListMaxLevel!;
    cConfig.skip_list_probability = mergedConfig.skipListProbability!;
    cConfig.default_isolation_level = mergedConfig.defaultIsolationLevel!;
    cConfig.min_disk_space = mergedConfig.minDiskSpace!;
    cConfig.l1_file_count_trigger = mergedConfig.l1FileCountTrigger!;
    cConfig.l0_queue_stall_threshold = mergedConfig.l0QueueStallThreshold!;

    // Set comparator name if provided
    if (mergedConfig.comparatorName) {
      const nameBytes = Buffer.from(mergedConfig.comparatorName, 'utf8');
      for (let i = 0; i < Math.min(nameBytes.length, 63); i++) {
        cConfig.comparator_name[i] = nameBytes[i];
      }
      cConfig.comparator_name[Math.min(nameBytes.length, 63)] = 0;
    }

    const result = lib.tidesdb_create_column_family(this._db, cName, cConfig.ref());
    checkResult(result, 'failed to create column family');
  }

  /**
   * Drop a column family and all associated data.
   */
  dropColumnFamily(name: string): void {
    if (!this._db) throw new Error('Database has been closed');

    const cName = Buffer.from(name + '\0', 'utf8');
    const result = lib.tidesdb_drop_column_family(this._db, cName);
    checkResult(result, 'failed to drop column family');
  }

  /**
   * Get a column family by name.
   */
  getColumnFamily(name: string): ColumnFamily {
    if (!this._db) throw new Error('Database has been closed');

    const cName = Buffer.from(name + '\0', 'utf8');
    const cfPtr = lib.tidesdb_get_column_family(this._db, cName);

    if (ref.isNull(cfPtr)) {
      throw new TidesDBError(ErrorCode.ErrNotFound, `column family not found: ${name}`);
    }

    return new ColumnFamily(cfPtr, name);
  }

  /**
   * List all column families in the database.
   */
  listColumnFamilies(): string[] {
    if (!this._db) throw new Error('Database has been closed');

    const namesPtrPtr = ref.alloc(charPtrPtr);
    const countPtr = ref.alloc(intPtr);

    const result = lib.tidesdb_list_column_families(this._db, namesPtrPtr, countPtr);
    checkResult(result, 'failed to list column families');

    const count = countPtr.deref() as unknown as number;
    if (count === 0) {
      return [];
    }

    const namesPtr = namesPtrPtr.deref();
    const names: string[] = [];

    for (let i = 0; i < count; i++) {
      const strPtr = ref.get(namesPtr, i * ref.sizeof.pointer);
      if (!ref.isNull(strPtr)) {
        names.push(ref.readCString(strPtr as Buffer, 0));
      }
    }

    return names;
  }

  /**
   * Begin a new transaction with default isolation level.
   */
  beginTransaction(): Transaction {
    if (!this._db) throw new Error('Database has been closed');

    const txnPtrPtrBuf = ref.alloc(txnPtrPtr);
    const result = lib.tidesdb_txn_begin(this._db, txnPtrPtrBuf);
    checkResult(result, 'failed to begin transaction');

    const txnPtr = txnPtrPtrBuf.deref();
    return new Transaction(txnPtr);
  }

  /**
   * Begin a new transaction with the specified isolation level.
   */
  beginTransactionWithIsolation(isolation: IsolationLevel): Transaction {
    if (!this._db) throw new Error('Database has been closed');

    const txnPtrPtrBuf = ref.alloc(txnPtrPtr);
    const result = lib.tidesdb_txn_begin_with_isolation(this._db, isolation, txnPtrPtrBuf);
    checkResult(result, 'failed to begin transaction with isolation');

    const txnPtr = txnPtrPtrBuf.deref();
    return new Transaction(txnPtr);
  }

  /**
   * Register a custom comparator with the database.
   */
  registerComparator(name: string, ctxStr: string = ''): void {
    if (!this._db) throw new Error('Database has been closed');

    const cName = Buffer.from(name + '\0', 'utf8');
    const cCtxStr = ctxStr ? Buffer.from(ctxStr + '\0', 'utf8') : ref.NULL;

    const result = lib.tidesdb_register_comparator(this._db, cName, ref.NULL, cCtxStr, ref.NULL);
    checkResult(result, 'failed to register comparator');
  }

  /**
   * Get statistics about the block cache.
   */
  getCacheStats(): CacheStats {
    if (!this._db) throw new Error('Database has been closed');

    const cStats = new CacheStatsStruct();
    const result = lib.tidesdb_get_cache_stats(this._db, cStats.ref());
    checkResult(result, 'failed to get cache stats');

    return {
      enabled: cStats.enabled !== 0,
      totalEntries: cStats.total_entries as number,
      totalBytes: cStats.total_bytes as number,
      hits: cStats.hits as number,
      misses: cStats.misses as number,
      hitRate: cStats.hit_rate as number,
      numPartitions: cStats.num_partitions as number,
    };
  }
}
