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
  tidesdb_open,
  tidesdb_close,
  tidesdb_default_column_family_config,
  tidesdb_create_column_family,
  tidesdb_drop_column_family,
  tidesdb_rename_column_family,
  tidesdb_clone_column_family,
  tidesdb_get_column_family,
  tidesdb_list_column_families,
  tidesdb_txn_begin,
  tidesdb_txn_begin_with_isolation,
  tidesdb_register_comparator,
  tidesdb_get_cache_stats,
  tidesdb_backup,
  tidesdb_checkpoint,
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

// Opaque pointer type for database
type DBPtr = unknown;

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
    logToFile: false,
    logTruncationAt: 24 * 1024 * 1024,
  };
}

/**
 * Default column family configuration.
 */
export function defaultColumnFamilyConfig(): ColumnFamilyConfig {
  const cConfig = tidesdb_default_column_family_config();
  return {
    writeBufferSize: cConfig.write_buffer_size as number,
    levelSizeRatio: cConfig.level_size_ratio as number,
    minLevels: cConfig.min_levels as number,
    dividingLevelOffset: cConfig.dividing_level_offset as number,
    klogValueThreshold: cConfig.klog_value_threshold as number,
    compressionAlgorithm: cConfig.compression_algorithm as CompressionAlgorithm,
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
    useBtree: cConfig.use_btree !== 0,
  };
}

/**
 * TidesDB database instance.
 */
export class TidesDB {
  private _db: DBPtr | null;

  private constructor(db: DBPtr) {
    this._db = db;
  }

  /**
   * Open a TidesDB database with the given configuration.
   */
  static open(config: Config): TidesDB {
    const defaults = defaultConfig();
    const mergedConfig = { ...defaults, ...config };

    const cConfig = {
      db_path: mergedConfig.dbPath,
      num_flush_threads: mergedConfig.numFlushThreads!,
      num_compaction_threads: mergedConfig.numCompactionThreads!,
      log_level: mergedConfig.logLevel!,
      block_cache_size: mergedConfig.blockCacheSize!,
      max_open_sstables: mergedConfig.maxOpenSSTables!,
      log_to_file: mergedConfig.logToFile ? 1 : 0,
      log_truncation_at: mergedConfig.logTruncationAt!,
    };

    const dbPtrOut: unknown[] = [null];
    const result = tidesdb_open(cConfig, dbPtrOut);
    checkResult(result, 'failed to open database');

    return new TidesDB(dbPtrOut[0]);
  }

  /**
   * Close the database.
   */
  close(): void {
    if (this._db) {
      const result = tidesdb_close(this._db);
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

    // Build the comparator_name as an array of char codes
    const comparatorNameArr = new Array(64).fill(0);
    if (mergedConfig.comparatorName) {
      const nameBytes = Buffer.from(mergedConfig.comparatorName, 'utf8');
      for (let i = 0; i < Math.min(nameBytes.length, 63); i++) {
        comparatorNameArr[i] = nameBytes[i];
      }
    }

    const comparatorCtxArr = new Array(256).fill(0);

    const cConfig = {
      write_buffer_size: mergedConfig.writeBufferSize!,
      level_size_ratio: mergedConfig.levelSizeRatio!,
      min_levels: mergedConfig.minLevels!,
      dividing_level_offset: mergedConfig.dividingLevelOffset!,
      klog_value_threshold: mergedConfig.klogValueThreshold!,
      compression_algorithm: mergedConfig.compressionAlgorithm!,
      enable_bloom_filter: mergedConfig.enableBloomFilter ? 1 : 0,
      bloom_fpr: mergedConfig.bloomFpr!,
      enable_block_indexes: mergedConfig.enableBlockIndexes ? 1 : 0,
      index_sample_ratio: mergedConfig.indexSampleRatio!,
      block_index_prefix_len: mergedConfig.blockIndexPrefixLen!,
      sync_mode: mergedConfig.syncMode!,
      sync_interval_us: mergedConfig.syncIntervalUs!,
      comparator_name: comparatorNameArr,
      comparator_ctx_str: comparatorCtxArr,
      comparator_fn_cached: null,
      comparator_ctx_cached: null,
      skip_list_max_level: mergedConfig.skipListMaxLevel!,
      skip_list_probability: mergedConfig.skipListProbability!,
      default_isolation_level: mergedConfig.defaultIsolationLevel!,
      min_disk_space: mergedConfig.minDiskSpace!,
      l1_file_count_trigger: mergedConfig.l1FileCountTrigger!,
      l0_queue_stall_threshold: mergedConfig.l0QueueStallThreshold!,
      use_btree: mergedConfig.useBtree ? 1 : 0,
    };

    const result = tidesdb_create_column_family(this._db, name, cConfig);
    checkResult(result, 'failed to create column family');
  }

  /**
   * Drop a column family and all associated data.
   */
  dropColumnFamily(name: string): void {
    if (!this._db) throw new Error('Database has been closed');

    const result = tidesdb_drop_column_family(this._db, name);
    checkResult(result, 'failed to drop column family');
  }

  /**
   * Rename a column family atomically.
   * Waits for any in-progress flush/compaction to complete before renaming.
   * @param oldName Current name of the column family.
   * @param newName New name for the column family.
   */
  renameColumnFamily(oldName: string, newName: string): void {
    if (!this._db) throw new Error('Database has been closed');

    const result = tidesdb_rename_column_family(this._db, oldName, newName);
    checkResult(result, 'failed to rename column family');
  }

  /**
   * Clone a column family with all its data to a new name.
   * The clone is completely independent of the source.
   * @param sourceName Name of the source column family.
   * @param destName Name for the cloned column family.
   */
  cloneColumnFamily(sourceName: string, destName: string): void {
    if (!this._db) throw new Error('Database has been closed');

    const result = tidesdb_clone_column_family(this._db, sourceName, destName);
    checkResult(result, 'failed to clone column family');
  }

  /**
   * Get a column family by name.
   */
  getColumnFamily(name: string): ColumnFamily {
    if (!this._db) throw new Error('Database has been closed');

    const cfPtr = tidesdb_get_column_family(this._db, name);

    if (!cfPtr) {
      throw new TidesDBError(ErrorCode.ErrNotFound, `column family not found: ${name}`);
    }

    return new ColumnFamily(cfPtr, name);
  }

  /**
   * List all column families in the database.
   */
  listColumnFamilies(): string[] {
    if (!this._db) throw new Error('Database has been closed');

    const namesPtrOut: unknown[] = [null];
    const countOut: number[] = [0];

    const result = tidesdb_list_column_families(this._db, namesPtrOut, countOut);
    checkResult(result, 'failed to list column families');

    const count = countOut[0];
    if (count === 0 || !namesPtrOut[0]) {
      return [];
    }

    // Decode the char** array using koffi
    const names: string[] = [];
    try {
      // Read array of char* pointers
      const ptrArray = koffi.decode(namesPtrOut[0], 'char *', count) as unknown[];
      for (let i = 0; i < count; i++) {
        if (ptrArray[i]) {
          // Decode each char* to string
          names.push(koffi.decode(ptrArray[i], 'char', 256) as string);
        }
      }
    } catch {
      // If decoding fails, return empty array
      return [];
    }

    return names;
  }

  /**
   * Begin a new transaction with default isolation level.
   */
  beginTransaction(): Transaction {
    if (!this._db) throw new Error('Database has been closed');

    const txnPtrOut: unknown[] = [null];
    const result = tidesdb_txn_begin(this._db, txnPtrOut);
    checkResult(result, 'failed to begin transaction');

    return new Transaction(txnPtrOut[0]);
  }

  /**
   * Begin a new transaction with the specified isolation level.
   */
  beginTransactionWithIsolation(isolation: IsolationLevel): Transaction {
    if (!this._db) throw new Error('Database has been closed');

    const txnPtrOut: unknown[] = [null];
    const result = tidesdb_txn_begin_with_isolation(this._db, isolation, txnPtrOut);
    checkResult(result, 'failed to begin transaction with isolation');

    return new Transaction(txnPtrOut[0]);
  }

  /**
   * Register a custom comparator with the database.
   */
  registerComparator(name: string, ctxStr: string = ''): void {
    if (!this._db) throw new Error('Database has been closed');

    const result = tidesdb_register_comparator(this._db, name, null, ctxStr || null, null);
    checkResult(result, 'failed to register comparator');
  }

  /**
   * Create an on-disk backup of the database without blocking reads/writes.
   * @param dir Backup directory path (must be non-existent or empty).
   */
  backup(dir: string): void {
    if (!this._db) throw new Error('Database has been closed');

    const result = tidesdb_backup(this._db, dir);
    checkResult(result, 'failed to create backup');
  }

  /**
   * Create a lightweight, near-instant snapshot of the database using hard links.
   * Much faster than backup() as it uses hard links instead of copying SSTable data.
   * @param dir Checkpoint directory path (must be non-existent or empty, same filesystem).
   */
  checkpoint(dir: string): void {
    if (!this._db) throw new Error('Database has been closed');

    const result = tidesdb_checkpoint(this._db, dir);
    checkResult(result, 'failed to create checkpoint');
  }

  /**
   * Get statistics about the block cache.
   */
  getCacheStats(): CacheStats {
    if (!this._db) throw new Error('Database has been closed');

    const cStats = {
      enabled: 0,
      total_entries: 0,
      total_bytes: 0,
      hits: 0,
      misses: 0,
      hit_rate: 0.0,
      num_partitions: 0,
    };

    const result = tidesdb_get_cache_stats(this._db, cStats);
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
