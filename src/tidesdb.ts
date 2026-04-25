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
  tidesdb_get_comparator,
  tidesdb_get_cache_stats,
  tidesdb_backup,
  tidesdb_checkpoint,
  tidesdb_purge,
  tidesdb_get_db_stats,
  tidesdb_delete_column_family,
  tidesdb_promote_to_primary,
  tidesdb_objstore_default_config,
  tidesdb_objstore_fs_create,
} from "./ffi";
import { checkResult, TidesDBError } from "./error";
import { ColumnFamily } from "./column-family";
import { Transaction } from "./transaction";
import {
  Config,
  ColumnFamilyConfig,
  ObjectStoreConfig,
  CacheStats,
  DbStats,
  LogLevel,
  IsolationLevel,
  CompressionAlgorithm,
  SyncMode,
  ErrorCode,
} from "./types";

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
    maxMemoryUsage: 0,
    logToFile: false,
    logTruncationAt: 24 * 1024 * 1024,
    unifiedMemtable: false,
    unifiedMemtableWriteBufferSize: 0,
    unifiedMemtableSkipListMaxLevel: 0,
    unifiedMemtableSkipListProbability: 0,
    unifiedMemtableSyncMode: SyncMode.None,
    unifiedMemtableSyncIntervalUs: 0,
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
    objectLazyCompaction: cConfig.object_lazy_compaction !== 0,
    objectPrefetchCompaction: cConfig.object_prefetch_compaction !== 0,
  };
}

/**
 * Default object store configuration.
 */
export function defaultObjectStoreConfig(): ObjectStoreConfig {
  const cConfig = tidesdb_objstore_default_config();
  return {
    localCachePath: null,
    localCacheMaxBytes: cConfig.local_cache_max_bytes as number,
    cacheOnRead: cConfig.cache_on_read !== 0,
    cacheOnWrite: cConfig.cache_on_write !== 0,
    maxConcurrentUploads: cConfig.max_concurrent_uploads as number,
    maxConcurrentDownloads: cConfig.max_concurrent_downloads as number,
    multipartThreshold: cConfig.multipart_threshold as number,
    multipartPartSize: cConfig.multipart_part_size as number,
    syncManifestToObject: cConfig.sync_manifest_to_object !== 0,
    replicateWal: cConfig.replicate_wal !== 0,
    walUploadSync: cConfig.wal_upload_sync !== 0,
    walSyncThresholdBytes: cConfig.wal_sync_threshold_bytes as number,
    walSyncOnCommit: cConfig.wal_sync_on_commit !== 0,
    replicaMode: cConfig.replica_mode !== 0,
    replicaSyncIntervalUs: cConfig.replica_sync_interval_us as number,
    replicaReplayWal: cConfig.replica_replay_wal !== 0,
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

    // Create object store connector and config if requested
    let objStorePtr: unknown = null;
    let objStoreCfg: Record<string, unknown> | null = null;

    if (mergedConfig.objectStoreFsPath) {
      objStorePtr = tidesdb_objstore_fs_create(mergedConfig.objectStoreFsPath);
      if (!objStorePtr) {
        throw new Error("failed to create filesystem object store connector");
      }

      const osDefaults = defaultObjectStoreConfig();
      const osConfig = { ...osDefaults, ...mergedConfig.objectStoreConfig };

      objStoreCfg = {
        local_cache_path: osConfig.localCachePath ?? null,
        local_cache_max_bytes: osConfig.localCacheMaxBytes ?? 0,
        cache_on_read: osConfig.cacheOnRead === false ? 0 : 1,
        cache_on_write: osConfig.cacheOnWrite === false ? 0 : 1,
        max_concurrent_uploads: osConfig.maxConcurrentUploads ?? 4,
        max_concurrent_downloads: osConfig.maxConcurrentDownloads ?? 8,
        multipart_threshold: osConfig.multipartThreshold ?? 67108864,
        multipart_part_size: osConfig.multipartPartSize ?? 8388608,
        sync_manifest_to_object: osConfig.syncManifestToObject === false ? 0 : 1,
        replicate_wal: osConfig.replicateWal === false ? 0 : 1,
        wal_upload_sync: osConfig.walUploadSync ? 1 : 0,
        wal_sync_threshold_bytes: osConfig.walSyncThresholdBytes ?? 1048576,
        wal_sync_on_commit: osConfig.walSyncOnCommit ? 1 : 0,
        replica_mode: osConfig.replicaMode ? 1 : 0,
        replica_sync_interval_us: osConfig.replicaSyncIntervalUs ?? 5000000,
        replica_replay_wal: osConfig.replicaReplayWal === false ? 0 : 1,
      };
    }

    const cConfig = {
      db_path: mergedConfig.dbPath,
      num_flush_threads: mergedConfig.numFlushThreads!,
      num_compaction_threads: mergedConfig.numCompactionThreads!,
      log_level: mergedConfig.logLevel!,
      block_cache_size: mergedConfig.blockCacheSize!,
      max_open_sstables: mergedConfig.maxOpenSSTables!,
      log_to_file: mergedConfig.logToFile ? 1 : 0,
      log_truncation_at: mergedConfig.logTruncationAt!,
      max_memory_usage: mergedConfig.maxMemoryUsage!,
      unified_memtable: mergedConfig.unifiedMemtable ? 1 : 0,
      unified_memtable_write_buffer_size: mergedConfig.unifiedMemtableWriteBufferSize!,
      unified_memtable_skip_list_max_level: mergedConfig.unifiedMemtableSkipListMaxLevel!,
      unified_memtable_skip_list_probability: mergedConfig.unifiedMemtableSkipListProbability!,
      unified_memtable_sync_mode: mergedConfig.unifiedMemtableSyncMode!,
      unified_memtable_sync_interval_us: mergedConfig.unifiedMemtableSyncIntervalUs!,
      object_store: objStorePtr,
      object_store_config: objStoreCfg,
    };

    const dbPtrOut: unknown[] = [null];
    const result = tidesdb_open(cConfig, dbPtrOut);
    checkResult(result, "failed to open database");

    return new TidesDB(dbPtrOut[0]);
  }

  /**
   * Close the database.
   */
  close(): void {
    if (this._db) {
      const result = tidesdb_close(this._db);
      this._db = null;
      checkResult(result, "failed to close database");
    }
  }

  /**
   * Create a new column family with the given configuration.
   */
  createColumnFamily(name: string, config: ColumnFamilyConfig = {}): void {
    if (!this._db) throw new Error("Database has been closed");

    const defaults = defaultColumnFamilyConfig();
    const mergedConfig = { ...defaults, ...config };

    // Build the comparator_name as an array of char codes
    const comparatorNameArr = new Array(64).fill(0);
    if (mergedConfig.comparatorName) {
      const nameBytes = Buffer.from(mergedConfig.comparatorName, "utf8");
      for (let i = 0; i < Math.min(nameBytes.length, 63); i++) {
        comparatorNameArr[i] = nameBytes[i];
      }
    }

    const comparatorCtxArr = new Array(256).fill(0);

    // Build the name as an array of char codes (128 bytes)
    const nameArr = new Array(128).fill(0);
    const cfNameBytes = Buffer.from(name, "utf8");
    for (let i = 0; i < Math.min(cfNameBytes.length, 127); i++) {
      nameArr[i] = cfNameBytes[i];
    }

    const cConfig = {
      name: nameArr,
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
      commit_hook_fn: null,
      commit_hook_ctx: null,
      object_target_file_size: 0,
      object_lazy_compaction: mergedConfig.objectLazyCompaction ? 1 : 0,
      object_prefetch_compaction: mergedConfig.objectPrefetchCompaction === false ? 0 : 1,
    };

    const result = tidesdb_create_column_family(this._db, name, cConfig);
    checkResult(result, "failed to create column family");
  }

  /**
   * Drop a column family and all associated data.
   */
  dropColumnFamily(name: string): void {
    if (!this._db) throw new Error("Database has been closed");

    const result = tidesdb_drop_column_family(this._db, name);
    checkResult(result, "failed to drop column family");
  }

  /**
   * Delete a column family by handle.
   * @param cf Column family handle to delete.
   */
  deleteColumnFamily(cf: ColumnFamily): void {
    if (!this._db) throw new Error("Database has been closed");

    const result = tidesdb_delete_column_family(this._db, cf.ptr);
    checkResult(result, "failed to delete column family");
  }

  /**
   * Rename a column family atomically.
   * Waits for any in-progress flush/compaction to complete before renaming.
   * @param oldName Current name of the column family.
   * @param newName New name for the column family.
   */
  renameColumnFamily(oldName: string, newName: string): void {
    if (!this._db) throw new Error("Database has been closed");

    const result = tidesdb_rename_column_family(this._db, oldName, newName);
    checkResult(result, "failed to rename column family");
  }

  /**
   * Clone a column family with all its data to a new name.
   * The clone is completely independent of the source.
   * @param sourceName Name of the source column family.
   * @param destName Name for the cloned column family.
   */
  cloneColumnFamily(sourceName: string, destName: string): void {
    if (!this._db) throw new Error("Database has been closed");

    const result = tidesdb_clone_column_family(this._db, sourceName, destName);
    checkResult(result, "failed to clone column family");
  }

  /**
   * Get a column family by name.
   */
  getColumnFamily(name: string): ColumnFamily {
    if (!this._db) throw new Error("Database has been closed");

    const cfPtr = tidesdb_get_column_family(this._db, name);

    if (!cfPtr) {
      throw new TidesDBError(
        ErrorCode.ErrNotFound,
        `column family not found: ${name}`,
      );
    }

    return new ColumnFamily(cfPtr, name);
  }

  /**
   * List all column families in the database.
   */
  listColumnFamilies(): string[] {
    if (!this._db) throw new Error("Database has been closed");

    const namesPtrOut: unknown[] = [null];
    const countOut: number[] = [0];

    const result = tidesdb_list_column_families(
      this._db,
      namesPtrOut,
      countOut,
    );
    checkResult(result, "failed to list column families");

    const count = countOut[0];
    if (count === 0 || !namesPtrOut[0]) {
      return [];
    }

    // Decode the char** array using koffi
    const names: string[] = [];
    try {
      // Read array of char* pointers
      const ptrArray = koffi.decode(
        namesPtrOut[0],
        "char *",
        count,
      ) as unknown[];
      for (let i = 0; i < count; i++) {
        if (ptrArray[i]) {
          // Decode each char* to string
          names.push(koffi.decode(ptrArray[i], "char", 256) as string);
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
    if (!this._db) throw new Error("Database has been closed");

    const txnPtrOut: unknown[] = [null];
    const result = tidesdb_txn_begin(this._db, txnPtrOut);
    checkResult(result, "failed to begin transaction");

    return new Transaction(txnPtrOut[0]);
  }

  /**
   * Begin a new transaction with the specified isolation level.
   */
  beginTransactionWithIsolation(isolation: IsolationLevel): Transaction {
    if (!this._db) throw new Error("Database has been closed");

    const txnPtrOut: unknown[] = [null];
    const result = tidesdb_txn_begin_with_isolation(
      this._db,
      isolation,
      txnPtrOut,
    );
    checkResult(result, "failed to begin transaction with isolation");

    return new Transaction(txnPtrOut[0]);
  }

  /**
   * Register a custom comparator with the database.
   */
  registerComparator(name: string, ctxStr: string = ""): void {
    if (!this._db) throw new Error("Database has been closed");

    const result = tidesdb_register_comparator(
      this._db,
      name,
      null,
      ctxStr || null,
      null,
    );
    checkResult(result, "failed to register comparator");
  }

  /**
   * Retrieve a registered comparator by name.
   * Returns true if the comparator is registered, false otherwise.
   * @param name Name of the comparator to look up.
   */
  getComparator(name: string): boolean {
    if (!this._db) throw new Error("Database has been closed");

    const fnPtrOut: unknown[] = [null];
    const ctxPtrOut: unknown[] = [null];

    const result = tidesdb_get_comparator(this._db, name, fnPtrOut, ctxPtrOut);
    return result === 0;
  }

  /**
   * Create an on-disk backup of the database without blocking reads/writes.
   * @param dir Backup directory path (must be non-existent or empty).
   */
  backup(dir: string): void {
    if (!this._db) throw new Error("Database has been closed");

    const result = tidesdb_backup(this._db, dir);
    checkResult(result, "failed to create backup");
  }

  /**
   * Create a lightweight, near-instant snapshot of the database using hard links.
   * Much faster than backup() as it uses hard links instead of copying SSTable data.
   * @param dir Checkpoint directory path (must be non-existent or empty, same filesystem).
   */
  checkpoint(dir: string): void {
    if (!this._db) throw new Error("Database has been closed");

    const result = tidesdb_checkpoint(this._db, dir);
    checkResult(result, "failed to create checkpoint");
  }

  /**
   * Purge the entire database by flushing all column families and
   * synchronously draining flush/compaction work.
   */
  purge(): void {
    if (!this._db) throw new Error("Database has been closed");

    const result = tidesdb_purge(this._db);
    checkResult(result, "failed to purge database");
  }

  /**
   * Promote a read-only replica to primary mode.
   * Only valid when the database was opened in replica mode.
   */
  promoteToPrimary(): void {
    if (!this._db) throw new Error("Database has been closed");

    const result = tidesdb_promote_to_primary(this._db);
    checkResult(result, "failed to promote to primary");
  }

  /**
   * Get aggregate statistics across the entire database instance.
   * The returned struct is stack-allocated; no free is needed.
   */
  getDbStats(): DbStats {
    if (!this._db) throw new Error("Database has been closed");

    const cStats = {
      num_column_families: 0,
      total_memory: 0,
      available_memory: 0,
      resolved_memory_limit: 0,
      memory_pressure_level: 0,
      flush_pending_count: 0,
      total_memtable_bytes: 0,
      total_immutable_count: 0,
      total_sstable_count: 0,
      total_data_size_bytes: 0,
      num_open_sstables: 0,
      global_seq: 0,
      txn_memory_bytes: 0,
      compaction_queue_size: 0,
      flush_queue_size: 0,
      unified_memtable_enabled: 0,
      unified_memtable_bytes: 0,
      unified_immutable_count: 0,
      unified_is_flushing: 0,
      unified_next_cf_index: 0,
      unified_wal_generation: 0,
      object_store_enabled: 0,
      object_store_connector: null as unknown,
      local_cache_bytes_used: 0,
      local_cache_bytes_max: 0,
      local_cache_num_files: 0,
      last_uploaded_generation: 0,
      upload_queue_depth: 0,
      total_uploads: 0,
      total_upload_failures: 0,
      replica_mode: 0,
    };

    const result = tidesdb_get_db_stats(this._db, cStats);
    checkResult(result, "failed to get database stats");

    // Decode connector string if present
    let connectorStr = "";
    if (cStats.object_store_connector) {
      try {
        connectorStr = koffi.decode(cStats.object_store_connector, "char", 64) as string;
      } catch {
        connectorStr = "";
      }
    }

    return {
      numColumnFamilies: cStats.num_column_families as number,
      totalMemory: cStats.total_memory as number,
      availableMemory: cStats.available_memory as number,
      resolvedMemoryLimit: cStats.resolved_memory_limit as number,
      memoryPressureLevel: cStats.memory_pressure_level as number,
      flushPendingCount: cStats.flush_pending_count as number,
      totalMemtableBytes: cStats.total_memtable_bytes as number,
      totalImmutableCount: cStats.total_immutable_count as number,
      totalSstableCount: cStats.total_sstable_count as number,
      totalDataSizeBytes: cStats.total_data_size_bytes as number,
      numOpenSstables: cStats.num_open_sstables as number,
      globalSeq: cStats.global_seq as number,
      txnMemoryBytes: cStats.txn_memory_bytes as number,
      compactionQueueSize: cStats.compaction_queue_size as number,
      flushQueueSize: cStats.flush_queue_size as number,
      unifiedMemtableEnabled: cStats.unified_memtable_enabled !== 0,
      unifiedMemtableBytes: cStats.unified_memtable_bytes as number,
      unifiedImmutableCount: cStats.unified_immutable_count as number,
      unifiedIsFlushing: cStats.unified_is_flushing !== 0,
      unifiedNextCfIndex: cStats.unified_next_cf_index as number,
      unifiedWalGeneration: cStats.unified_wal_generation as number,
      objectStoreEnabled: cStats.object_store_enabled !== 0,
      objectStoreConnector: connectorStr,
      localCacheBytesUsed: cStats.local_cache_bytes_used as number,
      localCacheBytesMax: cStats.local_cache_bytes_max as number,
      localCacheNumFiles: cStats.local_cache_num_files as number,
      lastUploadedGeneration: cStats.last_uploaded_generation as number,
      uploadQueueDepth: cStats.upload_queue_depth as number,
      totalUploads: cStats.total_uploads as number,
      totalUploadFailures: cStats.total_upload_failures as number,
      replicaMode: cStats.replica_mode !== 0,
    };
  }

  /**
   * Get statistics about the block cache.
   */
  getCacheStats(): CacheStats {
    if (!this._db) throw new Error("Database has been closed");

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
    checkResult(result, "failed to get cache stats");

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
