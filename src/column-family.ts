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
  tidesdb_get_stats,
  tidesdb_free_stats,
  tidesdb_compact,
  tidesdb_compact_range,
  tidesdb_flush_memtable,
  tidesdb_is_flushing,
  tidesdb_is_compacting,
  tidesdb_purge_cf,
  tidesdb_cf_update_runtime_config,
  tidesdb_range_cost,
  tidesdb_cf_set_commit_hook,
  tidesdb_sync_wal,
  CommitOpStruct,
  commitHookPtrType,
  StatsStruct,
  ColumnFamilyConfigStruct,
} from "./ffi";
import { checkResult } from "./error";
import {
  Stats,
  ColumnFamilyConfig,
  CompressionAlgorithm,
  SyncMode,
  IsolationLevel,
  CommitOp,
  CommitHookCallback,
} from "./types";

// Opaque pointer type for column family
type CFPtr = unknown;

/**
 * Represents a column family in TidesDB.
 */
export class ColumnFamily {
  private _cf: CFPtr;
  private _name: string;
  private _commitHookCb: unknown | null = null;

  constructor(cf: CFPtr, name: string) {
    this._cf = cf;
    this._name = name;
  }

  /**
   * Get the name of the column family.
   */
  get name(): string {
    return this._name;
  }

  /**
   * Get the internal pointer (for internal use).
   */
  get ptr(): CFPtr {
    return this._cf;
  }

  /**
   * Get statistics about the column family.
   */
  getStats(): Stats {
    const statsPtrOut: unknown[] = [null];

    const result = tidesdb_get_stats(this._cf, statsPtrOut);
    checkResult(result, "failed to get stats");

    const statsPtr = statsPtrOut[0];
    if (!statsPtr) {
      throw new Error("failed to get stats: null pointer returned");
    }

    // Decode the stats struct from the pointer
    const decoded = koffi.decode(statsPtr, StatsStruct) as Record<
      string,
      unknown
    >;

    const numLevels = (decoded.num_levels ?? 0) as number;
    const memtableSize = (decoded.memtable_size ?? 0) as number;
    const totalKeys = (decoded.total_keys ?? 0) as number;
    const totalDataSize = (decoded.total_data_size ?? 0) as number;
    const avgKeySize = (decoded.avg_key_size ?? 0) as number;
    const avgValueSize = (decoded.avg_value_size ?? 0) as number;
    const readAmp = (decoded.read_amp ?? 0) as number;
    const hitRate = (decoded.hit_rate ?? 0) as number;
    const useBtree = ((decoded.use_btree ?? 0) as number) !== 0;
    const btreeTotalNodes = (decoded.btree_total_nodes ?? 0) as number;
    const btreeMaxHeight = (decoded.btree_max_height ?? 0) as number;
    const btreeAvgHeight = (decoded.btree_avg_height ?? 0) as number;
    const totalTombstones = (decoded.total_tombstones ?? 0) as number;
    const tombstoneRatio = (decoded.tombstone_ratio ?? 0) as number;
    const maxSstDensity = (decoded.max_sst_density ?? 0) as number;
    const maxSstDensityLevel = (decoded.max_sst_density_level ?? 0) as number;

    // Parse level arrays
    const levelSizes: number[] = [];
    const levelNumSSTables: number[] = [];
    const levelKeyCounts: number[] = [];
    const levelTombstoneCounts: number[] = [];

    if (numLevels > 0) {
      try {
        if (decoded.level_sizes) {
          const sizes = koffi.decode(
            decoded.level_sizes,
            "size_t",
            numLevels,
          ) as number[];
          levelSizes.push(...sizes);
        }
        if (decoded.level_num_sstables) {
          const counts = koffi.decode(
            decoded.level_num_sstables,
            "int",
            numLevels,
          ) as number[];
          levelNumSSTables.push(...counts);
        }
        if (decoded.level_key_counts) {
          const keyCounts = koffi.decode(
            decoded.level_key_counts,
            "uint64_t",
            numLevels,
          ) as number[];
          levelKeyCounts.push(...keyCounts);
        }
        if (decoded.level_tombstone_counts) {
          const tombstoneCounts = koffi.decode(
            decoded.level_tombstone_counts,
            "uint64_t",
            numLevels,
          ) as number[];
          levelTombstoneCounts.push(...tombstoneCounts);
        }
      } catch {
        // If decoding fails, arrays remain empty
      }
    }

    // Decode config from the stats struct
    let config: ColumnFamilyConfig | undefined;
    if (decoded.config) {
      try {
        const cfgDecoded = koffi.decode(
          decoded.config,
          ColumnFamilyConfigStruct,
        ) as Record<string, unknown>;
        config = {
          writeBufferSize: cfgDecoded.write_buffer_size as number,
          levelSizeRatio: cfgDecoded.level_size_ratio as number,
          minLevels: cfgDecoded.min_levels as number,
          dividingLevelOffset: cfgDecoded.dividing_level_offset as number,
          klogValueThreshold: cfgDecoded.klog_value_threshold as number,
          compressionAlgorithm:
            cfgDecoded.compression_algorithm as CompressionAlgorithm,
          enableBloomFilter: (cfgDecoded.enable_bloom_filter as number) !== 0,
          bloomFpr: cfgDecoded.bloom_fpr as number,
          enableBlockIndexes: (cfgDecoded.enable_block_indexes as number) !== 0,
          indexSampleRatio: cfgDecoded.index_sample_ratio as number,
          blockIndexPrefixLen: cfgDecoded.block_index_prefix_len as number,
          syncMode: cfgDecoded.sync_mode as SyncMode,
          syncIntervalUs: cfgDecoded.sync_interval_us as number,
          skipListMaxLevel: cfgDecoded.skip_list_max_level as number,
          skipListProbability: cfgDecoded.skip_list_probability as number,
          defaultIsolationLevel:
            cfgDecoded.default_isolation_level as IsolationLevel,
          minDiskSpace: cfgDecoded.min_disk_space as number,
          l1FileCountTrigger: cfgDecoded.l1_file_count_trigger as number,
          l0QueueStallThreshold: cfgDecoded.l0_queue_stall_threshold as number,
          tombstoneDensityTrigger: cfgDecoded.tombstone_density_trigger as number,
          tombstoneDensityMinEntries: cfgDecoded.tombstone_density_min_entries as number,
          useBtree: (cfgDecoded.use_btree as number) !== 0,
          objectLazyCompaction: (cfgDecoded.object_lazy_compaction as number) !== 0,
          objectPrefetchCompaction: (cfgDecoded.object_prefetch_compaction as number) !== 0,
        };
      } catch {
        // If decoding fails, config remains undefined
      }
    }

    tidesdb_free_stats(statsPtr);

    return {
      numLevels,
      memtableSize,
      levelSizes,
      levelNumSSTables,
      config,
      totalKeys,
      totalDataSize,
      avgKeySize,
      avgValueSize,
      levelKeyCounts,
      readAmp,
      hitRate,
      useBtree,
      btreeTotalNodes: useBtree ? btreeTotalNodes : undefined,
      btreeMaxHeight: useBtree ? btreeMaxHeight : undefined,
      btreeAvgHeight: useBtree ? btreeAvgHeight : undefined,
      totalTombstones,
      tombstoneRatio,
      levelTombstoneCounts,
      maxSstDensity,
      maxSstDensityLevel,
    };
  }

  /**
   * Manually trigger compaction for the column family.
   */
  compact(): void {
    const result = tidesdb_compact(this._cf);
    checkResult(result, "failed to compact column family");
  }

  /**
   * Synchronously compact every SSTable whose key range overlaps
   * `[startKey, endKey)`. Output is merged toward the largest level affected.
   *
   * Pass `null` for an unbounded endpoint on that side. Both endpoints
   * `null`/empty is rejected with `ErrInvalidArgs`; for full CF compaction
   * use `compact()`.
   *
   * Blocks the calling thread until the merge commits or fails (does not
   * enqueue onto the compaction thread pool).
   *
   * @param startKey Inclusive start key, or null for unbounded.
   * @param endKey Exclusive end key, or null for unbounded.
   */
  compactRange(startKey: Buffer | null, endKey: Buffer | null): void {
    const startBuf = startKey && startKey.length > 0 ? startKey : null;
    const endBuf = endKey && endKey.length > 0 ? endKey : null;
    const startSize = startBuf ? startBuf.length : 0;
    const endSize = endBuf ? endBuf.length : 0;

    const result = tidesdb_compact_range(
      this._cf,
      startBuf,
      startSize,
      endBuf,
      endSize,
    );
    checkResult(result, "failed to compact range");
  }

  /**
   * Manually trigger memtable flush for the column family.
   */
  flushMemtable(): void {
    const result = tidesdb_flush_memtable(this._cf);
    checkResult(result, "failed to flush memtable");
  }

  /**
   * Check if a flush operation is in progress.
   * @returns true if flushing, false otherwise.
   */
  isFlushing(): boolean {
    return tidesdb_is_flushing(this._cf) !== 0;
  }

  /**
   * Check if a compaction operation is in progress.
   * @returns true if compacting, false otherwise.
   */
  isCompacting(): boolean {
    return tidesdb_is_compacting(this._cf) !== 0;
  }

  /**
   * Purge this column family by flushing and synchronously compacting.
   */
  purgeColumnFamily(): void {
    const result = tidesdb_purge_cf(this._cf);
    checkResult(result, "failed to purge column family");
  }

  /**
   * Update runtime-safe configuration settings.
   * Changes apply to new operations only.
   * @param config New configuration values.
   * @param persistToDisk If true, save changes to config.ini.
   */
  updateRuntimeConfig(
    config: ColumnFamilyConfig,
    persistToDisk: boolean = false,
  ): void {
    // Build the comparator_name as an array of char codes
    const comparatorNameArr = new Array(64).fill(0);
    if (config.comparatorName) {
      const nameBytes = Buffer.from(config.comparatorName, "utf8");
      for (let i = 0; i < Math.min(nameBytes.length, 63); i++) {
        comparatorNameArr[i] = nameBytes[i];
      }
    }

    const comparatorCtxArr = new Array(256).fill(0);

    const cConfig = {
      write_buffer_size: config.writeBufferSize ?? 0,
      level_size_ratio: config.levelSizeRatio ?? 0,
      min_levels: config.minLevels ?? 0,
      dividing_level_offset: config.dividingLevelOffset ?? 0,
      klog_value_threshold: config.klogValueThreshold ?? 0,
      compression_algorithm:
        config.compressionAlgorithm ?? CompressionAlgorithm.Lz4Compression,
      enable_bloom_filter: config.enableBloomFilter ? 1 : 0,
      bloom_fpr: config.bloomFpr ?? 0.01,
      enable_block_indexes: config.enableBlockIndexes ? 1 : 0,
      index_sample_ratio: config.indexSampleRatio ?? 1,
      block_index_prefix_len: config.blockIndexPrefixLen ?? 16,
      sync_mode: config.syncMode ?? SyncMode.Full,
      sync_interval_us: config.syncIntervalUs ?? 128000,
      comparator_name: comparatorNameArr,
      comparator_ctx_str: comparatorCtxArr,
      comparator_fn_cached: null,
      comparator_ctx_cached: null,
      skip_list_max_level: config.skipListMaxLevel ?? 12,
      skip_list_probability: config.skipListProbability ?? 0.25,
      default_isolation_level:
        config.defaultIsolationLevel ?? IsolationLevel.ReadCommitted,
      min_disk_space: config.minDiskSpace ?? 100 * 1024 * 1024,
      l1_file_count_trigger: config.l1FileCountTrigger ?? 4,
      l0_queue_stall_threshold: config.l0QueueStallThreshold ?? 20,
      tombstone_density_trigger: config.tombstoneDensityTrigger ?? 0,
      tombstone_density_min_entries: config.tombstoneDensityMinEntries ?? 0,
      use_btree: config.useBtree ? 1 : 0,
      commit_hook_fn: null,
      commit_hook_ctx: null,
      object_target_file_size: 0,
      object_lazy_compaction: config.objectLazyCompaction ? 1 : 0,
      object_prefetch_compaction: config.objectPrefetchCompaction === false ? 0 : 1,
    };

    const result = tidesdb_cf_update_runtime_config(
      this._cf,
      cConfig,
      persistToDisk ? 1 : 0,
    );
    checkResult(result, "failed to update runtime config");
  }

  /**
   * Set a commit hook callback for this column family.
   * The callback fires synchronously after every transaction commit,
   * receiving the full batch of committed operations atomically.
   * @param callback Function invoked with (ops, commitSeq). Return 0 on success.
   */
  setCommitHook(callback: CommitHookCallback): void {
    // Clear existing hook first
    if (this._commitHookCb) {
      this.clearCommitHook();
    }

    // Create wrapper that decodes C data to TypeScript types
    const wrapper = (
      opsPtr: unknown,
      numOps: number,
      commitSeq: number,
      _ctx: unknown,
    ): number => {
      try {
        const ops: CommitOp[] = [];

        if (numOps > 0 && opsPtr) {
          const rawOps = koffi.decode(opsPtr, CommitOpStruct, numOps) as Array<
            Record<string, unknown>
          >;

          for (const rawOp of rawOps) {
            const keySize = rawOp.key_size as number;
            const valueSize = rawOp.value_size as number;
            const isDelete = (rawOp.is_delete as number) !== 0;

            let key = Buffer.alloc(0);
            if (rawOp.key && keySize > 0) {
              const keyBytes = koffi.decode(
                rawOp.key,
                "uint8_t",
                keySize,
              ) as number[];
              key = Buffer.from(keyBytes);
            }

            let value: Buffer | null = null;
            if (!isDelete && rawOp.value && valueSize > 0) {
              const valueBytes = koffi.decode(
                rawOp.value,
                "uint8_t",
                valueSize,
              ) as number[];
              value = Buffer.from(valueBytes);
            }

            ops.push({
              key,
              value,
              ttl: rawOp.ttl as number,
              isDelete,
            });
          }
        }

        return callback(ops, commitSeq);
      } catch {
        return -1;
      }
    };

    this._commitHookCb = koffi.register(wrapper, commitHookPtrType);

    const result = tidesdb_cf_set_commit_hook(
      this._cf,
      this._commitHookCb,
      null,
    );
    checkResult(result, "failed to set commit hook");
  }

  /**
   * Clear the commit hook for this column family.
   * Disables the callback immediately.
   */
  clearCommitHook(): void {
    const result = tidesdb_cf_set_commit_hook(this._cf, null, null);
    checkResult(result, "failed to clear commit hook");

    if (this._commitHookCb) {
      koffi.unregister(this._commitHookCb as never);
      this._commitHookCb = null;
    }
  }

  /**
   * Force an immediate fsync of the active write-ahead log for this column family.
   * Useful for explicit durability control when using SyncMode.None or SyncMode.Interval.
   */
  syncWal(): void {
    const result = tidesdb_sync_wal(this._cf);
    checkResult(result, "failed to sync WAL");
  }

  rangeCost(keyA: Buffer, keyB: Buffer): number {
    const costOut: number[] = [0.0];

    const result = tidesdb_range_cost(
      this._cf,
      keyA,
      keyA.length,
      keyB,
      keyB.length,
      costOut,
    );
    checkResult(result, "failed to estimate range cost");

    return costOut[0];
  }
}
