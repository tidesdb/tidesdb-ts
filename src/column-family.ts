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
  tidesdb_flush_memtable,
  tidesdb_is_flushing,
  tidesdb_is_compacting,
  tidesdb_cf_update_runtime_config,
  tidesdb_range_cost,
  StatsStruct,
} from './ffi';
import { checkResult } from './error';
import { Stats, ColumnFamilyConfig, CompressionAlgorithm, SyncMode, IsolationLevel } from './types';

// Opaque pointer type for column family
type CFPtr = unknown;

/**
 * Represents a column family in TidesDB.
 */
export class ColumnFamily {
  private _cf: CFPtr;
  private _name: string;

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
    checkResult(result, 'failed to get stats');

    const statsPtr = statsPtrOut[0];
    if (!statsPtr) {
      throw new Error('failed to get stats: null pointer returned');
    }

    // Decode the stats struct from the pointer
    const decoded = koffi.decode(statsPtr, StatsStruct) as Record<string, unknown>;

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

    // Parse level arrays
    const levelSizes: number[] = [];
    const levelNumSSTables: number[] = [];
    const levelKeyCounts: number[] = [];

    if (numLevels > 0) {
      try {
        if (decoded.level_sizes) {
          const sizes = koffi.decode(decoded.level_sizes, 'size_t', numLevels) as number[];
          levelSizes.push(...sizes);
        }
        if (decoded.level_num_sstables) {
          const counts = koffi.decode(decoded.level_num_sstables, 'int', numLevels) as number[];
          levelNumSSTables.push(...counts);
        }
        if (decoded.level_key_counts) {
          const keyCounts = koffi.decode(decoded.level_key_counts, 'uint64_t', numLevels) as number[];
          levelKeyCounts.push(...keyCounts);
        }
      } catch {
        // If decoding fails, arrays remain empty
      }
    }

    tidesdb_free_stats(statsPtr);

    return {
      numLevels,
      memtableSize,
      levelSizes,
      levelNumSSTables,
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
    };
  }

  /**
   * Manually trigger compaction for the column family.
   */
  compact(): void {
    const result = tidesdb_compact(this._cf);
    checkResult(result, 'failed to compact column family');
  }

  /**
   * Manually trigger memtable flush for the column family.
   */
  flushMemtable(): void {
    const result = tidesdb_flush_memtable(this._cf);
    checkResult(result, 'failed to flush memtable');
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
   * Update runtime-safe configuration settings.
   * Changes apply to new operations only.
   * @param config New configuration values.
   * @param persistToDisk If true, save changes to config.ini.
   */
  updateRuntimeConfig(config: ColumnFamilyConfig, persistToDisk: boolean = false): void {
    // Build the comparator_name as an array of char codes
    const comparatorNameArr = new Array(64).fill(0);
    if (config.comparatorName) {
      const nameBytes = Buffer.from(config.comparatorName, 'utf8');
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
      compression_algorithm: config.compressionAlgorithm ?? CompressionAlgorithm.Lz4Compression,
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
      default_isolation_level: config.defaultIsolationLevel ?? IsolationLevel.ReadCommitted,
      min_disk_space: config.minDiskSpace ?? 100 * 1024 * 1024,
      l1_file_count_trigger: config.l1FileCountTrigger ?? 4,
      l0_queue_stall_threshold: config.l0QueueStallThreshold ?? 20,
      use_btree: config.useBtree ? 1 : 0,
    };

    const result = tidesdb_cf_update_runtime_config(this._cf, cConfig, persistToDisk ? 1 : 0);
    checkResult(result, 'failed to update runtime config');
  }

  /**
   * Estimate the computational cost of iterating between two keys.
   * The returned value is an opaque double — meaningful only for comparison
   * with other values from the same function. Uses only in-memory metadata
   * and performs no disk I/O.
   * @param keyA First key (bound of range).
   * @param keyB Second key (bound of range).
   * @returns Estimated traversal cost (higher = more expensive).
   */
  rangeCost(keyA: Buffer, keyB: Buffer): number {
    const costOut: number[] = [0.0];

    const result = tidesdb_range_cost(
      this._cf,
      keyA,
      keyA.length,
      keyB,
      keyB.length,
      costOut
    );
    checkResult(result, 'failed to estimate range cost');

    return costOut[0];
  }
}
