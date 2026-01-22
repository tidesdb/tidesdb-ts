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

import { lib, ref, StatsStructPtrPtr } from './ffi';
import { checkResult } from './error';
import { Stats, ColumnFamilyConfig, CompressionAlgorithm, SyncMode, IsolationLevel } from './types';

/**
 * Represents a column family in TidesDB.
 */
export class ColumnFamily {
  private _cf: Buffer;
  private _name: string;

  constructor(cf: Buffer, name: string) {
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
  get ptr(): Buffer {
    return this._cf;
  }

  /**
   * Get statistics about the column family.
   */
  getStats(): Stats {
    const statsPtrPtr = ref.alloc(StatsStructPtrPtr);

    const result = lib.tidesdb_get_stats(this._cf, statsPtrPtr);
    checkResult(result, 'failed to get stats');

    const statsPtr = statsPtrPtr.deref();
    const stats = statsPtr.deref();

    const numLevels = stats.num_levels as number;
    const memtableSize = stats.memtable_size as number;

    const levelSizes: number[] = [];
    const levelNumSSTables: number[] = [];

    if (numLevels > 0 && !ref.isNull(stats.level_sizes)) {
      const sizesPtr = stats.level_sizes;
      for (let i = 0; i < numLevels; i++) {
        levelSizes.push(ref.get(sizesPtr, i * ref.sizeof.size_t) as number);
      }
    }

    if (numLevels > 0 && !ref.isNull(stats.level_num_sstables)) {
      const numPtr = stats.level_num_sstables;
      for (let i = 0; i < numLevels; i++) {
        levelNumSSTables.push(ref.get(numPtr, i * ref.sizeof.int) as number);
      }
    }

    let config: ColumnFamilyConfig | undefined;
    if (!ref.isNull(stats.config)) {
      const cfgPtr = stats.config;
      const cfg = cfgPtr.deref();
      config = {
        writeBufferSize: cfg.write_buffer_size as number,
        levelSizeRatio: cfg.level_size_ratio as number,
        minLevels: cfg.min_levels as number,
        dividingLevelOffset: cfg.dividing_level_offset as number,
        klogValueThreshold: cfg.klog_value_threshold as number,
        compressionAlgorithm: cfg.compression_algo as CompressionAlgorithm,
        enableBloomFilter: cfg.enable_bloom_filter !== 0,
        bloomFpr: cfg.bloom_fpr as number,
        enableBlockIndexes: cfg.enable_block_indexes !== 0,
        indexSampleRatio: cfg.index_sample_ratio as number,
        blockIndexPrefixLen: cfg.block_index_prefix_len as number,
        syncMode: cfg.sync_mode as SyncMode,
        syncIntervalUs: cfg.sync_interval_us as number,
        skipListMaxLevel: cfg.skip_list_max_level as number,
        skipListProbability: cfg.skip_list_probability as number,
        defaultIsolationLevel: cfg.default_isolation_level as IsolationLevel,
        minDiskSpace: cfg.min_disk_space as number,
        l1FileCountTrigger: cfg.l1_file_count_trigger as number,
        l0QueueStallThreshold: cfg.l0_queue_stall_threshold as number,
      };
    }

    lib.tidesdb_free_stats(statsPtr);

    return {
      numLevels,
      memtableSize,
      levelSizes,
      levelNumSSTables,
      config,
    };
  }

  /**
   * Manually trigger compaction for the column family.
   */
  compact(): void {
    const result = lib.tidesdb_compact(this._cf);
    checkResult(result, 'failed to compact column family');
  }

  /**
   * Manually trigger memtable flush for the column family.
   */
  flushMemtable(): void {
    const result = lib.tidesdb_flush_memtable(this._cf);
    checkResult(result, 'failed to flush memtable');
  }
}
