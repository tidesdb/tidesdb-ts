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

import ffi from 'ffi-napi';
import ref from 'ref-napi';
import StructDI from 'ref-struct-di';
import ArrayDI from 'ref-array-di';

const Struct = StructDI(ref);
const ArrayType = ArrayDI(ref);

// Pointer types
export const voidPtr = ref.refType(ref.types.void);
export const charPtr = ref.refType(ref.types.char);
export const charPtrPtr = ref.refType(charPtr);
export const uint8Ptr = ref.refType(ref.types.uint8);
export const uint8PtrPtr = ref.refType(uint8Ptr);
export const sizeTPtr = ref.refType(ref.types.size_t);
export const intPtr = ref.refType(ref.types.int);

// Opaque pointer types for TidesDB structures
export const tidesdbPtr = voidPtr;
export const tidesdbPtrPtr = ref.refType(tidesdbPtr);
export const columnFamilyPtr = voidPtr;
export const txnPtr = voidPtr;
export const txnPtrPtr = ref.refType(txnPtr);
export const iterPtr = voidPtr;
export const iterPtrPtr = ref.refType(iterPtr);

// tidesdb_config_t structure
export const TidesDBConfig = Struct({
  db_path: charPtr,
  num_flush_threads: ref.types.int,
  num_compaction_threads: ref.types.int,
  log_level: ref.types.int,
  block_cache_size: ref.types.size_t,
  max_open_sstables: ref.types.size_t,
});

export const TidesDBConfigPtr = ref.refType(TidesDBConfig);

// tidesdb_column_family_config_t structure
export const ColumnFamilyConfig = Struct({
  write_buffer_size: ref.types.size_t,
  level_size_ratio: ref.types.size_t,
  min_levels: ref.types.int,
  dividing_level_offset: ref.types.int,
  klog_value_threshold: ref.types.size_t,
  compression_algo: ref.types.int,
  enable_bloom_filter: ref.types.int,
  bloom_fpr: ref.types.double,
  enable_block_indexes: ref.types.int,
  index_sample_ratio: ref.types.int,
  block_index_prefix_len: ref.types.int,
  sync_mode: ref.types.int,
  sync_interval_us: ref.types.uint64,
  comparator_name: ArrayType(ref.types.char, 64),
  comparator_ctx_str: ArrayType(ref.types.char, 256),
  comparator_fn_cached: voidPtr,
  comparator_ctx_cached: voidPtr,
  skip_list_max_level: ref.types.int,
  skip_list_probability: ref.types.float,
  default_isolation_level: ref.types.int,
  min_disk_space: ref.types.uint64,
  l1_file_count_trigger: ref.types.int,
  l0_queue_stall_threshold: ref.types.int,
});

export const ColumnFamilyConfigPtr = ref.refType(ColumnFamilyConfig);

// tidesdb_cache_stats_t structure
export const CacheStatsStruct = Struct({
  enabled: ref.types.int,
  total_entries: ref.types.size_t,
  total_bytes: ref.types.size_t,
  hits: ref.types.uint64,
  misses: ref.types.uint64,
  hit_rate: ref.types.double,
  num_partitions: ref.types.size_t,
});

export const CacheStatsPtr = ref.refType(CacheStatsStruct);

// tidesdb_stats_t structure
export const StatsStruct = Struct({
  num_levels: ref.types.int,
  memtable_size: ref.types.size_t,
  level_sizes: sizeTPtr,
  level_num_sstables: intPtr,
  config: ColumnFamilyConfigPtr,
});

export const StatsStructPtr = ref.refType(StatsStruct);
export const StatsStructPtrPtr = ref.refType(StatsStructPtr);

// Load the TidesDB library
function loadLibrary(): ffi.Library {
  const libName = process.platform === 'darwin' 
    ? 'libtidesdb' 
    : process.platform === 'win32' 
      ? 'tidesdb' 
      : 'libtidesdb';

  return ffi.Library(libName, {
    // Database operations
    tidesdb_open: [ref.types.int, [TidesDBConfigPtr, tidesdbPtrPtr]],
    tidesdb_close: [ref.types.int, [tidesdbPtr]],

    // Default config
    tidesdb_default_column_family_config: [ColumnFamilyConfig, []],
    tidesdb_default_config: [TidesDBConfig, []],

    // Comparator operations
    tidesdb_register_comparator: [ref.types.int, [tidesdbPtr, charPtr, voidPtr, charPtr, voidPtr]],

    // Column family operations
    tidesdb_create_column_family: [ref.types.int, [tidesdbPtr, charPtr, ColumnFamilyConfigPtr]],
    tidesdb_drop_column_family: [ref.types.int, [tidesdbPtr, charPtr]],
    tidesdb_get_column_family: [columnFamilyPtr, [tidesdbPtr, charPtr]],
    tidesdb_list_column_families: [ref.types.int, [tidesdbPtr, charPtrPtr, intPtr]],

    // Transaction operations
    tidesdb_txn_begin: [ref.types.int, [tidesdbPtr, txnPtrPtr]],
    tidesdb_txn_begin_with_isolation: [ref.types.int, [tidesdbPtr, ref.types.int, txnPtrPtr]],
    tidesdb_txn_put: [ref.types.int, [txnPtr, columnFamilyPtr, uint8Ptr, ref.types.size_t, uint8Ptr, ref.types.size_t, ref.types.int64]],
    tidesdb_txn_get: [ref.types.int, [txnPtr, columnFamilyPtr, uint8Ptr, ref.types.size_t, uint8PtrPtr, sizeTPtr]],
    tidesdb_txn_delete: [ref.types.int, [txnPtr, columnFamilyPtr, uint8Ptr, ref.types.size_t]],
    tidesdb_txn_commit: [ref.types.int, [txnPtr]],
    tidesdb_txn_rollback: [ref.types.int, [txnPtr]],
    tidesdb_txn_free: [ref.types.void, [txnPtr]],

    // Savepoint operations
    tidesdb_txn_savepoint: [ref.types.int, [txnPtr, charPtr]],
    tidesdb_txn_rollback_to_savepoint: [ref.types.int, [txnPtr, charPtr]],
    tidesdb_txn_release_savepoint: [ref.types.int, [txnPtr, charPtr]],

    // Iterator operations
    tidesdb_iter_new: [ref.types.int, [txnPtr, columnFamilyPtr, iterPtrPtr]],
    tidesdb_iter_seek: [ref.types.int, [iterPtr, uint8Ptr, ref.types.size_t]],
    tidesdb_iter_seek_for_prev: [ref.types.int, [iterPtr, uint8Ptr, ref.types.size_t]],
    tidesdb_iter_seek_to_first: [ref.types.int, [iterPtr]],
    tidesdb_iter_seek_to_last: [ref.types.int, [iterPtr]],
    tidesdb_iter_next: [ref.types.int, [iterPtr]],
    tidesdb_iter_prev: [ref.types.int, [iterPtr]],
    tidesdb_iter_valid: [ref.types.int, [iterPtr]],
    tidesdb_iter_key: [ref.types.int, [iterPtr, uint8PtrPtr, sizeTPtr]],
    tidesdb_iter_value: [ref.types.int, [iterPtr, uint8PtrPtr, sizeTPtr]],
    tidesdb_iter_free: [ref.types.void, [iterPtr]],

    // Maintenance operations
    tidesdb_compact: [ref.types.int, [columnFamilyPtr]],
    tidesdb_flush_memtable: [ref.types.int, [columnFamilyPtr]],

    // Statistics operations
    tidesdb_get_stats: [ref.types.int, [columnFamilyPtr, StatsStructPtrPtr]],
    tidesdb_free_stats: [ref.types.void, [StatsStructPtr]],
    tidesdb_get_cache_stats: [ref.types.int, [tidesdbPtr, CacheStatsPtr]],
  });
}

export const lib = loadLibrary();
export { ref };
