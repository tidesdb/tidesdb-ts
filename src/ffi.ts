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

import koffi from 'koffi';

// Define opaque pointer types
const tidesdb_t = koffi.opaque('tidesdb_t');
const tidesdb_column_family_t = koffi.opaque('tidesdb_column_family_t');
const tidesdb_txn_t = koffi.opaque('tidesdb_txn_t');
const tidesdb_iter_t = koffi.opaque('tidesdb_iter_t');

// Pointer types
export const tidesdbPtr = koffi.pointer(tidesdb_t);
export const tidesdbPtrPtr = koffi.pointer(tidesdbPtr);
export const columnFamilyPtr = koffi.pointer(tidesdb_column_family_t);
export const txnPtr = koffi.pointer(tidesdb_txn_t);
export const txnPtrPtr = koffi.pointer(txnPtr);
export const iterPtr = koffi.pointer(tidesdb_iter_t);
export const iterPtrPtr = koffi.pointer(iterPtr);

// tidesdb_config_t structure
export const TidesDBConfigStruct = koffi.struct('tidesdb_config_t', {
  db_path: 'char *',
  num_flush_threads: 'int',
  num_compaction_threads: 'int',
  log_level: 'int',
  block_cache_size: 'size_t',
  max_open_sstables: 'size_t',
  log_to_file: 'int',
  log_truncation_at: 'size_t',
});

// tidesdb_column_family_config_t structure
export const ColumnFamilyConfigStruct = koffi.struct('tidesdb_column_family_config_t', {
  name: koffi.array('char', 128),
  write_buffer_size: 'size_t',
  level_size_ratio: 'size_t',
  min_levels: 'int',
  dividing_level_offset: 'int',
  klog_value_threshold: 'size_t',
  compression_algorithm: 'int',
  enable_bloom_filter: 'int',
  bloom_fpr: 'double',
  enable_block_indexes: 'int',
  index_sample_ratio: 'int',
  block_index_prefix_len: 'int',
  sync_mode: 'int',
  sync_interval_us: 'uint64_t',
  comparator_name: koffi.array('char', 64),
  comparator_ctx_str: koffi.array('char', 256),
  comparator_fn_cached: 'void *',
  comparator_ctx_cached: 'void *',
  skip_list_max_level: 'int',
  skip_list_probability: 'float',
  default_isolation_level: 'int',
  min_disk_space: 'uint64_t',
  l1_file_count_trigger: 'int',
  l0_queue_stall_threshold: 'int',
  use_btree: 'int',
});

// tidesdb_cache_stats_t structure
export const CacheStatsStruct = koffi.struct('tidesdb_cache_stats_t', {
  enabled: 'int',
  total_entries: 'size_t',
  total_bytes: 'size_t',
  hits: 'uint64_t',
  misses: 'uint64_t',
  hit_rate: 'double',
  num_partitions: 'size_t',
});

// tidesdb_stats_t structure
export const StatsStruct = koffi.struct('tidesdb_stats_t', {
  num_levels: 'int',
  memtable_size: 'size_t',
  level_sizes: 'size_t *',
  level_num_sstables: 'int *',
  config: koffi.pointer(ColumnFamilyConfigStruct),
  total_keys: 'uint64_t',
  total_data_size: 'uint64_t',
  avg_key_size: 'double',
  avg_value_size: 'double',
  level_key_counts: 'uint64_t *',
  read_amp: 'double',
  hit_rate: 'double',
  use_btree: 'int',
  btree_total_nodes: 'uint64_t',
  btree_max_height: 'uint32_t',
  btree_avg_height: 'double',
});

export const StatsStructPtr = koffi.pointer(StatsStruct);

// Load the TidesDB library
function getLibraryPath(): string {
  if (process.platform === 'darwin') {
    return 'libtidesdb.dylib';
  } else if (process.platform === 'win32') {
    // MinGW builds produce libtidesdb.dll, MSVC produces tidesdb.dll
    return 'libtidesdb.dll';
  } else {
    return 'libtidesdb.so';
  }
}

const libPath = getLibraryPath();
export const lib = koffi.load(libPath);

// Database operations
export const tidesdb_open = lib.func('int tidesdb_open(tidesdb_config_t *config, _Out_ tidesdb_t **db)');
export const tidesdb_close = lib.func('int tidesdb_close(tidesdb_t *db)');

// Default config
export const tidesdb_default_column_family_config = lib.func('tidesdb_column_family_config_t tidesdb_default_column_family_config()');
export const tidesdb_default_config = lib.func('tidesdb_config_t tidesdb_default_config()');

// Column family operations
export const tidesdb_create_column_family = lib.func('int tidesdb_create_column_family(tidesdb_t *db, const char *name, tidesdb_column_family_config_t *config)');
export const tidesdb_drop_column_family = lib.func('int tidesdb_drop_column_family(tidesdb_t *db, const char *name)');
export const tidesdb_rename_column_family = lib.func('int tidesdb_rename_column_family(tidesdb_t *db, const char *old_name, const char *new_name)');
export const tidesdb_clone_column_family = lib.func('int tidesdb_clone_column_family(tidesdb_t *db, const char *source_name, const char *dest_name)');
export const tidesdb_get_column_family = lib.func('tidesdb_column_family_t *tidesdb_get_column_family(tidesdb_t *db, const char *name)');
export const tidesdb_list_column_families = lib.func('int tidesdb_list_column_families(tidesdb_t *db, _Out_ char ***names, _Out_ int *count)');

// Transaction operations
export const tidesdb_txn_begin = lib.func('int tidesdb_txn_begin(tidesdb_t *db, _Out_ tidesdb_txn_t **txn)');
export const tidesdb_txn_begin_with_isolation = lib.func('int tidesdb_txn_begin_with_isolation(tidesdb_t *db, int isolation, _Out_ tidesdb_txn_t **txn)');
export const tidesdb_txn_put = lib.func('int tidesdb_txn_put(tidesdb_txn_t *txn, tidesdb_column_family_t *cf, const uint8_t *key, size_t key_size, const uint8_t *value, size_t value_size, int64_t ttl)');
export const tidesdb_txn_get = lib.func('int tidesdb_txn_get(tidesdb_txn_t *txn, tidesdb_column_family_t *cf, const uint8_t *key, size_t key_size, _Out_ uint8_t **value, _Out_ size_t *value_size)');
export const tidesdb_txn_delete = lib.func('int tidesdb_txn_delete(tidesdb_txn_t *txn, tidesdb_column_family_t *cf, const uint8_t *key, size_t key_size)');
export const tidesdb_txn_commit = lib.func('int tidesdb_txn_commit(tidesdb_txn_t *txn)');
export const tidesdb_txn_rollback = lib.func('int tidesdb_txn_rollback(tidesdb_txn_t *txn)');
export const tidesdb_txn_reset = lib.func('int tidesdb_txn_reset(tidesdb_txn_t *txn, int isolation)');
export const tidesdb_txn_free = lib.func('void tidesdb_txn_free(tidesdb_txn_t *txn)');

// Savepoint operations
export const tidesdb_txn_savepoint = lib.func('int tidesdb_txn_savepoint(tidesdb_txn_t *txn, const char *name)');
export const tidesdb_txn_rollback_to_savepoint = lib.func('int tidesdb_txn_rollback_to_savepoint(tidesdb_txn_t *txn, const char *name)');
export const tidesdb_txn_release_savepoint = lib.func('int tidesdb_txn_release_savepoint(tidesdb_txn_t *txn, const char *name)');

// Iterator operations
export const tidesdb_iter_new = lib.func('int tidesdb_iter_new(tidesdb_txn_t *txn, tidesdb_column_family_t *cf, _Out_ tidesdb_iter_t **iter)');
export const tidesdb_iter_seek = lib.func('int tidesdb_iter_seek(tidesdb_iter_t *iter, const uint8_t *key, size_t key_size)');
export const tidesdb_iter_seek_for_prev = lib.func('int tidesdb_iter_seek_for_prev(tidesdb_iter_t *iter, const uint8_t *key, size_t key_size)');
export const tidesdb_iter_seek_to_first = lib.func('int tidesdb_iter_seek_to_first(tidesdb_iter_t *iter)');
export const tidesdb_iter_seek_to_last = lib.func('int tidesdb_iter_seek_to_last(tidesdb_iter_t *iter)');
export const tidesdb_iter_next = lib.func('int tidesdb_iter_next(tidesdb_iter_t *iter)');
export const tidesdb_iter_prev = lib.func('int tidesdb_iter_prev(tidesdb_iter_t *iter)');
export const tidesdb_iter_valid = lib.func('int tidesdb_iter_valid(tidesdb_iter_t *iter)');
export const tidesdb_iter_key = lib.func('int tidesdb_iter_key(tidesdb_iter_t *iter, _Out_ uint8_t **key, _Out_ size_t *key_size)');
export const tidesdb_iter_value = lib.func('int tidesdb_iter_value(tidesdb_iter_t *iter, _Out_ uint8_t **value, _Out_ size_t *value_size)');
export const tidesdb_iter_free = lib.func('void tidesdb_iter_free(tidesdb_iter_t *iter)');

// Maintenance operations
export const tidesdb_compact = lib.func('int tidesdb_compact(tidesdb_column_family_t *cf)');
export const tidesdb_flush_memtable = lib.func('int tidesdb_flush_memtable(tidesdb_column_family_t *cf)');
export const tidesdb_is_flushing = lib.func('int tidesdb_is_flushing(tidesdb_column_family_t *cf)');
export const tidesdb_is_compacting = lib.func('int tidesdb_is_compacting(tidesdb_column_family_t *cf)');
export const tidesdb_backup = lib.func('int tidesdb_backup(tidesdb_t *db, char *dir)');
export const tidesdb_checkpoint = lib.func('int tidesdb_checkpoint(tidesdb_t *db, const char *checkpoint_dir)');

// Comparator operations
export const tidesdb_register_comparator = lib.func('int tidesdb_register_comparator(tidesdb_t *db, const char *name, void *fn, const char *ctx_str, void *ctx)');
export const tidesdb_get_comparator = lib.func('int tidesdb_get_comparator(tidesdb_t *db, const char *name, _Out_ void **fn, _Out_ void **ctx)');

// Configuration operations
export const tidesdb_cf_config_load_from_ini = lib.func('int tidesdb_cf_config_load_from_ini(const char *ini_file, const char *section_name, _Out_ tidesdb_column_family_config_t *config)');
export const tidesdb_cf_config_save_to_ini = lib.func('int tidesdb_cf_config_save_to_ini(const char *ini_file, const char *section_name, tidesdb_column_family_config_t *config)');
export const tidesdb_cf_update_runtime_config = lib.func('int tidesdb_cf_update_runtime_config(tidesdb_column_family_t *cf, tidesdb_column_family_config_t *new_config, int persist_to_disk)');

// Statistics operations
export const tidesdb_get_stats = lib.func('int tidesdb_get_stats(tidesdb_column_family_t *cf, _Out_ tidesdb_stats_t **stats)');
export const tidesdb_free_stats = lib.func('void tidesdb_free_stats(tidesdb_stats_t *stats)');
export const tidesdb_get_cache_stats = lib.func('int tidesdb_get_cache_stats(tidesdb_t *db, _Out_ tidesdb_cache_stats_t *stats)');

// Memory management
export const tidesdb_free = lib.func('void tidesdb_free(void *ptr)');

export { koffi };
