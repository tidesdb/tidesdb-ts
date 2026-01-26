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
 * Compression algorithms supported by TidesDB.
 */
export enum CompressionAlgorithm {
  NoCompression = 0,
  SnappyCompression = 1,
  Lz4Compression = 2,
  ZstdCompression = 3,
  Lz4FastCompression = 4,
}

/**
 * Sync modes for durability.
 */
export enum SyncMode {
  None = 0,
  Full = 1,
  Interval = 2,
}

/**
 * Log levels for debugging.
 */
export enum LogLevel {
  Debug = 0,
  Info = 1,
  Warn = 2,
  Error = 3,
  Fatal = 4,
  None = 99,
}

/**
 * Transaction isolation levels.
 */
export enum IsolationLevel {
  ReadUncommitted = 0,
  ReadCommitted = 1,
  RepeatableRead = 2,
  Snapshot = 3,
  Serializable = 4,
}

/**
 * Error codes from TidesDB.
 */
export enum ErrorCode {
  Success = 0,
  ErrMemory = -1,
  ErrInvalidArgs = -2,
  ErrNotFound = -3,
  ErrIO = -4,
  ErrCorruption = -5,
  ErrExists = -6,
  ErrConflict = -7,
  ErrTooLarge = -8,
  ErrMemoryLimit = -9,
  ErrInvalidDB = -10,
  ErrUnknown = -11,
  ErrLocked = -12,
}

/**
 * Configuration for opening a TidesDB instance.
 */
export interface Config {
  /** Path to the database directory. */
  dbPath: string;
  /** Number of flush threads. Default: 2 */
  numFlushThreads?: number;
  /** Number of compaction threads. Default: 2 */
  numCompactionThreads?: number;
  /** Log level. Default: LogLevel.Info */
  logLevel?: LogLevel;
  /** Block cache size in bytes. Default: 64MB */
  blockCacheSize?: number;
  /** Maximum number of open SSTables. Default: 256 */
  maxOpenSSTables?: number;
}

/**
 * Configuration for a column family.
 */
export interface ColumnFamilyConfig {
  /** Size of write buffer in bytes. */
  writeBufferSize?: number;
  /** Ratio of level sizes. */
  levelSizeRatio?: number;
  /** Minimum number of levels. */
  minLevels?: number;
  /** Offset for dividing level. */
  dividingLevelOffset?: number;
  /** Threshold for klog value. */
  klogValueThreshold?: number;
  /** Compression algorithm. */
  compressionAlgorithm?: CompressionAlgorithm;
  /** Enable bloom filter. */
  enableBloomFilter?: boolean;
  /** Bloom filter false positive rate. */
  bloomFpr?: number;
  /** Enable block indexes. */
  enableBlockIndexes?: boolean;
  /** Index sample ratio. */
  indexSampleRatio?: number;
  /** Block index prefix length. */
  blockIndexPrefixLen?: number;
  /** Sync mode. */
  syncMode?: SyncMode;
  /** Sync interval in microseconds. */
  syncIntervalUs?: number;
  /** Comparator name. */
  comparatorName?: string;
  /** Skip list max level. */
  skipListMaxLevel?: number;
  /** Skip list probability. */
  skipListProbability?: number;
  /** Default isolation level. */
  defaultIsolationLevel?: IsolationLevel;
  /** Minimum free disk space required (bytes). */
  minDiskSpace?: number;
  /** L1 file count trigger for compaction. */
  l1FileCountTrigger?: number;
  /** L0 queue stall threshold for backpressure. */
  l0QueueStallThreshold?: number;
}

/**
 * Statistics for a column family.
 */
export interface Stats {
  /** Number of levels. */
  numLevels: number;
  /** Size of memtable in bytes. */
  memtableSize: number;
  /** Sizes of each level in bytes. */
  levelSizes: number[];
  /** Number of SSTables in each level. */
  levelNumSSTables: number[];
  /** Column family configuration. */
  config?: ColumnFamilyConfig;
  /** Total number of keys across memtable and all SSTables. */
  totalKeys: number;
  /** Total data size (klog + vlog) across all SSTables. */
  totalDataSize: number;
  /** Average key size in bytes. */
  avgKeySize: number;
  /** Average value size in bytes. */
  avgValueSize: number;
  /** Number of keys per level. */
  levelKeyCounts: number[];
  /** Read amplification (point lookup cost multiplier). */
  readAmp: number;
  /** Cache hit rate (0.0 if cache disabled). */
  hitRate: number;
}

/**
 * Statistics for the block cache.
 */
export interface CacheStats {
  /** Whether block cache is enabled. */
  enabled: boolean;
  /** Total number of cached entries. */
  totalEntries: number;
  /** Total bytes used by cache. */
  totalBytes: number;
  /** Cache hits. */
  hits: number;
  /** Cache misses. */
  misses: number;
  /** Hit rate (hits / (hits + misses)). */
  hitRate: number;
  /** Number of cache partitions. */
  numPartitions: number;
}
