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
  ErrReadonly = -13,
}

/**
 * Configuration for object store mode behavior.
 */
export interface ObjectStoreConfig {
  /** Local directory for cached SSTable files (null = use dbPath). */
  localCachePath?: string | null;
  /** Maximum local cache size in bytes (0 = unlimited). */
  localCacheMaxBytes?: number;
  /** Cache downloaded files locally. Default: true */
  cacheOnRead?: boolean;
  /** Keep local copy after upload. Default: true */
  cacheOnWrite?: boolean;
  /** Number of parallel upload threads. Default: 4 */
  maxConcurrentUploads?: number;
  /** Number of parallel download threads. Default: 8 */
  maxConcurrentDownloads?: number;
  /** Use multipart upload above this size in bytes. Default: 64MB */
  multipartThreshold?: number;
  /** Chunk size for multipart uploads in bytes. Default: 8MB */
  multipartPartSize?: number;
  /** Upload MANIFEST after each compaction. Default: true */
  syncManifestToObject?: boolean;
  /** Upload closed WAL segments for replication. Default: true */
  replicateWal?: boolean;
  /** Block flush until WAL is uploaded (true) or upload in background (false). Default: false */
  walUploadSync?: boolean;
  /** Sync active WAL to object store when it grows by this many bytes (0 = off). Default: 1MB */
  walSyncThresholdBytes?: number;
  /** Upload WAL after every txn commit for RPO=0 replication. Default: false */
  walSyncOnCommit?: boolean;
  /** Enable read-only replica mode. Default: false */
  replicaMode?: boolean;
  /** MANIFEST poll interval for replica sync in microseconds. Default: 5000000 (5s) */
  replicaSyncIntervalUs?: number;
  /** Replay WAL from object store for near-real-time reads on replicas. Default: true */
  replicaReplayWal?: boolean;
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
  /** Global memory limit in bytes. Default: 0 (auto, 50% of system RAM). */
  maxMemoryUsage?: number;
  /** Write logs to file instead of stderr. Default: false */
  logToFile?: boolean;
  /** Log file truncation size in bytes. Default: 24MB, 0 = no truncation */
  logTruncationAt?: number;
  /** Enable unified memtable mode. Default: false */
  unifiedMemtable?: boolean;
  /** Write buffer size for unified memtable (0 = auto). */
  unifiedMemtableWriteBufferSize?: number;
  /** Skip list max level for unified memtable (0 = default 12). */
  unifiedMemtableSkipListMaxLevel?: number;
  /** Skip list probability for unified memtable (0 = default 0.25). */
  unifiedMemtableSkipListProbability?: number;
  /** Sync mode for unified WAL. Default: SyncMode.None */
  unifiedMemtableSyncMode?: SyncMode;
  /** Sync interval for unified WAL in microseconds (0 = default). */
  unifiedMemtableSyncIntervalUs?: number;
  /** Filesystem object store root directory. When set, enables object store mode with the FS connector. */
  objectStoreFsPath?: string;
  /** Object store behavior configuration (used when objectStoreFsPath is set). */
  objectStoreConfig?: ObjectStoreConfig;
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
  /** Use B+tree format for klog (default: false = block-based). */
  useBtree?: boolean;
  /** Target SSTable size in object store mode (default 256MB, 0=auto). */
  objectTargetFileSize?: number;
  /** Compact less aggressively in object store mode (default: false). */
  objectLazyCompaction?: boolean;
  /** Download all inputs before merge in object store mode (default: true). */
  objectPrefetchCompaction?: boolean;
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
  /** Whether column family uses B+tree format. */
  useBtree?: boolean;
  /** Total B+tree nodes across all SSTables (only if useBtree=true). */
  btreeTotalNodes?: number;
  /** Maximum tree height across all SSTables (only if useBtree=true). */
  btreeMaxHeight?: number;
  /** Average tree height across all SSTables (only if useBtree=true). */
  btreeAvgHeight?: number;
}

/**
 * Database-level aggregate statistics.
 */
export interface DbStats {
  /** Number of column families. */
  numColumnFamilies: number;
  /** System total memory in bytes. */
  totalMemory: number;
  /** System available memory at open time in bytes. */
  availableMemory: number;
  /** Resolved memory limit (auto or configured) in bytes. */
  resolvedMemoryLimit: number;
  /** Current memory pressure level (0=normal, 1=elevated, 2=high, 3=critical). */
  memoryPressureLevel: number;
  /** Number of pending flush operations (queued + in-flight). */
  flushPendingCount: number;
  /** Total bytes in active memtables across all CFs. */
  totalMemtableBytes: number;
  /** Total immutable memtables across all CFs. */
  totalImmutableCount: number;
  /** Total SSTables across all CFs and levels. */
  totalSstableCount: number;
  /** Total data size (klog + vlog) across all CFs in bytes. */
  totalDataSizeBytes: number;
  /** Number of currently open SSTable file handles. */
  numOpenSstables: number;
  /** Current global sequence number. */
  globalSeq: number;
  /** Bytes held by in-flight transactions. */
  txnMemoryBytes: number;
  /** Number of pending compaction tasks. */
  compactionQueueSize: number;
  /** Number of pending flush tasks in queue. */
  flushQueueSize: number;
  /** Whether unified memtable mode is active. */
  unifiedMemtableEnabled: boolean;
  /** Bytes in unified active memtable. */
  unifiedMemtableBytes: number;
  /** Number of unified immutable memtables. */
  unifiedImmutableCount: number;
  /** Whether unified memtable is currently flushing/rotating. */
  unifiedIsFlushing: boolean;
  /** Next CF index to be assigned in unified mode. */
  unifiedNextCfIndex: number;
  /** Current unified WAL generation counter. */
  unifiedWalGeneration: number;
  /** Whether object store mode is active. */
  objectStoreEnabled: boolean;
  /** Object store connector name ("s3", "gcs", "fs", etc.). */
  objectStoreConnector: string;
  /** Current local file cache usage in bytes. */
  localCacheBytesUsed: number;
  /** Configured maximum local cache size in bytes. */
  localCacheBytesMax: number;
  /** Number of files tracked in local cache. */
  localCacheNumFiles: number;
  /** Highest WAL generation confirmed uploaded. */
  lastUploadedGeneration: number;
  /** Number of pending upload jobs in the queue. */
  uploadQueueDepth: number;
  /** Lifetime count of objects uploaded to object store. */
  totalUploads: number;
  /** Lifetime count of permanently failed uploads. */
  totalUploadFailures: number;
  /** Whether running in read-only replica mode. */
  replicaMode: boolean;
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

/**
 * Represents a single operation in a committed transaction batch.
 * Passed to commit hook callbacks.
 */
export interface CommitOp {
  /** Key data. */
  key: Buffer;
  /** Value data (null for deletes). */
  value: Buffer | null;
  /** Time-to-live (Unix timestamp, -1 = no expiry). */
  ttl: number;
  /** Whether this is a delete operation. */
  isDelete: boolean;
}

/**
 * Callback function invoked after a transaction commits to a column family.
 * Receives the full batch of operations atomically.
 * Return 0 on success; non-zero is logged as a warning but does not roll back.
 */
export type CommitHookCallback = (ops: CommitOp[], commitSeq: number) => number;
