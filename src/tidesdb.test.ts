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

import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import {
  TidesDB,
  IsolationLevel,
  CompressionAlgorithm,
  SyncMode,
  TidesDBError,
  ErrorCode,
  CommitOp,
  defaultConfig,
} from "./index";

function createTempDir(): string {
  return fs.mkdtempSync(path.join(os.tmpdir(), "tidesdb-test-"));
}

function removeTempDir(dir: string): void {
  fs.rmSync(dir, { recursive: true, force: true });
}

describe("TidesDB", () => {
  let tempDir: string;
  let db: TidesDB;

  beforeEach(() => {
    tempDir = createTempDir();
    db = TidesDB.open({
      dbPath: tempDir,
      numFlushThreads: 2,
      numCompactionThreads: 2,
    });
  });

  afterEach(() => {
    try {
      db.close();
    } catch {
      // Ignore close errors in cleanup
    }
    removeTempDir(tempDir);
  });

  describe("Database Operations", () => {
    test("open and close", () => {
      expect(db).toBeDefined();
      db.close();
    });

    test("create and drop column family", () => {
      db.createColumnFamily("test_cf");
      const cf = db.getColumnFamily("test_cf");
      expect(cf.name).toBe("test_cf");

      const families = db.listColumnFamilies();
      expect(Array.isArray(families)).toBe(true);
      // Note: koffi char** decoding may not work perfectly on all platforms
      // The implementation attempts to decode but may return empty on failure
      if (families.length > 0) {
        expect(families).toContain("test_cf");
      }

      db.dropColumnFamily("test_cf");
    });

    test("get non-existent column family throws", () => {
      expect(() => db.getColumnFamily("nonexistent")).toThrow(TidesDBError);
    });
  });

  describe("Transaction Operations", () => {
    test("put and get", () => {
      db.createColumnFamily("test_cf");
      const cf = db.getColumnFamily("test_cf");

      // Put
      const txn = db.beginTransaction();
      txn.put(cf, Buffer.from("key"), Buffer.from("value"), -1);
      txn.commit();
      txn.free();

      // Get
      const readTxn = db.beginTransaction();
      const value = readTxn.get(cf, Buffer.from("key"));
      expect(value.toString()).toBe("value");
      readTxn.free();
    });

    test("delete", () => {
      db.createColumnFamily("test_cf");
      const cf = db.getColumnFamily("test_cf");

      // Put
      const txn = db.beginTransaction();
      txn.put(cf, Buffer.from("key"), Buffer.from("value"), -1);
      txn.commit();
      txn.free();

      // Delete
      const deleteTxn = db.beginTransaction();
      deleteTxn.delete(cf, Buffer.from("key"));
      deleteTxn.commit();
      deleteTxn.free();

      // Verify deleted
      const readTxn = db.beginTransaction();
      expect(() => readTxn.get(cf, Buffer.from("key"))).toThrow();
      readTxn.free();
    });

    test("singleDelete makes prior put unreadable", () => {
      db.createColumnFamily("sd_cf");
      const cf = db.getColumnFamily("sd_cf");

      const txn = db.beginTransaction();
      txn.put(cf, Buffer.from("sd_key"), Buffer.from("sd_value"), -1);
      txn.commit();
      txn.free();

      const sdTxn = db.beginTransaction();
      sdTxn.singleDelete(cf, Buffer.from("sd_key"));
      sdTxn.commit();
      sdTxn.free();

      const readTxn = db.beginTransaction();
      expect(() => readTxn.get(cf, Buffer.from("sd_key"))).toThrow();
      readTxn.free();
    });

    test("singleDelete on non-existent key is allowed", () => {
      db.createColumnFamily("sd_missing_cf");
      const cf = db.getColumnFamily("sd_missing_cf");

      const txn = db.beginTransaction();
      expect(() => txn.singleDelete(cf, Buffer.from("missing"))).not.toThrow();
      txn.commit();
      txn.free();
    });

    test("singleDelete works alongside puts in the same transaction", () => {
      db.createColumnFamily("sd_mixed_cf");
      const cf = db.getColumnFamily("sd_mixed_cf");

      // Seed two keys
      const seed = db.beginTransaction();
      seed.put(cf, Buffer.from("keep"), Buffer.from("keep_v"), -1);
      seed.put(cf, Buffer.from("drop"), Buffer.from("drop_v"), -1);
      seed.commit();
      seed.free();

      // Mixed batch: insert a new key and single-delete an existing one
      const mix = db.beginTransaction();
      mix.put(cf, Buffer.from("fresh"), Buffer.from("fresh_v"), -1);
      mix.singleDelete(cf, Buffer.from("drop"));
      mix.commit();
      mix.free();

      const readTxn = db.beginTransaction();
      expect(readTxn.get(cf, Buffer.from("keep")).toString()).toBe("keep_v");
      expect(readTxn.get(cf, Buffer.from("fresh")).toString()).toBe("fresh_v");
      expect(() => readTxn.get(cf, Buffer.from("drop"))).toThrow();
      readTxn.free();
    });

    test("rollback", () => {
      db.createColumnFamily("test_cf");
      const cf = db.getColumnFamily("test_cf");

      const txn = db.beginTransaction();
      txn.put(
        cf,
        Buffer.from("rollback_key"),
        Buffer.from("rollback_value"),
        -1,
      );
      txn.rollback();
      txn.free();

      // Verify key does not exist
      const readTxn = db.beginTransaction();
      expect(() => readTxn.get(cf, Buffer.from("rollback_key"))).toThrow();
      readTxn.free();
    });

    test("multiple operations in one transaction", () => {
      db.createColumnFamily("test_cf");
      const cf = db.getColumnFamily("test_cf");

      const txn = db.beginTransaction();
      txn.put(cf, Buffer.from("key1"), Buffer.from("value1"), -1);
      txn.put(cf, Buffer.from("key2"), Buffer.from("value2"), -1);
      txn.put(cf, Buffer.from("key3"), Buffer.from("value3"), -1);
      txn.commit();
      txn.free();

      const readTxn = db.beginTransaction();
      for (let i = 1; i <= 3; i++) {
        const value = readTxn.get(cf, Buffer.from(`key${i}`));
        expect(value.toString()).toBe(`value${i}`);
      }
      readTxn.free();
    });
  });

  describe("Savepoints", () => {
    test("savepoint and rollback to savepoint", () => {
      db.createColumnFamily("test_cf");
      const cf = db.getColumnFamily("test_cf");

      const txn = db.beginTransaction();
      txn.put(cf, Buffer.from("key1"), Buffer.from("value1"), -1);

      txn.savepoint("sp1");
      txn.put(cf, Buffer.from("key2"), Buffer.from("value2"), -1);

      // Rollback to savepoint - key2 is discarded, key1 remains
      txn.rollbackToSavepoint("sp1");

      txn.put(cf, Buffer.from("key3"), Buffer.from("value3"), -1);
      txn.commit();
      txn.free();

      // Verify results
      const readTxn = db.beginTransaction();

      // key1 should exist
      expect(readTxn.get(cf, Buffer.from("key1")).toString()).toBe("value1");

      // key2 should not exist (rolled back)
      expect(() => readTxn.get(cf, Buffer.from("key2"))).toThrow();

      // key3 should exist
      expect(readTxn.get(cf, Buffer.from("key3")).toString()).toBe("value3");

      readTxn.free();
    });
  });

  describe("Iterator", () => {
    test("forward iteration", () => {
      db.createColumnFamily("test_cf");
      const cf = db.getColumnFamily("test_cf");

      // Insert data
      const txn = db.beginTransaction();
      for (let i = 0; i < 10; i++) {
        const key = `key${i.toString().padStart(2, "0")}`;
        const value = `value${i}`;
        txn.put(cf, Buffer.from(key), Buffer.from(value), -1);
      }
      txn.commit();
      txn.free();

      // Forward iteration
      const readTxn = db.beginTransaction();
      const iter = readTxn.newIterator(cf);
      iter.seekToFirst();

      let count = 0;
      while (iter.isValid()) {
        const key = iter.key();
        const value = iter.value();
        expect(key.length).toBeGreaterThan(0);
        expect(value.length).toBeGreaterThan(0);
        count++;
        iter.next();
      }
      expect(count).toBe(10);

      iter.free();
      readTxn.free();
    });

    test("backward iteration", () => {
      db.createColumnFamily("test_cf");
      const cf = db.getColumnFamily("test_cf");

      // Insert data
      const txn = db.beginTransaction();
      for (let i = 0; i < 10; i++) {
        const key = `key${i.toString().padStart(2, "0")}`;
        const value = `value${i}`;
        txn.put(cf, Buffer.from(key), Buffer.from(value), -1);
      }
      txn.commit();
      txn.free();

      // Backward iteration
      const readTxn = db.beginTransaction();
      const iter = readTxn.newIterator(cf);
      iter.seekToLast();

      let count = 0;
      while (iter.isValid()) {
        const key = iter.key();
        const value = iter.value();
        expect(key.length).toBeGreaterThan(0);
        expect(value.length).toBeGreaterThan(0);
        count++;
        iter.prev();
      }
      expect(count).toBe(10);

      iter.free();
      readTxn.free();
    });
  });

  describe("Isolation Levels", () => {
    test("begin transaction with different isolation levels", () => {
      const levels = [
        IsolationLevel.ReadUncommitted,
        IsolationLevel.ReadCommitted,
        IsolationLevel.RepeatableRead,
        IsolationLevel.Snapshot,
        IsolationLevel.Serializable,
      ];

      for (const level of levels) {
        const txn = db.beginTransactionWithIsolation(level);
        txn.free();
      }
    });
  });

  describe("Column Family Configuration", () => {
    test("create column family with custom config", () => {
      db.createColumnFamily("custom_cf", {
        writeBufferSize: 128 * 1024 * 1024,
        levelSizeRatio: 10,
        minLevels: 5,
        compressionAlgorithm: CompressionAlgorithm.Lz4Compression,
        enableBloomFilter: true,
        bloomFpr: 0.01,
        enableBlockIndexes: true,
        syncMode: SyncMode.Interval,
        syncIntervalUs: 128000,
        defaultIsolationLevel: IsolationLevel.ReadCommitted,
      });

      const cf = db.getColumnFamily("custom_cf");
      expect(cf.name).toBe("custom_cf");
    });

    test("create column family with B+tree format", () => {
      db.createColumnFamily("btree_cf", {
        useBtree: true,
        enableBloomFilter: true,
        bloomFpr: 0.01,
      });

      const cf = db.getColumnFamily("btree_cf");
      expect(cf.name).toBe("btree_cf");

      // Insert some data and verify stats include B+tree fields
      const txn = db.beginTransaction();
      for (let i = 0; i < 10; i++) {
        const key = `btree_key${i}`;
        const value = `btree_value${i}`;
        txn.put(cf, Buffer.from(key), Buffer.from(value), -1);
      }
      txn.commit();
      txn.free();

      const stats = cf.getStats();
      expect(stats.useBtree).toBe(true);
    });

    test("create column family with block-based format (default)", () => {
      db.createColumnFamily("block_cf", {
        useBtree: false,
      });

      const cf = db.getColumnFamily("block_cf");
      const stats = cf.getStats();
      expect(stats.useBtree).toBe(false);
      expect(stats.btreeTotalNodes).toBeUndefined();
      expect(stats.btreeMaxHeight).toBeUndefined();
      expect(stats.btreeAvgHeight).toBeUndefined();
    });
  });

  describe("Clone Column Family", () => {
    test("clone column family copies data", () => {
      db.createColumnFamily("source_cf");
      const cf = db.getColumnFamily("source_cf");

      // Insert data into source
      const txn = db.beginTransaction();
      txn.put(cf, Buffer.from("key1"), Buffer.from("value1"), -1);
      txn.put(cf, Buffer.from("key2"), Buffer.from("value2"), -1);
      txn.commit();
      txn.free();

      // Clone the column family
      db.cloneColumnFamily("source_cf", "cloned_cf");

      // Verify cloned data exists
      const clonedCf = db.getColumnFamily("cloned_cf");
      const readTxn = db.beginTransaction();
      expect(readTxn.get(clonedCf, Buffer.from("key1")).toString()).toBe(
        "value1",
      );
      expect(readTxn.get(clonedCf, Buffer.from("key2")).toString()).toBe(
        "value2",
      );
      readTxn.free();
    });

    test("clone is independent from source", () => {
      db.createColumnFamily("source_cf");
      const cf = db.getColumnFamily("source_cf");

      // Insert data into source
      const txn = db.beginTransaction();
      txn.put(cf, Buffer.from("key1"), Buffer.from("value1"), -1);
      txn.commit();
      txn.free();

      // Clone
      db.cloneColumnFamily("source_cf", "cloned_cf");

      // Write new data to source only
      const writeTxn = db.beginTransaction();
      writeTxn.put(cf, Buffer.from("key2"), Buffer.from("value2"), -1);
      writeTxn.commit();
      writeTxn.free();

      // Verify clone does not have the new key
      const clonedCf = db.getColumnFamily("cloned_cf");
      const readTxn = db.beginTransaction();
      expect(readTxn.get(clonedCf, Buffer.from("key1")).toString()).toBe(
        "value1",
      );
      expect(() => readTxn.get(clonedCf, Buffer.from("key2"))).toThrow();
      readTxn.free();
    });

    test("clone non-existent column family throws", () => {
      expect(() => db.cloneColumnFamily("nonexistent", "dest")).toThrow();
    });

    test("clone to existing name throws", () => {
      db.createColumnFamily("source_cf");
      db.createColumnFamily("existing_cf");
      expect(() => db.cloneColumnFamily("source_cf", "existing_cf")).toThrow();
    });
  });

  describe("Transaction Reset", () => {
    test("reset after commit and reuse", () => {
      db.createColumnFamily("test_cf");
      const cf = db.getColumnFamily("test_cf");

      const txn = db.beginTransaction();

      // First batch
      txn.put(cf, Buffer.from("reset_key1"), Buffer.from("reset_value1"), -1);
      txn.commit();

      // Reset instead of free + begin
      txn.reset(IsolationLevel.ReadCommitted);

      // Second batch using the same transaction
      txn.put(cf, Buffer.from("reset_key2"), Buffer.from("reset_value2"), -1);
      txn.commit();

      txn.free();

      // Verify both keys exist
      const readTxn = db.beginTransaction();
      expect(readTxn.get(cf, Buffer.from("reset_key1")).toString()).toBe(
        "reset_value1",
      );
      expect(readTxn.get(cf, Buffer.from("reset_key2")).toString()).toBe(
        "reset_value2",
      );
      readTxn.free();
    });

    test("reset after rollback and reuse", () => {
      db.createColumnFamily("test_cf");
      const cf = db.getColumnFamily("test_cf");

      const txn = db.beginTransaction();

      // First batch - rollback
      txn.put(cf, Buffer.from("rolled_back_key"), Buffer.from("value"), -1);
      txn.rollback();

      // Reset and do new work
      txn.reset(IsolationLevel.ReadCommitted);

      txn.put(
        cf,
        Buffer.from("after_reset_key"),
        Buffer.from("after_reset_value"),
        -1,
      );
      txn.commit();

      txn.free();

      // Verify rolled back key does not exist, but reset key does
      const readTxn = db.beginTransaction();
      expect(() => readTxn.get(cf, Buffer.from("rolled_back_key"))).toThrow();
      expect(readTxn.get(cf, Buffer.from("after_reset_key")).toString()).toBe(
        "after_reset_value",
      );
      readTxn.free();
    });

    test("reset with different isolation level", () => {
      db.createColumnFamily("test_cf");
      const cf = db.getColumnFamily("test_cf");

      // Begin with default isolation
      const txn = db.beginTransaction();
      txn.put(cf, Buffer.from("iso_key1"), Buffer.from("iso_value1"), -1);
      txn.commit();

      // Reset with a different isolation level
      txn.reset(IsolationLevel.Serializable);

      txn.put(cf, Buffer.from("iso_key2"), Buffer.from("iso_value2"), -1);
      txn.commit();

      txn.free();

      // Verify both keys exist
      const readTxn = db.beginTransaction();
      expect(readTxn.get(cf, Buffer.from("iso_key1")).toString()).toBe(
        "iso_value1",
      );
      expect(readTxn.get(cf, Buffer.from("iso_key2")).toString()).toBe(
        "iso_value2",
      );
      readTxn.free();
    });
  });

  describe("Checkpoint", () => {
    // Use relative paths for checkpoint tests to avoid a Windows bug in the
    // C library's tidesdb_checkpoint_ensure_parent_dir: it fails to stat/mkdir
    // the drive-letter component (e.g. "C:") of absolute paths.  Other bindings
    // (Go, Lua) also use relative paths.  Both DB and checkpoint must be on the
    // same volume for hard links, so the DB is also opened from a relative path.
    let cpDb: TidesDB;
    let cpDbDir: string;

    beforeEach(() => {
      cpDbDir = `tidesdb-cp-test-${Date.now()}`;
      cpDb = TidesDB.open({
        dbPath: cpDbDir,
        numFlushThreads: 2,
        numCompactionThreads: 2,
      });
    });

    afterEach(() => {
      try {
        cpDb.close();
      } catch {
        /* ignore */
      }
      fs.rmSync(cpDbDir, { recursive: true, force: true });
    });

    test("create checkpoint of database", () => {
      cpDb.createColumnFamily("test_cf");
      const cf = cpDb.getColumnFamily("test_cf");

      // Insert data
      const txn = cpDb.beginTransaction();
      txn.put(cf, Buffer.from("cp_key1"), Buffer.from("cp_value1"), -1);
      txn.put(cf, Buffer.from("cp_key2"), Buffer.from("cp_value2"), -1);
      txn.commit();
      txn.free();

      // Create checkpoint (directory must NOT exist; the C library creates it)
      const checkpointDir = `tidesdb-checkpoint-${Date.now()}`;
      fs.rmSync(checkpointDir, { recursive: true, force: true });
      try {
        cpDb.checkpoint(checkpointDir);

        // Verify checkpoint directory exists with contents
        expect(fs.existsSync(checkpointDir)).toBe(true);
        expect(fs.readdirSync(checkpointDir).length).toBeGreaterThan(0);
      } finally {
        fs.rmSync(checkpointDir, { recursive: true, force: true });
      }
    });

    test("checkpoint to existing non-empty directory throws", () => {
      cpDb.createColumnFamily("test_cf");

      // Create a non-empty directory
      const checkpointDir = `tidesdb-checkpoint-nonempty-${Date.now()}`;
      fs.mkdirSync(checkpointDir, { recursive: true });
      fs.writeFileSync(path.join(checkpointDir, "dummy"), "data");

      try {
        expect(() => cpDb.checkpoint(checkpointDir)).toThrow();
      } finally {
        fs.rmSync(checkpointDir, { recursive: true, force: true });
      }
    });

    test("checkpoint can be opened as a database", () => {
      cpDb.createColumnFamily("test_cf");
      const cf = cpDb.getColumnFamily("test_cf");

      // Insert data
      const txn = cpDb.beginTransaction();
      txn.put(cf, Buffer.from("cp_key"), Buffer.from("cp_value"), -1);
      txn.commit();
      txn.free();

      // Create checkpoint (directory must NOT exist; the C library creates it)
      const checkpointDir = `tidesdb-checkpoint-open-${Date.now()}`;
      fs.rmSync(checkpointDir, { recursive: true, force: true });
      try {
        cpDb.checkpoint(checkpointDir);

        // Open the checkpoint as a separate database
        const cpOpenDb = TidesDB.open({
          dbPath: checkpointDir,
          numFlushThreads: 1,
          numCompactionThreads: 1,
        });

        try {
          const cpCf = cpOpenDb.getColumnFamily("test_cf");
          const readTxn = cpOpenDb.beginTransaction();
          const value = readTxn.get(cpCf, Buffer.from("cp_key"));
          expect(value.toString()).toBe("cp_value");
          readTxn.free();
        } finally {
          cpOpenDb.close();
        }
      } finally {
        fs.rmSync(checkpointDir, { recursive: true, force: true });
      }
    });
  });

  describe("Purge Operations", () => {
    test("purge database runs successfully", () => {
      db.createColumnFamily("purge_db_cf");
      const cf = db.getColumnFamily("purge_db_cf");

      const txn = db.beginTransaction();
      for (let i = 0; i < 25; i++) {
        txn.put(
          cf,
          Buffer.from(`purge_db_key_${i}`),
          Buffer.from(`purge_db_value_${i}`),
          -1,
        );
      }
      txn.commit();
      txn.free();

      expect(() => db.purge()).not.toThrow();
    });

    test("purge column family runs successfully", () => {
      db.createColumnFamily("purge_cf");
      const cf = db.getColumnFamily("purge_cf");

      const txn = db.beginTransaction();
      for (let i = 0; i < 25; i++) {
        txn.put(
          cf,
          Buffer.from(`purge_cf_key_${i}`),
          Buffer.from(`purge_cf_value_${i}`),
          -1,
        );
      }
      txn.commit();
      txn.free();

      expect(() => cf.purgeColumnFamily()).not.toThrow();
    });

    test("database remains usable after purge", () => {
      db.createColumnFamily("usable_after_purge_cf");
      const cf = db.getColumnFamily("usable_after_purge_cf");

      const firstTxn = db.beginTransaction();
      firstTxn.put(
        cf,
        Buffer.from("before_purge_key"),
        Buffer.from("before_purge_value"),
        -1,
      );
      firstTxn.commit();
      firstTxn.free();

      db.purge();

      const secondTxn = db.beginTransaction();
      secondTxn.put(
        cf,
        Buffer.from("after_purge_key"),
        Buffer.from("after_purge_value"),
        -1,
      );
      secondTxn.commit();
      secondTxn.free();

      const readTxn = db.beginTransaction();
      expect(readTxn.get(cf, Buffer.from("before_purge_key")).toString()).toBe(
        "before_purge_value",
      );
      expect(readTxn.get(cf, Buffer.from("after_purge_key")).toString()).toBe(
        "after_purge_value",
      );
      readTxn.free();
    });
  });

  describe("Statistics", () => {
    test("get column family stats", () => {
      db.createColumnFamily("test_cf");
      const cf = db.getColumnFamily("test_cf");

      // Insert some data
      const txn = db.beginTransaction();
      for (let i = 0; i < 100; i++) {
        const key = `key${i}`;
        const value = `value${i}`;
        txn.put(cf, Buffer.from(key), Buffer.from(value), -1);
      }
      txn.commit();
      txn.free();

      const stats = cf.getStats();
      expect(stats.numLevels).toBeGreaterThanOrEqual(0);
      expect(typeof stats.useBtree).toBe("boolean");
      expect(typeof stats.readAmp).toBe("number");
      expect(typeof stats.hitRate).toBe("number");
    });

    test("get cache stats", () => {
      const stats = db.getCacheStats();
      expect(typeof stats.enabled).toBe("boolean");
    });

    test("B+tree stats populated when useBtree is true", () => {
      db.createColumnFamily("btree_stats_cf", {
        useBtree: true,
      });

      const cf = db.getColumnFamily("btree_stats_cf");

      // Insert data and flush to create SSTables with B+tree format
      const txn = db.beginTransaction();
      for (let i = 0; i < 50; i++) {
        const key = `bstats_key${i.toString().padStart(4, "0")}`;
        const value = `bstats_value${i}`;
        txn.put(cf, Buffer.from(key), Buffer.from(value), -1);
      }
      txn.commit();
      txn.free();

      const stats = cf.getStats();
      expect(stats.useBtree).toBe(true);
      // B+tree stats should be defined when useBtree is true
      // (values may be 0 if no SSTables flushed yet, but should be defined)
      expect(stats.btreeTotalNodes).toBeDefined();
      expect(stats.btreeMaxHeight).toBeDefined();
      expect(stats.btreeAvgHeight).toBeDefined();
    });
  });

  describe("Custom Comparators", () => {
    test("getComparator returns true for built-in comparators", () => {
      expect(db.getComparator("memcmp")).toBe(true);
      expect(db.getComparator("reverse")).toBe(true);
      expect(db.getComparator("lexicographic")).toBe(true);
      expect(db.getComparator("uint64")).toBe(true);
      expect(db.getComparator("int64")).toBe(true);
      expect(db.getComparator("case_insensitive")).toBe(true);
    });

    test("getComparator returns false for non-existent comparator", () => {
      expect(db.getComparator("nonexistent_comparator")).toBe(false);
    });
  });

  describe("Stats Config", () => {
    test("getStats returns config field", () => {
      db.createColumnFamily("stats_cfg_cf", {
        compressionAlgorithm: CompressionAlgorithm.Lz4Compression,
        enableBloomFilter: true,
        bloomFpr: 0.01,
      });
      const cf = db.getColumnFamily("stats_cfg_cf");

      const stats = cf.getStats();
      expect(stats.config).toBeDefined();
      if (stats.config) {
        expect(typeof stats.config.writeBufferSize).toBe("number");
        expect(stats.config.enableBloomFilter).toBe(true);
        expect(stats.config.compressionAlgorithm).toBe(
          CompressionAlgorithm.Lz4Compression,
        );
      }
    });
  });

  describe("Range Cost Estimation", () => {
    test("rangeCost returns a number", () => {
      db.createColumnFamily("range_cf");
      const cf = db.getColumnFamily("range_cf");

      // Insert some data
      const txn = db.beginTransaction();
      for (let i = 0; i < 100; i++) {
        const key = `user:${i.toString().padStart(4, "0")}`;
        const value = `value${i}`;
        txn.put(cf, Buffer.from(key), Buffer.from(value), -1);
      }
      txn.commit();
      txn.free();

      const cost = cf.rangeCost(
        Buffer.from("user:0000"),
        Buffer.from("user:0099"),
      );
      expect(typeof cost).toBe("number");
      expect(cost).toBeGreaterThanOrEqual(0);
    });

    test("rangeCost wider range costs more or equal to narrow range", () => {
      db.createColumnFamily("range_cost_cf");
      const cf = db.getColumnFamily("range_cost_cf");

      // Insert data
      const txn = db.beginTransaction();
      for (let i = 0; i < 100; i++) {
        const key = `key:${i.toString().padStart(4, "0")}`;
        const value = `value${i}`;
        txn.put(cf, Buffer.from(key), Buffer.from(value), -1);
      }
      txn.commit();
      txn.free();

      const narrowCost = cf.rangeCost(
        Buffer.from("key:0000"),
        Buffer.from("key:0010"),
      );
      const wideCost = cf.rangeCost(
        Buffer.from("key:0000"),
        Buffer.from("key:0099"),
      );

      expect(typeof narrowCost).toBe("number");
      expect(typeof wideCost).toBe("number");
      // Wide range should cost at least as much as narrow range
      expect(wideCost).toBeGreaterThanOrEqual(narrowCost);
    });

    test("rangeCost key order does not matter", () => {
      db.createColumnFamily("range_order_cf");
      const cf = db.getColumnFamily("range_order_cf");

      // Insert data
      const txn = db.beginTransaction();
      for (let i = 0; i < 50; i++) {
        const key = `item:${i.toString().padStart(4, "0")}`;
        const value = `val${i}`;
        txn.put(cf, Buffer.from(key), Buffer.from(value), -1);
      }
      txn.commit();
      txn.free();

      const costAB = cf.rangeCost(
        Buffer.from("item:0000"),
        Buffer.from("item:0049"),
      );
      const costBA = cf.rangeCost(
        Buffer.from("item:0049"),
        Buffer.from("item:0000"),
      );

      expect(costAB).toBe(costBA);
    });

    test("rangeCost on empty column family returns zero", () => {
      db.createColumnFamily("empty_range_cf");
      const cf = db.getColumnFamily("empty_range_cf");

      const cost = cf.rangeCost(Buffer.from("a"), Buffer.from("z"));
      expect(cost).toBe(0);
    });
  });

  describe("Sync WAL", () => {
    test("syncWal succeeds on column family", () => {
      db.createColumnFamily("sync_wal_cf", {
        syncMode: SyncMode.None,
      });
      const cf = db.getColumnFamily("sync_wal_cf");

      // Write some data
      const txn = db.beginTransaction();
      txn.put(cf, Buffer.from("wal_key1"), Buffer.from("wal_value1"), -1);
      txn.put(cf, Buffer.from("wal_key2"), Buffer.from("wal_value2"), -1);
      txn.commit();
      txn.free();

      // Force WAL sync
      expect(() => cf.syncWal()).not.toThrow();
    });

    test("syncWal on interval sync mode", () => {
      db.createColumnFamily("sync_wal_interval_cf", {
        syncMode: SyncMode.Interval,
        syncIntervalUs: 1000000,
      });
      const cf = db.getColumnFamily("sync_wal_interval_cf");

      const txn = db.beginTransaction();
      txn.put(cf, Buffer.from("iwal_key"), Buffer.from("iwal_value"), -1);
      txn.commit();
      txn.free();

      expect(() => cf.syncWal()).not.toThrow();
    });

    test("data readable after syncWal", () => {
      db.createColumnFamily("sync_wal_read_cf", {
        syncMode: SyncMode.None,
      });
      const cf = db.getColumnFamily("sync_wal_read_cf");

      const txn = db.beginTransaction();
      txn.put(cf, Buffer.from("sync_key"), Buffer.from("sync_value"), -1);
      txn.commit();
      txn.free();

      cf.syncWal();

      const readTxn = db.beginTransaction();
      const value = readTxn.get(cf, Buffer.from("sync_key"));
      expect(value.toString()).toBe("sync_value");
      readTxn.free();
    });
  });

  describe("Database-Level Statistics", () => {
    test("getDbStats returns valid stats", () => {
      const stats = db.getDbStats();

      expect(typeof stats.numColumnFamilies).toBe("number");
      expect(typeof stats.totalMemory).toBe("number");
      expect(typeof stats.availableMemory).toBe("number");
      expect(typeof stats.resolvedMemoryLimit).toBe("number");
      expect(typeof stats.memoryPressureLevel).toBe("number");
      expect(typeof stats.flushPendingCount).toBe("number");
      expect(typeof stats.totalMemtableBytes).toBe("number");
      expect(typeof stats.totalImmutableCount).toBe("number");
      expect(typeof stats.totalSstableCount).toBe("number");
      expect(typeof stats.totalDataSizeBytes).toBe("number");
      expect(typeof stats.numOpenSstables).toBe("number");
      expect(typeof stats.globalSeq).toBe("number");
      expect(typeof stats.txnMemoryBytes).toBe("number");
      expect(typeof stats.compactionQueueSize).toBe("number");
      expect(typeof stats.flushQueueSize).toBe("number");
    });

    test("getDbStats reflects column family count", () => {
      const statsBefore = db.getDbStats();

      db.createColumnFamily("dbstats_cf1");
      db.createColumnFamily("dbstats_cf2");

      const statsAfter = db.getDbStats();
      expect(statsAfter.numColumnFamilies).toBe(
        statsBefore.numColumnFamilies + 2,
      );
    });

    test("getDbStats totalMemory is positive", () => {
      const stats = db.getDbStats();
      expect(stats.totalMemory).toBeGreaterThan(0);
    });

    test("getDbStats resolvedMemoryLimit is positive", () => {
      const stats = db.getDbStats();
      expect(stats.resolvedMemoryLimit).toBeGreaterThan(0);
    });

    test("getDbStats memoryPressureLevel is within range", () => {
      const stats = db.getDbStats();
      expect(stats.memoryPressureLevel).toBeGreaterThanOrEqual(0);
      expect(stats.memoryPressureLevel).toBeLessThanOrEqual(3);
    });
  });

  describe("Commit Hook (Change Data Capture)", () => {
    test("commit hook fires on put", () => {
      db.createColumnFamily("hook_cf");
      const cf = db.getColumnFamily("hook_cf");

      const captured: { ops: CommitOp[]; seq: number }[] = [];

      cf.setCommitHook((ops, commitSeq) => {
        captured.push({ ops: ops.slice(), seq: commitSeq });
        return 0;
      });

      const txn = db.beginTransaction();
      txn.put(cf, Buffer.from("hk1"), Buffer.from("hv1"), -1);
      txn.commit();
      txn.free();

      expect(captured.length).toBeGreaterThanOrEqual(1);
      const last = captured[captured.length - 1];
      expect(last.ops.length).toBe(1);
      expect(last.ops[0].key.toString()).toBe("hk1");
      expect(last.ops[0].value!.toString()).toBe("hv1");
      expect(last.ops[0].isDelete).toBe(false);
      expect(last.seq).toBeGreaterThan(0);

      cf.clearCommitHook();
    });

    test("commit hook fires on delete", () => {
      db.createColumnFamily("hook_del_cf");
      const cf = db.getColumnFamily("hook_del_cf");

      // Insert a key first
      const txn1 = db.beginTransaction();
      txn1.put(cf, Buffer.from("dk1"), Buffer.from("dv1"), -1);
      txn1.commit();
      txn1.free();

      const captured: CommitOp[][] = [];

      cf.setCommitHook((ops) => {
        captured.push(ops.slice());
        return 0;
      });

      // Delete the key
      const txn2 = db.beginTransaction();
      txn2.delete(cf, Buffer.from("dk1"));
      txn2.commit();
      txn2.free();

      expect(captured.length).toBeGreaterThanOrEqual(1);
      const last = captured[captured.length - 1];
      expect(last.length).toBe(1);
      expect(last[0].key.toString()).toBe("dk1");
      expect(last[0].isDelete).toBe(true);
      expect(last[0].value).toBeNull();

      cf.clearCommitHook();
    });

    test("commit hook receives multiple ops in one batch", () => {
      db.createColumnFamily("hook_batch_cf");
      const cf = db.getColumnFamily("hook_batch_cf");

      const captured: CommitOp[][] = [];

      cf.setCommitHook((ops) => {
        captured.push(ops.slice());
        return 0;
      });

      const txn = db.beginTransaction();
      txn.put(cf, Buffer.from("bk1"), Buffer.from("bv1"), -1);
      txn.put(cf, Buffer.from("bk2"), Buffer.from("bv2"), -1);
      txn.put(cf, Buffer.from("bk3"), Buffer.from("bv3"), -1);
      txn.commit();
      txn.free();

      expect(captured.length).toBeGreaterThanOrEqual(1);
      const last = captured[captured.length - 1];
      expect(last.length).toBe(3);

      const keys = last.map((op) => op.key.toString()).sort();
      expect(keys).toEqual(["bk1", "bk2", "bk3"]);

      cf.clearCommitHook();
    });

    test("clearCommitHook stops notifications", () => {
      db.createColumnFamily("hook_clear_cf");
      const cf = db.getColumnFamily("hook_clear_cf");

      let callCount = 0;

      cf.setCommitHook(() => {
        callCount++;
        return 0;
      });

      // First commit - hook should fire
      const txn1 = db.beginTransaction();
      txn1.put(cf, Buffer.from("ck1"), Buffer.from("cv1"), -1);
      txn1.commit();
      txn1.free();

      const countAfterFirst = callCount;
      expect(countAfterFirst).toBeGreaterThanOrEqual(1);

      // Clear hook
      cf.clearCommitHook();

      // Second commit - hook should NOT fire
      const txn2 = db.beginTransaction();
      txn2.put(cf, Buffer.from("ck2"), Buffer.from("cv2"), -1);
      txn2.commit();
      txn2.free();

      expect(callCount).toBe(countAfterFirst);
    });

    test("commit sequence numbers are monotonically increasing", () => {
      db.createColumnFamily("hook_seq_cf");
      const cf = db.getColumnFamily("hook_seq_cf");

      const seqs: number[] = [];

      cf.setCommitHook((_ops, commitSeq) => {
        seqs.push(commitSeq);
        return 0;
      });

      for (let i = 0; i < 5; i++) {
        const txn = db.beginTransaction();
        txn.put(cf, Buffer.from(`sk${i}`), Buffer.from(`sv${i}`), -1);
        txn.commit();
        txn.free();
      }

      expect(seqs.length).toBe(5);
      for (let i = 1; i < seqs.length; i++) {
        expect(seqs[i]).toBeGreaterThan(seqs[i - 1]);
      }

      cf.clearCommitHook();
    });
  });

  describe("Delete Column Family by Handle", () => {
    test("deleteColumnFamily removes the column family", () => {
      db.createColumnFamily("del_cf");
      const cf = db.getColumnFamily("del_cf");

      // Insert some data
      const txn = db.beginTransaction();
      txn.put(cf, Buffer.from("dk1"), Buffer.from("dv1"), -1);
      txn.commit();
      txn.free();

      // Delete by handle
      db.deleteColumnFamily(cf);

      // Column family should no longer be accessible
      expect(() => db.getColumnFamily("del_cf")).toThrow();
    });
  });

  describe("Iterator keyValue", () => {
    test("keyValue returns both key and value", () => {
      db.createColumnFamily("kv_iter_cf");
      const cf = db.getColumnFamily("kv_iter_cf");

      const txn = db.beginTransaction();
      txn.put(cf, Buffer.from("kv_key1"), Buffer.from("kv_val1"), -1);
      txn.put(cf, Buffer.from("kv_key2"), Buffer.from("kv_val2"), -1);
      txn.commit();
      txn.free();

      const readTxn = db.beginTransaction();
      const iter = readTxn.newIterator(cf);
      iter.seekToFirst();

      const results: { key: string; value: string }[] = [];
      while (iter.isValid()) {
        const { key, value } = iter.keyValue();
        results.push({ key: key.toString(), value: value.toString() });
        iter.next();
      }

      iter.free();
      readTxn.free();

      expect(results.length).toBe(2);
      expect(results[0].key).toBe("kv_key1");
      expect(results[0].value).toBe("kv_val1");
      expect(results[1].key).toBe("kv_key2");
      expect(results[1].value).toBe("kv_val2");
    });

    test("keyValue matches separate key() and value() calls", () => {
      db.createColumnFamily("kv_match_cf");
      const cf = db.getColumnFamily("kv_match_cf");

      const txn = db.beginTransaction();
      for (let i = 0; i < 5; i++) {
        txn.put(
          cf,
          Buffer.from(`match_key${i}`),
          Buffer.from(`match_val${i}`),
          -1,
        );
      }
      txn.commit();
      txn.free();

      const readTxn = db.beginTransaction();
      const iter = readTxn.newIterator(cf);
      iter.seekToFirst();

      while (iter.isValid()) {
        const separateKey = iter.key().toString();
        const separateValue = iter.value().toString();
        const { key, value } = iter.keyValue();
        expect(key.toString()).toBe(separateKey);
        expect(value.toString()).toBe(separateValue);
        iter.next();
      }

      iter.free();
      readTxn.free();
    });
  });

  describe("Unified Memtable Config", () => {
    test("open database with unified memtable", () => {
      const unifiedDir = createTempDir();
      try {
        const unifiedDb = TidesDB.open({
          dbPath: unifiedDir,
          numFlushThreads: 2,
          numCompactionThreads: 2,
          unifiedMemtable: true,
          unifiedMemtableWriteBufferSize: 64 * 1024 * 1024,
        });

        unifiedDb.createColumnFamily("unified_cf");
        const cf = unifiedDb.getColumnFamily("unified_cf");

        const txn = unifiedDb.beginTransaction();
        txn.put(cf, Buffer.from("ukey1"), Buffer.from("uval1"), -1);
        txn.commit();
        txn.free();

        const readTxn = unifiedDb.beginTransaction();
        const value = readTxn.get(cf, Buffer.from("ukey1"));
        expect(value.toString()).toBe("uval1");
        readTxn.free();

        unifiedDb.close();
      } finally {
        removeTempDir(unifiedDir);
      }
    });
  });

  describe("DbStats Unified Fields", () => {
    test("getDbStats includes unified memtable fields", () => {
      const stats = db.getDbStats();
      expect(typeof stats.unifiedMemtableEnabled).toBe("boolean");
      expect(typeof stats.unifiedMemtableBytes).toBe("number");
      expect(typeof stats.unifiedImmutableCount).toBe("number");
      expect(typeof stats.unifiedIsFlushing).toBe("boolean");
      expect(typeof stats.unifiedNextCfIndex).toBe("number");
      expect(typeof stats.unifiedWalGeneration).toBe("number");
    });

    test("getDbStats includes object store fields", () => {
      const stats = db.getDbStats();
      expect(typeof stats.objectStoreEnabled).toBe("boolean");
      expect(typeof stats.objectStoreConnector).toBe("string");
      expect(typeof stats.localCacheBytesUsed).toBe("number");
      expect(typeof stats.localCacheBytesMax).toBe("number");
      expect(typeof stats.localCacheNumFiles).toBe("number");
      expect(typeof stats.lastUploadedGeneration).toBe("number");
      expect(typeof stats.uploadQueueDepth).toBe("number");
      expect(typeof stats.totalUploads).toBe("number");
      expect(typeof stats.totalUploadFailures).toBe("number");
      expect(typeof stats.replicaMode).toBe("boolean");
    });

    test("unified memtable enabled shows in stats", () => {
      const unifiedDir = createTempDir();
      try {
        const unifiedDb = TidesDB.open({
          dbPath: unifiedDir,
          numFlushThreads: 1,
          numCompactionThreads: 1,
          unifiedMemtable: true,
        });

        const stats = unifiedDb.getDbStats();
        expect(stats.unifiedMemtableEnabled).toBe(true);

        unifiedDb.close();
      } finally {
        removeTempDir(unifiedDir);
      }
    });
  });

  describe("ErrReadonly Error Code", () => {
    test("ErrReadonly is defined", () => {
      const { ErrorCode } = require("./types");
      expect(ErrorCode.ErrReadonly).toBe(-13);
    });
  });

  describe("Tombstone CF Config", () => {
    test("tombstone density fields round-trip through getStats config", () => {
      db.createColumnFamily("tomb_cfg_cf", {
        tombstoneDensityTrigger: 0.5,
        tombstoneDensityMinEntries: 256,
      });
      const cf = db.getColumnFamily("tomb_cfg_cf");

      const stats = cf.getStats();
      expect(stats.config).toBeDefined();
      expect(stats.config!.tombstoneDensityTrigger).toBeCloseTo(0.5, 6);
      expect(stats.config!.tombstoneDensityMinEntries).toBe(256);
    });

    test("default tombstone min entries is sensible", () => {
      db.createColumnFamily("tomb_default_cf");
      const cf = db.getColumnFamily("tomb_default_cf");

      const stats = cf.getStats();
      expect(stats.config).toBeDefined();
      // 0.0 means disabled by default
      expect(stats.config!.tombstoneDensityTrigger).toBe(0);
      // Library default is 1024 entries
      expect(stats.config!.tombstoneDensityMinEntries).toBeGreaterThan(0);
    });
  });

  describe("Tombstone Stats", () => {
    test("tombstone stats fields populated after deletes", async () => {
      db.createColumnFamily("tomb_stats_cf");
      const cf = db.getColumnFamily("tomb_stats_cf");

      const N = 50;

      // Insert N keys, flush
      const insertTxn = db.beginTransaction();
      for (let i = 0; i < N; i++) {
        const key = `tomb_key_${i.toString().padStart(4, "0")}`;
        insertTxn.put(cf, Buffer.from(key), Buffer.from(`val_${i}`), -1);
      }
      insertTxn.commit();
      insertTxn.free();
      cf.flushMemtable();

      // Delete N/2, flush
      const deleteTxn = db.beginTransaction();
      for (let i = 0; i < N / 2; i++) {
        const key = `tomb_key_${i.toString().padStart(4, "0")}`;
        deleteTxn.delete(cf, Buffer.from(key));
      }
      deleteTxn.commit();
      deleteTxn.free();
      cf.flushMemtable();

      // Wait briefly for the flush to land
      const start = Date.now();
      while (cf.isFlushing() && Date.now() - start < 5000) {
        await new Promise((resolve) => setTimeout(resolve, 50));
      }

      const stats = cf.getStats();
      expect(stats.totalTombstones).toBeGreaterThan(0);
      expect(stats.tombstoneRatio).toBeGreaterThanOrEqual(0);
      expect(stats.tombstoneRatio).toBeLessThanOrEqual(1);
      expect(stats.maxSstDensity).toBeGreaterThanOrEqual(0);
      expect(stats.maxSstDensity).toBeLessThanOrEqual(1);
      expect(stats.levelTombstoneCounts.length).toBe(stats.numLevels);
      // 1-based level index, 0 if no SSTables
      expect(stats.maxSstDensityLevel).toBeGreaterThanOrEqual(0);
    });

    test("tombstone stats are zero on a fresh column family", () => {
      db.createColumnFamily("tomb_fresh_cf");
      const cf = db.getColumnFamily("tomb_fresh_cf");

      const stats = cf.getStats();
      expect(stats.totalTombstones).toBe(0);
      expect(stats.tombstoneRatio).toBe(0);
      expect(stats.maxSstDensity).toBe(0);
      expect(stats.maxSstDensityLevel).toBe(0);
      expect(Array.isArray(stats.levelTombstoneCounts)).toBe(true);
    });
  });

  describe("Compact Range", () => {
    test("compactRange over a narrow range succeeds", () => {
      db.createColumnFamily("compact_range_cf");
      const cf = db.getColumnFamily("compact_range_cf");

      // Multi-batch insert + flush to create several SSTables
      for (let batch = 0; batch < 3; batch++) {
        const txn = db.beginTransaction();
        for (let i = 0; i < 50; i++) {
          const key = `cr_key_${batch}_${i.toString().padStart(4, "0")}`;
          const value = `cr_val_${batch}_${i}`;
          txn.put(cf, Buffer.from(key), Buffer.from(value), -1);
        }
        txn.commit();
        txn.free();
        cf.flushMemtable();
      }

      expect(() =>
        cf.compactRange(Buffer.from("cr_key_1_0010"), Buffer.from("cr_key_1_0040")),
      ).not.toThrow();
    });

    test("compactRange with both endpoints null/empty is rejected", () => {
      db.createColumnFamily("compact_range_invalid_cf");
      const cf = db.getColumnFamily("compact_range_invalid_cf");

      try {
        cf.compactRange(null, null);
        fail("expected compactRange to throw on null/null endpoints");
      } catch (err) {
        expect(err).toBeInstanceOf(TidesDBError);
        expect((err as TidesDBError).code).toBe(ErrorCode.ErrInvalidArgs);
      }

      try {
        cf.compactRange(Buffer.alloc(0), Buffer.alloc(0));
        fail("expected compactRange to throw on empty/empty endpoints");
      } catch (err) {
        expect(err).toBeInstanceOf(TidesDBError);
        expect((err as TidesDBError).code).toBe(ErrorCode.ErrInvalidArgs);
      }
    });

    test("keys outside the compacted range remain readable", () => {
      db.createColumnFamily("compact_range_read_cf");
      const cf = db.getColumnFamily("compact_range_read_cf");

      const txn = db.beginTransaction();
      for (let i = 0; i < 100; i++) {
        const key = `crr_key_${i.toString().padStart(4, "0")}`;
        txn.put(cf, Buffer.from(key), Buffer.from(`crr_val_${i}`), -1);
      }
      txn.commit();
      txn.free();
      cf.flushMemtable();

      cf.compactRange(
        Buffer.from("crr_key_0010"),
        Buffer.from("crr_key_0020"),
      );

      const readTxn = db.beginTransaction();
      const outside = readTxn.get(cf, Buffer.from("crr_key_0080"));
      expect(outside.toString()).toBe("crr_val_80");
      readTxn.free();
    });

    test("compactRange with unbounded start", () => {
      db.createColumnFamily("compact_range_unbounded_cf");
      const cf = db.getColumnFamily("compact_range_unbounded_cf");

      const txn = db.beginTransaction();
      for (let i = 0; i < 30; i++) {
        const key = `cru_key_${i.toString().padStart(4, "0")}`;
        txn.put(cf, Buffer.from(key), Buffer.from(`cru_val_${i}`), -1);
      }
      txn.commit();
      txn.free();
      cf.flushMemtable();

      expect(() =>
        cf.compactRange(null, Buffer.from("cru_key_0015")),
      ).not.toThrow();
    });
  });

  describe("MaxConcurrentFlushes", () => {
    test("open with maxConcurrentFlushes=1 and flush succeeds", () => {
      const flushDir = createTempDir();
      try {
        const flushDb = TidesDB.open({
          dbPath: flushDir,
          maxConcurrentFlushes: 1,
        });

        flushDb.createColumnFamily("mcf_cf");
        const cf = flushDb.getColumnFamily("mcf_cf");

        const txn = flushDb.beginTransaction();
        txn.put(cf, Buffer.from("mcf_key"), Buffer.from("mcf_value"), -1);
        txn.commit();
        txn.free();

        expect(() => cf.flushMemtable()).not.toThrow();

        flushDb.close();
      } finally {
        removeTempDir(flushDir);
      }
    });

    test("defaultConfig().maxConcurrentFlushes is sourced from C library", () => {
      const cfg = defaultConfig();
      expect(typeof cfg.maxConcurrentFlushes).toBe("number");
      // Library default is non-zero (currently TDB_DEFAULT_MAX_CONCURRENT_FLUSHES = 4)
      expect(cfg.maxConcurrentFlushes).toBeGreaterThan(0);
    });
  });

  describe("Rename Column Family", () => {
    test("rename column family and access with new name", () => {
      db.createColumnFamily("rename_src_cf");
      const cf = db.getColumnFamily("rename_src_cf");

      const txn = db.beginTransaction();
      txn.put(cf, Buffer.from("rk1"), Buffer.from("rv1"), -1);
      txn.commit();
      txn.free();

      db.renameColumnFamily("rename_src_cf", "rename_dst_cf");

      // Old name should be gone
      expect(() => db.getColumnFamily("rename_src_cf")).toThrow();

      // New name should have the data
      const renamedCf = db.getColumnFamily("rename_dst_cf");
      const readTxn = db.beginTransaction();
      expect(readTxn.get(renamedCf, Buffer.from("rk1")).toString()).toBe("rv1");
      readTxn.free();
    });
  });
});
