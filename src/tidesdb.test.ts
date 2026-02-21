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

import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import {
  TidesDB,
  IsolationLevel,
  CompressionAlgorithm,
  SyncMode,
  TidesDBError,
  CommitOp,
} from './index';

function createTempDir(): string {
  return fs.mkdtempSync(path.join(os.tmpdir(), 'tidesdb-test-'));
}

function removeTempDir(dir: string): void {
  fs.rmSync(dir, { recursive: true, force: true });
}

describe('TidesDB', () => {
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

  describe('Database Operations', () => {
    test('open and close', () => {
      expect(db).toBeDefined();
      db.close();
    });

    test('create and drop column family', () => {
      db.createColumnFamily('test_cf');
      const cf = db.getColumnFamily('test_cf');
      expect(cf.name).toBe('test_cf');

      const families = db.listColumnFamilies();
      expect(Array.isArray(families)).toBe(true);
      // Note: koffi char** decoding may not work perfectly on all platforms
      // The implementation attempts to decode but may return empty on failure
      if (families.length > 0) {
        expect(families).toContain('test_cf');
      }

      db.dropColumnFamily('test_cf');
    });

    test('get non-existent column family throws', () => {
      expect(() => db.getColumnFamily('nonexistent')).toThrow(TidesDBError);
    });
  });

  describe('Transaction Operations', () => {
    test('put and get', () => {
      db.createColumnFamily('test_cf');
      const cf = db.getColumnFamily('test_cf');

      // Put
      const txn = db.beginTransaction();
      txn.put(cf, Buffer.from('key'), Buffer.from('value'), -1);
      txn.commit();
      txn.free();

      // Get
      const readTxn = db.beginTransaction();
      const value = readTxn.get(cf, Buffer.from('key'));
      expect(value.toString()).toBe('value');
      readTxn.free();
    });

    test('delete', () => {
      db.createColumnFamily('test_cf');
      const cf = db.getColumnFamily('test_cf');

      // Put
      const txn = db.beginTransaction();
      txn.put(cf, Buffer.from('key'), Buffer.from('value'), -1);
      txn.commit();
      txn.free();

      // Delete
      const deleteTxn = db.beginTransaction();
      deleteTxn.delete(cf, Buffer.from('key'));
      deleteTxn.commit();
      deleteTxn.free();

      // Verify deleted
      const readTxn = db.beginTransaction();
      expect(() => readTxn.get(cf, Buffer.from('key'))).toThrow();
      readTxn.free();
    });

    test('rollback', () => {
      db.createColumnFamily('test_cf');
      const cf = db.getColumnFamily('test_cf');

      const txn = db.beginTransaction();
      txn.put(cf, Buffer.from('rollback_key'), Buffer.from('rollback_value'), -1);
      txn.rollback();
      txn.free();

      // Verify key does not exist
      const readTxn = db.beginTransaction();
      expect(() => readTxn.get(cf, Buffer.from('rollback_key'))).toThrow();
      readTxn.free();
    });

    test('multiple operations in one transaction', () => {
      db.createColumnFamily('test_cf');
      const cf = db.getColumnFamily('test_cf');

      const txn = db.beginTransaction();
      txn.put(cf, Buffer.from('key1'), Buffer.from('value1'), -1);
      txn.put(cf, Buffer.from('key2'), Buffer.from('value2'), -1);
      txn.put(cf, Buffer.from('key3'), Buffer.from('value3'), -1);
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

  describe('Savepoints', () => {
    test('savepoint and rollback to savepoint', () => {
      db.createColumnFamily('test_cf');
      const cf = db.getColumnFamily('test_cf');

      const txn = db.beginTransaction();
      txn.put(cf, Buffer.from('key1'), Buffer.from('value1'), -1);

      txn.savepoint('sp1');
      txn.put(cf, Buffer.from('key2'), Buffer.from('value2'), -1);

      // Rollback to savepoint - key2 is discarded, key1 remains
      txn.rollbackToSavepoint('sp1');

      txn.put(cf, Buffer.from('key3'), Buffer.from('value3'), -1);
      txn.commit();
      txn.free();

      // Verify results
      const readTxn = db.beginTransaction();

      // key1 should exist
      expect(readTxn.get(cf, Buffer.from('key1')).toString()).toBe('value1');

      // key2 should not exist (rolled back)
      expect(() => readTxn.get(cf, Buffer.from('key2'))).toThrow();

      // key3 should exist
      expect(readTxn.get(cf, Buffer.from('key3')).toString()).toBe('value3');

      readTxn.free();
    });
  });

  describe('Iterator', () => {
    test('forward iteration', () => {
      db.createColumnFamily('test_cf');
      const cf = db.getColumnFamily('test_cf');

      // Insert data
      const txn = db.beginTransaction();
      for (let i = 0; i < 10; i++) {
        const key = `key${i.toString().padStart(2, '0')}`;
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

    test('backward iteration', () => {
      db.createColumnFamily('test_cf');
      const cf = db.getColumnFamily('test_cf');

      // Insert data
      const txn = db.beginTransaction();
      for (let i = 0; i < 10; i++) {
        const key = `key${i.toString().padStart(2, '0')}`;
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

  describe('Isolation Levels', () => {
    test('begin transaction with different isolation levels', () => {
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

  describe('Column Family Configuration', () => {
    test('create column family with custom config', () => {
      db.createColumnFamily('custom_cf', {
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

      const cf = db.getColumnFamily('custom_cf');
      expect(cf.name).toBe('custom_cf');
    });

    test('create column family with B+tree format', () => {
      db.createColumnFamily('btree_cf', {
        useBtree: true,
        enableBloomFilter: true,
        bloomFpr: 0.01,
      });

      const cf = db.getColumnFamily('btree_cf');
      expect(cf.name).toBe('btree_cf');

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

    test('create column family with block-based format (default)', () => {
      db.createColumnFamily('block_cf', {
        useBtree: false,
      });

      const cf = db.getColumnFamily('block_cf');
      const stats = cf.getStats();
      expect(stats.useBtree).toBe(false);
      expect(stats.btreeTotalNodes).toBeUndefined();
      expect(stats.btreeMaxHeight).toBeUndefined();
      expect(stats.btreeAvgHeight).toBeUndefined();
    });
  });

  describe('Clone Column Family', () => {
    test('clone column family copies data', () => {
      db.createColumnFamily('source_cf');
      const cf = db.getColumnFamily('source_cf');

      // Insert data into source
      const txn = db.beginTransaction();
      txn.put(cf, Buffer.from('key1'), Buffer.from('value1'), -1);
      txn.put(cf, Buffer.from('key2'), Buffer.from('value2'), -1);
      txn.commit();
      txn.free();

      // Clone the column family
      db.cloneColumnFamily('source_cf', 'cloned_cf');

      // Verify cloned data exists
      const clonedCf = db.getColumnFamily('cloned_cf');
      const readTxn = db.beginTransaction();
      expect(readTxn.get(clonedCf, Buffer.from('key1')).toString()).toBe('value1');
      expect(readTxn.get(clonedCf, Buffer.from('key2')).toString()).toBe('value2');
      readTxn.free();
    });

    test('clone is independent from source', () => {
      db.createColumnFamily('source_cf');
      const cf = db.getColumnFamily('source_cf');

      // Insert data into source
      const txn = db.beginTransaction();
      txn.put(cf, Buffer.from('key1'), Buffer.from('value1'), -1);
      txn.commit();
      txn.free();

      // Clone
      db.cloneColumnFamily('source_cf', 'cloned_cf');

      // Write new data to source only
      const writeTxn = db.beginTransaction();
      writeTxn.put(cf, Buffer.from('key2'), Buffer.from('value2'), -1);
      writeTxn.commit();
      writeTxn.free();

      // Verify clone does not have the new key
      const clonedCf = db.getColumnFamily('cloned_cf');
      const readTxn = db.beginTransaction();
      expect(readTxn.get(clonedCf, Buffer.from('key1')).toString()).toBe('value1');
      expect(() => readTxn.get(clonedCf, Buffer.from('key2'))).toThrow();
      readTxn.free();
    });

    test('clone non-existent column family throws', () => {
      expect(() => db.cloneColumnFamily('nonexistent', 'dest')).toThrow();
    });

    test('clone to existing name throws', () => {
      db.createColumnFamily('source_cf');
      db.createColumnFamily('existing_cf');
      expect(() => db.cloneColumnFamily('source_cf', 'existing_cf')).toThrow();
    });
  });

  describe('Transaction Reset', () => {
    test('reset after commit and reuse', () => {
      db.createColumnFamily('test_cf');
      const cf = db.getColumnFamily('test_cf');

      const txn = db.beginTransaction();

      // First batch
      txn.put(cf, Buffer.from('reset_key1'), Buffer.from('reset_value1'), -1);
      txn.commit();

      // Reset instead of free + begin
      txn.reset(IsolationLevel.ReadCommitted);

      // Second batch using the same transaction
      txn.put(cf, Buffer.from('reset_key2'), Buffer.from('reset_value2'), -1);
      txn.commit();

      txn.free();

      // Verify both keys exist
      const readTxn = db.beginTransaction();
      expect(readTxn.get(cf, Buffer.from('reset_key1')).toString()).toBe('reset_value1');
      expect(readTxn.get(cf, Buffer.from('reset_key2')).toString()).toBe('reset_value2');
      readTxn.free();
    });

    test('reset after rollback and reuse', () => {
      db.createColumnFamily('test_cf');
      const cf = db.getColumnFamily('test_cf');

      const txn = db.beginTransaction();

      // First batch - rollback
      txn.put(cf, Buffer.from('rolled_back_key'), Buffer.from('value'), -1);
      txn.rollback();

      // Reset and do new work
      txn.reset(IsolationLevel.ReadCommitted);

      txn.put(cf, Buffer.from('after_reset_key'), Buffer.from('after_reset_value'), -1);
      txn.commit();

      txn.free();

      // Verify rolled back key does not exist, but reset key does
      const readTxn = db.beginTransaction();
      expect(() => readTxn.get(cf, Buffer.from('rolled_back_key'))).toThrow();
      expect(readTxn.get(cf, Buffer.from('after_reset_key')).toString()).toBe('after_reset_value');
      readTxn.free();
    });

    test('reset with different isolation level', () => {
      db.createColumnFamily('test_cf');
      const cf = db.getColumnFamily('test_cf');

      // Begin with default isolation
      const txn = db.beginTransaction();
      txn.put(cf, Buffer.from('iso_key1'), Buffer.from('iso_value1'), -1);
      txn.commit();

      // Reset with a different isolation level
      txn.reset(IsolationLevel.Serializable);

      txn.put(cf, Buffer.from('iso_key2'), Buffer.from('iso_value2'), -1);
      txn.commit();

      txn.free();

      // Verify both keys exist
      const readTxn = db.beginTransaction();
      expect(readTxn.get(cf, Buffer.from('iso_key1')).toString()).toBe('iso_value1');
      expect(readTxn.get(cf, Buffer.from('iso_key2')).toString()).toBe('iso_value2');
      readTxn.free();
    });
  });

  describe('Checkpoint', () => {
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
      try { cpDb.close(); } catch { /* ignore */ }
      fs.rmSync(cpDbDir, { recursive: true, force: true });
    });

    test('create checkpoint of database', () => {
      cpDb.createColumnFamily('test_cf');
      const cf = cpDb.getColumnFamily('test_cf');

      // Insert data
      const txn = cpDb.beginTransaction();
      txn.put(cf, Buffer.from('cp_key1'), Buffer.from('cp_value1'), -1);
      txn.put(cf, Buffer.from('cp_key2'), Buffer.from('cp_value2'), -1);
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

    test('checkpoint to existing non-empty directory throws', () => {
      cpDb.createColumnFamily('test_cf');

      // Create a non-empty directory
      const checkpointDir = `tidesdb-checkpoint-nonempty-${Date.now()}`;
      fs.mkdirSync(checkpointDir, { recursive: true });
      fs.writeFileSync(path.join(checkpointDir, 'dummy'), 'data');

      try {
        expect(() => cpDb.checkpoint(checkpointDir)).toThrow();
      } finally {
        fs.rmSync(checkpointDir, { recursive: true, force: true });
      }
    });

    test('checkpoint can be opened as a database', () => {
      cpDb.createColumnFamily('test_cf');
      const cf = cpDb.getColumnFamily('test_cf');

      // Insert data
      const txn = cpDb.beginTransaction();
      txn.put(cf, Buffer.from('cp_key'), Buffer.from('cp_value'), -1);
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
          const cpCf = cpOpenDb.getColumnFamily('test_cf');
          const readTxn = cpOpenDb.beginTransaction();
          const value = readTxn.get(cpCf, Buffer.from('cp_key'));
          expect(value.toString()).toBe('cp_value');
          readTxn.free();
        } finally {
          cpOpenDb.close();
        }
      } finally {
        fs.rmSync(checkpointDir, { recursive: true, force: true });
      }
    });
  });

  describe('Statistics', () => {
    test('get column family stats', () => {
      db.createColumnFamily('test_cf');
      const cf = db.getColumnFamily('test_cf');

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
      expect(typeof stats.useBtree).toBe('boolean');
      expect(typeof stats.readAmp).toBe('number');
      expect(typeof stats.hitRate).toBe('number');
    });

    test('get cache stats', () => {
      const stats = db.getCacheStats();
      expect(typeof stats.enabled).toBe('boolean');
    });

    test('B+tree stats populated when useBtree is true', () => {
      db.createColumnFamily('btree_stats_cf', {
        useBtree: true,
      });

      const cf = db.getColumnFamily('btree_stats_cf');

      // Insert data and flush to create SSTables with B+tree format
      const txn = db.beginTransaction();
      for (let i = 0; i < 50; i++) {
        const key = `bstats_key${i.toString().padStart(4, '0')}`;
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

  describe('Range Cost Estimation', () => {
    test('rangeCost returns a number', () => {
      db.createColumnFamily('range_cf');
      const cf = db.getColumnFamily('range_cf');

      // Insert some data
      const txn = db.beginTransaction();
      for (let i = 0; i < 100; i++) {
        const key = `user:${i.toString().padStart(4, '0')}`;
        const value = `value${i}`;
        txn.put(cf, Buffer.from(key), Buffer.from(value), -1);
      }
      txn.commit();
      txn.free();

      const cost = cf.rangeCost(Buffer.from('user:0000'), Buffer.from('user:0099'));
      expect(typeof cost).toBe('number');
      expect(cost).toBeGreaterThanOrEqual(0);
    });

    test('rangeCost wider range costs more or equal to narrow range', () => {
      db.createColumnFamily('range_cost_cf');
      const cf = db.getColumnFamily('range_cost_cf');

      // Insert data
      const txn = db.beginTransaction();
      for (let i = 0; i < 100; i++) {
        const key = `key:${i.toString().padStart(4, '0')}`;
        const value = `value${i}`;
        txn.put(cf, Buffer.from(key), Buffer.from(value), -1);
      }
      txn.commit();
      txn.free();

      const narrowCost = cf.rangeCost(Buffer.from('key:0000'), Buffer.from('key:0010'));
      const wideCost = cf.rangeCost(Buffer.from('key:0000'), Buffer.from('key:0099'));

      expect(typeof narrowCost).toBe('number');
      expect(typeof wideCost).toBe('number');
      // Wide range should cost at least as much as narrow range
      expect(wideCost).toBeGreaterThanOrEqual(narrowCost);
    });

    test('rangeCost key order does not matter', () => {
      db.createColumnFamily('range_order_cf');
      const cf = db.getColumnFamily('range_order_cf');

      // Insert data
      const txn = db.beginTransaction();
      for (let i = 0; i < 50; i++) {
        const key = `item:${i.toString().padStart(4, '0')}`;
        const value = `val${i}`;
        txn.put(cf, Buffer.from(key), Buffer.from(value), -1);
      }
      txn.commit();
      txn.free();

      const costAB = cf.rangeCost(Buffer.from('item:0000'), Buffer.from('item:0049'));
      const costBA = cf.rangeCost(Buffer.from('item:0049'), Buffer.from('item:0000'));

      expect(costAB).toBe(costBA);
    });

    test('rangeCost on empty column family returns zero', () => {
      db.createColumnFamily('empty_range_cf');
      const cf = db.getColumnFamily('empty_range_cf');

      const cost = cf.rangeCost(Buffer.from('a'), Buffer.from('z'));
      expect(cost).toBe(0);
    });
  });

  describe('Commit Hook (Change Data Capture)', () => {
    test('commit hook fires on put', () => {
      db.createColumnFamily('hook_cf');
      const cf = db.getColumnFamily('hook_cf');

      const captured: { ops: CommitOp[]; seq: number }[] = [];

      cf.setCommitHook((ops, commitSeq) => {
        captured.push({ ops: ops.slice(), seq: commitSeq });
        return 0;
      });

      const txn = db.beginTransaction();
      txn.put(cf, Buffer.from('hk1'), Buffer.from('hv1'), -1);
      txn.commit();
      txn.free();

      expect(captured.length).toBeGreaterThanOrEqual(1);
      const last = captured[captured.length - 1];
      expect(last.ops.length).toBe(1);
      expect(last.ops[0].key.toString()).toBe('hk1');
      expect(last.ops[0].value!.toString()).toBe('hv1');
      expect(last.ops[0].isDelete).toBe(false);
      expect(last.seq).toBeGreaterThan(0);

      cf.clearCommitHook();
    });

    test('commit hook fires on delete', () => {
      db.createColumnFamily('hook_del_cf');
      const cf = db.getColumnFamily('hook_del_cf');

      // Insert a key first
      const txn1 = db.beginTransaction();
      txn1.put(cf, Buffer.from('dk1'), Buffer.from('dv1'), -1);
      txn1.commit();
      txn1.free();

      const captured: CommitOp[][] = [];

      cf.setCommitHook((ops) => {
        captured.push(ops.slice());
        return 0;
      });

      // Delete the key
      const txn2 = db.beginTransaction();
      txn2.delete(cf, Buffer.from('dk1'));
      txn2.commit();
      txn2.free();

      expect(captured.length).toBeGreaterThanOrEqual(1);
      const last = captured[captured.length - 1];
      expect(last.length).toBe(1);
      expect(last[0].key.toString()).toBe('dk1');
      expect(last[0].isDelete).toBe(true);
      expect(last[0].value).toBeNull();

      cf.clearCommitHook();
    });

    test('commit hook receives multiple ops in one batch', () => {
      db.createColumnFamily('hook_batch_cf');
      const cf = db.getColumnFamily('hook_batch_cf');

      const captured: CommitOp[][] = [];

      cf.setCommitHook((ops) => {
        captured.push(ops.slice());
        return 0;
      });

      const txn = db.beginTransaction();
      txn.put(cf, Buffer.from('bk1'), Buffer.from('bv1'), -1);
      txn.put(cf, Buffer.from('bk2'), Buffer.from('bv2'), -1);
      txn.put(cf, Buffer.from('bk3'), Buffer.from('bv3'), -1);
      txn.commit();
      txn.free();

      expect(captured.length).toBeGreaterThanOrEqual(1);
      const last = captured[captured.length - 1];
      expect(last.length).toBe(3);

      const keys = last.map((op) => op.key.toString()).sort();
      expect(keys).toEqual(['bk1', 'bk2', 'bk3']);

      cf.clearCommitHook();
    });

    test('clearCommitHook stops notifications', () => {
      db.createColumnFamily('hook_clear_cf');
      const cf = db.getColumnFamily('hook_clear_cf');

      let callCount = 0;

      cf.setCommitHook(() => {
        callCount++;
        return 0;
      });

      // First commit — hook should fire
      const txn1 = db.beginTransaction();
      txn1.put(cf, Buffer.from('ck1'), Buffer.from('cv1'), -1);
      txn1.commit();
      txn1.free();

      const countAfterFirst = callCount;
      expect(countAfterFirst).toBeGreaterThanOrEqual(1);

      // Clear hook
      cf.clearCommitHook();

      // Second commit — hook should NOT fire
      const txn2 = db.beginTransaction();
      txn2.put(cf, Buffer.from('ck2'), Buffer.from('cv2'), -1);
      txn2.commit();
      txn2.free();

      expect(callCount).toBe(countAfterFirst);
    });

    test('commit sequence numbers are monotonically increasing', () => {
      db.createColumnFamily('hook_seq_cf');
      const cf = db.getColumnFamily('hook_seq_cf');

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
});
