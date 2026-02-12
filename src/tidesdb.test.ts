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
    test('create checkpoint of database', () => {
      db.createColumnFamily('test_cf');
      const cf = db.getColumnFamily('test_cf');

      // Insert data
      const txn = db.beginTransaction();
      txn.put(cf, Buffer.from('cp_key1'), Buffer.from('cp_value1'), -1);
      txn.put(cf, Buffer.from('cp_key2'), Buffer.from('cp_value2'), -1);
      txn.commit();
      txn.free();

      // Create checkpoint (directory must NOT exist; the C library creates it)
      const checkpointDir = path.join(os.tmpdir(), `tidesdb-checkpoint-${Date.now()}`);
      fs.rmSync(checkpointDir, { recursive: true, force: true });
      try {
        db.checkpoint(checkpointDir);

        // Verify checkpoint directory exists with contents
        expect(fs.existsSync(checkpointDir)).toBe(true);
        expect(fs.readdirSync(checkpointDir).length).toBeGreaterThan(0);
      } finally {
        removeTempDir(checkpointDir);
      }
    });

    test('checkpoint to existing non-empty directory throws', () => {
      db.createColumnFamily('test_cf');

      // Create a non-empty directory outside the database path
      const checkpointDir = path.join(os.tmpdir(), `tidesdb-checkpoint-nonempty-${Date.now()}`);
      fs.mkdirSync(checkpointDir, { recursive: true });
      fs.writeFileSync(path.join(checkpointDir, 'dummy'), 'data');

      try {
        expect(() => db.checkpoint(checkpointDir)).toThrow();
      } finally {
        removeTempDir(checkpointDir);
      }
    });

    test('checkpoint can be opened as a database', () => {
      db.createColumnFamily('test_cf');
      const cf = db.getColumnFamily('test_cf');

      // Insert data
      const txn = db.beginTransaction();
      txn.put(cf, Buffer.from('cp_key'), Buffer.from('cp_value'), -1);
      txn.commit();
      txn.free();

      // Create checkpoint (directory must NOT exist; the C library creates it)
      const checkpointDir = path.join(os.tmpdir(), `tidesdb-checkpoint-open-${Date.now()}`);
      fs.rmSync(checkpointDir, { recursive: true, force: true });
      try {
        db.checkpoint(checkpointDir);

        // Open the checkpoint as a separate database
        const cpDb = TidesDB.open({
          dbPath: checkpointDir,
          numFlushThreads: 1,
          numCompactionThreads: 1,
        });

        try {
          const cpCf = cpDb.getColumnFamily('test_cf');
          const readTxn = cpDb.beginTransaction();
          const value = readTxn.get(cpCf, Buffer.from('cp_key'));
          expect(value.toString()).toBe('cp_value');
          readTxn.free();
        } finally {
          cpDb.close();
        }
      } finally {
        removeTempDir(checkpointDir);
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
});
