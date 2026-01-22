# TidesDB TypeScript Bindings

TypeScript bindings for [TidesDB](https://github.com/tidesdb/tidesdb) - A high-performance embedded key-value storage engine.

## Features

- **MVCC** with five isolation levels from READ UNCOMMITTED to SERIALIZABLE
- **Column families** - isolated key-value stores with independent configuration
- **Bidirectional iterators** with forward/backward traversal and seek support
- **TTL support** with automatic key expiration
- **Compression** - LZ4, LZ4 Fast, ZSTD, Snappy, or no compression
- **Bloom filters** with configurable false positive rates
- **Global block CLOCK cache** for hot blocks
- **Savepoints** for partial transaction rollback
- **Six built-in comparators** plus custom registration

## Prerequisites

- Node.js >= 16.0.0
- TidesDB C library installed on your system
- Build tools for native modules (node-gyp)

### Installing TidesDB

Follow the installation instructions at [TidesDB GitHub](https://github.com/tidesdb/tidesdb).

On Linux, ensure the library is in your library path:
```bash
export LD_LIBRARY_PATH=/usr/local/lib:$LD_LIBRARY_PATH
```

On macOS:
```bash
export DYLD_LIBRARY_PATH=/usr/local/lib:$DYLD_LIBRARY_PATH
```

## Installation

```bash
npm install tidesdb
```

## Usage

### Basic Example

```typescript
import { TidesDB, IsolationLevel } from 'tidesdb';

// Open database
const db = TidesDB.open({
  dbPath: './mydb',
  numFlushThreads: 2,
  numCompactionThreads: 2,
});

// Create a column family
db.createColumnFamily('my_cf');

// Get the column family
const cf = db.getColumnFamily('my_cf');

// Write data in a transaction
const txn = db.beginTransaction();
txn.put(cf, Buffer.from('key1'), Buffer.from('value1'), -1);
txn.put(cf, Buffer.from('key2'), Buffer.from('value2'), -1);
txn.commit();
txn.free();

// Read data
const readTxn = db.beginTransaction();
const value = readTxn.get(cf, Buffer.from('key1'));
console.log('Value:', value.toString());
readTxn.free();

// Close database
db.close();
```

### Iterating Over Data

```typescript
const txn = db.beginTransaction();
const iter = txn.newIterator(cf);

// Forward iteration
iter.seekToFirst();
while (iter.isValid()) {
  const key = iter.key();
  const val = iter.value();
  console.log(`Key: ${key.toString()}, Value: ${val.toString()}`);
  iter.next();
}

// Backward iteration
iter.seekToLast();
while (iter.isValid()) {
  const key = iter.key();
  const val = iter.value();
  console.log(`Key: ${key.toString()}, Value: ${val.toString()}`);
  iter.prev();
}

iter.free();
txn.free();
```

### Using TTL (Time-To-Live)

```typescript
const txn = db.beginTransaction();

// Set TTL to 60 seconds from now
const ttl = Math.floor(Date.now() / 1000) + 60;
txn.put(cf, Buffer.from('temp_key'), Buffer.from('temp_value'), ttl);
txn.commit();
txn.free();
```

### Savepoints

```typescript
const txn = db.beginTransaction();

txn.put(cf, Buffer.from('key1'), Buffer.from('value1'), -1);
txn.savepoint('sp1');

txn.put(cf, Buffer.from('key2'), Buffer.from('value2'), -1);

// Rollback to savepoint - key2 is discarded, key1 remains
txn.rollbackToSavepoint('sp1');

txn.put(cf, Buffer.from('key3'), Buffer.from('value3'), -1);
txn.commit();
txn.free();
```

### Isolation Levels

```typescript
import { IsolationLevel } from 'tidesdb';

// Begin transaction with specific isolation level
const txn = db.beginTransactionWithIsolation(IsolationLevel.Serializable);
// ... perform operations
txn.commit();
txn.free();
```

### Column Family Configuration

```typescript
import { CompressionAlgorithm, SyncMode, IsolationLevel } from 'tidesdb';

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
```

### Statistics

```typescript
// Column family stats
const cf = db.getColumnFamily('my_cf');
const stats = cf.getStats();
console.log('Num levels:', stats.numLevels);
console.log('Memtable size:', stats.memtableSize);

// Cache stats
const cacheStats = db.getCacheStats();
console.log('Cache hit rate:', cacheStats.hitRate);
```

## API Reference

### TidesDB

- `TidesDB.open(config: Config): TidesDB` - Open a database
- `close(): void` - Close the database
- `createColumnFamily(name: string, config?: ColumnFamilyConfig): void` - Create a column family
- `dropColumnFamily(name: string): void` - Drop a column family
- `getColumnFamily(name: string): ColumnFamily` - Get a column family
- `listColumnFamilies(): string[]` - List all column families
- `beginTransaction(): Transaction` - Begin a transaction
- `beginTransactionWithIsolation(isolation: IsolationLevel): Transaction` - Begin with isolation level
- `getCacheStats(): CacheStats` - Get cache statistics

### Transaction

- `put(cf: ColumnFamily, key: Buffer, value: Buffer, ttl?: number): void` - Put a key-value pair
- `get(cf: ColumnFamily, key: Buffer): Buffer` - Get a value
- `delete(cf: ColumnFamily, key: Buffer): void` - Delete a key
- `commit(): void` - Commit the transaction
- `rollback(): void` - Rollback the transaction
- `savepoint(name: string): void` - Create a savepoint
- `rollbackToSavepoint(name: string): void` - Rollback to a savepoint
- `releaseSavepoint(name: string): void` - Release a savepoint
- `newIterator(cf: ColumnFamily): Iterator` - Create an iterator
- `free(): void` - Free transaction resources

### Iterator

- `seekToFirst(): void` - Seek to first key
- `seekToLast(): void` - Seek to last key
- `seek(key: Buffer): void` - Seek to key >= target
- `seekForPrev(key: Buffer): void` - Seek to key <= target
- `isValid(): boolean` - Check if positioned at valid entry
- `next(): void` - Move to next entry
- `prev(): void` - Move to previous entry
- `key(): Buffer` - Get current key
- `value(): Buffer` - Get current value
- `free(): void` - Free iterator resources

### ColumnFamily

- `name: string` - Column family name
- `getStats(): Stats` - Get statistics
- `compact(): void` - Trigger compaction
- `flushMemtable(): void` - Flush memtable

## License

Mozilla Public License, v. 2.0 - See [LICENSE](LICENSE) for details.
