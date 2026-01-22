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
  tidesdb_get_stats,
  tidesdb_free_stats,
  tidesdb_compact,
  tidesdb_flush_memtable,
} from './ffi';
import { checkResult } from './error';
import { Stats } from './types';

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

    const statsPtr = statsPtrOut[0] as Record<string, unknown>;

    const numLevels = (statsPtr?.num_levels ?? 0) as number;
    const memtableSize = (statsPtr?.memtable_size ?? 0) as number;

    // For now, return basic stats - full stats parsing requires more complex memory handling
    const levelSizes: number[] = [];
    const levelNumSSTables: number[] = [];

    if (statsPtr) {
      tidesdb_free_stats(statsPtr);
    }

    return {
      numLevels,
      memtableSize,
      levelSizes,
      levelNumSSTables,
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
}
