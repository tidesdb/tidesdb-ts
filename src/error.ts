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

import { ErrorCode } from './types';

/**
 * TidesDB error class.
 */
export class TidesDBError extends Error {
  public readonly code: ErrorCode;
  public readonly context: string;

  constructor(code: ErrorCode, context: string = '') {
    const message = TidesDBError.getErrorMessage(code);
    super(context ? `${context}: ${message}` : message);
    this.name = 'TidesDBError';
    this.code = code;
    this.context = context;
  }

  private static getErrorMessage(code: ErrorCode): string {
    switch (code) {
      case ErrorCode.Success:
        return 'success';
      case ErrorCode.ErrMemory:
        return 'memory allocation failed';
      case ErrorCode.ErrInvalidArgs:
        return 'invalid arguments';
      case ErrorCode.ErrNotFound:
        return 'not found';
      case ErrorCode.ErrIO:
        return 'I/O error';
      case ErrorCode.ErrCorruption:
        return 'data corruption';
      case ErrorCode.ErrExists:
        return 'already exists';
      case ErrorCode.ErrConflict:
        return 'transaction conflict';
      case ErrorCode.ErrTooLarge:
        return 'key or value too large';
      case ErrorCode.ErrMemoryLimit:
        return 'memory limit exceeded';
      case ErrorCode.ErrInvalidDB:
        return 'invalid database handle';
      case ErrorCode.ErrLocked:
        return 'database is locked';
      default:
        return 'unknown error';
    }
  }
}

/**
 * Check result code and throw error if not success.
 */
export function checkResult(code: number, context: string = ''): void {
  if (code !== ErrorCode.Success) {
    throw new TidesDBError(code as ErrorCode, context);
  }
}
