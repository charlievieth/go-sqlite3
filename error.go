// Copyright (C) 2019 Yasuhiro Matsumoto <mattn.jp@gmail.com>.
//
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

package sqlite3

/*
#ifndef USE_LIBSQLITE3
#include "sqlite3-binding.h"
#else
#include <sqlite3.h>
#endif
*/
import "C"
import (
	"sync"
	"syscall"
)

// ErrNo inherit errno.
type ErrNo int

// ErrNoMask is mask code.
const ErrNoMask = 0xff

// ErrNoExtended is extended errno.
type ErrNoExtended int

// Error implement sqlite error code.
type Error struct {
	Code         ErrNo         /* The error code returned by SQLite */
	ExtendedCode ErrNoExtended /* The extended error code returned by SQLite */
	SystemErrno  syscall.Errno /* The system errno returned by the OS through SQLite, if applicable */
	err          string        /* The error string returned by sqlite3_errmsg(),
	this usually contains more specific details. */
}

// result codes from http://www.sqlite.org/c3ref/c_abort.html
const (
	ErrError      = ErrNo(C.SQLITE_ERROR)      /* SQL error or missing database */
	ErrInternal   = ErrNo(C.SQLITE_INTERNAL)   /* Internal logic error in SQLite */
	ErrPerm       = ErrNo(C.SQLITE_PERM)       /* Access permission denied */
	ErrAbort      = ErrNo(C.SQLITE_ABORT)      /* Callback routine requested an abort */
	ErrBusy       = ErrNo(C.SQLITE_BUSY)       /* The database file is locked */
	ErrLocked     = ErrNo(C.SQLITE_LOCKED)     /* A table in the database is locked */
	ErrNomem      = ErrNo(C.SQLITE_NOMEM)      /* A malloc() failed */
	ErrReadonly   = ErrNo(C.SQLITE_READONLY)   /* Attempt to write a readonly database */
	ErrInterrupt  = ErrNo(C.SQLITE_INTERRUPT)  /* Operation terminated by sqlite3_interrupt() */
	ErrIoErr      = ErrNo(C.SQLITE_IOERR)      /* Some kind of disk I/O error occurred */
	ErrCorrupt    = ErrNo(C.SQLITE_CORRUPT)    /* The database disk image is malformed */
	ErrNotFound   = ErrNo(C.SQLITE_NOTFOUND)   /* Unknown opcode in sqlite3_file_control() */
	ErrFull       = ErrNo(C.SQLITE_FULL)       /* Insertion failed because database is full */
	ErrCantOpen   = ErrNo(C.SQLITE_CANTOPEN)   /* Unable to open the database file */
	ErrProtocol   = ErrNo(C.SQLITE_PROTOCOL)   /* Database lock protocol error */
	ErrEmpty      = ErrNo(C.SQLITE_EMPTY)      /* Database is empty */
	ErrSchema     = ErrNo(C.SQLITE_SCHEMA)     /* The database schema changed */
	ErrTooBig     = ErrNo(C.SQLITE_TOOBIG)     /* String or BLOB exceeds size limit */
	ErrConstraint = ErrNo(C.SQLITE_CONSTRAINT) /* Abort due to constraint violation */
	ErrMismatch   = ErrNo(C.SQLITE_MISMATCH)   /* Data type mismatch */
	ErrMisuse     = ErrNo(C.SQLITE_MISUSE)     /* Library used incorrectly */
	ErrNoLFS      = ErrNo(C.SQLITE_NOLFS)      /* Uses OS features not supported on host */
	ErrAuth       = ErrNo(C.SQLITE_AUTH)       /* Authorization denied */
	ErrFormat     = ErrNo(C.SQLITE_FORMAT)     /* Auxiliary database format error */
	ErrRange      = ErrNo(C.SQLITE_RANGE)      /* 2nd parameter to sqlite3_bind out of range */
	ErrNotADB     = ErrNo(C.SQLITE_NOTADB)     /* File opened that is not a database file */
	ErrNotice     = ErrNo(C.SQLITE_NOTICE)     /* Notifications from sqlite3_log() */
	ErrWarning    = ErrNo(C.SQLITE_WARNING)    /* Warnings from sqlite3_log() */
)

// Error return error message from errno.
func (err ErrNo) Error() string {
	return Error{Code: err}.Error()
}

// Extend return extended errno.
func (err ErrNo) Extend(by int) ErrNoExtended {
	return ErrNoExtended(int(err) | (by << 8))
}

// Error return error message that is extended code.
func (err ErrNoExtended) Error() string {
	return Error{Code: ErrNo(C.int(err) & ErrNoMask), ExtendedCode: err}.Error()
}

func (err Error) Error() string {
	var str string
	if err.err != "" {
		str = err.err
	} else {
		str = errorString(int(err.Code))
	}
	if err.SystemErrno != 0 {
		str += ": " + err.SystemErrno.Error()
	}
	return str
}

// result codes from http://www.sqlite.org/c3ref/c_abort_rollback.html
const (
	ErrIoErrRead              = ErrNoExtended(ErrIoErr | 1<<8)
	ErrIoErrShortRead         = ErrNoExtended(ErrIoErr | 2<<8)
	ErrIoErrWrite             = ErrNoExtended(ErrIoErr | 3<<8)
	ErrIoErrFsync             = ErrNoExtended(ErrIoErr | 4<<8)
	ErrIoErrDirFsync          = ErrNoExtended(ErrIoErr | 5<<8)
	ErrIoErrTruncate          = ErrNoExtended(ErrIoErr | 6<<8)
	ErrIoErrFstat             = ErrNoExtended(ErrIoErr | 7<<8)
	ErrIoErrUnlock            = ErrNoExtended(ErrIoErr | 8<<8)
	ErrIoErrRDlock            = ErrNoExtended(ErrIoErr | 9<<8)
	ErrIoErrDelete            = ErrNoExtended(ErrIoErr | 10<<8)
	ErrIoErrBlocked           = ErrNoExtended(ErrIoErr | 11<<8)
	ErrIoErrNoMem             = ErrNoExtended(ErrIoErr | 12<<8)
	ErrIoErrAccess            = ErrNoExtended(ErrIoErr | 13<<8)
	ErrIoErrCheckReservedLock = ErrNoExtended(ErrIoErr | 14<<8)
	ErrIoErrLock              = ErrNoExtended(ErrIoErr | 15<<8)
	ErrIoErrClose             = ErrNoExtended(ErrIoErr | 16<<8)
	ErrIoErrDirClose          = ErrNoExtended(ErrIoErr | 17<<8)
	ErrIoErrSHMOpen           = ErrNoExtended(ErrIoErr | 18<<8)
	ErrIoErrSHMSize           = ErrNoExtended(ErrIoErr | 19<<8)
	ErrIoErrSHMLock           = ErrNoExtended(ErrIoErr | 20<<8)
	ErrIoErrSHMMap            = ErrNoExtended(ErrIoErr | 21<<8)
	ErrIoErrSeek              = ErrNoExtended(ErrIoErr | 22<<8)
	ErrIoErrDeleteNoent       = ErrNoExtended(ErrIoErr | 23<<8)
	ErrIoErrMMap              = ErrNoExtended(ErrIoErr | 24<<8)
	ErrIoErrGetTempPath       = ErrNoExtended(ErrIoErr | 25<<8)
	ErrIoErrConvPath          = ErrNoExtended(ErrIoErr | 26<<8)
	ErrLockedSharedCache      = ErrNoExtended(ErrLocked | 1<<8)
	ErrBusyRecovery           = ErrNoExtended(ErrBusy | 1<<8)
	ErrBusySnapshot           = ErrNoExtended(ErrBusy | 2<<8)
	ErrCantOpenNoTempDir      = ErrNoExtended(ErrCantOpen | 1<<8)
	ErrCantOpenIsDir          = ErrNoExtended(ErrCantOpen | 2<<8)
	ErrCantOpenFullPath       = ErrNoExtended(ErrCantOpen | 3<<8)
	ErrCantOpenConvPath       = ErrNoExtended(ErrCantOpen | 4<<8)
	ErrCorruptVTab            = ErrNoExtended(ErrCorrupt | 1<<8)
	ErrReadonlyRecovery       = ErrNoExtended(ErrReadonly | 1<<8)
	ErrReadonlyCantLock       = ErrNoExtended(ErrReadonly | 2<<8)
	ErrReadonlyRollback       = ErrNoExtended(ErrReadonly | 3<<8)
	ErrReadonlyDbMoved        = ErrNoExtended(ErrReadonly | 4<<8)
	ErrAbortRollback          = ErrNoExtended(ErrAbort | 2<<8)
	ErrConstraintCheck        = ErrNoExtended(ErrConstraint | 1<<8)
	ErrConstraintCommitHook   = ErrNoExtended(ErrConstraint | 2<<8)
	ErrConstraintForeignKey   = ErrNoExtended(ErrConstraint | 3<<8)
	ErrConstraintFunction     = ErrNoExtended(ErrConstraint | 4<<8)
	ErrConstraintNotNull      = ErrNoExtended(ErrConstraint | 5<<8)
	ErrConstraintPrimaryKey   = ErrNoExtended(ErrConstraint | 6<<8)
	ErrConstraintTrigger      = ErrNoExtended(ErrConstraint | 7<<8)
	ErrConstraintUnique       = ErrNoExtended(ErrConstraint | 8<<8)
	ErrConstraintVTab         = ErrNoExtended(ErrConstraint | 9<<8)
	ErrConstraintRowID        = ErrNoExtended(ErrConstraint | 10<<8)
	ErrNoticeRecoverWAL       = ErrNoExtended(ErrNotice | 1<<8)
	ErrNoticeRecoverRollback  = ErrNoExtended(ErrNotice | 2<<8)
	ErrWarningAutoIndex       = ErrNoExtended(ErrWarning | 1<<8)
)

var errStrCache sync.Map // int => string

// errorString returns the result of sqlite3_errstr for result code rv,
// which may be cached.
func errorString(rv int) string {
	if v, ok := errStrCache.Load(rv); ok {
		return v.(string)
	}
	s := C.GoString(C.sqlite3_errstr(C.int(rv)))
	// Prevent the cache from growing unbounded by ignoring invalid
	// error codes.
	if s != "unknown error" {
		if v, loaded := errStrCache.LoadOrStore(rv, s); loaded {
			s = v.(string)
		}
	}
	return s
}
