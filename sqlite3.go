// Copyright (C) 2019 Yasuhiro Matsumoto <mattn.jp@gmail.com>.
// Copyright (C) 2018 G.J.R. Timmer <gjr.timmer@gmail.com>.
//
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

//go:build cgo
// +build cgo

package sqlite3

/*
#cgo CFLAGS: -std=gnu99
#cgo CFLAGS: -DSQLITE_ENABLE_RTREE
#cgo CFLAGS: -DSQLITE_THREADSAFE=1
#cgo CFLAGS: -DHAVE_USLEEP=1
#cgo CFLAGS: -DSQLITE_ENABLE_FTS3
#cgo CFLAGS: -DSQLITE_ENABLE_FTS3_PARENTHESIS
#cgo CFLAGS: -DSQLITE_TRACE_SIZE_LIMIT=15
#cgo CFLAGS: -DSQLITE_OMIT_DEPRECATED
#cgo CFLAGS: -DSQLITE_DEFAULT_WAL_SYNCHRONOUS=1
#cgo CFLAGS: -DSQLITE_ENABLE_UPDATE_DELETE_LIMIT
#cgo CFLAGS: -Wno-deprecated-declarations
#cgo openbsd CFLAGS: -I/usr/local/include
#cgo openbsd LDFLAGS: -L/usr/local/lib
#ifndef USE_LIBSQLITE3
#include "sqlite3-binding.h"
#else
#include <sqlite3.h>
#endif
#include <stdlib.h>
#include <string.h>

#ifdef __CYGWIN__
# include <errno.h>
#endif

#ifndef SQLITE_OPEN_READWRITE
# define SQLITE_OPEN_READWRITE 0
#endif

#ifndef SQLITE_OPEN_FULLMUTEX
# define SQLITE_OPEN_FULLMUTEX 0
#endif

#ifndef SQLITE_DETERMINISTIC
# define SQLITE_DETERMINISTIC 0
#endif

#if defined(HAVE_PREAD64) && defined(HAVE_PWRITE64)
# undef USE_PREAD
# undef USE_PWRITE
# define USE_PREAD64 1
# define USE_PWRITE64 1
#elif defined(HAVE_PREAD) && defined(HAVE_PWRITE)
# undef USE_PREAD
# undef USE_PWRITE
# define USE_PREAD64 1
# define USE_PWRITE64 1
#endif

static int
_sqlite3_open_v2(const char *filename, sqlite3 **ppDb, int flags, const char *zVfs) {
#ifdef SQLITE_OPEN_URI
  return sqlite3_open_v2(filename, ppDb, flags | SQLITE_OPEN_URI, zVfs);
#else
  return sqlite3_open_v2(filename, ppDb, flags, zVfs);
#endif
}

static int
_sqlite3_bind_text(sqlite3_stmt *stmt, int n, char *p, int np) {
  return sqlite3_bind_text(stmt, n, p, np, SQLITE_TRANSIENT);
}

static int
_sqlite3_bind_blob(sqlite3_stmt *stmt, int n, void *p, int np) {
  return sqlite3_bind_blob(stmt, n, p, np, SQLITE_TRANSIENT);
}

#include <stdio.h>
#include <stdint.h>

static int
_sqlite3_exec(sqlite3* db, const char* pcmd, long long* rowid, long long* changes)
{
  int rv = sqlite3_exec(db, pcmd, 0, 0, 0);
  *rowid = (long long) sqlite3_last_insert_rowid(db);
  *changes = (long long) sqlite3_changes(db);
  return rv;
}

#ifdef SQLITE_ENABLE_UNLOCK_NOTIFY
extern int _sqlite3_step_blocking(sqlite3_stmt *stmt);
extern int _sqlite3_step_row_blocking(sqlite3_stmt* stmt, long long* rowid, long long* changes);
extern int _sqlite3_prepare_v2_blocking(sqlite3 *db, const char *zSql, int nBytes, sqlite3_stmt **ppStmt, const char **pzTail);

static int
_sqlite3_step_internal(sqlite3_stmt *stmt)
{
  return _sqlite3_step_blocking(stmt);
}

static int
_sqlite3_step_row_internal(sqlite3_stmt* stmt, long long* rowid, long long* changes)
{
  return _sqlite3_step_row_blocking(stmt, rowid, changes);
}

static int
_sqlite3_prepare_v2_internal(sqlite3 *db, const char *zSql, int nBytes, sqlite3_stmt **ppStmt, const char **pzTail)
{
  return _sqlite3_prepare_v2_blocking(db, zSql, nBytes, ppStmt, pzTail);
}

#else
static int
_sqlite3_step_internal(sqlite3_stmt *stmt)
{
  return sqlite3_step(stmt);
}

static int
_sqlite3_step_row_internal(sqlite3_stmt* stmt, long long* rowid, long long* changes)
{
  int rv = sqlite3_step(stmt);
  sqlite3* db = sqlite3_db_handle(stmt);
  *rowid = (long long) sqlite3_last_insert_rowid(db);
  *changes = (long long) sqlite3_changes(db);
  return rv;
}

static int
_sqlite3_prepare_v2_internal(sqlite3 *db, const char *zSql, int nBytes, sqlite3_stmt **ppStmt, const char **pzTail)
{
  return sqlite3_prepare_v2(db, zSql, nBytes, ppStmt, pzTail);
}
#endif

#define GO_SQLITE_MULTIPLE_QUERIES -1

// Our own implementation of ctype.h's isspace (for simplicity and to avoid
// whatever locale shenanigans are involved with the Libc's isspace).
static int _sqlite3_isspace(unsigned char c) {
	return c == ' ' || c - '\t' < 5;
}

static int _sqlite3_prepare_query(sqlite3 *db, const char *zSql, int nBytes,
	sqlite3_stmt **ppStmt, int *paramCount) {

	const char *tail;
	int rc = _sqlite3_prepare_v2_internal(db, zSql, nBytes, ppStmt, &tail);
	if (rc != SQLITE_OK) {
		return rc;
	}
	*paramCount = sqlite3_bind_parameter_count(*ppStmt);

	// Check if the SQL query contains multiple statements.

	// Trim leading space to handle queries with trailing whitespace.
	// This can save us an additional call to sqlite3_prepare_v2.
	const char *end = zSql + nBytes;
	while (tail < end && _sqlite3_isspace(*tail)) {
		tail++;
	}
	nBytes -= (tail - zSql);

	// Attempt to parse the remaining SQL, if any.
	if (nBytes > 0 && *tail) {
		sqlite3_stmt *stmt;
		rc = _sqlite3_prepare_v2_internal(db, tail, nBytes, &stmt, NULL);
		if (rc != SQLITE_OK) {
			// sqlite3 will return OK and a NULL statement if it was
			goto error;
		}
		if (stmt != NULL) {
			sqlite3_finalize(stmt);
			rc = GO_SQLITE_MULTIPLE_QUERIES;
			goto error;
		}
	}

	// Ok, the SQL contained one valid statement.
	return SQLITE_OK;

error:
	if (*ppStmt) {
		sqlite3_finalize(*ppStmt);
	}
	return rc;
}

static int _sqlite3_prepare_v2(sqlite3 *db, const char *zSql, int nBytes, sqlite3_stmt **ppStmt, int *oBytes) {
	const char *tail = NULL;
	int rv = _sqlite3_prepare_v2_internal(db, zSql, nBytes, ppStmt, &tail);
	if (rv != SQLITE_OK) {
		return rv;
	}
	if (tail == NULL) {
		return rv; // NB: this should not happen
	}
	// Set oBytes to the number of bytes consumed instead of using the **pzTail
	// out param since that requires storing a Go pointer in a C pointer, which
	// is not allowed by CGO and will cause runtime.cgoCheckPointer to fail.
	*oBytes = tail - zSql;
	return rv;
}

// _sqlite3_exec_no_args executes all of the statements in zSql. None of the
// statements are allowed to have positional arguments.
int _sqlite3_exec_no_args(sqlite3 *db, const char *zSql, int nBytes, int64_t *rowid, int64_t *changes) {
	while (*zSql && nBytes > 0) {
		sqlite3_stmt *stmt;
		const char *tail;
		int rv = sqlite3_prepare_v2(db, zSql, nBytes, &stmt, &tail);
		if (rv != SQLITE_OK) {
			return rv;
		}

		// Process statement
		do {
			rv = _sqlite3_step_internal(stmt);
		} while (rv == SQLITE_ROW);

		// Only record the number of changes made by the last statement.
		*changes = sqlite3_changes64(db);
		*rowid = sqlite3_last_insert_rowid(db);

		sqlite3_finalize(stmt);
		if (rv != SQLITE_OK && rv != SQLITE_DONE) {
			return rv;
		}

		nBytes -= tail - zSql;
		zSql = tail;
	}
	return SQLITE_OK;
}

void _sqlite3_result_text(sqlite3_context* ctx, const char* s) {
  sqlite3_result_text(ctx, s, -1, &free);
}

void _sqlite3_result_blob(sqlite3_context* ctx, const void* b, int l) {
  sqlite3_result_blob(ctx, b, l, SQLITE_TRANSIENT);
}


int _sqlite3_create_function(
  sqlite3 *db,
  const char *zFunctionName,
  int nArg,
  int eTextRep,
  uintptr_t pApp,
  void (*xFunc)(sqlite3_context*,int,sqlite3_value**),
  void (*xStep)(sqlite3_context*,int,sqlite3_value**),
  void (*xFinal)(sqlite3_context*)
) {
  return sqlite3_create_function(db, zFunctionName, nArg, eTextRep, (void*) pApp, xFunc, xStep, xFinal);
}

void callbackTrampoline(sqlite3_context*, int, sqlite3_value**);
void stepTrampoline(sqlite3_context*, int, sqlite3_value**);
void doneTrampoline(sqlite3_context*);

int compareTrampoline(void*, int, char*, int, char*);
int commitHookTrampoline(void*);
void rollbackHookTrampoline(void*);
void updateHookTrampoline(void*, int, char*, char*, sqlite3_int64);

int authorizerTrampoline(void*, int, char*, char*, char*, char*);

#ifdef SQLITE_LIMIT_WORKER_THREADS
# define _SQLITE_HAS_LIMIT
# define SQLITE_LIMIT_LENGTH                    0
# define SQLITE_LIMIT_SQL_LENGTH                1
# define SQLITE_LIMIT_COLUMN                    2
# define SQLITE_LIMIT_EXPR_DEPTH                3
# define SQLITE_LIMIT_COMPOUND_SELECT           4
# define SQLITE_LIMIT_VDBE_OP                   5
# define SQLITE_LIMIT_FUNCTION_ARG              6
# define SQLITE_LIMIT_ATTACHED                  7
# define SQLITE_LIMIT_LIKE_PATTERN_LENGTH       8
# define SQLITE_LIMIT_VARIABLE_NUMBER           9
# define SQLITE_LIMIT_TRIGGER_DEPTH            10
# define SQLITE_LIMIT_WORKER_THREADS           11
# else
# define SQLITE_LIMIT_WORKER_THREADS           11
#endif

static int _sqlite3_limit(sqlite3* db, int limitId, int newLimit) {
#ifndef _SQLITE_HAS_LIMIT
  return -1;
#else
  return sqlite3_limit(db, limitId, newLimit);
#endif
}

#if SQLITE_VERSION_NUMBER < 3012000
static int sqlite3_system_errno(sqlite3 *db) {
  return 0;
}
#endif

#define GO_SQLITE3_DECL_DATE (1 << 7)
#define GO_SQLITE3_DECL_BOOL (1 << 6)
#define GO_SQLITE3_DECL_BLOB (1 << 5)
#define GO_SQLITE3_DECL_MASK (GO_SQLITE3_DECL_DATE | GO_SQLITE3_DECL_BOOL | GO_SQLITE3_DECL_BLOB)
#define GO_SQLITE3_TYPE_MASK (GO_SQLITE3_DECL_BLOB - 1)

#ifdef _Static_assert
_Static_assert(
	!(GO_SQLITE3_DECL_MASK & (SQLITE_INTEGER|SQLITE_FLOAT|SQLITE_BLOB|SQLITE_NULL|SQLITE3_TEXT)),
	"Error: GO_SQLITE3_DECL_MASK clobbers the sqlite3 fundamental datatypes");
#endif

// _sqlite3_column_decltypes stores the declared column type in the typs array.
// This function must always be called before _sqlite3_column_types since it
// overwrites the datatype.
static void _sqlite3_column_decltypes(sqlite3_stmt* stmt, uint8_t *typs, int ntyps) {
	for (int i = 0; i < ntyps; i++) {
		const char *typ = sqlite3_column_decltype(stmt, i);
		if (typ == NULL) {
			typs[i] = 0;
			continue;
		}
		switch (typ[0]) {
		case 'b':
		case 'B':
			if (!sqlite3_stricmp(typ, "boolean")) {
				typs[i] = GO_SQLITE3_DECL_BOOL;
			} else if (!sqlite3_stricmp(typ, "blob")) {
				typs[i] = GO_SQLITE3_DECL_BLOB;
			}
			break;
		case 'd':
		case 'D':
			if (!sqlite3_stricmp(typ, "date") || !sqlite3_stricmp(typ, "datetime")) {
				typs[i] = GO_SQLITE3_DECL_DATE;
			}
			break;
		case 't':
		case 'T':
			if (!sqlite3_stricmp(typ, "timestamp")) {
				typs[i] = GO_SQLITE3_DECL_DATE;
			}
			break;
		default:
			typs[i] = 0;
		}
	}
}

static void _sqlite3_column_types(sqlite3_stmt *stmt, uint8_t *typs, int ntyps) {
	for (int i = 0; i < ntyps; i++) {
		typs[i] &= GO_SQLITE3_DECL_MASK; // clear lower bits
		typs[i] |= (uint8_t)sqlite3_column_type(stmt, i);
	}
}

typedef struct {
	const unsigned char *value;
	int                 bytes;
} go_sqlite3_text_column;

// _sqlite3_column_text fetches the text for a column and its size in one CGO call.
static go_sqlite3_text_column _sqlite3_column_text(sqlite3_stmt *stmt, int idx) {
	go_sqlite3_text_column r;
	r.value = sqlite3_column_text(stmt, idx);
	if (r.value) {
		r.bytes = sqlite3_column_bytes(stmt, idx);
	} else {
		r.bytes = 0;
	}
	return r;
}

// _sqlite3_column_blob fetches the blob for a column and its size in one CGO call.
static go_sqlite3_text_column _sqlite3_column_blob(sqlite3_stmt *stmt, int idx) {
	go_sqlite3_text_column r;
	r.value = sqlite3_column_blob(stmt, idx);
	if (r.value) {
		r.bytes = sqlite3_column_bytes(stmt, idx);
	} else {
		r.bytes = 0;
	}
	return r;
}
*/
import "C"
import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"net/url"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
	"unsafe"

	"github.com/charlievieth/go-sqlite3/internal/timefmt"
)

// SQLiteTimestampFormats is timestamp formats understood by both this module
// and SQLite.  The first format in the slice will be used when saving time
// values into the database. When parsing a string from a timestamp or datetime
// column, the formats are tried in order.
var SQLiteTimestampFormats = []string{
	// By default, store timestamps with whatever timezone they come with.
	// When parsed, they will be returned with the same timezone.
	"2006-01-02 15:04:05.999999999-07:00",
	"2006-01-02T15:04:05.999999999-07:00",
	"2006-01-02 15:04:05.999999999",
	"2006-01-02T15:04:05.999999999",
	"2006-01-02 15:04:05",
	"2006-01-02T15:04:05",
	"2006-01-02 15:04",
	"2006-01-02T15:04",
	"2006-01-02",
}

// This variable can be replaced with -ldflags like below:
// go build -ldflags="-X 'github.com/charlievieth/go-sqlite3.driverName=my-sqlite3'"
var driverName = "sqlite3"

func init() {
	if driverName != "" {
		sql.Register(driverName, &SQLiteDriver{})
	}
}

// Version returns SQLite library version information.
func Version() (libVersion string, libVersionNumber int, sourceID string) {
	libVersion = C.GoString(C.sqlite3_libversion())
	libVersionNumber = int(C.sqlite3_libversion_number())
	sourceID = C.GoString(C.sqlite3_sourceid())
	return libVersion, libVersionNumber, sourceID
}

const (
	// used by authorizer and pre_update_hook
	SQLITE_DELETE = C.SQLITE_DELETE
	SQLITE_INSERT = C.SQLITE_INSERT
	SQLITE_UPDATE = C.SQLITE_UPDATE

	// used by authorzier - as return value
	SQLITE_OK     = C.SQLITE_OK
	SQLITE_IGNORE = C.SQLITE_IGNORE
	SQLITE_DENY   = C.SQLITE_DENY
	SQLITE_AUTH   = C.SQLITE_AUTH

	// different actions query tries to do - passed as argument to authorizer
	SQLITE_CREATE_INDEX        = C.SQLITE_CREATE_INDEX
	SQLITE_CREATE_TABLE        = C.SQLITE_CREATE_TABLE
	SQLITE_CREATE_TEMP_INDEX   = C.SQLITE_CREATE_TEMP_INDEX
	SQLITE_CREATE_TEMP_TABLE   = C.SQLITE_CREATE_TEMP_TABLE
	SQLITE_CREATE_TEMP_TRIGGER = C.SQLITE_CREATE_TEMP_TRIGGER
	SQLITE_CREATE_TEMP_VIEW    = C.SQLITE_CREATE_TEMP_VIEW
	SQLITE_CREATE_TRIGGER      = C.SQLITE_CREATE_TRIGGER
	SQLITE_CREATE_VIEW         = C.SQLITE_CREATE_VIEW
	SQLITE_CREATE_VTABLE       = C.SQLITE_CREATE_VTABLE
	SQLITE_DROP_INDEX          = C.SQLITE_DROP_INDEX
	SQLITE_DROP_TABLE          = C.SQLITE_DROP_TABLE
	SQLITE_DROP_TEMP_INDEX     = C.SQLITE_DROP_TEMP_INDEX
	SQLITE_DROP_TEMP_TABLE     = C.SQLITE_DROP_TEMP_TABLE
	SQLITE_DROP_TEMP_TRIGGER   = C.SQLITE_DROP_TEMP_TRIGGER
	SQLITE_DROP_TEMP_VIEW      = C.SQLITE_DROP_TEMP_VIEW
	SQLITE_DROP_TRIGGER        = C.SQLITE_DROP_TRIGGER
	SQLITE_DROP_VIEW           = C.SQLITE_DROP_VIEW
	SQLITE_DROP_VTABLE         = C.SQLITE_DROP_VTABLE
	SQLITE_PRAGMA              = C.SQLITE_PRAGMA
	SQLITE_READ                = C.SQLITE_READ
	SQLITE_SELECT              = C.SQLITE_SELECT
	SQLITE_TRANSACTION         = C.SQLITE_TRANSACTION
	SQLITE_ATTACH              = C.SQLITE_ATTACH
	SQLITE_DETACH              = C.SQLITE_DETACH
	SQLITE_ALTER_TABLE         = C.SQLITE_ALTER_TABLE
	SQLITE_REINDEX             = C.SQLITE_REINDEX
	SQLITE_ANALYZE             = C.SQLITE_ANALYZE
	SQLITE_FUNCTION            = C.SQLITE_FUNCTION
	SQLITE_SAVEPOINT           = C.SQLITE_SAVEPOINT
	SQLITE_COPY                = C.SQLITE_COPY
	/*SQLITE_RECURSIVE           = C.SQLITE_RECURSIVE*/
)

// Standard File Control Opcodes
// See: https://www.sqlite.org/c3ref/c_fcntl_begin_atomic_write.html
const (
	SQLITE_FCNTL_LOCKSTATE             = int(1)
	SQLITE_FCNTL_GET_LOCKPROXYFILE     = int(2)
	SQLITE_FCNTL_SET_LOCKPROXYFILE     = int(3)
	SQLITE_FCNTL_LAST_ERRNO            = int(4)
	SQLITE_FCNTL_SIZE_HINT             = int(5)
	SQLITE_FCNTL_CHUNK_SIZE            = int(6)
	SQLITE_FCNTL_FILE_POINTER          = int(7)
	SQLITE_FCNTL_SYNC_OMITTED          = int(8)
	SQLITE_FCNTL_WIN32_AV_RETRY        = int(9)
	SQLITE_FCNTL_PERSIST_WAL           = int(10)
	SQLITE_FCNTL_OVERWRITE             = int(11)
	SQLITE_FCNTL_VFSNAME               = int(12)
	SQLITE_FCNTL_POWERSAFE_OVERWRITE   = int(13)
	SQLITE_FCNTL_PRAGMA                = int(14)
	SQLITE_FCNTL_BUSYHANDLER           = int(15)
	SQLITE_FCNTL_TEMPFILENAME          = int(16)
	SQLITE_FCNTL_MMAP_SIZE             = int(18)
	SQLITE_FCNTL_TRACE                 = int(19)
	SQLITE_FCNTL_HAS_MOVED             = int(20)
	SQLITE_FCNTL_SYNC                  = int(21)
	SQLITE_FCNTL_COMMIT_PHASETWO       = int(22)
	SQLITE_FCNTL_WIN32_SET_HANDLE      = int(23)
	SQLITE_FCNTL_WAL_BLOCK             = int(24)
	SQLITE_FCNTL_ZIPVFS                = int(25)
	SQLITE_FCNTL_RBU                   = int(26)
	SQLITE_FCNTL_VFS_POINTER           = int(27)
	SQLITE_FCNTL_JOURNAL_POINTER       = int(28)
	SQLITE_FCNTL_WIN32_GET_HANDLE      = int(29)
	SQLITE_FCNTL_PDB                   = int(30)
	SQLITE_FCNTL_BEGIN_ATOMIC_WRITE    = int(31)
	SQLITE_FCNTL_COMMIT_ATOMIC_WRITE   = int(32)
	SQLITE_FCNTL_ROLLBACK_ATOMIC_WRITE = int(33)
	SQLITE_FCNTL_LOCK_TIMEOUT          = int(34)
	SQLITE_FCNTL_DATA_VERSION          = int(35)
	SQLITE_FCNTL_SIZE_LIMIT            = int(36)
	SQLITE_FCNTL_CKPT_DONE             = int(37)
	SQLITE_FCNTL_RESERVE_BYTES         = int(38)
	SQLITE_FCNTL_CKPT_START            = int(39)
	SQLITE_FCNTL_EXTERNAL_READER       = int(40)
	SQLITE_FCNTL_CKSM_FILE             = int(41)
)

// SQLiteDriver implements driver.Driver.
type SQLiteDriver struct {
	Extensions  []string
	ConnectHook func(*SQLiteConn) error
}

// SQLiteConn implements driver.Conn.
type SQLiteConn struct {
	mu          sync.Mutex
	db          *C.sqlite3
	loc         *time.Location
	txlock      string
	funcs       []*functionInfo
	aggregators []*aggInfo
}

// SQLiteTx implements driver.Tx.
type SQLiteTx struct {
	c *SQLiteConn
}

// SQLiteStmt implements driver.Stmt.
type SQLiteStmt struct {
	mu     sync.Mutex
	c      *SQLiteConn
	s      *C.sqlite3_stmt
	closed bool
	cls    bool // True if the statement was created by SQLiteConn.Query
	reset  bool // True if the statement needs to reset before re-use
}

// SQLiteResult implements sql.Result.
type SQLiteResult struct {
	id      int64
	changes int64
}

// A columnType is a compact representation of sqlite3 columns datatype and
// declared type. The first two bits store the declared type and the remaining
// six bits store the sqlite3 datatype.
type columnType uint8

// declType returns the declared type, which is currently GO_SQLITE3_DECL_DATE
// or GO_SQLITE3_DECL_BOOL, since those are the only two types that we need for
// converting values.
func (c columnType) declType() int {
	return int(c) & C.GO_SQLITE3_DECL_MASK
}

// dataType returns the sqlite3 datatype code of the column, which is the
// result of sqlite3_column_type.
func (c columnType) dataType() int {
	return int(c) & C.GO_SQLITE3_TYPE_MASK
}

// SQLiteRows implements driver.Rows.
type SQLiteRows struct {
	s        *SQLiteStmt
	nc       int32 // Number of columns
	cls      bool  // True if we need close the statement in Close
	cols     []string
	decltype []string
	coltype  []columnType
	ctx      context.Context // no better alternative to pass context into Next() method
	closemu  sync.Mutex
	// semaphore to signal the goroutine used to interrupt queries when a
	// cancellable context is passed to QueryContext
	sema chan struct{}
}

type functionInfo struct {
	f                 reflect.Value
	argConverters     []callbackArgConverter
	variadicConverter callbackArgConverter
	retConverter      callbackRetConverter
}

func (fi *functionInfo) Call(ctx *C.sqlite3_context, argv []*C.sqlite3_value) {
	args, err := callbackConvertArgs(argv, fi.argConverters, fi.variadicConverter)
	if err != nil {
		callbackError(ctx, err)
		return
	}

	ret := fi.f.Call(args)

	if len(ret) == 2 && ret[1].Interface() != nil {
		callbackError(ctx, ret[1].Interface().(error))
		return
	}

	err = fi.retConverter(ctx, ret[0])
	if err != nil {
		callbackError(ctx, err)
		return
	}
}

type aggInfo struct {
	constructor reflect.Value

	// Active aggregator objects for aggregations in flight. The
	// aggregators are indexed by a counter stored in the aggregation
	// user data space provided by sqlite.
	active map[int64]reflect.Value
	next   int64

	stepArgConverters     []callbackArgConverter
	stepVariadicConverter callbackArgConverter

	doneRetConverter callbackRetConverter
}

func (ai *aggInfo) agg(ctx *C.sqlite3_context) (int64, reflect.Value, error) {
	aggIdx := (*int64)(C.sqlite3_aggregate_context(ctx, C.int(8)))
	if *aggIdx == 0 {
		*aggIdx = ai.next
		ret := ai.constructor.Call(nil)
		if len(ret) == 2 && ret[1].Interface() != nil {
			return 0, reflect.Value{}, ret[1].Interface().(error)
		}
		if ret[0].IsNil() {
			return 0, reflect.Value{}, errors.New("aggregator constructor returned nil state")
		}
		ai.next++
		ai.active[*aggIdx] = ret[0]
	}
	return *aggIdx, ai.active[*aggIdx], nil
}

func (ai *aggInfo) Step(ctx *C.sqlite3_context, argv []*C.sqlite3_value) {
	_, agg, err := ai.agg(ctx)
	if err != nil {
		callbackError(ctx, err)
		return
	}

	args, err := callbackConvertArgs(argv, ai.stepArgConverters, ai.stepVariadicConverter)
	if err != nil {
		callbackError(ctx, err)
		return
	}

	ret := agg.MethodByName("Step").Call(args)
	if len(ret) == 1 && ret[0].Interface() != nil {
		callbackError(ctx, ret[0].Interface().(error))
		return
	}
}

func (ai *aggInfo) Done(ctx *C.sqlite3_context) {
	idx, agg, err := ai.agg(ctx)
	if err != nil {
		callbackError(ctx, err)
		return
	}
	defer func() { delete(ai.active, idx) }()

	ret := agg.MethodByName("Done").Call(nil)
	if len(ret) == 2 && ret[1].Interface() != nil {
		callbackError(ctx, ret[1].Interface().(error))
		return
	}

	err = ai.doneRetConverter(ctx, ret[0])
	if err != nil {
		callbackError(ctx, err)
		return
	}
}

// Commit transaction.
func (tx *SQLiteTx) Commit() error {
	_, err := tx.c.exec(context.Background(), "COMMIT", nil)
	if err != nil {
		// sqlite3 may leave the transaction open in this scenario.
		// However, database/sql considers the transaction complete once we
		// return from Commit() - we must clean up to honour its semantics.
		// We don't know if the ROLLBACK is strictly necessary, but according
		// to sqlite's docs, there is no harm in calling ROLLBACK unnecessarily.
		tx.c.exec(context.Background(), "ROLLBACK", nil)
	}
	return err
}

// Rollback transaction.
func (tx *SQLiteTx) Rollback() error {
	_, err := tx.c.exec(context.Background(), "ROLLBACK", nil)
	return err
}

// RegisterCollation makes a Go function available as a collation.
//
// cmp receives two UTF-8 strings, a and b. The result should be 0 if
// a==b, -1 if a < b, and +1 if a > b.
//
// cmp must always return the same result given the same
// inputs. Additionally, it must have the following properties for all
// strings A, B and C: if A==B then B==A; if A==B and B==C then A==C;
// if A<B then B>A; if A<B and B<C then A<C.
//
// If cmp does not obey these constraints, sqlite3's behavior is
// undefined when the collation is used.
func (c *SQLiteConn) RegisterCollation(name string, cmp func(string, string) int) error {
	handle := newHandle(c, cmp)
	cname := C.CString(name)
	defer C.free(unsafe.Pointer(cname))
	rv := C.sqlite3_create_collation(c.db, cname, C.SQLITE_UTF8, handle, (*[0]byte)(unsafe.Pointer(C.compareTrampoline)))
	if rv != C.SQLITE_OK {
		return c.lastError()
	}
	return nil
}

// RegisterCommitHook sets the commit hook for a connection.
//
// If the callback returns non-zero the transaction will become a rollback.
//
// If there is an existing commit hook for this connection, it will be
// removed. If callback is nil the existing hook (if any) will be removed
// without creating a new one.
func (c *SQLiteConn) RegisterCommitHook(callback func() int) {
	if callback == nil {
		C.sqlite3_commit_hook(c.db, nil, nil)
	} else {
		C.sqlite3_commit_hook(c.db, (*[0]byte)(C.commitHookTrampoline), newHandle(c, callback))
	}
}

// RegisterRollbackHook sets the rollback hook for a connection.
//
// If there is an existing rollback hook for this connection, it will be
// removed. If callback is nil the existing hook (if any) will be removed
// without creating a new one.
func (c *SQLiteConn) RegisterRollbackHook(callback func()) {
	if callback == nil {
		C.sqlite3_rollback_hook(c.db, nil, nil)
	} else {
		C.sqlite3_rollback_hook(c.db, (*[0]byte)(C.rollbackHookTrampoline), newHandle(c, callback))
	}
}

// RegisterUpdateHook sets the update hook for a connection.
//
// The parameters to the callback are the operation (one of the constants
// SQLITE_INSERT, SQLITE_DELETE, or SQLITE_UPDATE), the database name, the
// table name, and the rowid.
//
// If there is an existing update hook for this connection, it will be
// removed. If callback is nil the existing hook (if any) will be removed
// without creating a new one.
func (c *SQLiteConn) RegisterUpdateHook(callback func(int, string, string, int64)) {
	if callback == nil {
		C.sqlite3_update_hook(c.db, nil, nil)
	} else {
		C.sqlite3_update_hook(c.db, (*[0]byte)(C.updateHookTrampoline), newHandle(c, callback))
	}
}

// RegisterAuthorizer sets the authorizer for connection.
//
// The parameters to the callback are the operation (one of the constants
// SQLITE_INSERT, SQLITE_DELETE, or SQLITE_UPDATE), and 1 to 3 arguments,
// depending on operation. More details see:
// https://www.sqlite.org/c3ref/c_alter_table.html
func (c *SQLiteConn) RegisterAuthorizer(callback func(int, string, string, string) int) {
	if callback == nil {
		C.sqlite3_set_authorizer(c.db, nil, nil)
	} else {
		C.sqlite3_set_authorizer(c.db, (*[0]byte)(C.authorizerTrampoline), newHandle(c, callback))
	}
}

// RegisterFunc makes a Go function available as a SQLite function.
//
// The Go function can have arguments of the following types: any
// numeric type except complex, bool, []byte, string and any.
// any arguments are given the direct translation of the SQLite data type:
// int64 for INTEGER, float64 for FLOAT, []byte for BLOB, string for TEXT.
//
// The function can additionally be variadic, as long as the type of
// the variadic argument is one of the above.
//
// If pure is true. SQLite will assume that the function's return
// value depends only on its inputs, and make more aggressive
// optimizations in its queries.
//
// See _example/go_custom_funcs for a detailed example.
func (c *SQLiteConn) RegisterFunc(name string, impl any, pure bool) error {
	var fi functionInfo
	fi.f = reflect.ValueOf(impl)
	t := fi.f.Type()
	if t.Kind() != reflect.Func {
		return errors.New("Non-function passed to RegisterFunc")
	}
	if t.NumOut() != 1 && t.NumOut() != 2 {
		return errors.New("SQLite functions must return 1 or 2 values")
	}
	if t.NumOut() == 2 && !t.Out(1).Implements(reflect.TypeOf((*error)(nil)).Elem()) {
		return errors.New("Second return value of SQLite function must be error")
	}

	numArgs := t.NumIn()
	if t.IsVariadic() {
		numArgs--
	}

	if numArgs > 0 {
		fi.argConverters = make([]callbackArgConverter, 0, numArgs)
	}
	for i := 0; i < numArgs; i++ {
		conv, err := callbackArg(t.In(i))
		if err != nil {
			return err
		}
		fi.argConverters = append(fi.argConverters, conv)
	}

	if t.IsVariadic() {
		conv, err := callbackArg(t.In(numArgs).Elem())
		if err != nil {
			return err
		}
		fi.variadicConverter = conv
		// Pass -1 to sqlite so that it allows any number of
		// arguments. The call helper verifies that the minimum number
		// of arguments is present for variadic functions.
		numArgs = -1
	}

	conv, err := callbackRet(t.Out(0))
	if err != nil {
		return err
	}
	fi.retConverter = conv

	// fi must outlast the database connection, or we'll have dangling pointers.
	if c.funcs == nil {
		// We create 5 functions by default, but add room for a few more.
		c.funcs = make([]*functionInfo, 0, 8)
	}
	c.funcs = append(c.funcs, &fi)

	cname := C.CString(name)
	defer C.free(unsafe.Pointer(cname))
	opts := C.SQLITE_UTF8
	if pure {
		opts |= C.SQLITE_DETERMINISTIC
	}
	rv := sqlite3CreateFunction(c.db, cname, C.int(numArgs), C.int(opts), newHandle(c, &fi), C.callbackTrampoline, nil, nil)
	if rv != C.SQLITE_OK {
		return c.lastError()
	}
	return nil
}

func sqlite3CreateFunction(db *C.sqlite3, zFunctionName *C.char, nArg C.int, eTextRep C.int, pApp unsafe.Pointer, xFunc unsafe.Pointer, xStep unsafe.Pointer, xFinal unsafe.Pointer) C.int {
	return C._sqlite3_create_function(db, zFunctionName, nArg, eTextRep, C.uintptr_t(uintptr(pApp)), (*[0]byte)(xFunc), (*[0]byte)(xStep), (*[0]byte)(xFinal))
}

// RegisterAggregator makes a Go type available as a SQLite aggregation function.
//
// Because aggregation is incremental, it's implemented in Go with a
// type that has 2 methods: func Step(values) accumulates one row of
// data into the accumulator, and func Done() ret finalizes and
// returns the aggregate value. "values" and "ret" may be any type
// supported by RegisterFunc.
//
// RegisterAggregator takes as implementation a constructor function
// that constructs an instance of the aggregator type each time an
// aggregation begins. The constructor must return a pointer to a
// type, or an interface that implements Step() and Done().
//
// The constructor function and the Step/Done methods may optionally
// return an error in addition to their other return values.
//
// See _example/go_custom_funcs for a detailed example.
func (c *SQLiteConn) RegisterAggregator(name string, impl any, pure bool) error {
	var ai aggInfo
	ai.constructor = reflect.ValueOf(impl)
	t := ai.constructor.Type()
	if t.Kind() != reflect.Func {
		return errors.New("non-function passed to RegisterAggregator")
	}
	if t.NumOut() != 1 && t.NumOut() != 2 {
		return errors.New("SQLite aggregator constructors must return 1 or 2 values")
	}
	if t.NumOut() == 2 && !t.Out(1).Implements(reflect.TypeOf((*error)(nil)).Elem()) {
		return errors.New("Second return value of SQLite function must be error")
	}
	if t.NumIn() != 0 {
		return errors.New("SQLite aggregator constructors must not have arguments")
	}

	agg := t.Out(0)
	switch agg.Kind() {
	case reflect.Ptr, reflect.Interface:
	default:
		return errors.New("SQlite aggregator constructor must return a pointer object")
	}
	stepFn, found := agg.MethodByName("Step")
	if !found {
		return errors.New("SQlite aggregator doesn't have a Step() function")
	}
	step := stepFn.Type
	if step.NumOut() != 0 && step.NumOut() != 1 {
		return errors.New("SQlite aggregator Step() function must return 0 or 1 values")
	}
	if step.NumOut() == 1 && !step.Out(0).Implements(reflect.TypeOf((*error)(nil)).Elem()) {
		return errors.New("type of SQlite aggregator Step() return value must be error")
	}

	stepNArgs := step.NumIn()
	start := 0
	if agg.Kind() == reflect.Ptr {
		// Skip over the method receiver
		stepNArgs--
		start++
	}
	if step.IsVariadic() {
		stepNArgs--
	}
	for i := start; i < start+stepNArgs; i++ {
		conv, err := callbackArg(step.In(i))
		if err != nil {
			return err
		}
		ai.stepArgConverters = append(ai.stepArgConverters, conv)
	}
	if step.IsVariadic() {
		conv, err := callbackArg(step.In(start + stepNArgs).Elem())
		if err != nil {
			return err
		}
		ai.stepVariadicConverter = conv
		// Pass -1 to sqlite so that it allows any number of
		// arguments. The call helper verifies that the minimum number
		// of arguments is present for variadic functions.
		stepNArgs = -1
	}

	doneFn, found := agg.MethodByName("Done")
	if !found {
		return errors.New("SQlite aggregator doesn't have a Done() function")
	}
	done := doneFn.Type
	doneNArgs := done.NumIn()
	if agg.Kind() == reflect.Ptr {
		// Skip over the method receiver
		doneNArgs--
	}
	if doneNArgs != 0 {
		return errors.New("SQlite aggregator Done() function must have no arguments")
	}
	if done.NumOut() != 1 && done.NumOut() != 2 {
		return errors.New("SQLite aggregator Done() function must return 1 or 2 values")
	}
	if done.NumOut() == 2 && !done.Out(1).Implements(reflect.TypeOf((*error)(nil)).Elem()) {
		return errors.New("second return value of SQLite aggregator Done() function must be error")
	}

	conv, err := callbackRet(done.Out(0))
	if err != nil {
		return err
	}
	ai.doneRetConverter = conv
	ai.active = make(map[int64]reflect.Value)
	ai.next = 1

	// ai must outlast the database connection, or we'll have dangling pointers.
	c.aggregators = append(c.aggregators, &ai)

	cname := C.CString(name)
	defer C.free(unsafe.Pointer(cname))
	opts := C.SQLITE_UTF8
	if pure {
		opts |= C.SQLITE_DETERMINISTIC
	}
	rv := sqlite3CreateFunction(c.db, cname, C.int(stepNArgs), C.int(opts), newHandle(c, &ai), nil, C.stepTrampoline, C.doneTrampoline)
	if rv != C.SQLITE_OK {
		return c.lastError()
	}
	return nil
}

// AutoCommit return which currently auto commit or not.
func (c *SQLiteConn) AutoCommit() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return int(C.sqlite3_get_autocommit(c.db)) != 0
}

func (c *SQLiteConn) lastError() error {
	return lastError(c.db)
}

// Note: may be called with db == nil
func lastError(db *C.sqlite3) error {
	rv := C.sqlite3_errcode(db) // returns SQLITE_NOMEM if db == nil
	if rv == C.SQLITE_OK {
		return nil
	}
	extrv := C.sqlite3_extended_errcode(db)    // returns SQLITE_NOMEM if db == nil
	errStr := C.GoString(C.sqlite3_errmsg(db)) // returns "out of memory" if db == nil

	// https://www.sqlite.org/c3ref/system_errno.html
	// sqlite3_system_errno is only meaningful if the error code was SQLITE_CANTOPEN,
	// or it was SQLITE_IOERR and the extended code was not SQLITE_IOERR_NOMEM
	var systemErrno syscall.Errno
	if rv == C.SQLITE_CANTOPEN || (rv == C.SQLITE_IOERR && extrv != C.SQLITE_IOERR_NOMEM) {
		systemErrno = syscall.Errno(C.sqlite3_system_errno(db))
	}

	return Error{
		Code:         ErrNo(rv),
		ExtendedCode: ErrNoExtended(extrv),
		SystemErrno:  systemErrno,
		err:          errStr,
	}
}

// Exec implements Execer.
func (c *SQLiteConn) Exec(query string, args []driver.Value) (driver.Result, error) {
	list := make([]driver.NamedValue, len(args))
	for i, v := range args {
		list[i] = driver.NamedValue{
			Ordinal: i + 1,
			Value:   v,
		}
	}
	return c.exec(context.Background(), query, list)
}

func (c *SQLiteConn) exec(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	// Trim the query. This is mostly important for getting rid
	// of any trailing space.
	query = strings.TrimSpace(query)
	if len(args) > 0 {
		return c.execArgs(ctx, query, args)
	}
	return c.execNoArgs(ctx, query)
}

func (c *SQLiteConn) execArgs(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	var (
		stmtArgs []driver.NamedValue
		start    int
		s        SQLiteStmt // escapes to the heap so reuse it
		sz       C.int      // number of query bytes consumed: escapes to the heap
	)
	for {
		s = SQLiteStmt{c: c} // reset
		sz = 0
		rv := C._sqlite3_prepare_v2(c.db, (*C.char)(unsafe.Pointer(stringData(query))),
			C.int(len(query)), &s.s, &sz)
		if rv != C.SQLITE_OK {
			return nil, c.lastError()
		}
		query = strings.TrimSpace(query[sz:])

		var res driver.Result
		if s.s != nil {
			na := s.NumInput()
			if len(args)-start < na {
				s.finalize()
				return nil, fmt.Errorf("not enough args to execute query: want %d got %d", na, len(args))
			}
			// consume the number of arguments used in the current
			// statement and append all named arguments not
			// contained therein
			if stmtArgs == nil {
				stmtArgs = make([]driver.NamedValue, 0, na)
			}
			stmtArgs = append(stmtArgs[:0], args[start:start+na]...)
			for i := range args {
				if (i < start || i >= na) && args[i].Name != "" {
					stmtArgs = append(stmtArgs, args[i])
				}
			}
			for i := range stmtArgs {
				stmtArgs[i].Ordinal = i + 1
			}
			var err error
			res, err = s.exec(ctx, stmtArgs)
			if err != nil && err != driver.ErrSkip {
				s.finalize()
				return nil, err
			}
			start += na
		}
		s.finalize()
		if len(query) == 0 {
			if res == nil {
				// https://github.com/mattn/go-sqlite3/issues/963
				res = &SQLiteResult{0, 0}
			}
			return res, nil
		}
	}
}

// execNoArgsSync processes every SQL statement in query. All processing occurs
// in C code, which reduces the overhead of CGO calls.
func (c *SQLiteConn) execNoArgsSync(query string) (_ driver.Result, err error) {
	var rowid, changes C.int64_t
	rv := C._sqlite3_exec_no_args(c.db, (*C.char)(unsafe.Pointer(stringData(query))),
		C.int(len(query)), &rowid, &changes)
	if rv != C.SQLITE_OK {
		err = c.lastError()
	}
	return &SQLiteResult{id: int64(rowid), changes: int64(changes)}, err
}

func (c *SQLiteConn) execNoArgs(ctx context.Context, query string) (driver.Result, error) {
	done := ctx.Done()
	if done == nil {
		return c.execNoArgsSync(query)
	}

	// Fast check if the Context is cancelled
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	ch := make(chan struct{})
	defer close(ch)
	go func() {
		select {
		case <-done:
			C.sqlite3_interrupt(c.db)
			// Wait until signaled. We need to ensure that this goroutine
			// will not call interrupt after this method returns, which is
			// why we can't check if only done is closed when waiting below.
			<-ch
		case <-ch:
		}
	}()

	res, err := c.execNoArgsSync(query)

	// Stop the goroutine and make sure we're at a point where
	// sqlite3_interrupt cannot be called again.
	ch <- struct{}{}

	if isInterruptErr(err) {
		err = ctx.Err()
	}
	return res, err
}

// Query implements Queryer.
func (c *SQLiteConn) Query(query string, args []driver.Value) (driver.Rows, error) {
	list := make([]driver.NamedValue, len(args))
	for i, v := range args {
		list[i] = driver.NamedValue{
			Ordinal: i + 1,
			Value:   v,
		}
	}
	return c.query(context.Background(), query, list)
}

var closedRows = &SQLiteRows{s: &SQLiteStmt{closed: true}}

func (c *SQLiteConn) query(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	s := SQLiteStmt{c: c, cls: true}
	p := stringData(query)
	var paramCount C.int
	rv := C._sqlite3_prepare_query(c.db, (*C.char)(unsafe.Pointer(p)), C.int(len(query)), &s.s, &paramCount)
	if rv != C.SQLITE_OK {
		if rv == C.GO_SQLITE_MULTIPLE_QUERIES {
			return nil, errors.New("query contains multiple SQL statements")
		}
		return nil, c.lastError()
	}

	// The sqlite3_stmt will be nil if the SQL was valid but did not
	// contain a query. For now we're supporting this for the sake of
	// backwards compatibility, but that may change in the future.
	if s.s == nil {
		return closedRows, nil
	}

	na := int(paramCount)
	if n := len(args); n != na {
		s.finalize()
		if n < na {
			return nil, fmt.Errorf("not enough args to execute query: want %d got %d", na, n)
		}
		return nil, fmt.Errorf("too many args to execute query: want %d got %d", na, n)
	}

	rows, err := s.query(ctx, args)
	if err != nil && err != driver.ErrSkip {
		s.finalize()
		return rows, err
	}
	return rows, nil
}

// Begin transaction.
func (c *SQLiteConn) Begin() (driver.Tx, error) {
	return c.begin(context.Background())
}

func (c *SQLiteConn) begin(ctx context.Context) (driver.Tx, error) {
	if _, err := c.exec(ctx, c.txlock, nil); err != nil {
		return nil, err
	}
	return &SQLiteTx{c}, nil
}

// Open database and return a new connection.
//
// A pragma can take either zero or one argument.
// The argument is may be either in parentheses or it may be separated from
// the pragma name by an equal sign. The two syntaxes yield identical results.
// In many pragmas, the argument is a boolean. The boolean can be one of:
//
//	1 yes true on
//	0 no false off
//
// You can specify a DSN string using a URI as the filename.
//
//	test.db
//	file:test.db?cache=shared&mode=memory
//	:memory:
//	file::memory:
//
//	mode
//	  Access mode of the database.
//	  https://www.sqlite.org/c3ref/open.html
//	  Values:
//	   - ro
//	   - rw
//	   - rwc
//	   - memory
//
//	cache
//	  SQLite Shared-Cache Mode
//	  https://www.sqlite.org/sharedcache.html
//	  Values:
//	    - shared
//	    - private
//
//	immutable=Boolean
//	  The immutable parameter is a boolean query parameter that indicates
//	  that the database file is stored on read-only media. When immutable is set,
//	  SQLite assumes that the database file cannot be changed,
//	  even by a process with higher privilege,
//	  and so the database is opened read-only and all locking and change detection is disabled.
//	  Caution: Setting the immutable property on a database file that
//	  does in fact change can result in incorrect query results and/or SQLITE_CORRUPT errors.
//
// go-sqlite3 adds the following query parameters to those used by SQLite:
//
//	_loc=XXX
//	  Specify location of time format. It's possible to specify "auto".
//
//	_mutex=XXX
//	  Specify mutex mode. XXX can be "no", "full".
//
//	_txlock=XXX
//	  Specify locking behavior for transactions.  XXX can be "immediate",
//	  "deferred", "exclusive".
//
//	_auto_vacuum=X | _vacuum=X
//	  0 | none - Auto Vacuum disabled
//	  1 | full - Auto Vacuum FULL
//	  2 | incremental - Auto Vacuum Incremental
//
//	_busy_timeout=XXX"| _timeout=XXX
//	  Specify value for sqlite3_busy_timeout.
//
//	_case_sensitive_like=Boolean | _cslike=Boolean
//	  https://www.sqlite.org/pragma.html#pragma_case_sensitive_like
//	  Default or disabled the LIKE operation is case-insensitive.
//	  When enabling this options behaviour of LIKE will become case-sensitive.
//
//	_defer_foreign_keys=Boolean | _defer_fk=Boolean
//	  Defer Foreign Keys until outermost transaction is committed.
//
//	_foreign_keys=Boolean | _fk=Boolean
//	  Enable or disable enforcement of foreign keys.
//
//	_ignore_check_constraints=Boolean
//	  This pragma enables or disables the enforcement of CHECK constraints.
//	  The default setting is off, meaning that CHECK constraints are enforced by default.
//
//	_journal_mode=MODE | _journal=MODE
//	  Set journal mode for the databases associated with the current connection.
//	  https://www.sqlite.org/pragma.html#pragma_journal_mode
//
//	_locking_mode=X | _locking=X
//	  Sets the database connection locking-mode.
//	  The locking-mode is either NORMAL or EXCLUSIVE.
//	  https://www.sqlite.org/pragma.html#pragma_locking_mode
//
//	_query_only=Boolean
//	  The query_only pragma prevents all changes to database files when enabled.
//
//	_recursive_triggers=Boolean | _rt=Boolean
//	  Enable or disable recursive triggers.
//
//	_secure_delete=Boolean|FAST
//	  When secure_delete is on, SQLite overwrites deleted content with zeros.
//	  https://www.sqlite.org/pragma.html#pragma_secure_delete
//
//	_synchronous=X | _sync=X
//	  Change the setting of the "synchronous" flag.
//	  https://www.sqlite.org/pragma.html#pragma_synchronous
//
//	_writable_schema=Boolean
//	  When this pragma is on, the SQLITE_MASTER tables in which database
//	  can be changed using ordinary UPDATE, INSERT, and DELETE statements.
//	  Warning: misuse of this pragma can easily result in a corrupt database file.
func (d *SQLiteDriver) Open(dsn string) (driver.Conn, error) {
	if C.sqlite3_threadsafe() == 0 {
		return nil, errors.New("sqlite library was not compiled for thread-safe operation")
	}

	var pkey string

	// Options
	var loc *time.Location
	authCreate := false
	authUser := ""
	authPass := ""
	authCrypt := ""
	authSalt := ""
	mutex := C.int(C.SQLITE_OPEN_FULLMUTEX)
	txlock := "BEGIN"

	// PRAGMA's
	autoVacuum := -1
	busyTimeout := 5000
	caseSensitiveLike := -1
	deferForeignKeys := -1
	foreignKeys := -1
	ignoreCheckConstraints := -1
	var journalMode string
	lockingMode := "NORMAL"
	queryOnly := -1
	recursiveTriggers := -1
	secureDelete := "DEFAULT"
	synchronousMode := "NORMAL"
	writableSchema := -1
	vfsName := ""
	var cacheSize *int64

	pos := strings.IndexRune(dsn, '?')
	if pos >= 1 {
		params, err := url.ParseQuery(dsn[pos+1:])
		if err != nil {
			return nil, err
		}

		// Authentication
		if _, ok := params["_auth"]; ok {
			authCreate = true
		}
		if val := params.Get("_auth_user"); val != "" {
			authUser = val
		}
		if val := params.Get("_auth_pass"); val != "" {
			authPass = val
		}
		if val := params.Get("_auth_crypt"); val != "" {
			authCrypt = val
		}
		if val := params.Get("_auth_salt"); val != "" {
			authSalt = val
		}

		// _loc
		if val := params.Get("_loc"); val != "" {
			switch strings.ToLower(val) {
			case "auto":
				loc = time.Local
			default:
				loc, err = time.LoadLocation(val)
				if err != nil {
					return nil, fmt.Errorf("Invalid _loc: %v: %v", val, err)
				}
			}
		}

		// _mutex
		if val := params.Get("_mutex"); val != "" {
			switch strings.ToLower(val) {
			case "no":
				mutex = C.SQLITE_OPEN_NOMUTEX
			case "full":
				mutex = C.SQLITE_OPEN_FULLMUTEX
			default:
				return nil, fmt.Errorf("Invalid _mutex: %v", val)
			}
		}

		// _txlock
		if val := params.Get("_txlock"); val != "" {
			switch strings.ToLower(val) {
			case "immediate":
				txlock = "BEGIN IMMEDIATE"
			case "exclusive":
				txlock = "BEGIN EXCLUSIVE"
			case "deferred":
				txlock = "BEGIN"
			default:
				return nil, fmt.Errorf("Invalid _txlock: %v", val)
			}
		}

		// Auto Vacuum (_vacuum)
		//
		// https://www.sqlite.org/pragma.html#pragma_auto_vacuum
		//
		pkey = "" // Reset pkey
		if _, ok := params["_auto_vacuum"]; ok {
			pkey = "_auto_vacuum"
		}
		if _, ok := params["_vacuum"]; ok {
			pkey = "_vacuum"
		}
		if val := params.Get(pkey); val != "" {
			switch strings.ToLower(val) {
			case "0", "none":
				autoVacuum = 0
			case "1", "full":
				autoVacuum = 1
			case "2", "incremental":
				autoVacuum = 2
			default:
				return nil, fmt.Errorf("Invalid _auto_vacuum: %v, expecting value of '0 NONE 1 FULL 2 INCREMENTAL'", val)
			}
		}

		// Busy Timeout (_busy_timeout)
		//
		// https://www.sqlite.org/pragma.html#pragma_busy_timeout
		//
		pkey = "" // Reset pkey
		if _, ok := params["_busy_timeout"]; ok {
			pkey = "_busy_timeout"
		}
		if _, ok := params["_timeout"]; ok {
			pkey = "_timeout"
		}
		if val := params.Get(pkey); val != "" {
			iv, err := strconv.ParseInt(val, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("Invalid _busy_timeout: %v: %v", val, err)
			}
			busyTimeout = int(iv)
		}

		// Case Sensitive Like (_cslike)
		//
		// https://www.sqlite.org/pragma.html#pragma_case_sensitive_like
		//
		pkey = "" // Reset pkey
		if _, ok := params["_case_sensitive_like"]; ok {
			pkey = "_case_sensitive_like"
		}
		if _, ok := params["_cslike"]; ok {
			pkey = "_cslike"
		}
		if val := params.Get(pkey); val != "" {
			switch strings.ToLower(val) {
			case "0", "no", "false", "off":
				caseSensitiveLike = 0
			case "1", "yes", "true", "on":
				caseSensitiveLike = 1
			default:
				return nil, fmt.Errorf("Invalid _case_sensitive_like: %v, expecting boolean value of '0 1 false true no yes off on'", val)
			}
		}

		// Defer Foreign Keys (_defer_foreign_keys | _defer_fk)
		//
		// https://www.sqlite.org/pragma.html#pragma_defer_foreign_keys
		//
		pkey = "" // Reset pkey
		if _, ok := params["_defer_foreign_keys"]; ok {
			pkey = "_defer_foreign_keys"
		}
		if _, ok := params["_defer_fk"]; ok {
			pkey = "_defer_fk"
		}
		if val := params.Get(pkey); val != "" {
			switch strings.ToLower(val) {
			case "0", "no", "false", "off":
				deferForeignKeys = 0
			case "1", "yes", "true", "on":
				deferForeignKeys = 1
			default:
				return nil, fmt.Errorf("Invalid _defer_foreign_keys: %v, expecting boolean value of '0 1 false true no yes off on'", val)
			}
		}

		// Foreign Keys (_foreign_keys | _fk)
		//
		// https://www.sqlite.org/pragma.html#pragma_foreign_keys
		//
		pkey = "" // Reset pkey
		if _, ok := params["_foreign_keys"]; ok {
			pkey = "_foreign_keys"
		}
		if _, ok := params["_fk"]; ok {
			pkey = "_fk"
		}
		if val := params.Get(pkey); val != "" {
			switch strings.ToLower(val) {
			case "0", "no", "false", "off":
				foreignKeys = 0
			case "1", "yes", "true", "on":
				foreignKeys = 1
			default:
				return nil, fmt.Errorf("Invalid _foreign_keys: %v, expecting boolean value of '0 1 false true no yes off on'", val)
			}
		}

		// Ignore CHECK Constrains (_ignore_check_constraints)
		//
		// https://www.sqlite.org/pragma.html#pragma_ignore_check_constraints
		//
		if val := params.Get("_ignore_check_constraints"); val != "" {
			switch strings.ToLower(val) {
			case "0", "no", "false", "off":
				ignoreCheckConstraints = 0
			case "1", "yes", "true", "on":
				ignoreCheckConstraints = 1
			default:
				return nil, fmt.Errorf("Invalid _ignore_check_constraints: %v, expecting boolean value of '0 1 false true no yes off on'", val)
			}
		}

		// Journal Mode (_journal_mode | _journal)
		//
		// https://www.sqlite.org/pragma.html#pragma_journal_mode
		//
		pkey = "" // Reset pkey
		if _, ok := params["_journal_mode"]; ok {
			pkey = "_journal_mode"
		}
		if _, ok := params["_journal"]; ok {
			pkey = "_journal"
		}
		if val := params.Get(pkey); val != "" {
			switch strings.ToUpper(val) {
			case "DELETE", "TRUNCATE", "PERSIST", "MEMORY", "OFF":
				journalMode = strings.ToUpper(val)
			case "WAL":
				journalMode = strings.ToUpper(val)

				// For WAL Mode set Synchronous Mode to 'NORMAL'
				// See https://www.sqlite.org/pragma.html#pragma_synchronous
				synchronousMode = "NORMAL"
			default:
				return nil, fmt.Errorf("Invalid _journal: %v, expecting value of 'DELETE TRUNCATE PERSIST MEMORY WAL OFF'", val)
			}
		}

		// Locking Mode (_locking)
		//
		// https://www.sqlite.org/pragma.html#pragma_locking_mode
		//
		pkey = "" // Reset pkey
		if _, ok := params["_locking_mode"]; ok {
			pkey = "_locking_mode"
		}
		if _, ok := params["_locking"]; ok {
			pkey = "_locking"
		}
		if val := params.Get(pkey); val != "" {
			switch strings.ToUpper(val) {
			case "NORMAL", "EXCLUSIVE":
				lockingMode = strings.ToUpper(val)
			default:
				return nil, fmt.Errorf("Invalid _locking_mode: %v, expecting value of 'NORMAL EXCLUSIVE", val)
			}
		}

		// Query Only (_query_only)
		//
		// https://www.sqlite.org/pragma.html#pragma_query_only
		//
		if val := params.Get("_query_only"); val != "" {
			switch strings.ToLower(val) {
			case "0", "no", "false", "off":
				queryOnly = 0
			case "1", "yes", "true", "on":
				queryOnly = 1
			default:
				return nil, fmt.Errorf("Invalid _query_only: %v, expecting boolean value of '0 1 false true no yes off on'", val)
			}
		}

		// Recursive Triggers (_recursive_triggers)
		//
		// https://www.sqlite.org/pragma.html#pragma_recursive_triggers
		//
		pkey = "" // Reset pkey
		if _, ok := params["_recursive_triggers"]; ok {
			pkey = "_recursive_triggers"
		}
		if _, ok := params["_rt"]; ok {
			pkey = "_rt"
		}
		if val := params.Get(pkey); val != "" {
			switch strings.ToLower(val) {
			case "0", "no", "false", "off":
				recursiveTriggers = 0
			case "1", "yes", "true", "on":
				recursiveTriggers = 1
			default:
				return nil, fmt.Errorf("Invalid _recursive_triggers: %v, expecting boolean value of '0 1 false true no yes off on'", val)
			}
		}

		// Secure Delete (_secure_delete)
		//
		// https://www.sqlite.org/pragma.html#pragma_secure_delete
		//
		if val := params.Get("_secure_delete"); val != "" {
			switch strings.ToLower(val) {
			case "0", "no", "false", "off":
				secureDelete = "OFF"
			case "1", "yes", "true", "on":
				secureDelete = "ON"
			case "fast":
				secureDelete = "FAST"
			default:
				return nil, fmt.Errorf("Invalid _secure_delete: %v, expecting boolean value of '0 1 false true no yes off on fast'", val)
			}
		}

		// Synchronous Mode (_synchronous | _sync)
		//
		// https://www.sqlite.org/pragma.html#pragma_synchronous
		//
		pkey = "" // Reset pkey
		if _, ok := params["_synchronous"]; ok {
			pkey = "_synchronous"
		}
		if _, ok := params["_sync"]; ok {
			pkey = "_sync"
		}
		if val := params.Get(pkey); val != "" {
			switch strings.ToUpper(val) {
			case "0", "OFF", "1", "NORMAL", "2", "FULL", "3", "EXTRA":
				synchronousMode = strings.ToUpper(val)
			default:
				return nil, fmt.Errorf("Invalid _synchronous: %v, expecting value of '0 OFF 1 NORMAL 2 FULL 3 EXTRA'", val)
			}
		}

		// Writable Schema (_writeable_schema)
		//
		// https://www.sqlite.org/pragma.html#pragma_writeable_schema
		//
		if val := params.Get("_writable_schema"); val != "" {
			switch strings.ToLower(val) {
			case "0", "no", "false", "off":
				writableSchema = 0
			case "1", "yes", "true", "on":
				writableSchema = 1
			default:
				return nil, fmt.Errorf("Invalid _writable_schema: %v, expecting boolean value of '0 1 false true no yes off on'", val)
			}
		}

		// Cache size (_cache_size)
		//
		// https://sqlite.org/pragma.html#pragma_cache_size
		//
		if val := params.Get("_cache_size"); val != "" {
			iv, err := strconv.ParseInt(val, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("Invalid _cache_size: %v: %v", val, err)
			}
			cacheSize = &iv
		}

		if val := params.Get("vfs"); val != "" {
			vfsName = val
		}

		if !strings.HasPrefix(dsn, "file:") {
			dsn = dsn[:pos]
		}
	}

	var db *C.sqlite3
	name := C.CString(dsn)
	var vfs *C.char
	if vfsName != "" {
		vfs = C.CString(vfsName)
	}
	rv := C._sqlite3_open_v2(name, &db,
		mutex|C.SQLITE_OPEN_READWRITE|C.SQLITE_OPEN_CREATE,
		vfs)
	C.free(unsafe.Pointer(name))
	if vfs != nil {
		C.free(unsafe.Pointer(vfs))
	}
	if rv != 0 {
		// Save off the error _before_ closing the database.
		// This is safe even if db is nil.
		err := lastError(db)
		if db != nil {
			C.sqlite3_close_v2(db)
		}
		return nil, err
	}
	if db == nil {
		return nil, errors.New("sqlite succeeded without returning a database")
	}

	exec := func(s string) error {
		cs := C.CString(s)
		rv := C.sqlite3_exec(db, cs, nil, nil, nil)
		C.free(unsafe.Pointer(cs))
		if rv != C.SQLITE_OK {
			return lastError(db)
		}
		return nil
	}

	// Create connection to SQLite
	conn := &SQLiteConn{db: db, loc: loc, txlock: txlock}

	// All further database configuration *must* occur within this function, as
	// it simplifies closing the database connection and unregistering associated
	// functions or callbacks.
	//
	// Previously, this was handled at each return statement, which was error-prone
	// and led to bugs because the database connection was not always closed.
	err := func() error {
		// Busy timeout
		if err := exec("PRAGMA busy_timeout = " + strconv.Itoa(busyTimeout) + ";"); err != nil {
			return err
		}

		// USER AUTHENTICATION
		//
		// User Authentication is always performed even when
		// sqlite_userauth is not compiled in, because without user authentication
		// the authentication is a no-op.
		//
		// Workflow
		//	- Authenticate
		//		ON::SUCCESS		=> Continue
		//		ON::SQLITE_AUTH => Return error and exit Open(...)
		//
		//  - Activate User Authentication
		//		Check if the user wants to activate User Authentication.
		//		If so then first create a temporary AuthConn to the database
		//		This is possible because we are already successfully authenticated.
		//
		//	- Check if `sqlite_user`` table exists
		//		YES				=> Add the provided user from DSN as Admin User and
		//						   activate user authentication.
		//		NO				=> Continue
		//

		// Password Cipher has to be registered before authentication
		if len(authCrypt) > 0 {
			switch strings.ToUpper(authCrypt) {
			case "SHA1":
				if err := conn.RegisterFunc("sqlite_crypt", CryptEncoderSHA1, true); err != nil {
					return fmt.Errorf("CryptEncoderSHA1: %s", err)
				}
			case "SSHA1":
				if len(authSalt) == 0 {
					return fmt.Errorf("_auth_crypt=ssha1, requires _auth_salt")
				}
				if err := conn.RegisterFunc("sqlite_crypt", CryptEncoderSSHA1(authSalt), true); err != nil {
					return fmt.Errorf("CryptEncoderSSHA1: %s", err)
				}
			case "SHA256":
				if err := conn.RegisterFunc("sqlite_crypt", CryptEncoderSHA256, true); err != nil {
					return fmt.Errorf("CryptEncoderSHA256: %s", err)
				}
			case "SSHA256":
				if len(authSalt) == 0 {
					return fmt.Errorf("_auth_crypt=ssha256, requires _auth_salt")
				}
				if err := conn.RegisterFunc("sqlite_crypt", CryptEncoderSSHA256(authSalt), true); err != nil {
					return fmt.Errorf("CryptEncoderSSHA256: %s", err)
				}
			case "SHA384":
				if err := conn.RegisterFunc("sqlite_crypt", CryptEncoderSHA384, true); err != nil {
					return fmt.Errorf("CryptEncoderSHA384: %s", err)
				}
			case "SSHA384":
				if len(authSalt) == 0 {
					return fmt.Errorf("_auth_crypt=ssha384, requires _auth_salt")
				}
				if err := conn.RegisterFunc("sqlite_crypt", CryptEncoderSSHA384(authSalt), true); err != nil {
					return fmt.Errorf("CryptEncoderSSHA384: %s", err)
				}
			case "SHA512":
				if err := conn.RegisterFunc("sqlite_crypt", CryptEncoderSHA512, true); err != nil {
					return fmt.Errorf("CryptEncoderSHA512: %s", err)
				}
			case "SSHA512":
				if len(authSalt) == 0 {
					return fmt.Errorf("_auth_crypt=ssha512, requires _auth_salt")
				}
				if err := conn.RegisterFunc("sqlite_crypt", CryptEncoderSSHA512(authSalt), true); err != nil {
					return fmt.Errorf("CryptEncoderSSHA512: %s", err)
				}
			}
		}

		// Preform Authentication
		if err := conn.Authenticate(authUser, authPass); err != nil {
			return err
		}

		// Register: authenticate
		// Authenticate will perform an authentication of the provided username
		// and password against the database.
		//
		// If a database contains the SQLITE_USER table, then the
		// call to Authenticate must be invoked with an
		// appropriate username and password prior to enable read and write
		//access to the database.
		//
		// Return SQLITE_OK on success or SQLITE_ERROR if the username/password
		// combination is incorrect or unknown.
		//
		// If the SQLITE_USER table is not present in the database file, then
		// this interface is a harmless no-op returnning SQLITE_OK.
		if err := conn.registerAuthFunc("authenticate", conn.authenticate, true); err != nil {
			return err
		}
		//
		// Register: auth_user_add
		// auth_user_add can be used (by an admin user only)
		// to create a new user. When called on a no-authentication-required
		// database, this routine converts the database into an authentication-
		// required database, automatically makes the added user an
		// administrator, and logs in the current connection as that user.
		// The AuthUserAdd only works for the "main" database, not
		// for any ATTACH-ed databases. Any call to AuthUserAdd by a
		// non-admin user results in an error.
		if err := conn.registerAuthFunc("auth_user_add", conn.authUserAdd, true); err != nil {
			return err
		}
		//
		// Register: auth_user_change
		// auth_user_change can be used to change a users
		// login credentials or admin privilege.  Any user can change their own
		// login credentials. Only an admin user can change another users login
		// credentials or admin privilege setting. No user may change their own
		// admin privilege setting.
		if err := conn.registerAuthFunc("auth_user_change", conn.authUserChange, true); err != nil {
			return err
		}
		//
		// Register: auth_user_delete
		// auth_user_delete can be used (by an admin user only)
		// to delete a user. The currently logged-in user cannot be deleted,
		// which guarantees that there is always an admin user and hence that
		// the database cannot be converted into a no-authentication-required
		// database.
		if err := conn.registerAuthFunc("auth_user_delete", conn.authUserDelete, true); err != nil {
			return err
		}

		// Register: auth_enabled
		// auth_enabled can be used to check if user authentication is enabled
		if err := conn.registerAuthFunc("auth_enabled", conn.authEnabled, true); err != nil {
			return err
		}

		// Auto Vacuum
		// Moved auto_vacuum command, the user preference for auto_vacuum needs to be implemented directly after
		// the authentication and before the sqlite_user table gets created if the user
		// decides to activate User Authentication because
		// auto_vacuum needs to be set before any tables are created
		// and activating user authentication creates the internal table `sqlite_user`.
		if autoVacuum > -1 {
			if err := exec("PRAGMA auto_vacuum = " + strconv.Itoa(autoVacuum) + ";"); err != nil {
				return err
			}
		}

		// Check if user wants to activate User Authentication
		if authCreate {
			// Before going any further, we need to check that the user
			// has provided an username and password within the DSN.
			// We are not allowed to continue.
			if len(authUser) == 0 {
				return fmt.Errorf("Missing '_auth_user' while user authentication was requested with '_auth'")
			}
			if len(authPass) == 0 {
				return fmt.Errorf("Missing '_auth_pass' while user authentication was requested with '_auth'")
			}

			// Check if User Authentication is Enabled
			authExists := conn.AuthEnabled()
			if !authExists {
				if err := conn.AuthUserAdd(authUser, authPass, true); err != nil {
					return err
				}
			}
		}

		// Case Sensitive LIKE
		if caseSensitiveLike > -1 {
			if err := exec("PRAGMA case_sensitive_like = " + strconv.Itoa(caseSensitiveLike) + ";"); err != nil {
				return err
			}
		}

		// Defer Foreign Keys
		if deferForeignKeys > -1 {
			if err := exec("PRAGMA defer_foreign_keys = " + strconv.Itoa(deferForeignKeys) + ";"); err != nil {
				return err
			}
		}

		// Foreign Keys
		if foreignKeys > -1 {
			if err := exec("PRAGMA foreign_keys = " + strconv.Itoa(foreignKeys) + ";"); err != nil {
				return err
			}
		}

		// Ignore CHECK Constraints
		if ignoreCheckConstraints > -1 {
			if err := exec("PRAGMA ignore_check_constraints = " + strconv.Itoa(ignoreCheckConstraints) + ";"); err != nil {
				return err
			}
		}

		// Journal Mode
		if journalMode != "" {
			if err := exec("PRAGMA journal_mode = " + journalMode + ";"); err != nil {
				return err
			}
		}

		// Locking Mode
		// Because the default is NORMAL and this is not changed in this package
		// by using the compile time SQLITE_DEFAULT_LOCKING_MODE this PRAGMA can always be executed
		if err := exec("PRAGMA locking_mode = " + lockingMode + ";"); err != nil {
			return err
		}

		// Query Only
		if queryOnly > -1 {
			if err := exec("PRAGMA query_only = " + strconv.Itoa(queryOnly) + ";"); err != nil {
				return err
			}
		}

		// Recursive Triggers
		if recursiveTriggers > -1 {
			if err := exec("PRAGMA recursive_triggers = " + strconv.Itoa(recursiveTriggers) + ";"); err != nil {
				return err
			}
		}

		// Secure Delete
		//
		// Because this package can set the compile time flag SQLITE_SECURE_DELETE with a build tag
		// the default value for secureDelete var is 'DEFAULT' this way
		// you can compile with secure_delete 'ON' and disable it for a specific database connection.
		if secureDelete != "DEFAULT" {
			if err := exec("PRAGMA secure_delete = " + secureDelete + ";"); err != nil {
				return err
			}
		}

		// Synchronous Mode
		//
		// Because default is NORMAL this statement is always executed
		if err := exec("PRAGMA synchronous = " + synchronousMode + ";"); err != nil {
			return err
		}

		// Writable Schema
		if writableSchema > -1 {
			if err := exec("PRAGMA writable_schema = " + strconv.Itoa(writableSchema) + ";"); err != nil {
				return err
			}
		}

		// Cache Size
		if cacheSize != nil {
			if err := exec("PRAGMA cache_size = " + strconv.FormatInt(*cacheSize, 10) + ";"); err != nil {
				return err
			}
		}

		if len(d.Extensions) > 0 {
			if err := conn.loadExtensions(d.Extensions); err != nil {
				return err
			}
		}

		if d.ConnectHook != nil {
			if err := d.ConnectHook(conn); err != nil {
				return err
			}
		}
		return nil
	}()
	if err != nil {
		conn.Close()
		return nil, err
	}

	runtime.SetFinalizer(conn, (*SQLiteConn).Close)
	return conn, nil
}

// Close the connection.
func (c *SQLiteConn) Close() (err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.db == nil {
		return nil // Already closed
	}
	runtime.SetFinalizer(c, nil)
	rv := C.sqlite3_close_v2(c.db)
	if rv != C.SQLITE_OK {
		err = c.lastError()
	}
	deleteHandles(c)
	c.db = nil
	return err
}

func (c *SQLiteConn) dbConnOpen() bool {
	if c == nil {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.db != nil
}

// Prepare the query string. Return a new statement.
func (c *SQLiteConn) Prepare(query string) (driver.Stmt, error) {
	return c.prepare(context.Background(), query)
}

func (c *SQLiteConn) prepare(ctx context.Context, query string) (driver.Stmt, error) {
	p := stringData(query)
	var s *C.sqlite3_stmt
	rv := C._sqlite3_prepare_v2_internal(c.db, (*C.char)(unsafe.Pointer(p)), C.int(len(query)), &s, nil)
	if rv != C.SQLITE_OK {
		return nil, c.lastError()
	}
	ss := &SQLiteStmt{c: c, s: s}
	runtime.SetFinalizer(ss, (*SQLiteStmt).Close)
	return ss, nil
}

// Run-Time Limit Categories.
// See: http://www.sqlite.org/c3ref/c_limit_attached.html
const (
	SQLITE_LIMIT_LENGTH              = C.SQLITE_LIMIT_LENGTH
	SQLITE_LIMIT_SQL_LENGTH          = C.SQLITE_LIMIT_SQL_LENGTH
	SQLITE_LIMIT_COLUMN              = C.SQLITE_LIMIT_COLUMN
	SQLITE_LIMIT_EXPR_DEPTH          = C.SQLITE_LIMIT_EXPR_DEPTH
	SQLITE_LIMIT_COMPOUND_SELECT     = C.SQLITE_LIMIT_COMPOUND_SELECT
	SQLITE_LIMIT_VDBE_OP             = C.SQLITE_LIMIT_VDBE_OP
	SQLITE_LIMIT_FUNCTION_ARG        = C.SQLITE_LIMIT_FUNCTION_ARG
	SQLITE_LIMIT_ATTACHED            = C.SQLITE_LIMIT_ATTACHED
	SQLITE_LIMIT_LIKE_PATTERN_LENGTH = C.SQLITE_LIMIT_LIKE_PATTERN_LENGTH
	SQLITE_LIMIT_VARIABLE_NUMBER     = C.SQLITE_LIMIT_VARIABLE_NUMBER
	SQLITE_LIMIT_TRIGGER_DEPTH       = C.SQLITE_LIMIT_TRIGGER_DEPTH
	SQLITE_LIMIT_WORKER_THREADS      = C.SQLITE_LIMIT_WORKER_THREADS
)

// GetFilename returns the absolute path to the file containing
// the requested schema. When passed an empty string, it will
// instead use the database's default schema: "main".
// See: sqlite3_db_filename, https://www.sqlite.org/c3ref/db_filename.html
func (c *SQLiteConn) GetFilename(schemaName string) string {
	if schemaName == "" {
		schemaName = "main"
	}
	return C.GoString(C.sqlite3_db_filename(c.db, C.CString(schemaName)))
}

// GetLimit returns the current value of a run-time limit.
// See: sqlite3_limit, http://www.sqlite.org/c3ref/limit.html
func (c *SQLiteConn) GetLimit(id int) int {
	return int(C._sqlite3_limit(c.db, C.int(id), C.int(-1)))
}

// SetLimit changes the value of a run-time limits.
// Then this method returns the prior value of the limit.
// See: sqlite3_limit, http://www.sqlite.org/c3ref/limit.html
func (c *SQLiteConn) SetLimit(id int, newVal int) int {
	return int(C._sqlite3_limit(c.db, C.int(id), C.int(newVal)))
}

// SetFileControlInt invokes the xFileControl method on a given database. The
// dbName is the name of the database. It will default to "main" if left blank.
// The op is one of the opcodes prefixed by "SQLITE_FCNTL_". The arg argument
// and return code are both opcode-specific. Please see the SQLite documentation.
//
// This method is not thread-safe as the returned error code can be changed by
// another call if invoked concurrently.
//
// See: sqlite3_file_control, https://www.sqlite.org/c3ref/file_control.html
func (c *SQLiteConn) SetFileControlInt(dbName string, op int, arg int) error {
	if dbName == "" {
		dbName = "main"
	}

	cDBName := C.CString(dbName)
	defer C.free(unsafe.Pointer(cDBName))

	cArg := C.int(arg)
	rv := C.sqlite3_file_control(c.db, cDBName, C.int(op), unsafe.Pointer(&cArg))
	if rv != C.SQLITE_OK {
		return c.lastError()
	}
	return nil
}

// Close the statement.
func (s *SQLiteStmt) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	conn := s.c
	stmt := s.s
	s.c = nil
	s.s = nil
	runtime.SetFinalizer(s, nil)
	if !conn.dbConnOpen() {
		return errors.New("sqlite statement with already closed database connection")
	}
	rv := C.sqlite3_finalize(stmt)
	if rv != C.SQLITE_OK {
		return conn.lastError()
	}
	return nil
}

func (s *SQLiteStmt) finalize() {
	if s.s != nil {
		C.sqlite3_finalize(s.s)
		s.s = nil
	}
}

// NumInput return a number of parameters.
func (s *SQLiteStmt) NumInput() int {
	return int(C.sqlite3_bind_parameter_count(s.s))
}

var placeHolder = []byte{0}

func hasNamedArgs(args []driver.NamedValue) bool {
	for _, v := range args {
		if v.Name != "" {
			return true
		}
	}
	return false
}

func (s *SQLiteStmt) bind(args []driver.NamedValue) error {
	if s.reset {
		rv := C.sqlite3_reset(s.s)
		if rv != C.SQLITE_ROW && rv != C.SQLITE_OK && rv != C.SQLITE_DONE {
			return s.c.lastError()
		}
	} else {
		s.reset = true
	}

	if hasNamedArgs(args) {
		return s.bindIndices(args)
	}

	var rv C.int
	for _, arg := range args {
		n := C.int(arg.Ordinal)
		switch v := arg.Value.(type) {
		case nil:
			rv = C.sqlite3_bind_null(s.s, n)
		case string:
			p := stringData(v)
			rv = C._sqlite3_bind_text(s.s, n, (*C.char)(unsafe.Pointer(p)), C.int(len(v)))
		case int64:
			rv = C.sqlite3_bind_int64(s.s, n, C.sqlite3_int64(v))
		case bool:
			val := 0
			if v {
				val = 1
			}
			rv = C.sqlite3_bind_int(s.s, n, C.int(val))
		case float64:
			rv = C.sqlite3_bind_double(s.s, n, C.double(v))
		case []byte:
			if v == nil {
				rv = C.sqlite3_bind_null(s.s, n)
			} else {
				ln := len(v)
				if ln == 0 {
					v = placeHolder
				}
				rv = C._sqlite3_bind_blob(s.s, n, unsafe.Pointer(&v[0]), C.int(ln))
			}
		case time.Time:
			b := timefmt.Format(v)
			rv = C._sqlite3_bind_text(s.s, n, (*C.char)(unsafe.Pointer(&b[0])), C.int(len(b)))
		}
		if rv != C.SQLITE_OK {
			return s.c.lastError()
		}
	}
	return nil
}

func (s *SQLiteStmt) bindIndices(args []driver.NamedValue) error {
	// Find the longest named parameter name.
	n := 0
	for _, v := range args {
		if m := len(v.Name); m > n {
			n = m
		}
	}
	buf := make([]byte, 0, n+2) // +2 for placeholder and null terminator

	bindIndices := make([][3]int, len(args))
	for i, v := range args {
		bindIndices[i][0] = args[i].Ordinal
		if v.Name != "" {
			for j, c := range []byte{':', '@', '$'} {
				buf = append(buf[:0], c)
				buf = append(buf, v.Name...)
				buf = append(buf, 0)
				bindIndices[i][j] = int(C.sqlite3_bind_parameter_index(s.s, (*C.char)(unsafe.Pointer(&buf[0]))))
			}
			args[i].Ordinal = bindIndices[i][0]
		}
	}

	var rv C.int
	for i, arg := range args {
		for j := range bindIndices[i] {
			if bindIndices[i][j] == 0 {
				continue
			}
			n := C.int(bindIndices[i][j])
			switch v := arg.Value.(type) {
			case nil:
				rv = C.sqlite3_bind_null(s.s, n)
			case string:
				p := stringData(v)
				rv = C._sqlite3_bind_text(s.s, n, (*C.char)(unsafe.Pointer(p)), C.int(len(v)))
			case int64:
				rv = C.sqlite3_bind_int64(s.s, n, C.sqlite3_int64(v))
			case bool:
				val := 0
				if v {
					val = 1
				}
				rv = C.sqlite3_bind_int(s.s, n, C.int(val))
			case float64:
				rv = C.sqlite3_bind_double(s.s, n, C.double(v))
			case []byte:
				if v == nil {
					rv = C.sqlite3_bind_null(s.s, n)
				} else {
					ln := len(v)
					if ln == 0 {
						v = placeHolder
					}
					rv = C._sqlite3_bind_blob(s.s, n, unsafe.Pointer(&v[0]), C.int(ln))
				}
			case time.Time:
				b := timefmt.Format(v)
				rv = C._sqlite3_bind_text(s.s, n, (*C.char)(unsafe.Pointer(&b[0])), C.int(len(b)))
			}
			if rv != C.SQLITE_OK {
				return s.c.lastError()
			}
		}
	}
	return nil
}

// Query the statement with arguments. Return records.
func (s *SQLiteStmt) Query(args []driver.Value) (driver.Rows, error) {
	list := make([]driver.NamedValue, len(args))
	for i, v := range args {
		list[i] = driver.NamedValue{
			Ordinal: i + 1,
			Value:   v,
		}
	}
	return s.query(context.Background(), list)
}

func (s *SQLiteStmt) query(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	if err := s.bind(args); err != nil {
		return nil, err
	}

	rows := &SQLiteRows{
		s:        s,
		nc:       int32(C.sqlite3_column_count(s.s)),
		cls:      s.cls,
		cols:     nil,
		decltype: nil,
		ctx:      ctx,
	}

	return rows, nil
}

// LastInsertId return last inserted ID.
func (r *SQLiteResult) LastInsertId() (int64, error) {
	return r.id, nil
}

// RowsAffected return how many rows affected.
func (r *SQLiteResult) RowsAffected() (int64, error) {
	return r.changes, nil
}

// Exec execute the statement with arguments. Return result object.
func (s *SQLiteStmt) Exec(args []driver.Value) (driver.Result, error) {
	list := make([]driver.NamedValue, len(args))
	for i, v := range args {
		list[i] = driver.NamedValue{
			Ordinal: i + 1,
			Value:   v,
		}
	}
	return s.exec(context.Background(), list)
}

func isInterruptErr(err error) bool {
	if err == nil {
		return false
	}
	serr, ok := err.(Error)
	return ok && serr.Code == ErrInterrupt
}

// exec executes a query that doesn't return rows. Attempts to honor context timeout.
func (s *SQLiteStmt) exec(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	if ctx.Done() == nil {
		return s.execSync(args)
	}

	type result struct {
		r   driver.Result
		err error
	}
	resultCh := make(chan result)
	defer close(resultCh)
	go func() {
		r, err := s.execSync(args)
		resultCh <- result{r, err}
	}()
	var rv result
	select {
	case rv = <-resultCh:
	case <-ctx.Done():
		select {
		case rv = <-resultCh: // no need to interrupt, operation completed in db
		default:
			// this is still racy and can be no-op if executed between sqlite3_* calls in execSync.
			C.sqlite3_interrupt(s.c.db)
			rv = <-resultCh // wait for goroutine completed
			if isInterruptErr(rv.err) {
				return nil, ctx.Err()
			}
		}
	}
	return rv.r, rv.err
}

func (s *SQLiteStmt) execSync(args []driver.NamedValue) (driver.Result, error) {
	if err := s.bind(args); err != nil {
		C.sqlite3_reset(s.s)
		C.sqlite3_clear_bindings(s.s)
		return nil, err
	}

	var rowid, changes C.longlong
	rv := C._sqlite3_step_row_internal(s.s, &rowid, &changes)
	if rv != C.SQLITE_ROW && rv != C.SQLITE_OK && rv != C.SQLITE_DONE {
		err := s.c.lastError()
		C.sqlite3_reset(s.s)
		C.sqlite3_clear_bindings(s.s)
		return nil, err
	}

	return &SQLiteResult{id: int64(rowid), changes: int64(changes)}, nil
}

// Readonly reports if this statement is considered readonly by SQLite.
//
// See: https://sqlite.org/c3ref/stmt_readonly.html
func (s *SQLiteStmt) Readonly() bool {
	return C.sqlite3_stmt_readonly(s.s) == 1
}

// Close the rows.
func (rc *SQLiteRows) Close() error {
	rc.closemu.Lock()
	defer rc.closemu.Unlock()
	s := rc.s
	if s == nil {
		return nil
	}
	rc.s = nil // remove reference to SQLiteStmt
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	if rc.cls {
		s.mu.Unlock()
		return s.Close()
	}
	rv := C.sqlite3_reset(s.s)
	if rv != C.SQLITE_OK {
		s.mu.Unlock()
		return s.c.lastError()
	}
	s.mu.Unlock()
	return nil
}

// Columns return column names.
func (rc *SQLiteRows) Columns() []string {
	rc.s.mu.Lock()
	defer rc.s.mu.Unlock()
	if rc.s.s != nil && int(rc.nc) != len(rc.cols) {
		rc.cols = make([]string, rc.nc)
		for i := 0; i < int(rc.nc); i++ {
			// NB(CEV): I put a lot of effort into interning the column names
			// but the performance hit was 1-5%. That said, it reduced allocs
			// significantly. The important lesson here was to make the string
			// pool per-connection to reduce lock contention.
			rc.cols[i] = C.GoString(C.sqlite3_column_name(rc.s.s, C.int(i)))
		}
	}
	return rc.cols
}

// DeclTypes return column types.
func (rc *SQLiteRows) DeclTypes() []string {
	rc.s.mu.Lock()
	defer rc.s.mu.Unlock()
	if rc.s.s != nil && rc.decltype == nil {
		rc.decltype = make([]string, rc.nc)
		for i := 0; i < int(rc.nc); i++ {
			rc.decltype[i] = strings.ToLower(C.GoString(C.sqlite3_column_decltype(rc.s.s, C.int(i))))
		}
	}
	return rc.decltype
}

// Next move cursor to next. Attempts to honor context timeout from QueryContext call.
func (rc *SQLiteRows) Next(dest []driver.Value) error {
	rc.s.mu.Lock()
	defer rc.s.mu.Unlock()

	if rc.s.closed {
		return io.EOF
	}

	done := rc.ctx.Done()
	if done == nil {
		return rc.nextSyncLocked(dest)
	}
	if err := rc.ctx.Err(); err != nil {
		return err // Fast check if the channel is closed
	}

	if rc.sema == nil {
		rc.sema = make(chan struct{})
	}
	go func() {
		select {
		case <-done:
			C.sqlite3_interrupt(rc.s.c.db)
			// Wait until signaled. We need to ensure that this goroutine
			// will not call interrupt after this method returns.
			<-rc.sema
		case <-rc.sema:
		}
	}()

	err := rc.nextSyncLocked(dest)
	// Signal the goroutine to exit. This send will only succeed at a point
	// where it is impossible for the goroutine to call sqlite3_interrupt.
	//
	// This is necessary to ensure the goroutine does not interrupt an
	// unrelated query if the context is cancelled after this method returns
	// but before the goroutine exits (we don't wait for it to exit).
	rc.sema <- struct{}{}
	if err != nil && isInterruptErr(err) {
		err = rc.ctx.Err()
	}
	return err
}

func (rc *SQLiteRows) colTypePtr() *C.uint8_t {
	if len(rc.coltype) == 0 {
		return nil
	}
	return (*C.uint8_t)(unsafe.Pointer(&rc.coltype[0]))
}

var emptyBlob = []byte{}

// nextSyncLocked moves cursor to next; must be called with locked mutex.
func (rc *SQLiteRows) nextSyncLocked(dest []driver.Value) error {
	rv := C._sqlite3_step_internal(rc.s.s)
	if rv == C.SQLITE_DONE {
		return io.EOF
	}
	if rv != C.SQLITE_ROW {
		rv = C.sqlite3_reset(rc.s.s)
		if rv != C.SQLITE_OK {
			return rc.s.c.lastError()
		}
		return nil
	}
	if len(dest) == 0 {
		return nil
	}

	if rc.coltype == nil {
		rc.coltype = make([]columnType, rc.nc)
		C._sqlite3_column_decltypes(rc.s.s, rc.colTypePtr(), C.int(rc.nc))
	}
	// Must call this each time since sqlite3 is loosely
	// typed and the column types can vary between rows.
	C._sqlite3_column_types(rc.s.s, rc.colTypePtr(), C.int(rc.nc))

	for i := range dest {
		switch rc.coltype[i].dataType() {
		case C.SQLITE_INTEGER:
			val := int64(C.sqlite3_column_int64(rc.s.s, C.int(i)))
			switch rc.coltype[i].declType() {
			case C.GO_SQLITE3_DECL_DATE:
				var t time.Time
				// Assume a millisecond unix timestamp if it's 13 digits -- too
				// large to be a reasonable timestamp in seconds.
				if val > 1e12 || val < -1e12 {
					val *= int64(time.Millisecond) // convert ms to nsec
					t = time.Unix(0, val)
				} else {
					t = time.Unix(val, 0)
				}
				t = t.UTC()
				if rc.s.c.loc != nil {
					t = t.In(rc.s.c.loc)
				}
				dest[i] = t
			case C.GO_SQLITE3_DECL_BOOL:
				dest[i] = val > 0
			default:
				dest[i] = val
			}
		case C.SQLITE_FLOAT:
			dest[i] = float64(C.sqlite3_column_double(rc.s.s, C.int(i)))
		case C.SQLITE_BLOB:
			r := C._sqlite3_column_blob(rc.s.s, C.int(i))
			if r.bytes == 0 {
				dest[i] = emptyBlob
			} else {
				// The default is to not copy the bytes when assigning to dest
				// because the database/sql package assumes the assigned bytes
				// are a raw reference to a buffer internal to the database/driver
				// and makes a copy of them.
				//
				// Previously, the mattn/go-sqlite3 always made a copy of the bytes.
				// Programs that relied on that behavior (which will only be programs
				// that invoke this driver directly instead of using the database/sql
				// package) may need to set the "sqlite_copy_bytes" build tag if they
				// relied on the bytes being assigned to dest to be a copy of the blob.
				if !copyBytesOnAssignment {
					// Pass a raw reference to the C memory.
					dest[i] = unsafe.Slice((*byte)(unsafe.Pointer(r.value)), int(r.bytes))
				} else {
					// Create a copy of the BLOB.
					dest[i] = C.GoBytes(unsafe.Pointer(r.value), r.bytes)
				}
			}
		case C.SQLITE_NULL:
			dest[i] = nil
		case C.SQLITE_TEXT:
			r := C._sqlite3_column_text(rc.s.s, C.int(i))
			switch rc.coltype[i].declType() {
			case C.GO_SQLITE3_DECL_DATE:
				// Handle timestamps by parsing the raw sqlite3 bytes.
				s := unsafeString((*byte)(unsafe.Pointer(r.value)), int(r.bytes))
				s = strings.TrimSuffix(s, "Z")
				var err error
				var t time.Time
				for _, format := range SQLiteTimestampFormats {
					if t, err = time.ParseInLocation(format, s, time.UTC); err == nil {
						break
					}
				}
				if err != nil {
					// The column is a time value, so return the zero time on parse failure.
					t = time.Time{}
				}
				if rc.s.c.loc != nil {
					t = t.In(rc.s.c.loc)
				}
				dest[i] = t
			case C.GO_SQLITE3_DECL_BLOB:
				// If the column's declared type is BLOB but the value is stored
				// as TEXT, which can easily happen since this library uses the
				// type of the argument to Exec as the data type, treat it as a
				// BLOB. This can vastly improve performance when scanning a value
				// into a byte slice or sql.RawBytes since it saves a copy. There
				// is no performance impact if the destination is a string.
				r := C._sqlite3_column_blob(rc.s.s, C.int(i))
				if r.bytes == 0 {
					dest[i] = emptyBlob
				} else {
					// See the above comments about byte slice assignment.
					if !copyBytesOnAssignment {
						dest[i] = unsafe.Slice((*byte)(unsafe.Pointer(r.value)), int(r.bytes))
					} else {
						dest[i] = C.GoBytes(unsafe.Pointer(r.value), r.bytes)
					}
				}
			default:
				dest[i] = C.GoStringN((*C.char)(unsafe.Pointer(r.value)), r.bytes)
			}
		}
	}
	return nil
}
