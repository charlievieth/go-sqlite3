//go:build cgo && go1.27

package sqlite3

import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"strconv"
	"time"
	"unsafe"
)

// Starting with Go 1.27 the database/sql package uses the RowsColumnScanner
// interface, if implemented, instead of Rows.Next. This allows the driver to
// scan directly into the user provided destination, which avoids boxing each
// value into a driver.Value and the generic conversion logic in database/sql.
var _ driver.RowsColumnScanner = (*SQLiteRows)(nil)

var errScanBeforeNext = errors.New("sqlite3: ScanColumn called before NextRow")

// NextRow advances to the next row of data. It returns io.EOF when there are
// no more rows. NextRow implements driver.RowsColumnScanner.
func (rc *SQLiteRows) NextRow() error {
	s := rc.s
	if s == nil {
		return io.EOF // closed
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	row, err := rc.stepLocked()
	if err != nil || !row {
		return err
	}
	if rc.nc > 0 {
		rc.loadColumnTypesLocked()
	}
	return nil
}

// ScanColumn copies the column at the given index in the current row into the
// value pointed to by dest. ScanColumn implements driver.RowsColumnScanner.
//
// The common destination types are handled directly, all other types are
// handled by database/sql.ConvertAssign using the same driver.Value that
// Next would have produced.
//
// ScanColumn must not be called concurrently with NextRow, Next, or Close.
// Unlike NextRow it does not lock the statement since it is called once per
// column and the lock is measurably expensive: database/sql holds the driver
// connection lock for the duration of Scan, and closes statements under that
// same lock, so the statement cannot be finalized while ScanColumn runs.
func (rc *SQLiteRows) ScanColumn(scanCtx driver.ScanContext, index int, dest any) error {
	s := rc.s
	if s == nil {
		return errors.New("sqlite3: ScanColumn called on closed rows")
	}
	if s.closed || s.s == nil {
		return errors.New("sqlite3: ScanColumn called on closed statement")
	}
	if uint(index) >= uint(len(rc.coltype)) {
		if rc.coltype == nil {
			return errScanBeforeNext
		}
		return fmt.Errorf("sqlite3: column index %d out of range [0, %d)", index, len(rc.coltype))
	}
	ct := rc.coltype[index]
	null := ct.dataType() == dataTypeNull

	// Unwrap the sql.Null* types, which are the most common sql.Scanner
	// implementations. This mirrors their Scan methods, which set Valid and
	// delegate the conversion of the underlying value.
	switch d := dest.(type) {
	case *sql.NullString:
		if null {
			*d = sql.NullString{}
			return nil
		}
		d.Valid = true
		dest = &d.String
	case *sql.NullInt64:
		if null {
			*d = sql.NullInt64{}
			return nil
		}
		d.Valid = true
		dest = &d.Int64
	case *sql.NullInt32:
		if null {
			*d = sql.NullInt32{}
			return nil
		}
		d.Valid = true
		dest = &d.Int32
	case *sql.NullInt16:
		if null {
			*d = sql.NullInt16{}
			return nil
		}
		d.Valid = true
		dest = &d.Int16
	case *sql.NullByte:
		if null {
			*d = sql.NullByte{}
			return nil
		}
		d.Valid = true
		dest = &d.Byte
	case *sql.NullFloat64:
		if null {
			*d = sql.NullFloat64{}
			return nil
		}
		d.Valid = true
		dest = &d.Float64
	case *sql.NullBool:
		if null {
			*d = sql.NullBool{}
			return nil
		}
		d.Valid = true
		dest = &d.Bool
	case *sql.NullTime:
		if null {
			*d = sql.NullTime{}
			return nil
		}
		d.Valid = true
		dest = &d.Time
	}

	switch ct.dataType() {
	case dataTypeInteger:
		val := rc.columnInt64(index)
		switch ct.declType() {
		case declTypeDate:
			return scanTime(scanCtx, dest, rc.unixTime(val))
		case declTypeBool:
			return scanBool(scanCtx, dest, val > 0)
		default:
			return scanInt64(scanCtx, dest, val)
		}
	case dataTypeFloat:
		return scanFloat64(scanCtx, dest, rc.columnDouble(index))
	case dataTypeBlob:
		return scanBytes(scanCtx, dest, rc.columnBlob(index))
	case dataTypeNull:
		return scanNull(scanCtx, dest)
	case dataTypeText:
		switch ct.declType() {
		case declTypeDate:
			p, n := rc.columnText(index)
			return scanTime(scanCtx, dest, rc.parseTime(unsafeString(p, n)))
		case declTypeBlob:
			// See the comment in scanRowLocked.
			return scanBytes(scanCtx, dest, rc.columnBlob(index))
		default:
			p, n := rc.columnText(index)
			return scanText(scanCtx, dest, p, n)
		}
	}
	return fmt.Errorf("sqlite3: unsupported column type: %d", ct.dataType())
}

// copyBytes returns a copy of b. A nil slice is returned if b is nil.
func copyBytes(b []byte) []byte {
	if b == nil {
		return nil
	}
	c := make([]byte, len(b))
	copy(c, b)
	return c
}

// goString returns a Go string containing a copy of the n bytes of C memory
// starting at p.
func goString(p *byte, n int) string {
	if p == nil || n <= 0 {
		return ""
	}
	return string(unsafe.Slice(p, n)) // converting a []byte copies
}

// copyCBytes returns a copy of the n bytes of C memory starting at p.
func copyCBytes(p *byte, n int) []byte {
	b := make([]byte, n)
	if n > 0 {
		copy(b, unsafe.Slice(p, n))
	}
	return b
}

func scanInt64(scanCtx driver.ScanContext, dest any, v int64) error {
	switch d := dest.(type) {
	case *int64:
		if d == nil {
			break
		}
		*d = v
		return nil
	case *int:
		if d == nil || int64(int(v)) != v {
			break
		}
		*d = int(v)
		return nil
	case *string:
		if d == nil {
			break
		}
		*d = strconv.FormatInt(v, 10)
		return nil
	case *any:
		if d == nil {
			break
		}
		*d = v
		return nil
	}
	return sql.ConvertAssign(scanCtx, dest, v)
}

func scanFloat64(scanCtx driver.ScanContext, dest any, v float64) error {
	switch d := dest.(type) {
	case *float64:
		if d == nil {
			break
		}
		*d = v
		return nil
	case *any:
		if d == nil {
			break
		}
		*d = v
		return nil
	}
	return sql.ConvertAssign(scanCtx, dest, v)
}

func scanBool(scanCtx driver.ScanContext, dest any, v bool) error {
	switch d := dest.(type) {
	case *bool:
		if d == nil {
			break
		}
		*d = v
		return nil
	case *any:
		if d == nil {
			break
		}
		*d = v
		return nil
	}
	return sql.ConvertAssign(scanCtx, dest, v)
}

func scanTime(scanCtx driver.ScanContext, dest any, t time.Time) error {
	switch d := dest.(type) {
	case *time.Time:
		if d == nil {
			break
		}
		*d = t
		return nil
	case *any:
		if d == nil {
			break
		}
		*d = t
		return nil
	}
	return sql.ConvertAssign(scanCtx, dest, t)
}

func scanNull(scanCtx driver.ScanContext, dest any) error {
	switch d := dest.(type) {
	case *any:
		if d == nil {
			break
		}
		*d = nil
		return nil
	case *[]byte:
		if d == nil {
			break
		}
		*d = nil
		return nil
	case *sql.RawBytes:
		if d == nil {
			break
		}
		*d = nil
		return nil
	}
	return sql.ConvertAssign(scanCtx, dest, nil)
}

// scanBytes assigns BLOB b to dest. Unless the "sqlite_copy_bytes" build tag
// is set, b references memory owned by sqlite3 and is only valid until the
// statement is stepped, reset, or finalized.
func scanBytes(scanCtx driver.ScanContext, dest any, b []byte) error {
	switch d := dest.(type) {
	case *[]byte:
		if d == nil {
			break
		}
		*d = copyBytes(b)
		return nil
	case *sql.RawBytes:
		if d == nil {
			break
		}
		// database/sql holds a lock on the Rows until the next call to Next,
		// Scan, or Close which guarantees the memory is valid as long as the
		// RawBytes is.
		*d = b
		return nil
	case *string:
		if d == nil {
			break
		}
		*d = string(b)
		return nil
	case *any:
		if d == nil {
			break
		}
		*d = copyBytes(b)
		return nil
	}
	return sql.ConvertAssign(scanCtx, dest, b)
}

// scanText assigns the TEXT value stored in the n bytes of C memory starting
// at p to dest.
func scanText(scanCtx driver.ScanContext, dest any, p *byte, n int) error {
	switch d := dest.(type) {
	case *string:
		if d == nil {
			break
		}
		*d = goString(p, n)
		return nil
	case *[]byte:
		if d == nil {
			break
		}
		*d = copyCBytes(p, n)
		return nil
	case *sql.RawBytes:
		if d == nil {
			break
		}
		if copyBytesOnAssignment {
			*d = copyCBytes(p, n)
		} else {
			// Reference the C memory directly. See scanBytes for why this is safe.
			*d = unsafe.Slice(p, n)
		}
		return nil
	case *any:
		if d == nil {
			break
		}
		*d = goString(p, n)
		return nil
	}
	return sql.ConvertAssign(scanCtx, dest, goString(p, n))
}
