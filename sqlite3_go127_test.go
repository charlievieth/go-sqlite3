//go:build cgo && go1.27

package sqlite3

import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"
	"testing"
	"time"
)

// scanColumnTable is the schema used to exercise every sqlite3 datatype and
// every declared type that the driver treats specially.
const scanColumnTable = `
CREATE TABLE scan_column_test (
	id        INTEGER PRIMARY KEY,
	i         INTEGER,
	f         REAL,
	s         TEXT,
	b         BLOB,
	tb        BLOB,     -- text stored in a BLOB column
	bo        BOOLEAN,
	ts_text   DATETIME, -- text timestamp
	ts_int    DATETIME, -- unix timestamp
	ts_ms     TIMESTAMP,-- unix millisecond timestamp
	ts_bad    DATE,     -- unparsable timestamp
	n         INTEGER,  -- always NULL
	anyval               -- no declared type
);`

var scanColumnColumns = []string{
	"i", "f", "s", "b", "tb", "bo", "ts_text", "ts_int", "ts_ms", "ts_bad", "n", "anyval",
}

var scanColumnTime = time.Date(2024, 1, 2, 15, 4, 5, 123456789, time.UTC)

func scanColumnInsert(t testing.TB, db *sql.DB) {
	t.Helper()
	if _, err := db.Exec(scanColumnTable); err != nil {
		t.Fatal(err)
	}
	ny, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Fatal(err)
	}
	const query = `INSERT INTO scan_column_test
		(i, f, s, b, tb, bo, ts_text, ts_int, ts_ms, ts_bad, n, anyval)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);`
	rows := [][]any{
		{int64(1), 1.5, "hello", []byte("blob"), "text in blob", true,
			scanColumnTime, scanColumnTime.Unix(), scanColumnTime.UnixMilli(), "not a time", nil, int64(42)},
		{int64(-1), -2.25, "", []byte{}, "", false,
			scanColumnTime.In(ny), int64(0), int64(-1), "", nil, "str"},
		{int64(1 << 40), 1e300, strings.Repeat("x", 4096), bytes.Repeat([]byte{0, 1}, 2048), "", 1,
			"2024-01-02 15:04:05", int64(-86400), int64(2e12), nil, nil, 3.5},
		{nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil},
		{int64(0), 0.0, "0", []byte("0"), []byte("0"), 0,
			"2024-01-02T15:04:05.5Z", 1_000_000_000, 1_000_000_000_000, 12345, nil, []byte("bytes")},
	}
	for i, args := range rows {
		if _, err := db.Exec(query, args...); err != nil {
			t.Fatalf("row %d: %v", i, err)
		}
	}
}

// scanDest returns a fresh set of every destination type that is tested.
func scanDests() map[string]func() any {
	return map[string]func() any{
		"int64":       func() any { return new(int64) },
		"int":         func() any { return new(int) },
		"int32":       func() any { return new(int32) },
		"int8":        func() any { return new(int8) },
		"uint64":      func() any { return new(uint64) },
		"float64":     func() any { return new(float64) },
		"float32":     func() any { return new(float32) },
		"string":      func() any { return new(string) },
		"bytes":       func() any { return new([]byte) },
		"RawBytes":    func() any { return new(sql.RawBytes) },
		"bool":        func() any { return new(bool) },
		"time":        func() any { return new(time.Time) },
		"any":         func() any { return new(any) },
		"NullString":  func() any { return new(sql.NullString) },
		"NullInt64":   func() any { return new(sql.NullInt64) },
		"NullInt32":   func() any { return new(sql.NullInt32) },
		"NullInt16":   func() any { return new(sql.NullInt16) },
		"NullByte":    func() any { return new(sql.NullByte) },
		"NullFloat64": func() any { return new(sql.NullFloat64) },
		"NullBool":    func() any { return new(sql.NullBool) },
		"NullTime":    func() any { return new(sql.NullTime) },
		"Null[int64]": func() any { return new(sql.Null[int64]) },
		"Scanner":     func() any { return new(scanColumnScanner) },
	}
}

// scanColumnScanner is a sql.Scanner that records the driver.Value it is
// passed, which must be the same for both the legacy and ScanColumn paths.
type scanColumnScanner struct {
	typ string
	val string
}

func (s *scanColumnScanner) Scan(src any) error {
	switch v := src.(type) {
	case []byte:
		// The driver may hand us a reference to its own memory.
		s.val = string(v)
	default:
		s.val = fmt.Sprint(src)
	}
	s.typ = fmt.Sprintf("%T", src)
	return nil
}

// scanRawRows reads all rows of query using the driver.Rows.Next method and
// converts each value with sql.ConvertAssign, which is what database/sql did
// before Go 1.27.
func scanRawRows(t *testing.T, db *sql.DB, query string, makeDest func() any) (results []any, errs []error) {
	t.Helper()
	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	err = conn.Raw(func(dc any) error {
		c := dc.(*SQLiteConn)
		rows, err := c.Query(query, nil)
		if err != nil {
			return err
		}
		defer rows.Close()
		dest := make([]driver.Value, len(rows.Columns()))
		for {
			err := rows.Next(dest)
			if err == io.EOF {
				return nil
			}
			if err != nil {
				return err
			}
			for _, v := range dest {
				d := makeDest()
				err := sql.ConvertAssign(driver.ScanContext{}, d, v)
				// Dereference RawBytes since the memory is only valid
				// until the next call to Next.
				if rb, ok := d.(*sql.RawBytes); ok {
					if *rb != nil {
						*rb = sql.RawBytes(copyBytes(*rb))
					} else if v != nil {
						// database/sql appends to an internal buffer that
						// is never nil so an empty value results in an
						// empty RawBytes, not a nil one.
						*rb = sql.RawBytes{}
					}
				}
				results = append(results, d)
				errs = append(errs, err)
			}
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	return results, errs
}

// scanColumnRows reads all rows of query using database/sql, which uses the
// RowsColumnScanner interface.
func scanColumnRows(t *testing.T, db *sql.DB, query string, makeDest func() any) (results []any, errs []error) {
	t.Helper()
	rows, err := db.Query(query)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		t.Fatal(err)
	}
	// database/sql does not allow Scan to be called twice without calling
	// Next when scanning into sql.RawBytes so scan the whole row at once.
	_, rawBytes := makeDest().(*sql.RawBytes)
	for rows.Next() {
		if rawBytes {
			dests := make([]any, len(cols))
			for i := range dests {
				dests[i] = makeDest()
			}
			err := rows.Scan(dests...)
			if err != nil {
				t.Fatal(err)
			}
			for _, d := range dests {
				rb := d.(*sql.RawBytes)
				if *rb != nil {
					*rb = sql.RawBytes(copyBytes(*rb))
				}
				results = append(results, d)
				errs = append(errs, nil)
			}
			continue
		}
		for i := range cols {
			// Scan one column at a time so that a conversion error for one
			// destination type does not hide the results for the others.
			dests := make([]any, len(cols))
			for j := range dests {
				dests[j] = new(any)
			}
			d := makeDest()
			dests[i] = d
			err := rows.Scan(dests...)
			if err != nil {
				// Unwrap the "sql: Scan error on column index" prefix.
				err = errors.Unwrap(err)
				if err == nil {
					t.Fatal("expected a wrapped error")
				}
			}
			if rb, ok := d.(*sql.RawBytes); ok && *rb != nil {
				*rb = sql.RawBytes(copyBytes(*rb))
			}
			results = append(results, d)
			errs = append(errs, err)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return results, errs
}

func TestScanColumnParity(t *testing.T) {
	for _, loc := range []string{"", "UTC", "America/New_York", "auto"} {
		t.Run("loc="+loc, func(t *testing.T) {
			dsn := ":memory:"
			if loc != "" {
				dsn += "?_loc=" + loc
			}
			db, err := sql.Open("sqlite3", dsn)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			db.SetMaxOpenConns(1)
			scanColumnInsert(t, db)

			query := "SELECT " + strings.Join(scanColumnColumns, ", ") +
				" FROM scan_column_test ORDER BY id;"
			for name, makeDest := range scanDests() {
				t.Run(name, func(t *testing.T) {
					want, wantErrs := scanRawRows(t, db, query, makeDest)
					got, gotErrs := scanColumnRows(t, db, query, makeDest)
					if len(want) != len(got) {
						t.Fatalf("got %d results want: %d", len(got), len(want))
					}
					if len(want) == 0 {
						t.Fatal("no results")
					}
					for i := range want {
						row := i / len(scanColumnColumns)
						col := scanColumnColumns[i%len(scanColumnColumns)]
						if (wantErrs[i] == nil) != (gotErrs[i] == nil) {
							t.Errorf("row %d column %s: error mismatch:\n\tgot:  %v\n\twant: %v",
								row, col, gotErrs[i], wantErrs[i])
							continue
						}
						if wantErrs[i] != nil {
							if gotErrs[i].Error() != wantErrs[i].Error() {
								t.Errorf("row %d column %s: error mismatch:\n\tgot:  %v\n\twant: %v",
									row, col, gotErrs[i], wantErrs[i])
							}
							continue
						}
						// time.Time values must be compared with Equal and
						// have the same Location.
						if wt, ok := want[i].(*time.Time); ok {
							gt := got[i].(*time.Time)
							if !gt.Equal(*wt) || gt.Location().String() != wt.Location().String() {
								t.Errorf("row %d column %s: got: %v want: %v", row, col, *gt, *wt)
							}
							continue
						}
						if !reflect.DeepEqual(want[i], got[i]) {
							t.Errorf("row %d column %s: got: %#v (%T) want: %#v (%T)",
								row, col, got[i], got[i], want[i], want[i])
						}
					}
				})
			}
		})
	}
}

// TestScanColumnTypes checks a handful of conversions explicitly so that the
// parity test cannot pass simply because both code paths share a bug.
func TestScanColumnTypes(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	scanColumnInsert(t, db)

	var (
		i       int64
		f       float64
		s       string
		b       []byte
		tb      []byte
		bo      bool
		tsText  time.Time
		tsInt   time.Time
		tsMs    time.Time
		tsBad   time.Time
		n       any
		anyval  any
		nullStr sql.NullString
	)
	err = db.QueryRow(`SELECT i, f, s, b, tb, bo, ts_text, ts_int, ts_ms, ts_bad, n, anyval, n
		FROM scan_column_test WHERE id = 1`).Scan(
		&i, &f, &s, &b, &tb, &bo, &tsText, &tsInt, &tsMs, &tsBad, &n, &anyval, &nullStr)
	if err != nil {
		t.Fatal(err)
	}
	if i != 1 {
		t.Errorf("i: got %d want 1", i)
	}
	if f != 1.5 {
		t.Errorf("f: got %v want 1.5", f)
	}
	if s != "hello" {
		t.Errorf("s: got %q want %q", s, "hello")
	}
	if string(b) != "blob" {
		t.Errorf("b: got %q want %q", b, "blob")
	}
	if string(tb) != "text in blob" {
		t.Errorf("tb: got %q want %q", tb, "text in blob")
	}
	if !bo {
		t.Errorf("bo: got false want true")
	}
	if !tsText.Equal(scanColumnTime) {
		t.Errorf("ts_text: got %v want %v", tsText, scanColumnTime)
	}
	if want := scanColumnTime.Truncate(time.Second); !tsInt.Equal(want) {
		t.Errorf("ts_int: got %v want %v", tsInt, want)
	}
	if want := scanColumnTime.Truncate(time.Millisecond); !tsMs.Equal(want) {
		t.Errorf("ts_ms: got %v want %v", tsMs, want)
	}
	if !tsBad.IsZero() {
		t.Errorf("ts_bad: got %v want zero time", tsBad)
	}
	if n != nil {
		t.Errorf("n: got %v want nil", n)
	}
	if anyval != int64(42) {
		t.Errorf("anyval: got %#v want int64(42)", anyval)
	}
	if nullStr.Valid {
		t.Errorf("nullStr: got %+v want invalid", nullStr)
	}

	// NULL into non-nullable types must fail the same way database/sql does.
	var str string
	err = db.QueryRow(`SELECT n FROM scan_column_test WHERE id = 1`).Scan(&str)
	if err == nil || !strings.Contains(err.Error(), "converting NULL to string is unsupported") {
		t.Errorf("NULL into *string: got %v", err)
	}

	// Scanner implementations must still be used.
	var sc scanColumnScanner
	if err := db.QueryRow(`SELECT s FROM scan_column_test WHERE id = 1`).Scan(&sc); err != nil {
		t.Fatal(err)
	}
	if sc.typ != "string" || sc.val != "hello" {
		t.Errorf("Scanner: got %+v", sc)
	}

	// Out of range integers must fail.
	var i8 int8
	err = db.QueryRow(`SELECT i FROM scan_column_test WHERE id = 3`).Scan(&i8)
	if err == nil || !strings.Contains(err.Error(), "out of range") {
		t.Errorf("int8 overflow: got %v", err)
	}
}

// TestScanColumnRawBytes checks that sql.RawBytes references sqlite3's memory
// and is valid until the next call to Next.
func TestScanColumnRawBytes(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec(`CREATE TABLE raw (s TEXT, b BLOB)`); err != nil {
		t.Fatal(err)
	}
	var want []string
	for r := 'a'; r <= 'z'; r++ {
		s := strings.Repeat(string(r), 64)
		if _, err := db.Exec(`INSERT INTO raw VALUES (?, ?)`, s, []byte(s)); err != nil {
			t.Fatal(err)
		}
		want = append(want, s)
	}
	rows, err := db.Query(`SELECT s, b FROM raw`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var rs, rb sql.RawBytes
	for i := 0; rows.Next(); i++ {
		if err := rows.Scan(&rs, &rb); err != nil {
			t.Fatal(err)
		}
		if string(rs) != want[i] {
			t.Errorf("%d: text: got %q want %q", i, rs, want[i])
		}
		if string(rb) != want[i] {
			t.Errorf("%d: blob: got %q want %q", i, rb, want[i])
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	// Scanning RawBytes after Close must fail rather than reference freed memory.
	rows.Close()
	if err := rows.Scan(&rs, &rb); err == nil {
		t.Error("Scan after Close succeeded")
	}
}

func TestScanColumnErrors(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	err = conn.Raw(func(dc any) error {
		c := dc.(*SQLiteConn)
		rows, err := c.Query("SELECT 1, 'a'", nil)
		if err != nil {
			return err
		}
		rc := rows.(*SQLiteRows)
		var v any
		if err := rc.ScanColumn(driver.ScanContext{}, 0, &v); err != errScanBeforeNext {
			t.Errorf("ScanColumn before NextRow: got %v want %v", err, errScanBeforeNext)
		}
		if err := rc.NextRow(); err != nil {
			return err
		}
		if err := rc.ScanColumn(driver.ScanContext{}, 2, &v); err == nil {
			t.Error("ScanColumn with an out of range index succeeded")
		}
		if err := rc.ScanColumn(driver.ScanContext{}, -1, &v); err == nil {
			t.Error("ScanColumn with a negative index succeeded")
		}
		if err := rc.ScanColumn(driver.ScanContext{}, 1, (*string)(nil)); err == nil {
			t.Error("ScanColumn into a nil pointer succeeded")
		}
		if err := rc.ScanColumn(driver.ScanContext{}, 1, &v); err != nil {
			t.Error(err)
		} else if v != "a" {
			t.Errorf("got %#v want %q", v, "a")
		}
		if err := rc.NextRow(); err != io.EOF {
			t.Errorf("NextRow at end: got %v want io.EOF", err)
		}
		if err := rc.Close(); err != nil {
			return err
		}
		if err := rc.NextRow(); err != io.EOF {
			t.Errorf("NextRow after Close: got %v want io.EOF", err)
		}
		if err := rc.ScanColumn(driver.ScanContext{}, 0, &v); err == nil {
			t.Error("ScanColumn after Close succeeded")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

// TestScanColumnContextCancel checks that NextRow honors a cancelled context.
func TestScanColumnContextCancel(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// A recursive query that never terminates.
	rows, err := db.QueryContext(ctx, `WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM c) SELECT x FROM c`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var x int64
	for i := 0; i < 10 && rows.Next(); i++ {
		if err := rows.Scan(&x); err != nil {
			t.Fatal(err)
		}
	}
	if x != 10 {
		t.Fatalf("got x = %d want 10", x)
	}
	cancel()
	for rows.Next() {
	}
	if err := rows.Err(); !errors.Is(err, context.Canceled) {
		t.Errorf("got %v want %v", err, context.Canceled)
	}
}
