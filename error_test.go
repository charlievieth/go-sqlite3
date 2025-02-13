// Copyright (C) 2019 Yasuhiro Matsumoto <mattn.jp@gmail.com>.
//
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

//go:build cgo
// +build cgo

package sqlite3

import (
	"database/sql"
	"io/ioutil"
	"os"
	"path"
	"testing"
)

func TestSimpleError(t *testing.T) {
	e := ErrError.Error()
	if e != "SQL logic error or missing database" && e != "SQL logic error" {
		t.Error("wrong error code: " + e)
	}
}

func TestCorruptDbErrors(t *testing.T) {
	dirName, err := ioutil.TempDir("", "sqlite3")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dirName)

	dbFileName := path.Join(dirName, "test.db")
	f, err := os.Create(dbFileName)
	if err != nil {
		t.Error(err)
	}
	f.Write([]byte{1, 2, 3, 4, 5})
	f.Close()

	db, err := sql.Open("sqlite3", dbFileName)
	if err == nil {
		_, err = db.Exec("drop table foo")
	}

	sqliteErr := err.(Error)
	if sqliteErr.Code != ErrNotADB {
		t.Error("wrong error code for corrupted DB")
	}
	if err.Error() == "" {
		t.Error("wrong error string for corrupted DB")
	}
	db.Close()
}

func TestSqlLogicErrors(t *testing.T) {
	dirName, err := ioutil.TempDir("", "sqlite3")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dirName)

	dbFileName := path.Join(dirName, "test.db")
	db, err := sql.Open("sqlite3", dbFileName)
	if err != nil {
		t.Error(err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE Foo (id INTEGER PRIMARY KEY)")
	if err != nil {
		t.Error(err)
	}

	const expectedErr = "table Foo already exists"
	_, err = db.Exec("CREATE TABLE Foo (id INTEGER PRIMARY KEY)")
	if err.Error() != expectedErr {
		t.Errorf("Unexpected error: %s, expected %s", err.Error(), expectedErr)
	}

}

func TestExtendedErrorCodes_ForeignKey(t *testing.T) {
	dirName, err := ioutil.TempDir("", "sqlite3-err")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dirName)

	dbFileName := path.Join(dirName, "test.db")
	db, err := sql.Open("sqlite3", dbFileName)
	if err != nil {
		t.Error(err)
	}
	defer db.Close()

	_, err = db.Exec("PRAGMA foreign_keys=ON;")
	if err != nil {
		t.Errorf("PRAGMA foreign_keys=ON: %v", err)
	}

	_, err = db.Exec(`CREATE TABLE Foo (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		value INTEGER NOT NULL,
		ref INTEGER NULL REFERENCES Foo (id),
		UNIQUE(value)
	);`)
	if err != nil {
		t.Error(err)
	}

	_, err = db.Exec("INSERT INTO Foo (ref, value) VALUES (100, 100);")
	if err == nil {
		t.Error("No error!")
	} else {
		sqliteErr := err.(Error)
		if sqliteErr.Code != ErrConstraint {
			t.Errorf("Wrong basic error code: %d != %d",
				sqliteErr.Code, ErrConstraint)
		}
		if sqliteErr.ExtendedCode != ErrConstraintForeignKey {
			t.Errorf("Wrong extended error code: %d != %d",
				sqliteErr.ExtendedCode, ErrConstraintForeignKey)
		}
	}

}

func TestExtendedErrorCodes_NotNull(t *testing.T) {
	dirName, err := ioutil.TempDir("", "sqlite3-err")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dirName)

	dbFileName := path.Join(dirName, "test.db")
	db, err := sql.Open("sqlite3", dbFileName)
	if err != nil {
		t.Error(err)
	}
	defer db.Close()

	_, err = db.Exec("PRAGMA foreign_keys=ON;")
	if err != nil {
		t.Errorf("PRAGMA foreign_keys=ON: %v", err)
	}

	_, err = db.Exec(`CREATE TABLE Foo (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		value INTEGER NOT NULL,
		ref INTEGER NULL REFERENCES Foo (id),
		UNIQUE(value)
	);`)
	if err != nil {
		t.Error(err)
	}

	res, err := db.Exec("INSERT INTO Foo (value) VALUES (100);")
	if err != nil {
		t.Fatalf("Creating first row: %v", err)
	}

	id, err := res.LastInsertId()
	if err != nil {
		t.Fatalf("Retrieving last insert id: %v", err)
	}

	_, err = db.Exec("INSERT INTO Foo (ref) VALUES (?);", id)
	if err == nil {
		t.Error("No error!")
	} else {
		sqliteErr := err.(Error)
		if sqliteErr.Code != ErrConstraint {
			t.Errorf("Wrong basic error code: %d != %d",
				sqliteErr.Code, ErrConstraint)
		}
		if sqliteErr.ExtendedCode != ErrConstraintNotNull {
			t.Errorf("Wrong extended error code: %d != %d",
				sqliteErr.ExtendedCode, ErrConstraintNotNull)
		}
	}

}

func TestExtendedErrorCodes_Unique(t *testing.T) {
	dirName, err := ioutil.TempDir("", "sqlite3-err")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dirName)

	dbFileName := path.Join(dirName, "test.db")
	db, err := sql.Open("sqlite3", dbFileName)
	if err != nil {
		t.Error(err)
	}
	defer db.Close()

	_, err = db.Exec("PRAGMA foreign_keys=ON;")
	if err != nil {
		t.Errorf("PRAGMA foreign_keys=ON: %v", err)
	}

	_, err = db.Exec(`CREATE TABLE Foo (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		value INTEGER NOT NULL,
		ref INTEGER NULL REFERENCES Foo (id),
		UNIQUE(value)
	);`)
	if err != nil {
		t.Error(err)
	}

	res, err := db.Exec("INSERT INTO Foo (value) VALUES (100);")
	if err != nil {
		t.Fatalf("Creating first row: %v", err)
	}

	id, err := res.LastInsertId()
	if err != nil {
		t.Fatalf("Retrieving last insert id: %v", err)
	}

	_, err = db.Exec("INSERT INTO Foo (ref, value) VALUES (?, 100);", id)
	if err == nil {
		t.Error("No error!")
	} else {
		sqliteErr := err.(Error)
		if sqliteErr.Code != ErrConstraint {
			t.Errorf("Wrong basic error code: %d != %d",
				sqliteErr.Code, ErrConstraint)
		}
		if sqliteErr.ExtendedCode != ErrConstraintUnique {
			t.Errorf("Wrong extended error code: %d != %d",
				sqliteErr.ExtendedCode, ErrConstraintUnique)
		}
		extended := sqliteErr.Code.Extend(3).Error()
		expected := "constraint failed"
		if extended != expected {
			t.Errorf("Wrong basic error code: %q != %q",
				extended, expected)
		}
	}
}

func TestError_SystemErrno(t *testing.T) {
	_, n, _ := Version()
	if n < 3012000 {
		t.Skip("sqlite3_system_errno requires sqlite3 >= 3.12.0")
	}

	// open a non-existent database in read-only mode so we get an IO error.
	db, err := sql.Open("sqlite3", "file:nonexistent.db?mode=ro")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	err = db.Ping()
	if err == nil {
		t.Fatal("expected error pinging read-only non-existent database, but got nil")
	}

	serr, ok := err.(Error)
	if !ok {
		t.Fatalf("expected error to be of type Error, but got %[1]T %[1]v", err)
	}

	if serr.SystemErrno == 0 {
		t.Fatal("expected SystemErrno to be set")
	}

	if !os.IsNotExist(serr.SystemErrno) {
		t.Errorf("expected SystemErrno to be a not exists error, but got %v", serr.SystemErrno)
	}
}

func TestErrorStringCacheSize(t *testing.T) {
	for i := 0; i < 50_000; i++ {
		_ = errorString(i)
	}
	n := 0
	o := 0
	errStrCache.Range(func(_, v any) bool {
		n++
		return true
	})
	errStrCache.intern.Range(func(_, v any) bool {
		o++
		return true
	})
	if n > 1024 {
		t.Fatalf("len(errStrCache) should be capped at %d got: %d", 1024, n)
	}
	if o > 128 {
		// If sqlite3 adds a lot of new error messages this value
		// will need to be increased.
		t.Errorf("expected errStrMsgCache to be below %d got: %d", 128, o)
	}
}
