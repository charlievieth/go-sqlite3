//go:build sqlite_copy_bytes
// +build sqlite_copy_bytes

package sqlite3

// Create a copy of the BLOB's C memory with C.GoBytes when assigning to a
// destination in Scan. This is the prior behavior of mattn/go-sqlite3.
const copyBytesOnAssignment = true
