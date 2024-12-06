//go:build !sqlite_copy_bytes
// +build !sqlite_copy_bytes

package sqlite3

// Assign a reference to sqlite3's C memory when assigning a BLOB to a byte
// slice. This is safe because the database/sql package creates a copy.
const copyBytesOnAssignment = false
