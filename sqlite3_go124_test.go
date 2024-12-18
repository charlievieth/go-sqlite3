//go:build go1.24
// +build go1.24

package sqlite3

import "testing"

func TestGGO(t *testing.T) {
	const msg = `
#cgo noescape   _sqlite3_bind_blob
#cgo noescape   _sqlite3_bind_text
#cgo noescape   _sqlite3_column_blob
#cgo noescape   _sqlite3_column_decltypes
#cgo noescape   _sqlite3_column_text
#cgo noescape   _sqlite3_column_types
#cgo noescape   _sqlite3_create_function
#cgo noescape   _sqlite3_exec_no_args
#cgo noescape   _sqlite3_limit
#cgo noescape   _sqlite3_open_v2
#cgo noescape   _sqlite3_prepare_query
#cgo noescape   _sqlite3_prepare_v2
#cgo noescape   _sqlite3_prepare_v2_internal
#cgo noescape   _sqlite3_step_internal
#cgo noescape   _sqlite3_step_row_internal
#cgo noescape   sqlite3_aggregate_context
#cgo noescape   sqlite3_bind_double
#cgo noescape   sqlite3_bind_int
#cgo noescape   sqlite3_bind_int64
#cgo noescape   sqlite3_bind_null
#cgo noescape   sqlite3_bind_parameter_count
#cgo noescape   sqlite3_bind_parameter_index

#cgo nocallback sqlite3_bind_double
#cgo nocallback sqlite3_bind_int
#cgo nocallback sqlite3_bind_int64
#cgo nocallback sqlite3_bind_null
#cgo nocallback sqlite3_bind_parameter_count
#cgo nocallback sqlite3_bind_parameter_index

#cgo noescape   sqlite3_clear_bindings
#cgo noescape   sqlite3_close_v2
#cgo noescape   sqlite3_column_count
#cgo noescape   sqlite3_column_decltype
#cgo noescape   sqlite3_column_double
#cgo noescape   sqlite3_column_int64
#cgo noescape   sqlite3_column_name

#cgo nocallback sqlite3_column_count
#cgo nocallback sqlite3_column_decltype
#cgo nocallback sqlite3_column_double
#cgo nocallback sqlite3_column_int64
#cgo nocallback sqlite3_column_name

#cgo noescape   sqlite3_commit_hook
#cgo noescape   sqlite3_create_collation
#cgo noescape   sqlite3_db_filename
#cgo noescape   sqlite3_errcode
#cgo noescape   sqlite3_errmsg
#cgo noescape   sqlite3_exec
#cgo noescape   sqlite3_extended_errcode
#cgo noescape   sqlite3_file_control
#cgo noescape   sqlite3_finalize
#cgo noescape   sqlite3_get_autocommit
#cgo noescape   sqlite3_interrupt
#cgo noescape   sqlite3_libversion
#cgo noescape   sqlite3_libversion_number
#cgo noescape   sqlite3_reset
#cgo noescape   sqlite3_rollback_hook
#cgo noescape   sqlite3_set_authorizer
#cgo noescape   sqlite3_sourceid
#cgo noescape   sqlite3_stmt_readonly
#cgo noescape   sqlite3_system_errno
#cgo noescape   sqlite3_threadsafe
#cgo noescape   sqlite3_update_hook
`
	// https://pkg.go.dev/cmd/cgo@master#hdr-Optimizing_calls_of_C_code
	t.Fatal("TODO: see if adding go1.24 optimization hint helps")
}
