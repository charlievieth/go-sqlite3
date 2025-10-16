// Copyright (C) 2018 G.J.R. Timmer <gjr.timmer@gmail.com>.
//
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

//go:build !sqlite_userauth
// +build !sqlite_userauth

package sqlite3

import "errors"

var (
	ErrUnauthorized  = errors.New("SQLITE_AUTH: Unauthorized")
	ErrAdminRequired = errors.New("SQLITE_AUTH: Unauthorized; Admin Privileges Required")
)

// NB: Even though userauth is no longer supported, we preserve
// these methods to maintain backwards compatibility.

// Authenticate will perform an authentication of the provided username
// and password against the database.
//
// If a database contains the SQLITE_USER table, then the
// call to Authenticate must be invoked with an
// appropriate username and password prior to enable read and write
// access to the database.
//
// Return SQLITE_OK on success or SQLITE_ERROR if the username/password
// combination is incorrect or unknown.
//
// If the SQLITE_USER table is not present in the database file, then
// this interface is a harmless no-op returnning SQLITE_OK.
//
// Deprecated: The sqlite3 ext/userauth module is [deprecated].
//
// [deprecated]: https://www.sqlite.org/src/artifact/ca7e9ee82ca4e1c
func (c *SQLiteConn) Authenticate(username, password string) error {
	return nil
}

// AuthUserAdd can be used (by an admin user only)
// to create a new user.  When called on a no-authentication-required
// database, this routine converts the database into an authentication-
// required database, automatically makes the added user an
// administrator, and logs in the current connection as that user.
// The AuthUserAdd only works for the "main" database, not
// for any ATTACH-ed databases. Any call to AuthUserAdd by a
// non-admin user results in an error.
//
// Deprecated: The sqlite3 ext/userauth module is [deprecated].
//
// [deprecated]: https://www.sqlite.org/src/artifact/ca7e9ee82ca4e1c
func (c *SQLiteConn) AuthUserAdd(username, password string, admin bool) error {
	return nil
}

// AuthUserChange can be used to change a users
// login credentials or admin privilege.  Any user can change their own
// login credentials.  Only an admin user can change another users login
// credentials or admin privilege setting.  No user may change their own
// admin privilege setting.
//
// Deprecated: The sqlite3 ext/userauth module is [deprecated].
//
// [deprecated]: https://www.sqlite.org/src/artifact/ca7e9ee82ca4e1c
func (c *SQLiteConn) AuthUserChange(username, password string, admin bool) error {
	return nil
}

// AuthUserDelete can be used (by an admin user only)
// to delete a user.  The currently logged-in user cannot be deleted,
// which guarantees that there is always an admin user and hence that
// the database cannot be converted into a no-authentication-required
// database.
//
// Deprecated: The sqlite3 ext/userauth module is [deprecated].
//
// [deprecated]: https://www.sqlite.org/src/artifact/ca7e9ee82ca4e1c
func (c *SQLiteConn) AuthUserDelete(username string) error {
	return nil
}

// AuthEnabled checks if the database is protected by user authentication
//
// Deprecated: The sqlite3 ext/userauth module is [deprecated].
//
// [deprecated]: https://www.sqlite.org/src/artifact/ca7e9ee82ca4e1c
func (c *SQLiteConn) AuthEnabled() (exists bool) {
	return false
}
