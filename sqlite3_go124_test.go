//go:build go1.24
// +build go1.24

package sqlite3

import "testing"

func TestGGO(t *testing.T) {
	t.Skip("TODO: see if adding go1.24 optimization hint helps")
}
