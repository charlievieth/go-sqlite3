//go:build !go1.21
// +build !go1.21

package sqlite3

import (
	"C"
	"unsafe"
)

// stringData is a safe version of unsafe.StringData that handles empty strings.
func stringData(s string) *byte {
	if len(s) != 0 {
		b := *(*[]byte)(unsafe.Pointer(&s))
		return &b[0]
	}
	// The return value of unsafe.StringData
	// is unspecified if the string is empty.
	return &placeHolder[0]
}

// unsafeString converts ptr to a Go string.
func unsafeString(ptr *byte, n int) string {
	if ptr == nil || n <= 0 {
		return ""
	}
	return C.GoStringN((*C.char)(unsafe.Pointer(ptr)), C.int(n))
}
