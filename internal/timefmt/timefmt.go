package timefmt

import (
	"errors"
	"strconv"
	"time"
)

const digits = "0123456789"

const smallsString = "00010203040506070809" +
	"10111213141516171819" +
	"20212223242526272829" +
	"30313233343536373839" +
	"40414243444546474849" +
	"50515253545556575859" +
	"60616263646566676869" +
	"70717273747576777879" +
	"80818283848586878889" +
	"90919293949596979899"

func appendInt2(dst []byte, i int) []byte {
	u := uint(i)
	if u < 10 {
		return append(dst, '0', digits[u])
	}
	return append(dst, smallsString[u*2:u*2+2]...)
}

// appendInt appends the decimal form of x to b and returns the result.
// If the decimal form (excluding sign) is shorter than width, the result is padded with leading 0's.
// Duplicates functionality in strconv, but avoids dependency.
func appendInt(b []byte, x int, width int) []byte {
	u := uint(x)
	if x < 0 {
		b = append(b, '-')
		u = uint(-x)
	}

	// 2-digit and 4-digit fields are the most common in time formats.
	utod := func(u uint) byte { return '0' + byte(u) }
	switch {
	case width == 2 && u < 1e2:
		return append(b, utod(u/1e1), utod(u%1e1))
	case width == 4 && u < 1e4:
		return append(b, utod(u/1e3), utod(u/1e2%1e1), utod(u/1e1%1e1), utod(u%1e1))
	}

	// Compute the number of decimal digits.
	var n int
	if u == 0 {
		n = 1
	}
	for u2 := u; u2 > 0; u2 /= 10 {
		n++
	}

	// Add 0-padding.
	for pad := width - n; pad > 0; pad-- {
		b = append(b, '0')
	}

	// Ensure capacity.
	if len(b)+n <= cap(b) {
		b = b[:len(b)+n]
	} else {
		b = append(b, make([]byte, n)...)
	}

	// Assemble decimal in reverse order.
	i := len(b) - 1
	for u >= 10 && i > 0 {
		q := u / 10
		b[i] = utod(u - q*10)
		u = q
		i--
	}
	b[i] = utod(u)
	return b
}

// formatTime formats time t with layout "2006-01-02 15:04:05.999999999-07:00"
func Format(t time.Time) []byte {
	b := make([]byte, 0, len("2006-01-02 15:04:05.999999999-07:00"))

	// 2006-01-02
	year, month, day := t.Date()
	b = appendInt(b, year, 4)
	b = append(b, '-')
	b = appendInt2(b, int(month))
	b = append(b, '-')
	b = appendInt2(b, day)

	// 15:04:05
	b = append(b, ' ')
	b = appendInt2(b, t.Hour())
	b = append(b, ':')
	b = appendInt2(b, t.Minute())
	b = append(b, ':')
	b = appendInt2(b, t.Second())

	// .999999999
	b = append(b, '.')
	b = appendInt(b, t.Nanosecond(), 9)
	for len(b) > 0 && b[len(b)-1] == '0' {
		b = b[:len(b)-1]
	}
	if len(b) > 0 && b[len(b)-1] == '.' {
		b = b[:len(b)-1]
	}

	// -07:00
	_, offset := t.Zone()
	zone := offset / 60
	if zone < 0 {
		b = append(b, '-')
		zone = -zone
	} else {
		b = append(b, '+')
	}
	b = appendInt2(b, zone/60)
	b = append(b, ':')
	b = appendInt2(b, zone%60)

	return b
}

func isLeap(year int) bool {
	return year%4 == 0 && (year%100 != 0 || year%400 == 0)
}

// daysBefore[m] counts the number of days in a non-leap year
// before month m begins. There is an entry for m=12, counting
// the number of days before January of next year (365).
var daysBefore = [...]int32{
	0,
	31,
	31 + 28,
	31 + 28 + 31,
	31 + 28 + 31 + 30,
	31 + 28 + 31 + 30 + 31,
	31 + 28 + 31 + 30 + 31 + 30,
	31 + 28 + 31 + 30 + 31 + 30 + 31,
	31 + 28 + 31 + 30 + 31 + 30 + 31 + 31,
	31 + 28 + 31 + 30 + 31 + 30 + 31 + 31 + 30,
	31 + 28 + 31 + 30 + 31 + 30 + 31 + 31 + 30 + 31,
	31 + 28 + 31 + 30 + 31 + 30 + 31 + 31 + 30 + 31 + 30,
	31 + 28 + 31 + 30 + 31 + 30 + 31 + 31 + 30 + 31 + 30 + 31,
}

func daysIn(m time.Month, year int) int {
	if m == time.February && isLeap(year) {
		return 29
	}
	return int(daysBefore[m] - daysBefore[m-1])
}

// isDigit reports whether s[i] is in range and is a decimal digit.
func isDigit(s string, i int) bool {
	if len(s) <= i {
		return false
	}
	c := s[i]
	return '0' <= c && c <= '9'
}

func commaOrPeriod(b byte) bool {
	return b == '.' || b == ','
}

var errBad = errors.New("bad value for field") // placeholder not passed to user

func parseNanoseconds(value string, nbytes int) (ns int, rangeErrString string, err error) {
	if !commaOrPeriod(value[0]) {
		err = errBad
		return
	}
	if nbytes > 10 {
		value = value[:10]
		nbytes = 10
	}
	if ns, err = strconv.Atoi(value[1:nbytes]); err != nil {
		return
	}
	if ns < 0 {
		rangeErrString = "fractional second"
		return
	}
	// We need nanoseconds, which means scaling by the number
	// of missing digits in the format, maximum length 10.
	scaleDigits := 10 - nbytes
	for i := 0; i < scaleDigits; i++ {
		ns *= 10
	}
	return
}

func parse(s string, local *time.Location) (time.Time, bool) {
	// parseUint parses s as an unsigned decimal integer and
	// verifies that it is within some range.
	// If it is invalid or out-of-range,
	// it sets ok to false and returns the min value.
	ok := true
	parseUint := func(s string, min, max int) (x int) {
		for _, c := range []byte(s) {
			if c < '0' || '9' < c {
				ok = false
				return min
			}
			x = x*10 + int(c) - '0'
		}
		if x < min || max < x {
			ok = false
			return min
		}
		return x
	}

	// Parse the date and time.
	// "2006-01-02 15:04:05.999999999-07:00"
	if len(s) < len("2006-01-02 15:04:05") {
		return time.Time{}, false
	}
	year := parseUint(s[0:4], 0, 9999)                            // e.g., 2006
	month := parseUint(s[5:7], 1, 12)                             // e.g., 01
	day := parseUint(s[8:10], 1, daysIn(time.Month(month), year)) // e.g., 02
	hour := parseUint(s[11:13], 0, 23)                            // e.g., 15
	min := parseUint(s[14:16], 0, 59)                             // e.g., 04
	sec := parseUint(s[17:19], 0, 59)                             // e.g., 05

	if !ok || !(s[4] == '-' && s[7] == '-' && (s[10] == ' ' || s[10] == 'T') && s[13] == ':' && s[16] == ':') {
		return time.Time{}, false
	}
	s = s[19:]

	// Parse the fractional second.
	var nsec int
	if len(s) >= 2 && s[0] == '.' && isDigit(s, 1) {
		n := 2
		for ; n < len(s) && isDigit(s, n); n++ {
		}
		nsec, _, _ = parseNanoseconds(s, n)
		s = s[n:]
	}

	// Parse the time zone.
	t := time.Date(year, time.Month(month), day, hour, min, sec, nsec, time.UTC)
	if len(s) != 1 || s[0] != 'Z' {
		if len(s) != len("-07:00") {
			return time.Time{}, false
		}
		hr := parseUint(s[1:3], 0, 23) // e.g., 07
		mm := parseUint(s[4:6], 0, 59) // e.g., 00
		if !ok || !((s[0] == '-' || s[0] == '+') && s[3] == ':') {
			return time.Time{}, false
		}
		zoneOffset := (hr*60 + mm) * 60
		if s[0] == '-' {
			zoneOffset *= -1
		}
		t = t.Add(-(time.Duration(zoneOffset) * time.Second))

		// Use local zone with the given offset if possible.
		t2 := t.In(local)
		_, offset := t2.Zone()
		if offset == zoneOffset {
			t = t2
		} else {
			t = t.In(time.FixedZone("", zoneOffset))
		}
	}

	return t, true
}

// Parse is an specialized version of time.Parse that is optimized for
// the below two timestamps:
//
//   - "2006-01-02 15:04:05.999999999-07:00"
//   - "2006-01-02T15:04:05.999999999-07:00"
func Parse(s string, local *time.Location) (time.Time, error) {
	if t, ok := parse(s, local); ok {
		return t, nil
	}
	if len(s) > 10 && s[10] == 'T' {
		return time.Parse("2006-01-02T15:04:05.999999999-07:00", s)
	}
	return time.Parse("2006-01-02 15:04:05.999999999-07:00", s)
}
