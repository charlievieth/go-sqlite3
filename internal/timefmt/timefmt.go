package timefmt

import "time"

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
