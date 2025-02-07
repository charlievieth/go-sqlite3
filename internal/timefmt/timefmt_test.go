package timefmt_test

import (
	"math/rand"
	"strconv"
	"testing"
	"time"

	"github.com/charlievieth/go-sqlite3"
	"github.com/charlievieth/go-sqlite3/internal/timefmt"
)

func TestFormatTime(t *testing.T) {
	// Create time locations with random offsets
	rr := rand.New(rand.NewSource(time.Now().UnixNano()))
	locs := make([]*time.Location, 1000)
	for i := range locs {
		offset := rr.Intn(60 * 60 * 14) // 14 hours
		if rr.Int()&1 != 0 {
			offset = -offset
		}
		locs[i] = time.FixedZone(strconv.Itoa(offset), offset)
	}
	// Append some standard locations
	locs = append(locs, time.Local, time.UTC)

	times := []time.Time{
		{},
		time.Now(),
		time.Date(0, 0, 0, 0, 0, 0, 0, time.UTC),
		time.Date(1, 1, 1, 1, 1, 1, 1, time.UTC),
		time.Date(20_000, 1, 1, 1, 1, 1, 1, time.UTC),
		time.Date(-1, 0, 0, 0, 0, 0, 0, time.UTC),
	}

	for _, loc := range locs {
		for _, tt := range times {
			tt = tt.In(loc)
			got := timefmt.Format(tt)
			want := tt.Format(sqlite3.SQLiteTimestampFormats[0])
			if string(got) != want {
				t.Errorf("Format(%q) = %q; want: %q", tt.Format(time.RFC3339Nano), got, want)
			}
		}
	}
}

func TestFormatTimeAllocs(t *testing.T) {
	allocs := testing.AllocsPerRun(100, func() {
		_ = timefmt.Format(time.Now())
	})
	if allocs != 1 {
		t.Fatalf("expected 1 allocation per-run got: %.1f", allocs)
	}
}

func TestParse(t *testing.T) {
	rr := rand.New(rand.NewSource(time.Now().UnixNano()))
	locs := make([]*time.Location, 1000)
	for i := range locs {
		offset := rr.Intn(60 * 60 * 14) // 14 hours
		if rr.Int()&1 != 0 {
			offset = -offset
		}
		locs[i] = time.FixedZone(strconv.Itoa(offset), offset)
	}
	// Append some standard locations
	locs = append(locs, time.Local, time.UTC)

	times := []time.Time{
		{},
		time.Now(),
		time.Date(0, 0, 0, 0, 0, 0, 0, time.UTC),
		time.Date(1, 1, 1, 1, 1, 1, 1, time.UTC),
		time.Date(20_000, 1, 1, 1, 1, 1, 1, time.UTC),
		time.Date(-1, 0, 0, 0, 0, 0, 0, time.UTC),
		time.Date(2028, 2, 29, 0, 0, 0, 0, time.UTC),   // Leap day
		time.Date(2028, 2, 29, 1, 1, 1, 1, time.Local), // Leap day
	}
	for i := 0; i < 100; i++ {
		times = append(times, time.Now().Add(time.Duration(rr.Int63n(int64(time.Hour*24*365)))))
	}

	passed := 0
	for _, loc := range locs {
		for _, tt := range times {
			tt = tt.In(loc)
			for _, format := range sqlite3.SQLiteTimestampFormats[:2] {
				s := tt.Format(format)
				want, err := time.ParseInLocation(format, s, loc)
				if err != nil {
					continue
				}
				got, err := timefmt.Parse(s, loc)
				if err != nil {
					t.Error(err)
					continue
				}
				if !got.Equal(want) {
					t.Errorf("timefmt.Parse(%q) = %s; want: %s", s, got, want)
					continue
				}
				passed++
			}
		}
	}
	if passed == 0 {
		t.Fatal("No tests passed")
	}
}

func BenchmarkFormat(b *testing.B) {
	loc, err := time.LoadLocation("America/New_York")
	if err != nil {
		b.Fatal(err)
	}
	ts := time.Date(2024, 1, 2, 15, 4, 5, 123456789, loc)
	for i := 0; i < b.N; i++ {
		_ = timefmt.Format(ts)
	}
}

func BenchmarkParse(b *testing.B) {
	layout := sqlite3.SQLiteTimestampFormats[0]
	loc, err := time.LoadLocation("America/New_York")
	if err != nil {
		b.Fatal(err)
	}
	ts := time.Date(2024, 1, 2, 15, 4, 5, 123456789, loc).Format(layout)

	b.Run("Stdlib", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, err := time.Parse(layout, ts)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("Timefmt", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, err := timefmt.Parse(ts, time.Local)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("Timefmt_T", func(b *testing.B) {
		ts := time.Date(2024, 1, 2, 15, 4, 5, 123456789, loc).Format(sqlite3.SQLiteTimestampFormats[1])
		for i := 0; i < b.N; i++ {
			_, err := timefmt.Parse(ts, time.Local)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}
