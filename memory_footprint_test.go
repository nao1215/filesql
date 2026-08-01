//go:build benchmark

package filesql

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
)

// TestLoadMemoryFootprint reports what loading a CSV costs, at several sizes.
//
// It exists because the number this package had been tracking was the wrong
// one. `go test -benchmem` reports B/op, which counts every byte the operation
// ever allocated, garbage included — for the 100,000-row fixture that is around
// 141MB, and it says nothing about how much memory is held at once. The
// question a caller actually has is whether a given file fits, and the answer
// has two parts that B/op conflates:
//
//   - The Go heap stays small and flat. Loading is chunked, so the parser holds
//     roughly a chunk, not the file.
//   - The rows land in the in-memory SQLite database, which is not on the Go
//     heap. That is where the footprint is, and only RSS shows it.
//
// Run with: go test -tags benchmark -run TestLoadMemoryFootprint -v .
//
// The RSS reading is Linux-only; elsewhere the test reports heap alone.
//
// RSS is a coarse instrument: the allocator never returns pages, so one size's
// reading carries every earlier size's baseline and the absolute ratio comes out
// too high. The marginal column is the honest figure — the extra resident bytes
// per extra file byte between two consecutive sizes, which cancels the baseline.
// It agrees with running each size in a process of its own.
func TestLoadMemoryFootprint(t *testing.T) {
	header, body := readBenchmarkFixture(t)

	t.Logf("%9s %10s %14s %14s %10s", "rows", "file MB", "heap after MB", "RSS delta MB", "marginal")

	var prevFileMB, prevDeltaMB float64
	for _, rows := range []int{100000, 200000, 400000, 800000} {
		path := writeCSV(t, header, body, rows)
		fi, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}

		runtime.GC()
		runtime.GC()
		rssBefore := readRSS()

		ctx := context.Background()
		db, err := OpenContext(ctx, path)
		if err != nil {
			t.Fatalf("OpenContext(%d rows): %v", rows, err)
		}

		var got int
		if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM data").Scan(&got); err != nil {
			t.Fatal(err)
		}
		if got != rows {
			t.Fatalf("row count = %d, want %d", got, rows)
		}

		runtime.GC()
		rssAfter := readRSS()
		var ms runtime.MemStats
		runtime.ReadMemStats(&ms)

		fileMB := float64(fi.Size()) / (1 << 20)
		heapMB := float64(ms.HeapAlloc) / (1 << 20)
		if rssBefore == 0 || rssAfter == 0 {
			t.Logf("%9d %10.1f %14.1f %14s %10s", rows, fileMB, heapMB, "n/a", "n/a")
		} else {
			deltaMB := (float64(rssAfter) - float64(rssBefore)) / (1 << 20)
			marginal := "-"
			if prevFileMB > 0 {
				marginal = fmt.Sprintf("%.2fx", (deltaMB-prevDeltaMB)/(fileMB-prevFileMB))
			}
			t.Logf("%9d %10.1f %14.1f %14.1f %10s", rows, fileMB, heapMB, deltaMB, marginal)
			prevFileMB, prevDeltaMB = fileMB, deltaMB
		}

		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

// readBenchmarkFixture returns the benchmark CSV's header and its data rows.
func readBenchmarkFixture(t *testing.T) (string, []string) {
	t.Helper()

	f, err := os.Open(filepath.Join("testdata", "benchmark", "customers100000.csv"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	var lines []string
	for sc.Scan() {
		lines = append(lines, sc.Text())
	}
	if err := sc.Err(); err != nil {
		t.Fatal(err)
	}
	if len(lines) < 2 {
		t.Fatalf("fixture has %d lines, want a header and at least one row", len(lines))
	}
	return lines[0], lines[1:]
}

// writeCSV writes a CSV of the requested row count, cycling the fixture's rows.
func writeCSV(t *testing.T, header string, body []string, rows int) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), "data.csv")
	f, err := os.Create(path) //nolint:gosec // path is under t.TempDir()
	if err != nil {
		t.Fatal(err)
	}
	w := bufio.NewWriter(f)
	if _, err := fmt.Fprintln(w, header); err != nil {
		t.Fatal(err)
	}
	for i := range rows {
		if _, err := fmt.Fprintln(w, body[i%len(body)]); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	return path
}

// readRSS returns the process's resident set size, or 0 where it cannot be read.
// The Go heap alone understates a load, because the rows are held by the
// in-memory SQLite database rather than by Go.
func readRSS() uint64 {
	f, err := os.Open("/proc/self/status")
	if err != nil {
		return 0
	}
	defer f.Close()

	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := sc.Text()
		if !strings.HasPrefix(line, "VmRSS:") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 2 {
			return 0
		}
		kb, err := strconv.ParseUint(fields[1], 10, 64)
		if err != nil {
			return 0
		}
		return kb * 1024
	}
	return 0
}
