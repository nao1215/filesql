//go:build benchmark

package ach

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"testing"
)

// BenchmarkParseReader measures reading one ACH file into its seven tables.
func BenchmarkParseReader(b *testing.B) {
	raw := fixture(b, "ppd-debit.ach")

	b.ReportAllocs()
	for b.Loop() {
		if _, err := ParseReader(bytes.NewReader(raw)); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkWriteToWriter measures writing one file back out, which deep-copies
// the parsed file, applies each table's rows to it and recalculates every
// control record. The row loops now also record the coordinate each row names,
// so that a row naming a record another row already named is refused instead of
// overwriting it; that is one map insert per row against the copy and the
// recalculation around it.
func BenchmarkWriteToWriter(b *testing.B) {
	raw := fixture(b, "ppd-debit.ach")
	ts, err := ParseReader(bytes.NewReader(raw))
	if err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	for b.Loop() {
		if err := ts.WriteToWriter(io.Discard); err != nil {
			b.Fatal(err)
		}
	}
}

func fixture(b *testing.B, name string) []byte {
	b.Helper()

	raw, err := os.ReadFile(filepath.Join("testdata", name))
	if err != nil {
		b.Fatal(err)
	}
	return raw
}
