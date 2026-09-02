//go:build benchmark

package wire

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"testing"
)

// BenchmarkParseReader measures reading one Fedwire message into its table.
func BenchmarkParseReader(b *testing.B) {
	raw := fixture(b, "fedWireMessage-CustomerTransfer.fed")

	b.ReportAllocs()
	for b.Loop() {
		if _, err := ParseReader(bytes.NewReader(raw)); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkWriteToWriter measures writing one message back out.
//
// A write stages the message and reads it back before any of it reaches the
// caller's writer, so it costs a write plus a parse rather than a write. That
// is what the number here is for: the check is the only thing standing between
// a caller and a file in which one field has silently become a copy of another,
// and the price of it should be visible rather than assumed.
func BenchmarkWriteToWriter(b *testing.B) {
	raw := fixture(b, "fedWireMessage-CustomerTransfer.fed")
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
