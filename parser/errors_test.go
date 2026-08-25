package parser

import (
	"bytes"
	"errors"
	"strings"
	"testing"
)

// TestParseSentinels checks that every failure Parse reports for input it
// cannot turn into a table carries the sentinel a caller matches, and that
// attaching the sentinel left the message the read side wrote alone.
func TestParseSentinels(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    func() *bytes.Reader
		fileType FileType
		sentinel error
		message  string
	}{
		{
			name:     "no reader",
			input:    func() *bytes.Reader { return nil },
			fileType: CSV,
			sentinel: ErrNilReader,
			message:  "reader cannot be nil",
		},
		{
			name:     "file type this package does not parse",
			input:    func() *bytes.Reader { return bytes.NewReader([]byte("a,b\n")) },
			fileType: FileType(-1),
			sentinel: ErrUnsupportedFileType,
			message:  "unsupported file type",
		},
		{
			name:     "empty CSV",
			input:    func() *bytes.Reader { return bytes.NewReader(nil) },
			fileType: CSV,
			sentinel: ErrEmptyData,
			message:  "empty CSV data",
		},
		{
			name:     "empty JSONL",
			input:    func() *bytes.Reader { return bytes.NewReader(nil) },
			fileType: JSONL,
			sentinel: ErrEmptyData,
			message:  "empty JSONL data",
		},
		{
			name:     "LTSV holding no record of that shape",
			input:    func() *bytes.Reader { return bytes.NewReader([]byte("not a labeled field\n")) },
			fileType: LTSV,
			sentinel: ErrEmptyData,
			message:  "no valid LTSV records found",
		},
		{
			name:     "empty Parquet",
			input:    func() *bytes.Reader { return bytes.NewReader(nil) },
			fileType: Parquet,
			sentinel: ErrEmptyData,
			message:  "empty parquet file",
		},
		{
			name:     "empty XLSX",
			input:    func() *bytes.Reader { return bytes.NewReader(nil) },
			fileType: XLSX,
			sentinel: ErrEmptyData,
			message:  "empty XLSX file",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var err error
			if in := tt.input(); in == nil {
				_, err = Parse(nil, tt.fileType)
			} else {
				_, err = Parse(in, tt.fileType)
			}
			if err == nil {
				t.Fatalf("Parse(%v) returned no error", tt.fileType)
			}
			if !errors.Is(err, tt.sentinel) {
				t.Errorf("Parse(%v) = %v, want it to match %v", tt.fileType, err, tt.sentinel)
			}
			if err.Error() != tt.message {
				t.Errorf("Parse(%v) says %q, want %q", tt.fileType, err.Error(), tt.message)
			}
		})
	}
}

// TestParseSentinelsDoNotOverlap checks that a failure carrying one sentinel
// does not also match another: a caller that switches on them would take the
// first arm whatever the fault was.
func TestParseSentinelsDoNotOverlap(t *testing.T) {
	t.Parallel()

	_, err := Parse(bytes.NewReader(nil), CSV)
	if !errors.Is(err, ErrEmptyData) {
		t.Fatalf("empty CSV = %v, want it to match ErrEmptyData", err)
	}
	if errors.Is(err, ErrNilReader) || errors.Is(err, ErrUnsupportedFileType) {
		t.Errorf("empty CSV = %v, want it to match ErrEmptyData alone", err)
	}
}

// TestParseSyntaxErrorCarriesNoEmptySentinel checks that input that is broken
// rather than absent stays told apart from input holding no table.
func TestParseSyntaxErrorCarriesNoEmptySentinel(t *testing.T) {
	t.Parallel()

	_, err := Parse(strings.NewReader("a,b\n1\n"), CSV)
	if !errors.Is(err, ErrCSVSyntax) {
		t.Fatalf("short CSV record = %v, want it to match ErrCSVSyntax", err)
	}
	if errors.Is(err, ErrEmptyData) {
		t.Errorf("short CSV record = %v, want it not to match ErrEmptyData", err)
	}
}
