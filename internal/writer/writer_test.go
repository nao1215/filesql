package writer

import (
	"bytes"
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/nao1215/filesql/internal/reader"
)

// TestFormatsWriteWhatTheyRead holds each format's output against the bytes a
// reader of that format needs, over the records that used to come out wrong in
// one caller or another.
func TestFormatsWriteWhatTheyRead(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		format  Format
		opts    Options
		header  []string
		records [][]string
		want    string
	}{
		{
			name:    "CSV writes a header and its records",
			format:  FormatCSV,
			header:  []string{"a", "b"},
			records: [][]string{{"1", "2"}, {"3", "4"}},
			want:    "a,b\n1,2\n3,4\n",
		},
		{
			name:    "CSV quotes a lone empty field so the row is not a blank line",
			format:  FormatCSV,
			header:  []string{"note"},
			records: [][]string{{"x"}, {""}, {"y"}},
			want:    "note\nx\n\"\"\ny\n",
		},
		{
			name:    "CSV quotes an empty field only when it is alone on the line",
			format:  FormatCSV,
			header:  []string{"a", "b"},
			records: [][]string{{"", ""}},
			want:    "a,b\n,\n",
		},
		{
			name:    "CSV keeps a line break inside a field as the field has it",
			format:  FormatCSV,
			opts:    Options{LineEnding: "\r\n"},
			header:  []string{"a", "b"},
			records: [][]string{{"x\ny", "z"}},
			want:    "a,b\r\n\"x\ny\",z\r\n",
		},
		{
			name:    "CSV writes the lone empty field with the chosen terminator",
			format:  FormatCSV,
			opts:    Options{LineEnding: "\r\n"},
			header:  []string{"note"},
			records: [][]string{{""}},
			want:    "note\r\n\"\"\r\n",
		},
		{
			name:    "TSV takes every field literally",
			format:  FormatTSV,
			header:  []string{"a", "b"},
			records: [][]string{{`5'9" tall`, "x"}},
			want:    "a\tb\n5'9\" tall\tx\n",
		},
		{
			name:    "LTSV labels each field and writes no header line",
			format:  FormatLTSV,
			header:  []string{"a", "b"},
			records: [][]string{{"1", "2"}, {"3", "4"}},
			want:    "a:1\tb:2\na:3\tb:4\n",
		},
		{
			name:    "LTSV writes a label the record has no value for as empty",
			format:  FormatLTSV,
			header:  []string{"a", "b"},
			records: [][]string{{"1"}},
			want:    "a:1\tb:\n",
		},
		{
			name:    "JSONL compacts each value onto a line of its own",
			format:  FormatJSONL,
			header:  []string{"data"},
			records: [][]string{{"{\n  \"a\": 1\n}"}, {`{"b":2}`}},
			want:    "{\"a\":1}\n{\"b\":2}\n",
		},
		{
			name:    "JSONL skips a record holding nothing rather than writing a blank line",
			format:  FormatJSONL,
			header:  []string{"data"},
			records: [][]string{{`{"a":1}`}, {""}, {}, {`{"b":2}`}},
			want:    "{\"a\":1}\n{\"b\":2}\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var out bytes.Buffer
			w := New(&out, tt.format, tt.opts)
			if err := w.Header(tt.header); err != nil {
				t.Fatalf("Header(%v) = %v", tt.header, err)
			}
			for _, record := range tt.records {
				if err := w.Record(record); err != nil {
					t.Fatalf("Record(%v) = %v", record, err)
				}
			}
			if err := w.Flush(); err != nil {
				t.Fatalf("Flush() = %v", err)
			}
			if got := out.String(); got != tt.want {
				t.Errorf("output = %q, want %q", got, tt.want)
			}
		})
	}
}

// TestUnrepresentableValues holds that a value a format cannot hold is refused
// rather than written as something else, and that the refusal names the column
// and the character so a caller can say which cell to fix.
func TestUnrepresentableValues(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		format  Format
		header  []string
		record  []string
		wantMsg string
	}{
		{
			name:    "a tab in an LTSV value would open a second field",
			format:  FormatLTSV,
			header:  []string{"a"},
			record:  []string{"x\ty"},
			wantMsg: `LTSV cannot hold a value that contains a tab, and column "a" holds a tab`,
		},
		{
			name:    "a newline in an LTSV value would end the record",
			format:  FormatLTSV,
			header:  []string{"a"},
			record:  []string{"x\ny"},
			wantMsg: `LTSV cannot hold a value that contains a newline, and column "a" holds a newline`,
		},
		{
			name:    "a carriage return in an LTSV value would end the record",
			format:  FormatLTSV,
			header:  []string{"a"},
			record:  []string{"x\ry"},
			wantMsg: `LTSV cannot hold a value that contains a carriage return, and column "a" holds a carriage return`,
		},
		{
			name:    "a tab in a TSV field is the delimiter",
			format:  FormatTSV,
			header:  []string{"a"},
			record:  []string{"x\ty"},
			wantMsg: `field "x\ty" contains "\t"`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var out bytes.Buffer
			w := New(&out, tt.format, Options{})
			if err := w.Header(tt.header); err != nil {
				t.Fatalf("Header(%v) = %v", tt.header, err)
			}

			err := w.Record(tt.record)
			var werr *Error
			if !errors.As(err, &werr) {
				t.Fatalf("Record(%v) = %v, want a *writer.Error", tt.record, err)
			}
			if werr.Kind != KindUnrepresentable {
				t.Errorf("Kind = %v, want KindUnrepresentable", werr.Kind)
			}
			if werr.Error() != tt.wantMsg {
				t.Errorf("message = %q, want %q", werr.Error(), tt.wantMsg)
			}
		})
	}
}

// TestUnrepresentableLabels holds that a column name LTSV cannot read back is
// refused before any row is written, so a table with no rows is refused for the
// same reason a table with rows is.
func TestUnrepresentableLabels(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		column  string
		wantMsg string
	}{
		{
			name:    "a colon separates a label from its value",
			column:  "a:b",
			wantMsg: `an LTSV label cannot contain a colon, and column "a:b" holds a colon`,
		},
		{
			name:    "a tab separates one field from the next",
			column:  "a\tb",
			wantMsg: `an LTSV label cannot contain a tab, and column "a\tb" holds a tab`,
		},
		{
			name:    "a newline ends the record",
			column:  "a\nb",
			wantMsg: `an LTSV label cannot contain a newline, and column "a\nb" holds a newline`,
		},
		{
			name:    "a carriage return ends the record",
			column:  "a\rb",
			wantMsg: `an LTSV label cannot contain a carriage return, and column "a\rb" holds a carriage return`,
		},
		{
			name:    "a leading space is not part of the label a reader returns",
			column:  " a",
			wantMsg: `an LTSV label cannot begin or end with whitespace, and column " a" would be read back as "a"`,
		},
		{
			name:    "a trailing space is not part of it either",
			column:  "a ",
			wantMsg: `an LTSV label cannot begin or end with whitespace, and column "a " would be read back as "a"`,
		},
		{
			name:    "whitespace on both sides",
			column:  "  a b  ",
			wantMsg: `an LTSV label cannot begin or end with whitespace, and column "  a b  " would be read back as "a b"`,
		},
		{
			name:    "an ideographic space is whitespace too",
			column:  "\u3000a",
			wantMsg: `an LTSV label cannot begin or end with whitespace, and column "\u3000a" would be read back as "a"`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var out bytes.Buffer
			w := New(&out, FormatLTSV, Options{})

			err := w.Header([]string{tt.column})
			var werr *Error
			if !errors.As(err, &werr) {
				t.Fatalf("Header(%q) = %v, want a *writer.Error", tt.column, err)
			}
			if werr.Kind != KindUnrepresentable {
				t.Errorf("Kind = %v, want KindUnrepresentable", werr.Kind)
			}
			if werr.Error() != tt.wantMsg {
				t.Errorf("message = %q, want %q", werr.Error(), tt.wantMsg)
			}
			if out.Len() != 0 {
				t.Errorf("wrote %q before refusing the header", out.String())
			}
		})
	}
}

// TestJSONLRefusesWhatIsNotJSON holds that a value that cannot be compacted is
// reported rather than written, since a broken line breaks the whole file for
// a reader of it.
func TestJSONLRefusesWhatIsNotJSON(t *testing.T) {
	t.Parallel()

	var out bytes.Buffer
	w := New(&out, FormatJSONL, Options{})

	err := w.Record([]string{"{not json"})
	var werr *Error
	if !errors.As(err, &werr) {
		t.Fatalf("Record() = %v, want a *writer.Error", err)
	}
	if werr.Kind != KindEncode {
		t.Errorf("Kind = %v, want KindEncode", werr.Kind)
	}
	if !strings.Contains(werr.Error(), "the value is not JSON") {
		t.Errorf("message = %q, want it to say the value is not JSON", werr.Error())
	}
	if werr.Unwrap() == nil {
		t.Error("expected the encoder's own error underneath")
	}
}

// failingWriter fails every write, which is what a full disk or a compressor
// that cannot finish looks like from here.
type failingWriter struct{}

// Write always fails.
func (failingWriter) Write([]byte) (int, error) { return 0, errors.New("disk is full") }

// writerFormats are the four formats and a record each can hold.
//
//nolint:gochecknoglobals // shared table of cases
var writerFormats = []struct {
	name   string
	format Format
	record []string
}{
	{name: "CSV", format: FormatCSV, record: []string{"a"}},
	{name: "TSV", format: FormatTSV, record: []string{"a"}},
	{name: "LTSV", format: FormatLTSV, record: []string{"a"}},
	{name: "JSONL", format: FormatJSONL, record: []string{`{"a":1}`}},
}

// TestDestinationFailureIsReported holds that a destination that cannot take
// the bytes is reported by the time Flush returns, for every format. One
// record fits in the buffer, so this is the case where nothing has been
// written out yet when the caller stops handing records over.
func TestDestinationFailureIsReported(t *testing.T) {
	t.Parallel()

	for _, tt := range writerFormats {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			w := New(failingWriter{}, tt.format, Options{})
			err := errors.Join(
				w.Header([]string{"a"}),
				w.Record(tt.record),
				w.Flush(),
			)
			if err == nil {
				t.Fatal("writing to a failing destination reported no error")
			}
		})
	}
}

// TestDestinationFailurePartWayThroughIsReported holds the other case: enough
// records that the buffer empties itself into a destination that refuses them,
// so the failure arrives while records are still being handed over rather than
// at the end.
func TestDestinationFailurePartWayThroughIsReported(t *testing.T) {
	t.Parallel()

	// Comfortably past the 4 KiB a bufio.Writer holds, so the failure cannot be
	// waiting for Flush.
	const records = 2000

	lineEndings := []struct {
		name  string
		value string
	}{
		{name: "LF", value: "\n"},
		{name: "CRLF", value: "\r\n"},
	}

	for _, tt := range writerFormats {
		for _, ending := range lineEndings {
			t.Run(tt.name+"/"+ending.name, func(t *testing.T) {
				t.Parallel()

				w := New(failingWriter{}, tt.format, Options{LineEnding: ending.value})
				err := w.Header([]string{"a"})
				for i := 0; i < records && err == nil; i++ {
					err = w.Record(tt.record)
				}
				if err == nil {
					err = w.Flush()
				}
				if err == nil {
					t.Fatalf("wrote %d records to a failing destination and reported no error", records)
				}
			})
		}
	}
}

// TestCSVLoneEmptyFieldReportsAFailedDestination holds that the record written
// around the csv writer reports a destination that has already failed, rather
// than writing its two quotes over an output nothing received.
func TestCSVLoneEmptyFieldReportsAFailedDestination(t *testing.T) {
	t.Parallel()

	for _, ending := range []string{"\n", "\r\n"} {
		t.Run(strings.ReplaceAll(ending, "\r", "CR"), func(t *testing.T) {
			t.Parallel()

			w := New(failingWriter{}, FormatCSV, Options{LineEnding: ending})
			if err := w.Header([]string{"note"}); err != nil {
				t.Fatalf("Header() = %v", err)
			}
			// Fill the buffer so the destination has refused something before the
			// lone empty field is written.
			var err error
			for i := 0; i < 2000 && err == nil; i++ {
				err = w.Record([]string{strings.Repeat("x", 8)})
			}
			if err == nil {
				t.Fatal("filling the buffer reported no error")
			}
			if err := w.Record([]string{""}); err == nil {
				t.Error("the lone empty field reported no error over a failed destination")
			}
		})
	}
}

// TestTSVRecordWritesOneRecord covers the function on its own, which is what
// the parser package exposes: a caller of it hands over one record at a time
// and names the terminator, with the empty one meaning "\n".
func TestTSVRecordWritesOneRecord(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		record     []string
		lineEnding string
		want       string
	}{
		{name: "the empty line ending writes a line feed", record: []string{"a", "b"}, lineEnding: "", want: "a\tb\n"},
		{name: "a named line ending is written as it stands", record: []string{"a", "b"}, lineEnding: "\r\n", want: "a\tb\r\n"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var out bytes.Buffer
			if err := TSVRecord(&out, tt.record, tt.lineEnding); err != nil {
				t.Fatalf("TSVRecord() = %v", err)
			}
			if got := out.String(); got != tt.want {
				t.Errorf("output = %q, want %q", got, tt.want)
			}
		})
	}

	t.Run("a destination that cannot take the record says so", func(t *testing.T) {
		t.Parallel()

		err := TSVRecord(failingWriter{}, []string{"a"}, "\n")
		if err == nil {
			t.Fatal("writing to a failing destination reported no error")
		}
		if !strings.Contains(err.Error(), "failed to write TSV record") {
			t.Errorf("error = %q, want it to name the record it could not write", err.Error())
		}
	})

	t.Run("a quote is written as is", func(t *testing.T) {
		t.Parallel()

		var out bytes.Buffer
		if err := TSVRecord(&out, []string{"alice", `5'9" tall`}, "\n"); err != nil {
			t.Fatalf("TSVRecord() = %v", err)
		}
		if got, want := out.String(), "alice\t5'9\" tall\n"; got != want {
			t.Errorf("output = %q, want %q", got, want)
		}
	})

	t.Run("a record round trips through the TSV reader", func(t *testing.T) {
		t.Parallel()

		record := []string{`said "hi"`, `a""b`, "", "plain"}

		var out bytes.Buffer
		if err := TSVRecord(&out, record, "\r\n"); err != nil {
			t.Fatalf("TSVRecord() = %v", err)
		}
		got, err := reader.NewTSVReader(strings.NewReader(out.String())).ReadAll()
		if err != nil {
			t.Fatalf("ReadAll() = %v", err)
		}
		if len(got) != 1 || !slices.Equal(got[0], record) {
			t.Errorf("read back %q, want %q", got, [][]string{record})
		}
	})

	t.Run("a value the format cannot hold is refused and nothing is written", func(t *testing.T) {
		t.Parallel()

		for _, field := range []string{"a\tb", "a\nb", "a\rb"} {
			var out bytes.Buffer
			err := TSVRecord(&out, []string{field}, "\n")

			var writeErr *Error
			if !errors.As(err, &writeErr) || writeErr.Kind != KindUnrepresentable {
				t.Errorf("field %q: error = %v, want an unrepresentable one", field, err)
			}
			if out.Len() != 0 {
				t.Errorf("field %q: wrote %q, want nothing", field, out.String())
			}
		}
	})
}

// TestAMarkLedFirstColumnIsRefused holds that a first column name beginning
// with U+FEFF is refused by the text formats, and that a mark on any later name
// is written.
func TestAMarkLedFirstColumnIsRefused(t *testing.T) {
	t.Parallel()

	const mark = "\ufeff"

	for _, format := range []struct {
		name string
		f    Format
	}{{"csv", FormatCSV}, {"tsv", FormatTSV}, {"ltsv", FormatLTSV}} {
		t.Run(format.name, func(t *testing.T) {
			t.Parallel()

			t.Run("the first name is refused", func(t *testing.T) {
				t.Parallel()

				var out bytes.Buffer
				w := New(&out, format.f, Options{})

				err := w.Header([]string{mark + "a", "b"})
				var werr *Error
				if !errors.As(err, &werr) {
					t.Fatalf("Header = %v, want a *writer.Error", err)
				}
				if werr.Kind != KindUnrepresentableAsText {
					t.Errorf("Kind = %v, want KindUnrepresentableAsText", werr.Kind)
				}
				if out.Len() != 0 {
					t.Errorf("wrote %q before refusing the header", out.String())
				}
			})

			t.Run("a later name is written", func(t *testing.T) {
				t.Parallel()

				var out bytes.Buffer
				w := New(&out, format.f, Options{})

				if err := w.Header([]string{"a", mark + "b"}); err != nil {
					t.Fatalf("Header = %v, want the mark on a later name to be written", err)
				}
			})
		})
	}
}

// TestTextIsRefusedWhenItIsNotUTF8 pins that a text format holds characters
// rather than bytes. A value the database held that was not valid UTF-8 went
// out as itself, and the file it produced no longer loaded: the reader refused
// it with "invalid UTF-8", on a file this package had just written.
func TestTextIsRefusedWhenItIsNotUTF8(t *testing.T) {
	t.Parallel()

	for _, format := range []struct {
		name string
		f    Format
	}{{"csv", FormatCSV}, {"tsv", FormatTSV}, {"ltsv", FormatLTSV}, {"jsonl", FormatJSONL}} {
		t.Run(format.name, func(t *testing.T) {
			t.Parallel()

			t.Run("a column name", func(t *testing.T) {
				t.Parallel()

				var out bytes.Buffer
				w := New(&out, format.f, Options{})

				err := w.Header([]string{"a", "b\xff"})
				var werr *Error
				if !errors.As(err, &werr) {
					t.Fatalf("Header = %v, want a *writer.Error", err)
				}
				if werr.Kind != KindNotUTF8 {
					t.Errorf("Kind = %v, want KindNotUTF8", werr.Kind)
				}
				if out.Len() != 0 {
					t.Errorf("wrote %q before refusing the header", out.String())
				}
			})

			t.Run("a value", func(t *testing.T) {
				t.Parallel()

				var out bytes.Buffer
				w := New(&out, format.f, Options{})
				if err := w.Header([]string{"a", "b"}); err != nil {
					t.Fatalf("Header = %v", err)
				}

				err := w.Record([]string{"1", "\xff\xfe"})
				var werr *Error
				if !errors.As(err, &werr) {
					t.Fatalf("Record = %v, want a *writer.Error", err)
				}
				if werr.Kind != KindNotUTF8 {
					t.Errorf("Kind = %v, want KindNotUTF8", werr.Kind)
				}
			})

			t.Run("valid text is written", func(t *testing.T) {
				t.Parallel()

				var out bytes.Buffer
				w := New(&out, format.f, Options{})
				if err := w.Header([]string{"a", "b"}); err != nil {
					t.Fatalf("Header = %v", err)
				}
				if err := w.Record([]string{"1", "日本語🍣"}); err != nil {
					t.Errorf("Record = %v, want every valid character to be written", err)
				}
			})
		})
	}
}
