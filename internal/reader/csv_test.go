package reader

import (
	"encoding/csv"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCSVReader_ReadsWhatTheFileHolds covers the shapes a CSV reader has to get
// right, and the one this reader exists for: a line break between quotes is
// data, so the carriage return before it survives.
func TestCSVReader_ReadsWhatTheFileHolds(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		in   string
		want [][]string
	}{
		{"plain records", "a,b\n1,2\n", [][]string{{"a", "b"}, {"1", "2"}}},
		{"CRLF terminated", "a,b\r\n1,2\r\n", [][]string{{"a", "b"}, {"1", "2"}}},
		{"no trailing newline", "a,b\n1,2", [][]string{{"a", "b"}, {"1", "2"}}},
		{"quoted delimiter", "a,b\n\"x,y\",2\n", [][]string{{"a", "b"}, {"x,y", "2"}}},
		{"doubled quote", "a\n\"x\"\"y\"\n", [][]string{{"a"}, {`x"y`}}},
		{"quoted line feed", "a\n\"x\ny\"\n", [][]string{{"a"}, {"x\ny"}}},
		{"quoted carriage return and line feed", "a\n\"x\r\ny\"\n", [][]string{{"a"}, {"x\r\ny"}}},
		{"quoted lone carriage return", "a\n\"x\ry\"\n", [][]string{{"a"}, {"x\ry"}}},
		{"empty field", "a,b\n,2\n", [][]string{{"a", "b"}, {"", "2"}}},
		{"trailing empty field", "a,b\n1,\n", [][]string{{"a", "b"}, {"1", ""}}},
		{"quoted empty field", "a\n\"\"\n", [][]string{{"a"}, {""}}},
		{"blank line is not a record", "a,b\n\n1,2\n", [][]string{{"a", "b"}, {"1", "2"}}},
		{"multibyte data", "名前,住所\n太郎,\"東京\r\n千代田\"\n", [][]string{{"名前", "住所"}, {"太郎", "東京\r\n千代田"}}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := NewCSVReader(strings.NewReader(tt.in)).ReadAll()
			if err != nil {
				t.Fatalf("ReadAll: %v", err)
			}
			assert.Equal(t, tt.want, got)
		})
	}
}

// TestCSVReader_RefusesWhatIsNotCSV pins the failures, since a reader that
// accepts anything turns a malformed file into wrong data.
func TestCSVReader_RefusesWhatIsNotCSV(t *testing.T) {
	t.Parallel()

	tests := map[string]string{
		"bare quote in an unquoted field": "a\nx\"y\n",
		"quoted field never closed":       "a\n\"xy\n",
		"text after a closing quote":      "a\n\"xy\"z\n",
	}

	for name, in := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			_, err := NewCSVReader(strings.NewReader(in)).ReadAll()
			if !errors.Is(err, ErrCSVSyntax) {
				t.Fatalf("err = %v, want ErrCSVSyntax", err)
			}
		})
	}
}

// TestCSVReader_FieldsPerRecord pins the three settings, which the two callers
// rely on: the loader allows a ragged row so the malformed-row policy decides,
// and the one-shot parser does not.
func TestCSVReader_FieldsPerRecord(t *testing.T) {
	t.Parallel()

	const ragged = "a,b\n1\n"

	t.Run("zero takes the count from the first record", func(t *testing.T) {
		t.Parallel()
		_, err := NewCSVReader(strings.NewReader(ragged)).ReadAll()
		if !errors.Is(err, ErrCSVSyntax) {
			t.Fatalf("err = %v, want ErrCSVSyntax", err)
		}
	})

	t.Run("negative allows any count", func(t *testing.T) {
		t.Parallel()
		r := NewCSVReader(strings.NewReader(ragged))
		r.FieldsPerRecord = -1
		got, err := r.ReadAll()
		if err != nil {
			t.Fatalf("ReadAll: %v", err)
		}
		assert.Equal(t, [][]string{{"a", "b"}, {"1"}}, got)
	})
}

// TestCSVReader_RefusesAnUnusableDelimiter pins that a delimiter this reader
// cannot honor is reported rather than replaced with a comma. Falling back
// would split the file somewhere the caller did not ask for and say nothing,
// which is the shape of failure this reader exists to stop one level up.
func TestCSVReader_RefusesAnUnusableDelimiter(t *testing.T) {
	t.Parallel()

	for name, comma := range map[string]rune{
		"multi-byte": '§',
		"quote":      '"',
		"line feed":  '\n',
		// A negative rune is no character at all, and converting one to a byte
		// gives an unrelated byte to split fields at.
		"negative":        -1,
		"carriage return": '\r',
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			r := NewCSVReader(strings.NewReader("a,b\n1,2\n"))
			r.Comma = comma
			if _, err := r.ReadAll(); !errors.Is(err, ErrCSVSyntax) {
				t.Fatalf("err = %v, want ErrCSVSyntax", err)
			}
		})
	}

	t.Run("an ordinary delimiter is accepted", func(t *testing.T) {
		t.Parallel()

		r := NewCSVReader(strings.NewReader("a;b\n1;2\n"))
		r.Comma = ';'
		got, err := r.ReadAll()
		if err != nil {
			t.Fatalf("ReadAll: %v", err)
		}
		assert.Equal(t, [][]string{{"a", "b"}, {"1", "2"}}, got)
	})
}

// TestCSVReader_AgreesWithEncodingCSV is the differential the replacement has to
// satisfy: for input without a carriage return inside quotes, which is the case
// the standard library and this reader are meant to answer identically, they do.
func TestCSVReader_AgreesWithEncodingCSV(t *testing.T) {
	t.Parallel()

	inputs := []string{
		"a,b\n1,2\n",
		"a,b\r\n1,2\r\n",
		"a\n\"x,y\"\n",
		"a\n\"x\"\"y\"\n",
		"a\n\"x\ny\"\n",
		"a,b\n,\n",
		"a\n\"\"\n",
		"a,b\n1,2",
		"名前\n太郎\n",
	}

	for _, in := range inputs {
		t.Run(strings.ReplaceAll(in, "\n", "\\n"), func(t *testing.T) {
			t.Parallel()

			want, wantErr := csv.NewReader(strings.NewReader(in)).ReadAll()
			got, gotErr := NewCSVReader(strings.NewReader(in)).ReadAll()

			if (wantErr != nil) != (gotErr != nil) {
				t.Fatalf("error presence differs: encoding/csv=%v, ours=%v", wantErr, gotErr)
			}
			assert.Equal(t, want, got, "records differ from encoding/csv")
		})
	}
}

// FuzzCSVReader checks the floor: no input makes the reader panic or hang, and
// what it reads it can read again.
func FuzzCSVReader(f *testing.F) {
	for _, seed := range []string{
		"a,b\n1,2\n", "a\n\"x\r\ny\"\n", "\"", "a\n\"x\"\"y\"\n", "\n\n\n", "a,b\n1\n",
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, in string) {
		if len(in) > 1<<16 {
			t.Skip()
		}
		r := NewCSVReader(strings.NewReader(in))
		r.FieldsPerRecord = -1
		records, err := r.ReadAll()
		if err != nil && !errors.Is(err, ErrCSVSyntax) && !errors.Is(err, io.EOF) {
			t.Fatalf("unexpected error kind: %v", err)
		}
		_ = records
	})
}

// TestDelimitedRecordIsBounded pins the bound both delimited readers hold a
// record to. It lives beside the CSV reader because the limit is one rule
// shared by the two, stated once in delimited.go, and the point of the test is
// that they answer alike.
//
// TSV had a bound and CSV had none, so a CSV record with no terminator was
// buffered whole: reading CSV from an io.Reader had no memory bound at all, and
// a body arriving as one record cost the process the whole of it. A file on
// disk was bounded by its own size either way; a stream is bounded by whoever
// is sending it.
func TestDelimitedRecordIsBounded(t *testing.T) {
	t.Parallel()

	// A limit small enough to reach in a test, standing in for the real one.
	const limit = 1 << 10

	// readAll reads every record of body as format, under the lowered limit.
	readAll := func(t *testing.T, format Format, body string) error {
		t.Helper()
		if format == FormatTSV {
			reader := NewTSVReader(strings.NewReader(body))
			reader.maxRecord = limit
			_, err := reader.ReadAll()
			return err
		}
		reader := NewCSVReader(strings.NewReader(body))
		reader.maxRecord = limit
		_, err := reader.ReadAll()
		return err
	}

	// record builds a file of one header line and one record of size bytes.
	record := func(size int) string {
		return "v\n" + strings.Repeat("x", size) + "\n"
	}

	for _, format := range []Format{FormatCSV, FormatTSV} {
		t.Run(format.String()+" accepts a record under the limit", func(t *testing.T) {
			t.Parallel()

			assert.NoError(t, readAll(t, format, record(limit/2)))
		})

		t.Run(format.String()+" refuses a record past the limit", func(t *testing.T) {
			t.Parallel()

			err := readAll(t, format, record(limit*4))
			require.Error(t, err)
			assert.ErrorIs(t, err, ErrRecordTooLong)
			assert.Contains(t, err.Error(), "line 2", "the message names the line the record starts on")
			assert.Contains(t, err.Error(), "1 KiB", "and the limit it passed")
		})
	}

	t.Run("a CSV quote that is never closed is bounded too", func(t *testing.T) {
		t.Parallel()

		// Without a bound this is the case that costs most: the reader keeps
		// asking for the rest of the field until the stream ends.
		err := readAll(t, FormatCSV, "v\n\""+strings.Repeat("x", limit*4))
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrRecordTooLong)
	})

	t.Run("many ordinary records are unaffected by the bound", func(t *testing.T) {
		t.Parallel()

		body := "v\n" + strings.Repeat("short\n", limit)
		assert.NoError(t, readAll(t, FormatCSV, body))
		assert.NoError(t, readAll(t, FormatTSV, body))
	})
}

// TestReadInFullFormatsAreBoundedToo pins the same bound for the formats that
// are read whole. It lives beside the delimited test because it is that rule,
// stated once in delimited.go: a file cannot be longer than itself, but a stream
// can, and a record with no terminator asks for everything the sender chooses to
// send.
//
// LTSV held to no bound at all, so a 200 MiB record with no terminator loaded
// and answered a table -- the one format that gave a caller no way to know. A
// JSON document that is not an array becomes one row holding the whole of it,
// which is one record and had no bound either.
func TestReadInFullFormatsAreBoundedToo(t *testing.T) {
	t.Parallel()

	const limit = 1 << 10

	readLTSVBounded := func(body string) error {
		_, err := readLTSV(strings.NewReader(body), Options{maxRecord: limit}, func(*Chunk) error { return nil })
		return err
	}

	t.Run("LTSV refuses a record past the limit", func(t *testing.T) {
		t.Parallel()

		err := readLTSVBounded("v:" + strings.Repeat("x", limit*4) + "\n")
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrRecordTooLong)
		assert.Contains(t, err.Error(), "line 1", "the message names the record")
		assert.Contains(t, err.Error(), "1 KiB", "and the limit it passed")
	})

	t.Run("LTSV accepts a record under the limit", func(t *testing.T) {
		t.Parallel()

		assert.NoError(t, readLTSVBounded("v:"+strings.Repeat("x", limit/2)+"\n"))
	})

	t.Run("a large LTSV file of short records is unaffected", func(t *testing.T) {
		t.Parallel()

		// The bound is on the record and not on the file, so a file many times
		// the limit still loads. Refusing it would refuse an ordinary log.
		assert.NoError(t, readLTSVBounded(strings.Repeat("v:short\n", limit)))
	})

	t.Run("a record with no terminator at all is bounded", func(t *testing.T) {
		t.Parallel()

		err := readLTSVBounded("v:" + strings.Repeat("x", limit*4))
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrRecordTooLong)
	})
}

// TestLineBoundedReaderCountsFromEachTerminator covers the wrapper directly: it
// answers how long the current line is rather than how much has been read, so a
// long file of short lines passes and one long line does not.
func TestLineBoundedReaderCountsFromEachTerminator(t *testing.T) {
	t.Parallel()

	const limit = 16

	read := func(body string) error {
		_, err := io.ReadAll(newLineBoundedReader(strings.NewReader(body), limit))
		return err
	}

	assert.NoError(t, read(strings.Repeat("short\n", 1000)), "many short lines are under the bound")
	assert.NoError(t, read(strings.Repeat("x", limit)+"\n"), "a line of exactly the bound passes")

	err := read(strings.Repeat("x", limit+1))
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrRecordTooLong)
	assert.Contains(t, err.Error(), "line 1")

	err = read("a\nb\n" + strings.Repeat("x", limit+1))
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrRecordTooLong)
	assert.Contains(t, err.Error(), "line 3", "the message names the record the bytes belong to")
}
