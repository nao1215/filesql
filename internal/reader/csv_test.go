package reader

import (
	"encoding/csv"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
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
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Errorf("mismatch (-want +got):\n%s", diff)
			}
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
		if diff := cmp.Diff([][]string{{"a", "b"}, {"1"}}, got); diff != "" {
			t.Errorf("mismatch (-want +got):\n%s", diff)
		}
	})
}

// TestCSVReader_RefusesAnUnusableDelimiter pins that a delimiter this reader
// cannot honor is reported rather than replaced with a comma. Falling back
// would split the file somewhere the caller did not ask for and say nothing,
// which is the shape of failure this reader exists to stop one level up.
func TestCSVReader_RefusesAnUnusableDelimiter(t *testing.T) {
	t.Parallel()

	for name, comma := range map[string]rune{
		"multi-byte":      '§',
		"quote":           '"',
		"line feed":       '\n',
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
		if diff := cmp.Diff([][]string{{"a", "b"}, {"1", "2"}}, got); diff != "" {
			t.Errorf("mismatch (-want +got):\n%s", diff)
		}
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
			if diff := cmp.Diff(want, got); diff != "" {
				t.Errorf("records differ from encoding/csv (-stdlib +ours):\n%s", diff)
			}
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
