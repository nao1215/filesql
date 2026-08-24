package frame

import (
	"math"
	"strings"
	"testing"
)

// TestFilterStringPredicateMatchesDigits pins the case the typed accessors
// exist for: a CSV column of digits arrives as int64, so the obvious
// row["id"] == "1" matched nothing and said nothing. Row.String answers with
// the value as the file spelled it.
func TestFilterStringPredicateMatchesDigits(t *testing.T) {
	t.Parallel()

	df, err := NewDataFrame(strings.NewReader("id\n1\n2\n"), CSV)
	if err != nil {
		t.Fatalf("NewDataFrame: %v", err)
	}

	got := df.Filter(func(row map[string]any) bool {
		v, ok := Row(row).String("id")
		return ok && v == "1"
	}).ToRecords()

	if len(got) != 1 {
		t.Fatalf("Filter matched %d rows, want 1: %v", len(got), got)
	}
}

// TestFilterKeepsZeroPaddedCodeComparable is the other half: a column that must
// stay text compares as the text it is, through the same accessor.
func TestFilterKeepsZeroPaddedCodeComparable(t *testing.T) {
	t.Parallel()

	df, err := NewDataFrame(strings.NewReader("code\n007\n7\n"), CSV)
	if err != nil {
		t.Fatalf("NewDataFrame: %v", err)
	}

	got := df.Filter(func(row map[string]any) bool {
		v, ok := Row(row).String("code")
		return ok && v == "007"
	}).ToRecords()

	if len(got) != 1 {
		t.Fatalf("Filter matched %d rows, want 1: %v", len(got), got)
	}
}

func TestRowString(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		row    Row
		column string
		want   string
		wantOK bool
	}{
		{"text stays as it is", Row{"c": "007"}, "c", "007", true},
		{"an integer renders without a decimal point", Row{"c": int64(1)}, "c", "1", true},
		{"a negative integer keeps its sign", Row{"c": int64(-42)}, "c", "-42", true},
		{"a real renders in its shortest exact form", Row{"c": 1.5}, "c", "1.5", true},
		{"a real that is whole renders without a decimal point", Row{"c": 2.0}, "c", "2", true},
		{"an empty string is a value", Row{"c": ""}, "c", "", true},
		{"a missing column is not a value", Row{"c": "x"}, "other", "", false},
		{"a nil is not a value", Row{"c": nil}, "c", "", false},
		{"a nil row has no value", nil, "c", "", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, ok := tt.row.String(tt.column)
			if got != tt.want || ok != tt.wantOK {
				t.Errorf("String(%q) = (%q, %v), want (%q, %v)", tt.column, got, ok, tt.want, tt.wantOK)
			}
		})
	}
}

func TestRowInt(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		row    Row
		column string
		want   int64
		wantOK bool
	}{
		{"an integer is itself", Row{"c": int64(7)}, "c", 7, true},
		{"digits held as text convert", Row{"c": "7"}, "c", 7, true},
		{"a zero-padded code converts to its number", Row{"c": "007"}, "c", 7, true},
		{"a whole real converts", Row{"c": 7.0}, "c", 7, true},
		{"a real with a fraction does not convert", Row{"c": 7.5}, "c", 0, false},
		{"a real too large for int64 does not convert", Row{"c": math.MaxFloat64}, "c", 0, false},
		{"a NaN does not convert", Row{"c": math.NaN()}, "c", 0, false},
		{"text that is not a number does not convert", Row{"c": "seven"}, "c", 0, false},
		{"an empty string is not a number", Row{"c": ""}, "c", 0, false},
		{"a missing column is not a number", Row{"c": int64(1)}, "other", 0, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, ok := tt.row.Int(tt.column)
			if got != tt.want || ok != tt.wantOK {
				t.Errorf("Int(%q) = (%d, %v), want (%d, %v)", tt.column, got, ok, tt.want, tt.wantOK)
			}
		})
	}
}

func TestRowFloat(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		row    Row
		column string
		want   float64
		wantOK bool
	}{
		{"a real is itself", Row{"c": 1.5}, "c", 1.5, true},
		{"an integer widens", Row{"c": int64(7)}, "c", 7, true},
		{"a decimal held as text converts", Row{"c": "1.50"}, "c", 1.5, true},
		{"text that is not a number does not convert", Row{"c": "one"}, "c", 0, false},
		{"a missing column is not a number", Row{"c": 1.5}, "other", 0, false},
		{"a nil is not a number", Row{"c": nil}, "c", 0, false},
		// The vocabulary is the one a data file spells numbers in, not Go's:
		// an underscore separator and a hex float are Go source syntax, and the
		// infinity and NaN words hold no digit, so none of them is a quantity
		// here any more than it is to Row.Int or to SQLite's affinity.
		{"a Go underscore separator does not convert", Row{"c": "1_000"}, "c", 0, false},
		{"a Go hex float does not convert", Row{"c": "0x1p4"}, "c", 0, false},
		{"the word Inf does not convert", Row{"c": "Inf"}, "c", 0, false},
		{"the word Infinity does not convert", Row{"c": "Infinity"}, "c", 0, false},
		{"the word -Inf does not convert", Row{"c": "-Inf"}, "c", 0, false},
		{"the word NaN does not convert", Row{"c": "NaN"}, "c", 0, false},
		{"the word nan does not convert", Row{"c": "nan"}, "c", 0, false},
		// Deliberate acceptances stay: the zero-padded code, the exponent
		// spelling, and the overflow spelling SQLite's affinity saturates.
		{"a zero-padded code converts", Row{"c": "007"}, "c", 7, true},
		{"an exponent spelling converts", Row{"c": "1e3"}, "c", 1000, true},
		{"an overflowing spelling saturates to the infinity", Row{"c": "9e999"}, "c", math.Inf(1), true},
		{"a negative overflowing spelling saturates", Row{"c": "-9e999"}, "c", math.Inf(-1), true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, ok := tt.row.Float(tt.column)
			if got != tt.want || ok != tt.wantOK {
				t.Errorf("Float(%q) = (%v, %v), want (%v, %v)", tt.column, got, ok, tt.want, tt.wantOK)
			}
		})
	}
}

// TestRowIntAndFloatAgreeOnIntegerShapedText pins the pair to one vocabulary:
// for integer-shaped text within int64's range, the two accessors answer ok
// together or not at all. They part only where their result types part — on
// real spellings, which Int refuses, and past int64, which only Float holds.
func TestRowIntAndFloatAgreeOnIntegerShapedText(t *testing.T) {
	t.Parallel()

	for _, text := range []string{"7", "+7", "-7", "007", "0", "1_000", "0x10", "9223372036854775807", " 42", "42 "} {
		row := Row{"c": text}
		_, iok := row.Int("c")
		_, fok := row.Float("c")
		if iok != fok {
			t.Errorf("%q: Int ok = %v, Float ok = %v; integer-shaped text must convert through both or neither", text, iok, fok)
		}
	}
}

// TestRowStringRoundTripsTheSourceText checks the property the accessor is for:
// whatever a CSV cell held, String gives it back as the file spelled it, so a
// predicate can be written against the file rather than against an inference.
func TestRowStringRoundTripsTheSourceText(t *testing.T) {
	t.Parallel()

	// An empty line is not an empty cell to a CSV reader, so the empty value is
	// covered by the table test above rather than here.
	cells := []string{"1", "2", "007", "1.5", "-3", "hello", "9223372036854775808"}
	df, err := NewDataFrame(strings.NewReader("v\n"+strings.Join(cells, "\n")+"\n"), CSV)
	if err != nil {
		t.Fatalf("NewDataFrame: %v", err)
	}

	rows := df.ToRecords()
	if len(rows) != len(cells) {
		t.Fatalf("got %d rows, want %d", len(rows), len(cells))
	}
	for i, want := range cells {
		got, ok := Row(rows[i]).String("v")
		if !ok || got != want {
			t.Errorf("row %d: String(%q) = (%q, %v), want (%q, true)", i, "v", got, ok, want)
		}
	}
}
