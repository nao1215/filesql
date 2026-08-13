package dialect

import (
	"database/sql/driver"
	"math"
	"testing"
	"time"
)

// TestCastToBlob covers the BLOB target, which every dialect spells differently
// but which all of them mean as "the value's bytes".
func TestCastToBlob(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		value driver.Value
		want  string
		null  bool
	}{
		{name: "bytes pass through", value: []byte("abc"), want: "abc"},
		{name: "a string becomes its bytes", value: "abc", want: "abc"},
		{name: "a number becomes its digits", value: int64(255), want: "255"},
		{name: "a time becomes its written form", value: time.Date(2026, 7, 28, 13, 5, 9, 0, time.UTC), want: "2026-07-28 13:05:09"},
		{name: "a NULL has no bytes", value: nil, null: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := castToBlob(tt.value)
			if err != nil {
				t.Fatalf("castToBlob(%v) error: %v", tt.value, err)
			}
			if tt.null {
				if got != nil {
					t.Fatalf("castToBlob(%v) = %v, want NULL", tt.value, got)
				}
				return
			}
			b, ok := got.([]byte)
			if !ok {
				t.Fatalf("castToBlob(%v) = %T, want []byte", tt.value, got)
			}
			if string(b) != tt.want {
				t.Fatalf("castToBlob(%v) = %q, want %q", tt.value, b, tt.want)
			}
		})
	}
}

// TestCastToBool covers the two answers a non-boolean value gets. MySQL takes
// anything and reads it for truthiness; the other dialects refuse a value that
// is not a boolean, because silently reading "maybe" as false is a wrong answer
// rather than a missing one.
func TestCastToBool(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		value   driver.Value
		strict  bool
		want    int64
		wantErr bool
	}{
		{name: "a true boolean", value: true, want: 1},
		{name: "a false boolean", value: false, want: 0},
		{name: "a non-zero integer", value: int64(7), want: 1},
		{name: "zero", value: int64(0), want: 0},
		{name: "a non-zero float", value: 0.5, want: 1},
		{name: "a zero float", value: 0.0, want: 0},
		{name: "the word yes", value: " YES ", want: 1},
		{name: "the word off", value: "off", want: 0},
		{name: "a word that is not a boolean is read for truthiness", value: "maybe", want: 1},
		{name: "an empty value is not truthy", value: "", want: 0},
		{name: "a word that is not a boolean, strictly", value: "maybe", strict: true, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := castToBool(PostgreSQL, tt.value, tt.strict)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("castToBool(%v, strict) = %v, want an error", tt.value, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("castToBool(%v) error: %v", tt.value, err)
			}
			if got != tt.want {
				t.Fatalf("castToBool(%v) = %v, want %v", tt.value, got, tt.want)
			}
		})
	}
}

// TestSafeIntArithmetic covers the overflow checks behind SAFE_ADD,
// SAFE_SUBTRACT, and SAFE_MULTIPLY. A result that does not fit answers NULL,
// which is the whole point of the SAFE_ family: wrapping around would produce a
// number of the wrong sign and report success.
func TestSafeIntArithmetic(t *testing.T) {
	t.Parallel()

	t.Run("addition", func(t *testing.T) {
		t.Parallel()

		if _, ok := safeAddInt(math.MaxInt64, 1); ok {
			t.Fatal("adding past the maximum must not report success")
		}
		if _, ok := safeAddInt(math.MinInt64, -1); ok {
			t.Fatal("adding past the minimum must not report success")
		}
		if got, ok := safeAddInt(2, 3); !ok || got != 5 {
			t.Fatalf("safeAddInt(2, 3) = %v, %v", got, ok)
		}
	})

	t.Run("subtraction", func(t *testing.T) {
		t.Parallel()

		// Subtracting the minimum is negating it, which has no int64 form.
		if _, ok := safeSubInt(1, math.MinInt64); ok {
			t.Fatal("subtracting the minimum from a positive must not report success")
		}
		if got, ok := safeSubInt(-1, math.MinInt64); !ok || got != math.MaxInt64 {
			t.Fatalf("safeSubInt(-1, MinInt64) = %v, %v", got, ok)
		}
		if _, ok := safeSubInt(math.MinInt64, 1); ok {
			t.Fatal("subtracting past the minimum must not report success")
		}
		if got, ok := safeSubInt(5, 3); !ok || got != 2 {
			t.Fatalf("safeSubInt(5, 3) = %v, %v", got, ok)
		}
	})

	t.Run("multiplication", func(t *testing.T) {
		t.Parallel()

		if _, ok := safeMulInt(math.MaxInt64, 2); ok {
			t.Fatal("multiplying past the maximum must not report success")
		}
		if got, ok := safeMulInt(6, 7); !ok || got != 42 {
			t.Fatalf("safeMulInt(6, 7) = %v, %v", got, ok)
		}
	})
}

// TestToInt covers the conversion every numeric UDF starts from. A value it
// cannot read as a number is not zero: reporting false is what lets the caller
// answer NULL instead of counting it as 0.
func TestToInt(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		value driver.Value
		want  int64
		ok    bool
	}{
		{name: "a NULL is not a number", value: nil},
		{name: "an integer", value: int64(7), want: 7, ok: true},
		{name: "a float truncates", value: 7.9, want: 7, ok: true},
		{name: "true", value: true, want: 1, ok: true},
		{name: "false", value: false, want: 0, ok: true},
		{name: "a numeric string", value: " 42 ", want: 42, ok: true},
		{name: "a string that is not a number", value: "abc"},
		{name: "numeric bytes", value: []byte("42"), want: 42, ok: true},
		{name: "bytes that are not a number", value: []byte("abc")},
		{name: "a value of another type", value: time.Time{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, ok := toInt(tt.value)
			if ok != tt.ok {
				t.Fatalf("toInt(%v) ok = %v, want %v", tt.value, ok, tt.ok)
			}
			if ok && got != tt.want {
				t.Fatalf("toInt(%v) = %v, want %v", tt.value, got, tt.want)
			}
		})
	}
}

// TestHexFunctions covers the two HEX spellings. MySQL's answers with the
// hexadecimal of a number's value, where SQLite's own HEX would answer with the
// hexadecimal of the digits' bytes — "323535" for 255 instead of "FF".
func TestHexFunctions(t *testing.T) {
	t.Parallel()

	t.Run("MySQL HEX", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			name  string
			value driver.Value
			want  driver.Value
		}{
			{name: "a NULL stays NULL", value: nil, want: nil},
			{name: "an integer", value: int64(255), want: "FF"},
			{name: "a float truncates", value: 255.9, want: "FF"},
			{name: "bytes", value: []byte("abc"), want: "616263"},
			{name: "a numeric string is hexed as its value", value: "255", want: "FF"},
			{name: "a string that is not a number is hexed as its bytes", value: "abc", want: "616263"},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()

				got, err := fnMySQLHex([]driver.Value{tt.value})
				if err != nil {
					t.Fatalf("fnMySQLHex(%v) error: %v", tt.value, err)
				}
				if got != tt.want {
					t.Fatalf("fnMySQLHex(%v) = %v, want %v", tt.value, got, tt.want)
				}
			})
		}
	})

	t.Run("GoogleSQL TO_HEX", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			name  string
			value driver.Value
			want  driver.Value
		}{
			{name: "a NULL stays NULL", value: nil, want: nil},
			{name: "bytes", value: []byte("abc"), want: "616263"},
			{name: "a string is taken as its bytes", value: "abc", want: "616263"},
			{name: "a number is taken as the bytes of its digits", value: int64(255), want: "323535"},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()

				got, err := fnToHex([]driver.Value{tt.value})
				if err != nil {
					t.Fatalf("fnToHex(%v) error: %v", tt.value, err)
				}
				if got != tt.want {
					t.Fatalf("fnToHex(%v) = %v, want %v", tt.value, got, tt.want)
				}
			})
		}
	})
}

// TestSimilarToRegexpEscapes covers the characters the SIMILAR TO translation
// has to protect from the regular expression it produces, and the escape a
// pattern can use to make % or _ mean itself.
func TestSimilarToRegexpEscapes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		pattern string
		want    string
	}{
		{name: "an anchor is escaped", pattern: "^a$", want: `^\^a\$$`},
		{name: "an escape keeps the character after it", pattern: `a\%b`, want: `^a\%b$`},
		{name: "a trailing escape is kept as is", pattern: `a\`, want: `^a\$`},
		{name: "the regex parts pass through", pattern: "(a|b)+[0-9]{2}", want: "^(a|b)+[0-9]{2}$"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if got := similarToRegexp(tt.pattern); got != tt.want {
				t.Fatalf("similarToRegexp(%q) = %q, want %q", tt.pattern, got, tt.want)
			}
		})
	}
}

// TestAddInterval covers the interval arithmetic the three dialects share,
// including the month clamping Go's AddDate does not do: "January 31 plus one
// month" is the last day of February, not March 3.
func TestAddInterval(t *testing.T) {
	t.Parallel()

	base := time.Date(2026, 1, 31, 10, 0, 0, 0, time.UTC)

	tests := []struct {
		name    string
		n       int64
		unit    string
		want    time.Time
		wantErr bool
	}{
		{name: "years", n: 1, unit: unitYear, want: time.Date(2027, 1, 31, 10, 0, 0, 0, time.UTC)},
		{name: "quarters", n: 1, unit: unitQuarter, want: time.Date(2026, 4, 30, 10, 0, 0, 0, time.UTC)},
		{name: "months clamp to the last day", n: 1, unit: unitMonth, want: time.Date(2026, 2, 28, 10, 0, 0, 0, time.UTC)},
		{name: "months backwards", n: -1, unit: unitMonth, want: time.Date(2025, 12, 31, 10, 0, 0, 0, time.UTC)},
		{name: "weeks", n: 1, unit: unitWeek, want: time.Date(2026, 2, 7, 10, 0, 0, 0, time.UTC)},
		{name: "days", n: 1, unit: unitDay, want: time.Date(2026, 2, 1, 10, 0, 0, 0, time.UTC)},
		{name: "hours", n: 2, unit: unitHour, want: time.Date(2026, 1, 31, 12, 0, 0, 0, time.UTC)},
		{name: "minutes", n: 30, unit: unitMinute, want: time.Date(2026, 1, 31, 10, 30, 0, 0, time.UTC)},
		{name: "seconds", n: 45, unit: unitSecond, want: time.Date(2026, 1, 31, 10, 0, 45, 0, time.UTC)},
		{name: "a unit no dialect defines", n: 1, unit: "fortnight", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := addInterval(base, tt.n, tt.unit)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("addInterval(%d, %q) = %v, want an error", tt.n, tt.unit, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("addInterval(%d, %q) error: %v", tt.n, tt.unit, err)
			}
			if !got.Equal(tt.want) {
				t.Fatalf("addInterval(%d, %q) = %v, want %v", tt.n, tt.unit, got, tt.want)
			}
		})
	}
}
