package infer

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIsInteger(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input string
		want  bool
	}{
		{"42", true},
		{"-42", true},
		{"+7", true},
		{"0", true},
		{"100000", true},
		{"9223372036854775807", true},
		{"9223372036854775808", false},
		{"3.14", false},
		{"1e10", false},
		{"007", false},
		{"abc", false},
		{"", false},
		{"  42  ", false},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, IsInteger(tt.input))
		})
	}
}

func TestIsFloat(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input string
		want  bool
	}{
		{"3.14", true},
		{"-3.14", true},
		{"0.0", true},
		{"1.0e10", true},
		{"1E-5", true},
		{"1e3", true},
		{"42", true},
		{"11040320260000000000", false},
		{"007", false},
		{"1_000", false},
		{"0x1p4", false},
		{"abc", false},
		{"", false},
		// A spelling whose parse saturates is still a float: SQLite's affinity
		// stores the same saturated value for the same text, so refusing it
		// turned a dumped REAL column holding an infinity into TEXT.
		{"9e999", true},
		{"-9e999", true},
		{"1e309", true},
		{"1e-400", true},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, IsFloat(tt.input))
		})
	}
}

func TestIsDatetime(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input string
		want  bool
	}{
		{"2024-01-15", true},
		{"2024/01/15", true},
		{"2024/01/15 10:30:00", true},
		{"2024-01-15 10:30:00", true},
		{"2024-01-15T10:30:00Z", true},
		{"2024-01-15T10:30:00.000", true},
		{"12/31/2024", true},
		{"1/2/2006 3:04:05 PM", true},
		{"2.1.2006", true},
		{"15:04:05", true},
		{"15:04", true},
		{"Jan 2, 2024", false},
		{"January 2, 2024", false},
		{"02 Jan 2006", false},
		{"01-02-2006", false},
		{"abc", false},
		{"42", false},
		{"", false},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, IsDatetime(tt.input))
		})
	}
}

func TestClassify(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input string
		want  Type
	}{
		{"42", Integer},
		{"0", Integer},
		{"2.5", Real},
		{"1e3", Real},
		{"2024-01-15", Datetime},
		{"007", Text},
		{"11040320260000000000", Text},
		{"1_000", Text},
		{"hello", Text},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, Classify(tt.input))
		})
	}
}

func TestColumn_TypeHoldsEveryValue(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		values []string
		want   Type
	}{
		{"nothing seen", nil, Text},
		{"only blanks", []string{"", "  "}, Text},
		{"integers", []string{"1", "2", ""}, Integer},
		{"one decimal makes it real", []string{"1", "2.5"}, Real},
		{"one word makes it text", []string{"1", "2", "x"}, Text},
		{"a zero-padded code anywhere", []string{"1", "2", "007"}, Text},
		{"an int64 overflow anywhere", []string{"1", "11040320260000000000"}, Text},
		{"padded whitespace", []string{"1", " 2"}, Text},
		{"datetimes", []string{"2024-01-01", "2024-01-02"}, Datetime},
		{"datetime beside a number", []string{"2024-01-01", "5"}, Text},
		// An integer past 2^53 is exact in an INTEGER column and damaged in a
		// REAL one, so a float beside it forces the column to TEXT while the
		// integer alone keeps its numeric type.
		{"a float beside an integer past 2^53", []string{"9007199254740993", "0.5"}, Text},
		{"a float before an integer past 2^53", []string{"0.5", "9007199254740993"}, Text},
		{"a float beside a negative integer past 2^53", []string{"-9007199254740993", "1.5"}, Text},
		{"an integer past 2^53 alone", []string{"9007199254740993"}, Integer},
		{"a float beside 2^53 itself", []string{"9007199254740992", "0.5"}, Real},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, Column(tt.values))
		})
	}
}

func TestEvidence_OrderDoesNotMatter(t *testing.T) {
	t.Parallel()

	values := []string{"1", "2.5", "", "2024-01-01", "007", "abc"}
	want := Column(values)
	for i := range values {
		rotated := append(append([]string{}, values[i:]...), values[:i]...)
		assert.Equal(t, want, Column(rotated), "rotated by %d", i)
	}
}

func TestTypeString(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "TEXT", Text.String())
	assert.Equal(t, "INTEGER", Integer.String())
	assert.Equal(t, "REAL", Real.String())
	assert.Equal(t, "DATETIME", Datetime.String())
	assert.Equal(t, "TEXT", Type(99).String())
}
