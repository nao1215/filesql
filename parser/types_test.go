package parser

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_isInteger(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		input    string
		expected bool
	}{
		{"42", true},
		{"-42", true},
		{"0", true},
		{"100000", true},
		{"3.14", false},
		{"abc", false},
		{"", false},
		{"  42  ", true},
		{"1e10", false},
	}

	for _, tc := range testCases {
		t.Run(tc.input, func(t *testing.T) {
			t.Parallel()

			result := isInteger(tc.input)

			assert.Equal(t, tc.expected, result)
		})
	}
}

func Test_isFloat(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		input    string
		expected bool
	}{
		{"3.14", true},
		{"-3.14", true},
		{"0.0", true},
		{"1.0e10", true},
		{"1E-5", true},
		{"42", false}, // Integer without decimal
		{"abc", false},
		{"", false},
		{"  3.14  ", true},
	}

	for _, tc := range testCases {
		t.Run(tc.input, func(t *testing.T) {
			t.Parallel()

			result := isFloat(tc.input)

			assert.Equal(t, tc.expected, result)
		})
	}
}

func Test_isDatetime(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		input    string
		expected bool
	}{
		{"2024-01-15", true},
		{"2024/01/15", true},
		{"2024-01-15 10:30:00", true},
		{"2024-01-15T10:30:00Z", true},
		{"Jan 2, 2024", true},
		{"January 2, 2024", true},
		{"abc", false},
		{"42", false},
		{"", false},
	}

	for _, tc := range testCases {
		t.Run(tc.input, func(t *testing.T) {
			t.Parallel()

			result := isDatetime(tc.input)

			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestInferColumnTypes(t *testing.T) {
	t.Parallel()

	t.Run("infers integer type", func(t *testing.T) {
		t.Parallel()

		headers := []string{"count"}
		records := [][]string{{"1"}, {"2"}, {"3"}, {"4"}, {"5"}}

		types := inferColumnTypes(headers, records)

		assert.Equal(t, TypeInteger, types[0])
	})

	t.Run("infers real type", func(t *testing.T) {
		t.Parallel()

		headers := []string{"price"}
		records := [][]string{{"1.99"}, {"2.50"}, {"3.14"}, {"4.0"}, {"5.5"}}

		types := inferColumnTypes(headers, records)

		assert.Equal(t, TypeReal, types[0])
	})

	t.Run("infers text type for mixed data", func(t *testing.T) {
		t.Parallel()

		headers := []string{"mixed"}
		records := [][]string{{"hello"}, {"42"}, {"world"}, {"100"}, {"test"}}

		types := inferColumnTypes(headers, records)

		assert.Equal(t, TypeText, types[0])
	})

	t.Run("returns text for empty records", func(t *testing.T) {
		t.Parallel()

		headers := []string{"col"}
		records := [][]string{}

		types := inferColumnTypes(headers, records)

		assert.Equal(t, TypeText, types[0])
	})
}

func TestParseValue(t *testing.T) {
	t.Parallel()

	t.Run("parses integer", func(t *testing.T) {
		t.Parallel()

		result := ParseValue("42", TypeInteger)

		assert.Equal(t, int64(42), result)
	})

	t.Run("parses float", func(t *testing.T) {
		t.Parallel()

		result := ParseValue("3.14", TypeReal)

		assert.Equal(t, 3.14, result)
	})

	t.Run("returns string for text type", func(t *testing.T) {
		t.Parallel()

		result := ParseValue("hello", TypeText)

		assert.Equal(t, "hello", result)
	})

	t.Run("returns nil for empty value", func(t *testing.T) {
		t.Parallel()

		result := ParseValue("", TypeInteger)

		assert.Nil(t, result)
	})

	t.Run("returns original string for invalid integer", func(t *testing.T) {
		t.Parallel()

		result := ParseValue("not-a-number", TypeInteger)

		assert.Equal(t, "not-a-number", result)
	})
}
