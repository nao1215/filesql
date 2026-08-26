package filesql

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/nao1215/filesql/frame"
	"github.com/nao1215/filesql/internal/infer"
	"github.com/nao1215/filesql/parser"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewTableName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{"normal name", "users", "users"},
		{"with spaces trimmed", "  users  ", "users"},
		{"empty string", "", "table"},
		{"whitespace only", "   ", "table"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tn := newTableName(tt.input)
			assert.Equal(t, tt.expected, tn.String())
		})
	}
}

func TestTableName_String(t *testing.T) {
	t.Parallel()

	tn := newTableName("test_table")
	assert.Equal(t, "test_table", tn.String())
}

func TestTableName_Equal(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		tn1      tableName
		tn2      tableName
		expected bool
	}{
		{
			name:     "equal table names",
			tn1:      newTableName("users"),
			tn2:      newTableName("users"),
			expected: true,
		},
		{
			name:     "different table names",
			tn1:      newTableName("users"),
			tn2:      newTableName("orders"),
			expected: false,
		},
		{
			name:     "empty both",
			tn1:      newTableName(""),
			tn2:      newTableName(""),
			expected: true,
		},
		{
			name:     "case sensitive",
			tn1:      newTableName("Users"),
			tn2:      newTableName("users"),
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := tt.tn1.Equal(tt.tn2)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestTableName_Sanitize(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{"normal name", "users", "users"},
		{"with hyphen", "user-data", "user_data"},
		{"with spaces", "user data", "user_data"},
		{"with dots", "data.backup", "data_backup"},
		{"with special chars", "user@#$data", "userdata"}, // special chars are removed, not replaced
		{"starts with number", "123table", "table_123table"},
		{"only special chars", "@#$%", "table"},
		{"empty after sanitize", "", "table"},
		{"mixed invalid chars", "test-data.v2@new", "test_data_v2new"}, // only -, space, . are replaced with _
		{"uppercase preserved", "UserData", "UserData"},
		// The character set matches the file-path sanitizer, so a name in any
		// script survives here too; only the fallback and prefix differ.
		{"japanese preserved", "売上", "売上"},
		{"cyrillic preserved", "Данные", "Данные"},
		{"accented latin preserved", "café", "café"},
		{"non-ascii digit leads", "١٢", "table_١٢"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tn := newTableName(tt.input)
			sanitized := tn.sanitize()
			assert.Equal(t, tt.expected, sanitized.String())
		})
	}
}

func TestNewHeader(t *testing.T) {
	t.Parallel()

	t.Run("Create header from slice", func(t *testing.T) {
		t.Parallel()

		headerSlice := []string{"col1", "col2", "col3"}
		header := newHeader(headerSlice)

		assert.Len(t, header, 3, "Header length mismatch")

		for i, expected := range headerSlice {
			assert.Equal(t, expected, header[i], "Header element mismatch at index %d", i)
		}
	})
}

func TestHeader_Equal(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		header1  header
		header2  header
		expected bool
	}{
		{
			name:     "Equal headers",
			header1:  newHeader([]string{"col1", "col2"}),
			header2:  newHeader([]string{"col1", "col2"}),
			expected: true,
		},
		{
			name:     "Different length headers",
			header1:  newHeader([]string{"col1", "col2"}),
			header2:  newHeader([]string{"col1"}),
			expected: false,
		},
		{
			name:     "Different content headers",
			header1:  newHeader([]string{"col1", "col2"}),
			header2:  newHeader([]string{"col1", "col3"}),
			expected: false,
		},
		{
			name:     "Empty headers",
			header1:  newHeader([]string{}),
			header2:  newHeader([]string{}),
			expected: true,
		},
		{
			name:     "One empty one not",
			header1:  newHeader([]string{}),
			header2:  newHeader([]string{"col1"}),
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := tt.header1.equal(tt.header2)
			assert.Equal(t, tt.expected, result, "Header equality check failed")
		})
	}
}

func TestNewRecord(t *testing.T) {
	t.Parallel()

	t.Run("Create record from slice", func(t *testing.T) {
		t.Parallel()

		recordSlice := []string{"val1", "val2", "val3"}
		record := newRecord(recordSlice)

		assert.Len(t, record, 3, "Record length mismatch")

		for i, expected := range recordSlice {
			assert.Equal(t, expected, record[i], "Record element mismatch at index %d", i)
		}
	})
}

func TestRecord_Equal(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		record1  record
		record2  record
		expected bool
	}{
		{
			name:     "Equal records",
			record1:  newRecord([]string{"val1", "val2"}),
			record2:  newRecord([]string{"val1", "val2"}),
			expected: true,
		},
		{
			name:     "Different length records",
			record1:  newRecord([]string{"val1", "val2"}),
			record2:  newRecord([]string{"val1"}),
			expected: false,
		},
		{
			name:     "Different content records",
			record1:  newRecord([]string{"val1", "val2"}),
			record2:  newRecord([]string{"val1", "val3"}),
			expected: false,
		},
		{
			name:     "Empty records",
			record1:  newRecord([]string{}),
			record2:  newRecord([]string{}),
			expected: true,
		},
		{
			name:     "One empty one not",
			record1:  newRecord([]string{}),
			record2:  newRecord([]string{"val1"}),
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := tt.record1.equal(tt.record2)
			assert.Equal(t, tt.expected, result, "Record equality check failed")
		})
	}
}

func TestColumnType_String(t *testing.T) {
	t.Parallel()

	tests := []struct {
		columnType columnType
		expected   string
	}{
		{columnTypeText, "TEXT"},
		{columnTypeInteger, "INTEGER"},
		{columnTypeReal, "REAL"},
		{columnTypeDatetime, "TEXT"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			result := tt.columnType.string()
			assert.Equal(t, tt.expected, result, "columnType.string() returned unexpected value")
		})
	}
}

func TestInferColumnType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		values   []string
		expected columnType
	}{
		{
			name:     "all integers",
			values:   []string{"123", "456", "789"},
			expected: columnTypeInteger,
		},
		{
			name:     "mixed integers and floats",
			values:   []string{"123", "45.6", "789"},
			expected: columnTypeReal,
		},
		{
			name:     "all floats",
			values:   []string{"12.3", "45.6", "78.9"},
			expected: columnTypeReal,
		},
		{
			name:     "mixed numbers and text",
			values:   []string{"123", "hello", "789"},
			expected: columnTypeText,
		},
		{
			name:     "all text",
			values:   []string{"hello", "world", "test"},
			expected: columnTypeText,
		},
		{
			name:     "empty values",
			values:   []string{"", "", ""},
			expected: columnTypeText,
		},
		{
			name:     "integers with empty values",
			values:   []string{"123", "", "789"},
			expected: columnTypeInteger,
		},
		{
			name:     "negative integers",
			values:   []string{"-123", "456", "-789"},
			expected: columnTypeInteger,
		},
		{
			name:     "negative floats",
			values:   []string{"-12.3", "45.6", "-78.9"},
			expected: columnTypeReal,
		},
		{
			name:     "scientific notation",
			values:   []string{"1e10", "2.5e-3", "3.14e2"},
			expected: columnTypeReal,
		},
		{
			name:     "zero values",
			values:   []string{"0", "0.0", "0"},
			expected: columnTypeReal,
		},
		{
			name:     "ISO8601 dates",
			values:   []string{"2023-01-15", "2023-02-20", "2023-03-10"},
			expected: columnTypeDatetime,
		},
		{
			name:     "ISO8601 datetime",
			values:   []string{"2023-01-15T10:30:00", "2023-02-20T14:45:30", "2023-03-10T09:15:45"},
			expected: columnTypeDatetime,
		},
		{
			name:     "US date format",
			values:   []string{"1/15/2023", "2/20/2023", "3/10/2023"},
			expected: columnTypeDatetime,
		},
		{
			name:     "European date format",
			values:   []string{"15.1.2023", "20.2.2023", "10.3.2023"},
			expected: columnTypeDatetime,
		},
		{
			name:     "time only",
			values:   []string{"10:30:00", "14:45:30", "09:15:45"},
			expected: columnTypeDatetime,
		},
		{
			name:     "mixed datetime and text",
			values:   []string{"2023-01-15", "not a date", "2023-03-10"},
			expected: columnTypeText,
		},
		{
			name:     "datetime with timezone",
			values:   []string{"2023-01-15T10:30:00Z", "2023-02-20T14:45:30+09:00"},
			expected: columnTypeDatetime,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := columnTypeOf(infer.Column(tt.values))
			assert.Equal(t, tt.expected, result, "inferColumnType failed for values: %v", tt.values)
		})
	}
}

func TestNewColumnInfoList(t *testing.T) {
	t.Parallel()

	t.Run("mixed column types", func(t *testing.T) {
		header := newHeader([]string{"id", "name", "age", "salary", "hire_date"})
		records := []record{
			newRecord([]string{"1", "Alice", "30", "95000", "2023-01-15"}),
			newRecord([]string{"2", "Bob", "25", "78000", "2023-02-20"}),
			newRecord([]string{"3", "Charlie", "35", "102000", "2023-03-10"}),
		}

		result := newColumnInfoList(header, records)

		expected := columnInfoList{
			{Name: "id", Type: columnTypeInteger},
			{Name: "name", Type: columnTypeText},
			{Name: "age", Type: columnTypeInteger},
			{Name: "salary", Type: columnTypeInteger},
			{Name: "hire_date", Type: columnTypeDatetime},
		}

		require.Len(t, result, len(expected), "Column count mismatch")

		for i, exp := range expected {
			assert.Equal(t, exp.Name, result[i].Name, "Column %d name mismatch", i)
			assert.Equal(t, exp.Type, result[i].Type, "Column %d type mismatch", i)
		}
	})

	t.Run("empty records", func(t *testing.T) {
		header := newHeader([]string{"col1", "col2"})
		records := []record{}

		result := newColumnInfoList(header, records)

		require.Len(t, result, 2, "Expected 2 columns for empty records")

		for i, col := range result {
			assert.Equal(t, columnTypeText, col.Type, "Column %d should be TEXT type for empty records", i)
		}
	})

	t.Run("datetime column inference", func(t *testing.T) {
		header := newHeader([]string{"event_date", "event_time", "timestamp"})
		records := []record{
			newRecord([]string{"2023-01-15", "10:30:00", "2023-01-15T10:30:00Z"}),
			newRecord([]string{"2023-02-20", "14:45:30", "2023-02-20T14:45:30Z"}),
			newRecord([]string{"2023-03-10", "09:15:45", "2023-03-10T09:15:45Z"}),
		}

		result := newColumnInfoList(header, records)

		expected := columnInfoList{
			{Name: "event_date", Type: columnTypeDatetime},
			{Name: "event_time", Type: columnTypeDatetime},
			{Name: "timestamp", Type: columnTypeDatetime},
		}

		require.Len(t, result, len(expected), "Datetime column count mismatch")

		for i, exp := range expected {
			assert.Equal(t, exp.Name, result[i].Name, "Column %d name mismatch", i)
			assert.Equal(t, exp.Type, result[i].Type, "Column %d type mismatch", i)
		}
	})
}

func TestIsDatetime(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		value    string
		expected bool
	}{
		// ISO8601 formats
		{"ISO date", "2023-01-15", true},
		{"ISO datetime", "2023-01-15T10:30:00", true},
		{"ISO datetime with timezone Z", "2023-01-15T10:30:00Z", true},
		{"ISO datetime with timezone offset", "2023-01-15T10:30:00+09:00", true},
		{"ISO datetime with milliseconds", "2023-01-15T10:30:00.123", true},

		// US formats
		{"US date", "1/15/2023", true},
		{"US date padded", "01/15/2023", true},
		{"US datetime", "1/15/2023 10:30:00", true},

		// European formats
		{"European date", "15.1.2023", true},
		{"European datetime", "15.1.2023 10:30:00", true},

		// Time only
		{"Time HH:MM:SS", "10:30:00", true},
		{"Time HH:MM", "10:30", true},
		{"Time with milliseconds", "10:30:00.123", true},

		// Invalid cases - optimized early termination
		{"Plain text", "hello world", false},
		{"Number", "123", false},
		{"Invalid date", "2023-13-45", false},
		{"Invalid time", "25:70:90", false},
		{"Empty string", "", false},
		{"Partial date", "2023-01", false},
		{"Wrong format", "Jan 15, 2023", false},
		{"Too short", "ab", false},
		{"Too long", "this is a very long string that is definitely not a datetime format and should be rejected quickly", false},
		{"No digits", "abcdef", false},
		{"No separators", "123456", false},
		{"Whitespace only", "   ", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := infer.IsDatetime(tt.value)
			assert.Equal(t, tt.expected, result, "isDatetime failed for value: %q", tt.value)
		})
	}
}

// TestClassifyValue tests individual value classification
func TestClassifyValue(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		value    string
		expected columnType
	}{
		// Integer values
		{"positive integer", "123", columnTypeInteger},
		{"negative integer", "-456", columnTypeInteger},
		{"zero", "0", columnTypeInteger},
		{"leading plus sign", "+789", columnTypeInteger},

		// Real values
		{"positive float", "12.34", columnTypeReal},
		{"negative float", "-56.78", columnTypeReal},
		{"scientific notation", "1.23e10", columnTypeReal},
		{"zero float", "0.0", columnTypeReal},

		// Datetime values
		{"ISO date", "2023-01-15", columnTypeDatetime},
		{"ISO datetime", "2023-01-15T10:30:00", columnTypeDatetime},
		{"time only", "10:30:00", columnTypeDatetime},

		// Text values
		{"plain text", "hello", columnTypeText},
		{"mixed alphanumeric", "abc123", columnTypeText},
		{"special characters", "hello@world.com", columnTypeText},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := columnTypeOf(infer.Classify(tt.value))
			assert.Equal(t, tt.expected, result, "classifyValue failed for value: %q", tt.value)
		})
	}
}

// TestIsInteger tests optimized integer detection
func TestIsInteger(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		value    string
		expected bool
	}{
		// Valid integers
		{"positive integer", "123", true},
		{"negative integer", "-456", true},
		{"zero", "0", true},
		{"leading plus", "+789", true},
		{"large integer", "9223372036854775807", true},

		// Invalid integers
		{"float", "12.34", false},
		{"text", "hello", false},
		{"empty", "", false},
		{"scientific notation", "1e10", false},
		{"leading letter", "a123", false},
		{"trailing text", "123abc", false},
		{"multiple signs", "+-123", false},
		{"hex notation", "0x123", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := infer.IsInteger(tt.value)
			assert.Equal(t, tt.expected, result, "isInteger failed for value: %q", tt.value)
		})
	}
}

// TestIsFloat tests optimized float detection
func TestIsFloat(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		value    string
		expected bool
	}{
		// Valid floats
		{"simple float", "12.34", true},
		{"negative float", "-56.78", true},
		{"integer as float", "123", true},
		{"scientific notation", "1.23e10", true},
		{"negative scientific", "-1.23e-5", true},
		{"zero", "0", true},
		{"zero float", "0.0", true},

		// Invalid floats
		{"text", "hello", false},
		{"empty", "", false},
		{"no digits", "abc", false},
		{"multiple dots", "12.34.56", false},
		{"invalid scientific", "1e", false},

		// Go's parser accepts these spellings and SQLite's numeric affinity does
		// not convert them. Calling them numeric declared a REAL column that
		// stored every value as text, so the schema and typeof() disagreed.
		{"underscore separators", "1_000", false},
		{"underscore in a decimal", "1_000.5", false},
		{"short underscore form", "1_0", false},
		{"hexadecimal float", "0x1p4", false},
		{"hexadecimal integer", "0x10", false},
		{"binary literal", "0b101", false},
		{"octal literal", "0o17", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := infer.IsFloat(tt.value)
			assert.Equal(t, tt.expected, result, "isFloat failed for value: %q", tt.value)
		})
	}
}

// TestInferColumnType_PicksTheTypeThatHoldsEveryValue tests the rule that turns
// the values of a column into its declared type. Every kind of value present
// counts; how many of each there are does not, because a type chosen against a
// minority is a type SQLite then has to store those values around.
func TestInferColumnType_PicksTheTypeThatHoldsEveryValue(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		values   []string
		expected columnType
	}{
		{
			name:     "integers only",
			values:   []string{"1", "2", "3", "4", "5", "6", "7", "8"},
			expected: columnTypeInteger,
		},
		{
			name:     "text among integers",
			values:   []string{"1", "2", "3", "4", "5", "a", "b", "c"},
			expected: columnTypeText,
		},
		{
			// A datetime is stored as text, so a column holding one alongside a
			// number has no type that covers both. Answering INTEGER declared a
			// schema the storage did not match.
			name:     "datetime beside an integer",
			values:   []string{"5", "2026-08-20T10:00:00Z"},
			expected: columnTypeText,
		},
		{
			name:     "datetime beside a real",
			values:   []string{"1.5", "2.5", "3.5", "2026-08-20T10:00:00Z"},
			expected: columnTypeText,
		},
		{
			name:     "datetimes only",
			values:   []string{"2026-08-20", "2026-08-21", "2026-08-22"},
			expected: columnTypeDatetime,
		},
		{
			name:     "integers and reals mixed",
			values:   []string{"1", "2", "3", "1.5", "2.5", "3.5", "4.5"},
			expected: columnTypeReal,
		},
		{
			// One decimal is enough. Weighing them against the integers left an
			// INTEGER column that rewrote 4.0 to 4, and made 5 / 2 answer 2.
			name:     "a single real among many integers",
			values:   []string{"1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12.5"},
			expected: columnTypeReal,
		},
		{
			// REAL used to win this on confidence, and the two datetimes in it
			// were then stored as text under a REAL declaration.
			name:     "numerics with a datetime among them",
			values:   []string{"1", "2", "3", "1.5", "2.5", "3.5", "4.5", "2026-08-20", "2026-08-21"},
			expected: columnTypeText,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := columnTypeOf(infer.Column(tt.values))
			assert.Equal(t, tt.expected, result, "inferColumnType failed")
		})
	}
}

// TestInferColumnTypeOverLargeColumns covers columns big enough that a sampler
// would have looked at part of them, where a text value's share of the column
// used to decide whether it was noticed at all.
func TestInferColumnTypeOverLargeColumns(t *testing.T) {
	// Skip slow tests unless running in GitHub Actions
	if os.Getenv("GITHUB_ACTIONS") != "true" {
		t.Skip("Skipping slow test in local environment. Set GITHUB_ACTIONS=true to run.")
	}

	t.Parallel()

	t.Run("a third of a large column is text", func(t *testing.T) {
		t.Parallel()

		values := make([]string, 10000)
		for i := range 10000 {
			switch i % 3 {
			case 0:
				values[i] = "text_value"
			case 1:
				values[i] = strconv.Itoa(i)
			case 2:
				values[i] = fmt.Sprintf("%.2f", float64(i)/100)
			}
		}

		assert.Equal(t, columnTypeText, columnTypeOf(infer.Column(values)))
	})

	t.Run("text values scattered through a large column", func(t *testing.T) {
		t.Parallel()

		for _, texts := range []int{600, 400, 1} {
			values := make([]string, 1000)
			for i := range 1000 {
				if i < texts {
					values[i] = "text_value"
				} else {
					values[i] = strconv.Itoa(i)
				}
			}

			assert.Equal(t, columnTypeText, columnTypeOf(infer.Column(values)),
				"%d text values among 1000 integers", texts)
		}
	})

	t.Run("a text value at the very end of a large column", func(t *testing.T) {
		t.Parallel()

		values := make([]string, 10000)
		for i := range 10000 {
			values[i] = strconv.Itoa(i)
		}
		values[len(values)-1] = "text_value"

		assert.Equal(t, columnTypeText, columnTypeOf(infer.Column(values)))
	})
}

// Benchmark tests to validate performance improvements
func BenchmarkInferColumnType(b *testing.B) {
	// Skip benchmarks unless running in GitHub Actions
	if os.Getenv("GITHUB_ACTIONS") != "true" {
		b.Skip("Skipping benchmark in local environment. Set GITHUB_ACTIONS=true to run.")
	}

	// Create test datasets of different sizes and types
	sizes := []int{100, 1000, 10000, 100000}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("integers_%d", size), func(b *testing.B) {
			values := make([]string, size)
			for i := range size {
				values[i] = strconv.Itoa(i)
			}

			b.ResetTimer()
			for range b.N {
				_ = columnTypeOf(infer.Column(values))
			}
		})

		b.Run(fmt.Sprintf("mixed_types_%d", size), func(b *testing.B) {
			values := make([]string, size)
			for i := range size {
				switch i % 4 {
				case 0:
					values[i] = strconv.Itoa(i)
				case 1:
					values[i] = fmt.Sprintf("%.2f", float64(i)/100)
				case 2:
					values[i] = "2023-01-15"
				case 3:
					values[i] = "text_value"
				}
			}

			b.ResetTimer()
			for range b.N {
				_ = columnTypeOf(infer.Column(values))
			}
		})
	}
}

func BenchmarkIsDatetime(b *testing.B) {
	// Skip benchmarks unless running in GitHub Actions
	if os.Getenv("GITHUB_ACTIONS") != "true" {
		b.Skip("Skipping benchmark in local environment. Set GITHUB_ACTIONS=true to run.")
	}

	testValues := []string{
		"2023-01-15T10:30:00Z",
		"2023-01-15",
		"1/15/2023",
		"15.1.2023",
		"10:30:00",
		"not a date",
		"123456",
		"hello world",
	}

	b.ResetTimer()
	for range b.N {
		for _, value := range testValues {
			_ = infer.IsDatetime(value)
		}
	}
}

func BenchmarkGetSampleValues(b *testing.B) {
	// Skip benchmarks unless running in GitHub Actions
	if os.Getenv("GITHUB_ACTIONS") != "true" {
		b.Skip("Skipping benchmark in local environment. Set GITHUB_ACTIONS=true to run.")
	}

	sizes := []int{1000, 10000, 100000, 1000000}

	for _, size := range sizes {
		values := make([]string, size)
		for i := range size {
			values[i] = strconv.Itoa(i)
		}

		b.Run(fmt.Sprintf("size_%d", size), func(b *testing.B) {
			b.ResetTimer()
			for range b.N {
				_ = columnTypeOf(infer.Column(values))
			}
		})
	}
}

func BenchmarkClassifyValue(b *testing.B) {
	// Skip benchmarks unless running in GitHub Actions
	if os.Getenv("GITHUB_ACTIONS") != "true" {
		b.Skip("Skipping benchmark in local environment. Set GITHUB_ACTIONS=true to run.")
	}

	testValues := []string{
		"123",
		"-456",
		"12.34",
		"-56.78",
		"1.23e10",
		"2023-01-15T10:30:00Z",
		"2023-01-15",
		"10:30:00",
		"hello world",
		"abc123",
	}

	b.ResetTimer()
	for range b.N {
		for _, value := range testValues {
			_ = columnTypeOf(infer.Classify(value))
		}
	}
}

func TestColumnTypeString(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		ct       columnType
		expected string
	}{
		{"text type", columnTypeText, sqlTypeText},
		{"integer type", columnTypeInteger, sqlTypeInteger},
		{"real type", columnTypeReal, sqlTypeReal},
		{"datetime type", columnTypeDatetime, sqlTypeText},
		{"unknown type", columnType(99), sqlTypeText},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.expected, tt.ct.String())
		})
	}
}

func TestChunkSizeString(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		cs       chunkSizeValue
		expected string
	}{
		{"default chunk size", chunkSizeValue(defaultChunkSizeRows), strconv.Itoa(defaultChunkSizeRows)},
		{"custom chunk size", chunkSizeValue(5000), "5000"},
		{"the smallest chunk", chunkSizeValue(1), "1"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.expected, tt.cs.String())
		})
	}
}

// TestNewChunkSize_BelowTheMinimum checks the floor on a chunk size. A chunk of
// zero or fewer rows would read a file forever, so anything under the minimum
// falls back to the default.
func TestNewChunkSize_BelowTheMinimum(t *testing.T) {
	t.Parallel()

	assert.Equal(t, chunkSizeValue(defaultChunkSizeRows), newChunkSize(0))
	assert.Equal(t, chunkSizeValue(defaultChunkSizeRows), newChunkSize(-1))
	assert.Equal(t, chunkSizeValue(1), newChunkSize(1))
}

// TestNewColumnInfoList_NoColumns covers a header with nothing in it, which is
// what an input with no columns produces.
func TestNewColumnInfoList_NoColumns(t *testing.T) {
	t.Parallel()

	assert.Nil(t, newColumnInfoList(newHeader(nil), nil))
}

// TestColumnInfoList_EqualTypes covers the comparison that decides whether a
// later chunk widens the table already created.
func TestColumnInfoList_EqualTypes(t *testing.T) {
	t.Parallel()

	integers := columnInfoList{{Name: "a", Type: columnTypeInteger}}
	texts := columnInfoList{{Name: "a", Type: columnTypeText}}

	assert.True(t, integers.equalTypes(columnInfoList{{Name: "a", Type: columnTypeInteger}}))
	assert.False(t, integers.equalTypes(texts), "a widened column is not the same schema")
	assert.False(t, integers.equalTypes(columnInfoList{}), "a different column count is not the same schema")
}

// TestInferColumnType_NoValues covers a column with no values to judge by. Text
// is the only type that holds anything a later row can bring.
func TestInferColumnType_NoValues(t *testing.T) {
	t.Parallel()

	assert.Equal(t, columnTypeText, columnTypeOf(infer.Column(nil)))
}

// TestColumnTypeEvidence_ChoosesTheTypeThatHoldsEveryValue covers the rule that
// turns what a column was seen to hold into the type it is declared as. The
// answer follows from which kinds of value were present, not from how many of
// each, so a column cannot be typed against the values it holds least often.
func TestColumnTypeEvidence_ChoosesTheTypeThatHoldsEveryValue(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		values []string
		want   columnType
	}{
		{
			name:   "nothing to judge by",
			values: nil,
			want:   columnTypeText,
		},
		{
			name:   "empty cells only",
			values: []string{"", "  ", ""},
			want:   columnTypeText,
		},
		{
			name:   "integers alone",
			values: []string{"1", "2", "3"},
			want:   columnTypeInteger,
		},
		{
			name:   "one decimal among integers",
			values: []string{"1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11.5"},
			want:   columnTypeReal,
		},
		{
			name:   "one text value among integers",
			values: []string{"1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "abc"},
			want:   columnTypeText,
		},
		{
			name:   "datetimes alone",
			values: []string{"2026-08-20", "2026-08-21"},
			want:   columnTypeDatetime,
		},
		{
			// A datetime is stored as text, so a column that also holds a number
			// has no type covering both.
			name:   "a datetime beside a number",
			values: []string{"2026-08-20", "5"},
			want:   columnTypeText,
		},
		{
			// Empty cells say nothing, so they cannot outvote the values present.
			name:   "one integer among empty cells",
			values: []string{"", "", "", "7"},
			want:   columnTypeInteger,
		},
		{
			name:   "a zero padded code among integers",
			values: []string{"1", "2", "007"},
			want:   columnTypeText,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, columnTypeOf(infer.Column(tt.values)))
		})
	}
}

// TestColumnTypeEvidence_DoesNotDependOnOrder pins the property the streaming
// loader relies on: evidence folded in any order gives the same type, so a chunk
// boundary cannot change what a column is declared as.
func TestColumnTypeEvidence_DoesNotDependOnOrder(t *testing.T) {
	t.Parallel()

	values := []string{"1", "2", "3.5", "2026-08-20", "abc", "", "007"}
	want := columnTypeOf(infer.Column(values))

	for i := range values {
		rotated := append(append([]string{}, values[i:]...), values[:i]...)
		assert.Equal(t, want, columnTypeOf(infer.Column(rotated)), "rotated by %d", i)
	}
}

// TestIsIntegerLiteralOverflowingInt64_SignOnly covers a value that is a sign
// and nothing else, which is not a number at all.
func TestIsIntegerLiteralOverflowingInt64_SignOnly(t *testing.T) {
	t.Parallel()

	assert.False(t, infer.IsIntegerLiteralOverflowingInt64("+"))
	assert.False(t, infer.IsIntegerLiteralOverflowingInt64("-"))
}

// loadColumnForTypeTest loads a one-column CSV whose header is "v" and returns
// the declared column type together with every value, rendered with the Go type
// it scanned as. The chunk size is the caller's, so the same body can be read
// as one chunk or as many.
func loadColumnForTypeTest(t *testing.T, values []string, chunkSize int) (string, []string) {
	t.Helper()

	body := "v\n" + strings.Join(values, "\n") + "\n"
	ctx := context.Background()
	validated, err := NewBuilder().
		AddReader(strings.NewReader(body), "t", FileTypeCSV).
		SetDefaultChunkSize(chunkSize).
		Build(ctx)
	require.NoError(t, err)
	db, err := validated.Open(ctx)
	require.NoError(t, err)
	defer db.Close()

	var declared string
	require.NoError(t, db.QueryRowContext(ctx,
		`SELECT type FROM pragma_table_info('t') WHERE name = 'v'`).Scan(&declared))

	rows, err := db.QueryContext(ctx, `SELECT v FROM t`)
	require.NoError(t, err)
	defer rows.Close()

	var got []string
	for rows.Next() {
		var v any
		require.NoError(t, rows.Scan(&v))
		got = append(got, fmt.Sprintf("%T(%v)", v, v))
	}
	require.NoError(t, rows.Err())
	sort.Strings(got)

	return declared, got
}

// TestColumnEvidenceList_IgnoresCellsPastTheHeader covers a row wider than the
// header it belongs to. The columns a table has are the ones the header names,
// so a cell beyond the last of them cannot decide any column's type; a row
// shorter than the header leaves the columns it does not reach alone, which is
// what a missing cell means.
func TestColumnEvidenceList_IgnoresCellsPastTheHeader(t *testing.T) {
	t.Parallel()

	header := newHeader([]string{"a", "b"})
	evidence := newColumnEvidenceList(len(header))
	evidence.addRecords([]record{
		newRecord([]string{"1", "2", "abc"}),
		newRecord([]string{"3"}),
	})

	assert.Equal(t, columnInfoList{
		{Name: "a", Type: columnTypeInteger},
		{Name: "b", Type: columnTypeInteger},
	}, evidence.columnInfos(header))
}

// TestColumnType_IsTheSameWhereverTheAwkwardValueSits pins the type of a column
// to the values it holds and not to the row one of them happens to sit on. A
// value that arrives after the first chunk used to meet a type already decided
// without it, so the same multiset of values loaded as two different columns.
func TestColumnType_IsTheSameWhereverTheAwkwardValueSits(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		awkward  string
		wantType string
	}{
		{name: "text among integers", awkward: "abc", wantType: "TEXT"},
		{name: "a decimal among integers", awkward: "3.5", wantType: "REAL"},
		{name: "an integral decimal among integers", awkward: "4.0", wantType: "REAL"},
		{name: "a datetime among integers", awkward: "2026-08-20T10:00:00Z", wantType: "TEXT"},
		{name: "a zero padded code among integers", awkward: "007", wantType: "TEXT"},
		{name: "an int64 overflow among integers", awkward: "11040320260000000000", wantType: "TEXT"},
		{name: "a literal SQLite will not convert", awkward: "1_000", wantType: "TEXT"},
		{name: "a padded number among integers", awkward: " 5 ", wantType: "TEXT"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			const integers = 8
			positions := []int{0, integers / 2, integers}
			for _, chunkSize := range []int{1, 2, 4, defaultChunkSizeRows} {
				var wantValues []string
				for _, at := range positions {
					// The awkward value is inserted rather than substituted, so
					// every position loads the same multiset of values and only
					// their order differs.
					values := make([]string, 0, integers+1)
					for i := range integers {
						if i == at {
							values = append(values, tt.awkward)
						}
						values = append(values, strconv.Itoa(i+1))
					}
					if at == integers {
						values = append(values, tt.awkward)
					}

					declared, got := loadColumnForTypeTest(t, values, chunkSize)
					assert.Equal(t, tt.wantType, declared,
						"%q at row %d, chunk size %d", tt.awkward, at+1, chunkSize)
					if wantValues == nil {
						wantValues = got
						continue
					}
					assert.Equal(t, wantValues, got,
						"%q at row %d, chunk size %d loaded different values", tt.awkward, at+1, chunkSize)
				}
			}
		})
	}
}

// TestColumnType_TextAfterTheDefaultChunkBoundary covers the same defect on the
// path a caller reaches without configuring anything: the default chunk is 1000
// rows, so a text value on row 1001 used to leave the column INTEGER, and the
// answers to ORDER BY and to a comparison changed with it.
func TestColumnType_TextAfterTheDefaultChunkBoundary(t *testing.T) {
	t.Parallel()

	const rows = 1000
	early := make([]string, 0, rows+1)
	late := make([]string, 0, rows+1)
	early = append(early, "1", "abc")
	for i := 2; i <= rows; i++ {
		early = append(early, strconv.Itoa(i))
	}
	for i := 1; i <= rows; i++ {
		late = append(late, strconv.Itoa(i))
	}
	late = append(late, "abc")

	earlyType, earlyValues := loadColumnForTypeTest(t, early, defaultChunkSizeRows)
	lateType, lateValues := loadColumnForTypeTest(t, late, defaultChunkSizeRows)

	assert.Equal(t, "TEXT", earlyType)
	assert.Equal(t, "TEXT", lateType, "a text value on row 1001 has to reach the column type")
	assert.Equal(t, earlyValues, lateValues, "the same values loaded as different Go types")
}

// TestColumnType_ADecimalMakesTheColumnReal pins a numeric column holding any
// decimal to REAL. Deciding it by how many decimals the file happens to hold
// left an INTEGER column that either rewrote them or stored them against its own
// declared type, and adding one more decimal row changed the arithmetic of rows
// nobody touched.
func TestColumnType_ADecimalMakesTheColumnReal(t *testing.T) {
	t.Parallel()

	integers := []string{"5", "7", "9", "11", "13", "15", "17", "19", "21", "23"}

	tests := []struct {
		name     string
		values   []string
		wantType string
	}{
		{name: "integers alone", values: integers, wantType: "INTEGER"},
		{name: "one integral decimal", values: append(append([]string{}, integers...), "4.0"), wantType: "REAL"},
		{name: "two integral decimals", values: append(append([]string{}, integers...), "4.0", "6.0"), wantType: "REAL"},
		{name: "one fractional decimal", values: append(append([]string{}, integers...), "2.5"), wantType: "REAL"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			declared, _ := loadColumnForTypeTest(t, tt.values, defaultChunkSizeRows)
			assert.Equal(t, tt.wantType, declared)
		})
	}

	t.Run("the arithmetic of a decimal column is not integer division", func(t *testing.T) {
		t.Parallel()

		ctx := context.Background()
		body := "v\n" + strings.Join(append(append([]string{}, integers...), "4.0"), "\n") + "\n"
		validated, err := NewBuilder().
			AddReader(strings.NewReader(body), "t", FileTypeCSV).
			Build(ctx)
		require.NoError(t, err)
		db, err := validated.Open(ctx)
		require.NoError(t, err)
		defer db.Close()

		var half float64
		require.NoError(t, db.QueryRowContext(ctx, `SELECT v / 2 FROM t WHERE rowid = 1`).Scan(&half))
		assert.InDelta(t, 2.5, half, 0.0001, "5 / 2 in a column that holds decimals")
	})
}

// TestColumnType_DeclaredTypeAgreesWithStoredType pins the invariant behind both
// of the above: SQLite stores a value the declared type cannot hold under its
// own storage class, so a schema that disagrees with typeof() is a column whose
// type was inferred from less than the whole column.
func TestColumnType_DeclaredTypeAgreesWithStoredType(t *testing.T) {
	t.Parallel()

	bodies := map[string]string{
		"decimals among integers": "v\n10\n20\n30\n40\n50\n60\n70\n80\n90\n100\n2.5\n",
		"text among integers":     "v\n10\n20\n30\n40\n50\nabc\n",
		"integers among decimals": "v\n1.5\n2.5\n3\n4\n5\n",
	}
	storageOf := map[string]string{"INTEGER": "integer", "REAL": "real", "TEXT": "text"}

	for name, body := range bodies {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			validated, err := NewBuilder().
				AddReader(strings.NewReader(body), "t", FileTypeCSV).
				Build(ctx)
			require.NoError(t, err)
			db, err := validated.Open(ctx)
			require.NoError(t, err)
			defer db.Close()

			var declared string
			require.NoError(t, db.QueryRowContext(ctx,
				`SELECT type FROM pragma_table_info('t') WHERE name = 'v'`).Scan(&declared))
			storage, ok := storageOf[declared]
			require.True(t, ok, "unexpected declared type %q", declared)

			var disagreeing int
			require.NoError(t, db.QueryRowContext(ctx,
				`SELECT count(*) FROM t WHERE v IS NOT NULL AND typeof(v) != ?`, storage).Scan(&disagreeing))
			assert.Zero(t, disagreeing, "column declared %s holds values SQLite stored otherwise", declared)
		})
	}
}

// TestColumnType_FrameAndTheLoaderAgree pins the two inferences to each other.
// The README says frame applies the same rules to its own values, and it holds
// only while both answer the same question the same way — they are separate
// implementations, so the agreement has to be tested rather than assumed.
func TestColumnType_FrameAndTheLoaderAgree(t *testing.T) {
	t.Parallel()

	bodies := map[string]string{
		"integers":                  "v\n1\n2\n3\n",
		"decimals":                  "v\n1.5\n2.5\n3\n",
		"one decimal among many":    "v\n1\n2\n3\n4\n5\n6\n7\n8\n9\n10\n11.5\n",
		"one text among many":       "v\n1\n2\n3\n4\n5\n6\n7\n8\n9\n10\nabc\n",
		"a zero padded code":        "v\n1\n2\n007\n",
		"an int64 overflow":         "v\n1\n2\n11040320260000000000\n",
		"a literal only Go parses":  "v\n1\n2\n1_000\n",
		"a padded number":           "v\n1\n2\n  42\n",
		"a datetime among integers": "v\n1\n2\n3\n4\n5\n6\n7\n8\n9\n2026-08-20\n",
		"datetimes only":            "v\n2026-08-20\n2026-08-21\n",
		"empty cells":               "v\n\n\n5\n",
	}

	for name, body := range bodies {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			validated, err := NewBuilder().
				AddReader(strings.NewReader(body), "t", FileTypeCSV).
				Build(ctx)
			require.NoError(t, err)
			db, err := validated.Open(ctx)
			require.NoError(t, err)
			defer db.Close()

			rows, err := db.QueryContext(ctx, `SELECT v FROM t ORDER BY rowid`)
			require.NoError(t, err)
			defer rows.Close()
			var loaded []string
			for rows.Next() {
				var v any
				require.NoError(t, rows.Scan(&v))
				loaded = append(loaded, fmt.Sprintf("%T", v))
			}
			require.NoError(t, rows.Err())

			df, err := frame.NewDataFrame(strings.NewReader(body), parser.CSV)
			require.NoError(t, err)
			var framed []string
			for _, row := range df.ToRecords() {
				framed = append(framed, fmt.Sprintf("%T", row["v"]))
			}

			assert.Equal(t, loaded, framed, "the loader and frame typed the same column differently")
		})
	}
}

// TestDistinctValues_FrameAndTheLoaderAgree is the same pinning for the
// question the two answer after the typing: how many values a column holds.
// frame decides it from a key it builds and SQLite from its own comparison, so
// a column of numbers that are equal has to come back as one value from both.
//
// A negative zero broke it: frame's key carried the sign, so a column holding
// both signs of zero was two values there and one to SQLite.
func TestDistinctValues_FrameAndTheLoaderAgree(t *testing.T) {
	t.Parallel()

	bodies := map[string]string{
		"both signs of zero":              "v\n-0.0\n0.0\n0\n-0\n",
		"one quantity spelled three ways": "v\n1\n1.0\n1.00\n",
		"a negative beside a zero":        "v\n-0.0\n-0.5\n0\n",
		"distinct decimals":               "v\n1.5\n2.5\n3.5\n",
	}

	for name, body := range bodies {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			validated, err := NewBuilder().
				AddReader(strings.NewReader(body), "t", FileTypeCSV).
				Build(ctx)
			require.NoError(t, err)
			db, err := validated.Open(ctx)
			require.NoError(t, err)
			defer db.Close()

			var loaded int
			require.NoError(t, db.QueryRowContext(ctx, `SELECT COUNT(DISTINCT v) FROM t`).Scan(&loaded))

			df, err := frame.NewDataFrame(strings.NewReader(body), parser.CSV)
			require.NoError(t, err)

			assert.Equal(t, loaded, df.Distinct().Len(),
				"the loader and frame counted the same column's values differently")
		})
	}
}

// TestIsFloatRejectsInt64Overflow guards the core of nao1215/sqly#218: an
// integer literal whose magnitude exceeds int64 must not be classified as a
// float, because converting it to float64 loses precision and renders it in
// scientific notation. Such values fall through to TEXT instead.
func TestIsFloatRejectsInt64Overflow(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		value string
		want  bool
	}{
		{"in-range integer is float-parseable", "123", true},
		{"max int64 is float-parseable", "9223372036854775807", true},
		{"overflow integer is not treated as float", "11040320260000000000", false},
		{"huge integer is not treated as float", "99999999999999999999999999", false},
		{"negative overflow integer is not treated as float", "-11040320260000000000", false},
		{"decimal stays float", "3.14", true},
		{"scientific notation literal stays float", "1.104032026e+19", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tt.want, infer.IsFloat(tt.value))
		})
	}
}

// TestClassifyValueInt64Overflow verifies that int64-overflowing integers are
// classified as TEXT, while in-range integers and genuine floats keep their
// numeric types.
func TestClassifyValueInt64Overflow(t *testing.T) {
	t.Parallel()

	require.Equal(t, columnTypeText, columnTypeOf(infer.Classify("11040320260000000000")))
	require.Equal(t, columnTypeText, columnTypeOf(infer.Classify("-11040320260000000000")))
	require.Equal(t, columnTypeInteger, columnTypeOf(infer.Classify("9223372036854775807")))
	require.Equal(t, columnTypeReal, columnTypeOf(infer.Classify("1.104032026e+19")))
}

// TestInferColumnTypeInt64Overflow verifies that a column entirely made of
// int64-overflowing integers is inferred as TEXT.
func TestInferColumnTypeInt64Overflow(t *testing.T) {
	t.Parallel()

	got := columnTypeOf(infer.Column([]string{
		"11040320260000000000",
		"11040320260000000001",
		"11040320260000000002",
	}))
	require.Equal(t, columnTypeText, got)
}

// TestOpenContextPreservesLargeIntegerPastTheFirstChunk is the same rule across
// the boundary that decides the schema. Column types come from the first chunk,
// so an account number past int64 arriving later met a column that was already
// INTEGER, and came back as 1.104032026e+19 — the loss the classifier refuses
// to allow in the first chunk, reached by arriving after it.
func TestOpenContextPreservesLargeIntegerPastTheFirstChunk(t *testing.T) {
	t.Parallel()

	var b strings.Builder
	b.WriteString("account\n")
	for i := range defaultChunkSizeRows * 2 {
		fmt.Fprintf(&b, "%d\n", i+1)
	}
	b.WriteString("11040320260000000000\n")

	path := filepath.Join(t.TempDir(), "accounts.csv")
	require.NoError(t, os.WriteFile(path, []byte(b.String()), 0600))

	ctx := context.Background()
	db, err := OpenContext(ctx, path)
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	var got string
	require.NoError(t, db.QueryRowContext(ctx,
		`SELECT account FROM accounts WHERE length(account) = 20`).Scan(&got))
	require.Equal(t, "11040320260000000000", got)
}

// TestOpenContextKeepsAnIntegerPast2p53BesideAFloat pins the column-level form
// of the same loss. An integer between 2^53 and int64 max is exact in an
// INTEGER column, but a float beside it used to make the column REAL, and
// SQLite's REAL affinity then stored the nearest double: 9007199254740993 came
// back as 9007199254740992.0. Such a column has to be TEXT, and a dump of it
// has to read back byte-identical.
func TestOpenContextKeepsAnIntegerPast2p53BesideAFloat(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	csvPath := filepath.Join(dir, "mixed.csv")
	require.NoError(t, os.WriteFile(csvPath, []byte("v\n9007199254740993\n0.5\n"), 0600))

	ctx := context.Background()
	db, err := OpenContext(ctx, csvPath)
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	var kinds, values string
	require.NoError(t, db.QueryRowContext(ctx,
		`SELECT group_concat(typeof(v)), group_concat(quote(v)) FROM mixed`).Scan(&kinds, &values))
	require.Equal(t, "text,text", kinds)
	require.Equal(t, "'9007199254740993','0.5'", values)

	// The dump writes the exact digits, so loading it lands in the same place.
	out := filepath.Join(dir, "out")
	require.NoError(t, DumpDatabase(db, out))
	dumped, err := os.ReadFile(filepath.Join(out, "mixed.csv")) //nolint:gosec // test-owned path
	require.NoError(t, err)
	require.Equal(t, "v\n9007199254740993\n0.5\n", string(dumped))
}

// TestOpenContextPreservesLargeIntegerExactly is the end-to-end regression test
// for nao1215/sqly#218. A CSV value larger than math.MaxInt64 must round-trip
// through the loaded database as its exact textual value, not a lossy
// scientific-notation float.
func TestOpenContextPreservesLargeIntegerExactly(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	csvPath := filepath.Join(dir, "bigint.csv")
	content := "ctsn,pocode\n" +
		"11040320260000000000,100031464478\n" +
		"11040320260000000001,100031464478\n"
	require.NoError(t, os.WriteFile(csvPath, []byte(content), 0600))

	ctx := context.Background()
	db, err := OpenContext(ctx, csvPath)
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	var got string
	err = db.QueryRowContext(ctx, `SELECT ctsn FROM bigint ORDER BY ctsn LIMIT 1`).Scan(&got)
	require.NoError(t, err)
	require.Equal(t, "11040320260000000000", got)
}

// TestIsIntegerRejectsZeroPadded guards zero-padded codes such as ZIP codes and
// product IDs: an integer literal with a redundant leading zero must not be
// classified as an integer, because SQLite INTEGER would drop the leading zero
// (for example "02134" -> 2134). A lone "0" is a normal integer.
func TestIsIntegerRejectsZeroPadded(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		value string
		want  bool
	}{
		{"plain integer", "123", true},
		{"lone zero stays integer", "0", true},
		{"negative integer", "-42", true},
		{"zero-padded code is not an integer", "007", false},
		{"zip code is not an integer", "02134", false},
		{"double zero is not an integer", "00", false},
		{"signed zero-padded is not an integer", "-01", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tt.want, infer.IsInteger(tt.value))
		})
	}
}

// TestIsFloatRejectsZeroPadded ensures a zero-padded integer literal does not
// slip through to REAL either (float64 would render "007" as 7 too). A genuine
// decimal keeps its float classification.
func TestIsFloatRejectsZeroPadded(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		value string
		want  bool
	}{
		{"zero-padded code is not a float", "007", false},
		{"zip code is not a float", "02134", false},
		{"decimal stays float", "0.5", true},
		{"plain integer stays float-parseable", "42", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tt.want, infer.IsFloat(tt.value))
		})
	}
}

// TestClassifyValueZeroPadded verifies a zero-padded code is classified as TEXT
// while ordinary numbers keep their numeric types.
func TestClassifyValueZeroPadded(t *testing.T) {
	t.Parallel()

	require.Equal(t, columnTypeText, columnTypeOf(infer.Classify("02134")))
	require.Equal(t, columnTypeText, columnTypeOf(infer.Classify("007")))
	require.Equal(t, columnTypeInteger, columnTypeOf(infer.Classify("0")))
	require.Equal(t, columnTypeInteger, columnTypeOf(infer.Classify("42")))
	require.Equal(t, columnTypeReal, columnTypeOf(infer.Classify("0.5")))
}

// TestInferColumnTypeZeroPadded verifies a column entirely made of zero-padded
// codes is inferred as TEXT, so the leading zeros survive.
func TestInferColumnTypeZeroPadded(t *testing.T) {
	t.Parallel()

	got := columnTypeOf(infer.Column([]string{"02134", "00501", "10001"}))
	require.Equal(t, columnTypeText, got)
}

// TestInferColumnTypePreservesLateZeroPadded covers a value far enough down the
// column that a sampler would have skipped it. Which type a column gets has to
// follow from every value it holds, not from where in the file a value sits.
func TestInferColumnTypePreservesLateZeroPadded(t *testing.T) {
	t.Parallel()

	const values = 2000
	column := make([]string, 0, values)
	for i := range values {
		column = append(column, strconv.Itoa(i+1))
	}
	column[len(column)-1] = "007"

	require.Equal(t, columnTypeText, columnTypeOf(infer.Column(column)))
}

// TestOpenContextPreservesZeroPaddedCodesPastTheFirstChunk is the end-to-end
// half of the same rule, across the boundary that decides the schema. Types are
// inferred from the first chunk alone, so a code arriving in a later one met a
// column that was already INTEGER and was rewritten by SQLite's affinity: 007
// came back as 7, at no error and no warning.
func TestOpenContextPreservesZeroPaddedCodesPastTheFirstChunk(t *testing.T) {
	t.Parallel()

	var b strings.Builder
	b.WriteString("code\n")
	for i := range defaultChunkSizeRows * 2 {
		fmt.Fprintf(&b, "%d\n", i+1)
	}
	b.WriteString("007\n")

	path := filepath.Join(t.TempDir(), "codes.csv")
	require.NoError(t, os.WriteFile(path, []byte(b.String()), 0600))

	ctx := context.Background()
	db, err := OpenContext(ctx, path)
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	var got string
	require.NoError(t, db.QueryRowContext(ctx,
		`SELECT code FROM codes WHERE code LIKE '0%'`).Scan(&got))
	require.Equal(t, "007", got)

	// The rows that arrived before the promotion keep their own values: a plain
	// integer's text form is the digits it was read from.
	var first string
	require.NoError(t, db.QueryRowContext(ctx,
		`SELECT code FROM codes LIMIT 1`).Scan(&first))
	require.Equal(t, "1", first)

	var rows int
	require.NoError(t, db.QueryRowContext(ctx, `SELECT COUNT(*) FROM codes`).Scan(&rows))
	require.Equal(t, defaultChunkSizeRows*2+1, rows)
}

// TestOpenContextPreservesZeroPaddedCodes is the end-to-end regression test: a
// column of zero-padded codes must round-trip through the loaded database as its
// exact textual value, not an integer with the leading zeros stripped.
func TestOpenContextPreservesZeroPaddedCodes(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	csvPath := filepath.Join(dir, "zips.csv")
	content := "zip\n02134\n00501\n"
	require.NoError(t, os.WriteFile(csvPath, []byte(content), 0600))

	ctx := context.Background()
	db, err := OpenContext(ctx, csvPath)
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	var got string
	err = db.QueryRowContext(ctx, `SELECT zip FROM zips ORDER BY zip LIMIT 1`).Scan(&got)
	require.NoError(t, err)
	require.Equal(t, "00501", got)
}

// TestSurroundingWhitespaceKeepsAValueText covers the other way a numeric column
// rewrites what it was given. SQLite's affinity converts " 5 " to 5, so the
// spaces the file quoted were gone, while the text column beside it kept its
// own: the same input was preserved or altered depending on what it looked like.
//
// A value with no surrounding whitespace is unaffected, and whitespace around
// something that is not a number was already text.
func TestSurroundingWhitespaceKeepsAValueText(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		value string
		want  bool
	}{
		{name: "a padded integer", value: " 5 ", want: true},
		{name: "a leading-space integer", value: "  42", want: true},
		{name: "a trailing-space integer", value: "42 ", want: true},
		{name: "a padded real", value: " 1.5 ", want: true},
		{name: "a padded negative", value: " -7 ", want: true},
		{name: "a plain integer", value: "5", want: false},
		{name: "a plain real", value: "1.5", want: false},
		{name: "padded text", value: " ab ", want: false},
		{name: "whitespace only", value: "   ", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if got := infer.MustStayText(tt.value); got != tt.want {
				t.Errorf("infer.MustStayText(%q) = %v, want %v", tt.value, got, tt.want)
			}
		})
	}
}

// TestQuotedWhitespaceSurvivesTheLoad is the same rule seen through a load: both
// columns keep the bytes the file quoted, which is what the quotes said.
func TestQuotedWhitespaceSurvivesTheLoad(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "padded.csv")
	if err := os.WriteFile(path, []byte("num,txt\n\" 5 \",\" ab \"\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	db, err := OpenContext(ctx, path)
	if err != nil {
		t.Fatalf("OpenContext: %v", err)
	}
	defer func() { _ = db.Close() }()

	var num, txt string
	if err := db.QueryRowContext(ctx, "SELECT num, txt FROM padded").Scan(&num, &txt); err != nil {
		t.Fatalf("query: %v", err)
	}
	if num != " 5 " {
		t.Errorf("num = %q, want %q: the quotes made the spaces part of the value", num, " 5 ")
	}
	if txt != " ab " {
		t.Errorf("txt = %q, want %q", txt, " ab ")
	}
}

// TestBlankCellInNumericColumnIsNull pins what a blank cell becomes, which
// decides what every aggregate and comparison over that column answers.
//
// It was the empty string. SQLite orders text above every number, so MAX
// answered "" rather than the largest value, AVG divided by the rows that held
// nothing, COUNT of the column counted them, ORDER BY DESC put the blank row
// first, a numeric filter passed it, and IS NULL -- the way a caller asks which
// values are missing -- matched no row. None of it raised. The Parquet exporter
// already wrote that cell as a null, so the package disagreed with itself and a
// round trip through Parquet changed the value.
func TestBlankCellInNumericColumnIsNull(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		body     string
		declared string
		// wantNull is whether the blank cell has to reach the database as NULL.
		wantNull bool
		wantMax  any
	}{
		{
			name:     "an integer column",
			body:     "region,amount\nnorth,10\nsouth,\neast,30\n",
			declared: sqlTypeInteger,
			wantNull: true,
			wantMax:  int64(30),
		},
		{
			name:     "a real column",
			body:     "region,amount\nnorth,10.5\nsouth,\neast,30.5\n",
			declared: sqlTypeReal,
			wantNull: true,
			wantMax:  30.5,
		},
		{
			// The empty string is a value a text column can hold, and telling it
			// from a missing one is a distinction this package keeps.
			name:     "a text column keeps the empty string",
			body:     "region,label\nnorth,x\nsouth,\neast,z\n",
			declared: sqlTypeText,
			wantNull: false,
			wantMax:  "z",
		},
		{
			// A datetime column is stored as TEXT, where the empty string sorts
			// below every date and so produces no wrong maximum. It is left as
			// it is; this case is here so the decision is written down.
			name:     "a datetime column keeps the empty string",
			body:     "region,seen\nnorth,2024-01-02\nsouth,\neast,2024-03-04\n",
			declared: sqlTypeText,
			wantNull: false,
			wantMax:  "2024-03-04",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			src := filepath.Join(t.TempDir(), "rows.csv")
			require.NoError(t, os.WriteFile(src, []byte(tt.body), 0o600))

			db, err := OpenContext(ctx, src)
			require.NoError(t, err)
			defer db.Close()

			column := "amount"
			if !tt.wantNull {
				column = strings.TrimSpace(strings.Split(strings.SplitN(tt.body, "\n", 2)[0], ",")[1])
			}

			var declared string
			require.NoError(t, db.QueryRowContext(ctx,
				`SELECT type FROM pragma_table_info('rows') WHERE name = ?`, column).Scan(&declared))
			assert.Equal(t, tt.declared, declared)

			var kind string
			require.NoError(t, db.QueryRowContext(ctx,
				`SELECT typeof("`+column+`") FROM rows WHERE region = 'south'`).Scan(&kind))
			if tt.wantNull {
				assert.Equal(t, "null", kind, "a blank cell in a numeric column is a missing number")
			} else {
				assert.Equal(t, "text", kind, "a blank cell in a text column is the empty string")
			}

			var missing, present int
			require.NoError(t, db.QueryRowContext(ctx,
				`SELECT SUM("`+column+`" IS NULL), COUNT("`+column+`") FROM rows`).Scan(&missing, &present))
			if tt.wantNull {
				assert.Equal(t, 1, missing, "IS NULL is how a caller asks which values are missing")
				assert.Equal(t, 2, present, "counting a column counts the values it holds")
			} else {
				assert.Equal(t, 0, missing)
				assert.Equal(t, 3, present)
			}

			var got any
			require.NoError(t, db.QueryRowContext(ctx, `SELECT MAX("`+column+`") FROM rows`).Scan(&got))
			assert.Equal(t, tt.wantMax, got)

			if tt.wantNull {
				var top string
				require.NoError(t, db.QueryRowContext(ctx,
					`SELECT region FROM rows ORDER BY "`+column+`" DESC LIMIT 1`).Scan(&top))
				assert.Equal(t, "east", top, "a row with no value must not sort above the values")

				var passed int
				require.NoError(t, db.QueryRowContext(ctx,
					`SELECT COUNT(*) FROM rows WHERE "`+column+`" > 5`).Scan(&passed))
				assert.Equal(t, 2, passed, "a row with no value must not pass a numeric filter")
			}
		})
	}
}

// TestBlankCellInNumericColumnInEveryDelimitedFormat pins that the readers
// agree about what a blank cell is. Each of them produces the cell's text and
// hands it to the same insert, so a reader that spelled a blank differently
// would put the empty string back in a numeric column for its format alone.
func TestBlankCellInNumericColumnInEveryDelimitedFormat(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		file string
		body string
	}{
		{name: "csv", file: "rows.csv", body: "region,amount\nnorth,10\nsouth,\neast,30\n"},
		{name: "tsv", file: "rows.tsv", body: "region\tamount\nnorth\t10\nsouth\t\neast\t30\n"},
		{name: "ltsv", file: "rows.ltsv", body: "region:north\tamount:10\nregion:south\tamount:\nregion:east\tamount:30\n"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			src := filepath.Join(t.TempDir(), tt.file)
			require.NoError(t, os.WriteFile(src, []byte(tt.body), 0o600))

			db, err := OpenContext(ctx, src)
			require.NoError(t, err)
			defer db.Close()

			var missing, present int
			var largest int64
			require.NoError(t, db.QueryRowContext(ctx,
				`SELECT SUM(amount IS NULL), COUNT(amount), MAX(amount) FROM rows`).Scan(&missing, &present, &largest))
			assert.Equal(t, 1, missing)
			assert.Equal(t, 2, present)
			assert.Equal(t, int64(30), largest)
		})
	}
}

// TestBlankCellInNumericColumnAtEveryChunkSize pins that the blank cell reaches
// the database the same way wherever it falls, since the insert runs per chunk.
func TestBlankCellInNumericColumnAtEveryChunkSize(t *testing.T) {
	t.Parallel()

	const body = "region,amount\nnorth,10\nsouth,\neast,30\nwest,\nsouthwest,50\n"
	for _, chunk := range []int{1, 2, 3, 5, 1000} {
		t.Run(fmt.Sprintf("chunk=%d", chunk), func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			src := filepath.Join(t.TempDir(), "rows.csv")
			require.NoError(t, os.WriteFile(src, []byte(body), 0o600))

			validated, err := NewBuilder().AddPath(src).SetDefaultChunkSize(chunk).Build(ctx)
			require.NoError(t, err)
			db, err := validated.Open(ctx)
			require.NoError(t, err)
			defer db.Close()

			var missing, present int
			var largest int64
			require.NoError(t, db.QueryRowContext(ctx,
				`SELECT SUM(amount IS NULL), COUNT(amount), MAX(amount) FROM rows`).Scan(&missing, &present, &largest))
			assert.Equal(t, 2, missing)
			assert.Equal(t, 3, present)
			assert.Equal(t, int64(50), largest)
		})
	}
}
