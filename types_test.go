package filesql

import (
	"fmt"
	"os"
	"strconv"
	"testing"

	"github.com/nao1215/filesql/internal/infer"
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
