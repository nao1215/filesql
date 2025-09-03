package filesql

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
		record1  Record
		record2  Record
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
			values:   []string{"0", "0.0", "000"},
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
			result := inferColumnType(tt.values)
			assert.Equal(t, tt.expected, result, "inferColumnType failed for values: %v", tt.values)
		})
	}
}

func TestNewColumnInfoList(t *testing.T) {
	t.Parallel()

	t.Run("mixed column types", func(t *testing.T) {
		header := newHeader([]string{"id", "name", "age", "salary", "hire_date"})
		records := []Record{
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
		records := []Record{}

		result := newColumnInfoList(header, records)

		require.Len(t, result, 2, "Expected 2 columns for empty records")

		for i, col := range result {
			assert.Equal(t, columnTypeText, col.Type, "Column %d should be TEXT type for empty records", i)
		}
	})

	t.Run("datetime column inference", func(t *testing.T) {
		header := newHeader([]string{"event_date", "event_time", "timestamp"})
		records := []Record{
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
			result := isDatetime(tt.value)
			assert.Equal(t, tt.expected, result, "isDatetime failed for value: %q", tt.value)
		})
	}
}

// TestGetSampleValues tests the sampling optimization for large datasets
func TestGetSampleValues(t *testing.T) {
	t.Parallel()

	t.Run("small dataset - no sampling", func(t *testing.T) {
		t.Parallel()
		values := []string{"1", "2", "3", "4", "5"}
		result := getSampleValues(values)

		assert.Equal(t, values, result, "Small datasets should not be sampled")
	})

	t.Run("large dataset - stratified sampling applied", func(t *testing.T) {
		// Skip slow tests unless running in GitHub Actions
		if os.Getenv("GITHUB_ACTIONS") != "true" {
			t.Skip("Skipping slow test in local environment. Set GITHUB_ACTIONS=true to run.")
		}

		t.Parallel()

		// Create a large dataset with patterns in different sections
		values := make([]string, 3000)
		for i := range 3000 {
			switch {
			case i < 1000: // Beginning: integers
				values[i] = strconv.Itoa(i)
			case i < 2000: // Middle: floats
				values[i] = fmt.Sprintf("%.2f", float64(i)/100)
			default: // End: text
				values[i] = "text_" + strconv.Itoa(i)
			}
		}

		result := getSampleValues(values)

		assert.LessOrEqual(t, len(result), MaxSampleSize, "Sample size should not exceed MaxSampleSize")
		assert.Greater(t, len(result), 0, "Sample should not be empty")

		// Verify stratified sampling captured values from all sections
		hasInteger := false
		hasFloat := false
		hasText := false

		for _, val := range result {
			if _, err := strconv.Atoi(val); err == nil {
				hasInteger = true
			} else if _, err := strconv.ParseFloat(val, 64); err == nil {
				hasFloat = true
			} else if strings.HasPrefix(val, "text_") {
				hasText = true
			}
		}

		assert.True(t, hasInteger, "Sample should include integers from beginning section")
		assert.True(t, hasFloat, "Sample should include floats from middle section")
		assert.True(t, hasText, "Sample should include text from end section")
	})

	t.Run("empty dataset", func(t *testing.T) {
		t.Parallel()

		values := []string{}
		result := getSampleValues(values)

		assert.Empty(t, result, "Empty dataset should return empty sample")
	})

	t.Run("small dataset fallback to simple sampling", func(t *testing.T) {
		t.Parallel()

		// Create dataset smaller than sampleSize*SamplingStratificationFactor (3000)
		values := make([]string, 2500)
		for i := range 2500 {
			values[i] = strconv.Itoa(i)
		}

		result := getSampleValues(values)

		assert.LessOrEqual(t, len(result), MaxSampleSize, "Sample size should not exceed MaxSampleSize")
		assert.Greater(t, len(result), 0, "Sample should not be empty")
		assert.Equal(t, "0", result[0], "Simple sampling should start with first value")
	})

	t.Run("very small sample size edge case", func(t *testing.T) {
		t.Parallel()

		// Test with sample size smaller than number of sections
		values := make([]string, 100)
		for i := range 100 {
			values[i] = strconv.Itoa(i)
		}

		// Temporarily modify MaxSampleSize for this test by creating a custom function
		// Since we can't modify constants, we'll test the boundary conditions indirectly
		result := getSampleValues(values)

		assert.LessOrEqual(t, len(result), len(values), "Sample should not exceed input size")
		assert.Greater(t, len(result), 0, "Sample should not be empty")
	})
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

			result := classifyValue(tt.value)
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

			result := isInteger(tt.value)
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
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := isFloat(tt.value)
			assert.Equal(t, tt.expected, result, "isFloat failed for value: %q", tt.value)
		})
	}
}

// TestSelectColumnType tests confidence-based column type selection
func TestSelectColumnType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		typeCounts map[columnType]int
		totalCount int
		expected   columnType
	}{
		{
			name: "high confidence integer",
			typeCounts: map[columnType]int{
				columnTypeInteger:  8,
				columnTypeReal:     0,
				columnTypeDatetime: 0,
				columnTypeText:     0,
			},
			totalCount: 10,
			expected:   columnTypeInteger,
		},
		{
			name: "mixed with text preference",
			typeCounts: map[columnType]int{
				columnTypeInteger:  5,
				columnTypeReal:     0,
				columnTypeDatetime: 0,
				columnTypeText:     3,
			},
			totalCount: 10,
			expected:   columnTypeText,
		},
		{
			name: "high confidence datetime",
			typeCounts: map[columnType]int{
				columnTypeInteger:  0,
				columnTypeReal:     0,
				columnTypeDatetime: 9,
				columnTypeText:     0,
			},
			totalCount: 10,
			expected:   columnTypeDatetime,
		},
		{
			name: "low confidence fallback to most common",
			typeCounts: map[columnType]int{
				columnTypeInteger:  3,
				columnTypeReal:     4,
				columnTypeDatetime: 2,
				columnTypeText:     0,
			},
			totalCount: 10,
			expected:   columnTypeReal,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := selectColumnType(tt.typeCounts, tt.totalCount)
			assert.Equal(t, tt.expected, result, "selectColumnType failed")
		})
	}
}

// TestInferColumnTypePerformance tests performance improvements with large datasets
func TestInferColumnTypePerformance(t *testing.T) {
	// Skip slow tests unless running in GitHub Actions
	if os.Getenv("GITHUB_ACTIONS") != "true" {
		t.Skip("Skipping slow test in local environment. Set GITHUB_ACTIONS=true to run.")
	}

	t.Parallel()

	t.Run("large dataset sampling", func(t *testing.T) {
		t.Parallel()

		// Create a large dataset with mixed types - majority text to ensure text classification
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

		// The function should complete quickly due to sampling
		result := inferColumnType(values)

		// With majority text values, it should be classified as text
		assert.Equal(t, columnTypeText, result, "Large mixed dataset should be classified as text")
	})

	t.Run("early termination with text", func(t *testing.T) {
		t.Parallel()

		// Create dataset that exceeds EarlyTerminationThreshold (0.5) for text values
		values := make([]string, 1000)
		for i := range 1000 {
			if i < 600 { // 60% text values, exceeds EarlyTerminationThreshold
				values[i] = "text_value"
			} else {
				values[i] = strconv.Itoa(i)
			}
		}

		result := inferColumnType(values)

		// Should terminate early and classify as text
		assert.Equal(t, columnTypeText, result, "Dataset with >50% text values should be classified as text via early termination")
	})

	t.Run("no early termination below threshold", func(t *testing.T) {
		t.Parallel()

		// Create dataset that stays below EarlyTerminationThreshold (0.5) for text values
		values := make([]string, 1000)
		for i := range 1000 {
			if i < 400 { // 40% text values, below EarlyTerminationThreshold
				values[i] = "text_value"
			} else {
				values[i] = strconv.Itoa(i)
			}
		}

		result := inferColumnType(values)

		// Should still classify as text but not via early termination
		assert.Equal(t, columnTypeText, result, "Dataset with text values should still be classified as text even without early termination")
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
				_ = inferColumnType(values)
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
				_ = inferColumnType(values)
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
			_ = isDatetime(value)
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
				_ = getSampleValues(values)
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
			_ = classifyValue(value)
		}
	}
}
