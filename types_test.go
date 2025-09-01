package filesql

import (
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

		// Invalid cases
		{"Plain text", "hello world", false},
		{"Number", "123", false},
		{"Invalid date", "2023-13-45", false},
		{"Invalid time", "25:70:90", false},
		{"Empty string", "", false},
		{"Partial date", "2023-01", false},
		{"Wrong format", "Jan 15, 2023", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isDatetime(tt.value)
			assert.Equal(t, tt.expected, result, "isDatetime failed for value: %q", tt.value)
		})
	}
}
