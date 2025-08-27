package model

import (
	"testing"
)

func TestInferColumnType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		values   []string
		expected ColumnType
	}{
		{
			name:     "all integers",
			values:   []string{"123", "456", "789"},
			expected: ColumnTypeInteger,
		},
		{
			name:     "mixed integers and floats",
			values:   []string{"123", "45.6", "789"},
			expected: ColumnTypeReal,
		},
		{
			name:     "all floats",
			values:   []string{"12.3", "45.6", "78.9"},
			expected: ColumnTypeReal,
		},
		{
			name:     "mixed numbers and text",
			values:   []string{"123", "hello", "789"},
			expected: ColumnTypeText,
		},
		{
			name:     "all text",
			values:   []string{"hello", "world", "test"},
			expected: ColumnTypeText,
		},
		{
			name:     "empty values",
			values:   []string{"", "", ""},
			expected: ColumnTypeText,
		},
		{
			name:     "integers with empty values",
			values:   []string{"123", "", "789"},
			expected: ColumnTypeInteger,
		},
		{
			name:     "negative integers",
			values:   []string{"-123", "456", "-789"},
			expected: ColumnTypeInteger,
		},
		{
			name:     "negative floats",
			values:   []string{"-12.3", "45.6", "-78.9"},
			expected: ColumnTypeReal,
		},
		{
			name:     "scientific notation",
			values:   []string{"1e10", "2.5e-3", "3.14e2"},
			expected: ColumnTypeReal,
		},
		{
			name:     "zero values",
			values:   []string{"0", "0.0", "000"},
			expected: ColumnTypeReal,
		},
		{
			name:     "ISO8601 dates",
			values:   []string{"2023-01-15", "2023-02-20", "2023-03-10"},
			expected: ColumnTypeDatetime,
		},
		{
			name:     "ISO8601 datetime",
			values:   []string{"2023-01-15T10:30:00", "2023-02-20T14:45:30", "2023-03-10T09:15:45"},
			expected: ColumnTypeDatetime,
		},
		{
			name:     "US date format",
			values:   []string{"1/15/2023", "2/20/2023", "3/10/2023"},
			expected: ColumnTypeDatetime,
		},
		{
			name:     "European date format",
			values:   []string{"15.1.2023", "20.2.2023", "10.3.2023"},
			expected: ColumnTypeDatetime,
		},
		{
			name:     "time only",
			values:   []string{"10:30:00", "14:45:30", "09:15:45"},
			expected: ColumnTypeDatetime,
		},
		{
			name:     "mixed datetime and text",
			values:   []string{"2023-01-15", "not a date", "2023-03-10"},
			expected: ColumnTypeText,
		},
		{
			name:     "datetime with timezone",
			values:   []string{"2023-01-15T10:30:00Z", "2023-02-20T14:45:30+09:00"},
			expected: ColumnTypeDatetime,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := InferColumnType(tt.values)
			if result != tt.expected {
				t.Errorf("InferColumnType(%v) = %v, want %v", tt.values, result, tt.expected)
			}
		})
	}
}

func TestInferColumnsInfo(t *testing.T) {
	t.Parallel()

	t.Run("mixed column types", func(t *testing.T) {
		header := NewHeader([]string{"id", "name", "age", "salary", "hire_date"})
		records := []Record{
			NewRecord([]string{"1", "Alice", "30", "95000", "2023-01-15"}),
			NewRecord([]string{"2", "Bob", "25", "78000", "2023-02-20"}),
			NewRecord([]string{"3", "Charlie", "35", "102000", "2023-03-10"}),
		}

		result := InferColumnsInfo(header, records)

		expected := []ColumnInfo{
			{Name: "id", Type: ColumnTypeInteger},
			{Name: "name", Type: ColumnTypeText},
			{Name: "age", Type: ColumnTypeInteger},
			{Name: "salary", Type: ColumnTypeInteger},
			{Name: "hire_date", Type: ColumnTypeDatetime},
		}

		if len(result) != len(expected) {
			t.Fatalf("Expected %d columns, got %d", len(expected), len(result))
		}

		for i, exp := range expected {
			if result[i].Name != exp.Name {
				t.Errorf("Column %d: expected name %s, got %s", i, exp.Name, result[i].Name)
			}
			if result[i].Type != exp.Type {
				t.Errorf("Column %d: expected type %s, got %s", i, exp.Type, result[i].Type)
			}
		}
	})

	t.Run("empty records", func(t *testing.T) {
		header := NewHeader([]string{"col1", "col2"})
		records := []Record{}

		result := InferColumnsInfo(header, records)

		if len(result) != 2 {
			t.Fatalf("Expected 2 columns, got %d", len(result))
		}

		for i, col := range result {
			if col.Type != ColumnTypeText {
				t.Errorf("Column %d: expected TEXT type for empty records, got %s", i, col.Type)
			}
		}
	})

	t.Run("datetime column inference", func(t *testing.T) {
		header := NewHeader([]string{"event_date", "event_time", "timestamp"})
		records := []Record{
			NewRecord([]string{"2023-01-15", "10:30:00", "2023-01-15T10:30:00Z"}),
			NewRecord([]string{"2023-02-20", "14:45:30", "2023-02-20T14:45:30Z"}),
			NewRecord([]string{"2023-03-10", "09:15:45", "2023-03-10T09:15:45Z"}),
		}

		result := InferColumnsInfo(header, records)

		expected := []ColumnInfo{
			{Name: "event_date", Type: ColumnTypeDatetime},
			{Name: "event_time", Type: ColumnTypeDatetime},
			{Name: "timestamp", Type: ColumnTypeDatetime},
		}

		if len(result) != len(expected) {
			t.Fatalf("Expected %d columns, got %d", len(expected), len(result))
		}

		for i, exp := range expected {
			if result[i].Name != exp.Name {
				t.Errorf("Column %d: expected name %s, got %s", i, exp.Name, result[i].Name)
			}
			if result[i].Type != exp.Type {
				t.Errorf("Column %d: expected type %s, got %s", i, exp.Type, result[i].Type)
			}
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
			if result != tt.expected {
				t.Errorf("isDatetime(%q) = %v, want %v", tt.value, result, tt.expected)
			}
		})
	}
}
