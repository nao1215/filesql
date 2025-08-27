package model

import (
	"testing"
)

func TestNewHeader(t *testing.T) {
	t.Parallel()

	t.Run("Create header from slice", func(t *testing.T) {
		t.Parallel()

		headerSlice := []string{"col1", "col2", "col3"}
		header := NewHeader(headerSlice)

		if len(header) != 3 {
			t.Errorf("expected length 3, got %d", len(header))
		}

		for i, expected := range headerSlice {
			if header[i] != expected {
				t.Errorf("expected %s at index %d, got %s", expected, i, header[i])
			}
		}
	})
}

func TestHeader_Equal(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		header1  Header
		header2  Header
		expected bool
	}{
		{
			name:     "Equal headers",
			header1:  NewHeader([]string{"col1", "col2"}),
			header2:  NewHeader([]string{"col1", "col2"}),
			expected: true,
		},
		{
			name:     "Different length headers",
			header1:  NewHeader([]string{"col1", "col2"}),
			header2:  NewHeader([]string{"col1"}),
			expected: false,
		},
		{
			name:     "Different content headers",
			header1:  NewHeader([]string{"col1", "col2"}),
			header2:  NewHeader([]string{"col1", "col3"}),
			expected: false,
		},
		{
			name:     "Empty headers",
			header1:  NewHeader([]string{}),
			header2:  NewHeader([]string{}),
			expected: true,
		},
		{
			name:     "One empty one not",
			header1:  NewHeader([]string{}),
			header2:  NewHeader([]string{"col1"}),
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := tt.header1.Equal(tt.header2)
			if result != tt.expected {
				t.Errorf("expected %v, got %v", tt.expected, result)
			}
		})
	}
}

func TestNewRecord(t *testing.T) {
	t.Parallel()

	t.Run("Create record from slice", func(t *testing.T) {
		t.Parallel()

		recordSlice := []string{"val1", "val2", "val3"}
		record := NewRecord(recordSlice)

		if len(record) != 3 {
			t.Errorf("expected length 3, got %d", len(record))
		}

		for i, expected := range recordSlice {
			if record[i] != expected {
				t.Errorf("expected %s at index %d, got %s", expected, i, record[i])
			}
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
			record1:  NewRecord([]string{"val1", "val2"}),
			record2:  NewRecord([]string{"val1", "val2"}),
			expected: true,
		},
		{
			name:     "Different length records",
			record1:  NewRecord([]string{"val1", "val2"}),
			record2:  NewRecord([]string{"val1"}),
			expected: false,
		},
		{
			name:     "Different content records",
			record1:  NewRecord([]string{"val1", "val2"}),
			record2:  NewRecord([]string{"val1", "val3"}),
			expected: false,
		},
		{
			name:     "Empty records",
			record1:  NewRecord([]string{}),
			record2:  NewRecord([]string{}),
			expected: true,
		},
		{
			name:     "One empty one not",
			record1:  NewRecord([]string{}),
			record2:  NewRecord([]string{"val1"}),
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := tt.record1.Equal(tt.record2)
			if result != tt.expected {
				t.Errorf("expected %v, got %v", tt.expected, result)
			}
		})
	}
}

func TestColumnType_String(t *testing.T) {
	t.Parallel()

	tests := []struct {
		columnType ColumnType
		expected   string
	}{
		{ColumnTypeText, "TEXT"},
		{ColumnTypeInteger, "INTEGER"},
		{ColumnTypeReal, "REAL"},
		{ColumnTypeDatetime, "TEXT"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			result := tt.columnType.String()
			if result != tt.expected {
				t.Errorf("ColumnType.String() = %s, want %s", result, tt.expected)
			}
		})
	}
}
