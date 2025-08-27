package filesql

import (
	"testing"
)

func TestNewHeader(t *testing.T) {
	t.Parallel()

	t.Run("Create header from slice", func(t *testing.T) {
		t.Parallel()

		headerSlice := []string{"col1", "col2", "col3"}
		header := newHeader(headerSlice)

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
		record := newRecord(recordSlice)

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
			if result != tt.expected {
				t.Errorf("expected %v, got %v", tt.expected, result)
			}
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
			if result != tt.expected {
				t.Errorf("columnType.string() = %s, want %s", result, tt.expected)
			}
		})
	}
}
