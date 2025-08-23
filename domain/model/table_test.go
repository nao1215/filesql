package model

import (
	"testing"
)

func TestNewTable(t *testing.T) {
	t.Parallel()

	header := NewHeader([]string{"col1", "col2"})
	records := []Record{
		NewRecord([]string{"val1", "val2"}),
		NewRecord([]string{"val3", "val4"}),
	}

	table := NewTable("test", header, records)

	if table.Name() != "test" {
		t.Errorf("expected name 'test', got %s", table.Name())
	}

	if !table.Header().Equal(header) {
		t.Errorf("expected header %v, got %v", header, table.Header())
	}

	if len(table.Records()) != 2 {
		t.Errorf("expected 2 records, got %d", len(table.Records()))
	}

	if !table.Records()[0].Equal(records[0]) {
		t.Errorf("expected first record %v, got %v", records[0], table.Records()[0])
	}
}

func TestTable_Equal(t *testing.T) {
	t.Parallel()

	header := NewHeader([]string{"col1", "col2"})
	records := []Record{
		NewRecord([]string{"val1", "val2"}),
		NewRecord([]string{"val3", "val4"}),
	}

	table1 := NewTable("test", header, records)
	table2 := NewTable("test", header, records)
	table3 := NewTable("different", header, records)

	if !table1.Equal(table2) {
		t.Error("expected tables to be equal")
	}

	if table1.Equal(table3) {
		t.Error("expected tables with different names to be not equal")
	}

	// Test with different header
	differentHeader := NewHeader([]string{"col1", "col3"})
	table4 := NewTable("test", differentHeader, records)
	if table1.Equal(table4) {
		t.Error("expected tables with different headers to be not equal")
	}

	// Test with different records
	differentRecords := []Record{
		NewRecord([]string{"val1", "val2"}),
	}
	table5 := NewTable("test", header, differentRecords)
	if table1.Equal(table5) {
		t.Error("expected tables with different record count to be not equal")
	}

	// Test with different record values
	differentValueRecords := []Record{
		NewRecord([]string{"val1", "val2"}),
		NewRecord([]string{"val3", "different"}),
	}
	table6 := NewTable("test", header, differentValueRecords)
	if table1.Equal(table6) {
		t.Error("expected tables with different record values to be not equal")
	}
}

func TestTableFromFilePath_Additional(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		filePath string
		expected string
	}{
		{
			name:     "Simple file with extension",
			filePath: "data.csv",
			expected: "data",
		},
		{
			name:     "File with path",
			filePath: "/home/user/documents/data.csv",
			expected: "data",
		},
		{
			name:     "File with multiple dots",
			filePath: "data.backup.csv",
			expected: "data.backup",
		},
		{
			name:     "File without extension",
			filePath: "data",
			expected: "data",
		},
		{
			name:     "File with path and no extension",
			filePath: "/home/user/data",
			expected: "data",
		},
		{
			name:     "Hidden file",
			filePath: ".hidden",
			expected: "",
		},
		{
			name:     "Hidden file with extension",
			filePath: ".gitignore",
			expected: "",
		},
		{
			name:     "Compressed file",
			filePath: "data.csv.gz",
			expected: "data.csv",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := TableFromFilePath(tt.filePath)
			if result != tt.expected {
				t.Errorf("expected %s, got %s", tt.expected, result)
			}
		})
	}
}
