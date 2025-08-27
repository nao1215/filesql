package filesql

import (
	"testing"
)

func TestNewTable(t *testing.T) {
	t.Parallel()

	t.Run("Create table with header and records", func(t *testing.T) {
		t.Parallel()

		header := newHeader([]string{"col1", "col2"})
		records := []record{
			newRecord([]string{"val1", "val2"}),
			newRecord([]string{"val3", "val4"}),
		}

		table := newTable("test", header, records)

		if table.getName() != "test" {
			t.Errorf("expected name 'test', got %s", table.getName())
		}

		if !table.getHeader().equal(header) {
			t.Errorf("expected header %v, got %v", header, table.getHeader())
		}

		if len(table.getRecords()) != 2 {
			t.Errorf("expected 2 records, got %d", len(table.getRecords()))
		}

		if !table.getRecords()[0].equal(records[0]) {
			t.Errorf("expected first record %v, got %v", records[0], table.getRecords()[0])
		}
	})
}

func TestTable_Equal(t *testing.T) {
	t.Parallel()

	header := newHeader([]string{"col1", "col2"})
	records := []record{
		newRecord([]string{"val1", "val2"}),
		newRecord([]string{"val3", "val4"}),
	}

	table1 := newTable("test", header, records)
	table2 := newTable("test", header, records)
	table3 := newTable("different", header, records)

	t.Run("Equal tables", func(t *testing.T) {
		t.Parallel()

		if !table1.equal(table2) {
			t.Error("expected tables to be equal")
		}
	})

	t.Run("Different names", func(t *testing.T) {
		t.Parallel()

		if table1.equal(table3) {
			t.Error("expected tables with different names to be not equal")
		}
	})

	t.Run("Different header", func(t *testing.T) {
		t.Parallel()

		differentHeader := newHeader([]string{"col1", "col3"})
		table4 := newTable("test", differentHeader, records)
		if table1.equal(table4) {
			t.Error("expected tables with different headers to be not equal")
		}
	})

	t.Run("Different record count", func(t *testing.T) {
		t.Parallel()

		differentRecords := []record{
			newRecord([]string{"val1", "val2"}),
		}
		table5 := newTable("test", header, differentRecords)
		if table1.equal(table5) {
			t.Error("expected tables with different record count to be not equal")
		}
	})

	t.Run("Different record values", func(t *testing.T) {
		t.Parallel()

		differentValueRecords := []record{
			newRecord([]string{"val1", "val2"}),
			newRecord([]string{"val3", "different"}),
		}
		table6 := newTable("test", header, differentValueRecords)
		if table1.equal(table6) {
			t.Error("expected tables with different record values to be not equal")
		}
	})
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
			expected: "data",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := tableFromFilePath(tt.filePath)
			if result != tt.expected {
				t.Errorf("expected %s, got %s", tt.expected, result)
			}
		})
	}
}
