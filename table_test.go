package filesql

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
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

		assert.Equal(t, "test", table.getName(), "Table name mismatch")

		assert.True(t, table.getHeader().equal(header), "Header mismatch")

		assert.Len(t, table.getRecords(), 2, "Record count mismatch")

		assert.True(t, table.getRecords()[0].equal(records[0]), "First record mismatch")
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

		assert.True(t, table1.equal(table2), "Tables should be equal")
	})

	t.Run("Different names", func(t *testing.T) {
		t.Parallel()

		assert.False(t, table1.equal(table3), "Tables with different names should not be equal")
	})

	t.Run("Different header", func(t *testing.T) {
		t.Parallel()

		differentHeader := newHeader([]string{"col1", "col3"})
		table4 := newTable("test", differentHeader, records)
		assert.False(t, table1.equal(table4), "Tables with different headers should not be equal")
	})

	t.Run("Different record count", func(t *testing.T) {
		t.Parallel()

		differentRecords := []record{
			newRecord([]string{"val1", "val2"}),
		}
		table5 := newTable("test", header, differentRecords)
		assert.False(t, table1.equal(table5), "Tables with different record count should not be equal")
	})

	t.Run("Different record values", func(t *testing.T) {
		t.Parallel()

		differentValueRecords := []record{
			newRecord([]string{"val1", "val2"}),
			newRecord([]string{"val3", "different"}),
		}
		table6 := newTable("test", header, differentValueRecords)
		assert.False(t, table1.equal(table6), "Tables with different record values should not be equal")
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
			filePath: filepath.Join("home", "user", "documents", "data.csv"),
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
			filePath: filepath.Join("home", "user", "data"),
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
			assert.Equal(t, tt.expected, result, "tableFromFilePath failed for %s", tt.filePath)
		})
	}
}
