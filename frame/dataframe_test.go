package frame

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/nao1215/filesql/parser"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewDataFrame(t *testing.T) {
	t.Parallel()

	t.Run("creates DataFrame from CSV with header and data rows", func(t *testing.T) {
		t.Parallel()

		input := "name,age,city\nAlice,30,Tokyo\nBob,25,Osaka"
		reader := strings.NewReader(input)

		df, err := NewDataFrame(reader, CSV)

		require.NoError(t, err)
		assert.Equal(t, []string{"name", "age", "city"}, df.Columns())
		assert.Equal(t, 2, df.Len())
	})

	t.Run("parses integer values correctly", func(t *testing.T) {
		t.Parallel()

		input := "value\n42"
		reader := strings.NewReader(input)

		df, err := NewDataFrame(reader, CSV)

		require.NoError(t, err)
		records := df.ToRecords()
		assert.Equal(t, int64(42), records[0]["value"])
	})

	t.Run("parses float values correctly", func(t *testing.T) {
		t.Parallel()

		input := "value\n3.14"
		reader := strings.NewReader(input)

		df, err := NewDataFrame(reader, CSV)

		require.NoError(t, err)
		records := df.ToRecords()
		assert.Equal(t, 3.14, records[0]["value"])
	})

	t.Run("parses boolean values as strings (SQLite behavior)", func(t *testing.T) {
		t.Parallel()

		input := "value\ntrue"
		reader := strings.NewReader(input)

		df, err := NewDataFrame(reader, CSV)

		require.NoError(t, err)
		records := df.ToRecords()
		// SQLite stores boolean as string since it has no native bool type
		assert.Equal(t, "true", records[0]["value"])
	})

	t.Run("returns empty string for empty values (SQLite behavior)", func(t *testing.T) {
		t.Parallel()

		input := "a,b\n,test"
		reader := strings.NewReader(input)

		df, err := NewDataFrame(reader, CSV)

		require.NoError(t, err)
		require.Equal(t, 1, df.Len())
		records := df.ToRecords()
		// SQLite distinguishes between NULL and empty string; CSV empty values become ""
		assert.Equal(t, "", records[0]["a"])
		assert.Equal(t, "test", records[0]["b"])
	})

	t.Run("creates DataFrame with header only (no data rows)", func(t *testing.T) {
		t.Parallel()

		input := "name,age"
		reader := strings.NewReader(input)

		df, err := NewDataFrame(reader, CSV)

		require.NoError(t, err)
		assert.Equal(t, []string{"name", "age"}, df.Columns())
		assert.Equal(t, 0, df.Len())
	})

	t.Run("returns error for nil reader", func(t *testing.T) {
		t.Parallel()

		_, err := NewDataFrame(nil, CSV)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "reader cannot be nil")
	})

	t.Run("returns error for empty input", func(t *testing.T) {
		t.Parallel()

		reader := strings.NewReader("")

		_, err := NewDataFrame(reader, CSV)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "empty CSV data")
	})

	t.Run("reads TSV file correctly", func(t *testing.T) {
		t.Parallel()

		input := "name\tage\nAlice\t30"
		reader := strings.NewReader(input)

		df, err := NewDataFrame(reader, TSV)

		require.NoError(t, err)
		assert.Equal(t, []string{"name", "age"}, df.Columns())
		assert.Equal(t, 1, df.Len())
	})

	t.Run("reads CSV file from testdata", func(t *testing.T) {
		t.Parallel()

		f, err := os.Open(filepath.Join("testdata", "sales.csv"))
		require.NoError(t, err)
		defer f.Close()

		df, err := NewDataFrame(f, CSV)

		require.NoError(t, err)
		assert.Equal(t, []string{"product", "amount", "category"}, df.Columns())
		assert.Equal(t, 5, df.Len())
	})

	t.Run("reads TSV file from testdata", func(t *testing.T) {
		t.Parallel()

		f, err := os.Open(filepath.Join("testdata", "sales.tsv"))
		require.NoError(t, err)
		defer f.Close()

		df, err := NewDataFrame(f, TSV)

		require.NoError(t, err)
		assert.Equal(t, []string{"product", "amount", "category"}, df.Columns())
		assert.Equal(t, 3, df.Len())
	})
}

func TestNewDataFrameFromRecords(t *testing.T) {
	t.Parallel()

	t.Run("creates DataFrame from records", func(t *testing.T) {
		t.Parallel()

		records := []map[string]any{
			{"name": "Alice", "age": 30},
			{"name": "Bob", "age": 25},
		}

		df := NewDataFrameFromRecords(records)

		assert.Equal(t, 2, df.Len())
		assert.ElementsMatch(t, []string{"age", "name"}, df.Columns())
	})

	t.Run("creates empty DataFrame from empty records", func(t *testing.T) {
		t.Parallel()

		df := NewDataFrameFromRecords([]map[string]any{})

		assert.Equal(t, 0, df.Len())
		assert.Empty(t, df.Columns())
	})

	t.Run("creates copy of input records (immutability)", func(t *testing.T) {
		t.Parallel()

		records := []map[string]any{
			{"name": "Alice"},
		}

		df := NewDataFrameFromRecords(records)

		// Modify original record
		records[0]["name"] = "Modified"

		// DataFrame should not be affected
		dfRecords := df.ToRecords()
		assert.Equal(t, "Alice", dfRecords[0]["name"])
	})

	t.Run("preserves alphabetically-sorted column order within each record", func(t *testing.T) {
		t.Parallel()

		// When a single record has keys, they are sorted alphabetically
		// This ensures deterministic column order regardless of Go's map iteration order
		records := []map[string]any{
			{"zebra": 1, "apple": 2, "mango": 3},
		}

		df := NewDataFrameFromRecords(records)

		// Columns should be sorted alphabetically: apple, mango, zebra
		assert.Equal(t, []string{"apple", "mango", "zebra"}, df.Columns())
	})

	t.Run("columns from first record appear before columns from later records", func(t *testing.T) {
		t.Parallel()

		// First record has columns "b" and "d"
		// Second record introduces "a" and "c"
		// Expected order: b, d (from first), then a, c (from second, sorted)
		records := []map[string]any{
			{"b": 1, "d": 2},
			{"a": 3, "c": 4, "b": 5, "d": 6},
		}

		df := NewDataFrameFromRecords(records)

		// First record's columns (sorted): b, d
		// Second record's new columns (sorted): a, c
		assert.Equal(t, []string{"b", "d", "a", "c"}, df.Columns())
	})

	t.Run("handles records with different column sets", func(t *testing.T) {
		t.Parallel()

		records := []map[string]any{
			{"x": 1, "y": 2},
			{"y": 3, "z": 4},
			{"w": 5, "x": 6},
		}

		df := NewDataFrameFromRecords(records)

		// First record: x, y (sorted)
		// Second record adds: z
		// Third record adds: w
		assert.Equal(t, []string{"x", "y", "z", "w"}, df.Columns())
		assert.Equal(t, 3, df.Len())

		// Verify missing values are nil
		recordsOut := df.ToRecords()
		assert.Nil(t, recordsOut[0]["z"])
		assert.Nil(t, recordsOut[0]["w"])
		assert.Nil(t, recordsOut[1]["x"])
		assert.Nil(t, recordsOut[1]["w"])
		assert.Nil(t, recordsOut[2]["y"])
		assert.Nil(t, recordsOut[2]["z"])
	})

	t.Run("column order is deterministic regardless of Go map iteration order", func(t *testing.T) {
		t.Parallel()

		// Run multiple times to verify determinism despite Go's random map iteration
		for range 10 {
			records := []map[string]any{
				{"c": 1, "a": 2, "b": 3},
			}

			df := NewDataFrameFromRecords(records)

			// Within a single record, keys are sorted alphabetically
			assert.Equal(t, []string{"a", "b", "c"}, df.Columns(),
				"Column order should be deterministic (alphabetically sorted within record)")
		}
	})

	t.Run("multi-record column order: first record columns sorted, then new columns from subsequent records", func(t *testing.T) {
		t.Parallel()

		// This test explicitly documents the column ordering contract:
		// 1. Process records in order
		// 2. Within each record, keys are sorted alphabetically
		// 3. New columns (not seen before) are appended
		records := []map[string]any{
			{"z": 1, "m": 2},         // First record: columns sorted -> [m, z]
			{"a": 3, "z": 4, "m": 5}, // Second record: "a" is new -> [m, z, a]
			{"b": 6, "a": 7},         // Third record: "b" is new -> [m, z, a, b]
		}

		df := NewDataFrameFromRecords(records)

		// m, z from first record (sorted alphabetically)
		// a from second record (new column)
		// b from third record (new column)
		assert.Equal(t, []string{"m", "z", "a", "b"}, df.Columns())
	})
}

func TestDataFrame_Select(t *testing.T) {
	t.Parallel()

	t.Run("selects specified columns", func(t *testing.T) {
		t.Parallel()

		df := NewDataFrameFromRecords([]map[string]any{
			{"a": 1, "b": 2, "c": 3},
		})

		result, err := df.Select("a", "c")
		require.NoError(t, err)

		assert.Equal(t, []string{"a", "c"}, result.Columns())
		records := result.ToRecords()
		assert.Equal(t, 1, records[0]["a"])
		assert.Equal(t, 3, records[0]["c"])
		assert.Nil(t, records[0]["b"])
	})

	// A typo used to be skipped, so the frame came back quietly missing that
	// column while Sort, GroupBy and Rename all refused the same typo.
	t.Run("refuses a column that does not exist", func(t *testing.T) {
		t.Parallel()

		df := NewDataFrameFromRecords([]map[string]any{
			{"a": 1, "b": 2},
		})

		_, err := df.Select("a", "nonexistent")

		require.Error(t, err)
		assert.Contains(t, err.Error(), "nonexistent")
	})

	// Columns and ToCSV kept both, and ToRecords kept one, because a row is a
	// map and a map cannot hold the name twice.
	t.Run("refuses the same column twice", func(t *testing.T) {
		t.Parallel()

		df := NewDataFrameFromRecords([]map[string]any{
			{"a": 1, "b": 2},
		})

		_, err := df.Select("a", "a")

		require.Error(t, err)
	})

	t.Run("returns empty DataFrame when no columns specified", func(t *testing.T) {
		t.Parallel()

		df := NewDataFrameFromRecords([]map[string]any{
			{"a": 1},
		})

		result, err := df.Select()
		require.NoError(t, err)

		assert.Empty(t, result.Columns())
		assert.Empty(t, result.ToRecords())
	})

	t.Run("preserves column order as specified", func(t *testing.T) {
		t.Parallel()

		df := NewDataFrameFromRecords([]map[string]any{
			{"a": 1, "b": 2, "c": 3},
		})

		result, err := df.Select("c", "a", "b")
		require.NoError(t, err)

		assert.Equal(t, []string{"c", "a", "b"}, result.Columns())
	})
}

func TestDataFrame_Filter(t *testing.T) {
	t.Parallel()

	t.Run("filters rows based on predicate", func(t *testing.T) {
		t.Parallel()

		df := NewDataFrameFromRecords([]map[string]any{
			{"name": "Alice", "age": int64(30)},
			{"name": "Bob", "age": int64(25)},
			{"name": "Charlie", "age": int64(35)},
		})

		result := df.Filter(func(row map[string]any) bool {
			age, ok := row["age"].(int64)
			return ok && age >= 30
		})

		assert.Equal(t, 2, result.Len())
		records := result.ToRecords()
		assert.Equal(t, "Alice", records[0]["name"])
		assert.Equal(t, "Charlie", records[1]["name"])
	})

	t.Run("returns empty DataFrame when no rows match", func(t *testing.T) {
		t.Parallel()

		df := NewDataFrameFromRecords([]map[string]any{
			{"value": int64(1)},
			{"value": int64(2)},
		})

		result := df.Filter(func(row map[string]any) bool {
			v, ok := row["value"].(int64)
			return ok && v > 100
		})

		assert.Equal(t, 0, result.Len())
	})

	t.Run("returns clone when filter function is nil", func(t *testing.T) {
		t.Parallel()

		df := NewDataFrameFromRecords([]map[string]any{
			{"value": 1},
		})

		result := df.Filter(nil)

		assert.Equal(t, df.Len(), result.Len())
	})

	t.Run("creates copy of filtered rows (immutability)", func(t *testing.T) {
		t.Parallel()

		df := NewDataFrameFromRecords([]map[string]any{
			{"name": "Alice"},
		})

		result := df.Filter(func(_ map[string]any) bool {
			return true
		})

		// Modify original DataFrame's data (via ToRecords which returns copies)
		original := df.ToRecords()
		original[0]["name"] = "Modified"

		// Result should not be affected
		resultRecords := result.ToRecords()
		assert.Equal(t, "Alice", resultRecords[0]["name"])
	})
}

func TestDataFrame_Mutate(t *testing.T) {
	t.Parallel()

	t.Run("adds new column with computed value", func(t *testing.T) {
		t.Parallel()

		df := NewDataFrameFromRecords([]map[string]any{
			{"a": int64(1), "b": int64(2)},
			{"a": int64(3), "b": int64(4)},
		})

		result := df.Mutate("sum", func(row map[string]any) any {
			a, _ := row["a"].(int64) //nolint:errcheck // test code
			b, _ := row["b"].(int64) //nolint:errcheck // test code
			return a + b
		})

		assert.Contains(t, result.Columns(), "sum")
		records := result.ToRecords()
		assert.Equal(t, int64(3), records[0]["sum"])
		assert.Equal(t, int64(7), records[1]["sum"])
	})

	t.Run("modifies existing column", func(t *testing.T) {
		t.Parallel()

		df := NewDataFrameFromRecords([]map[string]any{
			{"value": int64(10)},
		})

		result := df.Mutate("value", func(row map[string]any) any {
			v, _ := row["value"].(int64) //nolint:errcheck // test code
			return v * 2
		})

		records := result.ToRecords()
		assert.Equal(t, int64(20), records[0]["value"])
	})

	t.Run("does not modify original DataFrame", func(t *testing.T) {
		t.Parallel()

		df := NewDataFrameFromRecords([]map[string]any{
			{"value": int64(10)},
		})

		_ = df.Mutate("value", func(_ map[string]any) any {
			return int64(999)
		})

		// Original should be unchanged
		records := df.ToRecords()
		assert.Equal(t, int64(10), records[0]["value"])
	})

	t.Run("returns clone when function is nil", func(t *testing.T) {
		t.Parallel()

		df := NewDataFrameFromRecords([]map[string]any{
			{"value": 1},
		})

		result := df.Mutate("new_col", nil)

		assert.Equal(t, df.Len(), result.Len())
		assert.NotContains(t, result.Columns(), "new_col")
	})

	t.Run("returns clone when column name is empty", func(t *testing.T) {
		t.Parallel()

		df := NewDataFrameFromRecords([]map[string]any{
			{"value": 1},
		})

		result := df.Mutate("", func(_ map[string]any) any {
			return "test"
		})

		assert.Equal(t, df.Columns(), result.Columns())
	})
}

func TestDataFrame_ToCSV(t *testing.T) {
	t.Parallel()

	t.Run("writes DataFrame to CSV file", func(t *testing.T) {
		t.Parallel()

		df := NewDataFrameFromRecords([]map[string]any{
			{"name": "Alice", "age": int64(30)},
			{"name": "Bob", "age": int64(25)},
		})

		tmpDir := t.TempDir()
		path := filepath.Join(tmpDir, "output.csv")

		err := df.ToCSV(path)

		require.NoError(t, err)

		// Read back and verify
		f, err := os.Open(path) //nolint:gosec // test file path
		require.NoError(t, err)
		defer f.Close()

		readDf, err := NewDataFrame(f, CSV)
		require.NoError(t, err)

		assert.Equal(t, df.Len(), readDf.Len())
	})

	t.Run("handles empty DataFrame", func(t *testing.T) {
		t.Parallel()

		df := NewDataFrameFromRecords([]map[string]any{})

		tmpDir := t.TempDir()
		path := filepath.Join(tmpDir, "empty.csv")

		err := df.ToCSV(path)

		require.NoError(t, err)
	})

	t.Run("returns error for invalid path", func(t *testing.T) {
		t.Parallel()

		df := NewDataFrameFromRecords([]map[string]any{
			{"value": 1},
		})

		err := df.ToCSV("/nonexistent/directory/file.csv")

		assert.Error(t, err)
	})
}

func TestDataFrame_ToTSV(t *testing.T) {
	t.Parallel()

	t.Run("writes DataFrame to TSV file", func(t *testing.T) {
		t.Parallel()

		df := NewDataFrameFromRecords([]map[string]any{
			{"name": "Alice", "age": int64(30)},
		})

		tmpDir := t.TempDir()
		path := filepath.Join(tmpDir, "output.tsv")

		err := df.ToTSV(path)

		require.NoError(t, err)

		// Read back and verify
		f, err := os.Open(path) //nolint:gosec // test file path
		require.NoError(t, err)
		defer f.Close()

		readDf, err := NewDataFrame(f, TSV)
		require.NoError(t, err)

		assert.Equal(t, df.Len(), readDf.Len())
	})
}

func TestDataFrame_ToRecords(t *testing.T) {
	t.Parallel()

	t.Run("returns copy of records", func(t *testing.T) {
		t.Parallel()

		df := NewDataFrameFromRecords([]map[string]any{
			{"name": "Alice"},
		})

		records := df.ToRecords()

		// Modify returned records
		records[0]["name"] = "Modified"

		// Original should be unchanged
		newRecords := df.ToRecords()
		assert.Equal(t, "Alice", newRecords[0]["name"])
	})
}

func TestConvertStringValue(t *testing.T) {
	t.Parallel()

	t.Run("converts valid integer string to int64", func(t *testing.T) {
		t.Parallel()

		// This is tested indirectly through NewDataFrame, but we test edge cases here
		input := "name,age\nAlice,30"
		df, err := NewDataFrame(strings.NewReader(input), CSV)
		require.NoError(t, err)
		records := df.ToRecords()
		assert.Equal(t, int64(30), records[0]["age"])
	})

	t.Run("returns original string for invalid integer", func(t *testing.T) {
		t.Parallel()

		// When a column is typed as integer but contains non-integer value
		// Note: This scenario is hard to trigger directly since parser determines types
		// but we verify the behavior through mixed-type columns
		input := "value\n123\nabc\n456"
		df, err := NewDataFrame(strings.NewReader(input), CSV)
		require.NoError(t, err)
		records := df.ToRecords()

		// Parser will detect this as TEXT column due to "abc"
		assert.Equal(t, "123", records[0]["value"])
		assert.Equal(t, "abc", records[1]["value"])
		assert.Equal(t, "456", records[2]["value"])
	})

	t.Run("returns empty string as-is for integer column", func(t *testing.T) {
		t.Parallel()

		input := "id,value\n1,100\n2,\n3,200"
		df, err := NewDataFrame(strings.NewReader(input), CSV)
		require.NoError(t, err)
		records := df.ToRecords()

		assert.Equal(t, int64(100), records[0]["value"])
		assert.Equal(t, "", records[1]["value"]) // Empty string preserved
		assert.Equal(t, int64(200), records[2]["value"])
	})

	t.Run("converts valid float string to float64", func(t *testing.T) {
		t.Parallel()

		input := "value\n3.14159"
		df, err := NewDataFrame(strings.NewReader(input), CSV)
		require.NoError(t, err)
		records := df.ToRecords()
		assert.InDelta(t, 3.14159, records[0]["value"], 0.00001)
	})

	t.Run("returns empty string as-is for float column", func(t *testing.T) {
		t.Parallel()

		input := "id,value\n1,1.5\n2,\n3,2.5"
		df, err := NewDataFrame(strings.NewReader(input), CSV)
		require.NoError(t, err)
		records := df.ToRecords()

		assert.Equal(t, 1.5, records[0]["value"])
		assert.Equal(t, "", records[1]["value"]) // Empty string preserved
		assert.Equal(t, 2.5, records[2]["value"])
	})

	t.Run("preserves string values for text columns", func(t *testing.T) {
		t.Parallel()

		input := "name\nAlice\nBob\n"
		df, err := NewDataFrame(strings.NewReader(input), CSV)
		require.NoError(t, err)
		records := df.ToRecords()

		assert.Equal(t, "Alice", records[0]["name"])
		assert.Equal(t, "Bob", records[1]["name"])
	})
}

func TestCopyRow(t *testing.T) {
	t.Parallel()

	t.Run("creates independent copy", func(t *testing.T) {
		t.Parallel()

		original := map[string]any{"key": "value"}
		copied := copyRow(original)

		copied["key"] = "modified"

		assert.Equal(t, "value", original["key"])
		assert.Equal(t, "modified", copied["key"])
	})

	t.Run("copies all keys", func(t *testing.T) {
		t.Parallel()

		original := map[string]any{"a": 1, "b": 2, "c": 3}
		copied := copyRow(original)

		assert.Equal(t, len(original), len(copied))
		for k, v := range original {
			assert.Equal(t, v, copied[k])
		}
	})
}

func TestNewDataFrameFromPath(t *testing.T) {
	t.Parallel()

	t.Run("reads CSV file", func(t *testing.T) {
		t.Parallel()

		df, err := NewDataFrameFromPath(filepath.Join("testdata", "sample.csv"))

		require.NoError(t, err)
		assert.Equal(t, []string{"id", "name", "age", "email"}, df.Columns())
		assert.Equal(t, 3, df.Len())

		records := df.ToRecords()
		assert.Equal(t, "John Doe", records[0]["name"])
		assert.Equal(t, int64(30), records[0]["age"])
	})

	t.Run("reads CSV.GZ file (gzip compressed)", func(t *testing.T) {
		t.Parallel()

		df, err := NewDataFrameFromPath(filepath.Join("testdata", "sample.csv.gz"))

		require.NoError(t, err)
		assert.Equal(t, []string{"id", "name", "age", "email"}, df.Columns())
		assert.Equal(t, 3, df.Len())

		records := df.ToRecords()
		assert.Equal(t, "John Doe", records[0]["name"])
	})

	t.Run("reads CSV.ZST file (zstd compressed)", func(t *testing.T) {
		t.Parallel()

		df, err := NewDataFrameFromPath(filepath.Join("testdata", "users.csv.zst"))

		require.NoError(t, err)
		assert.Equal(t, []string{"id", "name", "role"}, df.Columns())
		assert.Equal(t, 3, df.Len())

		records := df.ToRecords()
		assert.Equal(t, "Alice", records[0]["name"])
		assert.Equal(t, "admin", records[0]["role"])
	})

	t.Run("reads TSV file", func(t *testing.T) {
		t.Parallel()

		df, err := NewDataFrameFromPath(filepath.Join("testdata", "products.tsv"))

		require.NoError(t, err)
		assert.Equal(t, []string{"id", "name", "price"}, df.Columns())
		assert.Equal(t, 3, df.Len())

		records := df.ToRecords()
		assert.Equal(t, "Laptop", records[0]["name"])
		assert.Equal(t, int64(1000), records[0]["price"])
	})

	t.Run("reads TSV.BZ2 file (bzip2 compressed)", func(t *testing.T) {
		t.Parallel()

		df, err := NewDataFrameFromPath(filepath.Join("testdata", "products.tsv.bz2"))

		require.NoError(t, err)
		assert.Equal(t, []string{"id", "name", "price"}, df.Columns())
		assert.Equal(t, 3, df.Len())

		records := df.ToRecords()
		assert.Equal(t, "Laptop", records[0]["name"])
	})

	t.Run("reads LTSV file", func(t *testing.T) {
		t.Parallel()

		df, err := NewDataFrameFromPath(filepath.Join("testdata", "logs.ltsv"))

		require.NoError(t, err)
		assert.Equal(t, 3, df.Len())
		assert.Contains(t, df.Columns(), "time")
		assert.Contains(t, df.Columns(), "level")
		assert.Contains(t, df.Columns(), "message")

		records := df.ToRecords()
		assert.Equal(t, "INFO", records[0]["level"])
		assert.Equal(t, "Application started", records[0]["message"])
	})

	t.Run("reads LTSV.XZ file (xz compressed)", func(t *testing.T) {
		t.Parallel()

		df, err := NewDataFrameFromPath(filepath.Join("testdata", "logs.ltsv.xz"))

		require.NoError(t, err)
		assert.Equal(t, 3, df.Len())
		assert.Contains(t, df.Columns(), "time")
		assert.Contains(t, df.Columns(), "level")
		assert.Contains(t, df.Columns(), "message")
	})

	t.Run("reads XLSX file", func(t *testing.T) {
		t.Parallel()

		df, err := NewDataFrameFromPath(filepath.Join("testdata", "excel", "sample.xlsx"))

		require.NoError(t, err)
		assert.Greater(t, df.Len(), 0)
		assert.Greater(t, len(df.Columns()), 0)
	})

	t.Run("reads Parquet file", func(t *testing.T) {
		t.Parallel()

		df, err := NewDataFrameFromPath(filepath.Join("testdata", "products.parquet"))

		require.NoError(t, err)
		assert.Equal(t, 3, df.Len())
		assert.Contains(t, df.Columns(), "id")
		assert.Contains(t, df.Columns(), "name")
		assert.Contains(t, df.Columns(), "price")

		records := df.ToRecords()
		assert.Equal(t, "Laptop", records[0]["name"])
		assert.Equal(t, int64(1), records[0]["id"])
	})

	t.Run("returns error for non-existent file", func(t *testing.T) {
		t.Parallel()

		_, err := NewDataFrameFromPath(filepath.Join("testdata", "nonexistent.csv"))

		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to open file")
	})

	t.Run("returns error for unsupported extension", func(t *testing.T) {
		t.Parallel()

		// Create a temporary file with unsupported extension
		tmpDir := t.TempDir()
		jsonPath := filepath.Join(tmpDir, "data.json")
		err := os.WriteFile(jsonPath, []byte(`{"key": "value"}`), 0o600)
		require.NoError(t, err)

		_, err = NewDataFrameFromPath(jsonPath)

		require.Error(t, err)
		assert.Contains(t, err.Error(), "unsupported file type")
	})
}

// TestNewDataFrameFromPathAllFormats tests all supported file formats with real files.
// This ensures that fileframe can correctly read data from various file formats
// that are commonly used in data analysis workflows.
func TestNewDataFrameFromPathAllFormats(t *testing.T) {
	t.Parallel()

	t.Run("CSV format: reads uncompressed CSV correctly", func(t *testing.T) {
		t.Parallel()

		df, err := NewDataFrameFromPath(filepath.Join("testdata", "sample.csv"))

		require.NoError(t, err)
		assert.Equal(t, 4, len(df.Columns()), "CSV should have 4 columns")
		assert.Equal(t, 3, df.Len(), "CSV should have 3 data rows")

		// Verify data integrity
		records := df.ToRecords()
		assert.Equal(t, int64(1), records[0]["id"])
		assert.Equal(t, "John Doe", records[0]["name"])
		assert.Equal(t, int64(30), records[0]["age"])
		assert.Equal(t, "john@example.com", records[0]["email"])
	})

	t.Run("CSV.GZ format: reads gzip compressed CSV correctly", func(t *testing.T) {
		t.Parallel()

		df, err := NewDataFrameFromPath(filepath.Join("testdata", "sample.csv.gz"))

		require.NoError(t, err)
		assert.Equal(t, 4, len(df.Columns()), "CSV.GZ should have 4 columns")
		assert.Equal(t, 3, df.Len(), "CSV.GZ should have 3 data rows")

		// Verify data matches uncompressed version
		records := df.ToRecords()
		assert.Equal(t, int64(1), records[0]["id"])
		assert.Equal(t, "John Doe", records[0]["name"])
	})

	t.Run("CSV.ZST format: reads zstd compressed CSV correctly", func(t *testing.T) {
		t.Parallel()

		df, err := NewDataFrameFromPath(filepath.Join("testdata", "users.csv.zst"))

		require.NoError(t, err)
		assert.Equal(t, 3, len(df.Columns()), "CSV.ZST should have 3 columns")
		assert.Equal(t, 3, df.Len(), "CSV.ZST should have 3 data rows")

		// Verify data integrity
		records := df.ToRecords()
		assert.Equal(t, int64(1), records[0]["id"])
		assert.Equal(t, "Alice", records[0]["name"])
		assert.Equal(t, "admin", records[0]["role"])
	})

	t.Run("TSV format: reads uncompressed TSV correctly", func(t *testing.T) {
		t.Parallel()

		df, err := NewDataFrameFromPath(filepath.Join("testdata", "products.tsv"))

		require.NoError(t, err)
		assert.Equal(t, 3, len(df.Columns()), "TSV should have 3 columns")
		assert.Equal(t, 3, df.Len(), "TSV should have 3 data rows")

		// Verify data integrity
		records := df.ToRecords()
		assert.Equal(t, int64(1), records[0]["id"])
		assert.Equal(t, "Laptop", records[0]["name"])
		assert.Equal(t, int64(1000), records[0]["price"])
	})

	t.Run("TSV.BZ2 format: reads bzip2 compressed TSV correctly", func(t *testing.T) {
		t.Parallel()

		df, err := NewDataFrameFromPath(filepath.Join("testdata", "products.tsv.bz2"))

		require.NoError(t, err)
		assert.Equal(t, 3, len(df.Columns()), "TSV.BZ2 should have 3 columns")
		assert.Equal(t, 3, df.Len(), "TSV.BZ2 should have 3 data rows")

		// Verify data matches uncompressed version
		records := df.ToRecords()
		assert.Equal(t, int64(1), records[0]["id"])
		assert.Equal(t, "Laptop", records[0]["name"])
		assert.Equal(t, int64(1000), records[0]["price"])
	})

	t.Run("LTSV format: reads uncompressed LTSV correctly", func(t *testing.T) {
		t.Parallel()

		df, err := NewDataFrameFromPath(filepath.Join("testdata", "logs.ltsv"))

		require.NoError(t, err)
		assert.Equal(t, 3, len(df.Columns()), "LTSV should have 3 columns")
		assert.Equal(t, 3, df.Len(), "LTSV should have 3 data rows")

		// Verify LTSV-specific column names
		columns := df.Columns()
		assert.Contains(t, columns, "time")
		assert.Contains(t, columns, "level")
		assert.Contains(t, columns, "message")

		// Verify data integrity
		records := df.ToRecords()
		assert.Equal(t, "2024-01-01T10:00:00Z", records[0]["time"])
		assert.Equal(t, "INFO", records[0]["level"])
		assert.Equal(t, "Application started", records[0]["message"])
	})

	t.Run("LTSV.XZ format: reads xz compressed LTSV correctly", func(t *testing.T) {
		t.Parallel()

		df, err := NewDataFrameFromPath(filepath.Join("testdata", "logs.ltsv.xz"))

		require.NoError(t, err)
		assert.Equal(t, 3, len(df.Columns()), "LTSV.XZ should have 3 columns")
		assert.Equal(t, 3, df.Len(), "LTSV.XZ should have 3 data rows")

		// Verify data matches uncompressed version
		records := df.ToRecords()
		assert.Equal(t, "2024-01-01T10:00:00Z", records[0]["time"])
		assert.Equal(t, "INFO", records[0]["level"])
	})

	t.Run("XLSX format: reads Excel file correctly", func(t *testing.T) {
		t.Parallel()

		df, err := NewDataFrameFromPath(filepath.Join("testdata", "excel", "sample.xlsx"))

		require.NoError(t, err)
		assert.Greater(t, len(df.Columns()), 0, "XLSX should have columns")
		assert.Greater(t, df.Len(), 0, "XLSX should have data rows")
	})

	t.Run("Parquet format: reads Parquet file correctly", func(t *testing.T) {
		t.Parallel()

		df, err := NewDataFrameFromPath(filepath.Join("testdata", "products.parquet"))

		require.NoError(t, err)
		assert.Equal(t, 3, len(df.Columns()), "Parquet should have 3 columns")
		assert.Equal(t, 3, df.Len(), "Parquet should have 3 data rows")

		// Verify data integrity
		records := df.ToRecords()
		assert.Equal(t, int64(1), records[0]["id"])
		assert.Equal(t, "Laptop", records[0]["name"])
		// Parquet stores price as float64
		assert.InDelta(t, 999.99, records[0]["price"], 0.01)
	})
}

// TestNewDataFrameFromPathCompressionFormats specifically tests that compressed
// files produce the same data as their uncompressed counterparts.
func TestNewDataFrameFromPathCompressionFormats(t *testing.T) {
	t.Parallel()

	t.Run("CSV and CSV.GZ produce identical data", func(t *testing.T) {
		t.Parallel()

		dfUncompressed, err := NewDataFrameFromPath(filepath.Join("testdata", "sample.csv"))
		require.NoError(t, err)

		dfCompressed, err := NewDataFrameFromPath(filepath.Join("testdata", "sample.csv.gz"))
		require.NoError(t, err)

		assert.Equal(t, dfUncompressed.Columns(), dfCompressed.Columns())
		assert.Equal(t, dfUncompressed.Len(), dfCompressed.Len())
		assert.Equal(t, dfUncompressed.ToRecords(), dfCompressed.ToRecords())
	})

	t.Run("TSV and TSV.BZ2 produce identical data", func(t *testing.T) {
		t.Parallel()

		dfUncompressed, err := NewDataFrameFromPath(filepath.Join("testdata", "products.tsv"))
		require.NoError(t, err)

		dfCompressed, err := NewDataFrameFromPath(filepath.Join("testdata", "products.tsv.bz2"))
		require.NoError(t, err)

		assert.Equal(t, dfUncompressed.Columns(), dfCompressed.Columns())
		assert.Equal(t, dfUncompressed.Len(), dfCompressed.Len())
		assert.Equal(t, dfUncompressed.ToRecords(), dfCompressed.ToRecords())
	})

	t.Run("LTSV and LTSV.XZ produce identical data", func(t *testing.T) {
		t.Parallel()

		dfUncompressed, err := NewDataFrameFromPath(filepath.Join("testdata", "logs.ltsv"))
		require.NoError(t, err)

		dfCompressed, err := NewDataFrameFromPath(filepath.Join("testdata", "logs.ltsv.xz"))
		require.NoError(t, err)

		assert.Equal(t, dfUncompressed.Columns(), dfCompressed.Columns())
		assert.Equal(t, dfUncompressed.Len(), dfCompressed.Len())
		assert.Equal(t, dfUncompressed.ToRecords(), dfCompressed.ToRecords())
	})
}

// TestNewDataFrameFromPathWithOperations tests that DataFrame operations work correctly
// with data loaded from real files of various formats.
func TestNewDataFrameFromPathWithOperations(t *testing.T) {
	t.Parallel()

	t.Run("Filter on CSV data works correctly", func(t *testing.T) {
		t.Parallel()

		df, err := NewDataFrameFromPath(filepath.Join("testdata", "sample.csv"))
		require.NoError(t, err)

		filtered := df.Filter(func(row map[string]any) bool {
			age, ok := row["age"].(int64)
			return ok && age >= 30
		})

		assert.Equal(t, 2, filtered.Len(), "Should filter to 2 rows (age >= 30)")
	})

	t.Run("Select on TSV data works correctly", func(t *testing.T) {
		t.Parallel()

		df, err := NewDataFrameFromPath(filepath.Join("testdata", "products.tsv"))
		require.NoError(t, err)

		selected, err := df.Select("name", "price")
		require.NoError(t, err)

		assert.Equal(t, []string{"name", "price"}, selected.Columns())
		assert.Equal(t, 3, selected.Len())
	})

	t.Run("Mutate on LTSV data works correctly", func(t *testing.T) {
		t.Parallel()

		df, err := NewDataFrameFromPath(filepath.Join("testdata", "logs.ltsv"))
		require.NoError(t, err)

		mutated := df.Mutate("is_error", func(row map[string]any) any {
			level, _ := row["level"].(string) //nolint:errcheck // test code
			return level == "ERROR"
		})

		assert.Contains(t, mutated.Columns(), "is_error")
		records := mutated.ToRecords()
		assert.Equal(t, false, records[0]["is_error"])
		assert.Equal(t, true, records[1]["is_error"])
		assert.Equal(t, false, records[2]["is_error"])
	})

	t.Run("GroupBy on Parquet data works correctly", func(t *testing.T) {
		t.Parallel()

		df, err := NewDataFrameFromPath(filepath.Join("testdata", "products.parquet"))
		require.NoError(t, err)

		grouped, err := df.GroupBy("name")
		require.NoError(t, err)

		result := grouped.Count()
		assert.Equal(t, 3, result.Len(), "Should have 3 unique product names")
	})

	t.Run("chained operations on compressed CSV work correctly", func(t *testing.T) {
		t.Parallel()

		df, err := NewDataFrameFromPath(filepath.Join("testdata", "sample.csv.gz"))
		require.NoError(t, err)

		selected, err := df.Select("name", "age")
		require.NoError(t, err)
		result := selected.
			Filter(func(row map[string]any) bool {
				age, ok := row["age"].(int64)
				return ok && age >= 25
			}).
			Mutate("category", func(row map[string]any) any {
				age, ok := row["age"].(int64)
				if !ok {
					return "unknown"
				}
				if age >= 30 {
					return "senior"
				}
				return "junior"
			})

		assert.Equal(t, 3, result.Len())
		assert.Contains(t, result.Columns(), "category")
	})
}

// TestDataFrameCombinedOperations tests that multiple DataFrame operations work correctly together.
// This is important because the underlying data comes from SQL queries, and chaining operations
// should not cause unexpected behavior.
func TestDataFrameCombinedOperations(t *testing.T) {
	t.Parallel()

	t.Run("Select then Filter works correctly", func(t *testing.T) {
		t.Parallel()

		input := "name,age,city\nAlice,30,Tokyo\nBob,25,Osaka\nCharlie,35,Tokyo"
		reader := strings.NewReader(input)

		df, err := NewDataFrame(reader, CSV)
		require.NoError(t, err)

		// Select only name and age, then filter by age
		selected, err := df.Select("name", "age")
		require.NoError(t, err)
		result := selected.Filter(func(row map[string]any) bool {
			age, ok := row["age"].(int64)
			return ok && age >= 30
		})

		assert.Equal(t, 2, result.Len())
		assert.Equal(t, []string{"name", "age"}, result.Columns())

		records := result.ToRecords()
		names := make([]string, len(records))
		for i, r := range records {
			if name, ok := r["name"].(string); ok {
				names[i] = name
			}
		}
		assert.Contains(t, names, "Alice")
		assert.Contains(t, names, "Charlie")
	})

	t.Run("Filter then Mutate works correctly", func(t *testing.T) {
		t.Parallel()

		input := "name,age\nAlice,30\nBob,25\nCharlie,35"
		reader := strings.NewReader(input)

		df, err := NewDataFrame(reader, CSV)
		require.NoError(t, err)

		// Filter by age, then add a new column
		result := df.Filter(func(row map[string]any) bool {
			age, ok := row["age"].(int64)
			return ok && age >= 30
		}).Mutate("status", func(_ map[string]any) any {
			return "senior"
		})

		assert.Equal(t, 2, result.Len())
		assert.Contains(t, result.Columns(), "status")

		records := result.ToRecords()
		for _, r := range records {
			assert.Equal(t, "senior", r["status"])
		}
	})

	t.Run("Mutate then Select works correctly", func(t *testing.T) {
		t.Parallel()

		input := "first,last\nAlice,Smith\nBob,Jones"
		reader := strings.NewReader(input)

		df, err := NewDataFrame(reader, CSV)
		require.NoError(t, err)

		// Add full_name column, then select only that column
		mutated := df.Mutate("full_name", func(row map[string]any) any {
			first, ok1 := row["first"].(string)
			last, ok2 := row["last"].(string)
			if !ok1 || !ok2 {
				return ""
			}
			return first + " " + last
		})
		result, err := mutated.Select("full_name")
		require.NoError(t, err)

		assert.Equal(t, 2, result.Len())
		assert.Equal(t, []string{"full_name"}, result.Columns())

		records := result.ToRecords()
		assert.Equal(t, "Alice Smith", records[0]["full_name"])
		assert.Equal(t, "Bob Jones", records[1]["full_name"])
	})

	t.Run("chained Filter operations work correctly", func(t *testing.T) {
		t.Parallel()

		input := "name,age,city\nAlice,30,Tokyo\nBob,25,Osaka\nCharlie,35,Tokyo\nDiana,28,Tokyo"
		reader := strings.NewReader(input)

		df, err := NewDataFrame(reader, CSV)
		require.NoError(t, err)

		// Filter by city, then filter by age
		result := df.Filter(func(row map[string]any) bool {
			return row["city"] == "Tokyo"
		}).Filter(func(row map[string]any) bool {
			age, ok := row["age"].(int64)
			return ok && age >= 30
		})

		assert.Equal(t, 2, result.Len())
		records := result.ToRecords()
		names := make([]string, len(records))
		for i, r := range records {
			if name, ok := r["name"].(string); ok {
				names[i] = name
			}
		}
		assert.Contains(t, names, "Alice")
		assert.Contains(t, names, "Charlie")
	})

	t.Run("chained Mutate operations work correctly", func(t *testing.T) {
		t.Parallel()

		input := "value\n10\n20\n30"
		reader := strings.NewReader(input)

		df, err := NewDataFrame(reader, CSV)
		require.NoError(t, err)

		// Add two computed columns
		result := df.Mutate("doubled", func(row map[string]any) any {
			v, ok := row["value"].(int64)
			if !ok {
				return int64(0)
			}
			return v * 2
		}).Mutate("tripled", func(row map[string]any) any {
			v, ok := row["value"].(int64)
			if !ok {
				return int64(0)
			}
			return v * 3
		})

		assert.Equal(t, 3, result.Len())
		assert.Contains(t, result.Columns(), "doubled")
		assert.Contains(t, result.Columns(), "tripled")

		records := result.ToRecords()
		assert.Equal(t, int64(20), records[0]["doubled"])
		assert.Equal(t, int64(30), records[0]["tripled"])
	})

	t.Run("complex chain: Select -> Filter -> Mutate -> Filter", func(t *testing.T) {
		t.Parallel()

		input := "name,age,city,salary\nAlice,30,Tokyo,50000\nBob,25,Osaka,40000\nCharlie,35,Tokyo,60000\nDiana,28,Tokyo,45000"
		reader := strings.NewReader(input)

		df, err := NewDataFrame(reader, CSV)
		require.NoError(t, err)

		selected, err := df.Select("name", "age", "salary")
		require.NoError(t, err)
		result := selected.
			Filter(func(row map[string]any) bool {
				age, ok := row["age"].(int64)
				return ok && age >= 28
			}).
			Mutate("bonus", func(row map[string]any) any {
				salary, ok := row["salary"].(int64)
				if !ok {
					return int64(0)
				}
				return salary / 10
			}).
			Filter(func(row map[string]any) bool {
				bonus, ok := row["bonus"].(int64)
				return ok && bonus >= 5000
			})

		assert.Equal(t, 2, result.Len())
		records := result.ToRecords()
		names := make([]string, len(records))
		for i, r := range records {
			if name, ok := r["name"].(string); ok {
				names[i] = name
			}
		}
		assert.Contains(t, names, "Alice")
		assert.Contains(t, names, "Charlie")
	})

	t.Run("operations do not modify original DataFrame", func(t *testing.T) {
		t.Parallel()

		input := "name,age\nAlice,30\nBob,25"
		reader := strings.NewReader(input)

		df, err := NewDataFrame(reader, CSV)
		require.NoError(t, err)

		originalLen := df.Len()
		originalCols := df.Columns()

		// Perform various operations
		_, err = df.Select("name")
		require.NoError(t, err)
		_ = df.Filter(func(_ map[string]any) bool { return false })
		_ = df.Mutate("new_col", func(_ map[string]any) any { return "value" })

		// Original should be unchanged
		assert.Equal(t, originalLen, df.Len())
		assert.Equal(t, originalCols, df.Columns())
	})

	t.Run("GroupBy then aggregation with chained operations", func(t *testing.T) {
		t.Parallel()

		input := "name,department,salary\nAlice,Engineering,50000\nBob,Engineering,60000\nCharlie,Sales,40000\nDiana,Sales,45000"
		reader := strings.NewReader(input)

		df, err := NewDataFrame(reader, CSV)
		require.NoError(t, err)

		// Filter first, then group and aggregate
		filtered := df.Filter(func(row map[string]any) bool {
			salary, ok := row["salary"].(int64)
			return ok && salary >= 45000
		})
		grouped, err := filtered.GroupBy("department")
		require.NoError(t, err)
		result, err := grouped.Sum("salary")
		require.NoError(t, err)

		assert.Equal(t, 2, result.Len())
		records := result.ToRecords()

		// Find Engineering and Sales sums (column is "sum_salary")
		var engSum, salesSum float64
		for _, r := range records {
			dept, ok1 := r["department"].(string)
			sum, ok2 := r["sum_salary"].(float64)
			if !ok1 || !ok2 {
				continue
			}
			switch dept {
			case "Engineering":
				engSum = sum
			case "Sales":
				salesSum = sum
			}
		}
		assert.Equal(t, float64(110000), engSum)  // Alice (50000) + Bob (60000)
		assert.Equal(t, float64(45000), salesSum) // Only Diana (45000) passes filter
	})
}

// TestDataFrame_Join tests the Join functionality for combining two DataFrames.
func TestDataFrame_Join(t *testing.T) {
	t.Parallel()

	t.Run("inner join with matching rows", func(t *testing.T) {
		t.Parallel()

		users := NewDataFrameFromRecords([]map[string]any{
			{"id": int64(1), "name": "Alice"},
			{"id": int64(2), "name": "Bob"},
			{"id": int64(3), "name": "Charlie"},
		})
		orders := NewDataFrameFromRecords([]map[string]any{
			{"user_id": int64(1), "product": "Laptop"},
			{"user_id": int64(1), "product": "Mouse"},
			{"user_id": int64(2), "product": "Keyboard"},
		})

		result, err := users.Join(orders, JoinOption{
			On:  []string{"id", "user_id"},
			How: InnerJoin,
		})

		require.NoError(t, err)
		assert.Equal(t, 3, result.Len())
		assert.Contains(t, result.Columns(), "id")
		assert.Contains(t, result.Columns(), "name")
		assert.Contains(t, result.Columns(), "product")

		records := result.ToRecords()
		// Alice has 2 orders
		aliceOrders := 0
		for _, r := range records {
			if r["name"] == "Alice" {
				aliceOrders++
			}
		}
		assert.Equal(t, 2, aliceOrders)
	})

	t.Run("inner join with same column name", func(t *testing.T) {
		t.Parallel()

		df1 := NewDataFrameFromRecords([]map[string]any{
			{"id": int64(1), "value": "A"},
			{"id": int64(2), "value": "B"},
		})
		df2 := NewDataFrameFromRecords([]map[string]any{
			{"id": int64(1), "score": int64(100)},
			{"id": int64(2), "score": int64(200)},
		})

		result, err := df1.Join(df2, JoinOption{
			On:  []string{"id"},
			How: InnerJoin,
		})

		require.NoError(t, err)
		assert.Equal(t, 2, result.Len())
		assert.Equal(t, []string{"id", "value", "score"}, result.Columns())
	})

	t.Run("left join includes unmatched left rows", func(t *testing.T) {
		t.Parallel()

		users := NewDataFrameFromRecords([]map[string]any{
			{"id": int64(1), "name": "Alice"},
			{"id": int64(2), "name": "Bob"},
			{"id": int64(3), "name": "Charlie"},
		})
		orders := NewDataFrameFromRecords([]map[string]any{
			{"user_id": int64(1), "product": "Laptop"},
		})

		result, err := users.Join(orders, JoinOption{
			On:  []string{"id", "user_id"},
			How: LeftJoin,
		})

		require.NoError(t, err)
		assert.Equal(t, 3, result.Len())

		records := result.ToRecords()
		// Charlie (id=3) should have nil product
		for _, r := range records {
			if r["name"] == "Charlie" {
				assert.Nil(t, r["product"])
			}
		}
	})

	t.Run("right join includes unmatched right rows", func(t *testing.T) {
		t.Parallel()

		users := NewDataFrameFromRecords([]map[string]any{
			{"id": int64(1), "name": "Alice"},
		})
		orders := NewDataFrameFromRecords([]map[string]any{
			{"user_id": int64(1), "product": "Laptop"},
			{"user_id": int64(2), "product": "Mouse"},
		})

		result, err := users.Join(orders, JoinOption{
			On:  []string{"id", "user_id"},
			How: RightJoin,
		})

		require.NoError(t, err)
		assert.Equal(t, 2, result.Len())

		records := result.ToRecords()
		// Mouse order (user_id=2) should have nil name
		for _, r := range records {
			if r["product"] == "Mouse" {
				assert.Nil(t, r["name"])
				assert.Equal(t, int64(2), r["id"])
			}
		}
	})

	t.Run("outer join includes all rows from both DataFrames", func(t *testing.T) {
		t.Parallel()

		df1 := NewDataFrameFromRecords([]map[string]any{
			{"id": int64(1), "left_val": "A"},
			{"id": int64(2), "left_val": "B"},
		})
		df2 := NewDataFrameFromRecords([]map[string]any{
			{"id": int64(2), "right_val": "X"},
			{"id": int64(3), "right_val": "Y"},
		})

		result, err := df1.Join(df2, JoinOption{
			On:  []string{"id"},
			How: OuterJoin,
		})

		require.NoError(t, err)
		assert.Equal(t, 3, result.Len())

		records := result.ToRecords()
		// id=1 should have nil right_val
		// id=2 should have both values
		// id=3 should have nil left_val
		idValues := make(map[int64]map[string]any)
		for _, r := range records {
			id, _ := r["id"].(int64) //nolint:errcheck // test code
			idValues[id] = r
		}

		assert.Equal(t, "A", idValues[1]["left_val"])
		assert.Nil(t, idValues[1]["right_val"])
		assert.Equal(t, "B", idValues[2]["left_val"])
		assert.Equal(t, "X", idValues[2]["right_val"])
		assert.Nil(t, idValues[3]["left_val"])
		assert.Equal(t, "Y", idValues[3]["right_val"])
	})

	t.Run("handles column name conflicts with right_ prefix", func(t *testing.T) {
		t.Parallel()

		df1 := NewDataFrameFromRecords([]map[string]any{
			{"id": int64(1), "name": "Alice"},
		})
		df2 := NewDataFrameFromRecords([]map[string]any{
			{"id": int64(1), "name": "User1"},
		})

		result, err := df1.Join(df2, JoinOption{
			On:  []string{"id"},
			How: InnerJoin,
		})

		require.NoError(t, err)
		assert.Contains(t, result.Columns(), "name")
		assert.Contains(t, result.Columns(), "right_name")

		records := result.ToRecords()
		assert.Equal(t, "Alice", records[0]["name"])
		assert.Equal(t, "User1", records[0]["right_name"])
	})

	t.Run("returns error for nil other DataFrame", func(t *testing.T) {
		t.Parallel()

		df := NewDataFrameFromRecords([]map[string]any{{"id": 1}})

		_, err := df.Join(nil, JoinOption{On: []string{"id"}})

		require.Error(t, err)
		assert.Contains(t, err.Error(), "nil")
	})

	t.Run("returns error for empty On columns", func(t *testing.T) {
		t.Parallel()

		df1 := NewDataFrameFromRecords([]map[string]any{{"id": 1}})
		df2 := NewDataFrameFromRecords([]map[string]any{{"id": 1}})

		_, err := df1.Join(df2, JoinOption{On: []string{}})

		require.Error(t, err)
		assert.Contains(t, err.Error(), "at least one column")
	})

	t.Run("returns error for more than 2 On columns", func(t *testing.T) {
		t.Parallel()

		df1 := NewDataFrameFromRecords([]map[string]any{{"a": 1, "b": 2, "c": 3}})
		df2 := NewDataFrameFromRecords([]map[string]any{{"a": 1, "b": 2, "c": 3}})

		_, err := df1.Join(df2, JoinOption{On: []string{"a", "b", "c"}})

		require.Error(t, err)
		assert.Contains(t, err.Error(), "not supported")
	})

	t.Run("returns error for non-existent left column", func(t *testing.T) {
		t.Parallel()

		df1 := NewDataFrameFromRecords([]map[string]any{{"id": 1}})
		df2 := NewDataFrameFromRecords([]map[string]any{{"id": 1}})

		_, err := df1.Join(df2, JoinOption{On: []string{"nonexistent"}})

		require.Error(t, err)
		assert.Contains(t, err.Error(), "not found in left")
	})

	t.Run("returns error for non-existent right column", func(t *testing.T) {
		t.Parallel()

		df1 := NewDataFrameFromRecords([]map[string]any{{"id": 1}})
		df2 := NewDataFrameFromRecords([]map[string]any{{"other": 1}})

		_, err := df1.Join(df2, JoinOption{On: []string{"id", "nonexistent"}})

		require.Error(t, err)
		assert.Contains(t, err.Error(), "not found in right")
	})

	t.Run("join does not modify original DataFrames", func(t *testing.T) {
		t.Parallel()

		df1 := NewDataFrameFromRecords([]map[string]any{{"id": int64(1), "name": "Alice"}})
		df2 := NewDataFrameFromRecords([]map[string]any{{"id": int64(1), "score": int64(100)}})

		originalLen1 := df1.Len()
		originalLen2 := df2.Len()
		originalCols1 := df1.Columns()
		originalCols2 := df2.Columns()

		_, err := df1.Join(df2, JoinOption{On: []string{"id"}, How: InnerJoin})
		require.NoError(t, err)

		assert.Equal(t, originalLen1, df1.Len())
		assert.Equal(t, originalLen2, df2.Len())
		assert.Equal(t, originalCols1, df1.Columns())
		assert.Equal(t, originalCols2, df2.Columns())
	})
}

// TestDataFrame_Concat tests the Concat functionality for combining DataFrames vertically.
func TestDataFrame_Concat(t *testing.T) {
	t.Parallel()

	t.Run("concatenates two DataFrames with same columns", func(t *testing.T) {
		t.Parallel()

		df1 := NewDataFrameFromRecords([]map[string]any{
			{"name": "Alice", "age": int64(30)},
		})
		df2 := NewDataFrameFromRecords([]map[string]any{
			{"name": "Bob", "age": int64(25)},
		})

		result, err := df1.Concat(df2)

		require.NoError(t, err)
		assert.Equal(t, 2, result.Len())
		assert.Equal(t, df1.Columns(), result.Columns())

		records := result.ToRecords()
		assert.Equal(t, "Alice", records[0]["name"])
		assert.Equal(t, "Bob", records[1]["name"])
	})

	t.Run("concatenates multiple DataFrames", func(t *testing.T) {
		t.Parallel()

		df1 := NewDataFrameFromRecords([]map[string]any{{"id": int64(1)}})
		df2 := NewDataFrameFromRecords([]map[string]any{{"id": int64(2)}})
		df3 := NewDataFrameFromRecords([]map[string]any{{"id": int64(3)}})

		result, err := df1.Concat(df2, df3)

		require.NoError(t, err)
		assert.Equal(t, 3, result.Len())

		records := result.ToRecords()
		assert.Equal(t, int64(1), records[0]["id"])
		assert.Equal(t, int64(2), records[1]["id"])
		assert.Equal(t, int64(3), records[2]["id"])
	})

	t.Run("returns clone when no arguments provided", func(t *testing.T) {
		t.Parallel()

		df := NewDataFrameFromRecords([]map[string]any{{"id": int64(1)}})

		result, err := df.Concat()

		require.NoError(t, err)
		assert.Equal(t, df.Len(), result.Len())
		assert.Equal(t, df.Columns(), result.Columns())
	})

	t.Run("returns error for nil DataFrame in arguments", func(t *testing.T) {
		t.Parallel()

		df := NewDataFrameFromRecords([]map[string]any{{"id": 1}})

		_, err := df.Concat(nil)

		require.Error(t, err)
		assert.Contains(t, err.Error(), "nil")
	})

	t.Run("returns error for different columns", func(t *testing.T) {
		t.Parallel()

		df1 := NewDataFrameFromRecords([]map[string]any{{"name": "Alice"}})
		df2 := NewDataFrameFromRecords([]map[string]any{{"age": 30}})

		_, err := df1.Concat(df2)

		require.Error(t, err)
		assert.Contains(t, err.Error(), "different columns")
	})

	t.Run("concat does not modify original DataFrames", func(t *testing.T) {
		t.Parallel()

		df1 := NewDataFrameFromRecords([]map[string]any{{"id": int64(1)}})
		df2 := NewDataFrameFromRecords([]map[string]any{{"id": int64(2)}})

		originalLen1 := df1.Len()
		originalLen2 := df2.Len()

		_, err := df1.Concat(df2)
		require.NoError(t, err)

		assert.Equal(t, originalLen1, df1.Len())
		assert.Equal(t, originalLen2, df2.Len())
	})
}

// TestConcatAll tests the ConcatAll function for combining DataFrames with different columns.
func TestConcatAll(t *testing.T) {
	t.Parallel()

	t.Run("concatenates DataFrames with different columns", func(t *testing.T) {
		t.Parallel()

		df1 := NewDataFrameFromRecords([]map[string]any{
			{"name": "Alice", "age": int64(30)},
		})
		df2 := NewDataFrameFromRecords([]map[string]any{
			{"name": "Bob", "city": "Tokyo"},
		})

		result, err := ConcatAll(df1, df2)

		require.NoError(t, err)
		assert.Equal(t, 2, result.Len())
		// Columns are sorted alphabetically
		assert.Equal(t, []string{"age", "city", "name"}, result.Columns())

		records := result.ToRecords()
		// First row: has age, no city
		assert.Equal(t, int64(30), records[0]["age"])
		assert.Nil(t, records[0]["city"])
		assert.Equal(t, "Alice", records[0]["name"])
		// Second row: no age, has city
		assert.Nil(t, records[1]["age"])
		assert.Equal(t, "Tokyo", records[1]["city"])
		assert.Equal(t, "Bob", records[1]["name"])
	})

	t.Run("returns empty DataFrame for no arguments", func(t *testing.T) {
		t.Parallel()

		result, err := ConcatAll()

		require.NoError(t, err)
		assert.Equal(t, 0, result.Len())
		assert.Empty(t, result.Columns())
	})

	// A nil frame is almost always a constructor whose error was mishandled.
	// Skipping it returned a result quietly missing that data, while Concat
	// rejected the same nil.
	t.Run("rejects a nil DataFrame, as Concat does", func(t *testing.T) {
		t.Parallel()

		df := NewDataFrameFromRecords([]map[string]any{{"id": int64(1)}})

		_, err := ConcatAll(df, nil, df)

		require.Error(t, err)
		assert.Contains(t, err.Error(), "index 1")
	})

	t.Run("handles DataFrames with completely different columns", func(t *testing.T) {
		t.Parallel()

		df1 := NewDataFrameFromRecords([]map[string]any{{"a": 1}})
		df2 := NewDataFrameFromRecords([]map[string]any{{"b": 2}})
		df3 := NewDataFrameFromRecords([]map[string]any{{"c": 3}})

		result, err := ConcatAll(df1, df2, df3)

		require.NoError(t, err)
		assert.Equal(t, 3, result.Len())
		assert.Equal(t, []string{"a", "b", "c"}, result.Columns())

		records := result.ToRecords()
		// First row has only "a"
		assert.Equal(t, 1, records[0]["a"])
		assert.Nil(t, records[0]["b"])
		assert.Nil(t, records[0]["c"])
	})
}

// TestDataFrame_JoinWithRealFiles tests Join with data loaded from real files.
func TestDataFrame_JoinWithRealFiles(t *testing.T) {
	t.Parallel()

	t.Run("joins users from CSV with products from TSV", func(t *testing.T) {
		t.Parallel()

		users, err := NewDataFrameFromPath(filepath.Join("testdata", "sample.csv"))
		require.NoError(t, err)

		products, err := NewDataFrameFromPath(filepath.Join("testdata", "products.tsv"))
		require.NoError(t, err)

		// Join on id column
		result, err := users.Join(products, JoinOption{
			On:  []string{"id"},
			How: InnerJoin,
		})

		require.NoError(t, err)
		assert.Equal(t, 3, result.Len())
		assert.Contains(t, result.Columns(), "name")
		assert.Contains(t, result.Columns(), "right_name") // Conflict resolved
		assert.Contains(t, result.Columns(), "price")
	})

	t.Run("left join preserves all rows from left DataFrame", func(t *testing.T) {
		t.Parallel()

		// Create a DataFrame with more IDs than products
		users, err := NewDataFrameFromPath(filepath.Join("testdata", "sample.csv"))
		require.NoError(t, err)

		// Products only has 3 items
		products := NewDataFrameFromRecords([]map[string]any{
			{"user_id": int64(1), "product": "Item1"},
		})

		result, err := users.Join(products, JoinOption{
			On:  []string{"id", "user_id"},
			How: LeftJoin,
		})

		require.NoError(t, err)
		assert.Equal(t, users.Len(), result.Len())
	})
}

// TestDataFrame_ConcatWithRealFiles tests Concat with data loaded from real files.
func TestDataFrame_ConcatWithRealFiles(t *testing.T) {
	t.Parallel()

	t.Run("concatenates CSV and compressed CSV data", func(t *testing.T) {
		t.Parallel()

		df1, err := NewDataFrameFromPath(filepath.Join("testdata", "sample.csv"))
		require.NoError(t, err)

		df2, err := NewDataFrameFromPath(filepath.Join("testdata", "sample.csv.gz"))
		require.NoError(t, err)

		result, err := df1.Concat(df2)

		require.NoError(t, err)
		assert.Equal(t, df1.Len()+df2.Len(), result.Len())
		assert.Equal(t, df1.Columns(), result.Columns())
	})

	t.Run("ConcatAll combines files with different schemas", func(t *testing.T) {
		t.Parallel()

		csv, err := NewDataFrameFromPath(filepath.Join("testdata", "sample.csv"))
		require.NoError(t, err)

		tsv, err := NewDataFrameFromPath(filepath.Join("testdata", "products.tsv"))
		require.NoError(t, err)

		result, err := ConcatAll(csv, tsv)

		require.NoError(t, err)
		assert.Equal(t, csv.Len()+tsv.Len(), result.Len())
		// Should have union of all columns
		cols := result.Columns()
		assert.Contains(t, cols, "id")
		assert.Contains(t, cols, "name")
		assert.Contains(t, cols, "age")
		assert.Contains(t, cols, "email")
		assert.Contains(t, cols, "price")
	})
}

func TestDataFrame_Sort(t *testing.T) {
	t.Parallel()

	t.Run("sorts by string column ascending", func(t *testing.T) {
		t.Parallel()

		df := NewDataFrameFromRecords([]map[string]any{
			{"name": "Charlie", "age": int64(35)},
			{"name": "Alice", "age": int64(30)},
			{"name": "Bob", "age": int64(25)},
		})

		sorted, err := df.Sort("name", Ascending)

		require.NoError(t, err)
		records := sorted.ToRecords()
		assert.Equal(t, "Alice", records[0]["name"])
		assert.Equal(t, "Bob", records[1]["name"])
		assert.Equal(t, "Charlie", records[2]["name"])
	})

	t.Run("sorts by string column descending", func(t *testing.T) {
		t.Parallel()

		df := NewDataFrameFromRecords([]map[string]any{
			{"name": "Alice", "age": int64(30)},
			{"name": "Charlie", "age": int64(35)},
			{"name": "Bob", "age": int64(25)},
		})

		sorted, err := df.Sort("name", Descending)

		require.NoError(t, err)
		records := sorted.ToRecords()
		assert.Equal(t, "Charlie", records[0]["name"])
		assert.Equal(t, "Bob", records[1]["name"])
		assert.Equal(t, "Alice", records[2]["name"])
	})

	t.Run("sorts by int64 column ascending", func(t *testing.T) {
		t.Parallel()

		df := NewDataFrameFromRecords([]map[string]any{
			{"name": "Charlie", "age": int64(35)},
			{"name": "Alice", "age": int64(30)},
			{"name": "Bob", "age": int64(25)},
		})

		sorted, err := df.Sort("age", Ascending)

		require.NoError(t, err)
		records := sorted.ToRecords()
		assert.Equal(t, int64(25), records[0]["age"])
		assert.Equal(t, int64(30), records[1]["age"])
		assert.Equal(t, int64(35), records[2]["age"])
	})

	t.Run("sorts by float64 column descending", func(t *testing.T) {
		t.Parallel()

		df := NewDataFrameFromRecords([]map[string]any{
			{"name": "A", "score": 75.5},
			{"name": "B", "score": 92.3},
			{"name": "C", "score": 88.1},
		})

		sorted, err := df.Sort("score", Descending)

		require.NoError(t, err)
		records := sorted.ToRecords()
		assert.Equal(t, 92.3, records[0]["score"])
		assert.Equal(t, 88.1, records[1]["score"])
		assert.Equal(t, 75.5, records[2]["score"])
	})

	t.Run("places nil values at end regardless of sort order", func(t *testing.T) {
		t.Parallel()

		df := NewDataFrameFromRecords([]map[string]any{
			{"name": "Bob", "age": int64(25)},
			{"name": nil, "age": int64(30)},
			{"name": "Alice", "age": int64(35)},
		})

		sorted, err := df.Sort("name", Ascending)

		require.NoError(t, err)
		records := sorted.ToRecords()
		assert.Equal(t, "Alice", records[0]["name"])
		assert.Equal(t, "Bob", records[1]["name"])
		assert.Nil(t, records[2]["name"])
	})

	t.Run("returns error for non-existent column", func(t *testing.T) {
		t.Parallel()

		df := NewDataFrameFromRecords([]map[string]any{
			{"name": "Alice"},
		})

		_, err := df.Sort("unknown", Ascending)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "not found")
	})

	t.Run("does not modify original DataFrame", func(t *testing.T) {
		t.Parallel()

		df := NewDataFrameFromRecords([]map[string]any{
			{"name": "Charlie"},
			{"name": "Alice"},
			{"name": "Bob"},
		})
		originalFirst := df.ToRecords()[0]["name"]

		_, err := df.Sort("name", Ascending)

		require.NoError(t, err)
		assert.Equal(t, originalFirst, df.ToRecords()[0]["name"])
	})

	t.Run("sorts a uint column numerically", func(t *testing.T) {
		t.Parallel()

		df := NewDataFrameFromRecords([]map[string]any{
			{"v": uint(10)}, {"v": uint(9)}, {"v": uint(100)},
		})

		sorted, err := df.Sort("v", Ascending)

		require.NoError(t, err)
		records := sorted.ToRecords()
		assert.Equal(t, uint(9), records[0]["v"])
		assert.Equal(t, uint(10), records[1]["v"])
		assert.Equal(t, uint(100), records[2]["v"])
	})

	t.Run("sorts a column mixing numeric kinds numerically", func(t *testing.T) {
		t.Parallel()

		df := NewDataFrameFromRecords([]map[string]any{
			{"v": 9}, {"v": 10.5}, {"v": int64(2)}, {"v": uint64(100)}, {"v": 30},
		})

		sorted, err := df.Sort("v", Ascending)

		require.NoError(t, err)
		records := sorted.ToRecords()
		assert.Equal(t, int64(2), records[0]["v"])
		assert.Equal(t, 9, records[1]["v"])
		assert.Equal(t, 10.5, records[2]["v"])
		assert.Equal(t, 30, records[3]["v"])
		assert.Equal(t, uint64(100), records[4]["v"])
	})

	t.Run("keeps huge integers apart from their float neighbors", func(t *testing.T) {
		t.Parallel()

		// 1<<62 and 1<<62+1 collapse to the same float64; comparing them as
		// integers keeps the order exact.
		df := NewDataFrameFromRecords([]map[string]any{
			{"v": int64(1<<62 + 1)}, {"v": int64(1 << 62)},
		})

		sorted, err := df.Sort("v", Ascending)

		require.NoError(t, err)
		records := sorted.ToRecords()
		assert.Equal(t, int64(1<<62), records[0]["v"])
		assert.Equal(t, int64(1<<62+1), records[1]["v"])
	})

	t.Run("orders a huge integer after the float it does not equal", func(t *testing.T) {
		t.Parallel()

		// float64(1<<53) equals the integer 1<<53 exactly, so its successor
		// must sort after the float, not tie with it: a tie here made
		// equality intransitive (a == f, b == f, a < b) and the whole order
		// arbitrary.
		df := NewDataFrameFromRecords([]map[string]any{
			{"v": int64(1<<53 + 1)}, {"v": float64(1 << 53)}, {"v": int64(1 << 53)},
		})

		sorted, err := df.Sort("v", Ascending)

		require.NoError(t, err)
		records := sorted.ToRecords()
		assert.Equal(t, int64(1<<53+1), records[2]["v"])
	})
}

// TestCompareValuesIsAntisymmetric pins the contract slices.SortFunc requires
// of the comparator: swapping the arguments flips the sign, for every pair of
// kinds a frame can hold. An arm handling (int, float64) but not (float64, int)
// once made Sort return an arbitrary order.
func TestCompareValuesIsAntisymmetric(t *testing.T) {
	t.Parallel()

	values := []any{
		int(3), int8(4), int16(5), int32(6), int64(7),
		uint(3), uint8(4), uint16(5), uint32(6), uint64(7),
		float32(2.5), float64(3.5), int(-1), int64(1 << 62),
		uint64(1<<63 + 1), "3", "abc", true,
		int64(1 << 53), int64(1<<53 + 1), float64(1 << 53),
		uint64(1 << 53), uint64(1<<53 + 1),
	}

	for _, a := range values {
		for _, b := range values {
			got := compareValues(a, b)
			mirror := compareValues(b, a)
			if got != -mirror {
				t.Errorf("compareValues(%v(%T), %v(%T)) = %d but the mirror = %d", a, a, b, b, got, mirror)
			}
		}
	}

	// A strict weak ordering also needs transitive equality and order: an
	// integer that equaled the float its distinct neighbor also equaled once
	// broke this, and the sort's answer became arbitrary.
	for _, a := range values {
		for _, b := range values {
			for _, c := range values {
				ab, bc, ac := compareValues(a, b), compareValues(b, c), compareValues(a, c)
				if ab == 0 && bc == 0 && ac != 0 {
					t.Errorf("equality is not transitive over %v(%T), %v(%T), %v(%T)", a, a, b, b, c, c)
				}
				if ab < 0 && bc < 0 && ac >= 0 {
					t.Errorf("order is not transitive over %v(%T), %v(%T), %v(%T)", a, a, b, b, c, c)
				}
			}
		}
	}

	// A pair of numbers orders numerically whatever kinds spell them.
	if compareValues(uint(9), uint(10)) >= 0 {
		t.Error("compareValues(uint(9), uint(10)) should be negative")
	}
	if compareValues(float64(10.5), int(9)) <= 0 {
		t.Error("compareValues(10.5, int(9)) should be positive")
	}
	if compareValues(int(-1), uint64(1<<63+1)) >= 0 {
		t.Error("compareValues(int(-1), a uint64 past int64) should be negative")
	}
}

func TestDataFrame_SortBy(t *testing.T) {
	t.Parallel()

	t.Run("sorts by multiple columns", func(t *testing.T) {
		t.Parallel()

		df := NewDataFrameFromRecords([]map[string]any{
			{"category": "B", "price": int64(200)},
			{"category": "A", "price": int64(150)},
			{"category": "A", "price": int64(100)},
			{"category": "B", "price": int64(50)},
		})

		sorted, err := df.SortBy(
			SortOption{Column: "category", Order: Ascending},
			SortOption{Column: "price", Order: Descending},
		)

		require.NoError(t, err)
		records := sorted.ToRecords()
		// A category first, highest price first
		assert.Equal(t, "A", records[0]["category"])
		assert.Equal(t, int64(150), records[0]["price"])
		assert.Equal(t, "A", records[1]["category"])
		assert.Equal(t, int64(100), records[1]["price"])
		// B category second
		assert.Equal(t, "B", records[2]["category"])
		assert.Equal(t, int64(200), records[2]["price"])
		assert.Equal(t, "B", records[3]["category"])
		assert.Equal(t, int64(50), records[3]["price"])
	})

	t.Run("returns clone when no options provided", func(t *testing.T) {
		t.Parallel()

		df := NewDataFrameFromRecords([]map[string]any{
			{"name": "Alice"},
		})

		sorted, err := df.SortBy()

		require.NoError(t, err)
		assert.Equal(t, df.ToRecords(), sorted.ToRecords())
	})

	t.Run("returns error for non-existent column", func(t *testing.T) {
		t.Parallel()

		df := NewDataFrameFromRecords([]map[string]any{
			{"name": "Alice"},
		})

		_, err := df.SortBy(SortOption{Column: "unknown", Order: Ascending})

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "not found")
	})

	t.Run("keeps the input order of rows that compare equal", func(t *testing.T) {
		t.Parallel()

		rows := make([]map[string]any, 0, 15)
		for i := 1; i <= 15; i++ {
			k := "a"
			if i%2 == 0 {
				k = "b"
			}
			rows = append(rows, map[string]any{"k": k, "seq": i})
		}
		df := NewDataFrameFromRecords(rows)

		sorted, err := df.SortBy(SortOption{Column: "k", Order: Ascending})

		require.NoError(t, err)
		records := sorted.ToRecords()
		want := []int{1, 3, 5, 7, 9, 11, 13, 15, 2, 4, 6, 8, 10, 12, 14}
		for i, w := range want {
			assert.Equal(t, w, records[i]["seq"], "row %d", i)
		}
	})

	t.Run("keeps input order among rows equal on every sort key", func(t *testing.T) {
		t.Parallel()

		df := NewDataFrameFromRecords([]map[string]any{
			{"k": "a", "n": int64(1), "seq": 1},
			{"k": "a", "n": int64(1), "seq": 2},
			{"k": "a", "n": int64(1), "seq": 3},
			{"k": "a", "n": int64(1), "seq": 4},
		})

		sorted, err := df.SortBy(
			SortOption{Column: "k", Order: Ascending},
			SortOption{Column: "n", Order: Descending},
		)

		require.NoError(t, err)
		records := sorted.ToRecords()
		for i := range 4 {
			assert.Equal(t, i+1, records[i]["seq"], "row %d", i)
		}
	})
}

func TestDataFrame_Distinct(t *testing.T) {
	t.Parallel()

	t.Run("removes duplicate rows", func(t *testing.T) {
		t.Parallel()

		df := NewDataFrameFromRecords([]map[string]any{
			{"name": "Alice", "age": int64(30)},
			{"name": "Bob", "age": int64(25)},
			{"name": "Alice", "age": int64(30)},
			{"name": "Bob", "age": int64(25)},
		})

		unique := df.Distinct()

		assert.Equal(t, 2, unique.Len())
	})

	t.Run("preserves first occurrence order", func(t *testing.T) {
		t.Parallel()

		df := NewDataFrameFromRecords([]map[string]any{
			{"name": "Charlie"},
			{"name": "Alice"},
			{"name": "Charlie"},
			{"name": "Bob"},
		})

		unique := df.Distinct()

		records := unique.ToRecords()
		assert.Equal(t, 3, len(records))
		assert.Equal(t, "Charlie", records[0]["name"])
		assert.Equal(t, "Alice", records[1]["name"])
		assert.Equal(t, "Bob", records[2]["name"])
	})

	t.Run("does not modify original DataFrame", func(t *testing.T) {
		t.Parallel()

		df := NewDataFrameFromRecords([]map[string]any{
			{"name": "Alice"},
			{"name": "Alice"},
		})

		_ = df.Distinct()

		assert.Equal(t, 2, df.Len())
	})
}

func TestDataFrame_DistinctBy(t *testing.T) {
	t.Parallel()

	t.Run("removes duplicates based on specified columns only", func(t *testing.T) {
		t.Parallel()

		df := NewDataFrameFromRecords([]map[string]any{
			{"name": "Alice", "age": int64(30), "city": "Tokyo"},
			{"name": "Alice", "age": int64(30), "city": "Osaka"},
			{"name": "Bob", "age": int64(25), "city": "Tokyo"},
		})

		unique := df.DistinctBy("name", "age")

		assert.Equal(t, 2, unique.Len())
		records := unique.ToRecords()
		assert.Equal(t, "Alice", records[0]["name"])
		assert.Equal(t, "Tokyo", records[0]["city"]) // First occurrence kept
		assert.Equal(t, "Bob", records[1]["name"])
	})

	t.Run("returns clone when no columns specified", func(t *testing.T) {
		t.Parallel()

		df := NewDataFrameFromRecords([]map[string]any{
			{"name": "Alice"},
			{"name": "Alice"},
		})

		unique := df.DistinctBy()

		assert.Equal(t, 2, unique.Len())
	})
}

func TestDataFrame_Head(t *testing.T) {
	t.Parallel()

	t.Run("returns first n rows", func(t *testing.T) {
		t.Parallel()

		df := NewDataFrameFromRecords([]map[string]any{
			{"id": int64(1)},
			{"id": int64(2)},
			{"id": int64(3)},
			{"id": int64(4)},
			{"id": int64(5)},
		})

		head := df.Head(3)

		assert.Equal(t, 3, head.Len())
		records := head.ToRecords()
		assert.Equal(t, int64(1), records[0]["id"])
		assert.Equal(t, int64(2), records[1]["id"])
		assert.Equal(t, int64(3), records[2]["id"])
	})

	t.Run("returns all rows when n exceeds length", func(t *testing.T) {
		t.Parallel()

		df := NewDataFrameFromRecords([]map[string]any{
			{"id": int64(1)},
			{"id": int64(2)},
		})

		head := df.Head(10)

		assert.Equal(t, 2, head.Len())
	})

	t.Run("returns empty DataFrame for negative n", func(t *testing.T) {
		t.Parallel()

		df := NewDataFrameFromRecords([]map[string]any{
			{"id": int64(1)},
		})

		head := df.Head(-1)

		assert.Equal(t, 0, head.Len())
		assert.Equal(t, df.Columns(), head.Columns())
	})

	t.Run("returns empty DataFrame for n=0", func(t *testing.T) {
		t.Parallel()

		df := NewDataFrameFromRecords([]map[string]any{
			{"id": int64(1)},
		})

		head := df.Head(0)

		assert.Equal(t, 0, head.Len())
	})

	t.Run("does not modify original DataFrame", func(t *testing.T) {
		t.Parallel()

		df := NewDataFrameFromRecords([]map[string]any{
			{"id": int64(1)},
			{"id": int64(2)},
			{"id": int64(3)},
		})

		_ = df.Head(1)

		assert.Equal(t, 3, df.Len())
	})
}

func TestDataFrame_Tail(t *testing.T) {
	t.Parallel()

	t.Run("returns last n rows", func(t *testing.T) {
		t.Parallel()

		df := NewDataFrameFromRecords([]map[string]any{
			{"id": int64(1)},
			{"id": int64(2)},
			{"id": int64(3)},
			{"id": int64(4)},
			{"id": int64(5)},
		})

		tail := df.Tail(3)

		assert.Equal(t, 3, tail.Len())
		records := tail.ToRecords()
		assert.Equal(t, int64(3), records[0]["id"])
		assert.Equal(t, int64(4), records[1]["id"])
		assert.Equal(t, int64(5), records[2]["id"])
	})

	t.Run("returns all rows when n exceeds length", func(t *testing.T) {
		t.Parallel()

		df := NewDataFrameFromRecords([]map[string]any{
			{"id": int64(1)},
			{"id": int64(2)},
		})

		tail := df.Tail(10)

		assert.Equal(t, 2, tail.Len())
	})

	t.Run("returns empty DataFrame for negative n", func(t *testing.T) {
		t.Parallel()

		df := NewDataFrameFromRecords([]map[string]any{
			{"id": int64(1)},
		})

		tail := df.Tail(-1)

		assert.Equal(t, 0, tail.Len())
		assert.Equal(t, df.Columns(), tail.Columns())
	})
}

func TestDataFrame_Limit(t *testing.T) {
	t.Parallel()

	t.Run("is alias for Head", func(t *testing.T) {
		t.Parallel()

		df := NewDataFrameFromRecords([]map[string]any{
			{"id": int64(1)},
			{"id": int64(2)},
			{"id": int64(3)},
		})

		limited := df.Limit(2)
		head := df.Head(2)

		assert.Equal(t, head.ToRecords(), limited.ToRecords())
	})
}

func TestDataFrame_Drop(t *testing.T) {
	t.Parallel()

	t.Run("removes specified columns", func(t *testing.T) {
		t.Parallel()

		df := NewDataFrameFromRecords([]map[string]any{
			{"name": "Alice", "age": int64(30), "city": "Tokyo"},
		})

		dropped, err := df.Drop("age", "city")
		require.NoError(t, err)

		assert.Equal(t, []string{"name"}, dropped.Columns())
		records := dropped.ToRecords()
		assert.Equal(t, "Alice", records[0]["name"])
		_, hasAge := records[0]["age"]
		assert.False(t, hasAge)
	})

	// A typo used to be skipped, so the frame came back with the column the
	// caller believed had been dropped.
	t.Run("refuses a column that does not exist", func(t *testing.T) {
		t.Parallel()

		df := NewDataFrameFromRecords([]map[string]any{
			{"name": "Alice", "age": int64(30)},
		})

		_, err := df.Drop("unknown", "age")

		require.Error(t, err)
		assert.Contains(t, err.Error(), "unknown")
	})

	t.Run("drops a column that does exist", func(t *testing.T) {
		t.Parallel()

		df := NewDataFrameFromRecords([]map[string]any{
			{"name": "Alice", "age": int64(30)},
		})

		dropped, err := df.Drop("age")
		require.NoError(t, err)

		assert.Equal(t, []string{"name"}, dropped.Columns())
	})

	t.Run("returns clone when no columns specified", func(t *testing.T) {
		t.Parallel()

		df := NewDataFrameFromRecords([]map[string]any{
			{"name": "Alice"},
		})

		dropped, err := df.Drop()
		require.NoError(t, err)

		assert.Equal(t, df.Columns(), dropped.Columns())
		assert.Equal(t, df.ToRecords(), dropped.ToRecords())
	})

	t.Run("does not modify original DataFrame", func(t *testing.T) {
		t.Parallel()

		df := NewDataFrameFromRecords([]map[string]any{
			{"name": "Alice", "age": int64(30)},
		})

		_, err := df.Drop("age")
		require.NoError(t, err)

		assert.Equal(t, []string{"age", "name"}, df.Columns())
	})
}

func TestDataFrame_Rename(t *testing.T) {
	t.Parallel()

	t.Run("renames column successfully", func(t *testing.T) {
		t.Parallel()

		df := NewDataFrameFromRecords([]map[string]any{
			{"old_name": "Alice", "other": "value"},
		})

		renamed, err := df.Rename("old_name", "new_name")

		require.NoError(t, err)
		assert.Equal(t, []string{"new_name", "other"}, renamed.Columns())
		records := renamed.ToRecords()
		assert.Equal(t, "Alice", records[0]["new_name"])
		_, hasOld := records[0]["old_name"]
		assert.False(t, hasOld)
	})

	t.Run("returns error for non-existent column", func(t *testing.T) {
		t.Parallel()

		df := NewDataFrameFromRecords([]map[string]any{
			{"name": "Alice"},
		})

		_, err := df.Rename("unknown", "new_name")

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "not found")
	})

	t.Run("returns error when new name already exists", func(t *testing.T) {
		t.Parallel()

		df := NewDataFrameFromRecords([]map[string]any{
			{"name": "Alice", "other": "value"},
		})

		_, err := df.Rename("name", "other")

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "already exists")
	})

	t.Run("returns clone when old and new names are same", func(t *testing.T) {
		t.Parallel()

		df := NewDataFrameFromRecords([]map[string]any{
			{"name": "Alice"},
		})

		renamed, err := df.Rename("name", "name")

		require.NoError(t, err)
		assert.Equal(t, df.Columns(), renamed.Columns())
	})

	t.Run("does not modify original DataFrame", func(t *testing.T) {
		t.Parallel()

		df := NewDataFrameFromRecords([]map[string]any{
			{"old": "value"},
		})

		_, err := df.Rename("old", "new")

		require.NoError(t, err)
		assert.Equal(t, []string{"old"}, df.Columns())
	})
}

func TestDataFrame_RenameColumns(t *testing.T) {
	t.Parallel()

	t.Run("renames multiple columns", func(t *testing.T) {
		t.Parallel()

		df := NewDataFrameFromRecords([]map[string]any{
			{"a": 1, "b": 2, "c": 3},
		})

		renamed, err := df.RenameColumns(map[string]string{
			"a": "alpha",
			"b": "beta",
		})

		require.NoError(t, err)
		assert.Equal(t, []string{"alpha", "beta", "c"}, renamed.Columns())
	})

	t.Run("returns error for non-existent column", func(t *testing.T) {
		t.Parallel()

		df := NewDataFrameFromRecords([]map[string]any{
			{"name": "Alice"},
		})

		_, err := df.RenameColumns(map[string]string{"unknown": "new"})

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "not found")
	})

	t.Run("returns error for duplicate new names", func(t *testing.T) {
		t.Parallel()

		df := NewDataFrameFromRecords([]map[string]any{
			{"a": 1, "b": 2},
		})

		_, err := df.RenameColumns(map[string]string{
			"a": "same",
			"b": "same",
		})

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "duplicate")
	})

	t.Run("returns error when new name conflicts with existing", func(t *testing.T) {
		t.Parallel()

		df := NewDataFrameFromRecords([]map[string]any{
			{"a": 1, "b": 2, "c": 3},
		})

		_, err := df.RenameColumns(map[string]string{
			"a": "c", // c already exists and is not being renamed
		})

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "already exists")
	})

	t.Run("returns clone when empty map provided", func(t *testing.T) {
		t.Parallel()

		df := NewDataFrameFromRecords([]map[string]any{
			{"name": "Alice"},
		})

		renamed, err := df.RenameColumns(map[string]string{})

		require.NoError(t, err)
		assert.Equal(t, df.Columns(), renamed.Columns())
	})
}

func TestDataFrame_DropNA(t *testing.T) {
	t.Parallel()

	t.Run("removes rows with nil values", func(t *testing.T) {
		t.Parallel()

		df := NewDataFrameFromRecords([]map[string]any{
			{"name": "Alice", "age": int64(30)},
			{"name": nil, "age": int64(25)},
			{"name": "Charlie", "age": nil},
		})

		cleaned := df.DropNA()

		assert.Equal(t, 1, cleaned.Len())
		records := cleaned.ToRecords()
		assert.Equal(t, "Alice", records[0]["name"])
	})

	t.Run("removes rows with empty string values", func(t *testing.T) {
		t.Parallel()

		df := NewDataFrameFromRecords([]map[string]any{
			{"name": "Alice"},
			{"name": ""},
		})

		cleaned := df.DropNA()

		assert.Equal(t, 1, cleaned.Len())
	})

	t.Run("does not modify original DataFrame", func(t *testing.T) {
		t.Parallel()

		df := NewDataFrameFromRecords([]map[string]any{
			{"name": "Alice"},
			{"name": nil},
		})

		_ = df.DropNA()

		assert.Equal(t, 2, df.Len())
	})
}

func TestDataFrame_DropNASubset(t *testing.T) {
	t.Parallel()

	t.Run("removes rows with nil in specified columns only", func(t *testing.T) {
		t.Parallel()

		df := NewDataFrameFromRecords([]map[string]any{
			{"name": "Alice", "age": int64(30), "city": nil},
			{"name": nil, "age": int64(25), "city": "Tokyo"},
			{"name": "Charlie", "age": int64(35), "city": "Osaka"},
		})

		cleaned := df.DropNASubset("name", "age")

		// Should keep rows where name and age are not nil (even if city is nil)
		assert.Equal(t, 2, cleaned.Len())
	})

	t.Run("returns clone when no columns specified", func(t *testing.T) {
		t.Parallel()

		df := NewDataFrameFromRecords([]map[string]any{
			{"name": nil},
		})

		cleaned := df.DropNASubset()

		assert.Equal(t, 1, cleaned.Len())
	})
}

func TestDataFrame_FillNA(t *testing.T) {
	t.Parallel()

	t.Run("replaces nil values with specified value", func(t *testing.T) {
		t.Parallel()

		df := NewDataFrameFromRecords([]map[string]any{
			{"name": "Alice", "age": nil},
			{"name": nil, "age": int64(25)},
		})

		filled := df.FillNA("N/A")

		records := filled.ToRecords()
		assert.Equal(t, "Alice", records[0]["name"])
		assert.Equal(t, "N/A", records[0]["age"])
		assert.Equal(t, "N/A", records[1]["name"])
		assert.Equal(t, int64(25), records[1]["age"])
	})

	t.Run("replaces nil with zero value", func(t *testing.T) {
		t.Parallel()

		df := NewDataFrameFromRecords([]map[string]any{
			{"value": nil},
			{"value": int64(10)},
		})

		filled := df.FillNA(int64(0))

		records := filled.ToRecords()
		assert.Equal(t, int64(0), records[0]["value"])
		assert.Equal(t, int64(10), records[1]["value"])
	})

	t.Run("does not modify original DataFrame", func(t *testing.T) {
		t.Parallel()

		df := NewDataFrameFromRecords([]map[string]any{
			{"name": nil},
		})

		_ = df.FillNA("filled")

		records := df.ToRecords()
		assert.Nil(t, records[0]["name"])
	})
}

func TestDataFrame_FillNAByColumn(t *testing.T) {
	t.Parallel()

	t.Run("replaces nil with column-specific values", func(t *testing.T) {
		t.Parallel()

		df := NewDataFrameFromRecords([]map[string]any{
			{"name": nil, "age": nil, "active": nil},
		})

		filled := df.FillNAByColumn(map[string]any{
			"name":   "Unknown",
			"age":    int64(0),
			"active": false,
		})

		records := filled.ToRecords()
		assert.Equal(t, "Unknown", records[0]["name"])
		assert.Equal(t, int64(0), records[0]["age"])
		assert.Equal(t, false, records[0]["active"])
	})

	t.Run("leaves nil for columns not in map", func(t *testing.T) {
		t.Parallel()

		df := NewDataFrameFromRecords([]map[string]any{
			{"a": nil, "b": nil},
		})

		filled := df.FillNAByColumn(map[string]any{
			"a": "filled",
		})

		records := filled.ToRecords()
		assert.Equal(t, "filled", records[0]["a"])
		assert.Nil(t, records[0]["b"])
	})

	t.Run("returns clone when empty map provided", func(t *testing.T) {
		t.Parallel()

		df := NewDataFrameFromRecords([]map[string]any{
			{"name": nil},
		})

		filled := df.FillNAByColumn(map[string]any{})

		records := filled.ToRecords()
		assert.Nil(t, records[0]["name"])
	})
}

func TestCompareValues(t *testing.T) {
	t.Parallel()

	t.Run("compares strings", func(t *testing.T) {
		t.Parallel()

		assert.Equal(t, -1, compareValues("a", "b"))
		assert.Equal(t, 0, compareValues("a", "a"))
		assert.Equal(t, 1, compareValues("b", "a"))
	})

	t.Run("compares int64", func(t *testing.T) {
		t.Parallel()

		assert.Equal(t, -1, compareValues(int64(1), int64(2)))
		assert.Equal(t, 0, compareValues(int64(1), int64(1)))
		assert.Equal(t, 1, compareValues(int64(2), int64(1)))
	})

	t.Run("compares float64", func(t *testing.T) {
		t.Parallel()

		assert.Equal(t, -1, compareValues(1.0, 2.0))
		assert.Equal(t, 0, compareValues(1.0, 1.0))
		assert.Equal(t, 1, compareValues(2.0, 1.0))
	})

	t.Run("compares int64 with float64", func(t *testing.T) {
		t.Parallel()

		assert.Equal(t, -1, compareValues(int64(1), 2.0))
		assert.Equal(t, 0, compareValues(int64(1), 1.0))
		assert.Equal(t, 1, compareValues(int64(2), 1.0))
	})

	t.Run("compares int with int64", func(t *testing.T) {
		t.Parallel()

		assert.Equal(t, -1, compareValues(1, int64(2)))
		assert.Equal(t, 0, compareValues(1, int64(1)))
		assert.Equal(t, 1, compareValues(2, int64(1)))
	})

	t.Run("falls back to string comparison for mixed types", func(t *testing.T) {
		t.Parallel()

		// String representation comparison
		result := compareValues("abc", int64(123))
		assert.NotEqual(t, 0, result) // Just check it doesn't panic
	})
}

// TestDataFrameKeepsValuesOnlyTextHolds pins the fidelity the SQLite load path
// was given: a value that looks numeric but cannot be one without loss keeps its
// text form. A zero-padded code and an integer past int64 are the two, and both
// used to come back changed — 007 as 7, an account number as 1.104032026e+19 —
// from a round trip that only read the file and wrote it out again.
//
// Decimal scale is not among them: "1.50" loads as the real 1.5 here as it does
// everywhere else in filesql, because the quantity survives and only the way it
// was written does not.
func TestDataFrameKeepsValuesOnlyTextHolds(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		input string
		want  string
	}{
		{
			name:  "a zero-padded code keeps its zeros",
			input: "code\n007\n010\n",
			want:  "code\n007\n010\n",
		},
		{
			name:  "an integer past int64 keeps its digits",
			input: "n\n99999999999999999999\n",
			want:  "n\n99999999999999999999\n",
		},
		{
			name:  "an ordinary integer is still a number",
			input: "n\n1\n2\n",
			want:  "n\n1\n2\n",
		},
		{
			name:  "a decimal keeps its quantity, not its scale",
			input: "amt\n1.50\n2.00\n",
			want:  "amt\n1.5\n2\n",
		},
		{
			name:  "a zero-padded code mixed with a plain one keeps both",
			input: "code\n007\n42\n",
			want:  "code\n007\n42\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			df, err := NewDataFrame(strings.NewReader(tt.input), CSV)
			if err != nil {
				t.Fatalf("NewDataFrame: %v", err)
			}

			out := filepath.Join(t.TempDir(), "out.csv")
			if err := df.ToCSV(out); err != nil {
				t.Fatalf("ToCSV: %v", err)
			}
			got, err := os.ReadFile(out) //nolint:gosec // test-owned path
			if err != nil {
				t.Fatal(err)
			}
			if string(got) != tt.want {
				t.Errorf("round trip = %q, want %q", got, tt.want)
			}
		})
	}
}

// TestDistinctAndJoinCompareValuesAsWritten covers what typing a value costs
// when the value is an identifier. Both used to coerce: Distinct merged 007 into
// 7 and kept the one without its zeros, and Join matched a 007 account to a 7
// account, a row pair that exists in neither input.
//
// Values equal as numbers and written differently — 1, 1.0, 1.00 — still
// collapse. That is the decimal-scale trade-off filesql makes everywhere: the
// quantity survives and the spelling does not.
func TestDistinctAndJoinCompareValuesAsWritten(t *testing.T) {
	t.Parallel()

	t.Run("a zero-padded code is not the code without its zeros", func(t *testing.T) {
		t.Parallel()

		df, err := NewDataFrame(strings.NewReader("code\n007\n7\n7\n"), CSV)
		require.NoError(t, err)

		got := df.Distinct().ToRecords()

		// Both are text: a column holding a code is a text column, and the two
		// codes in it are two values.
		assert.Equal(t, []map[string]any{{"code": "007"}, {"code": "7"}}, got)
	})

	t.Run("values equal as numbers are one value however they were written", func(t *testing.T) {
		t.Parallel()

		df, err := NewDataFrame(strings.NewReader("v\n1\n1.0\n1.00\n"), CSV)
		require.NoError(t, err)

		got := df.Distinct().ToRecords()

		// Keeping the three spellings apart would mean keeping the column as
		// text, where 9.00 does not compare as less than 10.00. The quantity is
		// worth more than the spelling, so the scale goes.
		assert.Equal(t, []map[string]any{{"v": 1.0}}, got)
	})

	t.Run("a join does not match a code to its numeric reading", func(t *testing.T) {
		t.Parallel()

		left, err := NewDataFrame(strings.NewReader("id,x\n007,a\n"), CSV)
		require.NoError(t, err)
		right, err := NewDataFrame(strings.NewReader("id,y\n7,Z\n"), CSV)
		require.NoError(t, err)

		joined, err := left.Join(right, JoinOption{On: []string{"id"}, How: InnerJoin})
		require.NoError(t, err)

		assert.Empty(t, joined.ToRecords(), "007 and 7 are different identifiers")
	})

	t.Run("a number is not the text that spells it", func(t *testing.T) {
		t.Parallel()

		// The two rows arrive already typed, which is the shape a caller
		// building a frame from records has. Formatting both with %v to key
		// them made them one row.
		df := NewDataFrameFromRecords([]map[string]any{{"a": int64(7)}, {"a": "7"}})

		assert.Equal(t, 2, df.Distinct().Len(), "the integer 7 and the text \"7\" are two values")
	})

	t.Run("a missing value is not the text nil formats as", func(t *testing.T) {
		t.Parallel()

		df := NewDataFrameFromRecords([]map[string]any{{"a": nil}, {"a": "<nil>"}})

		assert.Equal(t, 2, df.Distinct().Len())
	})

	t.Run("a value carrying the separator stays in its own column", func(t *testing.T) {
		t.Parallel()

		// Joining the cells with a null byte let a value holding one reach
		// across into its neighbor: these two rows keyed the same.
		df := NewDataFrameFromRecords([]map[string]any{
			{"a": "x\x00y", "b": "z"},
			{"a": "x", "b": "y\x00z"},
		})

		assert.Equal(t, 2, df.Distinct().Len())
	})

	t.Run("a boolean is not the text that spells it", func(t *testing.T) {
		t.Parallel()

		df := NewDataFrameFromRecords([]map[string]any{{"a": true}, {"a": "true"}})

		assert.Equal(t, 2, df.Distinct().Len())
	})

	t.Run("an empty value is not a missing one", func(t *testing.T) {
		t.Parallel()

		df := NewDataFrameFromRecords([]map[string]any{{"a": ""}, {"a": nil}})

		assert.Equal(t, 2, df.Distinct().Len())
	})

	t.Run("a join matches an integer to the equal real", func(t *testing.T) {
		t.Parallel()

		// Distinct collapses 1 and 1.0, and doc.go says a join matches them to
		// each other. Indexing the join by the interface value did not: the two
		// are different map keys.
		left := NewDataFrameFromRecords([]map[string]any{{"id": int64(1), "x": "a"}})
		right := NewDataFrameFromRecords([]map[string]any{{"id": float64(1), "y": "Z"}})

		joined, err := left.Join(right, JoinOption{On: []string{"id"}, How: InnerJoin})
		require.NoError(t, err)

		assert.Equal(t, []map[string]any{{"id": int64(1), "x": "a", "y": "Z"}}, joined.ToRecords())
	})

	t.Run("a join matches an integer to the equal real the other way round", func(t *testing.T) {
		t.Parallel()

		left := NewDataFrameFromRecords([]map[string]any{{"id": float64(1), "x": "a"}})
		right := NewDataFrameFromRecords([]map[string]any{{"id": int64(1), "y": "Z"}})

		joined, err := left.Join(right, JoinOption{On: []string{"id"}, How: InnerJoin})
		require.NoError(t, err)

		assert.Equal(t, []map[string]any{{"id": float64(1), "x": "a", "y": "Z"}}, joined.ToRecords())
	})

	t.Run("a left join fills the right side when the reals match", func(t *testing.T) {
		t.Parallel()

		left := NewDataFrameFromRecords([]map[string]any{{"id": int64(1), "x": "a"}})
		right := NewDataFrameFromRecords([]map[string]any{{"id": float64(1), "y": "Z"}})

		joined, err := left.Join(right, JoinOption{On: []string{"id"}, How: LeftJoin})
		require.NoError(t, err)

		assert.Equal(t, []map[string]any{{"id": int64(1), "x": "a", "y": "Z"}}, joined.ToRecords())
	})

	t.Run("a join still matches a code to itself", func(t *testing.T) {
		t.Parallel()

		left, err := NewDataFrame(strings.NewReader("id,x\n007,a\n"), CSV)
		require.NoError(t, err)
		right, err := NewDataFrame(strings.NewReader("id,y\n007,Z\n"), CSV)
		require.NoError(t, err)

		joined, err := left.Join(right, JoinOption{On: []string{"id"}, How: InnerJoin})
		require.NoError(t, err)

		assert.Equal(t, []map[string]any{{"id": "007", "x": "a", "y": "Z"}}, joined.ToRecords())
	})
}

// TestToTSVTakesFieldsLiterally pins what a TSV file can hold. A CSV writer with
// its comma changed wrapped a value holding a tab in double quotes: to a TSV
// reader those quotes are two more characters, and the tab inside them is still
// a field boundary, so the file came out with the wrong shape and the quotes as
// data. A value TSV cannot represent is refused rather than written as
// something else.
func TestToTSVTakesFieldsLiterally(t *testing.T) {
	t.Parallel()

	t.Run("a quote is written as is, not doubled", func(t *testing.T) {
		t.Parallel()

		df, err := NewDataFrame(strings.NewReader("v\n\"a\"\"b\"\n"), CSV)
		require.NoError(t, err)

		out := filepath.Join(t.TempDir(), "out.tsv")
		require.NoError(t, df.ToTSV(out))

		got, err := os.ReadFile(out) //nolint:gosec // test-owned path
		require.NoError(t, err)
		assert.Equal(t, "v\na\"b\n", string(got))
	})

	t.Run("a value holding a tab is refused", func(t *testing.T) {
		t.Parallel()

		df, err := NewDataFrame(strings.NewReader("v\n\"a\tb\"\n"), CSV)
		require.NoError(t, err)

		err = df.ToTSV(filepath.Join(t.TempDir(), "out.tsv"))

		require.ErrorIs(t, err, parser.ErrTSVUnrepresentable)
	})

	t.Run("a value holding a newline is refused", func(t *testing.T) {
		t.Parallel()

		df, err := NewDataFrame(strings.NewReader("v\n\"c\nd\"\n"), CSV)
		require.NoError(t, err)

		err = df.ToTSV(filepath.Join(t.TempDir(), "out.tsv"))

		require.ErrorIs(t, err, parser.ErrTSVUnrepresentable)
	})

	t.Run("an ordinary frame round trips", func(t *testing.T) {
		t.Parallel()

		df, err := NewDataFrame(strings.NewReader("a,b\n1,x\n2,y\n"), CSV)
		require.NoError(t, err)

		out := filepath.Join(t.TempDir(), "out.tsv")
		require.NoError(t, df.ToTSV(out))

		reloaded, err := NewDataFrameFromPath(out)
		require.NoError(t, err)
		assert.Equal(t, df.ToRecords(), reloaded.ToRecords())
	})
}

// TestConcatAndConcatAllAgreeOnCompatibleFrames pins the two ways they used to
// disagree about the same pair of frames.
//
// Concat compared column slices positionally and refused frames whose columns
// were the same set in a different order, reporting "different columns" about
// columns that were the same — while ConcatAll accepted that very pair. And
// ConcatAll dropped a nil frame silently while Concat rejected it.
func TestConcatAndConcatAllAgreeOnCompatibleFrames(t *testing.T) {
	t.Parallel()

	t.Run("Concat accepts the same columns in a different order", func(t *testing.T) {
		t.Parallel()

		left, err := NewDataFrame(strings.NewReader("b,a\n1,2\n"), CSV)
		require.NoError(t, err)
		right, err := NewDataFrame(strings.NewReader("a,b\n3,4\n"), CSV)
		require.NoError(t, err)

		got, err := left.Concat(right)

		require.NoError(t, err)
		// The receiver's order is the result's, which is what it was before for
		// the frames Concat already accepted.
		assert.Equal(t, []string{"b", "a"}, got.Columns())
		assert.Equal(t, []map[string]any{
			{"b": int64(1), "a": int64(2)},
			{"b": int64(4), "a": int64(3)},
		}, got.ToRecords())
	})

	t.Run("Concat still refuses a genuinely different set", func(t *testing.T) {
		t.Parallel()

		left, err := NewDataFrame(strings.NewReader("a,b\n1,2\n"), CSV)
		require.NoError(t, err)
		right, err := NewDataFrame(strings.NewReader("a,c\n3,4\n"), CSV)
		require.NoError(t, err)

		_, err = left.Concat(right)

		require.Error(t, err)
	})

	t.Run("Concat still refuses a nil", func(t *testing.T) {
		t.Parallel()

		left, err := NewDataFrame(strings.NewReader("a\n1\n"), CSV)
		require.NoError(t, err)

		_, err = left.Concat(nil)

		require.Error(t, err)
	})

	t.Run("ConcatAll refuses a lone nil", func(t *testing.T) {
		t.Parallel()

		_, err := ConcatAll(nil)

		require.Error(t, err)
	})
}

// TestDropNAAndFillNAAgreeOnMissing pins the one definition. DropNA counted an
// empty string as missing and FillNA counted only a real nil, so on the same
// frame DropNA removed a row that FillNA would not fill — and a caller filling a
// frame to make it safe for later processing was left with the cell that made it
// unsafe. A CSV has no null, so "" is how a missing value arrives from the
// format most frames are read from.
func TestDropNAAndFillNAAgreeOnMissing(t *testing.T) {
	t.Parallel()

	const csv = "id,v\n1,a\n2,\n3,c\n"

	t.Run("FillNA fills what DropNA would drop", func(t *testing.T) {
		t.Parallel()

		dropped, err := NewDataFrame(strings.NewReader(csv), CSV)
		require.NoError(t, err)
		filled, err := NewDataFrame(strings.NewReader(csv), CSV)
		require.NoError(t, err)

		assert.Equal(t, 2, dropped.DropNA().Len(), "the empty cell is missing")
		assert.Equal(t, 3, filled.FillNA("X").Len())
		assert.Equal(t, "X", filled.FillNA("X").ToRecords()[1]["v"])
	})

	t.Run("a filled frame has nothing left for DropNA to drop", func(t *testing.T) {
		t.Parallel()

		df, err := NewDataFrame(strings.NewReader(csv), CSV)
		require.NoError(t, err)

		assert.Equal(t, 3, df.FillNA("X").DropNA().Len())
	})

	t.Run("FillNAByColumn fills what DropNASubset would drop", func(t *testing.T) {
		t.Parallel()

		df, err := NewDataFrame(strings.NewReader(csv), CSV)
		require.NoError(t, err)

		filled := df.FillNAByColumn(map[string]any{"v": "X"})

		assert.Equal(t, "X", filled.ToRecords()[1]["v"])
		assert.Equal(t, 3, filled.DropNASubset("v").Len())
	})

	t.Run("a column with no fill value named keeps its cells", func(t *testing.T) {
		t.Parallel()

		df, err := NewDataFrame(strings.NewReader("a,b\n,\n"), CSV)
		require.NoError(t, err)

		filled := df.FillNAByColumn(map[string]any{"a": "X"})

		record := filled.ToRecords()[0]
		assert.Equal(t, "X", record["a"])
		assert.Equal(t, "", record["b"], "a caller filling one column did not ask about the other")
	})
}

// TestValueIdentityForLargeIntegers requires two integers that differ to be two
// values. Every numeric type shares one canonical spelling so that 1 and 1.0 are
// one value; that spelling has to be exact, or two integers a float64 cannot tell
// apart become one row, one group, or a join match that was never equal.
func TestValueIdentityForLargeIntegers(t *testing.T) {
	t.Parallel()

	t.Run("Distinct keeps two int64 values past 2^53 apart", func(t *testing.T) {
		t.Parallel()

		df := NewDataFrameFromRecords([]map[string]any{
			{"id": int64(9007199254740993)},
			{"id": int64(9007199254740992)},
		})
		assert.Equal(t, 2, df.Distinct().Len())
	})

	t.Run("Distinct keeps three uint64 values near the maximum apart", func(t *testing.T) {
		t.Parallel()

		df := NewDataFrameFromRecords([]map[string]any{
			{"id": uint64(18446744073709551615)},
			{"id": uint64(18446744073709551614)},
			{"id": uint64(18446744073709549568)},
		})
		assert.Equal(t, 3, df.Distinct().Len())
	})

	t.Run("GroupBy makes one group per distinct large integer", func(t *testing.T) {
		t.Parallel()

		df := NewDataFrameFromRecords([]map[string]any{
			{"id": int64(9007199254740993)},
			{"id": int64(9007199254740992)},
		})
		grouped, err := df.GroupBy("id")
		require.NoError(t, err)
		assert.Equal(t, 2, grouped.Count().Len())
	})

	t.Run("Join does not match two large integers that differ", func(t *testing.T) {
		t.Parallel()

		left := NewDataFrameFromRecords([]map[string]any{
			{"id": int64(9007199254740993), "name": "alice"},
		})
		right := NewDataFrameFromRecords([]map[string]any{
			{"id": int64(9007199254740992), "tag": "other"},
		})
		joined, err := left.Join(right, JoinOption{On: []string{"id"}, How: InnerJoin})
		require.NoError(t, err)
		assert.Equal(t, 0, joined.Len())
	})

	t.Run("a quantity spelled by different types is still one value", func(t *testing.T) {
		t.Parallel()

		df := NewDataFrameFromRecords([]map[string]any{
			{"id": 1},
			{"id": int64(1)},
			{"id": float64(1.0)},
			{"id": float32(1)},
		})
		assert.Equal(t, 1, df.Distinct().Len())

		grouped, err := df.GroupBy("id")
		require.NoError(t, err)
		assert.Equal(t, 1, grouped.Count().Len())

		left := NewDataFrameFromRecords([]map[string]any{{"id": int64(1), "name": "alice"}})
		right := NewDataFrameFromRecords([]map[string]any{{"id": float64(1.0), "tag": "match"}})
		joined, err := left.Join(right, JoinOption{On: []string{"id"}, How: InnerJoin})
		require.NoError(t, err)
		assert.Equal(t, 1, joined.Len())
	})
}

// TestJoinNamesEveryColumnOnce requires a join to keep every column of both
// frames. The right column that collides with a left one is renamed, and the
// name it is renamed to has to be free, or the join overwrites the left column
// it was supposed to leave alone.
func TestJoinNamesEveryColumnOnce(t *testing.T) {
	t.Parallel()

	t.Run("the invented name is already a left column", func(t *testing.T) {
		t.Parallel()

		left := NewDataFrameFromRecords([]map[string]any{
			{"id": 1, "v": "left v", "right_v": "left right_v"},
		})
		right := NewDataFrameFromRecords([]map[string]any{
			{"id": 1, "v": "right v"},
		})
		joined, err := left.Join(right, JoinOption{On: []string{"id"}, How: InnerJoin})
		require.NoError(t, err)

		columns := joined.Columns()
		assert.Len(t, columns, 4)
		seen := make(map[string]struct{}, len(columns))
		for _, column := range columns {
			_, duplicate := seen[column]
			assert.False(t, duplicate, "column %q named twice in %v", column, columns)
			seen[column] = struct{}{}
		}

		require.Equal(t, 1, joined.Len())
		values := make([]string, 0, len(columns))
		for _, column := range columns {
			if text, ok := joined.ToRecords()[0][column].(string); ok {
				values = append(values, text)
			}
		}
		assert.Contains(t, values, "left v")
		assert.Contains(t, values, "left right_v")
		assert.Contains(t, values, "right v")
	})

	t.Run("two right columns want the same invented name", func(t *testing.T) {
		t.Parallel()

		left := NewDataFrameFromRecords([]map[string]any{
			{"id": 1, "x": "left x"},
		})
		right := NewDataFrameFromRecords([]map[string]any{
			{"id": 1, "x": "right x", "right_x": "right right_x"},
		})
		joined, err := left.Join(right, JoinOption{On: []string{"id"}, How: InnerJoin})
		require.NoError(t, err)

		columns := joined.Columns()
		assert.Len(t, columns, 4)
		require.Equal(t, 1, joined.Len())
		values := make([]string, 0, len(columns))
		for _, column := range columns {
			if text, ok := joined.ToRecords()[0][column].(string); ok {
				values = append(values, text)
			}
		}
		assert.Contains(t, values, "left x")
		assert.Contains(t, values, "right x")
		assert.Contains(t, values, "right right_x")
	})

	t.Run("an ordinary collision still becomes right_name", func(t *testing.T) {
		t.Parallel()

		left := NewDataFrameFromRecords([]map[string]any{
			{"id": 1, "name": "alice"},
		})
		right := NewDataFrameFromRecords([]map[string]any{
			{"id": 1, "name": "bob"},
		})
		joined, err := left.Join(right, JoinOption{On: []string{"id"}, How: InnerJoin})
		require.NoError(t, err)
		assert.Equal(t, []string{"id", "name", "right_name"}, joined.Columns())
		assert.Equal(t, "alice", joined.ToRecords()[0]["name"])
		assert.Equal(t, "bob", joined.ToRecords()[0]["right_name"])
	})
}
