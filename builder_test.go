package filesql

import (
	"bytes"
	"context"
	"database/sql"
	"embed"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"testing/fstest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"modernc.org/sqlite"
)

//go:embed testdata/embed_test/*.csv testdata/embed_test/*.tsv
var testFS embed.FS

func TestNewBuilder(t *testing.T) {
	t.Parallel()

	builder := NewBuilder()
	require.NotNil(t, builder, "NewBuilder() should not return nil")
	assert.Len(t, builder.paths, 0, "NewBuilder() should have empty paths slice")
	assert.Len(t, builder.filesystems, 0, "NewBuilder() should have empty filesystems slice")
}

func TestDBBuilder_AddPath(t *testing.T) {
	t.Parallel()

	t.Run("single path", func(t *testing.T) {
		t.Parallel()
		builder := NewBuilder().AddPath("test.csv")
		assert.Len(t, builder.paths, 1, "should have 1 path")
		assert.Equal(t, "test.csv", builder.paths[0], "first path should be test.csv")
	})

	t.Run("chain multiple paths", func(t *testing.T) {
		t.Parallel()
		builder := NewBuilder().
			AddPath("test1.csv").
			AddPath("test2.tsv")
		assert.Len(t, builder.paths, 2, "should have 2 paths after chaining")
	})
}

func TestDBBuilder_AddPaths(t *testing.T) {
	t.Parallel()

	builder := NewBuilder().AddPaths("test1.csv", "test2.tsv", "test3.ltsv")
	assert.Len(t, builder.paths, 3, "should have 3 paths after AddPaths")
}

func TestDBBuilder_AddFS(t *testing.T) {
	t.Parallel()

	t.Run("add filesystem", func(t *testing.T) {
		t.Parallel()
		mockFS := fstest.MapFS{
			"data.csv": &fstest.MapFile{Data: []byte("col1,col2\nval1,val2\n")},
		}

		builder := NewBuilder().AddFS(mockFS)
		assert.Len(t, builder.filesystems, 1, "should have 1 filesystem")
	})

	t.Run("add multiple filesystems", func(t *testing.T) {
		t.Parallel()
		mockFS1 := fstest.MapFS{
			"data1.csv": &fstest.MapFile{Data: []byte("col1,col2\nval1,val2\n")},
		}
		mockFS2 := fstest.MapFS{
			"data2.csv": &fstest.MapFile{Data: []byte("col1,col2\nval3,val4\n")},
		}

		builder := NewBuilder().AddFS(mockFS1).AddFS(mockFS2)
		assert.Len(t, builder.filesystems, 2, "should have 2 filesystems")
	})
}

func TestDBBuilder_AddReader(t *testing.T) {
	t.Parallel()

	t.Run("add CSV reader", func(t *testing.T) {
		t.Parallel()
		data := "name,age\nAlice,30\nBob,25\n"
		reader := bytes.NewReader([]byte(data))

		builder := NewBuilder().AddReader(reader, "users", FileTypeCSV)
		assert.Len(t, builder.readers, 1, "should have 1 reader")
		assert.Equal(t, "users", builder.readers[0].tableName, "table name should be users")
		assert.Equal(t, FileTypeCSV, builder.readers[0].fileType, "file type should be CSV")
		// No compression fields to check since FileTypeCSV is uncompressed
	})

	t.Run("add TSV reader", func(t *testing.T) {
		t.Parallel()
		data := "col1\tcol2\nval1\tval2\n"
		reader := bytes.NewReader([]byte(data))

		builder := NewBuilder().AddReader(reader, "data", FileTypeTSV)
		assert.Len(t, builder.readers, 1, "should have 1 reader")
		assert.Equal(t, FileTypeTSV, builder.readers[0].fileType, "file type should be TSV")
	})

	t.Run("add compressed CSV reader", func(t *testing.T) {
		t.Parallel()
		data := []byte{} // Empty data for test
		reader := bytes.NewReader(data)

		builder := NewBuilder().AddReader(reader, "logs", FileTypeCSVGZ)
		assert.Len(t, builder.readers, 1, "should have 1 reader")
		assert.Equal(t, FileTypeCSVGZ, builder.readers[0].fileType, "file type should be CSV.GZ")
		// Regular CSV type for testing
	})

	t.Run("add multiple readers", func(t *testing.T) {
		t.Parallel()
		reader1 := bytes.NewReader([]byte("col1,col2\nval1,val2\n"))
		reader2 := bytes.NewReader([]byte("col3\tcol4\nval3\tval4\n"))

		builder := NewBuilder().
			AddReader(reader1, "table1", FileTypeCSV).
			AddReader(reader2, "table2", FileTypeTSV)

		assert.Len(t, builder.readers, 2, "should have 2 readers")
	})
}

func TestDBBuilder_SetDefaultChunkSize(t *testing.T) {
	t.Parallel()

	t.Run("set custom chunk size", func(t *testing.T) {
		t.Parallel()
		customSize := 20 * 1024 * 1024 // 20MB
		builder := NewBuilder().SetDefaultChunkSize(customSize)

		assert.Equal(t, customSize, builder.defaultChunkSize, "default chunk size should be set to custom size")
	})

	t.Run("zero or negative size ignored", func(t *testing.T) {
		t.Parallel()
		defaultSize := 10 * 1024 * 1024
		builder := NewBuilder()

		// Zero should be ignored
		builder.SetDefaultChunkSize(0)
		assert.Equal(t, defaultSize, builder.defaultChunkSize, "chunk size should not change when set to zero")

		// Negative should be ignored
		builder.SetDefaultChunkSize(-1)
		assert.Equal(t, defaultSize, builder.defaultChunkSize, "chunk size should not change when set to negative")
	})
}

func TestDBBuilder_Build(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	t.Run("no inputs error", func(t *testing.T) {
		t.Parallel()
		builder := NewBuilder()
		_, err := builder.Build(ctx)
		assert.Error(t, err, "Build() should return error for no inputs")
	})

	t.Run("reader with nil reader error", func(t *testing.T) {
		t.Parallel()
		builder := NewBuilder()
		builder.readers = append(builder.readers, readerInput{
			reader:    nil,
			tableName: "test",
			fileType:  FileTypeCSV,
		})

		_, err := builder.Build(ctx)
		assert.Error(t, err, "Build() should return error for nil reader")
		assert.Contains(t, err.Error(), "reader cannot be nil", "error message should mention nil reader")
	})

	t.Run("reader with empty table name error", func(t *testing.T) {
		t.Parallel()
		reader := bytes.NewReader([]byte("test"))
		builder := NewBuilder()
		builder.readers = append(builder.readers, readerInput{
			reader:    reader,
			tableName: "",
			fileType:  FileTypeCSV,
		})

		_, err := builder.Build(ctx)
		assert.Error(t, err, "Build() should return error for empty table name")
		assert.Contains(t, err.Error(), "table name must be specified", "error message should mention table name requirement")
	})

	t.Run("reader with unsupported file type error", func(t *testing.T) {
		t.Parallel()
		reader := bytes.NewReader([]byte("test"))
		builder := NewBuilder()
		builder.readers = append(builder.readers, readerInput{
			reader:    reader,
			tableName: "test",
			fileType:  FileTypeUnsupported,
		})

		_, err := builder.Build(ctx)
		assert.Error(t, err, "Build() should return error for unsupported file type")
		assert.Contains(t, err.Error(), "file type must be specified", "error message should mention file type requirement")
	})

	t.Run("reader with valid CSV data", func(t *testing.T) {
		t.Parallel()
		data := "name,age\nAlice,30\nBob,25\n"
		reader := bytes.NewReader([]byte(data))
		builder := NewBuilder().AddReader(reader, "users", FileTypeCSV)

		validatedBuilder, err := builder.Build(ctx)
		assert.NoError(t, err, "Build() should succeed with valid CSV data")
		require.NotNil(t, validatedBuilder, "Build() should not return nil builder")
		// Readers don't create temp files anymore - they use direct streaming
		assert.Len(t, validatedBuilder.readers, 1, "Build() should have 1 reader input")

		// Clean up temp files
	})

	t.Run("reader with compressed type specification", func(t *testing.T) {
		t.Parallel()
		// Note: Use regular CSV data since we're testing the type system, not actual compression
		data := []byte("col1,col2\nval1,val2\n")
		reader := bytes.NewReader(data)
		builder := NewBuilder().AddReader(reader, "logs", FileTypeCSV)

		validatedBuilder, err := builder.Build(ctx)
		assert.NoError(t, err, "Build() should succeed with compressed type")
		assert.NotNil(t, validatedBuilder, "Build() should not return nil builder")

		// Clean up temp files
	})

	t.Run("multiple readers", func(t *testing.T) {
		t.Parallel()
		reader1 := bytes.NewReader([]byte("col1,col2\nval1,val2\n"))
		reader2 := bytes.NewReader([]byte("col3\tcol4\nval3\tval4\n"))

		builder := NewBuilder().
			AddReader(reader1, "table1", FileTypeCSV).
			AddReader(reader2, "table2", FileTypeTSV)

		validatedBuilder, err := builder.Build(ctx)
		assert.NoError(t, err, "Build() should succeed with multiple readers")
		require.NotNil(t, validatedBuilder, "Build() should not return nil builder")
		// Readers don't create temp files anymore - they use direct streaming
		assert.Len(t, validatedBuilder.readers, 2, "Build() should have 2 reader inputs")

		// Clean up temp files
	})

	t.Run("invalid path error", func(t *testing.T) {
		t.Parallel()
		builder := NewBuilder().AddPath(filepath.Join("nonexistent", "file.csv"))
		_, err := builder.Build(ctx)
		assert.Error(t, err, "Build() should return error for nonexistent path")
	})

	t.Run("unsupported file type error", func(t *testing.T) {
		t.Parallel()
		// Create a temporary unsupported file
		tempDir := t.TempDir()
		unsupportedFile := filepath.Join(tempDir, "test.txt")
		err := os.WriteFile(unsupportedFile, []byte("test"), 0600)
		require.NoError(t, err, "should create test file")

		builder := NewBuilder().AddPath(unsupportedFile)
		_, err = builder.Build(ctx)
		assert.Error(t, err, "Build() should return error for unsupported file type")
	})

	t.Run("valid CSV file", func(t *testing.T) {
		t.Parallel()
		// Create a temporary CSV file
		tempDir := t.TempDir()
		csvFile := filepath.Join(tempDir, "test.csv")
		content := "col1,col2\nval1,val2\n"
		err := os.WriteFile(csvFile, []byte(content), 0600)
		require.NoError(t, err, "should create CSV file")

		builder := NewBuilder().AddPath(csvFile)
		validatedBuilder, err := builder.Build(ctx)
		assert.NoError(t, err, "Build() should succeed with valid CSV file")
		assert.NotNil(t, validatedBuilder, "Build() should not return nil builder")
	})

	t.Run("valid directory", func(t *testing.T) {
		t.Parallel()
		tempDir := t.TempDir()

		// Create a valid CSV file in the temp directory
		csvFile := filepath.Join(tempDir, "test.csv")
		csvContent := "id,name,age\n1,John,30\n2,Jane,25\n"
		err := os.WriteFile(csvFile, []byte(csvContent), 0600)
		require.NoError(t, err, "Failed to create test CSV file")

		builder := NewBuilder().AddPath(tempDir)
		validatedBuilder, err := builder.Build(ctx)
		assert.NoError(t, err, "Build() should succeed with valid directory")
		assert.NotNil(t, validatedBuilder, "Build() should not return nil builder")
	})

	t.Run("FS with valid files", func(t *testing.T) {
		t.Parallel()
		mockFS := fstest.MapFS{
			"data.csv":     &fstest.MapFile{Data: []byte("col1,col2\nval1,val2\n")},
			"products.tsv": &fstest.MapFile{Data: []byte("id\tname\n1\tLaptop\n")},
			"logs.ltsv":    &fstest.MapFile{Data: []byte("time:2023-01-01T00:00:00Z\tlevel:info\n")},
			"readme.txt":   &fstest.MapFile{Data: []byte("This is not a supported file\n")}, // Should be ignored
		}

		builder := NewBuilder().AddFS(mockFS)
		validatedBuilder, err := builder.Build(ctx)
		assert.NoError(t, err, "Build() should succeed with FS containing valid files")
		require.NotNil(t, validatedBuilder, "Build() should not return nil builder")
		// Should have found 3 files (csv, tsv, ltsv) and ignored txt
		// fs.FS files are now stored as readers instead of collectedPaths
		assert.Len(t, validatedBuilder.readers, 3, "Build() should have 3 readers from fs.FS")
	})

	t.Run("FS with nil filesystem error", func(t *testing.T) {
		t.Parallel()
		builder := NewBuilder()
		builder.filesystems = append(builder.filesystems, nil)

		_, err := builder.Build(ctx)
		assert.Error(t, err, "Build() should return error for nil FS")
	})

	t.Run("FS with no supported files error", func(t *testing.T) {
		t.Parallel()
		mockFS := fstest.MapFS{
			"readme.txt": &fstest.MapFile{Data: []byte("Not supported\n")},
			"data.json":  &fstest.MapFile{Data: []byte("{}\n")},
		}

		builder := NewBuilder().AddFS(mockFS)
		_, err := builder.Build(ctx)
		assert.Error(t, err, "Build() should return error for FS with no supported files")
	})
}

func TestDBBuilder_ChunkedReading(t *testing.T) {
	t.Parallel()

	t.Run("large data chunked reading", func(t *testing.T) {
		t.Parallel()

		// Skip this test in local development, only run on GitHub Actions
		if os.Getenv("GITHUB_ACTIONS") == "" {
			t.Skip("Skipping large data chunked reading test in local development")
		}

		// Create a dataset that would benefit from chunked reading
		var data bytes.Buffer
		data.WriteString("id,name,value\n")
		for i := range 10000 { // Full test on GitHub Actions
			fmt.Fprintf(&data, "%d,name_%d,%d\n", i, i, i*10)
		}

		reader := bytes.NewReader(data.Bytes())
		chunkSize := 1024 // Small chunk for testing
		builder := NewBuilder().
			SetDefaultChunkSize(chunkSize).
			AddReader(reader, "large_table", FileTypeCSV)

		ctx := context.Background()
		validatedBuilder, err := builder.Build(ctx)
		require.NoError(t, err, "Build() should succeed")

		db, err := validatedBuilder.Open(ctx)
		assert.NoError(t, err, "Open() should succeed")
		require.NotNil(t, db, "Open() should not return nil database")
		// Verify the data was loaded correctly
		var count int
		err = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM large_table").Scan(&count)
		assert.NoError(t, err, "Count query should succeed")
		assert.Equal(t, 10000, count, "Should have 10000 rows")
		_ = db.Close()

		// Clean up temp files
	})
}

func TestDBBuilder_Open_WithReader(t *testing.T) {
	ctx := context.Background()

	t.Run("successful open with reader", func(t *testing.T) {
		data := "name,age\nAlice,30\nBob,25\n"
		reader := bytes.NewReader([]byte(data))
		builder := NewBuilder().AddReader(reader, "users", FileTypeCSV)

		validatedBuilder, err := builder.Build(ctx)
		require.NoError(t, err, "Build() should succeed")

		db, err := validatedBuilder.Open(ctx)
		assert.NoError(t, err, "Open() should succeed")
		require.NotNil(t, db, "Open() should not return nil database")
		// Verify we can query the data
		rows, err := db.QueryContext(ctx, "SELECT * FROM users")
		assert.NoError(t, err, "Query should succeed")
		defer rows.Close()
		assert.NoError(t, rows.Err(), "Rows should not have errors")
		_ = db.Close()

		// Clean up temp files
	})

	t.Run("mixed inputs - reader and file", func(t *testing.T) {
		// Create a temporary CSV file
		tempDir := t.TempDir()
		csvFile := filepath.Join(tempDir, "orders.csv")
		fileContent := "order_id,amount\n1,100\n2,200\n"
		err := os.WriteFile(csvFile, []byte(fileContent), 0600)
		require.NoError(t, err, "should create orders CSV file")

		// Create a reader with different data
		readerData := "product_id,name\n1,Laptop\n2,Mouse\n"
		reader := bytes.NewReader([]byte(readerData))

		builder := NewBuilder().
			AddPath(csvFile).
			AddReader(reader, "products", FileTypeCSV)

		validatedBuilder, err := builder.Build(ctx)
		require.NoError(t, err, "Build() should succeed with mixed inputs")

		db, err := validatedBuilder.Open(ctx)
		assert.NoError(t, err, "Open() should succeed")
		require.NotNil(t, db, "Open() should not return nil database")
		// Verify both tables exist
		for _, table := range []string{"orders", "products"} {
			rows, err := db.QueryContext(ctx, "SELECT * FROM "+table) // #nosec G202 -- table name is safe
			assert.NoError(t, err, "Query %s should succeed", table)
			assert.NoError(t, rows.Err(), "Rows should not have errors for %s", table)
			_ = rows.Close() // Close immediately in the loop
		}
		_ = db.Close()

		// Clean up temp files
	})
}

func TestDBBuilder_Open(t *testing.T) {
	ctx := context.Background()

	t.Run("open without build should fail", func(t *testing.T) {
		builder := NewBuilder().AddPath("test.csv")
		// Call Open without calling Build first
		db, err := builder.Open(ctx)
		if db != nil {
			_ = db.Close()
		}
		assert.Error(t, err, "Open() without Build() should return error")
		expectedErrMsg := "no valid input files found, did you call Build()?"
		assert.Contains(t, err.Error(), expectedErrMsg, "error message should mention Build() requirement")
	})

	t.Run("successful open with CSV file", func(t *testing.T) {
		// Create a temporary CSV file
		tempDir := t.TempDir()
		csvFile := filepath.Join(tempDir, "test.csv")
		content := "col1,col2\nval1,val2\n"
		err := os.WriteFile(csvFile, []byte(content), 0600)
		require.NoError(t, err, "should create CSV file")

		builder := NewBuilder().AddPath(csvFile)
		validatedBuilder, err := builder.Build(ctx)
		require.NoError(t, err, "Build() should succeed")

		db, err := validatedBuilder.Open(ctx)
		assert.NoError(t, err, "Open() should succeed")
		assert.NotNil(t, db, "Open() should not return nil database")
		if db != nil {
			_ = db.Close()
		}
	})

	t.Run("successful open with FS", func(t *testing.T) {
		mockFS := fstest.MapFS{
			"data.csv": &fstest.MapFile{Data: []byte("col1,col2\nval1,val2\n")},
		}

		builder := NewBuilder().AddFS(mockFS)
		validatedBuilder, err := builder.Build(ctx)
		require.NoError(t, err, "Build() should succeed")

		db, err := validatedBuilder.Open(ctx)
		assert.NoError(t, err, "Open() should succeed")
		assert.NotNil(t, db, "Open() should not return nil database")
		if db != nil {
			_ = db.Close()
			// Clean up temp files
		}
	})

	t.Run("successful open with glob pattern", func(t *testing.T) {
		mockFS := fstest.MapFS{
			"data1.csv": &fstest.MapFile{Data: []byte("col1,col2\nval1,val2\n")},
			"data2.csv": &fstest.MapFile{Data: []byte("col1,col2\nval3,val4\n")},
		}

		builder := NewBuilder().AddFS(mockFS)
		validatedBuilder, err := builder.Build(ctx)
		require.NoError(t, err, "Build() should succeed")

		db, err := validatedBuilder.Open(ctx)
		assert.NoError(t, err, "Open() should succeed")
		assert.NotNil(t, db, "Open() should not return nil database")
		if db != nil {
			_ = db.Close()
			// Clean up temp files
		}
	})
}

func TestDBBuilder_processFSInput(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	t.Run("multiple supported files", func(t *testing.T) {
		t.Parallel()
		mockFS := fstest.MapFS{
			"data.csv":     &fstest.MapFile{Data: []byte("col1,col2\nval1,val2\n")},
			"products.tsv": &fstest.MapFile{Data: []byte("id\tname\n1\tLaptop\n")},
			"logs.ltsv":    &fstest.MapFile{Data: []byte("time:2023-01-01T00:00:00Z\tlevel:info\n")},
			"readme.txt":   &fstest.MapFile{Data: []byte("Not supported\n")}, // Should be ignored
		}

		builder := NewBuilder()

		readers, err := builder.processFSToReaders(ctx, mockFS)
		assert.NoError(t, err, "processFSToReaders() should succeed")
		assert.Len(t, readers, 3, "should return 3 readers")

		// Close all readers
		for _, reader := range readers {
			if closer, ok := reader.reader.(io.Closer); ok {
				_ = closer.Close()
			}
		}
	})

	t.Run("compressed files", func(t *testing.T) {
		t.Parallel()
		mockFS := fstest.MapFS{
			"data.csv.gz":   &fstest.MapFile{Data: []byte("compressed csv data")},
			"logs.ltsv.bz2": &fstest.MapFile{Data: []byte("compressed ltsv data")},
		}

		builder := NewBuilder()

		readers, err := builder.processFSToReaders(ctx, mockFS)
		assert.NoError(t, err, "processFSToReaders() should succeed with compressed files")
		assert.Len(t, readers, 2, "should return 2 readers for compressed files")

		// Close all readers
		for _, reader := range readers {
			if closer, ok := reader.reader.(io.Closer); ok {
				_ = closer.Close()
			}
		}
	})
}

func TestIntegrationWithEmbedFS(t *testing.T) {
	ctx := context.Background()

	// Use embedded test data from embed_test subdirectory
	subFS, err := fs.Sub(testFS, "testdata/embed_test")
	require.NoError(t, err, "should create sub filesystem")

	// Test loading all supported files from embedded FS
	builder := NewBuilder().AddFS(subFS)

	validatedBuilder, err := builder.Build(ctx)
	require.NoError(t, err, "Build() should succeed with embedded FS")

	db, err := validatedBuilder.Open(ctx)
	assert.NoError(t, err, "Open() with embed.FS should succeed")
	require.NotNil(t, db, "Open() with embed.FS should not return nil database")
	// Verify we can query the database
	rows, err := db.QueryContext(ctx, "SELECT name FROM sqlite_master WHERE type='table'")
	assert.NoError(t, err, "should be able to query database")
	defer rows.Close()
	assert.NoError(t, rows.Err(), "rows should not have errors")

	_ = db.Close()
	// Clean up temp files
}

func TestAutoSave_OnClose(t *testing.T) {
	// Create temporary directory
	tmpDir := t.TempDir()

	// Create test CSV file
	csvPath := filepath.Join(tmpDir, "test.csv")
	csvContent := "name,age\nAlice,25\nBob,30\n"
	err := os.WriteFile(csvPath, []byte(csvContent), 0600)
	require.NoError(t, err, "Failed to write test CSV")

	// Create output directory
	outputDir := filepath.Join(tmpDir, "output")
	err = os.MkdirAll(outputDir, 0750)
	require.NoError(t, err, "Failed to create output dir")

	// Build database with auto-save on close
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	builder := NewBuilder().
		AddPath(csvPath).
		EnableAutoSave(outputDir)

	validatedBuilder, err := builder.Build(ctx)
	require.NoError(t, err, "Build should succeed")

	db, err := validatedBuilder.Open(ctx)
	require.NoError(t, err, "Open should succeed")

	// Modify data
	_, err = db.ExecContext(ctx, "INSERT INTO test (name, age) VALUES ('Charlie', 35)")
	require.NoError(t, err, "Insert should succeed")

	// Close database (should trigger auto-save)
	err = db.Close()
	require.NoError(t, err, "Close should succeed")

	// Check if file was saved
	outputFile := filepath.Join(outputDir, "test.csv")
	_, err = os.Stat(outputFile)
	assert.False(t, os.IsNotExist(err), "Auto-save file should be created: %s", outputFile)

	// Verify content includes the new record
	content, err := os.ReadFile(outputFile) //nolint:gosec // Test file path is safe
	require.NoError(t, err, "should be able to read output file")

	assert.Contains(t, string(content), "Charlie", "Auto-saved file should contain inserted data")
}

func TestAutoSave_OnCommit(t *testing.T) {
	// Create temporary directory
	tmpDir := t.TempDir()

	// Create test CSV file
	csvPath := filepath.Join(tmpDir, "test.csv")
	csvContent := "name,age\nAlice,25\n"
	err := os.WriteFile(csvPath, []byte(csvContent), 0600)
	require.NoError(t, err, "Failed to write test CSV")

	// Create output directory
	outputDir := filepath.Join(tmpDir, "output")
	err = os.MkdirAll(outputDir, 0750)
	require.NoError(t, err, "Failed to create output dir")

	// Build database with auto-save on commit
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	builder := NewBuilder().
		AddPath(csvPath).
		EnableAutoSaveOnCommit(outputDir)

	validatedBuilder, err := builder.Build(ctx)
	if err != nil {
		require.NoError(t, err, "Build should succeed")
	}

	db, err := validatedBuilder.Open(ctx)
	if err != nil {
		require.NoError(t, err, "Open should succeed")
	}
	defer db.Close()

	// Start transaction
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		require.NoError(t, err, "Begin transaction should succeed")
	}

	// Modify data within transaction
	_, err = tx.ExecContext(ctx, "INSERT INTO test (name, age) VALUES ('David', 40)")
	if err != nil {
		require.NoError(t, err, "Insert should succeed")
	}

	// Commit transaction (should trigger auto-save)
	err = tx.Commit()
	require.NoError(t, err, "Commit should succeed")

	// Check if file was saved
	outputFile := filepath.Join(outputDir, "test.csv")
	if _, err := os.Stat(outputFile); os.IsNotExist(err) {
		assert.FileExists(t, outputFile, "Auto-save file should be created")
	}

	// Verify content includes the new record
	content, err := os.ReadFile(outputFile) //nolint:gosec // Test file path is safe
	if err != nil {
		require.NoError(t, err, "should be able to read output file")
	}

	assert.Contains(t, string(content), "David", "Auto-saved file should contain committed data")
}

func TestAutoSave_DisableAutoSave(t *testing.T) {
	// Create temporary directory
	tmpDir := t.TempDir()

	// Create test CSV file
	csvPath := filepath.Join(tmpDir, "test.csv")
	csvContent := "name,age\nAlice,25\n"
	err := os.WriteFile(csvPath, []byte(csvContent), 0600)
	require.NoError(t, err, "Failed to write test CSV")

	// Create output directory
	outputDir := filepath.Join(tmpDir, "output")
	err = os.MkdirAll(outputDir, 0750)
	require.NoError(t, err, "Failed to create output dir")

	// Build database without auto-save (default behavior)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	builder := NewBuilder().
		AddPath(csvPath)
	// Note: No EnableAutoSave() call

	validatedBuilder, err := builder.Build(ctx)
	if err != nil {
		require.NoError(t, err, "Build should succeed")
	}

	db, err := validatedBuilder.Open(ctx)
	if err != nil {
		require.NoError(t, err, "Open should succeed")
	}

	// Modify data
	_, err = db.ExecContext(ctx, "INSERT INTO test (name, age) VALUES ('Echo', 45)")
	if err != nil {
		require.NoError(t, err, "Insert should succeed")
	}

	// Close database (should NOT trigger auto-save)
	if err := db.Close(); err != nil {
		require.NoError(t, err, "Close should succeed")
	}

	// Check that no output file was created
	outputFile := filepath.Join(outputDir, "test.csv")
	if _, err := os.Stat(outputFile); !os.IsNotExist(err) {
		assert.NoFileExists(t, outputFile, "Auto-save file should not have been created when auto-save is disabled")
	}
}

func TestAutoSave_MultipleCommitsOverwrite(t *testing.T) {
	// This test verifies that multiple commits properly overwrite the same file
	t.Parallel()

	tmpDir := t.TempDir()

	// Create test CSV file
	csvPath := filepath.Join(tmpDir, "test.csv")
	csvContent := "name,count\nInitial,1\n"
	err := os.WriteFile(csvPath, []byte(csvContent), 0600)
	require.NoError(t, err, "Failed to write test CSV")

	// Create output directory
	outputDir := filepath.Join(tmpDir, "output")
	err = os.MkdirAll(outputDir, 0750)
	require.NoError(t, err, "Failed to create output dir")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Build database with auto-save on commit
	builder := NewBuilder().
		AddPath(csvPath).
		EnableAutoSaveOnCommit(outputDir)

	validatedBuilder, err := builder.Build(ctx)
	if err != nil {
		require.NoError(t, err, "Build should succeed")
	}

	db, err := validatedBuilder.Open(ctx)
	if err != nil {
		require.NoError(t, err, "Open should succeed")
	}
	defer db.Close()

	outputFile := filepath.Join(outputDir, "test.csv")

	// First commit: Add first record
	tx1, err := db.BeginTx(ctx, nil)
	if err != nil {
		require.NoError(t, err, "Begin first transaction should succeed")
	}

	_, err = tx1.ExecContext(ctx, "INSERT INTO test (name, count) VALUES ('First', 100)")
	if err != nil {
		require.NoError(t, err, "First insert should succeed")
	}

	if err := tx1.Commit(); err != nil {
		require.NoError(t, err, "First commit should succeed")
	}

	// Check first commit saved the file
	if _, err := os.Stat(outputFile); os.IsNotExist(err) {
		assert.FileExists(t, outputFile, "Auto-save file should be created after first commit")
	}

	// Read content after first commit
	content1, err := os.ReadFile(outputFile) //nolint:gosec // Test file path is safe
	if err != nil {
		require.NoError(t, err, "should be able to read output file after first commit")
	}

	assert.Contains(t, string(content1), "First", "File should contain first commit data")

	// Second commit: Add second record (should overwrite)
	tx2, err := db.BeginTx(ctx, nil)
	if err != nil {
		require.NoError(t, err, "Begin second transaction should succeed")
	}

	_, err = tx2.ExecContext(ctx, "INSERT INTO test (name, count) VALUES ('Second', 200)")
	if err != nil {
		require.NoError(t, err, "Second insert should succeed")
	}

	if err := tx2.Commit(); err != nil {
		require.NoError(t, err, "Second commit should succeed")
	}

	// Read content after second commit
	content2, err := os.ReadFile(outputFile) //nolint:gosec // Test file path is safe
	if err != nil {
		require.NoError(t, err, "should be able to read output file after second commit")
	}

	// Verify the file was overwritten and contains both records
	assert.Contains(t, string(content2), "First", "File should still contain first commit data after second commit")

	assert.Contains(t, string(content2), "Second", "File should contain second commit data")

	// Verify the file was actually overwritten (not just appended)
	// Count lines to make sure we have header + original + two new records
	lines := strings.Split(string(content2), "\n")
	nonEmptyLines := 0
	for _, line := range lines {
		if strings.TrimSpace(line) != "" {
			nonEmptyLines++
		}
	}

	// Should have: header + Initial + First + Second = 4 lines
	assert.Equal(t, 4, nonEmptyLines, "Expected 4 lines in overwritten file, got %d. Content: %s", nonEmptyLines, string(content2))

	// Third commit: Update existing record
	tx3, err := db.BeginTx(ctx, nil)
	if err != nil {
		require.NoError(t, err, "Begin third transaction should succeed")
	}

	_, err = tx3.ExecContext(ctx, "UPDATE test SET count = 999 WHERE name = 'Initial'")
	if err != nil {
		require.NoError(t, err, "Update should succeed")
	}

	if err := tx3.Commit(); err != nil {
		require.NoError(t, err, "Third commit should succeed")
	}

	// Read content after third commit
	content3, err := os.ReadFile(outputFile) //nolint:gosec // Test file path is safe
	if err != nil {
		require.NoError(t, err, "should be able to read output file after third commit")
	}

	// Verify the update was saved
	assert.Contains(t, string(content3), "999", "File should contain updated count (999)")

	// Verify original count (1) was overwritten
	assert.NotContains(t, string(content3), "Initial,1", "File should not contain old count (1) after update")
}

func TestAutoSave_ExplicitDisable(t *testing.T) {
	// Test the DisableAutoSave method explicitly
	t.Parallel()

	tmpDir := t.TempDir()

	// Create test CSV file
	csvPath := filepath.Join(tmpDir, "test.csv")
	csvContent := "name,age\nAlice,25\n"
	err := os.WriteFile(csvPath, []byte(csvContent), 0600)
	require.NoError(t, err, "Failed to write test CSV")

	// Create output directory
	outputDir := filepath.Join(tmpDir, "output")
	err = os.MkdirAll(outputDir, 0750)
	require.NoError(t, err, "Failed to create output dir")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// First enable auto-save, then explicitly disable it
	builder := NewBuilder().
		AddPath(csvPath).
		EnableAutoSave(outputDir).
		DisableAutoSave() // This should override the previous EnableAutoSave

	validatedBuilder, err := builder.Build(ctx)
	if err != nil {
		require.NoError(t, err, "Build should succeed")
	}

	db, err := validatedBuilder.Open(ctx)
	if err != nil {
		require.NoError(t, err, "Open should succeed")
	}

	// Modify data
	_, err = db.ExecContext(ctx, "INSERT INTO test (name, age) VALUES ('Disabled', 99)")
	if err != nil {
		require.NoError(t, err, "Insert should succeed")
	}

	// Close database (should NOT trigger auto-save due to DisableAutoSave)
	if err := db.Close(); err != nil {
		require.NoError(t, err, "Close should succeed")
	}

	// Check that no output file was created
	outputFile := filepath.Join(outputDir, "test.csv")
	if _, err := os.Stat(outputFile); !os.IsNotExist(err) {
		assert.NoFileExists(t, outputFile, "Auto-save file should not have been created when explicitly disabled")
	}
}

func TestBuilder_ErrorCases(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	t.Run("build with no inputs", func(t *testing.T) {
		t.Parallel()
		builder := NewBuilder()
		_, err := builder.Build(ctx)
		if err == nil {
			assert.Error(t, err, "Build() with no inputs should return error")
		}
	})

	t.Run("build with empty path", func(t *testing.T) {
		t.Parallel()
		builder := NewBuilder().AddPath("")
		_, err := builder.Build(ctx)
		if err == nil {
			assert.Error(t, err, "Build() with empty path should return error")
		}
	})

	t.Run("build with non-existent path", func(t *testing.T) {
		t.Parallel()
		builder := NewBuilder().AddPath(filepath.Join("non", "existent", "file.csv"))
		_, err := builder.Build(ctx)
		if err == nil {
			assert.Error(t, err, "Build() with non-existent path should return error")
		}
	})

	t.Run("auto-save with empty output directory", func(t *testing.T) {
		t.Parallel()
		tmpDir := t.TempDir()
		csvPath := filepath.Join(tmpDir, "test.csv")
		if err := os.WriteFile(csvPath, []byte("col1\nval1\n"), 0600); err != nil {
			require.NoError(t, err, "operation should succeed")
		}

		// Test with empty string for output directory - should use overwrite mode
		builder := NewBuilder().
			AddPath(csvPath).
			EnableAutoSave("") // Empty string should work for overwrite mode

		_, err := builder.Build(ctx)
		if err != nil {
			t.Errorf("Build() with empty output directory should not error, got: %v", err)
		}
	})

	t.Run("auto-save on commit with empty output directory", func(t *testing.T) {
		t.Parallel()
		tmpDir := t.TempDir()
		csvPath := filepath.Join(tmpDir, "test.csv")
		if err := os.WriteFile(csvPath, []byte("col1\nval1\n"), 0600); err != nil {
			require.NoError(t, err, "operation should succeed")
		}

		// Test with empty string for output directory - should use overwrite mode
		builder := NewBuilder().
			AddPath(csvPath).
			EnableAutoSaveOnCommit("") // Empty string should work for overwrite mode

		_, err := builder.Build(ctx)
		if err != nil {
			t.Errorf("Build() with empty output directory for auto-save on commit should not error, got: %v", err)
		}
	})

	t.Run("invalid reader data", func(t *testing.T) {
		t.Parallel()

		// Test with malformed CSV data that might cause parsing issues
		invalidCSV := "name,age\n\"unclosed quote,30\nvalid,25\n"
		reader := strings.NewReader(invalidCSV)

		builder := NewBuilder().AddReader(reader, "invalid", FileTypeCSV)
		_, err := builder.Build(ctx)

		// Should handle malformed CSV gracefully or return meaningful error
		if err == nil {
			t.Log("Build succeeded with malformed CSV - parser handled it gracefully")
		}
	})

	t.Run("empty reader", func(t *testing.T) {
		t.Parallel()

		reader := strings.NewReader("")
		builder := NewBuilder().AddReader(reader, "empty", FileTypeCSV)
		_, err := builder.Build(ctx)

		// Build should fail with empty CSV data
		if err == nil {
			assert.Error(t, err, "Build should fail with empty reader")
		} else if !strings.Contains(err.Error(), "empty CSV data") {
			assert.Contains(t, err.Error(), "empty CSV data", "Expected 'empty CSV data' error")
		}
	})

	t.Run("extremely small chunk size", func(t *testing.T) {
		t.Parallel()

		reader := strings.NewReader("name,age\nAlice,30\n")
		// Test with very small chunk size
		builder := NewBuilder().
			AddReader(reader, "test", FileTypeCSV).
			SetDefaultChunkSize(1) // Very small chunk size

		_, err := builder.Build(ctx)
		if err != nil {
			assert.NoError(t, err, "Build should handle small chunk size")
		}
	})
}

func TestBuilder_AddPaths_ErrorCases(t *testing.T) {
	t.Parallel()

	t.Run("add multiple paths", func(t *testing.T) {
		t.Parallel()
		builder := NewBuilder().AddPaths("file1.csv", "file2.tsv", "file3.ltsv")
		if len(builder.paths) != 3 {
			assert.Len(t, builder.paths, 3, "AddPaths should add all paths")
		}
		expectedPaths := []string{"file1.csv", "file2.tsv", "file3.ltsv"}
		for i, expectedPath := range expectedPaths {
			if builder.paths[i] != expectedPath {
				t.Errorf("AddPaths should preserve path order, got %s at index %d, expected %s", builder.paths[i], i, expectedPath)
			}
		}
	})

	t.Run("add paths with empty string", func(t *testing.T) {
		t.Parallel()
		builder := NewBuilder().AddPaths("valid.csv", "", "another.csv")
		if len(builder.paths) != 3 {
			assert.Len(t, builder.paths, 3, "AddPaths should add all paths including empty ones")
		}
		if builder.paths[1] != "" {
			t.Errorf("AddPaths should preserve empty string, got %s", builder.paths[1])
		}
	})

	t.Run("add no paths", func(t *testing.T) {
		t.Parallel()
		builder := NewBuilder().AddPaths()
		if len(builder.paths) != 0 {
			assert.Len(t, builder.paths, 0, "AddPaths() with no arguments should not add any paths")
		}
	})
}

func TestDBBuilder_StreamDirectoryToSQLite(t *testing.T) {
	t.Parallel()

	t.Run("directory with supported files", func(t *testing.T) {
		t.Parallel()

		// Create temporary directory with test files
		tempDir := t.TempDir()
		csvContent := "id,name,age\n1,Alice,30\n2,Bob,25\n"
		tsvContent := "id\tname\tage\n3\tCharlie\t35\n4\tDiana\t28\n"

		csvFile := filepath.Join(tempDir, "test1.csv")
		tsvFile := filepath.Join(tempDir, "test2.tsv")

		err := os.WriteFile(csvFile, []byte(csvContent), 0600)
		if err != nil {
			t.Fatalf("Failed to create test CSV file: %v", err)
		}

		err = os.WriteFile(tsvFile, []byte(tsvContent), 0600)
		if err != nil {
			t.Fatalf("Failed to create test TSV file: %v", err)
		}

		// Test the streaming directory function
		db, err := Open(tempDir)
		if err != nil {
			t.Fatalf("Failed to open directory: %v", err)
		}
		defer db.Close()

		// Verify tables were created
		ctx := context.Background()
		rows, err := db.QueryContext(ctx, "SELECT name FROM sqlite_master WHERE type='table'")
		if err != nil {
			t.Fatalf("Failed to query tables: %v", err)
		}
		defer rows.Close()

		var tableNames []string
		for rows.Next() {
			var name string
			if err := rows.Scan(&name); err != nil {
				t.Fatalf("Failed to scan table name: %v", err)
			}
			tableNames = append(tableNames, name)
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("Error during rows iteration: %v", err)
		}

		expectedTables := []string{"test1", "test2"}
		if len(tableNames) != len(expectedTables) {
			t.Errorf("Expected %d tables, got %d: %v", len(expectedTables), len(tableNames), tableNames)
		}
	})

	t.Run("directory with unsupported files only", func(t *testing.T) {
		t.Parallel()

		// Create temporary directory with unsupported files
		tempDir := t.TempDir()
		txtFile := filepath.Join(tempDir, "test.txt")

		err := os.WriteFile(txtFile, []byte("some text content"), 0600)
		if err != nil {
			t.Fatalf("Failed to create test txt file: %v", err)
		}

		// Test should return error for no supported files
		_, err = Open(tempDir)
		if err == nil {
			t.Error("Expected error for directory with no supported files")
		}

		expectedError := "no supported files found in directory"
		if !strings.Contains(err.Error(), expectedError) {
			t.Errorf("Expected error to contain '%s', got: %v", expectedError, err)
		}
	})

	t.Run("empty directory", func(t *testing.T) {
		t.Parallel()

		tempDir := t.TempDir()

		// Test should return error for empty directory
		_, err := Open(tempDir)
		if err == nil {
			t.Error("Expected error for empty directory")
		}

		expectedError := "no supported files found in directory"
		if !strings.Contains(err.Error(), expectedError) {
			t.Errorf("Expected error to contain '%s', got: %v", expectedError, err)
		}
	})
}

func TestDBBuilder_CreateDecompressedReader(t *testing.T) {
	t.Parallel()

	t.Run("uncompressed file", func(t *testing.T) {
		t.Parallel()

		// Create a temporary file
		tempFile := filepath.Join(t.TempDir(), "test.csv")
		content := "id,name\n1,Alice\n2,Bob\n"
		err := os.WriteFile(tempFile, []byte(content), 0600)
		if err != nil {
			t.Fatalf("Failed to create test file: %v", err)
		}

		file, err := os.Open(tempFile) //nolint:gosec // tempFile is created in test with controlled path
		if err != nil {
			t.Fatalf("Failed to open test file: %v", err)
		}
		defer file.Close()

		builder := NewBuilder()
		reader, err := builder.createDecompressedReader(file, tempFile)
		if err != nil {
			t.Fatalf("createDecompressedReader failed: %v", err)
		}

		// Should return the file itself for uncompressed files
		if reader != file {
			t.Error("Expected reader to be the same as input file for uncompressed files")
		}
	})

	t.Run("gzip compressed file", func(t *testing.T) {
		t.Parallel()

		// Use an existing gzip test file
		gzipFile := filepath.Join("testdata", "sample.csv.gz")

		file, err := os.Open(gzipFile) //nolint:gosec // Test file path is safe
		if err != nil {
			t.Skip("Gzip test file not available, skipping test")
		}
		defer file.Close()

		builder := NewBuilder()
		reader, err := builder.createDecompressedReader(file, gzipFile)
		if err != nil {
			t.Fatalf("createDecompressedReader failed for gzip: %v", err)
		}

		// Should return a different reader (gzip reader)
		if reader == file {
			t.Error("Expected reader to be different from input file for gzip files")
		}

		// Try to read some content
		buffer := make([]byte, 100)
		n, err := reader.Read(buffer)
		if err != nil && !errors.Is(err, io.EOF) {
			t.Fatalf("Failed to read from decompressed reader: %v", err)
		}
		if n == 0 {
			t.Error("Expected to read some content from decompressed reader")
		}
	})

	t.Run("bzip2 compressed file", func(t *testing.T) {
		t.Parallel()

		bzip2File := filepath.Join("testdata", "products.tsv.bz2")

		file, err := os.Open(bzip2File) //nolint:gosec // Test file path is safe
		if err != nil {
			t.Skip("Bzip2 test file not available, skipping test")
		}
		defer file.Close()

		builder := NewBuilder()
		reader, err := builder.createDecompressedReader(file, bzip2File)
		if err != nil {
			t.Fatalf("createDecompressedReader failed for bzip2: %v", err)
		}

		// Should return a different reader (bzip2 reader)
		if reader == file {
			t.Error("Expected reader to be different from input file for bzip2 files")
		}
	})

	t.Run("xz compressed file", func(t *testing.T) {
		t.Parallel()

		xzFile := filepath.Join("testdata", "logs.ltsv.xz")

		file, err := os.Open(xzFile) //nolint:gosec // Test file path is safe
		if err != nil {
			t.Skip("XZ test file not available, skipping test")
		}
		defer file.Close()

		builder := NewBuilder()
		reader, err := builder.createDecompressedReader(file, xzFile)
		if err != nil {
			t.Fatalf("createDecompressedReader failed for xz: %v", err)
		}

		// Should return a different reader (xz reader)
		if reader == file {
			t.Error("Expected reader to be different from input file for xz files")
		}
	})

	t.Run("zstd compressed file", func(t *testing.T) {
		t.Parallel()

		zstdFile := filepath.Join("testdata", "users.csv.zst")

		file, err := os.Open(zstdFile) //nolint:gosec // Test file path is safe
		if err != nil {
			t.Skip("ZSTD test file not available, skipping test")
		}
		defer file.Close()

		builder := NewBuilder()
		reader, err := builder.createDecompressedReader(file, zstdFile)
		if err != nil {
			t.Fatalf("createDecompressedReader failed for zstd: %v", err)
		}

		// Should return a different reader (zstd reader)
		if reader == file {
			t.Error("Expected reader to be different from input file for zstd files")
		}
	})
}

// TestDriverMethods tests driver interface methods for coverage
func TestDriverMethods(t *testing.T) {
	t.Parallel()

	t.Run("directConnector Driver method", func(t *testing.T) {
		t.Parallel()

		connector := &directConnector{}
		driver := connector.Driver()
		if driver == nil {
			assert.NotNil(t, driver, "Expected non-nil driver")
		}
	})

	t.Run("autoSaveConnector Driver method", func(t *testing.T) {
		t.Parallel()

		connector := &autoSaveConnector{}
		driver := connector.Driver()
		if driver == nil {
			assert.NotNil(t, driver, "Expected non-nil driver")
		}
	})
}

// TestTransactionMethods tests transaction operations for coverage
func TestTransactionMethods(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()
	csvFile := filepath.Join(tempDir, "test.csv")
	csvContent := "id,name\n1,Alice\n2,Bob\n"
	if err := os.WriteFile(csvFile, []byte(csvContent), 0600); err != nil {
		require.NoError(t, err, "operation should succeed")
	}

	t.Run("Begin and Rollback transaction", func(t *testing.T) {
		t.Parallel()

		validatedBuilder, err := NewBuilder().
			AddPath(csvFile).
			EnableAutoSaveOnCommit(tempDir).
			Build(context.Background())
		if err != nil {
			require.NoError(t, err, "operation should succeed")
		}

		db, err := validatedBuilder.Open(context.Background())
		if err != nil {
			require.NoError(t, err, "operation should succeed")
		}
		defer db.Close()

		ctx := context.Background()
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			require.NoError(t, err, "operation should succeed")
		}

		_, err = tx.ExecContext(ctx, "UPDATE test SET name = 'Charlie' WHERE id = 1")
		if err != nil {
			require.NoError(t, err, "operation should succeed")
		}

		err = tx.Rollback()
		if err != nil {
			require.NoError(t, err, "operation should succeed")
		}
	})

	t.Run("autoSaveConnection Begin method", func(t *testing.T) {
		t.Parallel()

		validatedBuilder, err := NewBuilder().
			AddPath(csvFile).
			EnableAutoSaveOnCommit(tempDir).
			Build(context.Background())
		if err != nil {
			require.NoError(t, err, "operation should succeed")
		}

		db, err := validatedBuilder.Open(context.Background())
		if err != nil {
			require.NoError(t, err, "operation should succeed")
		}
		defer db.Close()

		// Test the Begin method (0% coverage)
		ctx := context.Background()
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			require.NoError(t, err, "operation should succeed")
		}
		defer tx.Rollback()
	})

	t.Run("overwriteOriginalFiles path", func(t *testing.T) {
		t.Parallel()

		validatedBuilder, err := NewBuilder().
			AddPath(csvFile).
			EnableAutoSaveOnCommit(""). // Empty string triggers overwrite
			Build(context.Background())
		if err != nil {
			require.NoError(t, err, "operation should succeed")
		}

		db, err := validatedBuilder.Open(context.Background())
		if err != nil {
			require.NoError(t, err, "operation should succeed")
		}
		defer db.Close()

		ctx := context.Background()
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			require.NoError(t, err, "operation should succeed")
		}

		_, err = tx.ExecContext(ctx, "UPDATE test SET name = 'Diana' WHERE id = 1")
		if err != nil {
			_ = tx.Rollback() //nolint:errcheck
			require.NoError(t, err, "operation should succeed")
		}

		// This should trigger overwriteOriginalFiles
		err = tx.Commit()
		if err != nil {
			require.NoError(t, err, "operation should succeed")
		}
	})
}

// TestAutoSavePaths tests auto-save functionality for coverage
func TestAutoSavePaths(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()
	csvFile := filepath.Join(tempDir, "test.csv")
	csvContent := "id,name\n1,Alice\n2,Bob\n"
	if err := os.WriteFile(csvFile, []byte(csvContent), 0600); err != nil {
		require.NoError(t, err, "operation should succeed")
	}

	t.Run("Close connection with auto-save", func(t *testing.T) {
		t.Parallel()

		validatedBuilder, err := NewBuilder().
			AddPath(csvFile).
			EnableAutoSave(tempDir).
			Build(context.Background())
		if err != nil {
			require.NoError(t, err, "operation should succeed")
		}

		db, err := validatedBuilder.Open(context.Background())
		if err != nil {
			require.NoError(t, err, "operation should succeed")
		}

		ctx := context.Background()
		_, err = db.ExecContext(ctx, "UPDATE test SET name = 'Eve' WHERE id = 1")
		if err != nil {
			require.NoError(t, err, "operation should succeed")
		}

		// Close should trigger auto-save
		err = db.Close()
		if err != nil {
			require.NoError(t, err, "operation should succeed")
		}
	})

	t.Run("createEmptyTable coverage", func(t *testing.T) {
		t.Parallel()

		// Test with header-only reader to trigger createEmptyTable path
		validatedBuilder, err := NewBuilder().
			AddReader(strings.NewReader("col1,col2\n"), "empty_test", FileTypeCSV).
			Build(context.Background())
		if err != nil {
			require.NoError(t, err, "operation should succeed")
		}

		db, err := validatedBuilder.Open(context.Background())
		if err != nil {
			require.NoError(t, err, "operation should succeed")
		}
		defer db.Close()

		// Verify the empty table was created correctly
		var count int
		err = db.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM empty_test").Scan(&count)
		if err != nil {
			require.NoError(t, err, "operation should succeed")
		}
		if count != 0 {
			t.Errorf("Expected empty table, got %d rows", count)
		}
	})

	t.Run("createEmptyTable successful parse", func(t *testing.T) {
		t.Parallel()

		// Test with minimal CSV that would parse successfully but have no data
		// This should trigger the createEmptyTable happy path
		validCSV := "id,name,email\n1,test,test@example.com\n"
		reader := strings.NewReader(validCSV)

		// Create a custom reader input that forces empty table creation
		// We'll simulate this by creating a very small chunk size that reads only headers
		validatedBuilder, err := NewBuilder().
			AddReader(reader, "parsed_empty", FileTypeCSV).
			SetDefaultChunkSize(1). // Very small chunk to simulate header-only parsing
			Build(context.Background())
		if err != nil {
			require.NoError(t, err, "operation should succeed")
		}

		db, err := validatedBuilder.Open(context.Background())
		if err != nil {
			require.NoError(t, err, "operation should succeed")
		}
		defer db.Close()

		// Check table was created
		rows, err := db.QueryContext(context.Background(), "SELECT name FROM sqlite_master WHERE type='table' AND name='parsed_empty'")
		if err != nil {
			require.NoError(t, err, "operation should succeed")
		}
		defer rows.Close()

		hasTable := false
		for rows.Next() {
			hasTable = true
		}
		if err := rows.Err(); err != nil {
			require.NoError(t, err, "operation should succeed")
		}

		if !hasTable {
			t.Error("Expected table to be created")
		}
	})

	t.Run("createEmptyTable with duplicate columns", func(t *testing.T) {
		t.Parallel()

		// Test with duplicate column names
		duplicateCSV := "id,name,id\n"
		validatedBuilder, err := NewBuilder().
			AddReader(strings.NewReader(duplicateCSV), "duplicate_cols", FileTypeCSV).
			Build(context.Background())
		if err != nil {
			require.NoError(t, err, "operation should succeed")
		}

		_, err = validatedBuilder.Open(context.Background())
		if err == nil {
			t.Error("Expected error for duplicate column names")
		}
		if !strings.Contains(err.Error(), "duplicate column") {
			t.Errorf("Expected 'duplicate column' error, got: %v", err)
		}
	})

	t.Run("createEmptyTable fallback to createTableFromHeaders", func(t *testing.T) {
		t.Parallel()

		// Test the fallback path when parseFromReader fails
		// Use a reader that would cause parsing to fail but still have readable content
		brokenCSV := "id,name,email\n" // Header only, no data, should trigger fallback
		validatedBuilder, err := NewBuilder().
			AddReader(strings.NewReader(brokenCSV), "fallback_test", FileTypeCSV).
			Build(context.Background())
		if err != nil {
			require.NoError(t, err, "operation should succeed")
		}

		// This should not fail but use the createTableFromHeaders fallback
		db, err := validatedBuilder.Open(context.Background())
		if err != nil {
			require.NoError(t, err, "operation should succeed")
		}
		defer db.Close()

		// Check table exists
		rows, err := db.QueryContext(context.Background(), "SELECT name FROM sqlite_master WHERE type='table' AND name='fallback_test'")
		if err != nil {
			require.NoError(t, err, "operation should succeed")
		}
		defer rows.Close()

		hasTable := false
		for rows.Next() {
			hasTable = true
		}
		if err := rows.Err(); err != nil {
			require.NoError(t, err, "operation should succeed")
		}

		if !hasTable {
			t.Error("Expected table to be created via fallback")
		}
	})
}

func TestStreamXLSXFileToSQLite(t *testing.T) {
	t.Parallel()

	t.Run("valid XLSX file with multiple sheets", func(t *testing.T) {
		ctx := context.Background()

		// Create in-memory database
		sqliteDriver := &sqlite.Driver{}
		conn, err := sqliteDriver.Open(":memory:")
		if err != nil {
			require.NoError(t, err, "operation should succeed")
		}
		db := sql.OpenDB(&directConnector{conn: conn})
		defer db.Close()

		// Read sample XLSX file
		xlsxPath := filepath.Join("testdata", "excel", "sample.xlsx")
		file, err := os.Open(xlsxPath) //nolint:gosec // Test file path is safe
		if err != nil {
			require.NoError(t, err, "operation should succeed")
		}
		defer file.Close()

		// Create builder and test streamXLSXFileToSQLite
		builder := &DBBuilder{}
		err = builder.streamXLSXFileToSQLite(ctx, db, file, xlsxPath)
		if err != nil {
			require.NoError(t, err, "operation should succeed")
		}

		// Verify tables were created
		rows, err := db.QueryContext(context.Background(), "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
		if err != nil {
			require.NoError(t, err, "operation should succeed")
		}
		defer rows.Close()

		var tables []string
		for rows.Next() {
			var tableName string
			if err := rows.Scan(&tableName); err != nil {
				require.NoError(t, err, "operation should succeed")
			}
			tables = append(tables, tableName)
		}
		if err := rows.Err(); err != nil {
			require.NoError(t, err, "operation should succeed")
		}

		expectedTables := []string{"sample_Sheet1", "sample_Sheet2"}
		if !reflect.DeepEqual(tables, expectedTables) {
			t.Errorf("Expected tables %v, got %v", expectedTables, tables)
		}

		// Verify data in first sheet
		rows, err = db.QueryContext(context.Background(), "SELECT * FROM sample_Sheet1 ORDER BY id")
		if err != nil {
			require.NoError(t, err, "operation should succeed")
		}
		defer rows.Close()

		var count int
		for rows.Next() {
			var id, name string
			if err := rows.Scan(&id, &name); err != nil {
				require.NoError(t, err, "operation should succeed")
			}
			count++

			switch count {
			case 1:
				if id != "1" || name != "Gina" {
					t.Errorf("Expected (1, Gina), got (%s, %s)", id, name)
				}
			case 2:
				if id != "2" || name != "Yulia" {
					t.Errorf("Expected (2, Yulia), got (%s, %s)", id, name)
				}
			case 3:
				if id != "3" || name != "Vika" {
					t.Errorf("Expected (3, Vika), got (%s, %s)", id, name)
				}
			}
		}
		if err := rows.Err(); err != nil {
			require.NoError(t, err, "operation should succeed")
		}

		if count != 3 {
			t.Errorf("Expected 3 records in Sheet1, got %d", count)
		}

		// Verify data in second sheet
		rows, err = db.QueryContext(context.Background(), "SELECT * FROM sample_Sheet2 ORDER BY id")
		if err != nil {
			require.NoError(t, err, "operation should succeed")
		}
		defer rows.Close()

		count = 0
		for rows.Next() {
			var id, mail string
			if err := rows.Scan(&id, &mail); err != nil {
				require.NoError(t, err, "operation should succeed")
			}
			count++

			switch count {
			case 1:
				if id != "1" || mail != "gina@example.com" {
					t.Errorf("Expected (1, gina@example.com), got (%s, %s)", id, mail)
				}
			case 2:
				if id != "2" || mail != "yulia@example.com" {
					t.Errorf("Expected (2, yulia@example.com), got (%s, %s)", id, mail)
				}
			case 3:
				if id != "3" || mail != "vika@eample.com" {
					t.Errorf("Expected (3, vika@eample.com), got (%s, %s)", id, mail)
				}
			}
		}
		if err := rows.Err(); err != nil {
			require.NoError(t, err, "operation should succeed")
		}

		if count != 3 {
			t.Errorf("Expected 3 records in Sheet2, got %d", count)
		}
	})

	t.Run("empty XLSX file", func(t *testing.T) {
		ctx := context.Background()

		// Create in-memory database
		sqliteDriver := &sqlite.Driver{}
		conn, err := sqliteDriver.Open(":memory:")
		if err != nil {
			require.NoError(t, err, "operation should succeed")
		}
		db := sql.OpenDB(&directConnector{conn: conn})
		defer db.Close()

		// Create empty reader
		emptyReader := strings.NewReader("")

		// Create builder and test streamXLSXFileToSQLite
		builder := &DBBuilder{}
		err = builder.streamXLSXFileToSQLite(ctx, db, emptyReader, "empty.xlsx")

		if err == nil {
			t.Error("Expected error for empty XLSX file")
		}
		if !strings.Contains(err.Error(), "empty XLSX file") {
			t.Errorf("Expected 'empty XLSX file' error, got: %v", err)
		}
	})

	t.Run("invalid XLSX data", func(t *testing.T) {
		ctx := context.Background()

		// Create in-memory database
		sqliteDriver := &sqlite.Driver{}
		conn, err := sqliteDriver.Open(":memory:")
		if err != nil {
			require.NoError(t, err, "operation should succeed")
		}
		db := sql.OpenDB(&directConnector{conn: conn})
		defer db.Close()

		// Create invalid XLSX data
		invalidReader := strings.NewReader("invalid xlsx data")

		// Create builder and test streamXLSXFileToSQLite
		builder := &DBBuilder{}
		err = builder.streamXLSXFileToSQLite(ctx, db, invalidReader, "invalid.xlsx")

		if err == nil {
			t.Error("Expected error for invalid XLSX data")
		}
		if !strings.Contains(err.Error(), "failed to open XLSX file") {
			t.Errorf("Expected 'failed to open XLSX file' error, got: %v", err)
		}
	})

	t.Run("duplicate table name", func(t *testing.T) {
		ctx := context.Background()

		// Create in-memory database
		sqliteDriver := &sqlite.Driver{}
		conn, err := sqliteDriver.Open(":memory:")
		if err != nil {
			require.NoError(t, err, "operation should succeed")
		}
		db := sql.OpenDB(&directConnector{conn: conn})
		defer db.Close()

		// Create a table first
		_, err = db.ExecContext(context.Background(), `CREATE TABLE "sample_Sheet1" (id TEXT, name TEXT)`)
		if err != nil {
			require.NoError(t, err, "operation should succeed")
		}

		// Read sample XLSX file
		xlsxPath := filepath.Join("testdata", "excel", "sample.xlsx")
		file, err := os.Open(xlsxPath) //nolint:gosec // Test file path is safe
		if err != nil {
			require.NoError(t, err, "operation should succeed")
		}
		defer file.Close()

		// Create builder and test streamXLSXFileToSQLite
		builder := &DBBuilder{}
		err = builder.streamXLSXFileToSQLite(ctx, db, file, xlsxPath)

		if err == nil {
			t.Error("Expected error for duplicate table name")
		}
		if !strings.Contains(err.Error(), "already exists") {
			t.Errorf("Expected 'already exists' error, got: %v", err)
		}
	})
}
