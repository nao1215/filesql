package filesql

import (
	"bytes"
	"context"
	"embed"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"testing/fstest"
	"time"

	"github.com/nao1215/filesql/domain/model"
)

//go:embed testdata/embed_test/*.csv testdata/embed_test/*.tsv
var testFS embed.FS

func TestNewBuilder(t *testing.T) {
	t.Parallel()

	builder := NewBuilder()
	if builder == nil {
		t.Fatal("NewBuilder() returned nil")
	}
	if len(builder.paths) != 0 {
		t.Errorf("NewBuilder() paths = %d, want 0", len(builder.paths))
	}
	if len(builder.filesystems) != 0 {
		t.Errorf("NewBuilder() filesystems = %d, want 0", len(builder.filesystems))
	}
}

func TestDBBuilder_AddPath(t *testing.T) {
	t.Parallel()

	t.Run("single path", func(t *testing.T) {
		t.Parallel()
		builder := NewBuilder().AddPath("test.csv")
		if len(builder.paths) != 1 {
			t.Errorf("paths = %d, want 1", len(builder.paths))
		}
		if builder.paths[0] != "test.csv" {
			t.Errorf("paths[0] = %s, want test.csv", builder.paths[0])
		}
	})

	t.Run("chain multiple paths", func(t *testing.T) {
		t.Parallel()
		builder := NewBuilder().
			AddPath("test1.csv").
			AddPath("test2.tsv")
		if len(builder.paths) != 2 {
			t.Errorf("paths = %d, want 2", len(builder.paths))
		}
	})
}

func TestDBBuilder_AddPaths(t *testing.T) {
	t.Parallel()

	builder := NewBuilder().AddPaths("test1.csv", "test2.tsv", "test3.ltsv")
	if len(builder.paths) != 3 {
		t.Errorf("paths = %d, want 3", len(builder.paths))
	}
}

func TestDBBuilder_AddFS(t *testing.T) {
	t.Parallel()

	t.Run("add filesystem", func(t *testing.T) {
		t.Parallel()
		mockFS := fstest.MapFS{
			"data.csv": &fstest.MapFile{Data: []byte("col1,col2\nval1,val2\n")},
		}

		builder := NewBuilder().AddFS(mockFS)
		if len(builder.filesystems) != 1 {
			t.Errorf("filesystems = %d, want 1", len(builder.filesystems))
		}
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
		if len(builder.filesystems) != 2 {
			t.Errorf("filesystems = %d, want 2", len(builder.filesystems))
		}
	})
}

func TestDBBuilder_AddReader(t *testing.T) {
	t.Parallel()

	t.Run("add CSV reader", func(t *testing.T) {
		t.Parallel()
		data := "name,age\nAlice,30\nBob,25\n"
		reader := bytes.NewReader([]byte(data))

		builder := NewBuilder().AddReader(reader, "users", model.FileTypeCSV)
		if len(builder.readers) != 1 {
			t.Errorf("readers = %d, want 1", len(builder.readers))
		}
		if builder.readers[0].TableName != "users" {
			t.Errorf("TableName = %s, want users", builder.readers[0].TableName)
		}
		if builder.readers[0].FileType != model.FileTypeCSV {
			t.Errorf("FileType = %v, want FileTypeCSV", builder.readers[0].FileType)
		}
		// No compression fields to check since FileTypeCSV is uncompressed
	})

	t.Run("add TSV reader", func(t *testing.T) {
		t.Parallel()
		data := "col1\tcol2\nval1\tval2\n"
		reader := bytes.NewReader([]byte(data))

		builder := NewBuilder().AddReader(reader, "data", model.FileTypeTSV)
		if len(builder.readers) != 1 {
			t.Errorf("readers = %d, want 1", len(builder.readers))
		}
		if builder.readers[0].FileType != model.FileTypeTSV {
			t.Errorf("FileType = %v, want FileTypeTSV", builder.readers[0].FileType)
		}
	})

	t.Run("add compressed CSV reader", func(t *testing.T) {
		t.Parallel()
		data := []byte{} // Empty data for test
		reader := bytes.NewReader(data)

		builder := NewBuilder().AddReader(reader, "logs", model.FileTypeCSVGZ)
		if len(builder.readers) != 1 {
			t.Errorf("readers = %d, want 1", len(builder.readers))
		}
		if builder.readers[0].FileType != model.FileTypeCSVGZ {
			t.Errorf("FileType = %v, want FileTypeCSVGZ", builder.readers[0].FileType)
		}
		// Regular CSV type for testing
	})

	t.Run("add multiple readers", func(t *testing.T) {
		t.Parallel()
		reader1 := bytes.NewReader([]byte("col1,col2\nval1,val2\n"))
		reader2 := bytes.NewReader([]byte("col3\tcol4\nval3\tval4\n"))

		builder := NewBuilder().
			AddReader(reader1, "table1", model.FileTypeCSV).
			AddReader(reader2, "table2", model.FileTypeTSV)

		if len(builder.readers) != 2 {
			t.Errorf("readers = %d, want 2", len(builder.readers))
		}
	})
}

func TestDBBuilder_SetDefaultChunkSize(t *testing.T) {
	t.Parallel()

	t.Run("set custom chunk size", func(t *testing.T) {
		t.Parallel()
		customSize := 20 * 1024 * 1024 // 20MB
		builder := NewBuilder().SetDefaultChunkSize(customSize)

		if builder.defaultChunkSize != customSize {
			t.Errorf("defaultChunkSize = %d, want %d", builder.defaultChunkSize, customSize)
		}
	})

	t.Run("zero or negative size ignored", func(t *testing.T) {
		t.Parallel()
		defaultSize := 10 * 1024 * 1024
		builder := NewBuilder()

		// Zero should be ignored
		builder.SetDefaultChunkSize(0)
		if builder.defaultChunkSize != defaultSize {
			t.Errorf("defaultChunkSize = %d, want %d (should not change)", builder.defaultChunkSize, defaultSize)
		}

		// Negative should be ignored
		builder.SetDefaultChunkSize(-1)
		if builder.defaultChunkSize != defaultSize {
			t.Errorf("defaultChunkSize = %d, want %d (should not change)", builder.defaultChunkSize, defaultSize)
		}
	})
}

func TestDBBuilder_Build(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	t.Run("no inputs error", func(t *testing.T) {
		t.Parallel()
		builder := NewBuilder()
		_, err := builder.Build(ctx)
		if err == nil {
			t.Error("Build() should return error for no inputs")
		}
	})

	t.Run("reader with nil reader error", func(t *testing.T) {
		t.Parallel()
		builder := NewBuilder()
		builder.readers = append(builder.readers, ReaderInput{
			Reader:    nil,
			TableName: "test",
			FileType:  model.FileTypeCSV,
		})

		_, err := builder.Build(ctx)
		if err == nil {
			t.Error("Build() should return error for nil reader")
		}
		if !strings.Contains(err.Error(), "reader cannot be nil") {
			t.Errorf("Expected 'reader cannot be nil' error, got: %v", err)
		}
	})

	t.Run("reader with empty table name error", func(t *testing.T) {
		t.Parallel()
		reader := bytes.NewReader([]byte("test"))
		builder := NewBuilder()
		builder.readers = append(builder.readers, ReaderInput{
			Reader:    reader,
			TableName: "",
			FileType:  model.FileTypeCSV,
		})

		_, err := builder.Build(ctx)
		if err == nil {
			t.Error("Build() should return error for empty table name")
		}
		if !strings.Contains(err.Error(), "table name must be specified") {
			t.Errorf("Expected 'table name must be specified' error, got: %v", err)
		}
	})

	t.Run("reader with unsupported file type error", func(t *testing.T) {
		t.Parallel()
		reader := bytes.NewReader([]byte("test"))
		builder := NewBuilder()
		builder.readers = append(builder.readers, ReaderInput{
			Reader:    reader,
			TableName: "test",
			FileType:  model.FileTypeUnsupported,
		})

		_, err := builder.Build(ctx)
		if err == nil {
			t.Error("Build() should return error for unsupported file type")
		}
		if !strings.Contains(err.Error(), "file type must be specified") {
			t.Errorf("Expected 'file type must be specified' error, got: %v", err)
		}
	})

	t.Run("reader with valid CSV data", func(t *testing.T) {
		t.Parallel()
		data := "name,age\nAlice,30\nBob,25\n"
		reader := bytes.NewReader([]byte(data))
		builder := NewBuilder().AddReader(reader, "users", model.FileTypeCSV)

		validatedBuilder, err := builder.Build(ctx)
		if err != nil {
			t.Errorf("Build() error = %v", err)
		}
		if validatedBuilder == nil {
			t.Error("Build() returned nil builder")
			return
		}
		// Readers don't create temp files anymore - they use direct streaming
		if len(validatedBuilder.readers) != 1 {
			t.Errorf("Build() should have 1 reader input, got %d", len(validatedBuilder.readers))
		}

		// Clean up temp files
	})

	t.Run("reader with compressed type specification", func(t *testing.T) {
		t.Parallel()
		// Note: Use regular CSV data since we're testing the type system, not actual compression
		data := []byte("col1,col2\nval1,val2\n")
		reader := bytes.NewReader(data)
		builder := NewBuilder().AddReader(reader, "logs", model.FileTypeCSV)

		validatedBuilder, err := builder.Build(ctx)
		if err != nil {
			t.Errorf("Build() error = %v", err)
		}
		if validatedBuilder == nil {
			t.Error("Build() returned nil builder")
		}

		// Clean up temp files
	})

	t.Run("multiple readers", func(t *testing.T) {
		t.Parallel()
		reader1 := bytes.NewReader([]byte("col1,col2\nval1,val2\n"))
		reader2 := bytes.NewReader([]byte("col3\tcol4\nval3\tval4\n"))

		builder := NewBuilder().
			AddReader(reader1, "table1", model.FileTypeCSV).
			AddReader(reader2, "table2", model.FileTypeTSV)

		validatedBuilder, err := builder.Build(ctx)
		if err != nil {
			t.Errorf("Build() error = %v", err)
		}
		if validatedBuilder == nil {
			t.Error("Build() returned nil builder")
			return
		}
		// Readers don't create temp files anymore - they use direct streaming
		if len(validatedBuilder.readers) != 2 {
			t.Errorf("Build() should have 2 reader inputs, got %d", len(validatedBuilder.readers))
		}

		// Clean up temp files
	})

	t.Run("invalid path error", func(t *testing.T) {
		t.Parallel()
		builder := NewBuilder().AddPath("/nonexistent/file.csv")
		_, err := builder.Build(ctx)
		if err == nil {
			t.Error("Build() should return error for nonexistent path")
		}
	})

	t.Run("unsupported file type error", func(t *testing.T) {
		t.Parallel()
		// Create a temporary unsupported file
		tempDir := t.TempDir()
		unsupportedFile := filepath.Join(tempDir, "test.txt")
		if err := os.WriteFile(unsupportedFile, []byte("test"), 0600); err != nil {
			t.Fatal(err)
		}

		builder := NewBuilder().AddPath(unsupportedFile)
		_, err := builder.Build(ctx)
		if err == nil {
			t.Error("Build() should return error for unsupported file type")
		}
	})

	t.Run("valid CSV file", func(t *testing.T) {
		t.Parallel()
		// Create a temporary CSV file
		tempDir := t.TempDir()
		csvFile := filepath.Join(tempDir, "test.csv")
		content := "col1,col2\nval1,val2\n"
		if err := os.WriteFile(csvFile, []byte(content), 0600); err != nil {
			t.Fatal(err)
		}

		builder := NewBuilder().AddPath(csvFile)
		validatedBuilder, err := builder.Build(ctx)
		if err != nil {
			t.Errorf("Build() error = %v", err)
		}
		if validatedBuilder == nil {
			t.Error("Build() returned nil builder")
		}
	})

	t.Run("valid directory", func(t *testing.T) {
		t.Parallel()
		tempDir := t.TempDir()

		builder := NewBuilder().AddPath(tempDir)
		validatedBuilder, err := builder.Build(ctx)
		if err != nil {
			t.Errorf("Build() error = %v", err)
		}
		if validatedBuilder == nil {
			t.Error("Build() returned nil builder")
		}
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
		if err != nil {
			t.Errorf("Build() error = %v", err)
		}
		if validatedBuilder == nil {
			t.Error("Build() returned nil builder")
		}
		// Should have found 3 files (csv, tsv, ltsv) and ignored txt
		// fs.FS files are now stored as readers instead of collectedPaths
		if validatedBuilder != nil && len(validatedBuilder.readers) != 3 {
			t.Errorf("Build() should have 3 readers from fs.FS, got %d", len(validatedBuilder.readers))
		}
	})

	t.Run("FS with nil filesystem error", func(t *testing.T) {
		t.Parallel()
		builder := NewBuilder()
		builder.filesystems = append(builder.filesystems, nil)

		_, err := builder.Build(ctx)
		if err == nil {
			t.Error("Build() should return error for nil FS")
		}
	})

	t.Run("FS with no supported files error", func(t *testing.T) {
		t.Parallel()
		mockFS := fstest.MapFS{
			"readme.txt": &fstest.MapFile{Data: []byte("Not supported\n")},
			"data.json":  &fstest.MapFile{Data: []byte("{}\n")},
		}

		builder := NewBuilder().AddFS(mockFS)
		_, err := builder.Build(ctx)
		if err == nil {
			t.Error("Build() should return error for FS with no supported files")
		}
	})
}

func TestDBBuilder_ChunkedReading(t *testing.T) {
	t.Parallel()

	t.Run("large data chunked reading", func(t *testing.T) {
		t.Parallel()
		// Create a large dataset that would benefit from chunked reading
		var data bytes.Buffer
		data.WriteString("id,name,value\n")
		for i := range 10000 {
			fmt.Fprintf(&data, "%d,name_%d,%d\n", i, i, i*10)
		}

		reader := bytes.NewReader(data.Bytes())
		chunkSize := 1024 // Small chunk for testing
		builder := NewBuilder().
			SetDefaultChunkSize(chunkSize).
			AddReader(reader, "large_table", model.FileTypeCSV)

		ctx := context.Background()
		validatedBuilder, err := builder.Build(ctx)
		if err != nil {
			t.Fatal(err)
		}

		db, err := validatedBuilder.Open(ctx)
		if err != nil {
			t.Errorf("Open() error = %v", err)
		}
		if db == nil {
			t.Error("Open() returned nil database")
		} else {
			// Verify the data was loaded correctly
			var count int
			err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM large_table").Scan(&count)
			if err != nil {
				t.Errorf("Count query failed: %v", err)
			}
			if count != 10000 {
				t.Errorf("Expected 10000 rows, got %d", count)
			}
			_ = db.Close()
		}

		// Clean up temp files
	})
}

func TestDBBuilder_Open_WithReader(t *testing.T) {
	ctx := context.Background()

	t.Run("successful open with reader", func(t *testing.T) {
		data := "name,age\nAlice,30\nBob,25\n"
		reader := bytes.NewReader([]byte(data))
		builder := NewBuilder().AddReader(reader, "users", model.FileTypeCSV)

		validatedBuilder, err := builder.Build(ctx)
		if err != nil {
			t.Fatal(err)
		}

		db, err := validatedBuilder.Open(ctx)
		if err != nil {
			t.Errorf("Open() error = %v", err)
		}
		if db == nil {
			t.Error("Open() returned nil database")
		} else {
			// Verify we can query the data
			rows, err := db.QueryContext(ctx, "SELECT * FROM users")
			if err != nil {
				t.Errorf("Query failed: %v", err)
			} else {
				defer rows.Close()
				if err := rows.Err(); err != nil {
					t.Errorf("Rows error: %v", err)
				}
			}
			_ = db.Close()
		}

		// Clean up temp files
	})

	t.Run("mixed inputs - reader and file", func(t *testing.T) {
		// Create a temporary CSV file
		tempDir := t.TempDir()
		csvFile := filepath.Join(tempDir, "orders.csv")
		fileContent := "order_id,amount\n1,100\n2,200\n"
		if err := os.WriteFile(csvFile, []byte(fileContent), 0600); err != nil {
			t.Fatal(err)
		}

		// Create a reader with different data
		readerData := "product_id,name\n1,Laptop\n2,Mouse\n"
		reader := bytes.NewReader([]byte(readerData))

		builder := NewBuilder().
			AddPath(csvFile).
			AddReader(reader, "products", model.FileTypeCSV)

		validatedBuilder, err := builder.Build(ctx)
		if err != nil {
			t.Fatal(err)
		}

		db, err := validatedBuilder.Open(ctx)
		if err != nil {
			t.Errorf("Open() error = %v", err)
		}
		if db == nil {
			t.Error("Open() returned nil database")
		} else {
			// Verify both tables exist
			for _, table := range []string{"orders", "products"} {
				rows, err := db.QueryContext(ctx, "SELECT * FROM "+table) // #nosec G202 -- table name is safe
				if err != nil {
					t.Errorf("Query %s failed: %v", table, err)
				} else {
					if err := rows.Err(); err != nil {
						t.Errorf("Rows error for %s: %v", table, err)
					}
					_ = rows.Close() // Close immediately in the loop
				}
			}
			_ = db.Close()
		}

		// Clean up temp files
	})
}

func TestDBBuilder_Open(t *testing.T) {
	ctx := context.Background()

	t.Run("open without build should fail", func(t *testing.T) {
		builder := NewBuilder().AddPath("test.csv")
		// Call Open without calling Build first
		db, err := builder.Open(ctx)
		if err == nil {
			if db != nil {
				_ = db.Close()
			}
			t.Error("Open() without Build() should return error")
		}
		expectedErrMsg := "no valid input files found, did you call Build()?"
		if !strings.Contains(err.Error(), expectedErrMsg) {
			t.Errorf("Expected error message containing '%s', got: %s", expectedErrMsg, err.Error())
		}
	})

	t.Run("successful open with CSV file", func(t *testing.T) {
		// Create a temporary CSV file
		tempDir := t.TempDir()
		csvFile := filepath.Join(tempDir, "test.csv")
		content := "col1,col2\nval1,val2\n"
		if err := os.WriteFile(csvFile, []byte(content), 0600); err != nil {
			t.Fatal(err)
		}

		builder := NewBuilder().AddPath(csvFile)
		validatedBuilder, err := builder.Build(ctx)
		if err != nil {
			t.Fatal(err)
		}

		db, err := validatedBuilder.Open(ctx)
		if err != nil {
			t.Errorf("Open() error = %v", err)
		}
		if db == nil {
			t.Error("Open() returned nil database")
		} else {
			_ = db.Close()
		}
	})

	t.Run("successful open with FS", func(t *testing.T) {
		mockFS := fstest.MapFS{
			"data.csv": &fstest.MapFile{Data: []byte("col1,col2\nval1,val2\n")},
		}

		builder := NewBuilder().AddFS(mockFS)
		validatedBuilder, err := builder.Build(ctx)
		if err != nil {
			t.Fatal(err)
		}

		db, err := validatedBuilder.Open(ctx)
		if err != nil {
			t.Errorf("Open() error = %v", err)
		}
		if db == nil {
			t.Error("Open() returned nil database")
		} else {
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
		if err != nil {
			t.Fatal(err)
		}

		db, err := validatedBuilder.Open(ctx)
		if err != nil {
			t.Errorf("Open() error = %v", err)
		}
		if db == nil {
			t.Error("Open() returned nil database")
		} else {
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
		if err != nil {
			t.Errorf("processFSToReaders() error = %v", err)
		}
		if len(readers) != 3 {
			t.Errorf("processFSToReaders() returned %d readers, want 3", len(readers))
		}

		// Close all readers
		for _, reader := range readers {
			if closer, ok := reader.Reader.(io.Closer); ok {
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
		if err != nil {
			t.Errorf("processFSToReaders() error = %v", err)
		}
		if len(readers) != 2 {
			t.Errorf("processFSToReaders() returned %d readers, want 2", len(readers))
		}

		// Close all readers
		for _, reader := range readers {
			if closer, ok := reader.Reader.(io.Closer); ok {
				_ = closer.Close()
			}
		}
	})
}

func TestIntegrationWithEmbedFS(t *testing.T) {
	ctx := context.Background()

	// Use embedded test data from embed_test subdirectory
	subFS, err := fs.Sub(testFS, "testdata/embed_test")
	if err != nil {
		t.Fatal(err)
	}

	// Test loading all supported files from embedded FS
	builder := NewBuilder().AddFS(subFS)

	validatedBuilder, err := builder.Build(ctx)
	if err != nil {
		t.Fatal(err)
	}

	db, err := validatedBuilder.Open(ctx)
	if err != nil {
		t.Errorf("Open() with embed.FS error = %v", err)
	}
	if db == nil {
		t.Error("Open() with embed.FS returned nil database")
	} else {
		// Verify we can query the database
		rows, err := db.QueryContext(ctx, "SELECT name FROM sqlite_master WHERE type='table'")
		if err != nil {
			t.Errorf("Failed to query database: %v", err)
		} else {
			defer rows.Close()
			if err := rows.Err(); err != nil {
				t.Errorf("Rows error: %v", err)
			}
		}

		_ = db.Close()
		// Clean up temp files
	}
}

func TestAutoSave_OnClose(t *testing.T) {
	// Create temporary directory
	tmpDir := t.TempDir()

	// Create test CSV file
	csvPath := filepath.Join(tmpDir, "test.csv")
	csvContent := "name,age\nAlice,25\nBob,30\n"
	if err := os.WriteFile(csvPath, []byte(csvContent), 0600); err != nil {
		t.Fatalf("Failed to write test CSV: %v", err)
	}

	// Create output directory
	outputDir := filepath.Join(tmpDir, "output")
	if err := os.MkdirAll(outputDir, 0750); err != nil {
		t.Fatalf("Failed to create output dir: %v", err)
	}

	// Build database with auto-save on close
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	builder := NewBuilder().
		AddPath(csvPath).
		EnableAutoSave(outputDir)

	validatedBuilder, err := builder.Build(ctx)
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	db, err := validatedBuilder.Open(ctx)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	// Modify data
	_, err = db.ExecContext(ctx, "INSERT INTO test (name, age) VALUES ('Charlie', 35)")
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}

	// Close database (should trigger auto-save)
	if err := db.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Check if file was saved
	outputFile := filepath.Join(outputDir, "test.csv")
	if _, err := os.Stat(outputFile); os.IsNotExist(err) {
		t.Fatalf("Auto-save file not created: %s", outputFile)
	}

	// Verify content includes the new record
	content, err := os.ReadFile(outputFile) //nolint:gosec // Test file path is safe
	if err != nil {
		t.Fatalf("Failed to read output file: %v", err)
	}

	if !strings.Contains(string(content), "Charlie") {
		t.Errorf("Auto-saved file should contain inserted data. Got: %s", string(content))
	}
}

func TestAutoSave_OnCommit(t *testing.T) {
	// Create temporary directory
	tmpDir := t.TempDir()

	// Create test CSV file
	csvPath := filepath.Join(tmpDir, "test.csv")
	csvContent := "name,age\nAlice,25\n"
	if err := os.WriteFile(csvPath, []byte(csvContent), 0600); err != nil {
		t.Fatalf("Failed to write test CSV: %v", err)
	}

	// Create output directory
	outputDir := filepath.Join(tmpDir, "output")
	if err := os.MkdirAll(outputDir, 0750); err != nil {
		t.Fatalf("Failed to create output dir: %v", err)
	}

	// Build database with auto-save on commit
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	builder := NewBuilder().
		AddPath(csvPath).
		EnableAutoSaveOnCommit(outputDir)

	validatedBuilder, err := builder.Build(ctx)
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	db, err := validatedBuilder.Open(ctx)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer db.Close()

	// Start transaction
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("Begin transaction failed: %v", err)
	}

	// Modify data within transaction
	_, err = tx.ExecContext(ctx, "INSERT INTO test (name, age) VALUES ('David', 40)")
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}

	// Commit transaction (should trigger auto-save)
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// Check if file was saved
	outputFile := filepath.Join(outputDir, "test.csv")
	if _, err := os.Stat(outputFile); os.IsNotExist(err) {
		t.Fatalf("Auto-save file not created: %s", outputFile)
	}

	// Verify content includes the new record
	content, err := os.ReadFile(outputFile) //nolint:gosec // Test file path is safe
	if err != nil {
		t.Fatalf("Failed to read output file: %v", err)
	}

	if !strings.Contains(string(content), "David") {
		t.Errorf("Auto-saved file should contain committed data. Got: %s", string(content))
	}
}

func TestAutoSave_DisableAutoSave(t *testing.T) {
	// Create temporary directory
	tmpDir := t.TempDir()

	// Create test CSV file
	csvPath := filepath.Join(tmpDir, "test.csv")
	csvContent := "name,age\nAlice,25\n"
	if err := os.WriteFile(csvPath, []byte(csvContent), 0600); err != nil {
		t.Fatalf("Failed to write test CSV: %v", err)
	}

	// Create output directory
	outputDir := filepath.Join(tmpDir, "output")
	if err := os.MkdirAll(outputDir, 0750); err != nil {
		t.Fatalf("Failed to create output dir: %v", err)
	}

	// Build database without auto-save (default behavior)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	builder := NewBuilder().
		AddPath(csvPath)
	// Note: No EnableAutoSave() call

	validatedBuilder, err := builder.Build(ctx)
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	db, err := validatedBuilder.Open(ctx)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	// Modify data
	_, err = db.ExecContext(ctx, "INSERT INTO test (name, age) VALUES ('Echo', 45)")
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}

	// Close database (should NOT trigger auto-save)
	if err := db.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Check that no output file was created
	outputFile := filepath.Join(outputDir, "test.csv")
	if _, err := os.Stat(outputFile); !os.IsNotExist(err) {
		t.Errorf("Auto-save file should not have been created when auto-save is disabled")
	}
}

func TestAutoSave_MultipleCommitsOverwrite(t *testing.T) {
	// This test verifies that multiple commits properly overwrite the same file
	t.Parallel()

	tmpDir := t.TempDir()

	// Create test CSV file
	csvPath := filepath.Join(tmpDir, "test.csv")
	csvContent := "name,count\nInitial,1\n"
	if err := os.WriteFile(csvPath, []byte(csvContent), 0600); err != nil {
		t.Fatalf("Failed to write test CSV: %v", err)
	}

	// Create output directory
	outputDir := filepath.Join(tmpDir, "output")
	if err := os.MkdirAll(outputDir, 0750); err != nil {
		t.Fatalf("Failed to create output dir: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Build database with auto-save on commit
	builder := NewBuilder().
		AddPath(csvPath).
		EnableAutoSaveOnCommit(outputDir)

	validatedBuilder, err := builder.Build(ctx)
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	db, err := validatedBuilder.Open(ctx)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer db.Close()

	outputFile := filepath.Join(outputDir, "test.csv")

	// First commit: Add first record
	tx1, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("Begin first transaction failed: %v", err)
	}

	_, err = tx1.ExecContext(ctx, "INSERT INTO test (name, count) VALUES ('First', 100)")
	if err != nil {
		t.Fatalf("First insert failed: %v", err)
	}

	if err := tx1.Commit(); err != nil {
		t.Fatalf("First commit failed: %v", err)
	}

	// Check first commit saved the file
	if _, err := os.Stat(outputFile); os.IsNotExist(err) {
		t.Fatalf("Auto-save file not created after first commit: %s", outputFile)
	}

	// Read content after first commit
	content1, err := os.ReadFile(outputFile) //nolint:gosec // Test file path is safe
	if err != nil {
		t.Fatalf("Failed to read output file after first commit: %v", err)
	}

	if !strings.Contains(string(content1), "First") {
		t.Errorf("File should contain first commit data. Got: %s", string(content1))
	}

	// Second commit: Add second record (should overwrite)
	tx2, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("Begin second transaction failed: %v", err)
	}

	_, err = tx2.ExecContext(ctx, "INSERT INTO test (name, count) VALUES ('Second', 200)")
	if err != nil {
		t.Fatalf("Second insert failed: %v", err)
	}

	if err := tx2.Commit(); err != nil {
		t.Fatalf("Second commit failed: %v", err)
	}

	// Read content after second commit
	content2, err := os.ReadFile(outputFile) //nolint:gosec // Test file path is safe
	if err != nil {
		t.Fatalf("Failed to read output file after second commit: %v", err)
	}

	// Verify the file was overwritten and contains both records
	if !strings.Contains(string(content2), "First") {
		t.Errorf("File should still contain first commit data after second commit. Got: %s", string(content2))
	}

	if !strings.Contains(string(content2), "Second") {
		t.Errorf("File should contain second commit data. Got: %s", string(content2))
	}

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
	if nonEmptyLines != 4 {
		t.Errorf("Expected 4 lines in overwritten file, got %d. Content: %s", nonEmptyLines, string(content2))
	}

	// Third commit: Update existing record
	tx3, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("Begin third transaction failed: %v", err)
	}

	_, err = tx3.ExecContext(ctx, "UPDATE test SET count = 999 WHERE name = 'Initial'")
	if err != nil {
		t.Fatalf("Update failed: %v", err)
	}

	if err := tx3.Commit(); err != nil {
		t.Fatalf("Third commit failed: %v", err)
	}

	// Read content after third commit
	content3, err := os.ReadFile(outputFile) //nolint:gosec // Test file path is safe
	if err != nil {
		t.Fatalf("Failed to read output file after third commit: %v", err)
	}

	// Verify the update was saved
	if !strings.Contains(string(content3), "999") {
		t.Errorf("File should contain updated count (999). Got: %s", string(content3))
	}

	// Verify original count (1) was overwritten
	if strings.Contains(string(content3), "Initial,1") {
		t.Errorf("File should not contain old count (1) after update. Got: %s", string(content3))
	}
}

func TestAutoSave_ExplicitDisable(t *testing.T) {
	// Test the DisableAutoSave method explicitly
	t.Parallel()

	tmpDir := t.TempDir()

	// Create test CSV file
	csvPath := filepath.Join(tmpDir, "test.csv")
	csvContent := "name,age\nAlice,25\n"
	if err := os.WriteFile(csvPath, []byte(csvContent), 0600); err != nil {
		t.Fatalf("Failed to write test CSV: %v", err)
	}

	// Create output directory
	outputDir := filepath.Join(tmpDir, "output")
	if err := os.MkdirAll(outputDir, 0750); err != nil {
		t.Fatalf("Failed to create output dir: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// First enable auto-save, then explicitly disable it
	builder := NewBuilder().
		AddPath(csvPath).
		EnableAutoSave(outputDir).
		DisableAutoSave() // This should override the previous EnableAutoSave

	validatedBuilder, err := builder.Build(ctx)
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	db, err := validatedBuilder.Open(ctx)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	// Modify data
	_, err = db.ExecContext(ctx, "INSERT INTO test (name, age) VALUES ('Disabled', 99)")
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}

	// Close database (should NOT trigger auto-save due to DisableAutoSave)
	if err := db.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Check that no output file was created
	outputFile := filepath.Join(outputDir, "test.csv")
	if _, err := os.Stat(outputFile); !os.IsNotExist(err) {
		t.Errorf("Auto-save file should not have been created when explicitly disabled")
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
			t.Error("Build() with no inputs should return error")
		}
	})

	t.Run("build with empty path", func(t *testing.T) {
		t.Parallel()
		builder := NewBuilder().AddPath("")
		_, err := builder.Build(ctx)
		if err == nil {
			t.Error("Build() with empty path should return error")
		}
	})

	t.Run("build with non-existent path", func(t *testing.T) {
		t.Parallel()
		builder := NewBuilder().AddPath("/non/existent/file.csv")
		_, err := builder.Build(ctx)
		if err == nil {
			t.Error("Build() with non-existent path should return error")
		}
	})

	t.Run("auto-save with empty output directory", func(t *testing.T) {
		t.Parallel()
		tmpDir := t.TempDir()
		csvPath := filepath.Join(tmpDir, "test.csv")
		if err := os.WriteFile(csvPath, []byte("col1\nval1\n"), 0600); err != nil {
			t.Fatal(err)
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
			t.Fatal(err)
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

		builder := NewBuilder().AddReader(reader, "invalid", model.FileTypeCSV)
		_, err := builder.Build(ctx)

		// Should handle malformed CSV gracefully or return meaningful error
		if err == nil {
			t.Log("Build succeeded with malformed CSV - parser handled it gracefully")
		}
	})

	t.Run("empty reader", func(t *testing.T) {
		t.Parallel()

		reader := strings.NewReader("")
		builder := NewBuilder().AddReader(reader, "empty", model.FileTypeCSV)
		validatedBuilder, err := builder.Build(ctx)

		// Build should succeed (validation happens during Open)
		if err != nil {
			t.Errorf("Build should succeed with empty reader, got error: %v", err)
		}

		if validatedBuilder != nil {
			// Open should fail with empty CSV data
			db, openErr := validatedBuilder.Open(ctx)
			if openErr == nil {
				if db != nil {
					_ = db.Close() // Ignore close error in test cleanup
				}
				t.Error("Open should fail with empty CSV data")
			}
		}
	})

	t.Run("extremely small chunk size", func(t *testing.T) {
		t.Parallel()

		reader := strings.NewReader("name,age\nAlice,30\n")
		// Test with very small chunk size
		builder := NewBuilder().
			AddReader(reader, "test", model.FileTypeCSV).
			SetDefaultChunkSize(1) // Very small chunk size

		_, err := builder.Build(ctx)
		if err != nil {
			t.Errorf("Build should handle small chunk size, got error: %v", err)
		}
	})
}

func TestBuilder_AddPaths_ErrorCases(t *testing.T) {
	t.Parallel()

	t.Run("add multiple paths", func(t *testing.T) {
		t.Parallel()
		builder := NewBuilder().AddPaths("file1.csv", "file2.tsv", "file3.ltsv")
		if len(builder.paths) != 3 {
			t.Errorf("AddPaths should add all paths, got %d", len(builder.paths))
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
			t.Errorf("AddPaths should add all paths including empty ones, got %d", len(builder.paths))
		}
		if builder.paths[1] != "" {
			t.Errorf("AddPaths should preserve empty string, got %s", builder.paths[1])
		}
	})

	t.Run("add no paths", func(t *testing.T) {
		t.Parallel()
		builder := NewBuilder().AddPaths()
		if len(builder.paths) != 0 {
			t.Errorf("AddPaths() with no arguments should not add any paths, got %d", len(builder.paths))
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
		gzipFile := "testdata/sample.csv.gz"

		file, err := os.Open(gzipFile)
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

		bzip2File := "testdata/products.tsv.bz2"

		file, err := os.Open(bzip2File)
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

		xzFile := "testdata/logs.ltsv.xz"

		file, err := os.Open(xzFile)
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

		zstdFile := "testdata/users.csv.zst"

		file, err := os.Open(zstdFile)
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
