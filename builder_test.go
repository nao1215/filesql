package filesql

import (
	"context"
	"embed"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"testing/fstest"
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
		if validatedBuilder != nil && len(validatedBuilder.collectedPaths) != 3 {
			t.Errorf("Build() collected %d paths, want 3", len(validatedBuilder.collectedPaths))
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
			if err := validatedBuilder.Cleanup(); err != nil {
				t.Errorf("Cleanup() error = %v", err)
			}
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
			if err := validatedBuilder.Cleanup(); err != nil {
				t.Errorf("Cleanup() error = %v", err)
			}
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

		paths, err := builder.processFSInput(ctx, mockFS)
		if err != nil {
			t.Errorf("processFSInput() error = %v", err)
		}
		if len(paths) != 3 {
			t.Errorf("processFSInput() returned %d paths, want 3", len(paths))
		}

		// Clean up temp files
		if err := builder.Cleanup(); err != nil {
			t.Errorf("Cleanup() error = %v", err)
		}
	})

	t.Run("compressed files", func(t *testing.T) {
		t.Parallel()
		mockFS := fstest.MapFS{
			"data.csv.gz":   &fstest.MapFile{Data: []byte("compressed csv data")},
			"logs.ltsv.bz2": &fstest.MapFile{Data: []byte("compressed ltsv data")},
		}

		builder := NewBuilder()

		paths, err := builder.processFSInput(ctx, mockFS)
		if err != nil {
			t.Errorf("processFSInput() error = %v", err)
		}
		if len(paths) != 2 {
			t.Errorf("processFSInput() returned %d paths, want 2", len(paths))
		}

		// Clean up temp files
		if err := builder.Cleanup(); err != nil {
			t.Errorf("Cleanup() error = %v", err)
		}
	})
}

func TestDBBuilder_Cleanup(t *testing.T) {
	t.Parallel()

	t.Run("successful cleanup", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		mockFS := fstest.MapFS{
			"data.csv": &fstest.MapFile{Data: []byte("col1,col2\nval1,val2\n")},
		}

		builder := NewBuilder()

		paths, err := builder.processFSInput(ctx, mockFS)
		if err != nil {
			t.Fatal(err)
		}

		// Verify temp file exists
		for _, path := range paths {
			if _, err := os.Stat(path); os.IsNotExist(err) {
				t.Errorf("Temp file %s should exist before cleanup", path)
			}
		}

		// Clean up
		if err := builder.Cleanup(); err != nil {
			t.Errorf("Cleanup() error = %v", err)
		}

		// Verify temp file is removed
		for _, path := range paths {
			if _, err := os.Stat(path); !os.IsNotExist(err) {
				t.Errorf("Temp file %s should be removed after cleanup", path)
			}
		}
	})

	t.Run("cleanup with no temp files", func(t *testing.T) {
		t.Parallel()
		builder := NewBuilder()

		// Clean up empty builder should not error
		if err := builder.Cleanup(); err != nil {
			t.Errorf("Cleanup() with no temp files should not error, got: %v", err)
		}
	})

	t.Run("cleanup with multiple failed removals", func(t *testing.T) {
		t.Parallel()
		builder := NewBuilder()

		// Add some non-existent temp files to simulate removal failures
		builder.tempFiles = []string{
			"/non/existent/path1.csv",
			"/non/existent/path2.csv",
			"/non/existent/path3.csv",
		}

		err := builder.Cleanup()
		if err == nil {
			t.Error("Cleanup() should return error for failed removals")
		}

		// Check that errors.Join was used - should contain multiple errors
		errStr := err.Error()
		if !strings.Contains(errStr, "path1.csv") || !strings.Contains(errStr, "path2.csv") {
			t.Errorf("Error should contain multiple file paths, got: %s", errStr)
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
		if err := validatedBuilder.Cleanup(); err != nil {
			t.Errorf("Cleanup() error = %v", err)
		}
	}
}
