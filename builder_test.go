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
	"time"
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
	defer validatedBuilder.Cleanup()

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
	defer validatedBuilder.Cleanup()

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
	defer validatedBuilder.Cleanup()

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
	defer validatedBuilder.Cleanup()

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
	defer validatedBuilder.Cleanup()

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
