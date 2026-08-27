package filesql

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOutputFormat_String(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		format OutputFormat
		want   string
	}{
		{
			name:   "CSV format",
			format: OutputFormatCSV,
			want:   "csv",
		},
		{
			name:   "TSV format",
			format: OutputFormatTSV,
			want:   "tsv",
		},
		{
			name:   "LTSV format",
			format: OutputFormatLTSV,
			want:   "ltsv",
		},
		{
			name:   "Parquet format",
			format: OutputFormatParquet,
			want:   "parquet",
		},
		{
			name:   "XLSX format",
			format: OutputFormatXLSX,
			want:   "xlsx",
		},
		{
			name:   "ACH format",
			format: OutputFormatACH,
			want:   "ach",
		},
		{
			name:   "FedWire format",
			format: OutputFormatFedWire,
			want:   "fed",
		},
		{
			name:   "Unknown format defaults to csv",
			format: OutputFormat(999),
			want:   "csv",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := tt.format.String()
			assert.Equal(t, tt.want, got, "OutputFormat.String() returned unexpected value")
		})
	}
}

func TestOutputFormat_Extension(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		format OutputFormat
		want   string
	}{
		{
			name:   "CSV extension",
			format: OutputFormatCSV,
			want:   ".csv",
		},
		{
			name:   "TSV extension",
			format: OutputFormatTSV,
			want:   ".tsv",
		},
		{
			name:   "LTSV extension",
			format: OutputFormatLTSV,
			want:   ".ltsv",
		},
		{
			name:   "Parquet extension",
			format: OutputFormatParquet,
			want:   ".parquet",
		},
		{
			name:   "XLSX extension",
			format: OutputFormatXLSX,
			want:   ".xlsx",
		},
		{
			name:   "ACH extension",
			format: OutputFormatACH,
			want:   ".ach",
		},
		{
			name:   "FedWire extension",
			format: OutputFormatFedWire,
			want:   ".fed",
		},
		{
			name:   "Unknown format defaults to csv",
			format: OutputFormat(999),
			want:   ".csv",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := tt.format.Extension()
			assert.Equal(t, tt.want, got, "OutputFormat.Extension() returned unexpected value")
		})
	}
}

func TestCompressionType_String(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		compression CompressionType
		want        string
	}{
		{
			name:        "No compression",
			compression: CompressionNone,
			want:        "none",
		},
		{
			name:        "GZ compression",
			compression: CompressionGZ,
			want:        "gz",
		},
		{
			name:        "BZ2 compression",
			compression: CompressionBZ2,
			want:        "bz2",
		},
		{
			name:        "XZ compression",
			compression: CompressionXZ,
			want:        "xz",
		},
		{
			name:        "ZSTD compression",
			compression: CompressionZSTD,
			want:        "zstd",
		},
		{
			name:        "Unknown compression defaults to none",
			compression: CompressionType(999),
			want:        "none",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := tt.compression.String()
			assert.Equal(t, tt.want, got, "CompressionType.String() returned unexpected value")
		})
	}
}

func TestCompressionType_Extension(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		compression CompressionType
		want        string
	}{
		{
			name:        "No compression",
			compression: CompressionNone,
			want:        "",
		},
		{
			name:        "GZ compression",
			compression: CompressionGZ,
			want:        ".gz",
		},
		{
			name:        "BZ2 compression",
			compression: CompressionBZ2,
			want:        ".bz2",
		},
		{
			name:        "XZ compression",
			compression: CompressionXZ,
			want:        ".xz",
		},
		{
			name:        "ZSTD compression",
			compression: CompressionZSTD,
			want:        ".zst",
		},
		{
			name:        "Unknown compression defaults to empty",
			compression: CompressionType(999),
			want:        "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := tt.compression.Extension()
			assert.Equal(t, tt.want, got, "CompressionType.Extension() returned unexpected value")
		})
	}
}

func TestNewDumpOptions(t *testing.T) {
	t.Parallel()

	options := NewDumpOptions()

	assert.Equal(t, OutputFormatCSV, options.Format, "NewDumpOptions().Format should default to CSV")
	assert.Equal(t, CompressionNone, options.Compression, "NewDumpOptions().Compression should default to None")
}

func TestDumpOptions_WithFormat(t *testing.T) {
	t.Parallel()

	options := NewDumpOptions()
	newOptions := options.WithFormat(OutputFormatTSV)

	// Original options should not be modified
	assert.Equal(t, OutputFormatCSV, options.Format, "Original options should not be modified")

	// New options should have the updated format
	assert.Equal(t, OutputFormatTSV, newOptions.Format, "WithFormat() should update format")

	// Other fields should remain unchanged
	assert.Equal(t, CompressionNone, newOptions.Compression, "WithFormat() should not change compression")
}

func TestDumpOptions_WithCompression(t *testing.T) {
	t.Parallel()

	options := NewDumpOptions()
	newOptions := options.WithCompression(CompressionGZ)

	// Original options should not be modified
	assert.Equal(t, CompressionNone, options.Compression, "Original options should not be modified")

	// New options should have the updated compression
	assert.Equal(t, CompressionGZ, newOptions.Compression, "WithCompression() should update compression")

	// Other fields should remain unchanged
	assert.Equal(t, OutputFormatCSV, newOptions.Format, "WithCompression() should not change format")
}

func TestDumpOptions_FileExtension(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		format      OutputFormat
		compression CompressionType
		want        string
	}{
		{
			name:        "CSV with no compression",
			format:      OutputFormatCSV,
			compression: CompressionNone,
			want:        ".csv",
		},
		{
			name:        "CSV with gzip compression",
			format:      OutputFormatCSV,
			compression: CompressionGZ,
			want:        ".csv.gz",
		},
		{
			name:        "TSV with bzip2 compression",
			format:      OutputFormatTSV,
			compression: CompressionBZ2,
			want:        ".tsv.bz2",
		},
		{
			name:        "LTSV with xz compression",
			format:      OutputFormatLTSV,
			compression: CompressionXZ,
			want:        ".ltsv.xz",
		},
		{
			name:        "TSV with zstd compression",
			format:      OutputFormatTSV,
			compression: CompressionZSTD,
			want:        ".tsv.zst",
		},
		{
			name:        "Parquet with no compression",
			format:      OutputFormatParquet,
			compression: CompressionNone,
			want:        ".parquet",
		},
		{
			name:        "XLSX with gzip compression",
			format:      OutputFormatXLSX,
			compression: CompressionGZ,
			want:        ".xlsx.gz",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			options := DumpOptions{
				Format:      tt.format,
				Compression: tt.compression,
			}
			got := options.FileExtension()
			assert.Equal(t, tt.want, got, "DumpOptions.FileExtension() returned unexpected value")
		})
	}
}

// TestOutputFormatStringEdgeCases tests edge cases for OutputFormat.String()
func TestOutputFormatStringEdgeCases(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		format OutputFormat
		want   string
	}{
		{
			name:   "Unknown format should default",
			format: OutputFormat(999), // Invalid format
			want:   "csv",             // Should default to CSV
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := tt.format.String()
			assert.Equal(t, tt.want, got, "OutputFormat.String() returned unexpected value")
		})
	}
}

func TestDumpOptions_ChainedMethods(t *testing.T) {
	t.Parallel()

	options := NewDumpOptions().
		WithFormat(OutputFormatLTSV).
		WithCompression(CompressionZSTD)

	assert.Equal(t, OutputFormatLTSV, options.Format, "Chained WithFormat() should work")
	assert.Equal(t, CompressionZSTD, options.Compression, "Chained WithCompression() should work")

	expectedExt := ".ltsv.zst"
	got := options.FileExtension()
	assert.Equal(t, expectedExt, got, "Chained options FileExtension() should work")
}

func TestAutoSaveConnection_PerformACHAutoSave(t *testing.T) {
	testFile := findTestACHFile(t)
	if testFile == "" {
		t.Skip("No test ACH file found")
	}

	ctx := context.Background()

	// Open ACH file to register table set
	db, err := OpenContext(ctx, testFile)
	require.NoError(t, err)
	defer db.Close()

	// The database records the ACH file it was loaded from.
	baseNames := fileSourceBaseNames(ctx, db, sourceFormatACH)
	require.NotEmpty(t, baseNames, "the ACH source should be recorded")

	// Create temp output directory
	outputDir := t.TempDir()

	// Create the connector with ACH format
	config := &autoSaveConfig{
		enabled:   true,
		timing:    autoSaveOnClose,
		outputDir: outputDir,
		options:   NewDumpOptions().WithFormat(OutputFormatACH),
	}

	conn := &autoSaveConnector{
		autoSaveConfig: config,
		originalPaths:  []string{testFile},
	}

	// Perform ACH auto-save using the existing db connection
	err = conn.performACHAutoSave(db, outputDir)
	require.NoError(t, err)

	// Verify output file was created
	for _, baseName := range baseNames {
		outputPath := filepath.Join(outputDir, baseName+".ach")
		info, err := os.Stat(outputPath)
		require.NoError(t, err, "ACH output file should exist: %s", outputPath)
		assert.Greater(t, info.Size(), int64(0), "ACH output file should not be empty")
	}
}

func TestAutoSaveConnection_PerformACHAutoSave_NoTables(t *testing.T) {
	ctx := context.Background()

	// Open a CSV file (not ACH) to ensure no ACH tables are registered
	db, err := OpenContext(ctx, filepath.Join("testdata", "test.csv"))
	require.NoError(t, err)
	defer db.Close()

	outputDir := t.TempDir()

	conn := &autoSaveConnector{
		autoSaveConfig: &autoSaveConfig{
			enabled:   true,
			outputDir: outputDir,
			options:   NewDumpOptions().WithFormat(OutputFormatACH),
		},
	}

	// Should fail because no ACH tables are registered
	err = conn.performACHAutoSave(db, outputDir)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no ACH tables found")
}

func TestAutoSaveConnection_OverwriteOriginalFiles_ACH(t *testing.T) {
	testFile := findTestACHFile(t)
	if testFile == "" {
		t.Skip("No test ACH file found")
	}

	ctx := context.Background()

	// Copy test file to temp directory
	tmpDir := t.TempDir()
	tmpFile := filepath.Join(tmpDir, "test.ach")

	content, err := os.ReadFile(testFile) //nolint:gosec // Test file path is from test helper
	require.NoError(t, err)
	err = os.WriteFile(tmpFile, content, 0600) //nolint:gosec // Test file path is constructed from t.TempDir()
	require.NoError(t, err)

	// Open the temp ACH file
	db, err := OpenContext(ctx, tmpFile)
	require.NoError(t, err)
	defer db.Close()

	// Create the connector for overwrite mode (no outputDir)
	conn := &autoSaveConnector{
		autoSaveConfig: &autoSaveConfig{
			enabled:   true,
			timing:    autoSaveOnClose,
			outputDir: "", // Empty means overwrite mode
			options:   NewDumpOptions(),
		},
		originalPaths: []string{tmpFile},
	}

	// Perform overwrite
	err = conn.overwriteOriginalFiles(db)
	require.NoError(t, err)

	// Verify file still exists and has content
	info, err := os.Stat(tmpFile)
	require.NoError(t, err)
	assert.Greater(t, info.Size(), int64(0), "Overwritten file should not be empty")
}

func TestAutoSaveConnection_OverwriteOriginalFiles_NoOriginalPaths(t *testing.T) {
	ctx := context.Background()

	db, err := OpenContext(ctx, filepath.Join("testdata", "test.csv"))
	require.NoError(t, err)
	defer db.Close()

	conn := &autoSaveConnector{
		autoSaveConfig: &autoSaveConfig{
			enabled: true,
			options: NewDumpOptions(),
		},
		originalPaths: []string{}, // No original paths
	}

	err = conn.overwriteOriginalFiles(db)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no original paths available")
}

func TestAutoSaveConnection_Begin(t *testing.T) {
	ctx := context.Background()

	// Use Builder to create a database with auto-save config
	builder := NewBuilder().AddPath(filepath.Join("testdata", "test.csv"))
	validatedBuilder, err := buildForTest(ctx, builder)
	require.NoError(t, err)

	db, err := validatedBuilder.Open(ctx)
	require.NoError(t, err)
	defer db.Close()

	// Start a transaction using BeginTx (the recommended method)
	tx, err := db.BeginTx(ctx, nil)
	require.NoError(t, err)

	// Rollback to clean up
	err = tx.Rollback()
	require.NoError(t, err)
}

func TestCompressionType_StringAllTypes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		compression CompressionType
		want        string
	}{
		{"ZLIB compression", CompressionZLIB, "zlib"},
		{"SNAPPY compression", CompressionSNAPPY, "snappy"},
		{"S2 compression", CompressionS2, "s2"},
		{"LZ4 compression", CompressionLZ4, "lz4"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := tt.compression.String()
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestCompressionType_ExtensionAllTypes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		compression CompressionType
		want        string
	}{
		{"ZLIB extension", CompressionZLIB, ".z"},
		{"SNAPPY extension", CompressionSNAPPY, ".snappy"},
		{"S2 extension", CompressionS2, ".s2"},
		{"LZ4 extension", CompressionLZ4, ".lz4"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := tt.compression.Extension()
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestAutoSaveConnection_PerformACHAutoSave_DumpError(t *testing.T) {
	ctx := context.Background()

	// Open a CSV file to get a valid db connection
	db, err := OpenContext(ctx, filepath.Join("testdata", "test.csv"))
	require.NoError(t, err)
	defer db.Close()

	// Record a source pointing at a file that does not exist, so rebuilding the
	// ACH structure fails when the dump runs.
	require.NoError(t, recordFileSource(ctx, db, "nonexistent_ach_table",
		filepath.Join(t.TempDir(), "gone.ach"), sourceFormatACH))

	outputDir := t.TempDir()

	conn := &autoSaveConnector{
		autoSaveConfig: &autoSaveConfig{
			enabled:   true,
			outputDir: outputDir,
			options:   NewDumpOptions().WithFormat(OutputFormatACH),
		},
	}

	// Should fail because the ACH tables don't actually exist
	err = conn.performACHAutoSave(db, outputDir)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to export ACH file")
}

func TestAutoSaveConnection_ExecContext(t *testing.T) {
	t.Parallel()

	// Create a test database connection
	tempDir := t.TempDir()
	csvFile := filepath.Join(tempDir, "test.csv")
	require.NoError(t, os.WriteFile(csvFile, []byte("id,name\n1,Alice\n2,Bob"), 0600))

	db, err := Open(csvFile)
	require.NoError(t, err)
	defer db.Close()

	ctx := context.Background()

	// Test ExecContext
	result, err := db.ExecContext(ctx, "UPDATE test SET name = 'Updated' WHERE id = 1")
	require.NoError(t, err)

	rowsAffected, err := result.RowsAffected()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), rowsAffected)
}

func TestAutoSaveConnection_QueryContext(t *testing.T) {
	t.Parallel()

	// Create a test database connection
	tempDir := t.TempDir()
	csvFile := filepath.Join(tempDir, "test.csv")
	require.NoError(t, os.WriteFile(csvFile, []byte("id,name\n1,Alice\n2,Bob"), 0600))

	db, err := Open(csvFile)
	require.NoError(t, err)
	defer db.Close()

	ctx := context.Background()

	// Test QueryContext
	rows, err := db.QueryContext(ctx, "SELECT * FROM test WHERE id = ?", 1)
	require.NoError(t, err)
	defer rows.Close()

	var id int
	var name string
	require.True(t, rows.Next())
	require.NoError(t, rows.Scan(&id, &name))
	assert.Equal(t, 1, id)
	assert.Equal(t, "Alice", name)
	require.NoError(t, rows.Err())
}

func TestAutoSaveTransaction_Rollback(t *testing.T) {
	t.Parallel()

	// Create a test database connection
	tempDir := t.TempDir()
	csvFile := filepath.Join(tempDir, "test.csv")
	require.NoError(t, os.WriteFile(csvFile, []byte("id,name\n1,Alice\n2,Bob"), 0600))

	db, err := Open(csvFile)
	require.NoError(t, err)
	defer db.Close()

	// Begin transaction using BeginTx with context
	ctx := context.Background()
	tx, err := db.BeginTx(ctx, nil)
	require.NoError(t, err)

	// Execute a modification
	_, err = tx.ExecContext(ctx, "UPDATE test SET name = 'Modified' WHERE id = 1")
	require.NoError(t, err)

	// Rollback
	err = tx.Rollback()
	assert.NoError(t, err)

	// Verify data was not changed
	var name string
	err = db.QueryRowContext(ctx, "SELECT name FROM test WHERE id = 1").Scan(&name)
	require.NoError(t, err)
	assert.Equal(t, "Alice", name, "Rollback should have preserved original data")
}

// TestPerformFedWireAutoSave covers the Fedwire branch of a directory save.
// Fedwire is rebuilt from the file it was loaded from, so a database that
// records no such file has nothing to write and says so.
func TestPerformFedWireAutoSave(t *testing.T) {
	t.Parallel()

	t.Run("writes one file per loaded Fedwire source", func(t *testing.T) {
		t.Parallel()

		db, err := Open(filepath.Join("testdata", "customer-transfer.fed"))
		require.NoError(t, err)
		t.Cleanup(func() { _ = db.Close() })

		outputDir := filepath.Join(t.TempDir(), "out")
		require.NoError(t, (&autoSaveConnector{}).performFedWireAutoSave(db, outputDir))

		// The file is named after the base table, which is the sanitized file name.
		assert.FileExists(t, filepath.Join(outputDir, "customer_transfer.fed"))
	})

	t.Run("reports a database with no Fedwire source", func(t *testing.T) {
		t.Parallel()

		db := openTestDB(t)

		err := (&autoSaveConnector{}).performFedWireAutoSave(db, t.TempDir())
		assert.ErrorContains(t, err, "no Fedwire tables found to save")
	})

	t.Run("reports an output directory it cannot create", func(t *testing.T) {
		t.Parallel()

		db, err := Open(filepath.Join("testdata", "customer-transfer.fed"))
		require.NoError(t, err)
		t.Cleanup(func() { _ = db.Close() })

		// A regular file cannot also be a directory.
		blocked := filepath.Join(t.TempDir(), "in-the-way")
		require.NoError(t, os.WriteFile(blocked, nil, 0o600))

		err = (&autoSaveConnector{}).performFedWireAutoSave(db, filepath.Join(blocked, "out"))
		assert.ErrorContains(t, err, "failed to create output directory")
	})
}

// TestPerformACHAutoSave_UncreatableOutputDirectory is the same refusal on the
// ACH branch.
func TestPerformACHAutoSave_UncreatableOutputDirectory(t *testing.T) {
	t.Parallel()

	db, err := Open(filepath.Join("testdata", "ppd-debit.ach"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	blocked := filepath.Join(t.TempDir(), "in-the-way")
	require.NoError(t, os.WriteFile(blocked, nil, 0o600))

	err = (&autoSaveConnector{}).performACHAutoSave(db, filepath.Join(blocked, "out"))
	assert.ErrorContains(t, err, "failed to create output directory")
}

// TestOverwriteOriginalFiles_NothingToOverwrite covers a save in overwrite mode
// on a database that has no file behind it, which is what a load from an
// io.Reader leaves.
func TestOverwriteOriginalFiles_NothingToOverwrite(t *testing.T) {
	t.Parallel()

	err := (&autoSaveConnector{}).overwriteOriginalFiles(openTestDB(t))
	assert.ErrorContains(t, err, "no original paths available for overwrite")
}

// TestOverwriteOriginalFile_WriteBackFormatFailures covers the two formats that
// are rebuilt from their source file. Each is reported with the path it failed
// on, because a save of several files has to say which one did not land.
func TestOverwriteOriginalFile_WriteBackFormatFailures(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db := openTestDB(t)

	t.Run("ACH overwrite", func(t *testing.T) {
		t.Parallel()

		path := filepath.Join(t.TempDir(), "payment.ach")
		err := (&autoSaveConnector{}).overwriteOriginalFile(ctx, db, path)
		assert.ErrorContains(t, err, "failed to overwrite ACH file")
	})

	t.Run("Fedwire", func(t *testing.T) {
		t.Parallel()

		path := filepath.Join(t.TempDir(), "payment.fed")
		err := (&autoSaveConnector{}).overwriteOriginalFile(ctx, db, path)
		assert.ErrorContains(t, err, "failed to overwrite Fedwire file")
	})
}

// TestOverwriteFormatFor pins which source formats can be written back. A format
// this package reads but cannot write is refused by name: quietly writing CSV
// instead left the caller's file untouched and the change in a file they never
// named.
func TestOverwriteFormatFor(t *testing.T) {
	t.Parallel()

	tests := []struct {
		path string
		want OutputFormat
	}{
		{"data.csv", OutputFormatCSV},
		{"data.tsv", OutputFormatTSV},
		{"data.ltsv", OutputFormatLTSV},
		{"data.parquet", OutputFormatParquet},
		{"data.xlsx", OutputFormatXLSX},
		{"data.csv.gz", OutputFormatCSV},
	}
	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			t.Parallel()

			got, err := overwriteFormatFor(tt.path)
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}

	t.Run("a format with no writer is refused", func(t *testing.T) {
		t.Parallel()

		_, err := overwriteFormatFor("data.json")
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrUnsupportedFormat)
		assert.Contains(t, err.Error(), "data.json", "the refusal names the file it is about")
	})
}

// TestOverwriteWorkbookAtPath_Failures covers the workbook branch, which writes
// every sheet of a file in one staged write.
func TestOverwriteWorkbookAtPath_Failures(t *testing.T) {
	t.Parallel()

	t.Run("the tables cannot be listed", func(t *testing.T) {
		t.Parallel()

		db := openTestDB(t)
		require.NoError(t, db.Close())

		err := overwriteWorkbookAtPath(db, filepath.Join(t.TempDir(), "book.xlsx"), "book", nil, NewDumpOptions())
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrDatabaseOperation)
	})

	t.Run("no table of the workbook is left", func(t *testing.T) {
		t.Parallel()

		db := openTestDB(t)

		err := overwriteWorkbookAtPath(db, filepath.Join(t.TempDir(), "book.xlsx"), "book", nil, NewDumpOptions())
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrEmptyData)
	})
}

// TestWriteXLSXWorkbookCompressed_UnknownCodec covers the refusal of a codec the
// workbook writer cannot open, before any sheet is written.
func TestWriteXLSXWorkbookCompressed_UnknownCodec(t *testing.T) {
	t.Parallel()

	err := writeXLSXWorkbookCompressed(&bytes.Buffer{}, "book.xlsx", nil, nil, unknownCompression)
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrCompression)
}

// TestTablesFromWorkbook_UnreadableCatalog covers the listing behind a workbook
// save.
func TestTablesFromWorkbook_UnreadableCatalog(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	require.NoError(t, db.Close())

	_, err := tablesFromWorkbook(db, "book", nil)
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrDatabaseOperation)
}

// TestOverwriteTableAtPath_Failures covers the single-table write-back. A table
// that is gone by save time cannot be written, and truncating the caller's file
// to nothing would be worse than refusing.
func TestOverwriteTableAtPath_Failures(t *testing.T) {
	t.Parallel()

	t.Run("the columns cannot be read", func(t *testing.T) {
		t.Parallel()

		db := openTestDB(t)
		require.NoError(t, db.Close())

		err := overwriteTableAtPath(db, filepath.Join(t.TempDir(), "data.csv"), "data", NewDumpOptions())
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrDatabaseOperation)
	})

	t.Run("the table no longer exists", func(t *testing.T) {
		t.Parallel()

		db := openTestDB(t)

		path := filepath.Join(t.TempDir(), "data.csv")
		err := overwriteTableAtPath(db, path, "data", NewDumpOptions())
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrEmptyData)
		assert.NoFileExists(t, path, "a refused save must not create the file it could not write")
	})
}

// TestDumpDatabase_RefusesACodecItCannotWrite pins the error chain a dump asking
// for bzip2 reports, so a caller can tell "this codec cannot be written" from
// "the compressor failed" without matching on the message. ErrUnsupportedFormat
// used to be text only, because the writer flattened the inner error with %s;
// then both sentinels matched at once, which told the two apart no better, since
// the caller wrapped whatever the handler classified in ErrCompression anyway.
func TestDumpDatabase_RefusesACodecItCannotWrite(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	src := filepath.Join(dir, "users.csv")
	require.NoError(t, os.WriteFile(src, []byte("id,name\n1,alice\n"), 0o600))

	validated, err := buildForTest(t.Context(), NewBuilder().AddPath(src))
	require.NoError(t, err)
	db, err := validated.Open(t.Context())
	require.NoError(t, err)
	defer db.Close()

	out := filepath.Join(dir, "out")
	err = DumpDatabase(db, out, NewDumpOptions().WithCompression(CompressionBZ2))
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrUnsupportedFormat)
	assert.ErrorIs(t, err, ErrIOOperation)
	assert.NotErrorIs(t, err, ErrCompression, "nothing failed to compress; there is no bzip2 compressor")
	assert.Contains(t, err.Error(), "bzip2")
}

// TestCheckOverwriteTargets pins the pre-flight overwrite mode runs before it
// replaces anything. What matters is that a list is answered as a whole: a
// source that cannot be written is reported wherever it sits, so the earlier
// entries are never written on the way to finding it.
func TestCheckOverwriteTargets(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		paths   []string
		wantErr string
	}{
		{
			name:  "every source has a writer",
			paths: []string{"a.csv", "b.tsv", "c.ltsv", "d.parquet", "e.xlsx", "f.csv.gz"},
		},
		{
			name:  "ACH and Fedwire have writers of their own",
			paths: []string{"payment.ach", "transfer.fed"},
		},
		{
			name:    "a format with no writer, last",
			paths:   []string{"a.csv", "z.json"},
			wantErr: "z.json",
		},
		{
			name:    "a format with no writer, first",
			paths:   []string{"a.jsonl", "z.csv"},
			wantErr: "a.jsonl",
		},
		{
			name:    "a codec with no writer, last",
			paths:   []string{"a.csv", "z.tsv.bz2"},
			wantErr: "z.tsv.bz2",
		},
		{
			name:    "a codec with no writer, first",
			paths:   []string{"a.csv.bz2", "z.csv"},
			wantErr: "a.csv.bz2",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := checkOverwriteTargets(tt.paths)
			if tt.wantErr == "" {
				assert.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.ErrorIs(t, err, ErrUnsupportedFormat)
			assert.Contains(t, err.Error(), tt.wantErr)
		})
	}
}
