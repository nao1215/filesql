package filesql

import (
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

	// Check that ACH tables are registered
	baseNames := getACHBaseTableNames()
	require.NotEmpty(t, baseNames, "ACH tables should be registered")

	// Create temp output directory
	outputDir := t.TempDir()

	// Create autoSaveConnection with ACH format
	config := &autoSaveConfig{
		enabled:   true,
		timing:    autoSaveOnClose,
		outputDir: outputDir,
		options:   NewDumpOptions().WithFormat(OutputFormatACH),
	}

	conn := &autoSaveConnection{
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

	// Clear any existing ACH registrations
	ClearACHTableSetRegistry()

	outputDir := t.TempDir()

	conn := &autoSaveConnection{
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

	// Create autoSaveConnection for overwrite mode (no outputDir)
	conn := &autoSaveConnection{
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

	conn := &autoSaveConnection{
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

func TestAutoSaveConnection_CleanupACHRegistry(t *testing.T) {
	testFile := findTestACHFile(t)
	if testFile == "" {
		t.Skip("No test ACH file found")
	}

	ctx := context.Background()

	// Open ACH file to register table set
	db, err := OpenContext(ctx, testFile)
	require.NoError(t, err)

	// Verify tables are registered
	baseNames := getACHBaseTableNames()
	require.NotEmpty(t, baseNames, "ACH tables should be registered after opening")

	// Create connection with ACH path
	conn := &autoSaveConnection{
		originalPaths: []string{testFile},
	}

	// Clean up registry
	conn.cleanupTableSetRegistries()

	// Close the db
	require.NoError(t, db.Close())

	// Tables should be unregistered now
	// Note: We need to check the specific table, not all tables
	baseTableName := sanitizeTableName(tableFromFilePath(testFile))
	ts := getACHTableSet(baseTableName)
	assert.Nil(t, ts, "ACH table set should be unregistered after cleanup")
}

func TestAutoSaveConnection_Begin(t *testing.T) {
	ctx := context.Background()

	// Use Builder to create a database with auto-save config
	builder := NewBuilder().AddPath(filepath.Join("testdata", "test.csv"))
	validatedBuilder, err := builder.Build(ctx)
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

	// Clear any existing ACH registrations and register a fake one
	ClearACHTableSetRegistry()

	// Register a fake ACH table set with an invalid base name
	// This will cause DumpACH to fail because the tables don't exist
	registerACHTableSet("nonexistent_ach_table", nil)
	defer UnregisterACHTableSet("nonexistent_ach_table")

	outputDir := t.TempDir()

	conn := &autoSaveConnection{
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

func TestAutoSaveConnection_CleanupTableSetRegistries_TabularPath(t *testing.T) {
	// Test that cleanupTableSetRegistries handles tabular-only paths correctly (no-op)
	conn := &autoSaveConnection{
		originalPaths: []string{"test.csv", "data.tsv"},
	}

	// Should not panic or error - just silently skip tabular files
	conn.cleanupTableSetRegistries()

	// Verify connection state is valid after cleanup
	assert.NotNil(t, conn.originalPaths)
}

func TestAutoSaveConnection_PerformAutoSave_Disabled(t *testing.T) {
	conn := &autoSaveConnection{
		autoSaveConfig: nil, // No config = disabled
	}

	// Should return nil when auto-save is disabled
	err := conn.performAutoSave()
	assert.NoError(t, err)

	// Also test with config but disabled
	conn2 := &autoSaveConnection{
		autoSaveConfig: &autoSaveConfig{
			enabled: false,
		},
	}
	err = conn2.performAutoSave()
	assert.NoError(t, err)
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
