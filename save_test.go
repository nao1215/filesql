package filesql

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nao1215/filesql/internal/reader"
	"github.com/xuri/excelize/v2"
)

func TestAutoSaveConnection_PerformACHAutoSave(t *testing.T) {
	testFile := findTestACHFile(t)
	if testFile == "" {
		t.Skip("No test ACH file found")
	}

	ctx := context.Background()

	// Open ACH file to register table set
	db, err := Open(ctx, testFile)
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
	db, err := Open(ctx, filepath.Join("testdata", "test.csv"))
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
	db, err := Open(ctx, tmpFile)
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

	db, err := Open(ctx, filepath.Join("testdata", "test.csv"))
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

func TestAutoSaveConnection_PerformACHAutoSave_DumpError(t *testing.T) {
	ctx := context.Background()

	// Open a CSV file to get a valid db connection
	db, err := Open(ctx, filepath.Join("testdata", "test.csv"))
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

	db, err := Open(context.Background(), csvFile)
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

	db, err := Open(context.Background(), csvFile)
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

	db, err := Open(context.Background(), csvFile)
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

		db, err := Open(context.Background(), filepath.Join("testdata", "customer-transfer.fed"))
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

		db, err := Open(context.Background(), filepath.Join("testdata", "customer-transfer.fed"))
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

	db, err := Open(context.Background(), filepath.Join("testdata", "ppd-debit.ach"))
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
		err := (&autoSaveConnector{}).overwriteOriginalFile(ctx, db, path, nil)
		assert.ErrorContains(t, err, "failed to overwrite ACH file")
	})

	t.Run("Fedwire", func(t *testing.T) {
		t.Parallel()

		path := filepath.Join(t.TempDir(), "payment.fed")
		err := (&autoSaveConnector{}).overwriteOriginalFile(ctx, db, path, nil)
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

		err := overwriteWorkbookAtPath(db, filepath.Join(t.TempDir(), "book.xlsx"), "book", nil, NewDumpOptions(), ExcelSheetPolicyAll, nil)
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrDatabaseOperation)
	})

	t.Run("no table of the workbook is left", func(t *testing.T) {
		t.Parallel()

		db := openTestDB(t)

		err := overwriteWorkbookAtPath(db, filepath.Join(t.TempDir(), "book.xlsx"), "book", nil, NewDumpOptions(), ExcelSheetPolicyAll, nil)
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrTableNotFound)
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

		err := overwriteTableAtPath(db, filepath.Join(t.TempDir(), "data.csv"), "data", NewDumpOptions(), nil)
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrDatabaseOperation)
	})

	t.Run("the table no longer exists", func(t *testing.T) {
		t.Parallel()

		db := openTestDB(t)

		path := filepath.Join(t.TempDir(), "data.csv")
		err := overwriteTableAtPath(db, path, "data", NewDumpOptions(), nil)
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrTableNotFound)
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
	err = DumpDatabase(context.Background(), db, out, NewDumpOptions().WithCompression(CompressionBZ2))
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

// TestDumpDatabase_RefusesABlankDestination pins what a dump does with a
// destination that names nothing. A path of spaces used to be a directory name,
// so the tables were written into a directory of spaces in the working
// directory -- one this package then refused to read back, since an input path
// of spaces is ErrEmptyPath. An empty one used to fail with the operating
// system's own "mkdir : no such file or directory", which carries neither a
// sentinel nor the name of what went wrong.
func TestDumpDatabase_RefusesABlankDestination(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "users.csv")
	require.NoError(t, os.WriteFile(src, []byte("id,name\n1,alice\n"), 0o600))

	db, err := Open(t.Context(), src)
	require.NoError(t, err)
	defer db.Close()

	// The blank destinations are relative, so the dump would write into the
	// working directory. Watching that directory is what proves nothing was
	// written rather than written somewhere else.
	work := t.TempDir()
	t.Chdir(work)

	for _, path := range []string{"", " ", "   ", "\t", "\n"} {
		err := DumpDatabase(context.Background(), db, path)
		assert.ErrorIs(t, err, ErrEmptyPath, "DumpDatabase(context.Background(), db, %q)", path)

		entries, readErr := os.ReadDir(work)
		require.NoError(t, readErr)
		assert.Empty(t, entries, "DumpDatabase(context.Background(), db, %q) created something in the working directory", path)
	}
}

// TestDumpDatabase_RefusesADestinationThatIsNotADirectory pins that a dump onto
// an existing file says so in this package's words. It used to answer the raw
// "mkdir /path/file: not a directory".
func TestDumpDatabase_RefusesADestinationThatIsNotADirectory(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	src := filepath.Join(dir, "users.csv")
	require.NoError(t, os.WriteFile(src, []byte("id,name\n1,alice\n"), 0o600))
	occupied := filepath.Join(dir, "occupied")
	require.NoError(t, os.WriteFile(occupied, []byte("x"), 0o600))

	db, err := Open(t.Context(), src)
	require.NoError(t, err)
	defer db.Close()

	err = DumpDatabase(context.Background(), db, occupied)
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrIOOperation)
	assert.Contains(t, err.Error(), "not a directory")
	assert.Contains(t, err.Error(), occupied)
	assert.NotContains(t, err.Error(), "mkdir", "the refusal is this package's own, not the one mkdir writes")
}

// TestSheetKeyFoldsTheWayExcelDoes pins which of the two folds this package
// uses answers here. A table name compared against another table name folds
// ASCII case, because SQLite's does. A table matched to the sheet it lives in
// folds the way the library writing the workbook matches sheet names, which is
// strings.EqualFold: folding only ASCII would miss the sheet and ask for a new
// one, and asking for a sheet that is already there hands back the existing
// one, whose rows the save then overwrites.
func TestSheetKeyFoldsTheWayExcelDoes(t *testing.T) {
	t.Parallel()

	// Every pair strings.EqualFold calls equal has to reach one key, including
	// the pairs strings.ToLower keys apart: sigma has three forms in one fold
	// orbit, and the long s folds to an ordinary one.
	equal := []struct{ a, b string }{
		{"book_xÄy", "book_xäy"},
		{"book_Data", "book_data"},
		{"book_xΣy", "book_xςy"},
		{"book_xΣy", "book_xσy"},
		{"book_xſy", "book_xsy"},
		{"book_xKy", "book_x\u212ay"},
	}
	for _, tc := range equal {
		if !strings.EqualFold(tc.a, tc.b) {
			t.Fatalf("test is wrong: %q and %q are not EqualFold", tc.a, tc.b)
		}
		if sheetKey(tc.a) != sheetKey(tc.b) {
			t.Errorf("sheetKey(%q) = %q and sheetKey(%q) = %q, but the workbook writer calls them one sheet",
				tc.a, sheetKey(tc.a), tc.b, sheetKey(tc.b))
		}
	}

	// And nothing else: two names the writer keeps apart must keep two keys.
	different := []struct{ a, b string }{
		{"book_orders", "book_invoices"},
		{"book_a", "book_ab"},
		{"book_xäy", "book_xay"},
	}
	for _, tc := range different {
		if sheetKey(tc.a) == sheetKey(tc.b) {
			t.Errorf("sheetKey collapsed %q and %q, which are two sheets", tc.a, tc.b)
		}
	}

	// The other fold, which this one is not: SQLite holds these as two tables.
	if reader.ASCIIFold("book_xÄy") == reader.ASCIIFold("book_xäy") {
		t.Error("ASCIIFold matched two names SQLite holds as two tables, which is the fold this is not")
	}
}

// TestOverwriteWorkbookRefusesTwoTablesForOneSheet drives the guard that keeps
// an in-place save from writing two tables into one sheet. The workbook writer
// matches sheet names without regard to case, so two tables whose sheets it
// calls one sheet have to be refused here; comparing the names as written let
// them past, and the second table's rows overwrote the first's while Close
// reported success.
func TestOverwriteWorkbookRefusesTwoTablesForOneSheet(t *testing.T) {
	t.Parallel()

	newBook := func(t *testing.T) string {
		t.Helper()
		path := filepath.Join(t.TempDir(), "book.xlsx")
		f := excelize.NewFile()
		require.NoError(t, f.SetSheetName("Sheet1", "s"))
		require.NoError(t, f.SetCellValue("s", "A1", "name"))
		require.NoError(t, f.SetCellValue("s", "A2", "s-value"))
		require.NoError(t, f.SaveAs(path))
		require.NoError(t, f.Close())
		return path
	}

	saveWith := func(t *testing.T, path string, statements ...string) error {
		t.Helper()
		ctx := context.Background()
		db, err := NewBuilder().AddPath(path).EnableAutoSave("").Open(ctx)
		require.NoError(t, err)
		for _, s := range statements {
			_, err := db.ExecContext(ctx, s)
			require.NoError(t, err)
		}
		return db.Close()
	}

	t.Run("names differing only outside ASCII are one sheet", func(t *testing.T) {
		t.Parallel()
		err := saveWith(t, newBook(t),
			`CREATE TABLE "book_xäy" (name TEXT)`,
			`INSERT INTO "book_xäy" VALUES ('lower')`,
			`CREATE TABLE "book_xÄy" (name TEXT)`,
			`INSERT INTO "book_xÄy" VALUES ('upper')`,
		)
		require.Error(t, err, "two tables that become one sheet must be refused")
		assert.ErrorIs(t, err, ErrUnsupportedFormat)
	})

	t.Run("names in one fold orbit are one sheet", func(t *testing.T) {
		t.Parallel()
		// Sigma has three forms in one orbit, so the workbook writer calls
		// these one sheet where lowercasing would have keyed them apart.
		err := saveWith(t, newBook(t),
			`CREATE TABLE "book_xΣy" (name TEXT)`,
			`INSERT INTO "book_xΣy" VALUES ('capital sigma')`,
			`CREATE TABLE "book_xςy" (name TEXT)`,
			`INSERT INTO "book_xςy" VALUES ('final sigma')`,
		)
		require.Error(t, err, "two tables that become one sheet must be refused")
		assert.ErrorIs(t, err, ErrUnsupportedFormat)
	})

	t.Run("two ordinary tables reach two sheets", func(t *testing.T) {
		t.Parallel()
		path := newBook(t)
		require.NoError(t, saveWith(t, path,
			`CREATE TABLE "book_orders" (name TEXT)`,
			`INSERT INTO "book_orders" VALUES ('an order')`,
		))

		f, err := excelize.OpenFile(path)
		require.NoError(t, err)
		defer func() { require.NoError(t, f.Close()) }()
		assert.ElementsMatch(t, []string{"s", "orders"}, f.GetSheetList())
	})
}

// TestSaveNamesAMissingTableAsMissing pins which sentinel a save carries when
// the table a source was loaded from is gone. It is not an empty data source:
// the source had records and the table was dropped, and a caller who reads
// ErrEmptyData to mean "the file I named was empty" would take that branch for
// a table they dropped themselves.
func TestSaveNamesAMissingTableAsMissing(t *testing.T) {
	t.Parallel()

	t.Run("a file whose table was dropped", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()
		path := filepath.Join(dir, "a.csv")
		require.NoError(t, os.WriteFile(path, []byte("n\nfirst\n"), 0o600))

		ctx := context.Background()
		db, err := NewBuilder().AddPath(path).EnableAutoSave("").Open(ctx)
		require.NoError(t, err)
		_, err = db.ExecContext(ctx, `DROP TABLE a`)
		require.NoError(t, err)

		err = db.Close()
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrTableNotFound)
		assert.NotErrorIs(t, err, ErrEmptyData,
			"a dropped table is not a source with no records; the two sentinels have to stay apart")
		assert.Contains(t, err.Error(), "a", "the message still names the table")
	})

	t.Run("a workbook whose only table was dropped", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()
		path := filepath.Join(dir, "book.xlsx")
		f := excelize.NewFile()
		require.NoError(t, f.SetSheetName("Sheet1", "data"))
		require.NoError(t, f.SetCellValue("data", "A1", "n"))
		require.NoError(t, f.SetCellValue("data", "A2", "first"))
		require.NoError(t, f.SaveAs(path))
		require.NoError(t, f.Close())

		ctx := context.Background()
		db, err := NewBuilder().AddPath(path).EnableAutoSave("").Open(ctx)
		require.NoError(t, err)
		_, err = db.ExecContext(ctx, `DROP TABLE book_data`)
		require.NoError(t, err)

		err = db.Close()
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrTableNotFound)
		assert.NotErrorIs(t, err, ErrEmptyData)
	})

	t.Run("a file that really has no records still answers ErrEmptyData", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()
		path := filepath.Join(dir, "empty.csv")
		require.NoError(t, os.WriteFile(path, nil, 0o600))

		_, err := Open(context.Background(), path)
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrEmptyData)
	})
}

// TestOverwriteWorkbookRefusesASheetWhoseTableIsGone pins what a workbook save
// does with a sheet whose table the session removed.
//
// Renaming a table used to write the new name as a new sheet and leave the old
// sheet with every row it had, so the workbook came back holding the rows twice
// and Close reported success; loading it again gave two tables where the
// session had one. The file-per-table save already refuses when the table a
// file was loaded from is gone, and this is the same situation.
func TestOverwriteWorkbookRefusesASheetWhoseTableIsGone(t *testing.T) {
	t.Parallel()

	// book builds a workbook of one column per named sheet and returns its path.
	book := func(t *testing.T, dir string, sheets ...string) string {
		t.Helper()

		rows := make(map[string][][]string, len(sheets))
		for _, name := range sheets {
			rows[name] = [][]string{{"n"}, {name + "-first"}, {name + "-second"}}
		}
		path := filepath.Join(dir, "book.xlsx")
		writeWorkbook(t, path, rows)
		return path
	}

	saveAfter := func(t *testing.T, path string, opts []func(*DBBuilder) *DBBuilder, statements ...string) error {
		t.Helper()
		ctx := context.Background()
		b := NewBuilder().AddPath(path).EnableAutoSave("")
		for _, o := range opts {
			b = o(b)
		}
		db, err := b.Open(ctx)
		require.NoError(t, err)
		for _, s := range statements {
			_, err := db.ExecContext(ctx, s)
			require.NoError(t, err)
		}
		return db.Close()
	}

	t.Run("a renamed table", func(t *testing.T) {
		t.Parallel()

		path := book(t, t.TempDir(), "data")
		err := saveAfter(t, path, nil, `ALTER TABLE book_data RENAME TO book_renamed`)
		require.Error(t, err, "the rows would otherwise be in the workbook twice")
		assert.ErrorIs(t, err, ErrTableNotFound)
		assert.Contains(t, err.Error(), "data", "the message names the sheet left without a table")
	})

	t.Run("one table of two dropped", func(t *testing.T) {
		t.Parallel()

		path := book(t, t.TempDir(), "data", "other")
		err := saveAfter(t, path, nil, `DROP TABLE book_other`)
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrTableNotFound)
		assert.Contains(t, err.Error(), "other")
	})

	t.Run("every table still there saves", func(t *testing.T) {
		t.Parallel()

		path := book(t, t.TempDir(), "data", "other")
		require.NoError(t, saveAfter(t, path, nil,
			`UPDATE book_data SET n = 'edited' WHERE n = 'data-first'`))

		f, err := excelize.OpenFile(path)
		require.NoError(t, err)
		defer func() { require.NoError(t, f.Close()) }()
		assert.ElementsMatch(t, []string{"data", "other"}, f.GetSheetList())
		rows, err := f.GetRows("data")
		require.NoError(t, err)
		assert.Equal(t, [][]string{{"n"}, {"edited"}, {"data-second"}}, rows)
	})

	t.Run("a table created during the session is still a new sheet", func(t *testing.T) {
		t.Parallel()

		path := book(t, t.TempDir(), "data")
		require.NoError(t, saveAfter(t, path, nil,
			`CREATE TABLE book_extra (x TEXT)`, `INSERT INTO book_extra VALUES ('new')`))

		f, err := excelize.OpenFile(path)
		require.NoError(t, err)
		defer func() { require.NoError(t, f.Close()) }()
		assert.ElementsMatch(t, []string{"data", "extra"}, f.GetSheetList())
	})

	t.Run("a session table cannot take the name of a sheet the policy left out", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()
		path := book(t, dir, "data", "hidden")
		f, err := excelize.OpenFile(path)
		require.NoError(t, err)
		require.NoError(t, f.SetSheetVisible("hidden", false))
		require.NoError(t, f.Save())
		require.NoError(t, f.Close())

		// The hidden sheet is not loaded, so nothing holds its name; the
		// workbook writer would answer a request for it with that sheet and the
		// rows it holds would be gone.
		err = saveAfter(t, path, []func(*DBBuilder) *DBBuilder{
			func(b *DBBuilder) *DBBuilder { return b.WithExcelSheetPolicy(ExcelSheetPolicyVisibleOnly) },
		}, `CREATE TABLE book_hidden (n TEXT)`, `INSERT INTO book_hidden VALUES ('from the session')`)
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrUnsupportedFormat)
		assert.Contains(t, err.Error(), "hidden")

		out, err := excelize.OpenFile(path)
		require.NoError(t, err)
		defer func() { require.NoError(t, out.Close()) }()
		rows, err := out.GetRows("hidden")
		require.NoError(t, err)
		assert.Equal(t, [][]string{{"n"}, {"hidden-first"}, {"hidden-second"}}, rows,
			"a refused save must not have written the sheet already")
	})

	t.Run("a session table whose name collides with nothing is still a new sheet", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()
		path := book(t, dir, "data", "hidden")
		f, err := excelize.OpenFile(path)
		require.NoError(t, err)
		require.NoError(t, f.SetSheetVisible("hidden", false))
		require.NoError(t, f.Save())
		require.NoError(t, f.Close())

		require.NoError(t, saveAfter(t, path, []func(*DBBuilder) *DBBuilder{
			func(b *DBBuilder) *DBBuilder { return b.WithExcelSheetPolicy(ExcelSheetPolicyVisibleOnly) },
		}, `CREATE TABLE book_extra (n TEXT)`, `INSERT INTO book_extra VALUES ('new')`))

		out, err := excelize.OpenFile(path)
		require.NoError(t, err)
		defer func() { require.NoError(t, out.Close()) }()
		assert.ElementsMatch(t, []string{"data", "hidden", "extra"}, out.GetSheetList())
	})

	t.Run("a sheet the policy did not load is not a missing table", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()
		path := book(t, dir, "data", "hidden")
		f, err := excelize.OpenFile(path)
		require.NoError(t, err)
		require.NoError(t, f.SetSheetVisible("hidden", false))
		require.NoError(t, f.Save())
		require.NoError(t, f.Close())

		// The hidden sheet is never loaded, so no table for it exists and none
		// went missing; the save has to tell those two apart.
		err = saveAfter(t, path, []func(*DBBuilder) *DBBuilder{
			func(b *DBBuilder) *DBBuilder { return b.WithExcelSheetPolicy(ExcelSheetPolicyVisibleOnly) },
		}, `UPDATE book_data SET n = 'edited' WHERE n = 'data-first'`)
		require.NoError(t, err)

		out, err := excelize.OpenFile(path)
		require.NoError(t, err)
		defer func() { require.NoError(t, out.Close()) }()
		rows, err := out.GetRows("hidden")
		require.NoError(t, err)
		assert.Equal(t, [][]string{{"n"}, {"hidden-first"}, {"hidden-second"}}, rows,
			"the sheet the policy skipped keeps its rows")
	})
}
