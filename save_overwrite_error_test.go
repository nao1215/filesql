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

		err := overwriteWorkbookAtPath(db, filepath.Join(t.TempDir(), "book.xlsx"), "book", NewDumpOptions())
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrDatabaseOperation)
	})

	t.Run("no table of the workbook is left", func(t *testing.T) {
		t.Parallel()

		db := openTestDB(t)

		err := overwriteWorkbookAtPath(db, filepath.Join(t.TempDir(), "book.xlsx"), "book", NewDumpOptions())
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrEmptyData)
	})
}

// TestWriteXLSXWorkbookCompressed_UnknownCodec covers the refusal of a codec the
// workbook writer cannot open, before any sheet is written.
func TestWriteXLSXWorkbookCompressed_UnknownCodec(t *testing.T) {
	t.Parallel()

	err := writeXLSXWorkbookCompressed(&bytes.Buffer{}, "book.xlsx", nil, unknownCompression)
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrCompression)
}

// TestTablesFromWorkbook_UnreadableCatalog covers the listing behind a workbook
// save.
func TestTablesFromWorkbook_UnreadableCatalog(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	require.NoError(t, db.Close())

	_, err := tablesFromWorkbook(db, "book")
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
