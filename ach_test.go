package filesql

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOpenACHFile(t *testing.T) {
	testFile := findTestACHFile(t)
	if testFile == "" {
		t.Skip("No test ACH file found")
	}

	ctx := context.Background()
	db, err := OpenContext(ctx, testFile)
	require.NoError(t, err)
	defer db.Close()

	// Check that tables were created
	rows, err := db.QueryContext(ctx, "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
	require.NoError(t, err)
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var name string
		require.NoError(t, rows.Scan(&name))
		tables = append(tables, name)
	}
	require.NoError(t, rows.Err())

	t.Logf("Created tables: %v", tables)

	// Should have at least entries table
	assert.Contains(t, tables, "ppd_debit_entries", "entries table should exist")
}

func TestOpenACHFile_QueryEntries(t *testing.T) {
	testFile := findTestACHFile(t)
	if testFile == "" {
		t.Skip("No test ACH file found")
	}

	ctx := context.Background()
	db, err := OpenContext(ctx, testFile)
	require.NoError(t, err)
	defer db.Close()

	// Query entries
	rows, err := db.QueryContext(ctx, "SELECT transaction_code, amount, individual_name FROM ppd_debit_entries")
	require.NoError(t, err)
	defer rows.Close()

	var count int
	for rows.Next() {
		var txCode int
		var amount int
		var name string
		require.NoError(t, rows.Scan(&txCode, &amount, &name))
		t.Logf("Entry: txCode=%d, amount=%d, name=%s", txCode, amount, name)
		count++
	}
	require.NoError(t, rows.Err())

	assert.Greater(t, count, 0, "should have at least one entry")
}

func TestOpenACHFile_QueryBatches(t *testing.T) {
	testFile := findTestACHFile(t)
	if testFile == "" {
		t.Skip("No test ACH file found")
	}

	ctx := context.Background()
	db, err := OpenContext(ctx, testFile)
	require.NoError(t, err)
	defer db.Close()

	// Query batches
	rows, err := db.QueryContext(ctx, "SELECT standard_entry_class_code, company_name FROM ppd_debit_batches")
	require.NoError(t, err)
	defer rows.Close()

	var count int
	for rows.Next() {
		var secCode string
		var companyName string
		require.NoError(t, rows.Scan(&secCode, &companyName))
		t.Logf("Batch: SEC=%s, company=%s", secCode, companyName)
		count++
	}
	require.NoError(t, rows.Err())

	assert.Greater(t, count, 0, "should have at least one batch")
}

func TestOpenACHFile_JoinEntriesAndBatches(t *testing.T) {
	testFile := findTestACHFile(t)
	if testFile == "" {
		t.Skip("No test ACH file found")
	}

	ctx := context.Background()
	db, err := OpenContext(ctx, testFile)
	require.NoError(t, err)
	defer db.Close()

	// Join entries with batches
	query := `
		SELECT
			e.amount,
			e.individual_name,
			b.standard_entry_class_code,
			b.company_name
		FROM ppd_debit_entries e
		JOIN ppd_debit_batches b ON e.batch_index = b.batch_index
	`
	rows, err := db.QueryContext(ctx, query)
	require.NoError(t, err)
	defer rows.Close()

	var count int
	for rows.Next() {
		var amount int
		var individualName, secCode, companyName string
		require.NoError(t, rows.Scan(&amount, &individualName, &secCode, &companyName))
		t.Logf("Joined: amount=%d, individual=%s, SEC=%s, company=%s",
			amount, individualName, secCode, companyName)
		count++
	}
	require.NoError(t, rows.Err())

	assert.Greater(t, count, 0, "should have at least one joined row")
}

func TestIsACHFile(t *testing.T) {
	tests := []struct {
		path     string
		expected bool
	}{
		{"payment.ach", true},
		{"payment.ACH", true}, // case insensitive
		{"payment.Ach", true}, // mixed case
		{"data.csv", false},
		{"data.ach.gz", false}, // compression not supported yet
		{".ach", false},        // too short
		{".ACH", false},        // too short (uppercase)
	}

	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			result := isACHFile(tt.path)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestBuilderAddPathACH(t *testing.T) {
	testFile := findTestACHFile(t)
	if testFile == "" {
		t.Skip("No test ACH file found")
	}

	ctx := context.Background()
	builder := NewBuilder().AddPath(testFile)
	builder, err := builder.Build(ctx)
	require.NoError(t, err)

	db, err := builder.Open(ctx)
	require.NoError(t, err)
	defer db.Close()

	// Verify entries table exists
	var count int
	err = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM ppd_debit_entries").Scan(&count)
	require.NoError(t, err)
	assert.Greater(t, count, 0)
}

func TestOpenACHFile_UpdateAndWrite(t *testing.T) {
	testFile := findTestACHFile(t)
	if testFile == "" {
		t.Skip("No test ACH file found")
	}

	ctx := context.Background()
	db, err := OpenContext(ctx, testFile)
	require.NoError(t, err)
	defer db.Close()

	// Update individual_name (doesn't affect batch control totals)
	_, err = db.ExecContext(ctx, "UPDATE ppd_debit_entries SET individual_name = 'Updated Name' WHERE entry_index = 0")
	require.NoError(t, err)

	// Create temp file for output
	tmpFile := filepath.Join(t.TempDir(), "output.ach")

	// Write modified ACH file
	err = DumpACH(ctx, db, "ppd_debit", tmpFile)
	require.NoError(t, err)

	// Verify file was created
	info, err := os.Stat(tmpFile)
	require.NoError(t, err)
	assert.Greater(t, info.Size(), int64(0), "output file should not be empty")

	// Re-open the written file and verify the change
	db2, err := OpenContext(ctx, tmpFile)
	require.NoError(t, err)
	defer db2.Close()

	var name string
	err = db2.QueryRowContext(ctx, "SELECT individual_name FROM output_entries WHERE entry_index = 0").Scan(&name)
	require.NoError(t, err)
	assert.Equal(t, "Updated Name", name, "name should be updated")
}

// TestOpenACHFile_UpdateAmountAndWrite covers the edit the test above steps
// around. An amount is what a batch control totals, so changing one is the case
// that decides whether write-back recalculates the controls or writes the file
// with the totals it was read with. It used to be the latter, and every amount
// edit failed to write as out-of-balance, which left no way to change an amount
// at all.
func TestOpenACHFile_UpdateAmountAndWrite(t *testing.T) {
	testFile := findTestACHFile(t)
	if testFile == "" {
		t.Skip("No test ACH file found")
	}

	ctx := context.Background()
	db, err := OpenContext(ctx, testFile)
	require.NoError(t, err)
	defer db.Close()

	var before int
	require.NoError(t, db.QueryRowContext(ctx,
		"SELECT amount FROM ppd_debit_entries WHERE entry_index = 0").Scan(&before))

	after := before + 1
	_, err = db.ExecContext(ctx,
		"UPDATE ppd_debit_entries SET amount = ? WHERE entry_index = 0", after)
	require.NoError(t, err)

	tmpFile := filepath.Join(t.TempDir(), "output.ach")
	require.NoError(t, DumpACH(ctx, db, "ppd_debit", tmpFile))

	// Re-opening runs the reader's own balance check, so a file that opens is a
	// file whose controls agree with its entries.
	db2, err := OpenContext(ctx, tmpFile)
	require.NoError(t, err)
	defer db2.Close()

	var got int
	require.NoError(t, db2.QueryRowContext(ctx,
		"SELECT amount FROM output_entries WHERE entry_index = 0").Scan(&got))
	assert.Equal(t, after, got, "the edited amount should survive the write")

	var totalDebit int
	require.NoError(t, db2.QueryRowContext(ctx,
		"SELECT total_debit FROM output_batches WHERE batch_index = 0").Scan(&totalDebit))
	assert.Equal(t, after, totalDebit, "the batch control should follow the entries")
}

// TestOpenACHFile_RefusesOverWidthText covers the other half of what a
// write-back has to do with an edit it cannot honor. An amount the record
// cannot hold fails the write and says so; a name too long for its fixed-width
// field was cut to fit and written at no error, so the file on disk held
// different data than the session that wrote it.
func TestOpenACHFile_RefusesOverWidthText(t *testing.T) {
	testFile := findTestACHFile(t)
	if testFile == "" {
		t.Skip("No test ACH file found")
	}

	tests := []struct {
		name   string
		update string
		column string
	}{
		{
			// IndividualName holds 22 characters.
			name:   "individual name past its field",
			update: "UPDATE ppd_debit_entries SET individual_name = 'This name is much longer than twenty two characters' WHERE entry_index = 0",
			column: "individual_name",
		},
		{
			// CompanyName holds 16.
			name:   "company name past its field",
			update: "UPDATE ppd_debit_batches SET company_name = 'A company name far beyond sixteen characters' WHERE batch_index = 0",
			column: "company_name",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			db, err := OpenContext(ctx, testFile)
			require.NoError(t, err)
			defer db.Close()

			_, err = db.ExecContext(ctx, tt.update)
			require.NoError(t, err)

			out := filepath.Join(t.TempDir(), "output.ach")
			err = DumpACH(ctx, db, "ppd_debit", out)
			require.Error(t, err, "a value the record cannot hold must fail the write, not be cut to fit")
			assert.ErrorIs(t, err, ErrACH)
			assert.Contains(t, err.Error(), tt.column)

			_, statErr := os.Stat(out)
			assert.True(t, os.IsNotExist(statErr), "a refused write must leave no file behind")
		})
	}
}

func TestOpenACHFile_IATFile(t *testing.T) {
	testFile := findACHTestFile(t, "iat-credit.ach")
	if testFile == "" {
		t.Skip("No IAT test ACH file found")
	}

	ctx := context.Background()
	db, err := OpenContext(ctx, testFile)
	require.NoError(t, err)
	defer db.Close()

	// Check that IAT tables were created
	rows, err := db.QueryContext(ctx, "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE '%iat%' ORDER BY name")
	require.NoError(t, err)
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var name string
		require.NoError(t, rows.Scan(&name))
		tables = append(tables, name)
	}
	require.NoError(t, rows.Err())

	t.Logf("IAT tables created: %v", tables)
	assert.NotEmpty(t, tables, "should have IAT tables")
}

func TestOpenACHFile_ReturnFile(t *testing.T) {
	// This test requires fileparser v0.3.0+ with the duplicate column fix
	// Skip until the dependency is updated
	t.Skip("Requires fileparser v0.3.0+ with duplicate column fix")

	testFile := findACHTestFile(t, "return-WEB.ach")
	if testFile == "" {
		t.Skip("No return test ACH file found")
	}

	ctx := context.Background()
	db, err := OpenContext(ctx, testFile)
	require.NoError(t, err)
	defer db.Close()

	// Query addenda table for return records
	rows, err := db.QueryContext(ctx, "SELECT addenda_type, return_code FROM return_WEB_addenda WHERE addenda_type = '99'")
	require.NoError(t, err)
	defer rows.Close()

	var count int
	for rows.Next() {
		var addendaType, returnCode string
		require.NoError(t, rows.Scan(&addendaType, &returnCode))
		t.Logf("Return addenda: type=%s, return_code=%s", addendaType, returnCode)
		count++
	}
	require.NoError(t, rows.Err())

	assert.Greater(t, count, 0, "should have return addenda records")
}

func TestWriteACHFile_InvalidTableSet(t *testing.T) {
	ctx := context.Background()

	// Create a database without ACH tables
	db, err := OpenContext(ctx, filepath.Join("testdata", "test.csv"))
	require.NoError(t, err)
	defer db.Close()

	tmpFile := filepath.Join(t.TempDir(), "output.ach")
	err = DumpACH(ctx, db, "nonexistent", tmpFile)
	assert.Error(t, err, "should fail with invalid table set")
}

func TestDumpACH_NilTableSet(t *testing.T) {
	ctx := context.Background()

	// Open a CSV file (not ACH)
	db, err := OpenContext(ctx, filepath.Join("testdata", "test.csv"))
	require.NoError(t, err)
	defer db.Close()

	tmpFile := filepath.Join(t.TempDir(), "output.ach")
	err = DumpACH(ctx, db, "test", tmpFile)
	assert.Error(t, err, "should fail when tableSet is not found")
}

// findTestACHFile looks for a test ACH file in common locations.
func findTestACHFile(t *testing.T) string {
	t.Helper()
	return findACHTestFile(t, "ppd-debit.ach")
}

// findACHTestFile looks for a specific test ACH file in testdata directory.
func findACHTestFile(t *testing.T, filename string) string {
	t.Helper()

	path := filepath.Join("testdata", filename)
	if _, err := os.Stat(path); err == nil {
		return path
	}

	return ""
}

func TestACHTableInfo_TableNames(t *testing.T) {
	t.Parallel()

	info := ACHTableInfo{BaseName: "payment"}

	t.Run("FileHeaderTable", func(t *testing.T) {
		assert.Equal(t, "payment_file_header", info.FileHeaderTable())
	})

	t.Run("BatchesTable", func(t *testing.T) {
		assert.Equal(t, "payment_batches", info.BatchesTable())
	})

	t.Run("EntriesTable", func(t *testing.T) {
		assert.Equal(t, "payment_entries", info.EntriesTable())
	})

	t.Run("AddendaTable", func(t *testing.T) {
		assert.Equal(t, "payment_addenda", info.AddendaTable())
	})

	t.Run("IATBatchesTable", func(t *testing.T) {
		assert.Equal(t, "payment_iat_batches", info.IATBatchesTable())
	})

	t.Run("IATEntriesTable", func(t *testing.T) {
		assert.Equal(t, "payment_iat_entries", info.IATEntriesTable())
	})

	t.Run("IATAddendaTable", func(t *testing.T) {
		assert.Equal(t, "payment_iat_addenda", info.IATAddendaTable())
	})

	t.Run("AllTableNames", func(t *testing.T) {
		expected := []string{
			"payment_file_header",
			"payment_batches",
			"payment_entries",
			"payment_addenda",
			"payment_iat_batches",
			"payment_iat_entries",
			"payment_iat_addenda",
		}
		assert.Equal(t, expected, info.AllTableNames())
	})
}

func TestGetACHTableInfos(t *testing.T) {
	testFile := findTestACHFile(t)
	if testFile == "" {
		t.Skip("No test ACH file found")
	}

	ctx := context.Background()

	// Clear registry before test
	ClearACHTableSetRegistry()

	// Open ACH file to register table set
	db, err := OpenContext(ctx, testFile)
	require.NoError(t, err)
	defer db.Close()

	// Get ACH table infos
	infos := GetACHTableInfos()
	require.NotEmpty(t, infos, "should have registered ACH table infos")

	// Verify we can use the table info
	for _, info := range infos {
		assert.NotEmpty(t, info.BaseName, "BaseName should not be empty")
		assert.Contains(t, info.EntriesTable(), "_entries")
		assert.Contains(t, info.BatchesTable(), "_batches")
	}
}

func TestGetACHTableInfos_Empty(t *testing.T) {
	// Clear registry
	ClearACHTableSetRegistry()

	// Should return empty slice, not nil
	infos := GetACHTableInfos()
	assert.NotNil(t, infos)
	assert.Empty(t, infos)
}

// TestDumpACH_FailedWriteLeavesDestinationIntact pins that a rejected ACH write
// does not damage the file it was going to overwrite. The moov-io writer
// validates while encoding, so a value the format cannot hold (an amount wider
// than its field) fails after the destination has been opened. Opening it with
// os.Create truncated the caller's own source file to zero bytes before the
// validation ran, so a rejected save destroyed the data it was saving.
func TestDumpACH_FailedWriteLeavesDestinationIntact(t *testing.T) {
	testFile := findTestACHFile(t)
	if testFile == "" {
		t.Skip("No test ACH file found")
	}

	ctx := context.Background()
	ClearACHTableSetRegistry()
	defer ClearACHTableSetRegistry()

	// Copy the fixture so the test writes back over its own file, which is what
	// an in-place save does.
	dir := t.TempDir()
	target := filepath.Join(dir, "ppd-debit.ach")
	original, err := os.ReadFile(testFile) //nolint:gosec // Test fixture path
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(target, original, 0o600)) //nolint:gosec // Test path is constructed from t.TempDir()

	db, err := OpenContext(ctx, target)
	require.NoError(t, err)
	defer db.Close()

	// An amount of twelve digits cannot fit the ten-digit ACH field, so the
	// writer rejects it partway through encoding.
	_, err = db.ExecContext(ctx, "UPDATE ppd_debit_entries SET amount = 999999999999")
	require.NoError(t, err)

	err = DumpACH(ctx, db, "ppd_debit", target)
	require.Error(t, err, "DumpACH should reject an amount wider than the ACH field")

	after, err := os.ReadFile(target) //nolint:gosec // Test fixture path
	require.NoError(t, err)
	assert.Equal(t, original, after, "a rejected write must leave the destination byte-for-byte unchanged")

	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	assert.Len(t, entries, 1, "a rejected write must not leave a temporary file behind: %v", entries)
}
