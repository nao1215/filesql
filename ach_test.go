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
	db, err := OpenContext(ctx, "testdata/test.csv")
	require.NoError(t, err)
	defer db.Close()

	tmpFile := filepath.Join(t.TempDir(), "output.ach")
	err = DumpACH(ctx, db, "nonexistent", tmpFile)
	assert.Error(t, err, "should fail with invalid table set")
}

func TestDumpACH_NilTableSet(t *testing.T) {
	ctx := context.Background()

	// Open a CSV file (not ACH)
	db, err := OpenContext(ctx, "testdata/test.csv")
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

// findACHTestFile looks for a specific test ACH file.
func findACHTestFile(t *testing.T, filename string) string {
	t.Helper()

	locations := []string{
		filepath.Join("testdata", filename),
		filepath.Join("../fileparser/ach/testdata", filename),
		filepath.Join(os.Getenv("HOME"), "go/pkg/mod/github.com/moov-io/ach@v1.53.4/test/testdata", filename),
	}

	for _, loc := range locations {
		if _, err := os.Stat(loc); err == nil {
			return loc
		}
	}

	return ""
}
