package filesql

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIsFedWireFile(t *testing.T) {
	t.Parallel()

	tests := []struct {
		path     string
		expected bool
	}{
		{"payment.fed", true},
		{"payment.FED", true},
		{"payment.Fed", true},
		{"data.csv", false},
		{"data.fed.gz", false},
		{".fed", false},
		{".FED", false},
	}

	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.expected, isFedWireFile(tt.path))
		})
	}
}

func TestOpenFedWireFile(t *testing.T) {
	testFile := findWireTestFile(t)
	if testFile == "" {
		t.Skip("No test Fedwire file found")
	}

	ctx := context.Background()
	ClearWireTableSetRegistry()
	defer ClearWireTableSetRegistry()

	db, err := OpenContext(ctx, testFile)
	require.NoError(t, err)
	defer db.Close()

	// Check that the message table was created
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
	assert.Contains(t, tables, "customer_transfer_message", "message table should exist")
}

func TestOpenFedWireFile_QueryFields(t *testing.T) {
	testFile := findWireTestFile(t)
	if testFile == "" {
		t.Skip("No test Fedwire file found")
	}

	ctx := context.Background()
	ClearWireTableSetRegistry()
	defer ClearWireTableSetRegistry()

	db, err := OpenContext(ctx, testFile)
	require.NoError(t, err)
	defer db.Close()

	// Query mandatory fields
	var senderDI, receiverDI, amount string
	err = db.QueryRowContext(ctx,
		"SELECT sender_di_routing_number, receiver_di_routing_number, amount FROM customer_transfer_message",
	).Scan(&senderDI, &receiverDI, &amount)
	require.NoError(t, err)

	assert.NotEmpty(t, senderDI, "sender DI should be populated")
	assert.NotEmpty(t, receiverDI, "receiver DI should be populated")
	assert.NotEmpty(t, amount, "amount should be populated")
	t.Logf("Sender: %s, Receiver: %s, Amount: %s", senderDI, receiverDI, amount)
}

func TestOpenFedWireFile_UpdateAndExport(t *testing.T) {
	testFile := findWireTestFile(t)
	if testFile == "" {
		t.Skip("No test Fedwire file found")
	}

	ctx := context.Background()
	ClearWireTableSetRegistry()
	defer ClearWireTableSetRegistry()

	db, err := OpenContext(ctx, testFile)
	require.NoError(t, err)
	defer db.Close()

	// Update the amount
	_, err = db.ExecContext(ctx, "UPDATE customer_transfer_message SET amount = '000005000000'")
	require.NoError(t, err)

	// Export to temp file
	tmpFile := filepath.Join(t.TempDir(), "output.fed")
	err = DumpFedWire(ctx, db, "customer_transfer", tmpFile)
	require.NoError(t, err)

	// Verify file was created and is non-empty
	info, err := os.Stat(tmpFile)
	require.NoError(t, err)
	assert.Greater(t, info.Size(), int64(0), "output file should not be empty")

	// Re-open the written file and verify the change
	ClearWireTableSetRegistry()
	db2, err := OpenContext(ctx, tmpFile)
	require.NoError(t, err)
	defer db2.Close()

	var amount string
	err = db2.QueryRowContext(ctx, "SELECT amount FROM output_message").Scan(&amount)
	require.NoError(t, err)
	assert.Equal(t, "000005000000", amount, "amount should be updated")
}

func TestBuilderAddPathFedWire(t *testing.T) {
	testFile := findWireTestFile(t)
	if testFile == "" {
		t.Skip("No test Fedwire file found")
	}

	ctx := context.Background()
	ClearWireTableSetRegistry()
	defer ClearWireTableSetRegistry()

	builder := NewBuilder().AddPath(testFile)
	builder, err := builder.Build(ctx)
	require.NoError(t, err)

	db, err := builder.Open(ctx)
	require.NoError(t, err)
	defer db.Close()

	// Verify message table exists
	var count int
	err = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM customer_transfer_message").Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 1, count, "should have exactly 1 message row")
}

func TestDumpFedWire_NoTableSet(t *testing.T) {
	ctx := context.Background()
	ClearWireTableSetRegistry()

	db, err := OpenContext(ctx, filepath.Join("testdata", "test.csv"))
	require.NoError(t, err)
	defer db.Close()

	tmpFile := filepath.Join(t.TempDir(), "output.fed")
	err = DumpFedWire(ctx, db, "nonexistent", tmpFile)
	assert.Error(t, err, "should fail when no TableSet is registered")
}

func TestDumpFedWireWithTableSet_NilTableSet(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	db, err := OpenContext(ctx, filepath.Join("testdata", "test.csv"))
	require.NoError(t, err)
	defer db.Close()

	tmpFile := filepath.Join(t.TempDir(), "output.fed")
	err = DumpFedWireWithTableSet(ctx, db, "test", tmpFile, nil)
	assert.Error(t, err, "should fail with nil TableSet")
}

func TestWireTableInfo_TableNames(t *testing.T) {
	t.Parallel()

	info := WireTableInfo{BaseName: "payment"}

	t.Run("MessageTable", func(t *testing.T) {
		t.Parallel()
		assert.Equal(t, "payment_message", info.MessageTable())
	})

	t.Run("AllTableNames", func(t *testing.T) {
		t.Parallel()
		expected := []string{"payment_message"}
		assert.Equal(t, expected, info.AllTableNames())
	})
}

func TestIsWireBaseTableName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		tableName    string
		expectedBase string
		expectedWire bool
	}{
		{"payment_message", "payment", true},
		{"my_file_message", "my_file", true},
		{"payment_entries", "", false},
		{"payment_batches", "", false},
		{"_message", "", false},
		{"message", "", false},
		{"", "", false},
	}

	for _, tt := range tests {
		t.Run(tt.tableName, func(t *testing.T) {
			t.Parallel()
			baseName, isWire := IsWireBaseTableName(tt.tableName)
			assert.Equal(t, tt.expectedBase, baseName)
			assert.Equal(t, tt.expectedWire, isWire)
		})
	}
}

func TestGetWireTableInfos(t *testing.T) {
	testFile := findWireTestFile(t)
	if testFile == "" {
		t.Skip("No test Fedwire file found")
	}

	ctx := context.Background()
	ClearWireTableSetRegistry()
	defer ClearWireTableSetRegistry()

	db, err := OpenContext(ctx, testFile)
	require.NoError(t, err)
	defer db.Close()

	infos := GetWireTableInfos()
	require.NotEmpty(t, infos, "should have registered Fedwire table infos")

	for _, info := range infos {
		assert.NotEmpty(t, info.BaseName)
		assert.Contains(t, info.MessageTable(), "_message")
	}
}

func TestGetWireTableInfos_Empty(t *testing.T) {
	ClearWireTableSetRegistry()

	infos := GetWireTableInfos()
	assert.NotNil(t, infos)
	assert.Empty(t, infos)
}

func TestWireRegistryConcurrency(t *testing.T) {
	t.Parallel()

	ClearWireTableSetRegistry()
	defer ClearWireTableSetRegistry()

	// Register and unregister should not race
	done := make(chan struct{})
	go func() {
		for range 100 {
			registerWireTableSet("test", nil)
			getWireTableSet("test")
			getWireBaseTableNames()
		}
		close(done)
	}()

	for range 100 {
		getWireTableSet("test")
		GetWireTableInfos()
	}
	<-done

	UnregisterWireTableSet("test")
}

func TestAddFS_FedWireFile(t *testing.T) {
	testFile := findWireTestFile(t)
	if testFile == "" {
		t.Skip("No test Fedwire file found")
	}

	ctx := context.Background()
	ClearWireTableSetRegistry()
	defer ClearWireTableSetRegistry()

	// Copy .fed file to an isolated temp directory to avoid picking up other testdata files
	tmpDir := t.TempDir()
	data, err := os.ReadFile(testFile) //nolint:gosec // Test file path
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(tmpDir, "customer-transfer.fed"), data, 0o600))

	fsys := os.DirFS(tmpDir)
	builder := NewBuilder().AddFS(fsys)
	builder, err = builder.Build(ctx)
	require.NoError(t, err, "Build should succeed with .fed file in AddFS")

	db, err := builder.Open(ctx)
	require.NoError(t, err)
	defer db.Close()

	// Verify the message table was created and has data
	var count int
	err = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM customer_transfer_message").Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 1, count, "should have exactly 1 message row")
}

func TestDumpFedWire_DroppedTable(t *testing.T) {
	testFile := findWireTestFile(t)
	if testFile == "" {
		t.Skip("No test Fedwire file found")
	}

	ctx := context.Background()
	ClearWireTableSetRegistry()
	defer ClearWireTableSetRegistry()

	db, err := OpenContext(ctx, testFile)
	require.NoError(t, err)
	defer db.Close()

	// Drop the message table
	_, err = db.ExecContext(ctx, "DROP TABLE customer_transfer_message")
	require.NoError(t, err)

	// DumpFedWire should fail because the table is gone
	tmpFile := filepath.Join(t.TempDir(), "output.fed")
	err = DumpFedWire(ctx, db, "customer_transfer", tmpFile)
	assert.Error(t, err, "DumpFedWire should fail when message table is dropped")
}

// findWireTestFile looks for a test Fedwire file in testdata directory.
func findWireTestFile(t *testing.T) string {
	t.Helper()

	path := filepath.Join("testdata", "customer-transfer.fed")
	if _, err := os.Stat(path); err == nil {
		return path
	}

	return ""
}
