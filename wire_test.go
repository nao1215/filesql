package filesql

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"testing/iotest"
	"time"

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
	db2, err := OpenContext(ctx, tmpFile)
	require.NoError(t, err)
	defer db2.Close()

	var amount string
	err = db2.QueryRowContext(ctx, "SELECT amount FROM output_message").Scan(&amount)
	require.NoError(t, err)
	assert.Equal(t, "000005000000", amount, "amount should be updated")
}

// TestOpenFedWireFile_RefusesOverWidthText is the Fedwire half of the rule the
// ACH tests pin: a fixed-width record cannot hold an over-long value, and
// cutting it to fit writes data the session never asked for.
func TestOpenFedWireFile_RefusesOverWidthText(t *testing.T) {
	testFile := findWireTestFile(t)
	if testFile == "" {
		t.Skip("No test Fedwire file found")
	}

	ctx := context.Background()

	db, err := OpenContext(ctx, testFile)
	require.NoError(t, err)
	defer db.Close()

	// The originator name field holds 35 characters.
	_, err = db.ExecContext(ctx,
		"UPDATE customer_transfer_message SET originator_name = 'A very long originator name that surely exceeds the field'")
	require.NoError(t, err)

	out := filepath.Join(t.TempDir(), "output.fed")
	err = DumpFedWire(ctx, db, "customer_transfer", out)
	require.Error(t, err, "a value the record cannot hold must fail the write, not be cut to fit")
	assert.Contains(t, err.Error(), "Name")

	_, statErr := os.Stat(out)
	assert.True(t, os.IsNotExist(statErr), "a refused write must leave no file behind")
}

func TestBuilderAddPathFedWire(t *testing.T) {
	testFile := findWireTestFile(t)
	if testFile == "" {
		t.Skip("No test Fedwire file found")
	}

	ctx := context.Background()

	builder := NewBuilder().AddPath(testFile)
	builder, err := buildForTest(ctx, builder)
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

	db, err := OpenContext(ctx, filepath.Join("testdata", "test.csv"))
	require.NoError(t, err)
	defer db.Close()

	tmpFile := filepath.Join(t.TempDir(), "output.fed")
	err = DumpFedWire(ctx, db, "nonexistent", tmpFile)
	assert.Error(t, err, "should fail when no TableSet is registered")
}

func TestDumpFedWireWithSource_NilSource(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	db, err := OpenContext(ctx, filepath.Join("testdata", "test.csv"))
	require.NoError(t, err)
	defer db.Close()

	tmpFile := filepath.Join(t.TempDir(), "output.fed")
	err = DumpFedWireWithSource(ctx, db, "test", tmpFile, nil)
	assert.ErrorIs(t, err, ErrNilInput, "a nil source is refused by name")
}

func TestIsWireBaseTableNameSuffix(t *testing.T) {
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
			baseName, isWire := isWireBaseTableName(tt.tableName)
			assert.Equal(t, tt.expectedBase, baseName)
			assert.Equal(t, tt.expectedWire, isWire)
		})
	}
}

// TestWireSourceRecordedPerDatabase pins that loading a Fedwire file records
// its source in the database it was loaded into, which is what makes the dump
// find the right file.
func TestWireSourceRecordedPerDatabase(t *testing.T) {
	testFile := findWireTestFile(t)
	if testFile == "" {
		t.Skip("No test Fedwire file found")
	}

	ctx := context.Background()

	db, err := OpenContext(ctx, testFile)
	require.NoError(t, err)
	defer db.Close()

	assert.Equal(t, []string{"customer_transfer"}, fileSourceBaseNames(ctx, db, sourceFormatFedWire))

	path, ok := fileSourcePath(ctx, db, "customer_transfer", sourceFormatFedWire)
	require.True(t, ok)
	assert.True(t, filepath.IsAbs(path), "the recorded path must survive a change of working directory: %s", path)
}

// TestWireSourceAbsentForOtherDatabases pins that a database that loaded no
// Fedwire file reports none, so nothing is dumped as Fedwire by accident.
func TestWireSourceAbsentForOtherDatabases(t *testing.T) {
	ctx := context.Background()

	db, err := OpenContext(ctx, filepath.Join("testdata", "test.csv"))
	require.NoError(t, err)
	defer db.Close()

	assert.Empty(t, fileSourceBaseNames(ctx, db, sourceFormatFedWire))
}

func TestAddFS_FedWireFile(t *testing.T) {
	testFile := findWireTestFile(t)
	if testFile == "" {
		t.Skip("No test Fedwire file found")
	}

	ctx := context.Background()

	// Copy .fed file to an isolated temp directory to avoid picking up other testdata files
	tmpDir := t.TempDir()
	data, err := os.ReadFile(testFile) //nolint:gosec // Test file path
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(tmpDir, "customer-transfer.fed"), data, 0o600)) //nolint:gosec // Test file path is constructed from t.TempDir()

	fsys := os.DirFS(tmpDir)
	builder := NewBuilder().AddFS(fsys)
	builder, err = buildForTest(ctx, builder)
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

// TestDumpFedWire_TwoDatabasesShareABaseName pins that two databases loaded
// from different Fedwire files that happen to share a base name each dump their
// own structure. The structure used to be looked up in a process-global map
// keyed by the base name alone, so the second load replaced the first and dbA's
// message was written into dbB's tag layout.
func TestDumpFedWire_TwoDatabasesShareABaseName(t *testing.T) {
	ctx := context.Background()

	// One fixture carries the optional remittance tags, the other only the
	// mandatory ones, so writing one database through the other's structure
	// shows up as tags gained or lost.
	pathA := copyWireFixture(t, "customer-transfer.fed")
	pathB := copyWireFixture(t, "customer-transfer-minimal.fed")

	dbA, err := OpenContext(ctx, pathA)
	require.NoError(t, err)
	defer dbA.Close()

	dbB, err := OpenContext(ctx, pathB)
	require.NoError(t, err)
	defer dbB.Close()

	outA := filepath.Join(t.TempDir(), "a.fed")
	require.NoError(t, DumpFedWire(ctx, dbA, "payment", outA))

	outB := filepath.Join(t.TempDir(), "b.fed")
	require.NoError(t, DumpFedWire(ctx, dbB, "payment", outB))

	assert.Contains(t, readFileString(t, outA), "{6500}", "dbA must dump the file it was loaded from")
	assert.NotContains(t, readFileString(t, outB), "{6500}", "dbB must dump the file it was loaded from")
}

// TestDumpFedWire_MissingSourceFileIsNamed pins that a dump whose source file is
// gone fails with an error naming that file.
func TestDumpFedWire_MissingSourceFileIsNamed(t *testing.T) {
	ctx := context.Background()

	source := copyWireFixture(t, "customer-transfer.fed")
	db, err := OpenContext(ctx, source)
	require.NoError(t, err)
	defer db.Close()

	require.NoError(t, os.Remove(source))

	err = DumpFedWire(ctx, db, "payment", filepath.Join(t.TempDir(), "out.fed"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), source, "the error must name the source file it could not read")
}

// copyWireFixture copies a Fedwire fixture into a fresh directory under the
// fixed name payment.fed, so several fixtures can share one base table name.
func copyWireFixture(t *testing.T, fixture string) string {
	t.Helper()

	return copyFixtureAs(t, fixture, "payment.fed")
}

func readFileString(t *testing.T, path string) string {
	t.Helper()

	content, err := os.ReadFile(path) //nolint:gosec // Test path is constructed from t.TempDir()
	require.NoError(t, err)
	return string(content)
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

// TestDumpFedWire_FailedWriteLeavesDestinationIntact pins that a rejected
// Fedwire write does not damage the file it was going to overwrite. The writer
// validates while encoding, and the failure path used to remove the output path
// as a "partial file" — which, for an in-place save, is the caller's source.
func TestDumpFedWire_FailedWriteLeavesDestinationIntact(t *testing.T) {
	testFile := findWireTestFile(t)
	if testFile == "" {
		t.Skip("No test Fedwire file found")
	}

	ctx := context.Background()

	dir := t.TempDir()
	target := filepath.Join(dir, "customer-transfer.fed")
	original, err := os.ReadFile(testFile) //nolint:gosec // Test fixture path
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(target, original, 0o600)) //nolint:gosec // Test path is constructed from t.TempDir()

	db, err := OpenContext(ctx, target)
	require.NoError(t, err)
	defer db.Close()

	_, err = db.ExecContext(ctx, "UPDATE customer_transfer_message SET amount = 'BADAMOUNT'")
	require.NoError(t, err)

	err = DumpFedWire(ctx, db, "customer_transfer", target)
	require.Error(t, err, "DumpFedWire should reject a malformed amount")

	after, err := os.ReadFile(target) //nolint:gosec // Test fixture path
	require.NoError(t, err)
	assert.Equal(t, original, after, "a rejected write must leave the destination byte-for-byte unchanged")

	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	assert.Len(t, entries, 1, "a rejected write must not leave a temporary file behind: %v", entries)
}

// TestDumpFedWire_OnCallerManagedDatabase is the Fedwire half of the guard in
// ach_test.go: the export runs its own queries rather than going through
// DumpDatabase, so a connection held anywhere in its path stops it on the pool
// this package asks a caller to pin to one connection, with no error to see.
func TestDumpFedWire_OnCallerManagedDatabase(t *testing.T) {
	testFile := findWireTestFile(t)
	if testFile == "" {
		t.Skip("No test Fedwire file found")
	}

	db := newCallerDB(t)
	ctx := context.Background()
	require.NoError(t, LoadInto(ctx, db, testFile))

	out := filepath.Join(t.TempDir(), "written.fed")
	done := make(chan error, 1)
	go func() { done <- DumpFedWire(ctx, db, "customer_transfer", out) }()

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(30 * time.Second):
		t.Fatal("DumpFedWire did not return: the export is waiting for a connection it is holding itself")
	}

	assert.FileExists(t, out)
}

// TestDumpFedWire_WriteBackKeepsEveryValue is the property a caller can rely on
// when the bytes change. A write-back is a rewrite rather than a patch, so the
// tags come back in the order the format defines rather than the order the file
// had them; what must survive is the data, so the file is written back with no
// edit at all and reloaded, and every column has to match what the first load
// held.
func TestDumpFedWire_WriteBackKeepsEveryValue(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	source := copyWireFixture(t, "customer-transfer.fed")

	db, err := OpenContext(ctx, source)
	require.NoError(t, err)
	before := achTableDump(ctx, t, db, "payment")
	require.NoError(t, DumpFedWire(ctx, db, "payment", source))
	require.NoError(t, db.Close())

	reloaded, err := OpenContext(ctx, source)
	require.NoError(t, err)
	defer reloaded.Close()
	after := achTableDump(ctx, t, reloaded, "payment")

	assert.Equal(t, before, after, "a write-back with no edit must keep every value")
}

// TestRecordingReaderKeepsTheFirstRealError pins what the recorder is for: the
// library that reads a Fedwire file answers with what the message it built is
// missing rather than with why the read stopped, so the reason has to be kept
// on the way past. The end of a stream is not a reason and is not kept.
func TestRecordingReaderKeepsTheFirstRealError(t *testing.T) {
	t.Parallel()

	t.Run("a read error is kept", func(t *testing.T) {
		t.Parallel()

		want := errors.New("the read stopped here")
		r := &recordingReader{src: iotest.ErrReader(want)}
		_, err := io.ReadAll(r)
		require.ErrorIs(t, err, want)
		assert.ErrorIs(t, r.err, want)
	})

	t.Run("the end of a stream is not", func(t *testing.T) {
		t.Parallel()

		r := &recordingReader{src: strings.NewReader("abc")}
		got, err := io.ReadAll(r)
		require.NoError(t, err)
		assert.Equal(t, "abc", string(got))
		assert.NoError(t, r.err)
	})

	t.Run("the first error is the one kept", func(t *testing.T) {
		t.Parallel()

		first := errors.New("first")
		r := &recordingReader{src: iotest.ErrReader(first)}
		_, err := r.Read(make([]byte, 1))
		require.ErrorIs(t, err, first)
		r.src = iotest.ErrReader(errors.New("second"))
		_, err = r.Read(make([]byte, 1))
		require.Error(t, err)
		assert.ErrorIs(t, r.err, first)
	})
}
