package filesql

import (
	"context"
	"database/sql/driver"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/nao1215/filesql/dialect"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestExcelSheetsInFile_UnreadableWorkbook covers the two ways the sheet listing
// can fail before it has a workbook to read: no file at that path, and a file
// that is not a workbook.
func TestExcelSheetsInFile_UnreadableWorkbook(t *testing.T) {
	t.Parallel()

	t.Run("a file that is not there", func(t *testing.T) {
		t.Parallel()

		_, err := ExcelSheetsInFile(filepath.Join(t.TempDir(), "missing.xlsx"))
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrIOOperation)
	})

	t.Run("a file that is not a workbook", func(t *testing.T) {
		t.Parallel()

		path := filepath.Join(t.TempDir(), "fake.xlsx")
		require.NoError(t, os.WriteFile(path, []byte("id,name\n1,Alice\n"), 0o600))

		_, err := ExcelSheetsInFile(path)
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrParsing)
	})
}

// TestExcelSheetsInReader_NotAWorkbook is the same refusal for a workbook that
// has no path.
func TestExcelSheetsInReader_NotAWorkbook(t *testing.T) {
	t.Parallel()

	_, err := ExcelSheetsInReader(strings.NewReader("id,name\n1,Alice\n"))
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrParsing)
}

// TestDialectConnector_UnusableDSN covers the connector that opens the
// translating connections. A DSN the driver refuses has to be reported when the
// connection is made rather than at the first query.
func TestDialectConnector_UnusableDSN(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	connector := &dialectConnector{
		drv:        db.Driver(),
		dsn:        "file:/nonexistent-directory/db.sqlite?mode=rw",
		sqlDialect: dialect.PostgreSQL,
	}

	_, err := connector.Connect(context.Background())
	assert.Error(t, err)
}

// TestDialectConnection_LegacyDriverFallbacks covers a wrapped connection that
// implements neither of the context-aware interfaces. Preparing and beginning
// still have to work, through the pre-context methods.
func TestDialectConnection_LegacyDriverFallbacks(t *testing.T) {
	t.Parallel()

	conn := &dialectConnection{conn: &plainConn{}, sqlDialect: dialect.PostgreSQL}

	t.Run("prepare", func(t *testing.T) {
		t.Parallel()

		_, err := conn.PrepareContext(context.Background(), "SELECT 1")
		assert.ErrorIs(t, err, errStub, "the legacy Prepare is what answers")
	})

	t.Run("begin", func(t *testing.T) {
		t.Parallel()

		tx, err := conn.BeginTx(context.Background(), driver.TxOptions{})
		require.NoError(t, err)
		assert.NotNil(t, tx)
	})
}
