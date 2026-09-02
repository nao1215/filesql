package filesql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"testing/fstest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nao1215/filesql/internal/reader"
)

// plainExecutor is a dbtx that is neither *sql.DB nor *sql.Tx. A load needs one
// of those two to run its input under a transaction or a savepoint, so a
// caller's own implementation has to be refused by name rather than crashing on
// a type assertion.
type plainExecutor struct {
	db *sql.DB
}

func (e plainExecutor) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return e.db.ExecContext(ctx, query, args...)
}

func (e plainExecutor) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	return e.db.QueryContext(ctx, query, args...)
}

func (e plainExecutor) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	return e.db.QueryRowContext(ctx, query, args...)
}

func (e plainExecutor) PrepareContext(ctx context.Context, query string) (*sql.Stmt, error) {
	return e.db.PrepareContext(ctx, query)
}

// openTestTx returns a transaction on an empty test database, which is the
// scope runInputScope hands every load. A test that calls one of the loading
// functions directly opens its own, so what it exercises is what a load
// exercises.
func openTestTx(t *testing.T) *sql.Tx {
	t.Helper()

	tx, err := openTestDB(t).BeginTx(context.Background(), nil)
	require.NoError(t, err)
	t.Cleanup(func() {
		// A test that ended its own transaction leaves nothing to roll back.
		if err := tx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
			t.Errorf("could not roll back the test transaction: %v", err)
		}
	})
	return tx
}

// failingCloser reports a failure when the loader closes a reader it opened.
type failingCloser struct{ closed bool }

func (c *failingCloser) Close() error {
	c.closed = true
	return errStub
}

// TestStreamFileToDatabase_UnsupportedFormat covers the refusal of a file whose
// extension names no format this package reads.
func TestStreamFileToDatabase_UnsupportedFormat(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "notes.docx")
	require.NoError(t, os.WriteFile(path, []byte("content"), 0o600))

	err := newStreamProcessor(100).streamFileToDatabase(context.Background(), openTestTx(t), path)
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrUnsupportedFormat)
}

// TestStreamWriteBackFormatFiles_Failures covers the two formats that are read
// from a path rather than through the chunk loader. Both are opened and measured
// before parsing, so a missing or empty file is reported as such instead of as a
// parse failure with nothing in it.
func TestStreamWriteBackFormatFiles_Failures(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	tests := []struct {
		name string
		ext  string
	}{
		{"ACH input", extACH},
		{"Fedwire", extFED},
	}
	for _, tt := range tests {
		t.Run(tt.name+" file that is not there", func(t *testing.T) {
			t.Parallel()

			path := filepath.Join(t.TempDir(), "missing"+tt.ext)
			err := newStreamProcessor(100).streamFileToDatabase(ctx, openTestTx(t), path)
			require.Error(t, err)
			assert.ErrorIs(t, err, ErrIOOperation)
		})

		t.Run(tt.name+" file with no bytes in it", func(t *testing.T) {
			t.Parallel()

			path := filepath.Join(t.TempDir(), "empty"+tt.ext)
			require.NoError(t, os.WriteFile(path, nil, 0o600))

			err := newStreamProcessor(100).streamFileToDatabase(ctx, openTestTx(t), path)
			require.Error(t, err)
			assert.ErrorIs(t, err, ErrEmptyData)
		})
	}
}

// TestRunInputScope_UnsupportedExecutor covers a dbtx an input cannot be scoped
// on. It is refused with the type in the message, because a caller who passed
// their own wrapper has no other way to tell what was wrong.
func TestRunInputScope_UnsupportedExecutor(t *testing.T) {
	t.Parallel()

	loaded := false
	err := newStreamProcessor(100).runInputScope(context.Background(), plainExecutor{db: openTestDB(t)}, rereadableInput, func(*sql.Tx) error {
		loaded = true
		return nil
	})
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrDatabaseOperation)
	assert.Contains(t, err.Error(), "unsupported database executor")
	assert.False(t, loaded, "an input with nowhere to be undone must not be loaded at all")
}

// TestStreamReaderToDatabase_UnusableDatabase covers the check for a table of
// the same name, which is the first thing a load asks the database.
func TestStreamReaderToDatabase_UnusableDatabase(t *testing.T) {
	t.Parallel()

	tx := openTestTx(t)
	require.NoError(t, tx.Rollback())

	err := newStreamProcessor(100).streamReaderToDatabase(context.Background(), tx, readerInput{
		reader:    strings.NewReader("id,name\n1,Alice\n"),
		tableName: "users",
		fileType:  FileTypeCSV,
	})
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrDatabaseOperation)
}

// TestStreamReaderToDatabase_ReservedTableName pins that the reserved namespace
// is refused for readers too, not only for paths.
func TestStreamReaderToDatabase_ReservedTableName(t *testing.T) {
	t.Parallel()

	err := newStreamProcessor(100).streamReaderToDatabase(context.Background(), openTestTx(t), readerInput{
		reader:    strings.NewReader("id\n1\n"),
		tableName: sourceTablePrefix + "report",
		fileType:  FileTypeCSV,
	})
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrReservedTableName)
}

// TestCloseReaderInput_ReportsNothingToTheCaller checks that a reader this
// package opened itself is closed, and that a failure to close it does not fail
// the load: the rows are already in the database by then.
func TestCloseReaderInput_ReportsNothingToTheCaller(t *testing.T) {
	t.Parallel()

	closer := &failingCloser{}
	newStreamProcessor(100).closeReaderInput(readerInput{tableName: "users", closer: closer})
	assert.True(t, closer.closed, "a reader opened by this package must be closed")
}

// TestDropIfReplacing covers the drop that lets a reload install its own schema.
func TestDropIfReplacing(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	t.Run("does nothing in open mode", func(t *testing.T) {
		t.Parallel()

		db := openTestDB(t)
		require.NoError(t, db.Close())

		// A closed database would fail any statement, so a successful call proves
		// none was sent.
		assert.NoError(t, newStreamProcessor(100).dropIfReplacing(ctx, db, "users"))
	})

	t.Run("reports a drop the database refused", func(t *testing.T) {
		t.Parallel()

		db := openTestDB(t)
		require.NoError(t, db.Close())

		sp := newStreamProcessor(100)
		sp.replaceExisting = true

		err := sp.dropIfReplacing(ctx, db, "users")
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrDatabaseOperation)
	})
}

// TestLoadTyped_ReadAgain covers the path a file takes when a later chunk
// widens a column: the first attempt is dropped and the file is read again
// under the types the whole of it calls for.
func TestLoadTyped_ReadAgain(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	const body = "v\n1\n2.50\nabc\n"

	newSource := func(reread func(emit chunkProcessor) (columnInfoList, error)) tableSource {
		return tableSource{
			read: func(emit chunkProcessor) (columnInfoList, error) {
				return newStreamingParser(FileTypeCSV, CompressionNone, "t", 1).ProcessInChunks(strings.NewReader(body), emit)
			},
			reread: reread,
		}
	}

	t.Run("a second read that does not match the first is refused", func(t *testing.T) {
		t.Parallel()

		tx := openTestTx(t)
		changed := func(emit chunkProcessor) (columnInfoList, error) {
			return newStreamingParser(FileTypeCSV, CompressionNone, "t", 1).ProcessInChunks(strings.NewReader("v\n1\n2\n"), emit)
		}
		// Through the scope, because undoing a refused load is what the scope is
		// for: loadTable itself only reports.
		sp := newStreamProcessor(1)
		err := sp.runInputScope(ctx, tx, rereadableInput, func(scope *sql.Tx) error {
			return sp.loadTable(ctx, scope, "t", newSource(changed))
		})
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrParsing)
		assert.Contains(t, err.Error(), "changed while it was being read")

		var tables int
		require.NoError(t, tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM sqlite_master WHERE type='table'`).Scan(&tables))
		assert.Equal(t, 0, tables, "a refused load leaves no table behind")
	})

	t.Run("a source that cannot be opened again reports why", func(t *testing.T) {
		t.Parallel()

		failing := func(chunkProcessor) (columnInfoList, error) { return nil, errStub }
		err := newStreamProcessor(1).loadTable(ctx, openTestTx(t), "t", newSource(failing))
		require.ErrorIs(t, err, errStub)
	})

	t.Run("a second read stores the file's text at every row", func(t *testing.T) {
		t.Parallel()

		tx := openTestTx(t)
		again := func(emit chunkProcessor) (columnInfoList, error) {
			return newStreamingParser(FileTypeCSV, CompressionNone, "t", 1).ProcessInChunks(strings.NewReader(body), emit)
		}
		require.NoError(t, newStreamProcessor(1).loadTable(ctx, tx, "t", newSource(again)))

		rows, err := tx.QueryContext(ctx, `SELECT v FROM t ORDER BY rowid`)
		require.NoError(t, err)
		defer rows.Close()
		var got []string
		for rows.Next() {
			var v string
			require.NoError(t, rows.Scan(&v))
			got = append(got, v)
		}
		require.NoError(t, rows.Err())
		assert.Equal(t, []string{"1", "2.50", "abc"}, got)
	})
}

// TestRunInputScope_CallerTransaction covers a failed load inside a transaction
// the caller owns: rolling back to the savepoint takes the staging table and
// every other trace of the input with it, leaves what the caller had, and
// leaves the transaction itself for the caller to end.
func TestRunInputScope_CallerTransaction(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	tx := openTestTx(t)
	_, err := tx.ExecContext(ctx, `CREATE TABLE keep (v)`)
	require.NoError(t, err)

	source := tableSource{read: func(emit chunkProcessor) (columnInfoList, error) {
		return newStreamingParser(FileTypeCSV, CompressionNone, "t", 1).ProcessInChunks(strings.NewReader("v\n1\n2\nx,y\n"), emit)
	}}
	sp := newStreamProcessor(1)
	err = sp.runInputScope(ctx, tx, rereadableInput, func(scope *sql.Tx) error {
		return sp.loadTable(ctx, scope, "t", source)
	})
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrColumnMismatch)

	var left []string
	rows, err := tx.QueryContext(ctx, `SELECT name FROM sqlite_master WHERE type='table' ORDER BY name`)
	require.NoError(t, err, "the caller's transaction is still usable")
	for rows.Next() {
		var name string
		require.NoError(t, rows.Scan(&name))
		left = append(left, name)
	}
	require.NoError(t, rows.Err())
	require.NoError(t, rows.Close())
	assert.Equal(t, []string{"keep"}, left, "the failed input left nothing in the caller's transaction")
}

// TestUndoInput_TransactionAlreadyEnded covers the undo of an input whose
// transaction is already gone, which is what a load canceled through the
// context the caller's transaction was built on finds: database/sql has rolled
// the whole transaction back, taking the savepoint with it, and that is the undo
// having happened rather than a failure to report.
func TestUndoInput_TransactionAlreadyEnded(t *testing.T) {
	t.Parallel()

	tx := openTestTx(t)
	require.NoError(t, tx.Rollback())

	assert.NoError(t, undoInput(context.Background(), tx))
}

// TestLoadStaged_TypesTheTableOnce covers the two ways a staged table is
// declared: renamed when every column is TEXT, copied when one is not.
func TestLoadStaged_TypesTheTableOnce(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	tests := []struct {
		name     string
		body     string
		wantType string
		wantRows []string
	}{
		{"all text is renamed in place", "a,b\nx,y\n", "TEXT", []string{"x"}},
		{"a numeric column is copied into its type", "a,b\n1,y\n2,z\n", "INTEGER", []string{"1", "2"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tx := openTestTx(t)
			source := tableSource{read: func(emit chunkProcessor) (columnInfoList, error) {
				return newStreamingParser(FileTypeCSV, CompressionNone, "t", 1).ProcessInChunks(strings.NewReader(tt.body), emit)
			}}
			require.NoError(t, newStreamProcessor(1).loadTable(ctx, tx, "t", source))

			var declared string
			require.NoError(t, tx.QueryRowContext(ctx, `SELECT type FROM pragma_table_info('t') WHERE name = 'a'`).Scan(&declared))
			assert.Equal(t, tt.wantType, declared)

			var names []string
			rows, err := tx.QueryContext(ctx, `SELECT name FROM sqlite_master WHERE type='table'`)
			require.NoError(t, err)
			for rows.Next() {
				var name string
				require.NoError(t, rows.Scan(&name))
				names = append(names, name)
			}
			require.NoError(t, rows.Err())
			require.NoError(t, rows.Close())
			assert.Equal(t, []string{"t"}, names, "the staging table is gone")

			rows, err = tx.QueryContext(ctx, `SELECT a FROM t ORDER BY rowid`)
			require.NoError(t, err)
			defer rows.Close()
			var got []string
			for rows.Next() {
				var a string
				require.NoError(t, rows.Scan(&a))
				got = append(got, a)
			}
			require.NoError(t, rows.Err())
			assert.Equal(t, tt.wantRows, got)
		})
	}
}

// TestLoadStaged_NamesTheCallersTable holds that a failure on the staged path
// says which table the caller asked for.
//
// The rows of an input that cannot be read twice wait in a table under this
// package's reserved prefix, and a failure there was reported under that name:
// a table the caller never wrote, hidden from every listing and dropped before
// the transaction ends, so the message named nothing they could look for. The
// same input from a path reported its own table, so what an operator read
// depended on how the bytes arrived. What the database says underneath is still
// the database's own: SQLite names the table the statement ran against, which on
// this path is where the rows were staged.
func TestLoadStaged_NamesTheCallersTable(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	// A header of more columns than SQLite holds fails the create, which is the
	// first statement either path runs. The columns are handed straight to the
	// loader rather than read out of a file, because a file that wide is refused
	// by the reader before a statement is written -- and the subject here is
	// what the loader says when a statement fails, not which layer refuses.
	columns := make(columnInfoList, reader.MaxColumns+1)
	for i := range columns {
		columns[i] = columnInfo{Name: fmt.Sprintf("c%d", i), Type: columnTypeText}
	}

	names := make(header, len(columns))
	values := make(record, len(columns))
	for i, column := range columns {
		names[i] = column.Name
		values[i] = "1"
	}
	read := func(emit chunkProcessor) (columnInfoList, error) {
		return columns, emit(&tableChunk{
			tableName: "orders",
			headers:   names,
			records:   []record{values},
			types:     columns,
		})
	}

	tests := []struct {
		name   string
		source tableSource
	}{
		{name: "staged, for an input that cannot be read twice", source: tableSource{read: read}},
		{name: "typed, for one that can", source: tableSource{read: read, reread: read}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := newStreamProcessor(1).loadTable(ctx, openTestTx(t), "orders", tt.source)
			require.Error(t, err)
			assert.Contains(t, err.Error(), `failed to create table "orders"`,
				"the wording this package adds names the table the caller asked for")
		})
	}
}

// TestLoadTable_SourceWithoutChunks covers a source that returns without
// emitting a chunk, which every reader in this package is written not to do.
func TestLoadTable_SourceWithoutChunks(t *testing.T) {
	t.Parallel()

	silent := func(chunkProcessor) (columnInfoList, error) { return nil, nil }
	for name, source := range map[string]tableSource{
		"once":  {read: silent},
		"twice": {read: silent, reread: silent},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			err := newStreamProcessor(1).loadTable(context.Background(), openTestTx(t), "t", source)
			require.ErrorIs(t, err, ErrEmptyData)
		})
	}
}

// TestAddFS_ReadsAFileAgainWhenAColumnWidens covers the reopen an fs.FS input
// carries: a file whose column widens late is read twice and stored as written.
func TestAddFS_ReadsAFileAgainWhenAColumnWidens(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	mockFS := fstest.MapFS{"t.csv": &fstest.MapFile{Data: []byte("v\n1\n2.50\nabc\n")}}
	built, err := buildForTest(ctx, NewBuilder().AddFS(mockFS).SetDefaultChunkSize(1))
	require.NoError(t, err)
	db, err := built.Open(ctx)
	require.NoError(t, err)
	defer db.Close()

	rows, err := db.QueryContext(ctx, `SELECT v FROM t ORDER BY rowid`)
	require.NoError(t, err)
	defer rows.Close()
	var got []string
	for rows.Next() {
		var v string
		require.NoError(t, rows.Scan(&v))
		got = append(got, v)
	}
	require.NoError(t, rows.Err())
	assert.Equal(t, []string{"1", "2.50", "abc"}, got)
}

// TestParseFromReader_EmptyInput covers what each format's parser answers for an
// input with nothing in it. JSON and JSONL are excluded on purpose: an empty one
// is a valid zero-row table, which the loader turns into an empty table rather
// than a failure. XLSX is excluded because no bytes at all is not an empty
// workbook but an unreadable one, which the case below covers.
func TestParseFromReader_EmptyInput(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		fileType FileType
	}{
		{"CSV", FileTypeCSV},
		{"TSV", FileTypeTSV},
		{"Parquet", FileTypeParquet},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			parser := newStreamingParser(tt.fileType, CompressionNone, "empty", 100)
			_, err := parser.parseFromReader(strings.NewReader(""))
			require.Error(t, err)
			assert.ErrorIs(t, err, ErrEmptyData)
		})
	}
}

// TestParseFromReader_UnparsableInput covers the binary formats given bytes that
// are not the format at all, which is what a mislabelled file looks like.
func TestParseFromReader_UnparsableInput(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		fileType FileType
	}{
		{"Parquet", FileTypeParquet},
		{"XLSX", FileTypeXLSX},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			parser := newStreamingParser(tt.fileType, CompressionNone, "wrong", 100)
			_, err := parser.parseFromReader(strings.NewReader("id,name\n1,Alice\n"))
			assert.Error(t, err, "bytes that are not the format must not load as a table")
		})
	}
}

// TestLoadFailureCarriesTheContextError pins that a load that ends because its
// context ended reports an error carrying that context's error, whatever the
// database said on the way out.
//
// It did not always: the insert runs on a statement prepared inside the load's
// transaction, and when the context ends database/sql tears that transaction
// down from a goroutine of its own. Reaching the insert after the teardown gave
// "sql: statement is closed", about a statement the caller never held and with
// nothing of the context in the chain, so errors.Is(err, context.Canceled) was
// true most of the time and false the rest for the same cancellation.
func TestLoadFailureCarriesTheContextError(t *testing.T) {
	t.Parallel()

	t.Run("a load that failed under a dead context reports it", func(t *testing.T) {
		t.Parallel()

		db, err := sql.Open("sqlite", ":memory:")
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = db.Close() }()
		db.SetMaxOpenConns(1)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		sp := newStreamProcessor(defaultChunkSizeRows)
		// The load reports what database/sql reports once it has torn the
		// transaction down, which says nothing about the context. Canceling
		// inside the load puts the two in the order the race produces, without
		// racing.
		torn := errors.New("sql: statement is closed")
		err = sp.runInputScope(ctx, db, spentInput, func(*sql.Tx) error {
			cancel()
			return torn
		})

		if err == nil {
			t.Fatal("a load under a canceled context returned no error")
		}
		if !errors.Is(err, context.Canceled) {
			t.Errorf("error = %v, want it to match context.Canceled", err)
		}
		if !errors.Is(err, torn) {
			t.Errorf("error = %v, want it to keep what the database said", err)
		}
	})

	t.Run("a failure under a live context gains no context error", func(t *testing.T) {
		t.Parallel()

		db, err := sql.Open("sqlite", ":memory:")
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = db.Close() }()
		db.SetMaxOpenConns(1)

		own := errors.New("the file was not readable")
		err = sp2RunInput(t, db, own)

		if !errors.Is(err, own) {
			t.Fatalf("error = %v, want it to be the load's own", err)
		}
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("error = %v, want no context error attached to a failure the context had nothing to do with", err)
		}
	})
}

// sp2RunInput runs one input under a context that stays alive, so the test above
// can compare the two answers without repeating the setup.
func sp2RunInput(t *testing.T, db *sql.DB, fail error) error {
	t.Helper()
	sp := newStreamProcessor(defaultChunkSizeRows)
	return sp.runInputScope(context.Background(), db, spentInput, func(*sql.Tx) error { return fail })
}

// TestCancelingALoadAlwaysReportsTheContextError is the property the test above
// pins one case of, run over the real load so the race it comes from is the one
// being exercised.
func TestCancelingALoadAlwaysReportsTheContextError(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "big.csv")
	var body strings.Builder
	body.WriteString("id,name,amount\n")
	for i := range 200000 {
		fmt.Fprintf(&body, "%d,customer%d,%d.5\n", i, i, i)
	}
	if err := os.WriteFile(path, []byte(body.String()), 0o600); err != nil {
		t.Fatal(err)
	}

	interrupted := 0
	for attempt := range 30 {
		// A spread of deadlines so the cancellation lands at a different point
		// of the load each time, which is what makes the race happen at all.
		deadline := time.Duration(1+attempt) * time.Millisecond
		ctx, cancel := context.WithTimeout(context.Background(), deadline)
		db, err := Open(ctx, path)
		if db != nil {
			_ = db.Close()
		}
		cancel()

		if err == nil {
			continue // the load beat the deadline, which is not this test's case
		}
		interrupted++
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("deadline %v: error = %v, want it to match context.DeadlineExceeded", deadline, err)
		}
	}
	// Without this the test passes by never having tested anything: a machine
	// that loads the file inside one millisecond would take every attempt to
	// the end and assert nothing.
	if interrupted == 0 {
		t.Fatal("no attempt was interrupted, so no cancellation was checked")
	}
}

// TestStagedCopyReadsBlankTheWayTheTypingDoes pins the staged path's answer to
// the question the typing asks of the same cell. That path has every column as
// TEXT while it reads, so the blank-cell rule is applied by the copy that types
// the table, in SQL rather than in Go -- and SQLite's own TRIM strips spaces
// alone, so a copy that asked about the empty string left a cell of spaces in a
// numeric column, where MAX answered it.
func TestStagedCopyReadsBlankTheWayTheTypingDoes(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	blanks := []struct {
		name string
		cell string
	}{
		{name: "nothing at all", cell: ""},
		{name: "a space", cell: " "},
		{name: "several spaces", cell: "   "},
		{name: "a tab", cell: "\t"},
		{name: "a newline", cell: "\n"},
		{name: "a no-break space", cell: " "},
		{name: "an ideographic space", cell: "　"},
	}

	for _, tt := range blanks {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			body := "region,amount\nnorth,10\nsouth,\"" + tt.cell + "\"\neast,30\n"
			db, err := openReaderTable(ctx, body, FileTypeCSV)
			require.NoError(t, err)
			defer db.Close()

			var declared, kind string
			var largest int64
			require.NoError(t, db.QueryRowContext(ctx,
				`SELECT type FROM pragma_table_info('rows') WHERE name = 'amount'`).Scan(&declared))
			require.NoError(t, db.QueryRowContext(ctx,
				`SELECT typeof(amount) FROM rows WHERE region = 'south'`).Scan(&kind))
			require.NoError(t, db.QueryRowContext(ctx,
				`SELECT MAX(amount) FROM rows`).Scan(&largest))

			assert.Equal(t, sqlTypeInteger, declared, "a blank cell says nothing about the column's type")
			assert.Equal(t, "null", kind, "a blank cell in a number column is a missing number")
			assert.Equal(t, int64(30), largest, "MAX answers the largest number, not the blank")
		})
	}

	t.Run("a value that is not blank is copied as it is", func(t *testing.T) {
		t.Parallel()

		// The copy tests the trimmed text and copies the cell, so a text
		// column keeps its padding and a number column keeps the value that
		// made it a number column.
		body := "region,amount,label\nnorth,10,  padded  \nsouth,20,x\n"
		db, err := openReaderTable(ctx, body, FileTypeCSV)
		require.NoError(t, err)
		defer db.Close()

		var label string
		var amount int64
		require.NoError(t, db.QueryRowContext(ctx,
			`SELECT label, amount FROM rows WHERE region = 'north'`).Scan(&label, &amount))
		assert.Equal(t, "  padded  ", label, "the padding of a text cell is data")
		assert.Equal(t, int64(10), amount)
	})
}

// endlessReader emits one byte forever with no record terminator, counting what
// it has handed over and refusing past a cap so a test cannot run away. It is
// the source the record bound exists for: a stream that never ends a record.
type endlessReader struct {
	fill   byte
	served int64
	cap    int64
}

func (e *endlessReader) Read(p []byte) (int, error) {
	if e.served >= e.cap {
		return 0, fmt.Errorf("test cap of %d bytes reached without a refusal", e.cap)
	}
	for i := range p {
		p[i] = e.fill
	}
	e.served += int64(len(p))
	return len(p), nil
}

// TestReaderRecordBoundHoldsForEveryFormat drives one unterminated stream
// through every format a reader can be loaded as. The bound is what keeps a
// source that never ends a record from being read for as long as the sender
// keeps sending, so a format that does not hold to it is the one hole worth
// having a test across all of them for: each format was correct on its own and
// the pair of formats read by a library of their own disagreed.
func TestReaderRecordBoundHoldsForEveryFormat(t *testing.T) {
	t.Parallel()

	// Past the bound with room to see a refusal, and far short of what an
	// unbounded read would take.
	const readCap = 3 * (64 << 20)

	tests := []struct {
		name     string
		fileType FileType
		fill     byte
	}{
		{"CSV", FileTypeCSV, 'x'},
		{"TSV", FileTypeTSV, 'x'},
		{"LTSV", FileTypeLTSV, 'x'},
		{"JSON", FileTypeJSON, 'x'},
		{"JSONL", FileTypeJSONL, 'x'},
		{"ACH", FileTypeACH, '1'},
		{"FedWire", FileTypeFedWire, '1'},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			src := &endlessReader{fill: tt.fill, cap: readCap}
			db, err := NewBuilder().
				AddReader(src, "t", tt.fileType).
				Open(context.Background())
			if db != nil {
				require.NoError(t, db.Close())
			}
			require.Error(t, err, "an unterminated stream must be refused")
			assert.ErrorIs(t, err, reader.ErrRecordTooLong,
				"the refusal must be the record bound rather than whatever the format complains about first")
			assert.Less(t, src.served, int64(readCap),
				"the stream was read to the test's cap, so nothing bounded it")
		})
	}
}

// TestDumpWithSourceHoldsTheRecordBound is
// TestReaderRecordBoundHoldsForEveryFormat for the other direction. The two
// exports that take the original file's bytes read them with the same library
// the load path uses, so they need the same bound: without it a stream that
// never ends a record is read for as long as the sender keeps sending, and the
// export is reachable with a reader a caller supplies.
func TestDumpWithSourceHoldsTheRecordBound(t *testing.T) {
	t.Parallel()

	const readCap = 3 * (64 << 20)

	tests := []struct {
		name string
		dump func(ctx context.Context, db *sql.DB, out string, source io.Reader) error
	}{
		{"ACH", func(ctx context.Context, db *sql.DB, out string, source io.Reader) error {
			return DumpACHWithSource(ctx, db, "payment", out, source)
		}},
		{"FedWire", func(ctx context.Context, db *sql.DB, out string, source io.Reader) error {
			return DumpFedWireWithSource(ctx, db, "payment", out, source)
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			db, err := NewBuilder().
				AddReader(strings.NewReader("id\n1\n"), "seed", FileTypeCSV).
				Open(context.Background())
			require.NoError(t, err)
			defer db.Close()

			src := &endlessReader{fill: '1', cap: readCap}
			out := filepath.Join(t.TempDir(), "out")
			err = tt.dump(context.Background(), db, out, src)

			require.Error(t, err, "an unterminated stream must be refused")
			assert.ErrorIs(t, err, reader.ErrRecordTooLong,
				"the refusal must be the record bound rather than whatever the format complains about first")
			assert.Less(t, src.served, int64(readCap),
				"the stream was read to the test's cap, so nothing bounded it")
		})
	}
}
