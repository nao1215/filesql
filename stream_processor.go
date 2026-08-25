package filesql

import (
	"bufio"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/nao1215/filesql/internal/reader"
	"modernc.org/sqlite"
	sqlite3 "modernc.org/sqlite/lib"
)

// Structured log attribute keys used across stream processing.
const (
	logKeyTable = "table"
	logKeySheet = "sheet"
)

// streamProcessor handles streaming operations for database loading
type streamProcessor struct {
	chunkSize int
	logger    Logger
	// replaceExisting makes table creation drop a same-named table first instead
	// of erroring or appending. It is enabled for LoadInto (loading into a
	// caller-owned database, where last-wins replacement is the useful default)
	// and left false for Open (fresh in-memory database, no collisions).
	replaceExisting bool
	// malformedRowPolicy controls how a CSV/TSV record whose field count differs
	// from the header is handled. The zero value is MalformedRowStop.
	malformedRowPolicy MalformedRowPolicy
	// excelSheetPolicy controls which sheets of a workbook are loaded. The zero
	// value is ExcelSheetPolicyAll.
	excelSheetPolicy ExcelSheetPolicy
	// skipped records what MalformedRowSkip discarded, per table, so a caller
	// can report it. Loads run one file at a time here, but a builder can hold
	// several, so the mutex covers a future that runs them together.
	skippedMu sync.Mutex
	skipped   []SkippedRows
}

// SkippedRows is how much of one table's input the malformed-row policy
// discarded.
type SkippedRows struct {
	// Table is the table the rows would have gone into.
	Table string
	// Count is how many data rows were dropped.
	Count int
	// Total is how many data rows the input held, dropped ones included, so a
	// caller can say "2 of 4" rather than a bare number.
	Total int
}

// recordSkippedRows keeps what one load discarded. Nothing is recorded for a
// load that dropped nothing, so a caller can treat a non-empty result as
// something worth telling the user about.
func (sp *streamProcessor) recordSkippedRows(table string, count, total int) {
	if count == 0 {
		return
	}
	sp.skippedMu.Lock()
	defer sp.skippedMu.Unlock()
	sp.skipped = append(sp.skipped, SkippedRows{Table: table, Count: count, Total: total})
}

// skippedRows returns what the loads so far discarded.
func (sp *streamProcessor) skippedRows() []SkippedRows {
	sp.skippedMu.Lock()
	defer sp.skippedMu.Unlock()
	return append([]SkippedRows(nil), sp.skipped...)
}

// dropIfReplacing drops a same-named table when replaceExisting is set, so the
// following CREATE installs the file's schema and rows in place of any prior
// table. It is a no-op in Open mode.
func (sp *streamProcessor) dropIfReplacing(ctx context.Context, db DBTX, tableName string) error {
	if !sp.replaceExisting {
		return nil
	}
	if _, err := db.ExecContext(ctx, `DROP TABLE IF EXISTS "`+tableName+`"`); err != nil {
		return fmt.Errorf("%w: failed to drop existing table %q: %w", ErrDatabaseOperation, tableName, err)
	}
	return nil
}

// newStreamProcessor creates a new stream processor instance
func newStreamProcessor(chunkSize int) *streamProcessor {
	return &streamProcessor{
		chunkSize: chunkSize,
		logger:    newNopLogger(),
	}
}

// setChunkSize sets how many rows the processor reads at a time.
func (sp *streamProcessor) setChunkSize(size int) {
	if size > 0 {
		sp.chunkSize = size
	}
}

// setLogger sets the logger for the stream processor
func (sp *streamProcessor) setLogger(logger Logger) {
	if logger != nil {
		sp.logger = logger
	}
}

// streamAllFilesToDatabase streams all collected file paths to the database
func (sp *streamProcessor) streamAllFilesToDatabase(ctx context.Context, db DBTX, collectedPaths []string) error {
	sp.logger.Info("starting file streaming", "file_count", len(collectedPaths))
	for i, path := range collectedPaths {
		sp.logger.Debug("streaming file", "path", path, "index", i+1, "total", len(collectedPaths))
		// A path can be read again, so an input that met another load's lock
		// waits and starts over rather than failing.
		err := sp.runInputScope(ctx, db, rereadableInput, func(scope *sql.Tx) error {
			return sp.streamFileToDatabase(ctx, scope, path)
		})
		if err != nil {
			sp.logger.Error("failed to stream file", "path", path, "error", err)
			// A *ParseError rather than more text: it names the file once and
			// carries both ErrParsing and whatever sentinel the cause holds (for
			// example ErrColumnMismatch from the malformed-row policy), so
			// errors.Is finds either and a caller that already named the file can
			// reach the cause alone.
			return &ParseError{Source: path, Err: err}
		}
	}
	sp.logger.Info("completed file streaming", "file_count", len(collectedPaths))
	return nil
}

// streamAllReadersToDatabase streams all reader inputs to the database
func (sp *streamProcessor) streamAllReadersToDatabase(ctx context.Context, db DBTX, readers []readerInput) error {
	if len(readers) == 0 {
		return nil
	}
	sp.logger.Info("starting reader streaming", "reader_count", len(readers))
	for i, ri := range readers {
		sp.logger.Debug("streaming reader", logKeyTable, ri.tableName, "file_type", ri.fileType.String(), "index", i+1, "total", len(readers))
		// A reader is spent by the attempt that reads it, so only the step
		// before anything is read can be tried again.
		err := sp.runInputScope(ctx, db, spentInput, func(scope *sql.Tx) error {
			return sp.streamReaderToDatabase(ctx, scope, ri)
		})
		sp.closeReaderInput(ri)
		if err != nil {
			sp.logger.Error("failed to stream reader", logKeyTable, ri.tableName, "error", err)
			return &ParseError{Source: ri.tableName, Err: err}
		}
	}
	sp.logger.Info("completed reader streaming", "reader_count", len(readers))
	return nil
}

// inputSavepoint is the savepoint one input loads under when the caller owns
// the transaction. Its name carries the prefix this package reserves, so it
// cannot be the name of a savepoint the caller opened.
const inputSavepoint = `"_filesql_input"`

// runInputScope runs one input's whole load atomically: either all of it lands
// or none of it does, the drop of a table it was replacing included. The scope
// is per input rather than per table because an input and a table are not the
// same thing -- a workbook is one input and one table per sheet, an ACH file is
// one input and several tables, and a Fedwire file is one input, its message
// table and its write-back row -- and the documented contract is per input.
//
// On a database this package drives, the scope is a transaction. It is also
// what batches the inserts, which is most of a load's speed, and the creates
// belong inside it for the same reason the rows do: a load that fails leaves
// nothing behind, not a table named after the file with no rows in it.
//
// Inside a transaction the caller owns, the scope is a savepoint: the
// transaction is not this package's to end, but rolling back to the savepoint
// leaves it exactly as the input found it, which is what lets the caller keep
// using it after a failure.
func (sp *streamProcessor) runInputScope(ctx context.Context, db DBTX, kind inputKind, load func(*sql.Tx) error) error {
	switch d := db.(type) {
	case *sql.DB:
		if kind == spentInput {
			return sp.runInputTx(ctx, d, load)
		}
		return retryWhileLocked(ctx, func() error { return sp.runInputTx(ctx, d, load) })
	case *sql.Tx:
		if _, err := d.ExecContext(ctx, `SAVEPOINT `+inputSavepoint); err != nil {
			return fmt.Errorf("%w: failed to open savepoint: %w", ErrDatabaseOperation, err)
		}
		if err := load(d); err != nil {
			return joinCleanup(err, undoInput(ctx, d), "roll back to savepoint")
		}
		if _, err := d.ExecContext(ctx, `RELEASE `+inputSavepoint); err != nil {
			return fmt.Errorf("%w: failed to release savepoint: %w", ErrDatabaseOperation, err)
		}
		return nil
	default:
		return fmt.Errorf("%w: unsupported database executor %T", ErrDatabaseOperation, db)
	}
}

// runInputTx runs one input in a transaction of its own.
func (sp *streamProcessor) runInputTx(ctx context.Context, db *sql.DB, load func(*sql.Tx) error) error {
	var tx *sql.Tx
	// Beginning is retried whatever the input is: nothing has been read yet, so
	// starting over costs nothing and loses nothing.
	if err := retryWhileLocked(ctx, func() (err error) {
		tx, err = db.BeginTx(ctx, nil)
		return err
	}); err != nil {
		return fmt.Errorf("%w: failed to begin transaction: %w", ErrDatabaseOperation, err)
	}
	if err := load(tx); err != nil {
		// When the context is done, database/sql has already rolled the
		// transaction back itself, so this call loses the race and reports
		// sql.ErrTxDone. That is cancellation working as documented, and the
		// cause is already in err.
		rollbackErr := tx.Rollback()
		if errors.Is(rollbackErr, sql.ErrTxDone) && ctx.Err() != nil {
			return err
		}
		return joinCleanup(err, rollbackErr, "rollback import transaction")
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("%w: failed to commit transaction: %w", ErrDatabaseOperation, err)
	}
	return nil
}

// inputKind says whether an input can be read a second time, which decides how
// much of a load may be tried again when another load holds the database.
type inputKind int

const (
	// rereadableInput is a path: the file is still there, so a whole load can
	// start over.
	rereadableInput inputKind = iota
	// spentInput is a reader: what it held is gone once it has been read, so
	// only the steps before the reading can be tried again.
	spentInput
)

// How long an input waits for another load to let go, and how the wait grows.
//
// SQLite does not queue a second writer; it refuses one. Creating a table takes
// a lock on the schema, so two loads into the same database at the same time
// left one of them reporting `database schema is locked` on a shared-cache
// database or `database is locked` on a file, with its table not created and
// nothing having queued behind anything. Waiting is what every SQLite
// application does about that, and five seconds is the budget the drivers'
// busy_timeout defaults sit around; past it the error the database gave is
// returned as it stands.
const (
	loadLockBudget  = 5 * time.Second
	loadLockFloor   = time.Millisecond
	loadLockCeiling = 50 * time.Millisecond
)

// retryWhileLocked runs step until it succeeds, fails for a reason other than
// another connection's lock, runs out of budget, or the context ends.
//
// The wait doubles and carries jitter, because the loads that collide are the
// loads that would otherwise retry in step with one another.
func retryWhileLocked(ctx context.Context, step func() error) error {
	deadline := time.Now().Add(loadLockBudget)
	for wait := loadLockFloor; ; {
		err := step()
		if err == nil || !lockedByAnotherConnection(err) {
			return err
		}
		// The wait is cut to what is left of the budget, so the last attempt
		// happens inside it rather than just past it.
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return err
		}
		delay := wait/2 + rand.N(wait/2+1) //nolint:gosec // Jitter, not a secret
		if delay > remaining {
			delay = remaining
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return err
		case <-timer.C:
		}
		if wait *= 2; wait > loadLockCeiling {
			wait = loadLockCeiling
		}
	}
}

// lockedByAnotherConnection reports the two answers SQLite gives when someone
// else holds what this load needs. It reads the driver's code rather than the
// message, which differs between a file database and a shared-cache one and is
// not this package's to depend on: SQLITE_BUSY for a file, SQLITE_LOCKED for a
// shared-cache table, each with extended codes above them that carry the same
// primary code in their low byte.
func lockedByAnotherConnection(err error) bool {
	var sqliteErr *sqlite.Error
	if !errors.As(err, &sqliteErr) {
		return false
	}
	return isLockCode(sqliteErr.Code())
}

// isLockCode reads a result code's low byte, which is where an extended code
// carries the primary one it refines.
func isLockCode(code int) bool {
	switch code & 0xFF {
	case sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED:
		return true
	default:
		return false
	}
}

// undoInput returns the caller's transaction to the savepoint the input opened
// and pops it, since rolling back to a savepoint in SQLite leaves it standing.
//
// The two statements run under a context that cannot be canceled. A canceled
// context is one of the reasons a load fails, and it is no reason to leave the
// caller holding half an input: the undo is what has to happen either way. When
// the caller opened their transaction with that same context, database/sql has
// already rolled the whole transaction back and reports sql.ErrTxDone here,
// which is this undo having happened by a larger one.
func undoInput(ctx context.Context, tx *sql.Tx) error {
	ctx = context.WithoutCancel(ctx)
	if _, err := tx.ExecContext(ctx, `ROLLBACK TO `+inputSavepoint); err != nil {
		if errors.Is(err, sql.ErrTxDone) {
			return nil
		}
		return err
	}
	_, err := tx.ExecContext(ctx, `RELEASE `+inputSavepoint)
	return err
}

// closeReaderInput closes the underlying resource of a reader input if it was opened internally (e.g. from AddFS).
func (sp *streamProcessor) closeReaderInput(ri readerInput) {
	if ri.closer != nil {
		if err := ri.closer.Close(); err != nil {
			sp.logger.Debug("error closing reader input", logKeyTable, ri.tableName, "error", err)
		}
	}
}

// streamFileToDatabase streams data from a file path directly to SQLite database using chunked processing
func (sp *streamProcessor) streamFileToDatabase(ctx context.Context, tx *sql.Tx, filePath string) error {
	// Check if file is ACH format
	if isACHFile(filePath) {
		sp.logger.Debug("detected ACH file format", "path", filePath)
		return sp.streamACHFileToDatabase(ctx, tx, filePath)
	}

	// Check if file is Fedwire format
	if isFedWireFile(filePath) {
		sp.logger.Debug("detected Fedwire file format", "path", filePath)
		return sp.streamFedWireFileToDatabase(ctx, tx, filePath)
	}

	// Check if file is supported
	if !isSupportedFile(filePath) {
		sp.logger.Warn("unsupported file format", "path", filePath)
		return fmt.Errorf("%w: %s", ErrUnsupportedFormat, filePath)
	}

	// Open the file and create a reader
	file, err := os.Open(filePath) //nolint:gosec // File path is validated and comes from user input
	if err != nil {
		sp.logger.Error("failed to open file", "path", filePath, "error", err)
		return fmt.Errorf("%w: failed to open file %s: %w", ErrIOOperation, filePath, err)
	}
	defer file.Close()

	// Determine the type before checking size so empty JSON/JSONL inputs can be
	// represented as zero-row tables by the same streaming load path.
	fileModel := newFile(filePath)
	baseFileType := fileModel.getFileType()

	// Check if file is empty before processing. JSON and JSONL are allowed to
	// reach their parser because their empty input is a valid zero-row table.
	fileInfo, err := file.Stat()
	if err != nil {
		sp.logger.Error("failed to get file info", "path", filePath, "error", err)
		return fmt.Errorf("%w: failed to get file info for %s: %w", ErrIOOperation, filePath, err)
	}
	if fileInfo.Size() == 0 && baseFileType != FileTypeJSON && baseFileType != FileTypeJSONL {
		sp.logger.Warn("empty file detected", "path", filePath)
		return fmt.Errorf("%w: file is empty", ErrEmptyData)
	}
	sp.logger.Debug("file opened", "path", filePath, "size", fileInfo.Size())

	sp.logger.Debug("detected file type", "path", filePath, "type", baseFileType.String())

	// Create decompressed reader if needed
	reader, closer, err := sp.createDecompressedReader(file, filePath)
	if err != nil {
		sp.logger.Error("failed to create decompressed reader", "path", filePath, "error", err)
		return fmt.Errorf("%w: failed to create decompressed reader for %s: %w", ErrCompression, filePath, err)
	}
	if closer != nil {
		sp.logger.Debug("compression detected, created decompressed reader", "path", filePath)
	}
	defer func() {
		if closer != nil {
			if closeErr := closer(); closeErr != nil {
				sp.logger.Debug("error closing decompressed reader", "path", filePath, "error", closeErr)
			}
		}
	}()

	// Handle XLSX files specially - each sheet becomes a separate table
	if baseFileType == FileTypeXLSX {
		sp.logger.Debug("processing XLSX file with multiple sheets", "path", filePath)
		return sp.streamXLSXFileToDatabase(ctx, tx, reader, filePath)
	}

	// Create reader input for streaming
	tableName := sanitizeTableName(tableFromFilePath(filePath))
	sp.logger.Debug("streaming file to table", "path", filePath, logKeyTable, tableName, "type", baseFileType.String())
	readerInput := readerInput{
		reader:      reader, // Use decompressed reader
		tableName:   tableName,
		fileType:    baseFileType,
		compression: CompressionNone, // already unwrapped above
		reopen: func() (io.Reader, func() error, error) {
			return NewCompressionFactory().CreateReaderForFile(filePath)
		},
	}
	return sp.streamReaderToDatabase(ctx, tx, readerInput)
}

// streamACHFileToDatabase handles ACH files by creating multiple tables
func (sp *streamProcessor) streamACHFileToDatabase(ctx context.Context, tx *sql.Tx, filePath string) error {
	sp.logger.Debug("processing ACH file", "path", filePath)

	// Open the file
	file, err := os.Open(filePath) //nolint:gosec // File path is validated and comes from user input
	if err != nil {
		sp.logger.Error("failed to open ACH file", "path", filePath, "error", err)
		return fmt.Errorf("%w: failed to open ACH file %s: %w", ErrIOOperation, filePath, err)
	}
	defer file.Close()

	// Check if file is empty
	fileInfo, err := file.Stat()
	if err != nil {
		sp.logger.Error("failed to get ACH file info", "path", filePath, "error", err)
		return fmt.Errorf("%w: failed to get file info for %s: %w", ErrIOOperation, filePath, err)
	}
	if fileInfo.Size() == 0 {
		sp.logger.Warn("empty ACH file detected", "path", filePath)
		return fmt.Errorf("%w: ACH file is empty", ErrEmptyData)
	}

	sp.logger.Debug("streaming ACH file to database", "path", filePath, "size", fileInfo.Size())
	return streamACHFileToDatabase(ctx, tx, file, filePath, filePath, sp.replaceExisting)
}

// streamFedWireFileToDatabase handles Fedwire files by creating a single message table
func (sp *streamProcessor) streamFedWireFileToDatabase(ctx context.Context, tx *sql.Tx, filePath string) error {
	sp.logger.Debug("processing Fedwire file", "path", filePath)

	// Open the file
	file, err := os.Open(filePath) //nolint:gosec // File path is validated and comes from user input
	if err != nil {
		sp.logger.Error("failed to open Fedwire file", "path", filePath, "error", err)
		return fmt.Errorf("%w: failed to open Fedwire file %s: %w", ErrIOOperation, filePath, err)
	}
	defer file.Close()

	// Check if file is empty
	fileInfo, err := file.Stat()
	if err != nil {
		sp.logger.Error("failed to get Fedwire file info", "path", filePath, "error", err)
		return fmt.Errorf("%w: failed to get file info for %s: %w", ErrIOOperation, filePath, err)
	}
	if fileInfo.Size() == 0 {
		sp.logger.Warn("empty Fedwire file detected", "path", filePath)
		return fmt.Errorf("%w: Fedwire file is empty", ErrEmptyData)
	}

	sp.logger.Debug("streaming Fedwire file to database", "path", filePath, "size", fileInfo.Size())
	return streamWireFileToDatabase(ctx, tx, file, filePath, filePath, sp.replaceExisting)
}

// streamReaderToDatabase loads one reader into the table named for it.
func (sp *streamProcessor) streamReaderToDatabase(ctx context.Context, tx *sql.Tx, input readerInput) error {
	// Route ACH/Fedwire readers to dedicated handlers. No source path is
	// recorded: a reader has no file to read again at dump time, so these tables
	// can only be written back through DumpACHWithTableSet or
	// DumpFedWireWithTableSet.
	if input.fileType == FileTypeACH {
		return streamACHFileToDatabase(ctx, tx, input.reader, input.tableName+extACH, "", sp.replaceExisting)
	}
	if input.fileType == FileTypeFedWire {
		return streamWireFileToDatabase(ctx, tx, input.reader, input.tableName+extFED, "", sp.replaceExisting)
	}

	if err := validateTableName(input.tableName); err != nil {
		return err
	}
	if err := sp.refuseExistingTable(ctx, tx, input.tableName); err != nil {
		return err
	}

	// Reader should already be validated at Build time, but ensure it's buffered
	if _, ok := input.reader.(*bufio.Reader); !ok {
		input.reader = bufio.NewReader(input.reader)
	}

	newParser := func() *streamingParser {
		parser := newStreamingParser(input.fileType, input.compression, input.tableName, sp.chunkSize)
		parser.malformedRowPolicy = sp.malformedRowPolicy
		parser.excelSheetPolicy = sp.excelSheetPolicy
		return parser
	}
	parser := newParser()
	// What the malformed-row policy dropped is worth saying even when the load
	// itself succeeded, so it is collected however this function returns.
	defer func() { sp.recordSkippedRows(input.tableName, parser.skippedRows, parser.totalRows) }()

	source := tableSource{
		read: func(emit chunkProcessor) (columnInfoList, error) {
			return parser.ProcessInChunks(input.reader, emit)
		},
	}
	if input.reopen != nil {
		source.reread = func(emit chunkProcessor) (columnInfoList, error) {
			again, closeAgain, err := input.reopen()
			if err != nil {
				return nil, err
			}
			defer closeQuietly(closeAgain)
			return newParser().ProcessInChunks(again, emit)
		}
	}
	return sp.loadTable(ctx, tx, input.tableName, source)
}

// refuseExistingTable reports ErrDuplicateTable when a table of this name is
// already there and this load is not allowed to replace it. The comparison
// folds ASCII case because SQLite folds it when it matches identifiers:
// without NOCASE, a second file named Users.csv beside users.csv found the
// name free, and its rows went under the first file's headers.
func (sp *streamProcessor) refuseExistingTable(ctx context.Context, db DBTX, tableName string) error {
	if sp.replaceExisting {
		return nil
	}
	var tableExists int
	err := db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name = ? COLLATE NOCASE`,
		tableName,
	).Scan(&tableExists)
	if err != nil {
		return fmt.Errorf("%w: failed to check table existence: %w", ErrDatabaseOperation, err)
	}
	if tableExists > 0 {
		sp.logger.Warn("table already exists", logKeyTable, tableName)
		return fmt.Errorf("%w: table '%s' already exists from another file", ErrDuplicateTable, tableName)
	}
	return nil
}

// tableSource is what a load reads. read hands every chunk of rows to emit,
// and then says what columns the rows require; reread is read again from the
// first byte, or nil for an input that can only be read once.
type tableSource struct {
	read   func(emit chunkProcessor) (columnInfoList, error)
	reread func(emit chunkProcessor) (columnInfoList, error)
}

// loadTable loads one table from source into the scope its input runs in.
// Undoing a failure is the scope's business, not this function's: everything
// here happens inside one transaction, which runInputScope rolls back.
//
// A column's type is decided by every row in the input, and for a format
// without a schema the last row can still change it. Declaring the table from
// the first rows and widening it later was where a chunk boundary changed the
// data: a value inserted into a numeric column had already taken the column's
// storage class, and the rebuild that widened the column to TEXT could only
// carry forward SQLite's spelling of the number, not the file's. So no row is
// stored under a type a later row can still widen. An input that can be read
// again is loaded under the types its first chunk requires and, should a later
// chunk require more, read again under the types the whole of it requires. An
// input that cannot be read again is staged as text and typed once it has all
// been read.
func (sp *streamProcessor) loadTable(ctx context.Context, tx *sql.Tx, tableName string, source tableSource) error {
	if source.reread == nil {
		return sp.loadStaged(ctx, tx, tableName, source.read)
	}
	return sp.loadTyped(ctx, tx, tableName, source)
}

// tableWriter inserts the chunks of one table into a table of a given name.
type tableWriter struct {
	sp        *streamProcessor
	tx        *sql.Tx
	tableName string
	// types are the columns the table was created with, nil until it was.
	types  columnInfoList
	stmt   *sql.Stmt
	chunks int
	rows   int
}

// create makes the table with the given columns and prepares its insert.
func (w *tableWriter) create(ctx context.Context, headers header, types columnInfoList) error {
	w.sp.logger.Debug("creating table", logKeyTable, w.tableName, "columns", len(headers))
	if err := createTable(ctx, w.tx, w.tableName, types); err != nil {
		return fmt.Errorf("%w: failed to create table: %w", ErrDatabaseOperation, err)
	}
	stmt, err := w.tx.PrepareContext(ctx, insertQuery(w.tableName, len(headers))) //nolint:sqlclosecheck // Closed by close once the load has ended.
	if err != nil {
		return fmt.Errorf("%w: failed to prepare insert statement: %w", ErrDatabaseOperation, err)
	}
	w.types = types
	w.stmt = stmt
	return nil
}

// insert stores one chunk.
func (w *tableWriter) insert(ctx context.Context, chunk *tableChunk) error {
	w.chunks++
	w.rows += len(chunk.getRecords())
	w.sp.logger.Debug("inserting chunk", logKeyTable, w.tableName, "chunk", w.chunks, "rows", len(chunk.getRecords()))
	if err := w.sp.insertChunkData(ctx, w.stmt, chunk); err != nil {
		return fmt.Errorf("%w: failed to insert chunk data: %w", ErrDatabaseOperation, err)
	}
	return nil
}

// close releases the insert statement. Its failure is joined onto err rather
// than dropped: a statement that cannot be closed holds a connection, which
// shows up later as an unrelated hang rather than as this load's problem.
func (w *tableWriter) close(err error) error {
	if w.stmt == nil {
		return err
	}
	err = joinCleanup(err, w.stmt.Close(), "close insert statement")
	w.stmt = nil
	return err
}

// loadTyped declares the table from the types its first chunk requires and
// inserts as it reads, which is the whole cost of a load whose columns are what
// their first rows say. When a later chunk requires a wider type, the rest of
// the input is read for its types alone, the table is dropped, and the input
// is read again under the types the whole of it requires.
func (sp *streamProcessor) loadTyped(ctx context.Context, tx *sql.Tx, tableName string, source tableSource) (err error) {
	w := &tableWriter{sp: sp, tx: tx, tableName: tableName}
	defer func() { err = w.close(err) }()

	widened := false
	final, err := source.read(func(chunk *tableChunk) error {
		if widened {
			return nil
		}
		if w.stmt == nil {
			if err := sp.dropIfReplacing(ctx, tx, tableName); err != nil {
				return err
			}
			if err := w.create(ctx, chunk.getHeaders(), chunk.types); err != nil {
				return err
			}
		}
		if !w.types.equalTypes(chunk.types) {
			sp.logger.Debug("a later chunk widens a column; the input will be read again", logKeyTable, tableName)
			widened = true
			return nil
		}
		return w.insert(ctx, chunk)
	})
	if err != nil {
		return err
	}
	if w.stmt == nil {
		return fmt.Errorf("%w: no table found for %s", ErrEmptyData, tableName)
	}
	if !widened {
		sp.logger.Info("table created", logKeyTable, tableName, "total_rows", w.rows)
		return nil
	}

	if err := w.close(nil); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DROP TABLE "`+tableName+`"`); err != nil {
		return fmt.Errorf("%w: failed to drop table before reading again: %w", ErrDatabaseOperation, err)
	}
	w.rows = 0
	again, err := source.reread(func(chunk *tableChunk) error {
		if w.stmt == nil {
			if err := w.create(ctx, chunk.getHeaders(), final); err != nil {
				return err
			}
		}
		return w.insert(ctx, chunk)
	})
	if err != nil {
		return err
	}
	// The second read decides nothing, so it has to have read the same input.
	if !again.equalTypes(final) {
		return fmt.Errorf("%w: %s changed while it was being read", ErrParsing, tableName)
	}
	sp.logger.Info("table created", logKeyTable, tableName, "total_rows", w.rows, "read_twice", true)
	return nil
}

// loadStaged stages every row as text and declares the table once the last
// row has been read. It is for an input that cannot be read twice: text is the
// one form every later type can still be made from, so it is what is kept
// until the type is final. The cost is a copy of the table inside SQLite at
// the end, which an input that can be read again does not pay.
func (sp *streamProcessor) loadStaged(ctx context.Context, tx *sql.Tx, tableName string, read func(emit chunkProcessor) (columnInfoList, error)) (err error) {
	staging := stagingTableName(tableName)
	w := &tableWriter{sp: sp, tx: tx, tableName: staging}
	defer func() { err = w.close(err) }()

	columns, err := read(func(chunk *tableChunk) error {
		if w.stmt == nil {
			if err := w.create(ctx, chunk.getHeaders(), textColumns(chunk.getHeaders())); err != nil {
				return err
			}
		}
		return w.insert(ctx, chunk)
	})
	if err != nil {
		return err
	}
	if w.stmt == nil {
		return fmt.Errorf("%w: no table found for %s", ErrEmptyData, tableName)
	}
	if err := sp.declareTable(ctx, tx, staging, tableName, columns); err != nil {
		return err
	}
	sp.logger.Info("table created", logKeyTable, tableName, "total_rows", w.rows, "staged", true)
	return nil
}

// declareTable gives the staged rows their table's name and column types.
//
// A table whose every column is declared TEXT is the staging table renamed,
// since that is already what it is. Any other is created with its types and
// the rows copied into it. The copy is where a value written as text takes the
// storage class its column calls for: SQLite applies the column's affinity to
// each value, the same conversion an insert into that column would have made,
// applied once, to every row, under the final types.
func (sp *streamProcessor) declareTable(ctx context.Context, tx *sql.Tx, staging, tableName string, columns columnInfoList) error {
	if err := sp.dropIfReplacing(ctx, tx, tableName); err != nil {
		return err
	}
	if columns.allText() {
		if _, err := tx.ExecContext(ctx, fmt.Sprintf(`ALTER TABLE "%s" RENAME TO "%s"`, staging, tableName)); err != nil {
			return fmt.Errorf("%w: failed to name table: %w", ErrDatabaseOperation, err)
		}
		return nil
	}
	if err := createTable(ctx, tx, tableName, columns); err != nil {
		return fmt.Errorf("%w: failed to create table: %w", ErrDatabaseOperation, err)
	}
	statements := []string{
		fmt.Sprintf(`INSERT INTO "%s" SELECT * FROM "%s"`, tableName, staging),
		fmt.Sprintf(`DROP TABLE "%s"`, staging),
	}
	for _, statement := range statements {
		if _, err := tx.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("%w: failed to type table: %w", ErrDatabaseOperation, err)
		}
	}
	return nil
}

// stagingTableName names the table a load's rows wait in. It is under the
// prefix this package keeps for itself, so no input can load into it, and it
// is gone before the transaction ends.
func stagingTableName(tableName string) string {
	return sourceTablePrefix + "stage_" + tableName
}

// textColumns names every column of a header as TEXT, which is what a staging
// table is.
func textColumns(headers header) columnInfoList {
	columns := make(columnInfoList, len(headers))
	for i, name := range headers {
		columns[i] = columnInfo{Name: name, Type: columnTypeText}
	}
	return columns
}

// createTable creates a table with the given columns.
func createTable(ctx context.Context, db DBTX, tableName string, columns columnInfoList) error {
	defs := make([]string, 0, len(columns))
	for _, col := range columns {
		defs = append(defs, fmt.Sprintf(`%s %s`, quoteIdentifier(col.Name), col.Type.string()))
	}
	_, err := db.ExecContext(ctx, fmt.Sprintf(`CREATE TABLE %s (%s)`, quoteIdentifier(tableName), strings.Join(defs, ", ")))
	return err
}

// insertQuery is the INSERT for a table of the given width.
func insertQuery(tableName string, width int) string {
	placeholders := make([]string, width)
	for i := range placeholders {
		placeholders[i] = "?"
	}
	return fmt.Sprintf(`INSERT INTO "%s" VALUES (%s)`, tableName, strings.Join(placeholders, ", "))
}

// insertChunkData inserts a chunk's worth of rows through a prepared statement.
// One values slice is reused across rows to keep allocations down.
func (sp *streamProcessor) insertChunkData(ctx context.Context, stmt *sql.Stmt, chunk *tableChunk) error {
	records := chunk.getRecords()
	if len(records) == 0 {
		return nil
	}

	// The header is the authoritative width.
	colCount := len(chunk.getHeaders())
	values := make([]any, colCount)
	nulls := chunk.getNulls()

	for rowIdx, record := range records {
		// A record wider than the header would lose cells silently.
		if len(record) > colCount {
			return fmt.Errorf("%w: record has more columns (%d) than headers (%d)", ErrColumnMismatch, len(record), colCount)
		}

		// A record shorter than the header is missing its last cells, which are
		// NULL.
		for i := range colCount {
			switch {
			case nulls != nil && rowIdx < len(nulls) && i < len(nulls[rowIdx]) && nulls[rowIdx][i]:
				values[i] = nil // a source NULL (e.g. a Parquet null) inserts as SQL NULL
			case i < len(record):
				values[i] = record[i]
			default:
				values[i] = nil
			}
		}

		if _, err := stmt.ExecContext(ctx, values...); err != nil {
			return fmt.Errorf("%w: failed to insert record: %w", ErrDatabaseOperation, err)
		}
	}

	return nil
}

// createDecompressedReader creates a reader that handles compression
func (sp *streamProcessor) createDecompressedReader(file *os.File, filePath string) (io.Reader, func() error, error) {
	factory := NewCompressionFactory()
	handler := factory.CreateHandlerForFile(filePath)

	reader, cleanup, err := handler.CreateReader(file)
	if err != nil {
		return nil, nil, err
	}

	// Return reader with cleanup that doesn't close the file (caller handles that)
	return reader, cleanup, nil
}

// streamXLSXFileToDatabase handles XLSX files by creating separate tables for each sheet
func (sp *streamProcessor) streamXLSXFileToDatabase(ctx context.Context, tx *sql.Tx, source io.Reader, filePath string) error {
	sp.logger.Debug("reading XLSX data into memory", "path", filePath)

	workbook, err := reader.OpenWorkbook(source)
	if err != nil {
		sp.logger.Error("failed to open XLSX file", "path", filePath, "error", err)
		return wrapReadError(err)
	}
	defer func() {
		_ = workbook.Close() // Ignore close error
	}()

	// Get the sheet names the policy admits. Filtering here rather than while
	// looping is what lets the collision check below see exactly the sheets that
	// will become tables.
	sheetNames, skipped, err := selectExcelSheets(workbook.Source(), sp.excelSheetPolicy)
	if err != nil {
		sp.logger.Error("failed to read sheet visibility", "path", filePath, "error", err)
		return err
	}
	if len(skipped) > 0 {
		sp.logger.Info("skipping sheets the workbook hides", "path", filePath, "skipped_count", len(skipped))
	}
	if len(sheetNames) == 0 {
		sp.logger.Warn("no sheets to load from XLSX file", "path", filePath)
		return noExcelSheetsError(workbook.Source(), sp.excelSheetPolicy)
	}
	sp.logger.Info("processing XLSX file", "path", filePath, "sheet_count", len(sheetNames))

	// Every sheet's table name is worked out before any of them is created, so a
	// workbook whose sheets would share a table is refused instead of loading the
	// last one over the others.
	sheetTables, err := ExcelSheetTableNames(filePath, sheetNames)
	if err != nil {
		sp.logger.Error("sheet names collide", "path", filePath, "error", err)
		return err
	}
	for _, tableName := range sheetTables {
		if err := validateTableName(tableName); err != nil {
			return err
		}
	}

	// Process each sheet as a separate table
	for i, sheetName := range sheetNames {
		sp.logger.Debug("processing sheet", "path", filePath, logKeySheet, sheetName, "index", i+1, "total", len(sheetNames))
		if err := sp.streamXLSXSheetToDatabase(ctx, tx, workbook, sheetName, sheetTables[i], filePath); err != nil {
			return err
		}
	}

	return nil
}

// streamXLSXSheetToDatabase loads one sheet of an open workbook into its table.
//
// The sheet is read into memory first, which it already is: a workbook is a zip
// archive read by random access. So its types are final before its rows are
// inserted, and reading it again is handing out the same chunks.
func (sp *streamProcessor) streamXLSXSheetToDatabase(ctx context.Context, tx *sql.Tx, workbook *reader.Workbook, sheetName, tableName, filePath string) error {
	var chunks []*reader.Chunk
	read, err := workbook.ReadSheet(sheetName, reader.Options{ChunkSize: sp.chunkSize}, func(chunk *reader.Chunk) error {
		chunks = append(chunks, chunk)
		return nil
	})
	if err != nil {
		// A sheet holding nothing a table can be made from is passed over, the
		// way a workbook's blank scratch sheet always has been. Anything else is
		// the file being unreadable, and names the sheet it happened in.
		var readErr *reader.Error
		if errors.As(err, &readErr) && readErr.Kind == reader.KindEmpty {
			sp.logger.Debug("skipping empty sheet", "path", filePath, logKeySheet, sheetName)
			return nil
		}
		sp.logger.Error("failed to read sheet", "path", filePath, logKeySheet, sheetName, "error", err)
		return fmt.Errorf("sheet %s: %w", sheetName, wrapReadError(err))
	}

	sp.logger.Debug("creating table from sheet", "path", filePath, logKeySheet, sheetName, logKeyTable, tableName, "rows", read.Rows)
	if err := sp.refuseExistingTable(ctx, tx, tableName); err != nil {
		return err
	}

	types := columnInfos(read.Header, read.Types)
	headers := newHeader(read.Header)
	emitAll := func(emit chunkProcessor) (columnInfoList, error) {
		for _, chunk := range chunks {
			records := make([]record, len(chunk.Records))
			for i, r := range chunk.Records {
				records[i] = newRecord(r)
			}
			if err := emit(&tableChunk{tableName: tableName, headers: headers, records: records, types: types}); err != nil {
				return nil, err
			}
		}
		return types, nil
	}
	if err := sp.loadTable(ctx, tx, tableName, tableSource{read: emitAll, reread: emitAll}); err != nil {
		return fmt.Errorf("sheet %s: %w", sheetName, err)
	}
	return nil
}
