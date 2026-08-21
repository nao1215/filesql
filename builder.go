package filesql

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"

	"github.com/nao1215/filesql/dialect"
)

// DBBuilder configures and creates database connections from various data sources.
//
// Basic usage:
//
//	builder := filesql.NewBuilder().
//		AddPath("data.csv").
//		AddPath("users.tsv")
//
//	validatedBuilder, err := builder.Build(ctx)
//	if err != nil {
//		return err
//	}
//
//	db, err := validatedBuilder.Open(ctx)
//	defer db.Close()
//
// Supports:
//   - File paths (AddPath)
//   - Embedded filesystems (AddFS)
//   - io.Reader streams (AddReader)
//   - Auto-save functionality (EnableAutoSave)
//   - Read-only mode (OpenReadOnly)
type DBBuilder struct {
	// paths contains regular file paths
	paths []string
	// filesystems contains fs.FS instances
	filesystems []fs.FS
	// readers contains reader configurations
	readers []readerInput
	// collectedPaths contains all paths after Build validation
	collectedPaths []string
	// parsedTables contains tables parsed from streaming readers
	parsedTables []*table
	// autoSaveConfig contains auto-save settings
	autoSaveConfig *autoSaveConfig
	// sqlDialect is the SQL dialect accepted for queries against the opened
	// database. Loading always uses SQLite regardless of this setting. The zero
	// value ("") is treated as dialect.SQLite (no translation).
	sqlDialect dialect.Dialect
	// memDSN is the shared-cache in-memory DSN of the database created by
	// createInMemoryDatabase, used by the dialect connector.
	memDSN string
	// defaultChunkSize is the default chunk size for reading large files (10MB)
	defaultChunkSize int
	// excelSheetPolicy decides which sheets of a workbook this builder loads.
	// The zero value is ExcelSheetPolicyAll. It is mirrored onto the stream
	// processor, which is what the per-source load paths read, the same way the
	// chunk size is.
	excelSheetPolicy ExcelSheetPolicy
	// logger is the logger instance for internal logging
	logger Logger

	// Internal processors for handling different responsibilities
	validator       *validator
	fileProcessor   *fileProcessor
	streamProcessor *streamProcessor
}

// readerInput represents configuration for reading from io.Reader
type readerInput struct {
	// reader is the data source
	reader io.Reader
	// tableName is the name of the table to create
	tableName string
	// fileType specifies the file format using domain/model types
	fileType FileType
	// compression is the codec wrapping the reader, CompressionNone when the
	// bytes are already the format.
	compression CompressionType
	// closer is an optional closer for the reader (set when the reader was opened internally, e.g. from AddFS).
	// User-provided readers (from AddReader) do not set this field.
	closer io.Closer
	// reopen returns the same input read again from its first byte, in the
	// same state of compression as reader, with what to close when done. It is
	// nil for a reader a caller handed over, which can only be read once.
	reopen func() (io.Reader, func() error, error)
}

// ReaderOption configures one reader added with AddReader.
//
// It is the place per-source settings go. A reader has no path, so anything a
// file's name would have answered — which codec wraps it — has to be stated,
// and stating it here keeps AddReader's ordinary three-argument call unchanged.
type ReaderOption func(*readerInput)

// WithCompression declares the codec wrapping a reader passed to AddReader.
//
// Example:
//
//	gz, err := os.Open("users.csv.gz")
//	if err != nil {
//		return err
//	}
//	defer gz.Close()
//
//	builder.AddReader(gz, "users", filesql.FileTypeCSV, filesql.WithCompression(filesql.CompressionGZ))
func WithCompression(compression CompressionType) ReaderOption {
	return func(input *readerInput) {
		input.compression = compression
	}
}

// NewBuilder creates a new database builder.
//
// Start here when you need:
//   - Multiple data sources (files, streams, embedded FS)
//   - Auto-save functionality
//   - Custom chunk sizes for large files
//   - More control than the simple Open() function
//
// Example:
//
//	builder := filesql.NewBuilder().
//		AddPath("data.csv").
//		EnableAutoSave("./backup")
func NewBuilder() *DBBuilder {
	return &DBBuilder{
		paths:            make([]string, 0),
		filesystems:      make([]fs.FS, 0),
		readers:          make([]readerInput, 0),
		collectedPaths:   make([]string, 0),
		parsedTables:     make([]*table, 0),
		autoSaveConfig:   nil, // Default: no auto-save
		defaultChunkSize: DefaultChunkSize,
		logger:           newNopLogger(), // Default: no-op logger

		// Initialize internal processors
		validator:       newValidator(),
		fileProcessor:   newFileProcessor(),
		streamProcessor: newStreamProcessor(DefaultChunkSize),
	}
}

// AddPath adds a file or directory to load.
//
// Examples:
//   - Single file: AddPath("users.csv")
//   - Compressed: AddPath("data.tsv.gz")
//   - Directory: AddPath("/data/") // loads all CSV/TSV/LTSV files
//
// Returns self for chaining.
func (b *DBBuilder) AddPath(path string) *DBBuilder {
	b.paths = append(b.paths, path)
	return b
}

// AddPaths adds multiple files or directories at once.
//
// Example:
//
//	builder.AddPaths("users.csv", "products.tsv", "/data/logs/")
//
// Returns self for chaining.
func (b *DBBuilder) AddPaths(paths ...string) *DBBuilder {
	b.paths = append(b.paths, paths...)
	return b
}

// AddReader adds data from an io.Reader (file, network stream, etc.).
//
// Parameters:
//   - reader: Any io.Reader (file, bytes.Buffer, http.Response.Body, etc.)
//   - tableName: Name for the SQL table (e.g., "users")
//   - fileType: Data format (FileTypeCSV, FileTypeTSV, FileTypeLTSV, etc.)
//   - opts: Per-source settings, such as WithCompression
//
// The reader's bytes are read as fileType directly. When they are wrapped in a
// codec, say so with WithCompression — a reader has no path to infer it from.
//
// Example:
//
//	resp, _ := http.Get("https://example.com/data.csv")
//	builder.AddReader(resp.Body, "remote_data", FileTypeCSV)
//
//	gz, _ := os.Open("users.csv.gz")
//	builder.AddReader(gz, "users", FileTypeCSV, WithCompression(CompressionGZ))
//
// Returns self for chaining.
func (b *DBBuilder) AddReader(reader io.Reader, tableName string, fileType FileType, opts ...ReaderOption) *DBBuilder {
	input := readerInput{
		reader:      reader,
		tableName:   tableName,
		fileType:    fileType,
		compression: CompressionNone,
	}
	for _, opt := range opts {
		opt(&input)
	}
	b.readers = append(b.readers, input)
	return b
}

// SetDefaultChunkSize sets chunk size (number of rows) for large file processing.
//
// Default: 1000 rows. Adjust based on available memory and processing needs.
// A size of zero or less is ignored and the current size is kept.
//
// Example:
//
//	builder.SetDefaultChunkSize(5000) // 5000 rows per chunk
//
// Returns self for chaining.
func (b *DBBuilder) SetDefaultChunkSize(size int) *DBBuilder {
	if size > 0 {
		b.defaultChunkSize = size
		// The processor is what reads in chunks, and it was built with the
		// default before this call. Leaving it to Build would make the option
		// depend on being set before the build, which no other option does.
		b.streamProcessor.setChunkSize(size)
	}
	return b
}

// WithMalformedRowPolicy sets how a CSV/TSV record whose field count differs
// from the header is handled during import.
//
// The default is MalformedRowStop, which aborts the import with an error so a
// corrupt or misaligned file is not imported as partial or empty data. Use
// MalformedRowSkip to drop ragged rows and keep the well-formed ones, or
// MalformedRowFill to keep every short row by padding it with empty strings. A
// long row is refused whichever of the two is chosen, because truncating it
// would discard a cell the file holds without saying so.
//
// The policy reaches delimited text alone. A workbook's rows are checked by the
// XLSX reader itself, which refuses one wider than its header whatever this is
// set to; LTSV pads a missing label; and Parquet and JSON/JSONL carry no
// per-row field count to disagree about.
//
// Example:
//
//	builder.WithMalformedRowPolicy(filesql.MalformedRowSkip)
//
// Returns self for chaining.
func (b *DBBuilder) WithMalformedRowPolicy(policy MalformedRowPolicy) *DBBuilder {
	b.streamProcessor.malformedRowPolicy = policy
	return b
}

// WithExcelSheetPolicy decides which sheets of an Excel workbook this builder
// loads. It applies to every source: a path, a directory, an embedded
// filesystem, a reader, and a compressed workbook alike.
//
// The default is ExcelSheetPolicyAll, which loads every sheet whatever the
// workbook says about showing it. ExcelSheetPolicyVisibleOnly loads only the
// sheets the workbook shows, which is what a tool presenting a workbook to
// someone who did not build it usually wants: a hidden sheet holds the
// spreadsheet's own working-out, and turning that into a queryable table
// surprises the reader.
//
// Table names are worked out after the policy has run, so a hidden sheet that
// would sanitize to the same table as a visible one is not a collision when it
// is not loaded.
//
// Example:
//
//	builder.AddPath("book.xlsx").WithExcelSheetPolicy(filesql.ExcelSheetPolicyVisibleOnly)
//
// Returns self for chaining.
func (b *DBBuilder) WithExcelSheetPolicy(policy ExcelSheetPolicy) *DBBuilder {
	b.excelSheetPolicy = policy
	b.streamProcessor.excelSheetPolicy = policy
	return b
}

// WithLogger sets a custom logger for internal operations.
//
// The logger interface is compatible with slog.Logger. You can use the provided
// SlogAdapter to wrap an existing slog.Logger, or implement your own Logger.
//
// Examples:
//
//	// Using slog
//	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
//	builder.WithLogger(filesql.NewSlogAdapter(logger))
//
//	// Using a custom logger
//	builder.WithLogger(myCustomLogger)
//
// Returns self for chaining.
func (b *DBBuilder) WithLogger(logger Logger) *DBBuilder {
	if logger != nil {
		b.logger = logger
	}
	return b
}

// AddFS adds files from an embedded filesystem (go:embed).
//
// Automatically finds all CSV, TSV, and LTSV files in the filesystem.
//
// Example:
//
//	//go:embed data/*.csv data/*.tsv
//	var dataFS embed.FS
//
//	builder.AddFS(dataFS)
//
// Returns self for chaining.
func (b *DBBuilder) AddFS(filesystem fs.FS) *DBBuilder {
	b.filesystems = append(b.filesystems, filesystem)
	return b
}

// EnableAutoSave automatically saves changes when the database is closed.
//
// Parameters:
//   - outputDir: Where to save files
//   - "" (empty): Overwrite original files
//   - "./backup": Save to backup directory
//
// Example:
//
//	builder.AddPath("data.csv").
//		EnableAutoSave("") // Auto-save to original file on db.Close()
//
// Overwrite mode writes each table back to the file it was loaded from, in that
// file's own format, compression and line terminator; options is ignored,
// because all of those are what the file already has. An output directory is an
// export instead, and writes what options says even when the directory named is
// the one a source was loaded from. Only those files are written: a table created
// during the session has no file to be written back to and is left unsaved, so
// pass an output directory when you want everything in the database on disk. A
// source in a format this package reads but does not write (JSON, JSONL), and a
// workbook of more than one sheet, fail the save rather than being written as
// something else.
//
// The save runs once, when Close returns, so a database with auto-save is as
// safe to share across goroutines as one without it.
//
// Returns self for chaining.
func (b *DBBuilder) EnableAutoSave(outputDir string, options ...DumpOptions) *DBBuilder {
	opts := NewDumpOptions()
	if len(options) > 0 {
		opts = options[0]
	}

	b.autoSaveConfig = &autoSaveConfig{
		enabled:   true,
		timing:    autoSaveOnClose, // Default to close-time saving
		outputDir: outputDir,
		options:   opts,
	}
	return b
}

// EnableAutoSaveOnCommit automatically saves changes after each transaction
// commit, and again when the database is closed.
//
// Use this for real-time persistence. Note: May impact performance.
//
// Example:
//
//	builder.AddPath("data.csv").
//		EnableAutoSaveOnCommit("./output") // Save after each commit
//
// Saving at close as well is what keeps a statement run outside a transaction
// from being lost: it is committed as soon as it runs, but no commit hook sees
// it. This timing therefore saves earlier and more often than EnableAutoSave,
// never less.
//
// Returns self for chaining.
func (b *DBBuilder) EnableAutoSaveOnCommit(outputDir string, options ...DumpOptions) *DBBuilder {
	opts := NewDumpOptions()
	if len(options) > 0 {
		opts = options[0]
	}

	b.autoSaveConfig = &autoSaveConfig{
		enabled:   true,
		timing:    autoSaveOnCommit,
		outputDir: outputDir,
		options:   opts,
	}
	return b
}

// DisableAutoSave disables automatic saving (default behavior).
// Returns the builder for method chaining.
func (b *DBBuilder) DisableAutoSave() *DBBuilder {
	b.autoSaveConfig = nil
	return b
}

// WithDialect sets the SQL dialect accepted by the database returned from Open
// and OpenReadOnly. Queries are translated from the given dialect to SQLite
// before execution; see the dialect package for the supported translations and
// their limitations.
//
// Loading data (CSV/TSV/Parquet/... ingestion) always uses SQLite internally
// regardless of this setting, so only the queries a caller runs are affected.
// The default is dialect.SQLite, which performs no translation.
//
// Constraints (enforced by Build):
//   - The dialect must be a built-in dialect or one registered with
//     dialect.RegisterTranslator.
//   - A non-SQLite dialect cannot be combined with auto-save; the two connector
//     wrappers are not composed in this version.
//
// Example:
//
//	db, err := filesql.NewBuilder().
//		AddPath("users.csv").
//		WithDialect(dialect.PostgreSQL).
//		Build(ctx)
//	// ... db.Query("SELECT name::text FROM users WHERE name ILIKE 'a%'")
//
// Returns the builder for method chaining.
func (b *DBBuilder) WithDialect(d dialect.Dialect) *DBBuilder {
	b.sqlDialect = d
	return b
}

// usesDialectTranslation reports whether a non-SQLite dialect is configured.
func (b *DBBuilder) usesDialectTranslation() bool {
	return b.sqlDialect != "" && b.sqlDialect != dialect.SQLite
}

// Build validates all configured inputs and prepares the builder for opening a database.
// This method must be called before Open(). It performs the following operations:
//
// 1. Validates that at least one input source is configured
// 2. Checks existence and format of all file paths
// 3. Processes embedded filesystems by converting files to streaming readers
// 4. Validates that all files have supported extensions
//
// After successful validation, the builder is ready to create database connections
// with Open(). The context is used for file operations and can be used for cancellation.
//
// Returns the same builder instance for method chaining, or an error if validation fails.
func (b *DBBuilder) Build(ctx context.Context) (*DBBuilder, error) {
	b.logger.Debug("starting build", "paths", len(b.paths), "filesystems", len(b.filesystems), "readers", len(b.readers))

	// Validate that we have at least one input
	if len(b.paths) == 0 && len(b.filesystems) == 0 && len(b.readers) == 0 {
		return nil, fmt.Errorf("%w: at least one path must be provided", ErrNoFiles)
	}

	if err := ctx.Err(); err != nil {
		return nil, err
	}

	// Use validator to validate auto-save config
	if err := b.validator.validateAutoSaveConfig(b.autoSaveConfig); err != nil {
		return nil, err
	}

	// Validate the SQL dialect and its constraints.
	if err := b.validateDialect(); err != nil {
		return nil, err
	}

	// Use file processor to collect paths
	collectedPaths, err := b.fileProcessor.collectFilesFromPaths(b.paths)
	if err != nil {
		return nil, err
	}
	b.collectedPaths = collectedPaths

	// Use file processor to handle filesystems
	fsReaders, err := b.fileProcessor.processFilesystemsToReaders(ctx, b.filesystems)
	if err != nil {
		return nil, err
	}
	b.readers = append(b.readers, fsReaders...)

	// Use validator to validate reader inputs
	for _, readerInput := range b.readers {
		if err := b.validator.validateReader(readerInput.reader, readerInput.tableName, readerInput.fileType); err != nil {
			return nil, err
		}
	}

	// Use validator to validate final state
	if err := b.validator.validateFinalState(b.collectedPaths, b.readers, b.paths); err != nil {
		return nil, err
	}

	// Pass logger to internal processors
	b.streamProcessor.setLogger(b.logger)
	b.fileProcessor.setLogger(b.logger)

	b.logger.Info("build completed", "collected_paths", len(b.collectedPaths), "readers", len(b.readers))
	return b, nil
}

// SkippedRows reports what WithMalformedRowPolicy(MalformedRowSkip) discarded
// during the loads this builder has performed, one entry per table that lost
// rows. A load that dropped nothing is not listed, so a non-empty result is
// always something worth telling a user about.
//
// Skipping is an instruction from the caller, but an instruction that reports
// nothing leaves one dropped row and most of the file dropped looking exactly
// alike — and a write-back afterwards makes either one permanent. The counts
// are what lets a caller say which of the two happened before that.
//
// It is valid after Open, OpenContext, LoadInto, or LoadIntoTx have run.
func (b *DBBuilder) SkippedRows() []SkippedRows {
	return b.streamProcessor.skippedRows()
}

// Open creates and returns a database connection using the configured and validated inputs.
// This method can only be called after Build() has been successfully executed.
// It creates an in-memory SQLite database and loads all configured files as tables using streaming.
//
// Table names are derived from file names without extensions:
// - "users.csv" becomes table "users"
// - "data.tsv.gz" becomes table "data"
// - "user-data.csv" becomes table "user_data" (hyphens become underscores)
// - "my file.csv" becomes table "my_file" (spaces become underscores)
//
// Special characters in file names are automatically sanitized for SQL safety.
//
// The returned database connection supports the full SQLite3 SQL syntax.
// Auto-save functionality is supported for both file paths and reader inputs.
// The caller is responsible for closing the connection when done.
//
// Returns a *sql.DB connection or an error if the database cannot be created.
func (b *DBBuilder) Open(ctx context.Context) (*sql.DB, error) {
	b.logger.Debug("opening database")

	// Use validator to validate inputs availability
	if err := b.validator.validateInputsAvailable(b.collectedPaths, b.readers); err != nil {
		return nil, err
	}

	if err := ctx.Err(); err != nil {
		return nil, err
	}

	// Use file processor to deduplicate compressed files
	b.collectedPaths = b.fileProcessor.deduplicateCompressedFiles(b.collectedPaths)

	b.logger.Debug("creating in-memory database")
	db, err := b.createInMemoryDatabase()
	if err != nil {
		b.logger.Error("failed to create database", "error", err)
		return nil, err
	}

	// Use stream processor for all streaming operations (now includes XLSX support)
	if err := b.streamProcessor.streamAllFilesToDatabase(ctx, db, b.collectedPaths); err != nil {
		b.logger.Error("failed to stream files", "error", err)
		_ = db.Close() // Ignore close error during error handling
		return nil, err
	}

	if err := b.streamProcessor.streamAllReadersToDatabase(ctx, db, b.readers); err != nil {
		b.logger.Error("failed to stream readers", "error", err)
		_ = db.Close() // Ignore close error during error handling
		return nil, err
	}

	if err := b.validateDatabaseConnection(ctx, db); err != nil {
		_ = db.Close() // Ignore close error during error handling
		return nil, err
	}

	db, err = b.setupAutoSaveIfNeeded(ctx, db)
	if err != nil {
		return nil, err
	}

	db, err = b.setupDialectIfNeeded(ctx, db)
	if err != nil {
		return nil, err
	}

	b.logger.Info("database opened successfully")
	return db, nil
}

// validateDialect checks the configured SQL dialect and rejects unsupported
// combinations. It is a no-op for the default (SQLite) dialect.
func (b *DBBuilder) validateDialect() error {
	if !b.usesDialectTranslation() {
		return nil
	}
	// The dialect must be translatable: either a probe translation succeeds or a
	// custom translator is registered. Translate of a trivial query surfaces
	// dialect.ErrUnknownDialect for an unrecognized dialect.
	if _, err := dialect.Translate(b.sqlDialect, "SELECT 1"); err != nil {
		if errors.Is(err, dialect.ErrUnknownDialect) {
			return fmt.Errorf("%w: unknown SQL dialect %q", ErrDatabaseOperation, b.sqlDialect)
		}
		return fmt.Errorf("%w: dialect %q is not usable: %w", ErrDatabaseOperation, b.sqlDialect, err)
	}
	if b.autoSaveConfig != nil && b.autoSaveConfig.enabled {
		return fmt.Errorf("%w: WithDialect(%s) cannot be combined with auto-save", ErrDatabaseOperation, b.sqlDialect)
	}
	return nil
}

// OpenReadOnly creates a read-only database connection.
// This is a convenience method that calls Open() and wraps the result in a ReadOnlyDB.
// All SELECT queries work normally, but write operations return ErrReadOnly.
//
// This is useful for audit scenarios where you want to query data without
// risk of accidental modification.
//
// Example:
//
//	builder := filesql.NewBuilder().
//		AddPath("payment.ach")
//
//	validatedBuilder, err := builder.Build(ctx)
//	if err != nil {
//		return err
//	}
//
//	rodb, err := validatedBuilder.OpenReadOnly(ctx)
//	if err != nil {
//		return err
//	}
//	defer rodb.Close()
//
//	// SELECT works
//	rows, _ := rodb.Query("SELECT * FROM payment_entries")
//
//	// Write operations are rejected
//	_, err = rodb.Exec("DELETE FROM payment_entries") // returns ErrReadOnly
func (b *DBBuilder) OpenReadOnly(ctx context.Context) (*ReadOnlyDB, error) {
	db, err := b.Open(ctx)
	if err != nil {
		return nil, err
	}
	return NewReadOnlyDB(db), nil
}

// LoadInto loads the builder's configured inputs into an existing database
// instead of creating a new in-memory one as Open does. Use it to combine
// file-derived tables with a caller-managed database, such as a long-lived
// session or a database that already holds other tables, without copying the
// data through a second database.
//
// Semantics:
//   - A table whose name matches a loaded file or sheet is replaced (dropped and
//     recreated), so reloading a file is idempotent and same-named inputs are
//     last-wins. Other tables in db are left untouched.
//   - The caller keeps ownership of db. LoadInto never closes it, even on error.
//   - Each input is loaded atomically: a file that fails partway leaves the
//     database as it was, including the table it was replacing. Inputs are not
//     atomic with respect to each other, so when the third of three files fails
//     the first two are loaded. Use LoadIntoTx when the whole set has to land or
//     none of it.
//   - For an in-memory database, pin the pool to one connection
//     (db.SetMaxOpenConns(1)). SQLite ":memory:" is private per connection, so a
//     multi-connection pool would not share the loaded tables.
//
// Auto-save is not supported because the caller owns the database lifecycle;
// configuring it returns an error.
//
// Example:
//
//	db, _ := sql.Open("sqlite", ":memory:")
//	db.SetMaxOpenConns(1)
//	builder, err := filesql.NewBuilder().AddPath("users.csv").Build(ctx)
//	if err != nil {
//		return err
//	}
//	if err := builder.LoadInto(ctx, db); err != nil {
//		return err
//	}
//	// db now has a "users" table alongside any tables it already had.
func (b *DBBuilder) LoadInto(ctx context.Context, db *sql.DB) error {
	if db == nil {
		return fmt.Errorf("%w: target database is nil", ErrDatabaseOperation)
	}
	if b.autoSaveConfig != nil && b.autoSaveConfig.enabled {
		return fmt.Errorf("%w: auto-save is not supported by LoadInto; the caller owns the database lifecycle", ErrDatabaseOperation)
	}

	if err := b.validator.validateInputsAvailable(b.collectedPaths, b.readers); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	b.collectedPaths = b.fileProcessor.deduplicateCompressedFiles(b.collectedPaths)

	// Replace same-named tables so loading into an existing database is last-wins
	// rather than erroring on or appending to a pre-existing table. Reset
	// afterward so reusing the builder (e.g. a later Open) is not affected.
	b.streamProcessor.replaceExisting = true
	defer func() { b.streamProcessor.replaceExisting = false }()

	return b.loadIntoExecutor(ctx, db)
}

// LoadIntoTx loads the builder's inputs into an existing transaction. No
// transaction is started or committed by this method; the caller owns the
// transaction and decides whether all changes are committed or rolled back.
// This is intended for callers that need several files, including table
// replacement and empty-table creation, to be one atomic operation.
// Write-back metadata for ACH and Fedwire files is written inside tx, so a
// rollback discards it along with the tables it describes.
func (b *DBBuilder) LoadIntoTx(ctx context.Context, tx *sql.Tx) error {
	if tx == nil {
		return fmt.Errorf("%w: target transaction is nil", ErrDatabaseOperation)
	}
	if b.autoSaveConfig != nil && b.autoSaveConfig.enabled {
		return fmt.Errorf("%w: auto-save is not supported by LoadIntoTx", ErrDatabaseOperation)
	}
	if err := b.validator.validateInputsAvailable(b.collectedPaths, b.readers); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	b.collectedPaths = b.fileProcessor.deduplicateCompressedFiles(b.collectedPaths)
	b.streamProcessor.replaceExisting = true
	defer func() { b.streamProcessor.replaceExisting = false }()
	return b.loadIntoExecutor(ctx, tx)
}

func (b *DBBuilder) loadIntoExecutor(ctx context.Context, db DBTX) error {
	if err := b.streamProcessor.streamAllFilesToDatabase(ctx, db, b.collectedPaths); err != nil {
		return err
	}
	return b.streamProcessor.streamAllReadersToDatabase(ctx, db, b.readers)
}

// deduplicateCompressedFiles removes compressed duplicates when uncompressed versions exist.
// DEPRECATED: This method has been moved to fileProcessor.deduplicateCompressedFiles()
// createInMemoryDatabase creates a new in-memory SQLite database connection.
func (b *DBBuilder) createInMemoryDatabase() (*sql.DB, error) {
	// Use a uniquely-named shared-cache in-memory database rather than a bare
	// ":memory:" connection. A bare ":memory:" database is private to a single
	// connection, which forced earlier versions to reuse one connection for the
	// whole pool and made the returned *sql.DB unsafe to use from multiple
	// goroutines. With "mode=memory&cache=shared" every pooled connection opens
	// its own real connection to the same in-memory database, so database/sql
	// can serialize access per connection: the result is safe to share across
	// goroutines and still supports queries issued while iterating rows.
	name, err := randomMemoryDBName()
	if err != nil {
		return nil, fmt.Errorf("%w: failed to name in-memory database: %w", ErrDatabaseOperation, err)
	}
	dsn := fmt.Sprintf("file:%s?mode=memory&cache=shared", name)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("%w: failed to create in-memory database: %w", ErrDatabaseOperation, err)
	}
	// Remember the DSN so a dialect-translating connector can open its own
	// connections to the same shared-cache in-memory database (see
	// setupDialectIfNeeded).
	b.memDSN = dsn
	// A shared-cache in-memory database is discarded once its last connection
	// closes. Disable idle timeouts so the pool keeps at least one connection
	// (and therefore the data) alive until the caller closes the *sql.DB.
	db.SetConnMaxIdleTime(0)
	db.SetConnMaxLifetime(0)
	return db, nil
}

// randomMemoryDBName returns a process-unique name for a shared-cache in-memory
// SQLite database. A random name keeps separate filesql databases from sharing
// the same in-memory cache within a process.
func randomMemoryDBName() (string, error) {
	var buf [16]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return "", err
	}
	return "filesql_mem_" + hex.EncodeToString(buf[:]), nil
}

// validateDatabaseConnection validates the database connection is working.
func (b *DBBuilder) validateDatabaseConnection(ctx context.Context, db *sql.DB) error {
	if err := db.PingContext(ctx); err != nil {
		closeErr := db.Close()

		var allErrors []error
		allErrors = append(allErrors, err)
		if closeErr != nil {
			allErrors = append(allErrors, fmt.Errorf("failed to close database: %w", closeErr))
		}

		return errors.Join(allErrors...)
	}
	return nil
}

// setupAutoSaveIfNeeded swaps the plain loader database for one whose
// connections run the save when the caller closes it. It is a no-op when
// auto-save is off.
//
// The loaded data lives in a shared-cache in-memory database, so the auto-save
// database opens its own connections to the same DSN rather than loading the
// files a second time. A live connection is established before the loader
// database is closed, so the shared cache—and the data—survives the swap.
func (b *DBBuilder) setupAutoSaveIfNeeded(ctx context.Context, db *sql.DB) (*sql.DB, error) {
	if b.autoSaveConfig == nil || !b.autoSaveConfig.enabled {
		return db, nil
	}
	if b.memDSN == "" {
		_ = db.Close()
		return nil, fmt.Errorf("%w: auto-save requires the in-memory database DSN", ErrDatabaseOperation)
	}

	connector := &autoSaveConnector{
		drv:            db.Driver(),
		dsn:            b.memDSN,
		autoSaveConfig: b.autoSaveConfig,
		originalPaths:  b.collectOriginalPaths(),
	}
	// The connector's own connection is what keeps the shared-cache database
	// alive between pooled connections, so it has to exist before the loader
	// database closes.
	if err := connector.open(); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("%w: failed to open auto-save database: %w", ErrDatabaseOperation, err)
	}

	adb := sql.OpenDB(connector)
	if err := adb.PingContext(ctx); err != nil {
		_ = adb.Close()
		_ = db.Close()
		return nil, fmt.Errorf("%w: failed to open auto-save database: %w", ErrDatabaseOperation, err)
	}
	if err := db.Close(); err != nil {
		_ = adb.Close()
		return nil, fmt.Errorf("%w: failed to close intermediate database: %w", ErrDatabaseOperation, err)
	}

	// Everything that could still discard the data is behind us, so a close from
	// here on is the caller's and has to save.
	connector.arm()
	return adb, nil
}

// setupDialectIfNeeded swaps the plain loader database for one whose queries are
// translated from the configured dialect to SQLite. It is a no-op for the
// default (SQLite) dialect.
//
// The loaded data lives in a shared-cache in-memory database, so the translating
// database can open its own connection to the same DSN. A live connection is
// established (via Ping) before the loader database is closed, so the shared
// cache—and the data—survives the swap.
func (b *DBBuilder) setupDialectIfNeeded(ctx context.Context, db *sql.DB) (*sql.DB, error) {
	if !b.usesDialectTranslation() {
		return db, nil
	}
	if b.memDSN == "" {
		_ = db.Close()
		return nil, fmt.Errorf("%w: dialect translation requires the in-memory database DSN", ErrDatabaseOperation)
	}

	// Register the dialect helper UDFs before opening the connection that runs
	// translated queries; modernc exposes functions only to connections opened
	// afterward.
	if err := dialect.RegisterFunctions(); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("%w: failed to register dialect functions: %w", ErrDatabaseOperation, err)
	}

	// Open translated connections through the same driver instance the loader
	// used, so the dialect helper functions registered above are visible.
	tdb := sql.OpenDB(&dialectConnector{drv: db.Driver(), dsn: b.memDSN, sqlDialect: b.sqlDialect})
	tdb.SetConnMaxIdleTime(0)
	tdb.SetConnMaxLifetime(0)

	if err := tdb.PingContext(ctx); err != nil {
		_ = tdb.Close()
		_ = db.Close()
		return nil, fmt.Errorf("%w: failed to open dialect database: %w", ErrDatabaseOperation, err)
	}

	if err := db.Close(); err != nil {
		_ = tdb.Close()
		return nil, fmt.Errorf("%w: failed to close loader database: %w", ErrDatabaseOperation, err)
	}
	return tdb, nil
}

// collectOriginalPaths collects original file paths for overwrite mode
func (b *DBBuilder) collectOriginalPaths() []string {
	paths := make([]string, 0, len(b.collectedPaths))
	paths = append(paths, b.collectedPaths...)
	return paths
}
