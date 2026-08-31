package filesql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"github.com/nao1215/filesql/internal/codec"
	"github.com/nao1215/filesql/internal/reader"
	"modernc.org/sqlite"
)

// directConnector implements driver.Connector to wrap an existing driver.Conn
type directConnector struct {
	conn driver.Conn
}

func (dc *directConnector) Connect(_ context.Context) (driver.Conn, error) {
	return dc.conn, nil
}

func (dc *directConnector) Driver() driver.Driver {
	return &sqlite.Driver{}
}

// OutputFormat represents the output file format
type OutputFormat int

const (
	// OutputFormatCSV represents CSV output format
	OutputFormatCSV OutputFormat = iota
	// OutputFormatTSV represents TSV output format
	OutputFormatTSV
	// OutputFormatLTSV represents LTSV output format
	OutputFormatLTSV
	// OutputFormatParquet represents Parquet output format
	OutputFormatParquet
	// OutputFormatXLSX represents Excel XLSX output format
	OutputFormatXLSX
	// OutputFormatACH represents ACH (NACHA) output format
	OutputFormatACH
	// OutputFormatFedWire represents Fedwire output format
	OutputFormatFedWire
)

// String returns the string representation of OutputFormat
func (f OutputFormat) String() string {
	switch f {
	case OutputFormatCSV:
		return formatCSVStr
	case OutputFormatTSV:
		return formatTSVStr
	case OutputFormatLTSV:
		return formatLTSVStr
	case OutputFormatParquet:
		return formatParquetStr
	case OutputFormatXLSX:
		return formatXLSXStr
	case OutputFormatACH:
		return formatACHStr
	case OutputFormatFedWire:
		return formatFedWireStr
	default:
		return formatCSVStr
	}
}

// Extension returns the file extension for the format
func (f OutputFormat) Extension() string {
	switch f {
	case OutputFormatCSV:
		return extCSV
	case OutputFormatTSV:
		return extTSV
	case OutputFormatLTSV:
		return extLTSV
	case OutputFormatParquet:
		return extParquet
	case OutputFormatXLSX:
		return extXLSX
	case OutputFormatACH:
		return extACH
	case OutputFormatFedWire:
		return extFED
	default:
		return extCSV
	}
}

// CompressionType represents the compression type
type CompressionType int

const (
	// CompressionNone represents no compression
	CompressionNone CompressionType = iota
	// CompressionGZ represents gzip compression
	CompressionGZ
	// CompressionBZ2 represents bzip2 compression
	CompressionBZ2
	// CompressionXZ represents xz compression
	CompressionXZ
	// CompressionZSTD represents zstd compression
	CompressionZSTD
	// CompressionZLIB represents zlib compression
	CompressionZLIB
	// CompressionSNAPPY represents snappy compression
	CompressionSNAPPY
	// CompressionS2 represents s2 compression
	CompressionS2
	// CompressionLZ4 represents lz4 compression
	CompressionLZ4
)

// string constants for output format names
const (
	formatCSVStr     = "csv"
	formatTSVStr     = "tsv"
	formatLTSVStr    = "ltsv"
	formatParquetStr = "parquet"
	formatXLSXStr    = "xlsx"
	formatACHStr     = "ach"
	formatFedWireStr = "fed"
)

// String returns the string representation of CompressionType
func (c CompressionType) String() string {
	return codec.Codec(c).String()
}

// Extension returns the file extension for the compression type
func (c CompressionType) Extension() string {
	return codec.Codec(c).Extension()
}

// DumpOptions configures how database tables are exported to files.
//
// Example:
//
//	options := NewDumpOptions().
//		WithFormat(OutputFormatTSV).
//		WithCompression(CompressionGZ)
//
//	err := DumpDatabase(db, "./output", options)
type DumpOptions struct {
	// Format specifies the output file format
	Format OutputFormat
	// Compression specifies the compression type
	Compression CompressionType
	// Encoding specifies the text encoding of csv, tsv, and ltsv output. It has
	// no effect on Parquet and XLSX, which carry their own.
	Encoding Encoding
	// LineEnding specifies the line terminator of csv, tsv, and ltsv output. It
	// has no effect on Parquet and XLSX, which are not line-based.
	LineEnding LineEnding
}

// NewDumpOptions creates default export options (CSV, no compression).
//
// Modify with:
//   - WithFormat(): Change file format (CSV, TSV, LTSV)
//   - WithCompression(): Add compression (GZ, BZ2, XZ, ZSTD)
func NewDumpOptions() DumpOptions {
	return DumpOptions{
		Format:      OutputFormatCSV,
		Compression: CompressionNone,
		Encoding:    EncodingUTF8,
		LineEnding:  LineEndingLF,
	}
}

// WithFormat sets the output file format.
//
// Options:
//   - OutputFormatCSV: Comma-separated values
//   - OutputFormatTSV: Tab-separated values
//   - OutputFormatLTSV: Labeled tab-separated values
//   - OutputFormatParquet: Apache Parquet columnar format
func (o DumpOptions) WithFormat(format OutputFormat) DumpOptions {
	o.Format = format
	return o
}

// WithCompression adds compression to output files.
//
// Options:
//   - CompressionNone: No compression (default)
//   - CompressionGZ: Gzip compression (.gz)
//   - CompressionBZ2: Bzip2 compression (.bz2) - read only, writing not supported
//   - CompressionXZ: XZ compression (.xz)
//   - CompressionZSTD: Zstandard compression (.zst)
//   - CompressionZLIB: Zlib compression (.z)
//   - CompressionSNAPPY: Snappy compression (.snappy)
//   - CompressionS2: S2 compression (.s2) - Snappy compatible
//   - CompressionLZ4: LZ4 compression (.lz4)
func (o DumpOptions) WithCompression(compression CompressionType) DumpOptions {
	o.Compression = compression
	return o
}

// WithEncoding sets the text encoding of csv, tsv, and ltsv output.
//
// It exists so a caller that decoded a legacy source before loading can write
// one back: without it every save produced UTF-8, so an in-place save changed
// the file's encoding on disk and the caller's next read of the same file
// returned mojibake.
//
// filesql reads UTF-8 only, so a file written in another encoding is for other
// tools rather than for loading back.
//
// A value the encoding cannot write fails the save with ErrEncoding, naming the
// encoding, rather than being replaced — a substitution is the silent corruption
// the read side already refuses. Parquet and XLSX carry their own encoding and
// are unaffected.
//
// Options:
//   - EncodingUTF8: UTF-8 (default)
//   - EncodingShiftJIS: Shift-JIS (CP932)
//   - EncodingEUCJP: EUC-JP
//   - EncodingISO2022JP: ISO-2022-JP
//   - EncodingUTF16LE: UTF-16 little-endian, with a byte-order mark
//   - EncodingUTF16BE: UTF-16 big-endian, with a byte-order mark
func (o DumpOptions) WithEncoding(enc Encoding) DumpOptions {
	o.Encoding = enc
	return o
}

// WithLineEnding sets the line terminator of csv, tsv, and ltsv output.
//
// It exists for the same reason WithEncoding does: a save wrote "\n" whatever
// the source used, so a CRLF file saved in place came back LF throughout — every
// line of the file changed although the caller had edited one row. Writing back
// in place, which is EnableAutoSave with an empty output directory, detects the
// file's own terminator and keeps it. Every other save is an export and writes
// what it is told, including one aimed at the directory a source came from, so
// this option is how an export is asked for CRLF.
//
// Options:
//   - LineEndingLF: "\n" (default)
//   - LineEndingCRLF: "\r\n"
//
// Parquet and XLSX are not line-based and are unaffected.
func (o DumpOptions) WithLineEnding(lineEnding LineEnding) DumpOptions {
	o.LineEnding = lineEnding
	return o
}

// FileExtension returns the complete file extension including compression
func (o DumpOptions) FileExtension() string {
	baseExt := o.Format.Extension()
	compExt := o.Compression.Extension()
	return baseExt + compExt
}

// autoSaveTiming defines when auto-save should be triggered
type autoSaveTiming int

const (
	// autoSaveOnClose saves data when db.Close() is called (default)
	autoSaveOnClose autoSaveTiming = iota
	// autoSaveOnCommit saves data when transaction is committed
	autoSaveOnCommit
)

// autoSaveConfig holds configuration for automatic saving
type autoSaveConfig struct {
	// enabled indicates whether auto-save is enabled
	enabled bool
	// timing specifies when to save (on close or on commit)
	timing autoSaveTiming
	// outputDir is the directory where files will be saved. Empty means overwrite
	// mode, where each table goes back to the file it came from and options is
	// unused, because the format is the one that file already has.
	outputDir string
	// options contains dump options for formatting
	options DumpOptions
}

// autoSaveConnector implements driver.Connector interface with auto-save support.
//
// Every connection it hands out is a real connection of its own, opened against
// the shared-cache in-memory database the files were loaded into, so the pool
// behaves the way it does without auto-save: connections are independent and the
// database is safe to share across goroutines. The connector counts them so the
// save runs once, through the last connection still open, which is the moment
// the caller closes the database.
type autoSaveConnector struct {
	drv            driver.Driver
	dsn            string
	autoSaveConfig *autoSaveConfig
	originalPaths  []string
	// readOnly reports whether the handle these connections belong to refuses
	// writes already, which is what a read-only transaction would otherwise set.
	readOnly bool
	// gate is the queue this handle's transactions and statements wait in. The
	// anchor connection is deliberately not in it: a save reads, and a read
	// runs beside the other reads rather than behind them.
	gate *txGate

	mu sync.Mutex
	// anchor is a connection of the connector's own. A shared-cache in-memory
	// database is discarded once its last connection closes, and the pool is
	// free to close a connection whenever it likes, so the anchor is what keeps
	// the data alive between pooled connections and what the save reads from.
	anchor driver.Conn
	// saveMu serializes the saves themselves. A save reads the whole database
	// through the anchor connection, and a SQLite connection carries one
	// statement at a time, so two goroutines committing at once drove statements
	// into the same connection with nothing between them: the pair hung, and
	// sometimes faulted inside the SQLite library. It is separate from mu, which
	// only guards the fields, so a save never holds mu while it runs.
	saveMu sync.Mutex
	// armed reports whether a close has to save. It is set once the database is
	// fully assembled, so a connection opened by a setup that then fails does
	// not write out what that failure is about to discard.
	armed bool
	// openTx counts the transactions the caller has begun and not yet finished.
	// A save reads every table, and a transaction that has written holds the
	// lock on the table it touched, so a save that starts while one is open
	// waits for a lock that only the caller can release -- and the caller is
	// inside Close. That wait has no deadline and no context, so it never ends.
	// Counting the transactions is what lets Close return instead.
	openTx int
}

// Connect implements driver.Connector interface
func (c *autoSaveConnector) Connect(_ context.Context) (driver.Conn, error) {
	conn, err := c.drv.Open(c.dsn)
	if err != nil {
		return nil, err
	}
	return &guardedConn{conn: conn, readOnly: c.readOnly, tracker: c, gate: c.gate}, nil
}

// Driver implements driver.Connector interface
func (c *autoSaveConnector) Driver() driver.Driver {
	return c.drv
}

// open establishes the anchor connection. It has to succeed before the loader
// database is closed, or the data goes with it.
func (c *autoSaveConnector) open() error {
	conn, err := c.drv.Open(c.dsn)
	if err != nil {
		return err
	}
	c.mu.Lock()
	c.anchor = conn
	c.mu.Unlock()
	return nil
}

// arm allows a later close to save.
func (c *autoSaveConnector) arm() {
	c.mu.Lock()
	c.armed = true
	c.mu.Unlock()
}

// Close implements io.Closer, which database/sql calls once, when the caller
// closes the database and after it has closed every pooled connection. That
// makes it the one moment where saving is both correct and unambiguous: the
// caller is done, and the anchor connection still holds the data.
func (c *autoSaveConnector) Close() error {
	c.mu.Lock()
	anchor, armed, open := c.anchor, c.armed, c.openTx
	c.anchor, c.armed = nil, false
	c.mu.Unlock()

	if anchor == nil {
		return nil
	}
	var saveErr error
	switch {
	case !armed:
	case open > 0:
		// The database is closing with work the caller has neither committed
		// nor rolled back, so what is in it is not a state they asked to keep.
		// Saying so is the whole of the fix: reading it here would wait on a
		// lock that nothing left running can release.
		saveErr = fmt.Errorf("%w: auto-save was skipped because the database was closed while a transaction was still open; commit or roll back before closing",
			ErrDatabaseOperation)
	default:
		if err := c.save(anchor); err != nil {
			saveErr = fmt.Errorf("auto-save failed: %w", err)
		}
	}
	closeErr := anchor.Close()
	if saveErr != nil {
		if closeErr != nil {
			return fmt.Errorf("%w (also failed to close connection: %w)", saveErr, closeErr)
		}
		return saveErr
	}
	return closeErr
}

// transactionBegan and transactionEnded keep the count Close reads. A
// transaction the caller started through database/sql always reaches one of
// Commit and Rollback, so the count returns to zero on its own; one begun by
// running BEGIN as a statement is counted by the connection that ran it, and
// stays counted, because closing that connection rolls it back rather than
// finishing it.
func (c *autoSaveConnector) transactionBegan() {
	c.mu.Lock()
	c.openTx++
	c.mu.Unlock()
}

func (c *autoSaveConnector) transactionEnded() {
	c.mu.Lock()
	if c.openTx > 0 {
		c.openTx--
	}
	c.mu.Unlock()
}

// transactionCommitted runs the save a committed transaction owes when the
// timing asks for one.
func (c *autoSaveConnector) transactionCommitted() error {
	if !c.savesOnCommit() {
		return nil
	}
	if err := c.saveNow(); err != nil {
		// The transaction is already committed, so the rows are kept and only
		// the file is out of date; saying which is what lets a caller decide.
		return fmt.Errorf("transaction committed successfully, but auto-save failed: %w", err)
	}
	return nil
}

// savesOnCommit reports whether a committed transaction has to save.
func (c *autoSaveConnector) savesOnCommit() bool {
	return c.autoSaveConfig != nil && c.autoSaveConfig.enabled && c.autoSaveConfig.timing == autoSaveOnCommit
}

// saveNow writes the database out through the anchor connection, for a caller
// that is not the one tearing the connector down.
func (c *autoSaveConnector) saveNow() error {
	c.mu.Lock()
	anchor := c.anchor
	c.mu.Unlock()
	if anchor == nil {
		return nil
	}
	return c.save(anchor)
}

// save executes automatic saving using the configured settings, reading the
// database through conn. One save runs at a time; see saveMu.
func (c *autoSaveConnector) save(conn driver.Conn) error {
	if c.autoSaveConfig == nil || !c.autoSaveConfig.enabled {
		return nil // No auto-save configured
	}

	c.saveMu.Lock()
	defer c.saveMu.Unlock()

	// Read the database through the given connection. Closing tempDB would close
	// that connection, which the caller still owns, so the pool is left to be
	// collected with the connector.
	tempDB := sql.OpenDB(&directConnector{conn: conn})

	outputDir := c.autoSaveConfig.outputDir
	if outputDir == "" {
		// Overwrite mode - save to original file locations
		return c.overwriteOriginalFiles(tempDB)
	}

	// Use the configured DumpOptions directly
	dumpOptions := c.autoSaveConfig.options

	// Handle ACH format specially - need to export ACH files separately
	if dumpOptions.Format == OutputFormatACH {
		return c.performACHAutoSave(tempDB, outputDir)
	}

	// Handle Fedwire format specially - need to export Fedwire files separately
	if dumpOptions.Format == OutputFormatFedWire {
		return c.performFedWireAutoSave(tempDB, outputDir)
	}

	// Use the existing DumpDatabase method for other formats
	return DumpDatabase(tempDB, outputDir, dumpOptions)
}

// performACHAutoSave saves all ACH tables back to ACH files
func (c *autoSaveConnector) performACHAutoSave(db *sql.DB, outputDir string) error {
	ctx := context.Background()

	// The database records the ACH files it was loaded from.
	achBaseNames := fileSourceBaseNames(ctx, db, sourceFormatACH)
	if len(achBaseNames) == 0 {
		return errors.New("no ACH tables found to save")
	}

	// Create output directory if it doesn't exist
	if err := os.MkdirAll(outputDir, 0750); err != nil {
		return fmt.Errorf("failed to create output directory: %w", err)
	}

	// Export each ACH file
	for _, baseName := range achBaseNames {
		outputPath := filepath.Join(outputDir, baseName+".ach")
		if err := DumpACH(ctx, db, baseName, outputPath); err != nil {
			return fmt.Errorf("failed to export ACH file %s: %w", baseName, err)
		}
	}

	return nil
}

// performFedWireAutoSave saves all Fedwire tables back to Fedwire files
func (c *autoSaveConnector) performFedWireAutoSave(db *sql.DB, outputDir string) error {
	ctx := context.Background()

	wireBaseNames := fileSourceBaseNames(ctx, db, sourceFormatFedWire)
	if len(wireBaseNames) == 0 {
		return errors.New("no Fedwire tables found to save")
	}

	if err := os.MkdirAll(outputDir, 0750); err != nil {
		return fmt.Errorf("failed to create output directory: %w", err)
	}

	for _, baseName := range wireBaseNames {
		outputPath := filepath.Join(outputDir, baseName+".fed")
		if err := DumpFedWire(ctx, db, baseName, outputPath); err != nil {
			return fmt.Errorf("failed to export Fedwire file %s: %w", baseName, err)
		}
	}

	return nil
}

// overwriteOriginalFiles saves each table back to the file it was loaded from.
//
// Every file is written in its own format and its own compression, taken from
// its path, and to that exact path. The ACH and Fedwire branches always worked
// this way; the tabular ones handed the whole database to DumpDatabase with one
// output directory and one format from the auto-save options, which defaults to
// CSV. A .tsv source therefore got a new .csv beside it holding the change while
// the .tsv the caller asked to overwrite still held the old rows, sources in
// different directories all landed next to whichever was loaded first, and a
// table the caller created was written out as a file of its own.
//
// A source whose format has no writer, or which holds more than one table, fails
// the save rather than turning into something else on disk.
func (c *autoSaveConnector) overwriteOriginalFiles(db *sql.DB) error {
	if len(c.originalPaths) == 0 {
		return errors.New("no original paths available for overwrite")
	}

	// Nothing is replaced until every source is known to be writable. The loop
	// below wrote them one at a time and stopped at the first it could not
	// write, so a set holding one of those came out of a failed save with its
	// earlier files carrying the session's rows and the rest carrying the old
	// ones, and nothing on disk saying which was which.
	if err := checkOverwriteTargets(c.originalPaths); err != nil {
		return err
	}

	ctx := context.Background()

	for _, path := range c.originalPaths {
		if err := c.overwriteOriginalFile(ctx, db, path); err != nil {
			return err
		}
	}

	return nil
}

// overwriteOriginalFile writes the table or tables path was loaded as back to
// path, keeping the format and compression the file already has.
func (c *autoSaveConnector) overwriteOriginalFile(ctx context.Context, db *sql.DB, path string) error {
	baseTableName := sanitizeTableName(tableFromFilePath(path))

	switch {
	case isACHFile(path):
		if err := DumpACH(ctx, db, baseTableName, path); err != nil {
			return fmt.Errorf("failed to overwrite ACH file %s: %w", path, err)
		}
		return nil
	case isFedWireFile(path):
		if err := DumpFedWire(ctx, db, baseTableName, path); err != nil {
			return fmt.Errorf("failed to overwrite Fedwire file %s: %w", path, err)
		}
		return nil
	}

	format, err := overwriteFormatFor(path)
	if err != nil {
		return err
	}

	factory := NewCompressionFactory()
	// The text encoding and the line terminator are read from the file about to
	// be replaced, for the same reason the compression is read from its name:
	// what comes back has to be the file the caller had, with their edit in it.
	// Writing "\n" over a CRLF file changed every line of it while one row had
	// been edited, and writing UTF-8 over a UTF-16 file changed every byte.
	options := DumpOptions{
		Format:      format,
		Compression: factory.detectCompressionType(path),
		Encoding:    detectSourceEncoding(path),
		LineEnding:  detectLineEnding(path, format),
	}

	// An Excel workbook holds a table per sheet, so all of them are written back
	// together into the one file. The tables of a workbook are named after it,
	// which is how the ones belonging to this path are found.
	if format == OutputFormatXLSX {
		return overwriteWorkbookAtPath(db, path, baseTableName, c.siblingBaseTableNames(path), options)
	}

	return overwriteTableAtPath(db, path, baseTableName, options)
}

// siblingBaseTableNames is the table name every source of this save other than
// path was loaded under. It is what tells a workbook's own tables from those of
// a source whose name sits inside the workbook's prefix.
func (c *autoSaveConnector) siblingBaseTableNames(path string) []string {
	bases := make([]string, 0, len(c.originalPaths))
	for _, other := range c.originalPaths {
		if other == path {
			continue
		}
		bases = append(bases, sanitizeTableName(tableFromFilePath(other)))
	}
	return bases
}

// checkOverwriteTargets reports the first of paths that overwrite mode could
// never write back, from the path alone: a format this package reads but does
// not write, or a compression codec it reads but does not write. Both answers
// are in the file's name, so this runs from Build as well, where the caller
// hears about it before the session rather than after the session's work has
// been discarded.
//
// A workbook holding more than one table is the failure this cannot see: it
// takes opening the file to know, and it is left to the save.
func checkOverwriteTargets(paths []string) error {
	factory := NewCompressionFactory()
	for _, path := range paths {
		if isACHFile(path) || isFedWireFile(path) {
			// Both have writers of their own and take no external compression.
			continue
		}
		if _, err := overwriteFormatFor(path); err != nil {
			return err
		}
		if err := codec.Codec(factory.detectCompressionType(path)).CannotWrite(); err != nil {
			return fmt.Errorf("%w: %s cannot be written back: %w", ErrUnsupportedFormat, path, err)
		}
	}
	return nil
}

// overwriteFormatFor is the output format a source file is written back in, or
// an error naming the file when nothing can write it. JSON and JSONL are read
// but have no writer, so a save that quietly turned them into CSV left the
// caller's file untouched and the change in a file they never named.
func overwriteFormatFor(path string) (OutputFormat, error) {
	factory := NewCompressionFactory()
	switch factory.getBaseFileType(path) {
	case FileTypeCSV:
		return OutputFormatCSV, nil
	case FileTypeTSV:
		return OutputFormatTSV, nil
	case FileTypeLTSV:
		return OutputFormatLTSV, nil
	case FileTypeParquet:
		return OutputFormatParquet, nil
	case FileTypeXLSX:
		return OutputFormatXLSX, nil
	default:
		return 0, fmt.Errorf("%w: %s cannot be written back because this package reads that format but does not write it; save to a directory with a format it writes instead",
			ErrUnsupportedFormat, path)
	}
}

// overwriteWorkbookAtPath writes every table of a workbook back to it, one sheet
// per table, in a single staged write.
//
// A workbook of more than one sheet used to be refused here, because the writer
// wrote one sheet per file and so could not represent the rest. Refusing meant a
// caller who opened a two-sheet workbook with auto-save could not save at all.
func overwriteWorkbookAtPath(db *sql.DB, path, baseTableName string, siblingBases []string, options DumpOptions) error {
	tables, err := tablesFromWorkbook(db, baseTableName, siblingBases)
	if err != nil {
		return err
	}
	if len(tables) == 0 {
		return fmt.Errorf("%w: no table for %s remains", ErrEmptyData, path)
	}
	// The sheets go back in a fixed order rather than whatever the catalog
	// happens to list, so the same workbook saved twice is the same file.
	sort.Strings(tables)

	// Excel caps a sheet name at 31 runes and forbids some characters, so two
	// table names can arrive at the same sheet. excelize's NewSheet answers with
	// the existing sheet's index rather than an error, so the second table would
	// overwrite the first's sheet and one table's rows would be gone while the
	// save reported success. Losing a table silently is worse than not saving, so
	// the collision is refused, the same way a format this package cannot write is.
	bySheet := make(map[string]string, len(tables))
	sheets := make([]xlsxSheet, 0, len(tables))
	for _, tableName := range tables {
		sheetName := xlsxSheetNameForTable(baseTableName, tableName)
		if first, clash := bySheet[sheetName]; clash {
			return fmt.Errorf("%w: tables %s and %s both become the sheet %q in %s, and Excel holds one sheet per name; rename one, or save to a directory instead",
				ErrUnsupportedFormat, first, tableName, sheetName, path)
		}
		bySheet[sheetName] = tableName

		sheets = append(sheets, xlsxSheet{
			name: sheetName,
			open: func() ([]string, *sql.Rows, error) {
				// A save at close has no caller context to honor: Close takes
				// none, and the rows have to be written before the database goes.
				columns, declTypes, err := getSQLiteTableColumns(context.Background(), db, tableName)
				if err != nil {
					return nil, nil, fmt.Errorf("%w: failed to get columns for table %s: %w", ErrDatabaseOperation, tableName, err)
				}
				if len(columns) == 0 {
					return nil, nil, fmt.Errorf("%w: table %s for %s no longer exists", ErrEmptyData, tableName, path)
				}
				query := fmt.Sprintf("SELECT %s FROM %s", dumpSelectList(columns, declTypes), quoteIdentifier(tableName)) //nolint:gosec // Table and column names are quoted
				rows, err := db.QueryContext(context.Background(), query)
				if err != nil {
					return nil, nil, err
				}
				return columns, rows, nil
			},
		})
	}

	// The save writes onto the workbook it is replacing rather than onto a new
	// one, so what this package does not hold survives: a sheet the sheet policy
	// chose not to load, and the widths, merges and comments of the sheets it
	// did. A workbook that cannot be reopened is written fresh, which is what
	// every save did before this.
	base, err := openWorkbookForOverwrite(path)
	if err != nil {
		return err
	}

	if err := writeFileAtomically(path, func(w io.Writer) error {
		return writeXLSXWorkbookCompressed(w, path, base, sheets, options.Compression)
	}); err != nil {
		return fmt.Errorf("%w: failed to overwrite %s: %w", ErrIOOperation, path, err)
	}
	return nil
}

// writeXLSXWorkbookCompressed writes sheets to w through the requested codec.
//
// The named return lets the deferred close report its own failure. A compressor
// writes its trailer on Close, so an archive that failed to finish is only
// detectable there; dropping that error would commit a truncated file over the
// caller's workbook. A write error already in flight wins, because it is the one
// that explains the failure.
func writeXLSXWorkbookCompressed(w io.Writer, path string, base *reader.Workbook, sheets []xlsxSheet, compression CompressionType) (err error) {
	writer, closeWriter, err := createCompressedWriter(w, compression)
	if err != nil {
		// The handler separates a codec with no writer from a compressor that
		// failed to start, and wrapping both in ErrCompression here undid that:
		// an output this build cannot write reported a compression failure and
		// matched both sentinels at once.
		return err
	}
	defer func() {
		// Closing a compressing writer is what flushes it, so a failure here can
		// mean a truncated file. Joining rather than dropping it means a caller
		// whose write already failed still learns the output is unusable.
		err = joinCleanup(err, closeWriter(), "finish writing "+path)
	}()

	return writeXLSXWorkbookOnto(writer, base, sheets)
}

// openWorkbookForOverwrite reads the workbook a save is about to replace, so the
// save can write onto it. The file is read through its own codec, the way the
// loader read it.
//
// A file that cannot be read as a workbook is not an error here: the save
// replaces it either way, and a fresh workbook is what every save wrote before
// this. Only a file that cannot be read at all stops the save, since that is the
// file about to be overwritten.
func openWorkbookForOverwrite(path string) (*reader.Workbook, error) {
	src, closeReader, err := NewCompressionFactory().CreateReaderForFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil //nolint:nilnil // No file to write onto is not a failure; the save creates one.
		}
		return nil, nil //nolint:nilnil // Unreadable through its codec; the save writes a fresh workbook.
	}
	defer func() { _ = closeReader() }()

	// The workbook is opened through the reader rather than through excelize
	// directly, because the save needs what the load knew: which rows of a sheet
	// the table came from, which the reader answers from the file's own bytes.
	book, err := reader.OpenWorkbook(src)
	if err != nil {
		return nil, nil //nolint:nilnil // Unreadable as a workbook; the save writes a fresh one.
	}
	return book, nil
}

// tablesFromWorkbook lists the tables an Excel workbook was loaded as. A sheet
// becomes baseTableName_sheet, or baseTableName alone when the sheet repeats the
// file name.
//
// siblingBases names the other sources loaded alongside this one, because the
// prefix a workbook claims can hold another source whole. "book_v2.xlsx" loads
// its sheet "Orders" as "book_v2_Orders", which is also a name "book.xlsx"
// would answer to, so saving book.xlsx took the sibling's table for one of its
// own: it looked for a sheet "v2_Orders" that book.xlsx never had, and the save
// failed there with every file left as it was and the session's edits gone. A
// name another source claims more specifically is that source's, so it is left
// out here.
func tablesFromWorkbook(db *sql.DB, baseTableName string, siblingBases []string) ([]string, error) {
	names, err := getSQLiteTableNames(context.Background(), db)
	if err != nil {
		return nil, fmt.Errorf("%w: failed to get table names: %w", ErrDatabaseOperation, err)
	}

	tables := make([]string, 0, 1)
	for _, name := range names {
		if !tableBelongsTo(name, baseTableName) {
			continue
		}
		if claimedBySibling(name, baseTableName, siblingBases) {
			continue
		}
		tables = append(tables, name)
	}
	return tables, nil
}

// tableBelongsTo reports whether tableName is one a source loaded as
// baseTableName would be saved back as.
func tableBelongsTo(tableName, baseTableName string) bool {
	return tableName == baseTableName || strings.HasPrefix(tableName, baseTableName+"_")
}

// claimedBySibling reports whether another source loaded in the same session
// names tableName more precisely than baseTableName does. Longer is more
// precise: a base can only compete for this name by being baseTableName plus a
// suffix, which is exactly the source whose own name the table starts with.
func claimedBySibling(tableName, baseTableName string, siblingBases []string) bool {
	for _, sibling := range siblingBases {
		if len(sibling) > len(baseTableName) && tableBelongsTo(tableName, sibling) {
			return true
		}
	}
	return false
}

// overwriteTableAtPath dumps one table to one path. It is the write half of
// DumpDatabase's per-table loop, without the directory and the name derived from
// it: the destination here is the file the table came from.
func overwriteTableAtPath(db *sql.DB, path, tableName string, options DumpOptions) error {
	columns, declTypes, err := getSQLiteTableColumns(context.Background(), db, tableName)
	if err != nil {
		return fmt.Errorf("%w: failed to get columns for table %s: %w", ErrDatabaseOperation, tableName, err)
	}
	if len(columns) == 0 {
		return fmt.Errorf("%w: table %s for %s no longer exists", ErrEmptyData, tableName, path)
	}

	query := fmt.Sprintf("SELECT %s FROM %s", dumpSelectList(columns, declTypes), quoteIdentifier(tableName)) //nolint:gosec // Table and column names are quoted
	rows, err := db.QueryContext(context.Background(), query)
	if err != nil {
		return err
	}
	defer rows.Close()

	// The save writes onto the file it loaded, so the types that file declares
	// are read before it is replaced: nothing in the database remembers them.
	var prior parquetPrior
	if options.Format == OutputFormatParquet {
		prior = readParquetPrior(path)
	}
	if err := writeSQLiteTableData(path, tableName, columns, rows, options, prior); err != nil {
		return fmt.Errorf("%w: failed to overwrite %s: %w", ErrIOOperation, path, err)
	}
	return nil
}
