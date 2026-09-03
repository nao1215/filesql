package filesql

import (
	"errors"

	"github.com/nao1215/filesql/internal/reader"
	"github.com/nao1215/filesql/internal/textin"
)

// Sentinel errors for consistent error handling across the package.
// Use errors.Is() to check for these errors.
var (
	// ErrEmptyData indicates that the data source contains no records.
	ErrEmptyData = reader.ErrEmptyData

	// ErrUnsupportedFormat indicates an unsupported file format.
	ErrUnsupportedFormat = reader.ErrUnsupportedFormat

	// ErrInvalidData indicates malformed or invalid data.
	ErrInvalidData = reader.ErrInvalidData

	// ErrNoTables indicates no tables found in database.
	ErrNoTables = errors.New("filesql: no tables found in database")

	// ErrFileNotFound indicates file not found.
	ErrFileNotFound = errors.New("filesql: file not found")

	// ErrDuplicateColumn indicates duplicate column names in the data source.
	ErrDuplicateColumn = reader.ErrDuplicateColumn

	// ErrInvalidUTF8 indicates a text source that is not valid UTF-8. SQLite
	// stores TEXT as UTF-8, so such bytes would be stored verbatim and read back
	// as mojibake by every consumer; the load fails instead, and the caller
	// transcodes.
	ErrInvalidUTF8 = textin.ErrInvalidUTF8

	// ErrDuplicateTable indicates a table with the same name already exists.
	ErrDuplicateTable = errors.New("filesql: duplicate table name")

	// ErrReservedTableName indicates an input would be loaded into a table whose
	// name this package reserves for its own bookkeeping.
	ErrReservedTableName = errors.New("filesql: reserved table name")

	// ErrNilInput indicates a required input parameter is nil.
	ErrNilInput = errors.New("filesql: nil input")

	// ErrEmptyPath indicates an empty path was provided.
	ErrEmptyPath = errors.New("filesql: empty path")

	// ErrNoFiles indicates no supported files were found.
	ErrNoFiles = errors.New("filesql: no supported files found")

	// ErrTableNotFound indicates the specified table does not exist. A save
	// returns it for a source whose table the session dropped or renamed, which
	// is a table that is gone rather than a source with no records.
	ErrTableNotFound = errors.New("filesql: table not found")

	// ErrColumnMismatch indicates a record that does not fit the columns of its
	// table: a delimited record whose field count differs from the header, or an
	// LTSV record holding a field that names no label.
	ErrColumnMismatch = reader.ErrColumnMismatch

	// ErrDatabaseOperation indicates a database operation failed.
	ErrDatabaseOperation = errors.New("filesql: database operation failed")

	// ErrIOOperation indicates an I/O operation failed.
	ErrIOOperation = errors.New("filesql: I/O operation failed")

	// ErrCompression indicates a compression/decompression operation failed. A
	// file whose suffix claims a compression its bytes do not carry answers it
	// whichever codec the suffix names, whether that codec reads its header
	// when the stream opens or on the first read.
	ErrCompression = errors.New("filesql: compression operation failed")

	// ErrEncoding indicates a text encoding operation failed, which for a save
	// means the target encoding has no way to write a value the table holds.
	ErrEncoding = textin.ErrEncoding

	// ErrParsing indicates a file parsing operation failed.
	ErrParsing = reader.ErrParsing

	// ErrACH indicates an ACH file operation failed.
	ErrACH = errors.New("filesql: ACH operation failed")

	// ErrWire indicates a Fedwire file operation failed.
	ErrWire = errors.New("filesql: Fedwire operation failed")

	// ErrSourceUnavailable indicates an ACH or Fedwire export failed because the
	// file its tables were loaded from is unknown or unreadable.
	ErrSourceUnavailable = errors.New("filesql: source file for write-back is unavailable")
)

// ParseError is a failure reading one input, with the input named.
//
// Both this package and its callers want to say which file failed, and both
// used to. Loading one bad file produced "filesql: parsing failed: failed to
// stream file rm.csv: filesql: column count mismatch: row 1 has 2 fields, want
// 3" — two framing verbs for one event, this package's name twice — and a
// caller that had already named the file added a third mention of it, because
// there was no way to reach the cause on its own.
//
// So the framing is a type instead of more text. Error names the source once
// for a caller that loaded several inputs at once and cannot otherwise tell
// which one failed; a caller that loads one file at a time reads Err and says
// the path itself.
type ParseError struct {
	// Source is the input that failed: a file path, or the table name of a
	// reader input, which is all a reader has to be known by.
	Source string
	// Err is what went wrong reading it.
	Err error
}

func (e *ParseError) Error() string { return e.Source + ": " + e.Err.Error() }

// Unwrap reports the cause and ErrParsing together, so errors.Is still finds
// the sentinel the message used to spell out and errors.As still reaches the
// cause.
func (e *ParseError) Unwrap() []error { return []error{ErrParsing, e.Err} }
