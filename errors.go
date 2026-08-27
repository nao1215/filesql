package filesql

import (
	"errors"
)

// Sentinel errors for consistent error handling across the package.
// Use errors.Is() to check for these errors.
var (
	// ErrEmptyData indicates that the data source contains no records.
	ErrEmptyData = errors.New("filesql: empty data source")

	// ErrUnsupportedFormat indicates an unsupported file format.
	ErrUnsupportedFormat = errors.New("filesql: unsupported file format")

	// ErrInvalidData indicates malformed or invalid data.
	ErrInvalidData = errors.New("filesql: invalid data format")

	// ErrNoTables indicates no tables found in database.
	ErrNoTables = errors.New("filesql: no tables found in database")

	// ErrFileNotFound indicates file not found.
	ErrFileNotFound = errors.New("filesql: file not found")

	// ErrDuplicateColumn indicates duplicate column names in the data source.
	ErrDuplicateColumn = errors.New("filesql: duplicate column name")

	// ErrInvalidUTF8 indicates a text source that is not valid UTF-8. SQLite
	// stores TEXT as UTF-8, so such bytes would be stored verbatim and read back
	// as mojibake by every consumer; the load fails instead, and the caller
	// transcodes.
	ErrInvalidUTF8 = errors.New("filesql: invalid UTF-8")

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

	// ErrTableNotFound indicates the specified table does not exist.
	ErrTableNotFound = errors.New("filesql: table not found")

	// ErrColumnMismatch indicates a record that does not fit the columns of its
	// table: a delimited record whose field count differs from the header, or an
	// LTSV record holding a field that names no label.
	ErrColumnMismatch = errors.New("filesql: column count mismatch")

	// ErrDatabaseOperation indicates a database operation failed.
	ErrDatabaseOperation = errors.New("filesql: database operation failed")

	// ErrIOOperation indicates an I/O operation failed.
	ErrIOOperation = errors.New("filesql: I/O operation failed")

	// ErrCompression indicates a compression/decompression operation failed.
	ErrCompression = errors.New("filesql: compression operation failed")

	// ErrEncoding indicates a text encoding operation failed, which for a save
	// means the target encoding has no way to write a value the table holds.
	ErrEncoding = errors.New("filesql: text encoding failed")

	// ErrParsing indicates a file parsing operation failed.
	ErrParsing = errors.New("filesql: parsing failed")

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
