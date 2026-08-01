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

	// ErrPermissionDenied indicates permission denied.
	ErrPermissionDenied = errors.New("filesql: permission denied")

	// ErrMemoryLimit indicates memory limit exceeded.
	ErrMemoryLimit = errors.New("filesql: memory limit exceeded")

	// ErrContextCancelled indicates context was cancelled.
	ErrContextCancelled = errors.New("filesql: context cancelled")

	// ErrDuplicateColumn indicates duplicate column names in the data source.
	ErrDuplicateColumn = errors.New("filesql: duplicate column name")

	// ErrDuplicateTable indicates a table with the same name already exists.
	ErrDuplicateTable = errors.New("filesql: duplicate table name")

	// ErrNilInput indicates a required input parameter is nil.
	ErrNilInput = errors.New("filesql: nil input")

	// ErrEmptyPath indicates an empty path was provided.
	ErrEmptyPath = errors.New("filesql: empty path")

	// ErrNoFiles indicates no supported files were found.
	ErrNoFiles = errors.New("filesql: no supported files found")

	// ErrTableNotFound indicates the specified table does not exist.
	ErrTableNotFound = errors.New("filesql: table not found")

	// ErrColumnMismatch indicates record column count doesn't match header.
	ErrColumnMismatch = errors.New("filesql: column count mismatch")

	// ErrDatabaseOperation indicates a database operation failed.
	ErrDatabaseOperation = errors.New("filesql: database operation failed")

	// ErrIOOperation indicates an I/O operation failed.
	ErrIOOperation = errors.New("filesql: I/O operation failed")

	// ErrCompression indicates a compression/decompression operation failed.
	ErrCompression = errors.New("filesql: compression operation failed")

	// ErrParsing indicates a file parsing operation failed.
	ErrParsing = errors.New("filesql: parsing failed")

	// ErrACH indicates an ACH file operation failed.
	ErrACH = errors.New("filesql: ACH operation failed")

	// ErrWire indicates a Fedwire file operation failed.
	ErrWire = errors.New("filesql: Fedwire operation failed")
)

// errDuplicateColumnName is an internal alias for backward compatibility.
// Deprecated: Use ErrDuplicateColumn instead.
var errDuplicateColumnName = ErrDuplicateColumn
