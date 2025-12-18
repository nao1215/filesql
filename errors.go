package filesql

import (
	"errors"
	"fmt"
	"strings"
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
)

// errDuplicateColumnName is an internal alias for backward compatibility.
// Deprecated: Use ErrDuplicateColumn instead.
var errDuplicateColumnName = ErrDuplicateColumn

// ErrorContext provides context for where an error occurred
type ErrorContext struct {
	Operation string
	FilePath  string
	TableName string
	Details   string
}

// NewErrorContext creates a new error context
func NewErrorContext(operation, filePath string) *ErrorContext {
	return &ErrorContext{
		Operation: operation,
		FilePath:  filePath,
	}
}

// WithTable adds table context to the error
func (ec *ErrorContext) WithTable(tableName string) *ErrorContext {
	ec.TableName = tableName
	return ec
}

// WithDetails adds details to the error context
func (ec *ErrorContext) WithDetails(details string) *ErrorContext {
	ec.Details = details
	return ec
}

// Error creates a formatted error with context
func (ec *ErrorContext) Error(baseErr error) error {
	var parts []string
	parts = append(parts, fmt.Sprintf("filesql: %s failed", ec.Operation))

	if ec.FilePath != "" {
		parts = append(parts, "file: "+ec.FilePath)
	}

	if ec.TableName != "" {
		parts = append(parts, "table: "+ec.TableName)
	}

	if ec.Details != "" {
		parts = append(parts, "details: "+ec.Details)
	}

	context := strings.Join(parts, ", ")
	if baseErr != nil {
		return fmt.Errorf("%s: %w", context, baseErr)
	}
	return fmt.Errorf("%s", context)
}
