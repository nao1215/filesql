package filesql

import (
	"errors"
	"fmt"
	"strings"
)

// Standard error messages and error creation functions for consistency
var (
	// errDuplicateColumnName is returned when a file contains duplicate column names
	errDuplicateColumnName = errors.New("duplicate column name")

	// ErrEmptyData indicates that the data source contains no records
	ErrEmptyData = errors.New("filesql: empty data source")

	// ErrUnsupportedFormat indicates an unsupported file format
	ErrUnsupportedFormat = errors.New("filesql: unsupported file format")

	// ErrInvalidData indicates malformed or invalid data
	ErrInvalidData = errors.New("filesql: invalid data format")

	// ErrNoTables indicates no tables found in database
	ErrNoTables = errors.New("filesql: no tables found in database")

	// ErrFileNotFound indicates file not found
	ErrFileNotFound = errors.New("filesql: file not found")

	// ErrPermissionDenied indicates permission denied
	ErrPermissionDenied = errors.New("filesql: permission denied")

	// ErrMemoryLimit indicates memory limit exceeded
	ErrMemoryLimit = errors.New("filesql: memory limit exceeded")

	// ErrContextCancelled indicates context was cancelled
	ErrContextCancelled = errors.New("filesql: context cancelled")
)

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
