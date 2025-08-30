package filesql

import (
	"errors"
	"fmt"
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

// Error creation functions for consistent error formatting

// NewFileError creates a file-related error with consistent formatting
func NewFileError(operation, filename string, err error) error {
	return fmt.Errorf("filesql: failed to %s file '%s': %w", operation, filename, err)
}

// NewDatabaseError creates a database-related error
func NewDatabaseError(operation string, err error) error {
	return fmt.Errorf("filesql: database %s failed: %w", operation, err)
}

// NewParsingError creates a parsing-related error
func NewParsingError(format, details string, err error) error {
	if err != nil {
		return fmt.Errorf("filesql: failed to parse %s (%s): %w", format, details, err)
	}
	return fmt.Errorf("filesql: failed to parse %s (%s)", format, details)
}

// IsDataError checks if error is data-related
func IsDataError(err error) bool {
	return errors.Is(err, ErrEmptyData) || errors.Is(err, ErrInvalidData) || errors.Is(err, errDuplicateColumnName)
}
