package filesql

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestErrorVariables(t *testing.T) {
	t.Parallel()

	t.Run("error variables are not nil", func(t *testing.T) {
		t.Parallel()

		assert.NotNil(t, errDuplicateColumnName)
		assert.NotNil(t, ErrEmptyData)
		assert.NotNil(t, ErrUnsupportedFormat)
		assert.NotNil(t, ErrInvalidData)
		assert.NotNil(t, ErrNoTables)
		assert.NotNil(t, ErrFileNotFound)
		assert.NotNil(t, ErrPermissionDenied)
		assert.NotNil(t, ErrMemoryLimit)
		assert.NotNil(t, ErrContextCancelled)
		assert.NotNil(t, ErrDuplicateColumn)
		assert.NotNil(t, ErrDuplicateTable)
		assert.NotNil(t, ErrNilInput)
		assert.NotNil(t, ErrEmptyPath)
		assert.NotNil(t, ErrNoFiles)
		assert.NotNil(t, ErrTableNotFound)
		assert.NotNil(t, ErrColumnMismatch)
		assert.NotNil(t, ErrDatabaseOperation)
		assert.NotNil(t, ErrIOOperation)
		assert.NotNil(t, ErrCompression)
		assert.NotNil(t, ErrParsing)
		assert.NotNil(t, ErrACH)
	})

	t.Run("error messages are meaningful", func(t *testing.T) {
		t.Parallel()

		assert.Contains(t, errDuplicateColumnName.Error(), "duplicate column name")
		assert.Contains(t, ErrEmptyData.Error(), "empty data")
		assert.Contains(t, ErrUnsupportedFormat.Error(), "unsupported")
		assert.Contains(t, ErrInvalidData.Error(), "invalid")
		assert.Contains(t, ErrNoTables.Error(), "no tables")
		assert.Contains(t, ErrFileNotFound.Error(), "not found")
		assert.Contains(t, ErrPermissionDenied.Error(), "permission")
		assert.Contains(t, ErrMemoryLimit.Error(), "memory limit")
		assert.Contains(t, ErrContextCancelled.Error(), "cancelled")
		assert.Contains(t, ErrDuplicateColumn.Error(), "duplicate column")
		assert.Contains(t, ErrDuplicateTable.Error(), "duplicate table")
		assert.Contains(t, ErrNilInput.Error(), "nil input")
		assert.Contains(t, ErrEmptyPath.Error(), "empty path")
		assert.Contains(t, ErrNoFiles.Error(), "no supported files")
		assert.Contains(t, ErrTableNotFound.Error(), "table not found")
		assert.Contains(t, ErrColumnMismatch.Error(), "column count")
		assert.Contains(t, ErrDatabaseOperation.Error(), "database operation")
		assert.Contains(t, ErrIOOperation.Error(), "I/O operation")
		assert.Contains(t, ErrCompression.Error(), "compression")
		assert.Contains(t, ErrParsing.Error(), "parsing")
		assert.Contains(t, ErrACH.Error(), "ACH")
	})

	t.Run("errors can be compared with errors.Is", func(t *testing.T) {
		t.Parallel()

		wrappedErr := errors.Join(ErrEmptyData, errors.New("additional context"))
		assert.True(t, errors.Is(wrappedErr, ErrEmptyData))
	})
}

func TestSentinelErrorWrapping(t *testing.T) {
	t.Parallel()

	t.Run("wrapped errors support errors.Is", func(t *testing.T) {
		t.Parallel()

		testCases := []struct {
			name     string
			sentinel error
			wrapped  error
		}{
			{
				name:     "ErrEmptyData wrapped",
				sentinel: ErrEmptyData,
				wrapped:  fmt.Errorf("%w: empty CSV data", ErrEmptyData),
			},
			{
				name:     "ErrFileNotFound wrapped",
				sentinel: ErrFileNotFound,
				wrapped:  fmt.Errorf("%w: /path/to/file.csv", ErrFileNotFound),
			},
			{
				name:     "ErrDatabaseOperation wrapped",
				sentinel: ErrDatabaseOperation,
				wrapped:  fmt.Errorf("%w: failed to execute query: %v", ErrDatabaseOperation, "table not found"),
			},
			{
				name:     "ErrIOOperation wrapped",
				sentinel: ErrIOOperation,
				wrapped:  fmt.Errorf("%w: failed to read file: %v", ErrIOOperation, "permission denied"),
			},
			{
				name:     "ErrCompression wrapped",
				sentinel: ErrCompression,
				wrapped:  fmt.Errorf("%w: failed to create gzip reader: %v", ErrCompression, "invalid header"),
			},
			{
				name:     "ErrParsing wrapped",
				sentinel: ErrParsing,
				wrapped:  fmt.Errorf("%w: failed to parse CSV: %v", ErrParsing, "invalid field count"),
			},
			{
				name:     "ErrDuplicateTable wrapped",
				sentinel: ErrDuplicateTable,
				wrapped:  fmt.Errorf("%w: table 'users' already exists", ErrDuplicateTable),
			},
			{
				name:     "ErrNoFiles wrapped",
				sentinel: ErrNoFiles,
				wrapped:  fmt.Errorf("%w: no supported files found in directory", ErrNoFiles),
			},
			{
				name:     "ErrACH wrapped",
				sentinel: ErrACH,
				wrapped:  fmt.Errorf("%w: failed to parse ACH file: %v", ErrACH, "invalid record type"),
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()
				assert.True(t, errors.Is(tc.wrapped, tc.sentinel),
					"errors.Is should return true for wrapped sentinel error")
				assert.Contains(t, tc.wrapped.Error(), tc.sentinel.Error(),
					"wrapped error should contain sentinel error message")
			})
		}
	})

	t.Run("errDuplicateColumnName is alias for ErrDuplicateColumn", func(t *testing.T) {
		t.Parallel()
		assert.True(t, errors.Is(errDuplicateColumnName, ErrDuplicateColumn),
			"errDuplicateColumnName should be the same as ErrDuplicateColumn")
	})
}
