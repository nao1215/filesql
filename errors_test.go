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

func TestNewErrorContext(t *testing.T) {
	t.Parallel()

	t.Run("creates error context with operation and file path", func(t *testing.T) {
		t.Parallel()

		ec := NewErrorContext("parse", "/path/to/file.csv")

		assert.Equal(t, "parse", ec.Operation)
		assert.Equal(t, "/path/to/file.csv", ec.FilePath)
		assert.Empty(t, ec.TableName)
		assert.Empty(t, ec.Details)
	})

	t.Run("creates error context with empty values", func(t *testing.T) {
		t.Parallel()

		ec := NewErrorContext("", "")

		assert.Empty(t, ec.Operation)
		assert.Empty(t, ec.FilePath)
	})
}

func TestErrorContext_WithTable(t *testing.T) {
	t.Parallel()

	t.Run("adds table name to context", func(t *testing.T) {
		t.Parallel()

		ec := NewErrorContext("query", "/path/to/file.csv").
			WithTable("users")

		assert.Equal(t, "users", ec.TableName)
	})

	t.Run("returns same instance for chaining", func(t *testing.T) {
		t.Parallel()

		ec := NewErrorContext("query", "/path/to/file.csv")
		result := ec.WithTable("users")

		assert.Same(t, ec, result)
	})
}

func TestErrorContext_WithDetails(t *testing.T) {
	t.Parallel()

	t.Run("adds details to context", func(t *testing.T) {
		t.Parallel()

		ec := NewErrorContext("parse", "/path/to/file.csv").
			WithDetails("row 42 has invalid format")

		assert.Equal(t, "row 42 has invalid format", ec.Details)
	})

	t.Run("returns same instance for chaining", func(t *testing.T) {
		t.Parallel()

		ec := NewErrorContext("parse", "/path/to/file.csv")
		result := ec.WithDetails("some details")

		assert.Same(t, ec, result)
	})
}

func TestErrorContext_Error(t *testing.T) {
	t.Parallel()

	t.Run("creates error with all context fields", func(t *testing.T) {
		t.Parallel()

		baseErr := errors.New("underlying error")
		ec := NewErrorContext("parse", "/path/to/file.csv").
			WithTable("products").
			WithDetails("invalid CSV format")

		err := ec.Error(baseErr)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "filesql: parse failed")
		assert.Contains(t, err.Error(), "file: /path/to/file.csv")
		assert.Contains(t, err.Error(), "table: products")
		assert.Contains(t, err.Error(), "details: invalid CSV format")
		assert.Contains(t, err.Error(), "underlying error")
	})

	t.Run("creates error with only operation", func(t *testing.T) {
		t.Parallel()

		ec := NewErrorContext("init", "")
		err := ec.Error(nil)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "filesql: init failed")
		assert.NotContains(t, err.Error(), "file:")
		assert.NotContains(t, err.Error(), "table:")
		assert.NotContains(t, err.Error(), "details:")
	})

	t.Run("creates error with operation and file path only", func(t *testing.T) {
		t.Parallel()

		ec := NewErrorContext("read", "/data/file.csv")
		err := ec.Error(nil)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "filesql: read failed")
		assert.Contains(t, err.Error(), "file: /data/file.csv")
		assert.NotContains(t, err.Error(), "table:")
		assert.NotContains(t, err.Error(), "details:")
	})

	t.Run("wraps base error for errors.Is compatibility", func(t *testing.T) {
		t.Parallel()

		ec := NewErrorContext("parse", "/path/to/file.csv")
		err := ec.Error(ErrInvalidData)

		assert.True(t, errors.Is(err, ErrInvalidData))
	})

	t.Run("creates error without base error", func(t *testing.T) {
		t.Parallel()

		ec := NewErrorContext("validate", "/path/to/file.csv").
			WithDetails("missing required column")

		err := ec.Error(nil)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "filesql: validate failed")
		assert.Contains(t, err.Error(), "details: missing required column")
	})
}

func TestErrorContext_Chaining(t *testing.T) {
	t.Parallel()

	t.Run("supports fluent chaining", func(t *testing.T) {
		t.Parallel()

		err := NewErrorContext("import", "/data/sales.xlsx").
			WithTable("Sheet1").
			WithDetails("row count mismatch").
			Error(ErrInvalidData)

		assert.Error(t, err)
		assert.True(t, errors.Is(err, ErrInvalidData))
		assert.Contains(t, err.Error(), "import")
		assert.Contains(t, err.Error(), "sales.xlsx")
		assert.Contains(t, err.Error(), "Sheet1")
		assert.Contains(t, err.Error(), "row count mismatch")
	})
}
