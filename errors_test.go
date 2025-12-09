package filesql

import (
	"errors"
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
	})

	t.Run("errors can be compared with errors.Is", func(t *testing.T) {
		t.Parallel()

		wrappedErr := errors.Join(ErrEmptyData, errors.New("additional context"))
		assert.True(t, errors.Is(wrappedErr, ErrEmptyData))
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
