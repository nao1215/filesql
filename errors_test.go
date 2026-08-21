package filesql

import (
	"errors"
	"fmt"
	"os"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestErrorVariables(t *testing.T) {
	t.Parallel()

	t.Run("error variables are not nil", func(t *testing.T) {
		t.Parallel()

		assert.NotNil(t, ErrEmptyData)
		assert.NotNil(t, ErrUnsupportedFormat)
		assert.NotNil(t, ErrInvalidData)
		assert.NotNil(t, ErrNoTables)
		assert.NotNil(t, ErrFileNotFound)
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

		assert.Contains(t, ErrEmptyData.Error(), "empty data")
		assert.Contains(t, ErrUnsupportedFormat.Error(), "unsupported")
		assert.Contains(t, ErrInvalidData.Error(), "invalid")
		assert.Contains(t, ErrNoTables.Error(), "no tables")
		assert.Contains(t, ErrFileNotFound.Error(), "not found")
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
}

// TestExportedSentinelsAreReturnedSomewhere fails when this package exports an
// error sentinel that no code path ever wraps.
//
// Three of them did not: ErrPermissionDenied, ErrMemoryLimit and
// ErrContextCancelled were declared, referenced only by their own existence
// test, and never returned. A caller writing errors.Is against one got false
// forever, which is worse than the sentinel not existing — it reads like a
// supported way to ask a question and silently answers wrong.
//
// The check reads this package's own sources rather than using reflection,
// because "is it ever wrapped" is a property of the code and not of a value.
func TestExportedSentinelsAreReturnedSomewhere(t *testing.T) {
	t.Parallel()

	const declarations = "errors.go"

	raw, err := os.ReadFile(declarations)
	require.NoError(t, err)

	declared := regexp.MustCompile(`\n\t(Err[A-Za-z0-9]*)\s*=\s*(errors\.New|fmt\.Errorf)`).FindAllStringSubmatch(string(raw), -1)
	require.NotEmpty(t, declared, "the declarations moved; this guard needs to follow them")

	entries, err := os.ReadDir(".")
	require.NoError(t, err)

	var body strings.Builder
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") || name == declarations {
			continue
		}
		content, readErr := os.ReadFile(name) //nolint:gosec // fixed, in-repo source file
		require.NoError(t, readErr)
		body.Write(content)
	}
	sources := body.String()

	for _, match := range declared {
		name := match[1]
		if !regexp.MustCompile(`\b` + name + `\b`).MatchString(sources) {
			t.Errorf("%s is exported but never returned: either wrap it where it belongs, or remove it so errors.Is against it cannot answer false forever", name)
		}
	}
}
