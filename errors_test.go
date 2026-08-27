package filesql

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"testing"
	"time"

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

// TestErrorSentinelsAreReachable checks the property rather than one path: an
// error this package returns has to satisfy errors.Is for every sentinel it
// names, not only the outermost one.
//
// It did not. Errors were wrapped as fmt.Errorf("%w: ...: %s", Sentinel,
// err.Error()) in 115 places, so the sentinel of the frame that failed was
// rendered to text and lost. A save that failed because bzip2 has no writer
// said "unsupported file format" and did not satisfy
// errors.Is(err, ErrUnsupportedFormat), leaving a caller to match on the
// message. Ref #216.
func TestErrorSentinelsAreReachable(t *testing.T) {
	t.Parallel()

	// Each case names the sentinels its message mentions. The point is that the
	// message and the chain agree.
	tests := []struct {
		name  string
		run   func(t *testing.T) error
		wants []error
	}{
		{
			name: "a codec that cannot be written",
			run: func(t *testing.T) error {
				t.Helper()
				dir := t.TempDir()
				src := filepath.Join(dir, "users.csv")
				require.NoError(t, os.WriteFile(src, []byte("id,name\n1,alice\n"), 0o600))
				validated, err := buildForTest(t.Context(), NewBuilder().AddPath(src))
				require.NoError(t, err)
				db, err := validated.Open(t.Context())
				require.NoError(t, err)
				defer db.Close()
				return DumpDatabase(db, filepath.Join(dir, "out"), NewDumpOptions().WithCompression(CompressionBZ2))
			},
			// Not ErrCompression: a codec with no writer is an unsupported
			// format, and matching both would leave a caller unable to tell it
			// from a compressor that failed to start.
			wants: []error{ErrIOOperation, ErrUnsupportedFormat},
		},
		{
			name: "data that is not the codec it was declared to be",
			run: func(t *testing.T) error {
				t.Helper()
				parser := newStreamingParser(FileTypeCSV, CompressionGZ, "t", 1024)
				_, err := parser.parseFromReader(strings.NewReader("not gzip at all"))
				return err
			},
			wants: []error{ErrCompression},
		},
		{
			// A malformed JSON document is invalid data whichever value it
			// opens with. The array branch used to report only ErrParsing,
			// which every load failure carries, so a caller matching
			// ErrInvalidData to mean "this file is not JSON" matched an
			// unterminated object and missed an unterminated array.
			name: "a JSON array that is not JSON",
			run: func(t *testing.T) error {
				t.Helper()
				dir := t.TempDir()
				src := filepath.Join(dir, "broken.json")
				require.NoError(t, os.WriteFile(src, []byte(`[{"a":`), 0o600))
				_, err := OpenContext(context.Background(), src)
				return err
			},
			wants: []error{ErrParsing, ErrInvalidData},
		},
		{
			name: "a path with no file this package reads",
			run: func(t *testing.T) error {
				t.Helper()
				dir := t.TempDir()
				src := filepath.Join(dir, "notes.txt")
				require.NoError(t, os.WriteFile(src, []byte("hello"), 0o600))
				_, err := OpenContext(context.Background(), src)
				return err
			},
			wants: []error{ErrUnsupportedFormat},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := tt.run(t)
			require.Error(t, err)
			for _, want := range tt.wants {
				assert.ErrorIs(t, err, want,
					"%v is named in %q but not reachable through the chain", want, err)
			}
		})
	}
}

// TestErrorWrappingKeepsTheMessage pins that making the chain reachable did not
// change what an error says. fmt renders %w through the error's Error method,
// so "%w: x: %w" and "%w: x: %s" with err.Error() produce the same string --
// which is why 115 call sites could be converted without a message changing.
func TestErrorWrappingKeepsTheMessage(t *testing.T) {
	t.Parallel()

	inner := errors.New("filesql: unsupported file format: bzip2 compression is not supported for writing")
	withS := errors.New("filesql: compression operation failed: failed to create writer: " + inner.Error())
	withW := wrapForTest(inner)

	assert.Equal(t, withS.Error(), withW.Error())
	assert.ErrorIs(t, withW, inner)
	assert.NotErrorIs(t, withS, inner)
}

func wrapForTest(inner error) error {
	return fmt.Errorf("%w: failed to create writer: %w", ErrCompression, inner)
}

// TestParseFailure_NamesTheInputOnce is the message half of the same concern.
//
// A file that fails to load used to be reported as
// "filesql: parsing failed: failed to stream file rm.csv: filesql: column count
// mismatch: row 1 has 2 fields, want 3": two generic framing verbs saying the
// same thing, and this package's own name twice. A caller that already knows
// which file it asked for then added a third mention of the path, because there
// was no way to reach the cause without it.
func TestParseFailure_NamesTheInputOnce(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		file    string
		content string
		// wantSentinel is the sentinel the cause carries, beyond ErrParsing.
		wantSentinel error
	}{
		{
			name:         "a row with fewer fields than the header",
			file:         "rm.csv",
			content:      "a,b,c\n1,2\n",
			wantSentinel: ErrColumnMismatch,
		},
		{
			// This one used to take the other framing path: the cause already
			// carried ErrParsing, and the reader wrapped it in ErrParsing again,
			// so the message announced the package twice on its own.
			name:         "a quoted field the CSV reader cannot parse",
			file:         "quote.csv",
			content:      "a,b\n\"unclosed,2\n",
			wantSentinel: ErrParsing,
		},
		{
			// The decoder reports this one with a sentinel of this package's
			// own from inside the chain the reader reads through, and the
			// reader framed it as a parse failure on top, so the package
			// announced itself twice about one byte.
			name:         "a byte that is not part of a UTF-8 character",
			file:         "utf8.csv",
			content:      "a,b\n\xff\xfe,2\n",
			wantSentinel: ErrInvalidUTF8,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			path := filepath.Join(t.TempDir(), tt.file)
			require.NoError(t, os.WriteFile(path, []byte(tt.content), 0o600))

			db, err := OpenContext(context.Background(), path)
			if db != nil {
				defer func() { _ = db.Close() }()
			}
			require.Error(t, err)

			msg := err.Error()
			assert.Equal(t, 1, strings.Count(msg, tt.file), "the path is named once: %s", msg)
			assert.Equal(t, 1, strings.Count(msg, "filesql: "), "the package names itself once: %s", msg)
			assert.NotContains(t, msg, "streaming processing failed", msg)
			assert.NotContains(t, msg, "failed to stream file", msg)

			// The sentinels the old message spelled out are still reachable, and so
			// is the cause, so nothing that branched on either has to change.
			assert.ErrorIs(t, err, ErrParsing)
			assert.ErrorIs(t, err, tt.wantSentinel)

			// A caller that already named the file reaches the cause without the path.
			var parseErr *ParseError
			require.ErrorAs(t, err, &parseErr)
			assert.Equal(t, path, parseErr.Source)
			assert.NotContains(t, parseErr.Err.Error(), tt.file)
		})
	}
}

// TestUnreadableFileReportsFsErrPermission pins that a caller can tell "I may
// not read this" from "this is not there" without a sentinel of this package's
// own.
//
// filesql wraps the operating system's error all the way up, so the standard
// library's answer already works. That is why ErrPermissionDenied was removed
// rather than wired up: a package-specific alias for a condition io/fs already
// names is a second way to ask the same question, and it was the way that
// always answered false.
func TestUnreadableFileReportsFsErrPermission(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("clearing the read bit does not deny the owner on Windows")
	}
	if os.Geteuid() == 0 {
		t.Skip("root ignores the read bit")
	}
	t.Parallel()

	path := filepath.Join(t.TempDir(), "unreadable.csv")
	require.NoError(t, os.WriteFile(path, []byte("id\n1\n"), 0o600))
	require.NoError(t, os.Chmod(path, 0o000))

	_, err := OpenContext(t.Context(), path)
	require.Error(t, err)
	assert.ErrorIs(t, err, fs.ErrPermission, "the caller needs to see a permission problem as one")
	assert.ErrorIs(t, err, ErrIOOperation, "and it stays inside this package's I/O category")
}

// TestCancelledLoadReportsContextError pins the same for cancellation:
// context.Canceled is what a load reports, which is what a caller already knows
// how to test for.
func TestCancelledLoadReportsContextError(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "t.csv")
	require.NoError(t, os.WriteFile(path, []byte("id\n1\n"), 0o600))

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := OpenContext(ctx, path)
	require.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)
}

// TestCanceledLoadNamesThePackageOnce pins that the load path frames a failed
// insert once, the way a failed read is framed once.
//
// The insert wrapped ErrDatabaseOperation and its caller wrapped the same
// sentinel again to add the table name, so one failure read as
// "filesql: database operation failed: failed to insert chunk data into table
// "t": filesql: database operation failed: failed to insert record: ...".
func TestCanceledLoadNamesThePackageOnce(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "big.csv")
	var body strings.Builder
	body.WriteString("id,name\n")
	for i := range 200000 {
		fmt.Fprintf(&body, "%d,customer%d\n", i, i)
	}
	require.NoError(t, os.WriteFile(path, []byte(body.String()), 0o600))

	interrupted := 0
	for attempt := range 20 {
		ctx, cancel := context.WithTimeout(context.Background(), time.Duration(1+attempt)*time.Millisecond)
		db, err := OpenContext(ctx, path)
		if db != nil {
			_ = db.Close()
		}
		cancel()
		if err == nil {
			continue
		}
		interrupted++
		// At most once rather than exactly once: a deadline that expires before
		// the load has begun is answered with the context's own error and
		// nothing else, which names the package no times and is right.
		msg := err.Error()
		require.LessOrEqual(t, strings.Count(msg, "filesql: "), 1, "the package names itself at most once: %s", msg)
		assert.ErrorIs(t, err, context.DeadlineExceeded)
	}
	// Without this the test passes by never having tested anything: a machine
	// that loads the file inside one millisecond would take every attempt to
	// the end and assert nothing.
	require.Positive(t, interrupted, "no attempt was interrupted, so the framing was never checked")
}
