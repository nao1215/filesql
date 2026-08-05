package filesql

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
				src := filepath.Join(dir, "products.tsv.bz2")
				fixture, err := os.ReadFile(filepath.Join("testdata", "products.tsv.bz2"))
				require.NoError(t, err)
				require.NoError(t, os.WriteFile(src, fixture, 0o600)) //nolint:gosec // src is under t.TempDir()
				return autoSaveOverwrite(t, []string{src}, "UPDATE products SET price = 1")
			},
			wants: []error{ErrIOOperation, ErrCompression, ErrUnsupportedFormat},
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
