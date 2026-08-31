package filesql

import (
	"compress/gzip"
	"context"
	"database/sql"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/nao1215/filesql/internal/codec"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/xuri/excelize/v2"
)

// TestAutoSaveConnector_CloseReportsBothFailures covers a close where the save
// and the close itself both fail. The save error is the one a caller acts on, so
// it leads, but a connection that also failed to close is worth saying.
func TestAutoSaveConnector_CloseReportsBothFailures(t *testing.T) {
	t.Parallel()

	connector := &autoSaveConnector{
		// Overwrite mode with no original paths: the save has nowhere to write, so
		// it fails without touching the filesystem.
		autoSaveConfig: &autoSaveConfig{enabled: true, timing: autoSaveOnClose},
		anchor:         &plainConn{closeErr: errStub},
		armed:          true,
	}

	err := connector.Close()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "auto-save failed")
	assert.Contains(t, err.Error(), "also failed to close connection", "a connection left open is worth reporting too")
}

// TestAutoSaveConnector_CloseBeforeArmingDoesNotSave checks that a setup which
// fails after opening the connector does not write out what it is discarding.
func TestAutoSaveConnector_CloseBeforeArmingDoesNotSave(t *testing.T) {
	t.Parallel()

	connector := &autoSaveConnector{
		autoSaveConfig: &autoSaveConfig{enabled: true, timing: autoSaveOnClose},
		anchor:         &plainConn{},
	}

	assert.NoError(t, connector.Close(), "an unarmed connector closes without saving")
	assert.NoError(t, connector.Close(), "closing twice is a no-op")
}

// TestSave_DisabledDoesNothing covers the two states in which a close has
// nothing to save.
func TestSave_DisabledDoesNothing(t *testing.T) {
	t.Parallel()

	t.Run("no configuration", func(t *testing.T) {
		t.Parallel()
		connector := &autoSaveConnector{}
		assert.NoError(t, connector.save(&plainConn{}))
	})

	t.Run("configuration turned off", func(t *testing.T) {
		t.Parallel()
		connector := &autoSaveConnector{autoSaveConfig: &autoSaveConfig{enabled: false}}
		assert.NoError(t, connector.save(&plainConn{}))
	})
}

// autoSaveSource writes a one-row CSV and returns its path.
func autoSaveSource(t *testing.T, body string) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), "users.csv")
	require.NoError(t, os.WriteFile(path, []byte(body), 0o600))
	return path
}

// openAutoSave opens path with auto-save configured by configure.
func openAutoSave(t *testing.T, path string, configure func(*DBBuilder) *DBBuilder) *sql.DB {
	t.Helper()

	validated, err := buildForTest(t.Context(), configure(NewBuilder().AddPath(path)))
	require.NoError(t, err)
	db, err := validated.Open(t.Context())
	require.NoError(t, err)
	return db
}

// TestAutoSaveSurvivesAPoolWithMoreThanOneConnection pins that an auto-save
// database behaves like any other pooled database: a second connection is a
// second connection, and closing the pool saves once.
//
// It did not: every pooled connection wrapped one shared driver.Conn, so the
// first wrapper the pool closed ran the save and closed that connection, and the
// next wrapper ran the save against a connection that was already gone. The
// crash was a SIGSEGV inside the SQLite driver, which a caller cannot recover
// from, and it took the save that was meant to persist their work with it.
func TestAutoSaveSurvivesAPoolWithMoreThanOneConnection(t *testing.T) {
	t.Parallel()

	t.Run("the pool trims the connection a write just used", func(t *testing.T) {
		t.Parallel()

		path := autoSaveSource(t, "id,name\n1,alice\n")
		db := openAutoSave(t, path, func(b *DBBuilder) *DBBuilder { return b.EnableAutoSave("") })
		db.SetMaxIdleConns(0)

		_, err := db.ExecContext(t.Context(), "UPDATE users SET name = 'bob'")
		require.NoError(t, err)
		require.NoError(t, db.Close())

		saved, err := os.ReadFile(path) //nolint:gosec // Test path from t.TempDir()
		require.NoError(t, err)
		assert.Equal(t, "id,name\n1,bob\n", string(saved))
	})

	t.Run("a query issued while rows are open", func(t *testing.T) {
		t.Parallel()

		path := autoSaveSource(t, "id,name\n1,alice\n2,carol\n")
		db := openAutoSave(t, path, func(b *DBBuilder) *DBBuilder { return b.EnableAutoSave("") })

		rows, err := db.QueryContext(t.Context(), "SELECT id FROM users")
		require.NoError(t, err)
		var count int
		require.NoError(t, db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM users").Scan(&count))
		assert.Equal(t, 2, count)
		require.NoError(t, rows.Err())
		require.NoError(t, rows.Close())

		_, err = db.ExecContext(t.Context(), "UPDATE users SET name = 'bob' WHERE id = 1")
		require.NoError(t, err)
		require.NoError(t, db.Close())

		saved, err := os.ReadFile(path) //nolint:gosec // Test path from t.TempDir()
		require.NoError(t, err)
		assert.Equal(t, "id,name\n1,bob\n2,carol\n", string(saved))
	})
}

// TestAutoSaveWritesEachTableOnce pins that closing a database that has held
// several connections leaves one file per table holding the final rows, not one
// save per connection the pool happened to open.
func TestAutoSaveWritesEachTableOnce(t *testing.T) {
	t.Parallel()

	path := autoSaveSource(t, "id,name\n1,alice\n")
	outputDir := t.TempDir()
	db := openAutoSave(t, path, func(b *DBBuilder) *DBBuilder { return b.EnableAutoSave(outputDir) })

	// A second pooled connection, so the pool holds more than one when it closes.
	rows, err := db.QueryContext(t.Context(), "SELECT id FROM users")
	require.NoError(t, err)
	var count int
	require.NoError(t, db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM users").Scan(&count))
	require.NoError(t, rows.Err())
	require.NoError(t, rows.Close())

	_, err = db.ExecContext(t.Context(), "UPDATE users SET name = 'bob'")
	require.NoError(t, err)
	require.NoError(t, db.Close())

	assert.Equal(t, []string{"users.csv"}, dirEntries(t, outputDir))
	saved, err := os.ReadFile(filepath.Join(outputDir, "users.csv")) //nolint:gosec // Test path from t.TempDir()
	require.NoError(t, err)
	assert.Equal(t, "id,name\n1,bob\n", string(saved))
}

// TestAutoSaveOnCommitSavesWhatNoTransactionWrapped pins that a change which
// survives to a clean Close is on disk whichever auto-save timing was chosen.
//
// It did not: the commit-timing save lived only in the transaction wrapper, so a
// statement run outside an explicit transaction — committed as far as SQLite was
// concerned — never reached the file, and Close did not save either. The change
// was lost with no error to say so.
func TestAutoSaveOnCommitSavesWhatNoTransactionWrapped(t *testing.T) {
	t.Parallel()

	t.Run("overwrite mode", func(t *testing.T) {
		t.Parallel()

		path := autoSaveSource(t, "id,name\n1,alice\n")
		db := openAutoSave(t, path, func(b *DBBuilder) *DBBuilder { return b.EnableAutoSaveOnCommit("") })

		_, err := db.ExecContext(t.Context(), "UPDATE users SET name = 'bob'")
		require.NoError(t, err)
		require.NoError(t, db.Close())

		saved, err := os.ReadFile(path) //nolint:gosec // Test path from t.TempDir()
		require.NoError(t, err)
		assert.Equal(t, "id,name\n1,bob\n", string(saved))
	})

	t.Run("export mode", func(t *testing.T) {
		t.Parallel()

		path := autoSaveSource(t, "id,name\n1,alice\n")
		outputDir := t.TempDir()
		db := openAutoSave(t, path, func(b *DBBuilder) *DBBuilder { return b.EnableAutoSaveOnCommit(outputDir) })

		_, err := db.ExecContext(t.Context(), "UPDATE users SET name = 'bob'")
		require.NoError(t, err)
		require.NoError(t, db.Close())

		saved, err := os.ReadFile(filepath.Join(outputDir, "users.csv")) //nolint:gosec // Test path from t.TempDir()
		require.NoError(t, err)
		assert.Equal(t, "id,name\n1,bob\n", string(saved))
	})

	t.Run("a commit and a bare statement both land", func(t *testing.T) {
		t.Parallel()

		path := autoSaveSource(t, "id,name\n1,alice\n2,carol\n")
		db := openAutoSave(t, path, func(b *DBBuilder) *DBBuilder { return b.EnableAutoSaveOnCommit("") })

		tx, err := db.BeginTx(t.Context(), nil)
		require.NoError(t, err)
		_, err = tx.ExecContext(t.Context(), "UPDATE users SET name = 'bob' WHERE id = 1")
		require.NoError(t, err)
		require.NoError(t, tx.Commit())

		committed, err := os.ReadFile(path) //nolint:gosec // Test path from t.TempDir()
		require.NoError(t, err)
		assert.Equal(t, "id,name\n1,bob\n2,carol\n", string(committed), "the commit saves immediately")

		_, err = db.ExecContext(t.Context(), "UPDATE users SET name = 'dave' WHERE id = 2")
		require.NoError(t, err)
		require.NoError(t, db.Close())

		saved, err := os.ReadFile(path) //nolint:gosec // Test path from t.TempDir()
		require.NoError(t, err)
		assert.Equal(t, "id,name\n1,bob\n2,dave\n", string(saved))
	})

	t.Run("a rolled back transaction stays out", func(t *testing.T) {
		t.Parallel()

		path := autoSaveSource(t, "id,name\n1,alice\n2,carol\n")
		db := openAutoSave(t, path, func(b *DBBuilder) *DBBuilder { return b.EnableAutoSaveOnCommit("") })

		_, err := db.ExecContext(t.Context(), "UPDATE users SET name = 'bob' WHERE id = 1")
		require.NoError(t, err)

		tx, err := db.BeginTx(t.Context(), nil)
		require.NoError(t, err)
		_, err = tx.ExecContext(t.Context(), "UPDATE users SET name = 'dave' WHERE id = 2")
		require.NoError(t, err)
		require.NoError(t, tx.Rollback())
		require.NoError(t, db.Close())

		saved, err := os.ReadFile(path) //nolint:gosec // Test path from t.TempDir()
		require.NoError(t, err)
		assert.Equal(t, "id,name\n1,bob\n2,carol\n", string(saved))
	})
}

// TestAutoSaveOverwriteKeepsLineEnding pins that a save in place writes back the
// terminator the file already used.
//
// It did not: every record was written with "\n" whatever the source used, so a
// CRLF file came back LF throughout. A caller who edited one row got a file
// whose every line had changed — a whole-file diff in a repository configured
// for CRLF, and a file the tools that read it no longer saw as they had.
func TestAutoSaveOverwriteKeepsLineEnding(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		file    string
		content string
		update  string
		want    string
	}{
		{
			name:    "CSV keeps CRLF",
			file:    "crlf.csv",
			content: "id,v\r\n1,a\r\n2,b\r\n",
			update:  "UPDATE crlf SET v='x' WHERE id=1",
			want:    "id,v\r\n1,x\r\n2,b\r\n",
		},
		{
			name:    "CSV keeps LF",
			file:    "lf.csv",
			content: "id,v\n1,a\n2,b\n",
			update:  "UPDATE lf SET v='x' WHERE id=1",
			want:    "id,v\n1,x\n2,b\n",
		},
		{
			name:    "TSV keeps CRLF",
			file:    "crlf.tsv",
			content: "id\tv\r\n1\ta\r\n2\tb\r\n",
			update:  "UPDATE crlf SET v='x' WHERE id=1",
			want:    "id\tv\r\n1\tx\r\n2\tb\r\n",
		},
		{
			name:    "LTSV keeps CRLF",
			file:    "crlf.ltsv",
			content: "id:1\tv:a\r\nid:2\tv:b\r\n",
			update:  "UPDATE crlf SET v='x' WHERE id=1",
			want:    "id:1\tv:x\r\nid:2\tv:b\r\n",
		},
		{
			// The parser reads a CR-terminated file as lines rather than as one
			// very long line, so the save has to be able to put CR back. It could
			// not: the count only looked for "\n", so a file with none came back
			// rewritten line by line.
			name:    "CSV keeps a lone CR",
			file:    "cr.csv",
			content: "id,v\r1,a\r2,b\r",
			update:  "UPDATE cr SET v='x' WHERE id=1",
			want:    "id,v\r1,x\r2,b\r",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			path := filepath.Join(t.TempDir(), tt.file)
			require.NoError(t, os.WriteFile(path, []byte(tt.content), 0o600))

			require.NoError(t, autoSaveOverwrite(t, []string{path}, tt.update))

			got, err := os.ReadFile(path) //nolint:gosec // Test path from t.TempDir()
			require.NoError(t, err)
			assert.Equal(t, tt.want, string(got), "only the edited row may differ from what was there")
		})
	}
}

// TestAutoSaveOverwriteKeepsLineEndingUnderCompression checks that the
// terminator is read from the bytes inside the codec, not from the archive.
func TestAutoSaveOverwriteKeepsLineEndingUnderCompression(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "crlf.csv.gz")
	file, err := os.Create(path) //nolint:gosec // Test path from t.TempDir()
	require.NoError(t, err)
	gz := gzip.NewWriter(file)
	_, err = gz.Write([]byte("id,v\r\n1,a\r\n2,b\r\n"))
	require.NoError(t, err)
	require.NoError(t, gz.Close())
	require.NoError(t, file.Close())

	require.NoError(t, autoSaveOverwrite(t, []string{path}, "UPDATE crlf SET v='x' WHERE id=1"))

	reopened, err := os.Open(path) //nolint:gosec // Test path from t.TempDir()
	require.NoError(t, err)
	defer reopened.Close()
	reader, err := gzip.NewReader(reopened)
	require.NoError(t, err)
	defer reader.Close()

	decompressed, err := io.ReadAll(reader)
	require.NoError(t, err)
	assert.Equal(t, "id,v\r\n1,x\r\n2,b\r\n", string(decompressed))
}

// TestDumpDatabase_WithLineEnding covers the option on a dump to a new
// destination, where there is no existing file to take the terminator from.
func TestDumpDatabase_WithLineEnding(t *testing.T) {
	t.Parallel()

	source := filepath.Join(t.TempDir(), "users.csv")
	require.NoError(t, os.WriteFile(source, []byte("id,v\n1,a\n"), 0o600))

	db, err := Open(source)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	outputDir := t.TempDir()
	require.NoError(t, DumpDatabase(db, outputDir, NewDumpOptions().WithLineEnding(LineEndingCRLF)))

	got, err := os.ReadFile(filepath.Join(outputDir, "users.csv")) //nolint:gosec // Test path from t.TempDir()
	require.NoError(t, err)
	assert.Equal(t, "id,v\r\n1,a\r\n", string(got))
}

// TestNewDumpOptions_DefaultsToLF pins the default, which is what a save wrote
// before the option existed.
func TestNewDumpOptions_DefaultsToLF(t *testing.T) {
	t.Parallel()

	assert.Equal(t, LineEndingLF, NewDumpOptions().LineEnding)
	assert.Equal(t, LineEndingCRLF, NewDumpOptions().WithLineEnding(LineEndingCRLF).LineEnding)
}

// TestSaveLineEndingByDestination pins which save reads a source's terminator
// and which does not, for every way of writing back over the file a table was
// loaded from.
//
// Only the in-place mode reads it. A dump is an export: it writes the same bytes
// whatever already sits in the destination, so pointing one at the source
// directory replaces a CRLF file with an LF copy. That is a defensible split —
// an export whose output depended on the destination's contents would write
// different bytes on its second run — but it is not what a caller expects from
// "save it back where it came from", so the three are pinned side by side here
// and the README names the mode rather than describing every overwrite.
func TestSaveLineEndingByDestination(t *testing.T) {
	t.Parallel()

	const source = "id,v\r\n1,a\r\n2,b\r\n"
	const update = "UPDATE crlf SET v='x' WHERE id=1"

	newSource := func(t *testing.T) string {
		t.Helper()
		path := filepath.Join(t.TempDir(), "crlf.csv")
		require.NoError(t, os.WriteFile(path, []byte(source), 0o600))
		return path
	}

	t.Run("in place keeps CRLF", func(t *testing.T) {
		t.Parallel()

		path := newSource(t)
		require.NoError(t, autoSaveOverwrite(t, []string{path}, update))

		got, err := os.ReadFile(path) //nolint:gosec // Test path from t.TempDir()
		require.NoError(t, err)
		assert.Equal(t, "id,v\r\n1,x\r\n2,b\r\n", string(got))
	})

	t.Run("auto-save into the source directory writes LF", func(t *testing.T) {
		t.Parallel()

		path := newSource(t)
		ctx := t.Context()
		validated, err := buildForTest(ctx, NewBuilder().AddPath(path).EnableAutoSave(filepath.Dir(path)))
		require.NoError(t, err)
		db, err := validated.Open(ctx)
		require.NoError(t, err)
		_, err = db.ExecContext(ctx, update)
		require.NoError(t, err)
		require.NoError(t, db.Close())

		got, err := os.ReadFile(path) //nolint:gosec // Test path from t.TempDir()
		require.NoError(t, err)
		assert.Equal(t, "id,v\n1,x\n2,b\n", string(got))
	})

	t.Run("dump into the source directory writes LF", func(t *testing.T) {
		t.Parallel()

		path := newSource(t)
		ctx := t.Context()
		db, err := Open(path)
		require.NoError(t, err)
		t.Cleanup(func() { _ = db.Close() })
		_, err = db.ExecContext(ctx, update)
		require.NoError(t, err)
		require.NoError(t, DumpDatabase(db, filepath.Dir(path)))

		got, err := os.ReadFile(path) //nolint:gosec // Test path from t.TempDir()
		require.NoError(t, err)
		assert.Equal(t, "id,v\n1,x\n2,b\n", string(got))
	})

	t.Run("auto-save into a directory is told the terminator instead", func(t *testing.T) {
		t.Parallel()

		path := newSource(t)
		ctx := t.Context()
		validated, err := buildForTest(

			ctx, NewBuilder().
				AddPath(path).
				EnableAutoSave(filepath.Dir(path), NewDumpOptions().WithLineEnding(LineEndingCRLF)))

		require.NoError(t, err)
		db, err := validated.Open(ctx)
		require.NoError(t, err)
		_, err = db.ExecContext(ctx, update)
		require.NoError(t, err)
		require.NoError(t, db.Close())

		got, err := os.ReadFile(path) //nolint:gosec // Test path from t.TempDir()
		require.NoError(t, err)
		assert.Equal(t, "id,v\r\n1,x\r\n2,b\r\n", string(got))
	})

	t.Run("an export is told the terminator instead", func(t *testing.T) {
		t.Parallel()

		path := newSource(t)
		ctx := t.Context()
		db, err := Open(path)
		require.NoError(t, err)
		t.Cleanup(func() { _ = db.Close() })
		_, err = db.ExecContext(ctx, update)
		require.NoError(t, err)
		require.NoError(t, DumpDatabase(db, filepath.Dir(path), NewDumpOptions().WithLineEnding(LineEndingCRLF)))

		got, err := os.ReadFile(path) //nolint:gosec // Test path from t.TempDir()
		require.NoError(t, err)
		assert.Equal(t, "id,v\r\n1,x\r\n2,b\r\n", string(got))
	})
}

// TestAutoSaveOverwriteWithNoStatementIsByteIdentical pins the property the
// terminator detection exists for, stated as an invariant rather than as one
// terminator at a time: a database nobody wrote to has nothing to change on
// disk, so the file has to come back exactly as it was.
func TestAutoSaveOverwriteWithNoStatementIsByteIdentical(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		file    string
		content string
	}{
		{name: "LF", file: "lf.csv", content: "id,v\n1,a\n2,b\n"},
		{name: "CRLF", file: "crlf.csv", content: "id,v\r\n1,a\r\n2,b\r\n"},
		{name: "lone CR", file: "cr.csv", content: "id,v\r1,a\r2,b\r"},
		{name: "quoted CR is data, not a terminator", file: "quoted.csv", content: "id,v\n1,\"a\rb\"\n2,c\n"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			path := filepath.Join(t.TempDir(), tt.file)
			require.NoError(t, os.WriteFile(path, []byte(tt.content), 0o600))

			require.NoError(t, autoSaveOverwrite(t, []string{path}))

			got, err := os.ReadFile(path) //nolint:gosec // Test path from t.TempDir()
			require.NoError(t, err)
			assert.Equal(t, tt.content, string(got))
		})
	}
}

// autoSaveOverwrite opens path with auto-save in overwrite mode, runs stmts, and
// closes, returning the close error. Closing is what performs the save.
func autoSaveOverwrite(t *testing.T, paths []string, stmts ...string) error {
	t.Helper()

	ctx := t.Context()
	builder := NewBuilder()
	for _, p := range paths {
		builder = builder.AddPath(p)
	}
	validated, err := buildForTest(ctx, builder.EnableAutoSave(""))
	require.NoError(t, err)

	db, err := validated.Open(ctx)
	require.NoError(t, err)

	for _, stmt := range stmts {
		_, execErr := db.ExecContext(ctx, stmt)
		require.NoError(t, execErr)
	}
	return db.Close()
}

func dirEntries(t *testing.T, dir string) []string {
	t.Helper()

	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		names = append(names, e.Name())
	}
	sort.Strings(names)
	return names
}

// TestAutoSaveOverwriteKeepsSourceFormat pins that overwrite mode writes each
// table back to the file it came from, in that file's own format.
//
// It did not: overwrite mode handed the whole database to DumpDatabase with the
// output format from the auto-save options, which defaults to CSV. A .tsv source
// therefore got a new .csv beside it holding the change, while the .tsv the
// caller had asked to overwrite still held the old rows — the save went to a file
// nobody named, and the file that was named went stale.
func TestAutoSaveOverwriteKeepsSourceFormat(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		file    string
		content string
		want    string
	}{
		{name: "csv", file: "data.csv", content: "id,name\n1,alice\n", want: "id,name\n1,bob\n"},
		{name: "tsv", file: "data.tsv", content: "id\tname\n1\talice\n", want: "id\tname\n1\tbob\n"},
		{name: "ltsv", file: "data.ltsv", content: "id:1\tname:alice\n", want: "id:1\tname:bob\n"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			dir := t.TempDir()
			src := filepath.Join(dir, tt.file)
			require.NoError(t, os.WriteFile(src, []byte(tt.content), 0o600))

			require.NoError(t, autoSaveOverwrite(t, []string{src}, "UPDATE data SET name = 'bob'"))

			assert.Equal(t, []string{tt.file}, dirEntries(t, dir), "overwrite mode writes no file the caller did not open")

			got, err := os.ReadFile(src) //nolint:gosec // Test path from t.TempDir()
			require.NoError(t, err)
			assert.Equal(t, tt.want, string(got))
		})
	}
}

// TestAutoSaveOverwriteKeepsCompression pins that a compressed source is written
// back compressed, and in place. A .csv.gz source used to get a plain .csv beside
// it while the archive kept the old rows.
func TestAutoSaveOverwriteKeepsCompression(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	dir := t.TempDir()

	// Build the fixture through the dump path so it is a real archive.
	plain := filepath.Join(dir, "seed.csv")
	require.NoError(t, os.WriteFile(plain, []byte("id,name\n1,alice\n"), 0o600))
	seedDB, err := OpenContext(ctx, plain)
	require.NoError(t, err)
	gzDir := filepath.Join(dir, "gz")
	require.NoError(t, DumpDatabase(seedDB, gzDir, NewDumpOptions().WithCompression(CompressionGZ)))
	require.NoError(t, seedDB.Close())

	src := filepath.Join(gzDir, "seed.csv.gz")
	require.NoError(t, autoSaveOverwrite(t, []string{src}, "UPDATE seed SET name = 'bob'"))

	assert.Equal(t, []string{"seed.csv.gz"}, dirEntries(t, gzDir), "the archive is replaced, not sidestepped")

	// Reading it back is what proves it is still a gzip archive holding the change.
	reloaded, err := OpenContext(ctx, src)
	require.NoError(t, err)
	defer reloaded.Close()

	var name string
	require.NoError(t, reloaded.QueryRowContext(ctx, "SELECT name FROM seed").Scan(&name))
	assert.Equal(t, "bob", name)
}

// TestAutoSaveOverwriteAcrossDirectories pins that each source is written back to
// its own directory. The output directory was taken from the first source path, so
// every table landed next to whichever file happened to be loaded first.
func TestAutoSaveOverwriteAcrossDirectories(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	dirA := filepath.Join(root, "a")
	dirB := filepath.Join(root, "b")
	require.NoError(t, os.MkdirAll(dirA, 0o750))
	require.NoError(t, os.MkdirAll(dirB, 0o750))

	srcA := filepath.Join(dirA, "x.csv")
	srcB := filepath.Join(dirB, "y.csv")
	require.NoError(t, os.WriteFile(srcA, []byte("id,name\n1,alice\n"), 0o600))
	require.NoError(t, os.WriteFile(srcB, []byte("id,name\n2,carol\n"), 0o600))

	require.NoError(t, autoSaveOverwrite(t, []string{srcA, srcB},
		"UPDATE x SET name = 'bob'", "UPDATE y SET name = 'dave'"))

	assert.Equal(t, []string{"x.csv"}, dirEntries(t, dirA))
	assert.Equal(t, []string{"y.csv"}, dirEntries(t, dirB))

	gotA, err := os.ReadFile(srcA) //nolint:gosec // Test path from t.TempDir()
	require.NoError(t, err)
	assert.Equal(t, "id,name\n1,bob\n", string(gotA))

	gotB, err := os.ReadFile(srcB) //nolint:gosec // Test path from t.TempDir()
	require.NoError(t, err)
	assert.Equal(t, "id,name\n2,dave\n", string(gotB))
}

// TestAutoSaveOverwriteLeavesNewTablesAlone pins that a table the caller created
// is not written anywhere. Overwrite mode is defined by the files that were
// opened, and a new table is not one of them; it used to appear as a new file in
// the source directory.
func TestAutoSaveOverwriteLeavesNewTablesAlone(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	src := filepath.Join(dir, "data.csv")
	require.NoError(t, os.WriteFile(src, []byte("id,name\n1,alice\n"), 0o600))

	require.NoError(t, autoSaveOverwrite(t, []string{src},
		"CREATE TABLE scratch (a TEXT)",
		"INSERT INTO scratch VALUES ('temporary')",
		"UPDATE data SET name = 'bob'"))

	assert.Equal(t, []string{"data.csv"}, dirEntries(t, dir))

	got, err := os.ReadFile(src) //nolint:gosec // Test path from t.TempDir()
	require.NoError(t, err)
	assert.Equal(t, "id,name\n1,bob\n", string(got))
}

// TestAutoSaveOverwriteRefusesFormatItCannotWrite pins that a source in a format
// with no writer fails the save instead of quietly becoming a CSV beside it.
func TestAutoSaveOverwriteRefusesFormatItCannotWrite(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		file    string
		content string
	}{
		{name: "json", file: "records.json", content: `[{"id":1}]`},
		{name: "jsonl", file: "records.jsonl", content: "{\"id\":1}\n"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			dir := t.TempDir()
			src := filepath.Join(dir, tt.file)
			require.NoError(t, os.WriteFile(src, []byte(tt.content), 0o600))

			// The extension is the whole of the answer, so Build is where the
			// caller hears it: no database is opened and no file is touched.
			_, err := buildForTest(t.Context(), NewBuilder().AddPath(src).EnableAutoSave(""))
			require.Error(t, err)
			assert.ErrorIs(t, err, ErrUnsupportedFormat)
			assert.Contains(t, err.Error(), tt.file)

			assert.Equal(t, []string{tt.file}, dirEntries(t, dir), "nothing else may be written")

			got, readErr := os.ReadFile(src) //nolint:gosec // Test path from t.TempDir()
			require.NoError(t, readErr)
			assert.Equal(t, tt.content, string(got), "the source is left as it was")
		})
	}
}

// TestAutoSaveOverwriteXLSX pins the two shapes an Excel source can have. A
// workbook of one sheet is written back to itself. A workbook of several sheets
// became one CSV per sheet next to it, which is not the file the caller opened;
// it now fails and says so, because the XLSX writer holds one sheet per file.
func TestAutoSaveOverwriteXLSX(t *testing.T) {
	t.Parallel()

	t.Run("a workbook of one sheet is written back to itself", func(t *testing.T) {
		t.Parallel()

		ctx := t.Context()
		dir := t.TempDir()

		// Build a single-sheet workbook through the dump path.
		plain := filepath.Join(dir, "book.csv")
		require.NoError(t, os.WriteFile(plain, []byte("id,name\n1,alice\n"), 0o600))
		seedDB, err := OpenContext(ctx, plain)
		require.NoError(t, err)
		bookDir := filepath.Join(dir, "book")
		require.NoError(t, DumpDatabase(seedDB, bookDir, NewDumpOptions().WithFormat(OutputFormatXLSX)))
		require.NoError(t, seedDB.Close())

		src := filepath.Join(bookDir, "book.xlsx")
		require.NoError(t, autoSaveOverwrite(t, []string{src}, "UPDATE book SET name = 'bob'"))

		assert.Equal(t, []string{"book.xlsx"}, dirEntries(t, bookDir))

		reloaded, err := OpenContext(ctx, src)
		require.NoError(t, err)
		defer reloaded.Close()

		var name string
		require.NoError(t, reloaded.QueryRowContext(ctx, "SELECT name FROM book").Scan(&name))
		assert.Equal(t, "bob", name)
	})

	t.Run("a sheet keeps its name across a round trip", func(t *testing.T) {
		t.Parallel()

		ctx := t.Context()
		dir := t.TempDir()
		src := filepath.Join(dir, "book.xlsx")
		writeWorkbook(t, src, map[string][][]string{
			"Orders": {{"id", "name"}, {"1", "alice"}},
		})

		require.NoError(t, autoSaveOverwrite(t, []string{src}, "UPDATE book_Orders SET name = 'bob'"))

		assert.Equal(t, []string{"Orders"}, workbookSheets(t, src),
			"overwriting a workbook in place must not rename its sheet")

		// The name has to survive repeatedly, not just once: a prefix added on
		// every save accumulates until Excel's 31-rune sheet name limit truncates it.
		require.NoError(t, autoSaveOverwrite(t, []string{src}, "UPDATE book_Orders SET name = 'carol'"))
		assert.Equal(t, []string{"Orders"}, workbookSheets(t, src))

		reloaded, err := OpenContext(ctx, src)
		require.NoError(t, err)
		defer reloaded.Close()
		var name string
		require.NoError(t, reloaded.QueryRowContext(ctx, "SELECT name FROM book_Orders").Scan(&name))
		assert.Equal(t, "carol", name)
	})

	t.Run("a workbook of several sheets is written back to itself", func(t *testing.T) {
		t.Parallel()

		ctx := t.Context()
		dir := t.TempDir()
		src := filepath.Join(dir, "book.xlsx")
		writeWorkbook(t, src, map[string][][]string{
			"Orders":    {{"id", "name"}, {"1", "alice"}},
			"Customers": {{"id", "city"}, {"1", "tokyo"}},
		})

		require.NoError(t, autoSaveOverwrite(t, []string{src},
			"UPDATE book_Orders SET name = 'bob'",
			"UPDATE book_Customers SET city = 'osaka'"))

		assert.Equal(t, []string{"Customers", "Orders"}, workbookSheets(t, src),
			"every sheet has to come back, under its own name")
		assert.Equal(t, []string{"book.xlsx"}, dirEntries(t, dir), "nothing else may be written")

		reloaded, err := OpenContext(ctx, src)
		require.NoError(t, err)
		defer reloaded.Close()
		var name, city string
		require.NoError(t, reloaded.QueryRowContext(ctx, "SELECT name FROM book_Orders").Scan(&name))
		require.NoError(t, reloaded.QueryRowContext(ctx, "SELECT city FROM book_Customers").Scan(&city))
		assert.Equal(t, "bob", name)
		assert.Equal(t, "osaka", city)
	})

	t.Run("a workbook keeps the sheets of a sibling whose name it prefixes out", func(t *testing.T) {
		t.Parallel()

		ctx := t.Context()
		dir := t.TempDir()
		book := filepath.Join(dir, "book.xlsx")
		sibling := filepath.Join(dir, "book_v2.xlsx")
		writeWorkbook(t, book, map[string][][]string{
			"Orders": {{"id", "name"}, {"1", "alice"}},
		})
		writeWorkbook(t, sibling, map[string][][]string{
			"Orders": {{"id", "name"}, {"2", "dave"}},
		})

		require.NoError(t, autoSaveOverwrite(t, []string{book, sibling},
			"UPDATE book_Orders SET name = 'bob'",
			"UPDATE book_v2_Orders SET name = 'erin'"))

		assert.Equal(t, []string{"Orders"}, workbookSheets(t, book),
			"book.xlsx holds its own sheet only: book_v2.xlsx's tables are named inside book's prefix space, but they are not book's")
		assert.Equal(t, []string{"Orders"}, workbookSheets(t, sibling))

		reloaded, err := OpenContext(ctx, book, sibling)
		require.NoError(t, err)
		defer reloaded.Close()

		var name string
		require.NoError(t, reloaded.QueryRowContext(ctx, "SELECT name FROM book_Orders").Scan(&name))
		assert.Equal(t, "bob", name)
		require.NoError(t, reloaded.QueryRowContext(ctx, "SELECT name FROM book_v2_Orders").Scan(&name))
		assert.Equal(t, "erin", name)
	})

	t.Run("a workbook keeps out a sibling of another format whose name it prefixes", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()
		book := filepath.Join(dir, "book.xlsx")
		sibling := filepath.Join(dir, "book_v2.csv")
		writeWorkbook(t, book, map[string][][]string{
			"Orders": {{"id", "name"}, {"1", "alice"}},
		})
		require.NoError(t, os.WriteFile(sibling, []byte("id,name\n2,dave\n"), 0o600))

		require.NoError(t, autoSaveOverwrite(t, []string{book, sibling},
			"UPDATE book_Orders SET name = 'bob'"))

		assert.Equal(t, []string{"Orders"}, workbookSheets(t, book),
			"a CSV sibling loads as one table named inside the workbook's prefix space, and it is not the workbook's either")
	})

	t.Run("a compressed workbook of several sheets round-trips", func(t *testing.T) {
		t.Parallel()

		ctx := t.Context()
		dir := t.TempDir()
		plain := filepath.Join(dir, "book.xlsx")
		writeWorkbook(t, plain, map[string][][]string{
			"Orders":    {{"id", "name"}, {"1", "alice"}},
			"Customers": {{"id", "city"}, {"1", "tokyo"}},
		})

		// A compressed source has to be written back through its own codec, and
		// the workbook still has to arrive whole on the other side of it.
		raw, err := os.ReadFile(plain) //nolint:gosec // plain is under t.TempDir()
		require.NoError(t, err)
		require.NoError(t, os.Remove(plain))

		src := filepath.Join(dir, "book.xlsx.gz")
		out, err := os.Create(src) //nolint:gosec // src is under t.TempDir()
		require.NoError(t, err)
		gz := gzip.NewWriter(out)
		_, err = gz.Write(raw)
		require.NoError(t, err)
		require.NoError(t, gz.Close())
		require.NoError(t, out.Close())

		require.NoError(t, autoSaveOverwrite(t, []string{src},
			"UPDATE book_Orders SET name = 'bob'",
			"UPDATE book_Customers SET city = 'osaka'"))

		assert.Equal(t, []string{"book.xlsx.gz"}, dirEntries(t, dir), "nothing else may be written")

		reloaded, err := OpenContext(ctx, src)
		require.NoError(t, err)
		defer reloaded.Close()
		var name, city string
		require.NoError(t, reloaded.QueryRowContext(ctx, "SELECT name FROM book_Orders").Scan(&name))
		require.NoError(t, reloaded.QueryRowContext(ctx, "SELECT city FROM book_Customers").Scan(&city))
		assert.Equal(t, "bob", name)
		assert.Equal(t, "osaka", city)
	})

	t.Run("two tables that would share a sheet name are refused", func(t *testing.T) {
		t.Parallel()

		ctx := t.Context()
		dir := t.TempDir()
		src := filepath.Join(dir, "book.xlsx")
		writeWorkbook(t, src, map[string][][]string{
			"Orders": {{"id", "name"}, {"1", "alice"}},
		})
		before, err := os.ReadFile(src) //nolint:gosec // src is under t.TempDir()
		require.NoError(t, err)

		validated, err := buildForTest(ctx, NewBuilder().AddPath(src).EnableAutoSave(""))
		require.NoError(t, err)
		db, err := validated.Open(ctx)
		require.NoError(t, err)

		// Excel caps a sheet name at 31 runes, so two tables of this workbook
		// whose names agree for the first 31 and differ after map to one sheet.
		// excelize's NewSheet returns the existing index rather than erroring, so
		// the second table used to overwrite the first's sheet and one table's
		// rows vanished while the save reported success.
		stem := strings.Repeat("a", excelSheetNameMaxLen)
		for _, suffix := range []string{stem + "X", stem + "Y"} {
			_, execErr := db.ExecContext(ctx, "CREATE TABLE `book_"+suffix+"` (id TEXT)")
			require.NoError(t, execErr)
		}

		err = db.Close()
		require.Error(t, err, "a save that cannot keep both tables must not report success")
		assert.ErrorIs(t, err, ErrUnsupportedFormat)
		// Both table names, not just the sheet: the error's job is to say which
		// two tables collided, and asserting only the sheet would pass an error
		// that named neither.
		assert.Contains(t, err.Error(), "book_"+stem+"X")
		assert.Contains(t, err.Error(), "book_"+stem+"Y")
		assert.Contains(t, err.Error(), stem, "the error names the sheet the two tables collide on")

		after, err := os.ReadFile(src) //nolint:gosec // src is under t.TempDir()
		require.NoError(t, err)
		assert.Equal(t, before, after, "the workbook must be left as it was")
	})

	t.Run("a workbook read from a fixture round-trips whole", func(t *testing.T) {
		t.Parallel()

		ctx := t.Context()
		dir := t.TempDir()
		src := filepath.Join(dir, "book.xlsx")
		data, err := os.ReadFile(filepath.Join("testdata", "excel", "sample.xlsx"))
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(src, data, 0o600)) //nolint:gosec // src is under t.TempDir()

		before := workbookSheets(t, src)
		require.NoError(t, autoSaveOverwrite(t, []string{src}, "UPDATE book_Sheet1 SET name = 'bob'"))

		assert.Equal(t, before, workbookSheets(t, src), "the sheets have to come back as they were")
		assert.Equal(t, []string{"book.xlsx"}, dirEntries(t, dir), "nothing else may be written")

		reloaded, err := OpenContext(ctx, src)
		require.NoError(t, err)
		defer reloaded.Close()
		var name string
		require.NoError(t, reloaded.QueryRowContext(ctx, "SELECT name FROM book_Sheet1").Scan(&name))
		assert.Equal(t, "bob", name)
	})
}

// writeWorkbook builds an xlsx at path holding the given sheets. Each sheet's
// first row is its header.
func writeWorkbook(t *testing.T, path string, sheets map[string][][]string) {
	t.Helper()

	f := excelize.NewFile()
	defer func() {
		_ = f.Close()
	}()

	names := make([]string, 0, len(sheets))
	for name := range sheets {
		names = append(names, name)
	}
	sort.Strings(names)

	for _, name := range names {
		if _, err := f.NewSheet(name); err != nil {
			t.Fatal(err)
		}
		for r, row := range sheets[name] {
			for c, value := range row {
				cell, err := excelize.CoordinatesToCellName(c+1, r+1)
				require.NoError(t, err)
				require.NoError(t, f.SetCellValue(name, cell, value))
			}
		}
	}
	require.NoError(t, f.DeleteSheet(defaultSheetName))
	require.NoError(t, f.SaveAs(path))
}

// workbookSheets returns the sheet names of the workbook at path, sorted.
func workbookSheets(t *testing.T, path string) []string {
	t.Helper()

	f, err := excelize.OpenFile(path)
	require.NoError(t, err)
	defer func() {
		_ = f.Close()
	}()

	names := f.GetSheetList()
	sort.Strings(names)
	return names
}

// TestAutoSaveOverwriteKeepsTheFileItWasGiven pins overwrite mode's core
// promise from the file's side: the bytes go back into the path that was
// opened, under the name it already had, or the save fails and the file is
// left alone. Nothing covered either half for a name that is not already a
// valid SQL identifier, and the table name is derived from the file name by a
// mapping that is not reversible.
func TestAutoSaveOverwriteKeepsTheFileItWasGiven(t *testing.T) {
	t.Parallel()

	// Each name loads as a table spelled differently from the file: "my-data"
	// becomes my_data, "sales report" becomes sales_report, and a name starting
	// with a digit gains a prefix. The file must keep its own spelling.
	names := []string{
		"my-data.csv",
		"sales report.csv",
		"2024.q1.csv",
		"café.csv",
	}

	for _, name := range names {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			dir := t.TempDir()
			src := filepath.Join(dir, name)
			require.NoError(t, os.WriteFile(src, []byte("id,v\n1,a\n"), 0o600))

			validated, err := buildForTest(ctx, NewBuilder().AddPath(src).EnableAutoSave(""))
			require.NoError(t, err)
			db, err := validated.Open(ctx)
			require.NoError(t, err)

			tables, err := getSQLiteTableNames(context.Background(), db)
			require.NoError(t, err)
			require.Len(t, tables, 1)

			//nolint:gosec // the table name comes from the file this test just wrote
			_, err = db.ExecContext(ctx, "UPDATE `"+tables[0]+"` SET v = 'b'")
			require.NoError(t, err)
			require.NoError(t, db.Close())

			assert.Equal(t, []string{name}, dirEntries(t, dir),
				"the save goes back to the file that was opened, under its own name")

			content, err := os.ReadFile(src) //nolint:gosec // src is under t.TempDir()
			require.NoError(t, err)
			assert.Equal(t, "id,v\n1,b\n", string(content))
		})
	}
}

// TestAutoSaveOverwriteRefusesCodecItCannotWrite pins the other half: bzip2 is
// read but has no writer in this library, so a .bz2 source cannot be written
// back. The save has to say so and leave the file untouched rather than report
// success over a file it never wrote.
func TestAutoSaveOverwriteRefusesCodecItCannotWrite(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	src := filepath.Join(dir, "products.tsv.bz2")
	fixture, err := os.ReadFile(filepath.Join("testdata", "products.tsv.bz2"))
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(src, fixture, 0o600)) //nolint:gosec // src is under t.TempDir()

	_, err = buildForTest(t.Context(), NewBuilder().AddPath(src).EnableAutoSave(""))
	require.Error(t, err, "a codec this package cannot write must not report a successful save")
	assert.Contains(t, err.Error(), "bzip2")
	// The codec is read off the name, so the refusal comes from Build, before
	// there is a database to change or a file to replace. ErrUnsupportedFormat
	// is the sentinel it carries, the same one the writer reports when a dump
	// asks for bzip2; TestDumpDatabase_RefusesACodecItCannotWrite covers the
	// rest of that chain.
	assert.ErrorIs(t, err, ErrUnsupportedFormat)
	assert.ErrorIs(t, err, codec.ErrNoBZ2Writer)

	after, err := os.ReadFile(src) //nolint:gosec // src is under t.TempDir()
	require.NoError(t, err)
	assert.Equal(t, fixture, after, "the source must be left byte for byte as it was")
	assert.Equal(t, []string{"products.tsv.bz2"}, dirEntries(t, dir), "nothing else may be written")
}

// TestAutoSaveOverwriteLongSourceName pins the auto-save form of the staged-name
// bug. Overwrite mode is where the failure costs the caller their edit: the save
// runs from Close, after the change is in the database and with nowhere else for
// it to go.
func TestAutoSaveOverwriteLongSourceName(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	base := strings.Repeat("s", 246) + ".csv"
	src := filepath.Join(dir, base)
	if err := os.WriteFile(src, []byte("id,name\n1,alice\n"), 0o600); err != nil {
		t.Skipf("this filesystem does not accept a %d-byte name: %v", len(base), err)
	}

	table := sanitizeTableName(tableFromFilePath(src))
	require.NoError(t, autoSaveOverwrite(t, []string{src}, `UPDATE "`+table+`" SET name = 'bob'`))

	got, err := os.ReadFile(src) //nolint:gosec // Test path from t.TempDir()
	require.NoError(t, err)
	assert.Equal(t, "id,name\n1,bob\n", string(got))
	assert.Equal(t, []string{base}, dirEntries(t, dir), "no staged file may be left beside the source")
}

// TestAutoSaveCloseWithAnOpenTransaction pins that closing a database with a
// transaction still open returns. It did not: the save reads every table
// through the connector's own connection, an uncommitted write holds the lock
// on the table it touched, and the driver waits for that lock with no deadline
// and no context. The only goroutine that could release it was the one inside
// Close, so a caller whose error path forgot a rollback did not leak a
// connection -- their process stopped.
func TestAutoSaveCloseWithAnOpenTransaction(t *testing.T) {
	t.Parallel()

	// closeWithin runs Close on a goroutine so a Close that never returns fails
	// the test instead of hanging the run until the package timeout.
	closeWithin := func(t *testing.T, db *sql.DB) error {
		t.Helper()

		done := make(chan error, 1)
		go func() { done <- db.Close() }()
		select {
		case err := <-done:
			return err
		case <-time.After(30 * time.Second):
			t.Fatal("db.Close did not return; the save is waiting on a lock it cannot get")
			return nil
		}
	}

	setup := func(t *testing.T, enable func(*DBBuilder) *DBBuilder) (*sql.DB, string) {
		t.Helper()

		dir := t.TempDir()
		src := filepath.Join(dir, "users.csv")
		require.NoError(t, os.WriteFile(src, []byte("id,name\n1,alice\n"), 0o600))

		validated, err := buildForTest(t.Context(), enable(NewBuilder().AddPath(src)))
		require.NoError(t, err)
		db, err := validated.Open(t.Context())
		require.NoError(t, err)
		return db, src
	}

	onClose := func(b *DBBuilder) *DBBuilder { return b.EnableAutoSave("") }
	onCommit := func(b *DBBuilder) *DBBuilder { return b.EnableAutoSaveOnCommit("") }

	for _, tt := range []struct {
		name   string
		enable func(*DBBuilder) *DBBuilder
	}{
		{name: "save on close", enable: onClose},
		{name: "save on commit", enable: onCommit},
	} {
		t.Run("a write left uncommitted stops the save ("+tt.name+")", func(t *testing.T) {
			t.Parallel()

			db, src := setup(t, tt.enable)
			tx, err := db.BeginTx(t.Context(), nil)
			require.NoError(t, err)
			_, err = tx.ExecContext(t.Context(), "INSERT INTO users VALUES (2,'bob')")
			require.NoError(t, err)

			err = closeWithin(t, db)
			require.Error(t, err, "a save that was skipped must be reported, not passed off as done")
			assert.ErrorIs(t, err, ErrDatabaseOperation)
			assert.Contains(t, err.Error(), "transaction")

			got, readErr := os.ReadFile(src) //nolint:gosec // src is under t.TempDir()
			require.NoError(t, readErr)
			assert.Equal(t, "id,name\n1,alice\n", string(got), "nothing uncommitted may reach the file")
		})
	}

	t.Run("a transaction that only read is refused the same way", func(t *testing.T) {
		t.Parallel()

		// Reading takes no write lock, so this one never hung. It is refused
		// all the same: a transaction still open at Close is a caller who is
		// not done with the database, and one rule is easier to rely on than a
		// rule that depends on what the transaction happened to run.
		db, src := setup(t, onClose)
		tx, err := db.BeginTx(t.Context(), nil)
		require.NoError(t, err)
		var n int
		require.NoError(t, tx.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM users").Scan(&n))
		require.Equal(t, 1, n)

		err = closeWithin(t, db)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "transaction")

		got, readErr := os.ReadFile(src) //nolint:gosec // src is under t.TempDir()
		require.NoError(t, readErr)
		assert.Equal(t, "id,name\n1,alice\n", string(got))
	})

	t.Run("a committed transaction saves", func(t *testing.T) {
		t.Parallel()

		db, src := setup(t, onClose)
		tx, err := db.BeginTx(t.Context(), nil)
		require.NoError(t, err)
		_, err = tx.ExecContext(t.Context(), "INSERT INTO users VALUES (2,'bob')")
		require.NoError(t, err)
		require.NoError(t, tx.Commit())

		require.NoError(t, closeWithin(t, db))

		got, readErr := os.ReadFile(src) //nolint:gosec // src is under t.TempDir()
		require.NoError(t, readErr)
		assert.Equal(t, "id,name\n1,alice\n2,bob\n", string(got))
	})

	t.Run("a rolled back transaction saves what the rollback left", func(t *testing.T) {
		t.Parallel()

		db, src := setup(t, onClose)
		_, err := db.ExecContext(t.Context(), "INSERT INTO users VALUES (2,'bob')")
		require.NoError(t, err)
		tx, err := db.BeginTx(t.Context(), nil)
		require.NoError(t, err)
		_, err = tx.ExecContext(t.Context(), "INSERT INTO users VALUES (3,'carol')")
		require.NoError(t, err)
		require.NoError(t, tx.Rollback())

		require.NoError(t, closeWithin(t, db))

		got, readErr := os.ReadFile(src) //nolint:gosec // src is under t.TempDir()
		require.NoError(t, readErr)
		assert.Equal(t, "id,name\n1,alice\n2,bob\n", string(got))
	})

	t.Run("a BEGIN run from a prepared statement stops the save", func(t *testing.T) {
		t.Parallel()

		// database/sql runs a prepared statement against the driver statement
		// rather than against the connection, so this reaches the tracker only
		// if the statement itself carries the reading.
		db, src := setup(t, onClose)
		stmt, err := db.PrepareContext(t.Context(), "BEGIN")
		require.NoError(t, err)
		_, err = stmt.ExecContext(t.Context())
		require.NoError(t, err)
		require.NoError(t, stmt.Close())
		_, err = db.ExecContext(t.Context(), "INSERT INTO users VALUES (2,'bob')")
		require.NoError(t, err)

		err = closeWithin(t, db)
		require.Error(t, err, "a save that was skipped must be reported, not passed off as done")
		assert.ErrorIs(t, err, ErrDatabaseOperation)

		got, readErr := os.ReadFile(src) //nolint:gosec // src is under t.TempDir()
		require.NoError(t, readErr)
		assert.Equal(t, "id,name\n1,alice\n", string(got))
	})

	t.Run("a prepared BEGIN and COMMIT pair still saves", func(t *testing.T) {
		t.Parallel()

		db, src := setup(t, onClose)
		for _, q := range []string{"BEGIN", "INSERT INTO users VALUES (2,'bob')", "COMMIT"} {
			stmt, err := db.PrepareContext(t.Context(), q)
			require.NoError(t, err)
			_, err = stmt.ExecContext(t.Context())
			require.NoError(t, err, q)
			require.NoError(t, stmt.Close())
		}

		require.NoError(t, closeWithin(t, db))

		got, readErr := os.ReadFile(src) //nolint:gosec // src is under t.TempDir()
		require.NoError(t, readErr)
		assert.Equal(t, "id,name\n1,alice\n2,bob\n", string(got))
	})

	t.Run("an unclosed rows iterator still saves", func(t *testing.T) {
		t.Parallel()

		// Rows hold a pooled connection but no transaction and no lock the save
		// waits on, so this has to keep working: a fix that refused whenever a
		// connection was still checked out would break it silently.
		db, src := setup(t, onClose)
		_, err := db.ExecContext(t.Context(), "INSERT INTO users VALUES (2,'bob')")
		require.NoError(t, err)
		rows, err := db.QueryContext(t.Context(), "SELECT * FROM users")
		require.NoError(t, err)
		require.True(t, rows.Next())

		require.NoError(t, closeWithin(t, db))
		require.NoError(t, rows.Err())
		require.NoError(t, rows.Close())

		got, readErr := os.ReadFile(src) //nolint:gosec // src is under t.TempDir()
		require.NoError(t, readErr)
		assert.Equal(t, "id,name\n1,alice\n2,bob\n", string(got))
	})

	// A transaction begun by running the statement never reaches BeginTx, so the
	// count that stops the save has to come from the statement itself. Without
	// it the close reported success and wrote the rows the file already held,
	// because closing the pooled connection rolled the caller's work back first.
	for _, tt := range []struct {
		name  string
		stmts []string
		want  string
		saved bool
	}{
		{name: "a BEGIN left open stops the save", stmts: []string{"BEGIN", "INSERT INTO users VALUES (2,'bob')"}, want: "id,name\n1,alice\n"},
		{name: "a lower case begin counts too", stmts: []string{"  begin ", "INSERT INTO users VALUES (2,'bob')"}, want: "id,name\n1,alice\n"},
		{name: "a qualified BEGIN counts too", stmts: []string{"BEGIN IMMEDIATE", "INSERT INTO users VALUES (2,'bob')"}, want: "id,name\n1,alice\n"},
		{name: "a COMMIT releases it", stmts: []string{"BEGIN", "INSERT INTO users VALUES (2,'bob')", "COMMIT"}, want: "id,name\n1,alice\n2,bob\n", saved: true},
		{name: "an END releases it", stmts: []string{"BEGIN TRANSACTION", "INSERT INTO users VALUES (2,'bob')", "END"}, want: "id,name\n1,alice\n2,bob\n", saved: true},
		{name: "a ROLLBACK releases it", stmts: []string{"BEGIN", "INSERT INTO users VALUES (2,'bob')", "ROLLBACK"}, want: "id,name\n1,alice\n", saved: true},
		{name: "a statement that only starts with the letters is not one", stmts: []string{"UPDATE users SET name='beginning' WHERE id=1"}, want: "id,name\n1,beginning\n", saved: true},
		{name: "a rollback to a savepoint does not release it", stmts: []string{"BEGIN", "SAVEPOINT s", "INSERT INTO users VALUES (2,'bob')", "ROLLBACK TRANSACTION TO SAVEPOINT s"}, want: "id,name\n1,alice\n"},
		// A comment is not part of the statement, so the keyword behind one
		// counts as the same keyword. Reading only from the first non-space
		// character made a commented BEGIN invisible and a commented COMMIT
		// leave a transaction counted open that SQLite had already ended, so
		// the save a caller had committed for was refused.
		{name: "a BEGIN behind a block comment counts", stmts: []string{"/* batch */ BEGIN", "INSERT INTO users VALUES (2,'bob')"}, want: "id,name\n1,alice\n"},
		{name: "a BEGIN behind a line comment counts", stmts: []string{"-- batch\nBEGIN", "INSERT INTO users VALUES (2,'bob')"}, want: "id,name\n1,alice\n"},
		{name: "a COMMIT behind a block comment releases it", stmts: []string{"BEGIN", "INSERT INTO users VALUES (2,'bob')", "/* done */ COMMIT"}, want: "id,name\n1,alice\n2,bob\n", saved: true},
		{name: "a COMMIT behind a line comment releases it", stmts: []string{"BEGIN", "INSERT INTO users VALUES (2,'bob')", "-- done\nCOMMIT"}, want: "id,name\n1,alice\n2,bob\n", saved: true},
		{name: "a ROLLBACK behind stacked comments releases it", stmts: []string{"BEGIN", "INSERT INTO users VALUES (2,'bob')", "-- one\n /* two */\tROLLBACK"}, want: "id,name\n1,alice\n", saved: true},
		{name: "a comment inside a string literal is not one", stmts: []string{"BEGIN", "INSERT INTO users VALUES (2,'/* done */ COMMIT')"}, want: "id,name\n1,alice\n"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			db, src := setup(t, onClose)
			for _, stmt := range tt.stmts {
				_, err := db.ExecContext(t.Context(), stmt)
				require.NoError(t, err, stmt)
			}

			err := closeWithin(t, db)
			if tt.saved {
				require.NoError(t, err)
			} else {
				require.Error(t, err, "a save that was skipped must be reported, not passed off as done")
				assert.ErrorIs(t, err, ErrDatabaseOperation)
				assert.Contains(t, err.Error(), "transaction")
			}

			got, readErr := os.ReadFile(src) //nolint:gosec // src is under t.TempDir()
			require.NoError(t, readErr)
			assert.Equal(t, tt.want, string(got))
		})
	}
}

// TestAutoSaveOverwriteFollowsASymlink pins that a source reached through a
// symbolic link is written back through it. The staged file was renamed onto
// the link itself, so the link became a regular file holding the change and the
// file it named still held the old rows: the save reported success while the
// data the caller meant to update never moved.
func TestAutoSaveOverwriteFollowsASymlink(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	target := filepath.Join(dir, "real.csv")
	require.NoError(t, os.WriteFile(target, []byte("id,name\n1,alice\n"), 0o600))
	link := filepath.Join(dir, "users.csv")
	if err := os.Symlink(target, link); err != nil {
		t.Skipf("this platform does not allow a symlink to be created: %v", err)
	}

	require.NoError(t, autoSaveOverwrite(t, []string{link}, "INSERT INTO users VALUES (2,'bob')"))

	info, err := os.Lstat(link)
	require.NoError(t, err)
	assert.NotZero(t, info.Mode()&os.ModeSymlink, "the link must survive the save")

	got, err := os.ReadFile(target) //nolint:gosec // target is under t.TempDir()
	require.NoError(t, err)
	assert.Equal(t, "id,name\n1,alice\n2,bob\n", string(got), "the file the link names is what receives the row")
	assert.Equal(t, []string{"real.csv", "users.csv"}, dirEntries(t, dir), "no staged file may be left behind")
}

// TestAutoSaveOverwriteRefusesASourceItCannotWriteBeforeOpening pins where a
// source that overwrite mode can never write back is reported. It was reported
// from Close, one file at a time, so a set holding such a source had its earlier
// files replaced before the caller heard about it: half the directory held the
// session's rows and half held the old ones, with nothing on disk saying which
// was which. The extension decides the answer, so Build knows it before any
// database exists and refuses there.
func TestAutoSaveOverwriteRefusesASourceItCannotWriteBeforeOpening(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	csvPath := filepath.Join(dir, "aaa.csv")
	require.NoError(t, os.WriteFile(csvPath, []byte("id,name\n1,alice\n"), 0o600))
	jsonPath := filepath.Join(dir, "zzz.json")
	require.NoError(t, os.WriteFile(jsonPath, []byte(`[{"id":1}]`), 0o600))

	_, err := buildForTest(t.Context(), NewBuilder().AddPath(csvPath).AddPath(jsonPath).EnableAutoSave(""))
	require.Error(t, err, "a set that cannot be saved must be refused before it is loaded")
	assert.ErrorIs(t, err, ErrUnsupportedFormat)
	assert.Contains(t, err.Error(), "zzz.json")

	got, readErr := os.ReadFile(csvPath) //nolint:gosec // csvPath is under t.TempDir()
	require.NoError(t, readErr)
	assert.Equal(t, "id,name\n1,alice\n", string(got), "no file may be replaced by a save that cannot finish")

	t.Run("an output directory is unaffected", func(t *testing.T) {
		t.Parallel()

		// Export mode writes what DumpOptions says into a directory of its own,
		// so a source with no writer is read and written out as CSV and no
		// source file is replaced. The refusal is about overwrite mode only.
		out := filepath.Join(t.TempDir(), "out")
		validated, buildErr := buildForTest(t.Context(), NewBuilder().AddPath(csvPath).AddPath(jsonPath).EnableAutoSave(out))
		require.NoError(t, buildErr)
		db, openErr := validated.Open(t.Context())
		require.NoError(t, openErr)
		require.NoError(t, db.Close())

		assert.Equal(t, []string{"aaa.csv", "zzz.csv"}, dirEntries(t, out))
	})
}

// TestAutoSaveOverwriteXLSXWritesTheTableBackWhereItSat pins that a save writes
// a table back to the rows it was read from.
//
// A sheet may hold a row with no cell in it -- above the header, which is a
// spreadsheet with a gap under its title, or between two records, which is one
// with its rows in blocks. Such a row is not a record and the load passes over
// it, so the table's row N is not the sheet's row N+1. A save that writes the
// header at row 1 and the records under it lands every cell on a row belonging
// to a different record: the comparison that recognizes an untouched cell never
// matches, so every cell is written -- as a string, which is what this writer
// writes -- and a date cell that nothing edited stops being a date and starts
// wearing whichever format the cell it landed on carried. Nothing reports it,
// and a save with no edit at all is enough to do it.
func TestAutoSaveOverwriteXLSXWritesTheTableBackWhereItSat(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name string
		// dataRows is the sheet row each record sits on, in order. The header
		// sits on the row before the first of them.
		headerRow int
		dataRows  []int
	}{
		{name: "no blank row at all", headerRow: 1, dataRows: []int{2, 3, 4}},
		{name: "a blank row above the header", headerRow: 2, dataRows: []int{3, 4, 5}},
		{name: "two blank rows above the header", headerRow: 3, dataRows: []int{4, 5, 6}},
		{name: "a blank row between two records", headerRow: 1, dataRows: []int{2, 3, 5}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			dir := t.TempDir()
			path := filepath.Join(dir, "book.xlsx")
			writeDatedWorkbook(t, path, tt.headerRow, tt.dataRows)

			before := workbookCells(t, path)
			// Nothing is edited: the save still rewrites the sheet.
			require.NoError(t, autoSaveOverwrite(t, []string{path}, "UPDATE book SET id = id"))

			assert.Equal(t, before, workbookCells(t, path),
				"a save that changed nothing leaves every cell where and as it was")
		})
	}

	t.Run("a record added to the table goes under the last one", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()
		path := filepath.Join(dir, "book.xlsx")
		writeDatedWorkbook(t, path, 2, []int{3, 4, 5})

		want := workbookCells(t, path)
		// The id column is a number and the note column is text, so each new
		// cell is written as what its column holds.
		want["A6"] = xlsxCell{raw: "4", kind: excelize.CellTypeUnset}
		want["C6"] = xlsxCell{raw: "row4", kind: excelize.CellTypeSharedString}

		require.NoError(t, autoSaveOverwrite(t, []string{path},
			"INSERT INTO book (id, \"when\", note) VALUES (4, NULL, 'row4')"))
		assert.Equal(t, want, workbookCells(t, path))
	})

	t.Run("a record removed leaves the sheet no longer than the table", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()
		path := filepath.Join(dir, "book.xlsx")
		writeDatedWorkbook(t, path, 2, []int{3, 4, 5})

		before := workbookCells(t, path)
		require.NoError(t, autoSaveOverwrite(t, []string{path}, "DELETE FROM book WHERE id = 3"))

		// The rows the two remaining records came from keep what they held, and
		// the row the third came from is gone.
		cells := workbookCells(t, path)
		assert.Equal(t, before["B3"], cells["B3"])
		assert.Equal(t, before["B4"], cells["B4"])
		assert.NotContains(t, cells, "B5")
	})

	t.Run("an edited cell is the only one that changes", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()
		path := filepath.Join(dir, "book.xlsx")
		writeDatedWorkbook(t, path, 2, []int{3, 4, 5})

		want := workbookCells(t, path)
		want["C4"] = xlsxCell{raw: "edited", kind: excelize.CellTypeSharedString}

		require.NoError(t, autoSaveOverwrite(t, []string{path}, "UPDATE book SET note = 'edited' WHERE id = 2"))
		assert.Equal(t, want, workbookCells(t, path))
	})
}

// xlsxCell is what a cell holds, apart from where it is: the value the file
// stores rather than the one a format renders, the type that says how to read
// it, and the number format that decides whether a spreadsheet calls it a date.
type xlsxCell struct {
	raw    string
	kind   excelize.CellType
	numFmt int
}

// writeDatedWorkbook writes a sheet whose header sits on headerRow and whose
// records sit on dataRows, with a date column stored the way a workbook stores
// one: a serial wearing a date format.
func writeDatedWorkbook(t *testing.T, path string, headerRow int, dataRows []int) {
	t.Helper()

	// The sheet is named after the file, so the workbook loads as one table and
	// the save writes back onto it.
	const sheet = "book"
	// The serial of the first date, which the records count up from, and the
	// format that makes a spreadsheet read the serials as days. A date column
	// is stored this way, and it is what the save must not turn into text.
	const (
		firstSerial = 45000
		dateNumFmt  = 14
	)

	f := excelize.NewFile()
	defer func() {
		_ = f.Close()
	}()
	require.NoError(t, f.SetSheetName(defaultSheetName, sheet))

	style, err := f.NewStyle(&excelize.Style{NumFmt: dateNumFmt})
	require.NoError(t, err)

	for c, name := range []string{"id", "when", "note"} {
		require.NoError(t, f.SetCellValue(sheet, cellAt(t, c+1, headerRow), name))
	}
	for i, row := range dataRows {
		require.NoError(t, f.SetCellValue(sheet, cellAt(t, 1, row), i+1))
		require.NoError(t, f.SetCellValue(sheet, cellAt(t, 2, row), float64(firstSerial+i)))
		require.NoError(t, f.SetCellStyle(sheet, cellAt(t, 2, row), cellAt(t, 2, row), style))
		require.NoError(t, f.SetCellValue(sheet, cellAt(t, 3, row), fmt.Sprintf("row%d", i+1)))
	}
	require.NoError(t, f.SaveAs(path))
}

// workbookCells is every cell a sheet holds, by its reference.
func workbookCells(t *testing.T, path string) map[string]xlsxCell {
	t.Helper()

	const sheet = "book"

	f, err := excelize.OpenFile(path)
	require.NoError(t, err)
	defer func() {
		_ = f.Close()
	}()

	rows, err := f.GetRows(sheet)
	require.NoError(t, err)

	cells := make(map[string]xlsxCell)
	for r := range rows {
		for c := range 3 {
			axis := cellAt(t, c+1, r+1)
			raw, err := f.GetCellValue(sheet, axis, excelize.Options{RawCellValue: true})
			require.NoError(t, err)
			if raw == "" {
				continue
			}
			kind, err := f.GetCellType(sheet, axis)
			require.NoError(t, err)
			styleID, err := f.GetCellStyle(sheet, axis)
			require.NoError(t, err)
			style, err := f.GetStyle(styleID)
			require.NoError(t, err)
			cells[axis] = xlsxCell{raw: raw, kind: kind, numFmt: style.NumFmt}
		}
	}
	return cells
}

// cellAt names a cell by its column and row, both numbered from one.
func cellAt(t *testing.T, col, row int) string {
	t.Helper()

	name, err := excelize.CoordinatesToCellName(col, row)
	require.NoError(t, err)
	return name
}

// TestAutoSaveOverwriteXLSXKeepsANumberANumber pins that a cell the save
// changes is written as what the column holds rather than always as text.
//
// A workbook stores a number as a number, and a spreadsheet sums, charts and
// sorts by that; writing an edited value as a string left one cell of a numeric
// column out of all of them, and the reload through this package was unaffected,
// so nothing said so.
func TestAutoSaveOverwriteXLSXKeepsANumberANumber(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name string
		// held is what the sheet holds before the save, one column under a
		// header. A float is written as a number and a string as a string,
		// which is how a workbook tells the two apart.
		held []any
		stmt string
		// want is the cell each row holds afterwards.
		want []xlsxCell
	}{
		{
			name: "a number stays a number",
			held: []any{10.0, 20.0, 30.0},
			stmt: "UPDATE book SET v = 40 WHERE v = 10",
			want: []xlsxCell{
				{raw: "40", kind: excelize.CellTypeUnset},
				{raw: "20", kind: excelize.CellTypeUnset},
				{raw: "30", kind: excelize.CellTypeUnset},
			},
		},
		{
			name: "a fraction stays a number",
			held: []any{1.5, 2.5},
			stmt: "UPDATE book SET v = 3.25 WHERE v = 1.5",
			want: []xlsxCell{
				{raw: "3.25", kind: excelize.CellTypeUnset},
				{raw: "2.5", kind: excelize.CellTypeUnset},
			},
		},
		{
			name: "text stays text",
			held: []any{"alice", "bob"},
			stmt: "UPDATE book SET v = 'carol' WHERE v = 'alice'",
			want: []xlsxCell{
				{raw: "carol", kind: excelize.CellTypeSharedString},
				{raw: "bob", kind: excelize.CellTypeSharedString},
			},
		},
		{
			// A zero-padded code is a text column under this package's own
			// rule, so writing it as a number would lose the padding the
			// column exists to keep.
			name: "a zero-padded code stays text",
			held: []any{"007", "042"},
			stmt: "UPDATE book SET v = '008' WHERE v = '007'",
			want: []xlsxCell{
				{raw: "008", kind: excelize.CellTypeSharedString},
				{raw: "042", kind: excelize.CellTypeSharedString},
			},
		},
		{
			// An integer past what a float64 spells exactly is written
			// through an int64, so the digits the column holds are the
			// digits the cell holds.
			name: "an integer past what a float64 holds keeps its digits",
			held: []any{1, 2},
			stmt: "UPDATE book SET v = 9223372036854775807 WHERE v = 1",
			want: []xlsxCell{
				{raw: "9223372036854775807", kind: excelize.CellTypeUnset},
				{raw: "2", kind: excelize.CellTypeUnset},
			},
		},
		{
			// A literal past int64 is text for the same reason: a number
			// cannot hold its digits.
			name: "a literal past int64 stays text",
			held: []any{"11040320260000000000", "1"},
			stmt: "UPDATE book SET v = '11040320260000000001' WHERE v = '1'",
			want: []xlsxCell{
				{raw: "11040320260000000000", kind: excelize.CellTypeSharedString},
				{raw: "11040320260000000001", kind: excelize.CellTypeSharedString},
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			dir := t.TempDir()
			path := filepath.Join(dir, "book.xlsx")
			writeOneColumnWorkbook(t, path, tt.held)

			require.NoError(t, autoSaveOverwrite(t, []string{path}, tt.stmt))

			cells := workbookCells(t, path)
			for i, want := range tt.want {
				axis := cellAt(t, 1, i+2)
				assert.Equal(t, want, cells[axis], "cell %s", axis)
			}
		})
	}

	t.Run("a date cell keeps its serial when its row is edited elsewhere", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()
		path := filepath.Join(dir, "book.xlsx")
		writeDatedWorkbook(t, path, 1, []int{2, 3, 4})

		before := workbookCells(t, path)
		require.NoError(t, autoSaveOverwrite(t, []string{path}, "UPDATE book SET note = 'edited' WHERE id = 2"))

		cells := workbookCells(t, path)
		assert.Equal(t, before["B3"], cells["B3"], "the date beside the edited cell is untouched")
		assert.Equal(t, xlsxCell{raw: "2", kind: excelize.CellTypeUnset}, cells["A3"],
			"the number beside the edited cell is written as a number if it is written at all")
	})
}

// writeOneColumnWorkbook writes a sheet with a single column named v, holding
// values under a header. A float lands as a number and a string as a string,
// which is how a workbook tells a number from text.
func writeOneColumnWorkbook(t *testing.T, path string, held []any) {
	t.Helper()

	const sheet = "book"
	f := excelize.NewFile()
	defer func() {
		_ = f.Close()
	}()
	require.NoError(t, f.SetSheetName(defaultSheetName, sheet))
	require.NoError(t, f.SetCellValue(sheet, "A1", "v"))
	for i, value := range held {
		require.NoError(t, f.SetCellValue(sheet, cellAt(t, 1, i+2), value))
	}
	require.NoError(t, f.SaveAs(path))
}

// TestAutoSaveOverwriteXLSXFindsTheSheetItRead pins which sheet of a workbook a
// table is written back into.
//
// A table's name is its sheet's name run through sanitizeTableName, which turns
// spaces, hyphens and dots into underscores, prefixes a leading digit, and drops
// whatever is left over. None of that is reversible, so a sheet cannot be
// spelled back out of the table it was loaded as: deriving the destination from
// the table name sent "Q1 Sales" to a sheet named "Q1_Sales" that the workbook
// never had, and the save failed there with every edit in the session discarded,
// including the edits to sheets whose names were fine.
func TestAutoSaveOverwriteXLSXFindsTheSheetItRead(t *testing.T) {
	t.Parallel()

	// One sheet name per way sanitizeTableName rewrites a name.
	sheets := []struct {
		sheet string
		table string
	}{
		{sheet: "Q1 Sales", table: "book_Q1_Sales"},
		{sheet: "Q1-Sales", table: "book_Q1_Sales"},
		{sheet: "Rev.1", table: "book_Rev_1"},
		{sheet: "2024", table: "book_sheet_2024"},
		{sheet: "(draft)", table: "book_draft"},
	}

	for _, tt := range sheets {
		t.Run(tt.sheet, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			src := filepath.Join(t.TempDir(), "book.xlsx")
			writeWorkbook(t, src, map[string][][]string{
				tt.sheet: {{"id", "name"}, {"1", "alice"}},
			})

			require.NoError(t, autoSaveOverwrite(t, []string{src},
				"UPDATE "+quoteIdentifier(tt.table)+" SET name = 'bob'"))

			// The sheet list, not only the value: a save that added a sheet
			// named after the table would leave the original sheet holding the
			// old rows, and the next load would refuse the workbook because two
			// sheets now map to one table.
			assert.Equal(t, []string{tt.sheet}, workbookSheets(t, src),
				"the rows belong in the sheet they were read from")

			reloaded, err := OpenContext(ctx, src)
			require.NoError(t, err)
			defer reloaded.Close()

			var name string
			require.NoError(t, reloaded.QueryRowContext(ctx,
				"SELECT name FROM "+quoteIdentifier(tt.table)).Scan(&name))
			assert.Equal(t, "bob", name)
		})
	}

	t.Run("one sheet that cannot be spelled back does not discard the others", func(t *testing.T) {
		t.Parallel()

		ctx := t.Context()
		src := filepath.Join(t.TempDir(), "book.xlsx")
		writeWorkbook(t, src, map[string][][]string{
			"Orders":   {{"id", "name"}, {"1", "alice"}},
			"Q1 Sales": {{"id", "name"}, {"1", "carol"}},
		})

		require.NoError(t, autoSaveOverwrite(t, []string{src},
			"UPDATE book_Orders SET name = 'bob'",
			"UPDATE book_Q1_Sales SET name = 'dave'"))

		assert.Equal(t, []string{"Orders", "Q1 Sales"}, workbookSheets(t, src))

		reloaded, err := OpenContext(ctx, src)
		require.NoError(t, err)
		defer reloaded.Close()

		for table, want := range map[string]string{"book_Orders": "bob", "book_Q1_Sales": "dave"} {
			var name string
			require.NoError(t, reloaded.QueryRowContext(ctx,
				"SELECT name FROM "+quoteIdentifier(table)).Scan(&name))
			assert.Equal(t, want, name, "table %s", table)
		}
	})

	t.Run("a table created during the session becomes a sheet", func(t *testing.T) {
		t.Parallel()

		ctx := t.Context()
		src := filepath.Join(t.TempDir(), "book.xlsx")
		writeWorkbook(t, src, map[string][][]string{
			"Orders": {{"id", "name"}, {"1", "alice"}},
		})

		require.NoError(t, autoSaveOverwrite(t, []string{src},
			"CREATE TABLE book_Extra (id TEXT, name TEXT)",
			"INSERT INTO book_Extra VALUES ('9', 'new')",
			"UPDATE book_Orders SET name = 'bob'"))

		assert.Equal(t, []string{"Extra", "Orders"}, workbookSheets(t, src),
			"a table of this workbook with no sheet of its own is written as a new one")

		reloaded, err := OpenContext(ctx, src)
		require.NoError(t, err)
		defer reloaded.Close()

		var name string
		require.NoError(t, reloaded.QueryRowContext(ctx, "SELECT name FROM book_Orders").Scan(&name))
		assert.Equal(t, "bob", name, "the edit to the sheet that already existed survives")
		require.NoError(t, reloaded.QueryRowContext(ctx, "SELECT name FROM book_Extra").Scan(&name))
		assert.Equal(t, "new", name)
	})
}
