package filesql

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"testing/quick"
	"unicode/utf8"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/xuri/excelize/v2"
	"golang.org/x/text/encoding/japanese"
	"golang.org/x/text/encoding/unicode"
)

// TestOpenHonorsUnicodeBOM verifies that a leading Unicode byte-order mark on a
// text input is honored rather than leaking into the data: a UTF-8 BOM is
// stripped so the first column keeps its plain name, and UTF-16 input is
// transcoded to UTF-8. Tools such as Excel, Notepad, and PowerShell emit these
// encodings, so a user must be able to query the first column by its real name.
func TestOpenHonorsUnicodeBOM(t *testing.T) {
	t.Parallel()

	utf8BOM := []byte{0xEF, 0xBB, 0xBF}

	utf16LE := func(s string) []byte {
		enc := unicode.UTF16(unicode.LittleEndian, unicode.UseBOM)
		out, err := enc.NewEncoder().Bytes([]byte(s))
		require.NoError(t, err)
		return out
	}

	tests := []struct {
		name    string
		file    string
		content []byte
		query   string
		want    string
	}{
		{
			name:    "utf-8 BOM on CSV keeps first column name queryable",
			file:    "bom.csv",
			content: append(append([]byte{}, utf8BOM...), []byte("name,age\nalice,30\n")...),
			query:   "SELECT name FROM bom WHERE age = 30",
			want:    "alice",
		},
		{
			name:    "utf-8 BOM on TSV keeps first column name queryable",
			file:    "bom.tsv",
			content: append(append([]byte{}, utf8BOM...), []byte("name\tage\nalice\t30\n")...),
			query:   "SELECT name FROM bom WHERE age = 30",
			want:    "alice",
		},
		{
			name:    "utf-8 BOM on LTSV keeps first label queryable",
			file:    "bom.ltsv",
			content: append(append([]byte{}, utf8BOM...), []byte("name:alice\tage:30\n")...),
			query:   "SELECT name FROM bom WHERE age = 30",
			want:    "alice",
		},
		{
			// The streaming JSON reader stores each document in a "data" column, so
			// the contract this guards is that a BOM no longer aborts the parse.
			name:    "utf-8 BOM on JSON parses instead of failing",
			file:    "bom.json",
			content: append(append([]byte{}, utf8BOM...), []byte(`[{"name":"alice","age":30}]`)...),
			query:   "SELECT json_extract(data, '$.name') FROM bom",
			want:    "alice",
		},
		{
			name:    "utf-8 BOM on JSONL parses instead of failing",
			file:    "bom.jsonl",
			content: append(append([]byte{}, utf8BOM...), []byte("{\"name\":\"alice\",\"age\":30}\n")...),
			query:   "SELECT json_extract(data, '$.name') FROM bom",
			want:    "alice",
		},
		{
			name:    "utf-16 LE CSV is transcoded to UTF-8",
			file:    "utf16.csv",
			content: utf16LE("name,age\nalice,30\n"),
			query:   "SELECT name FROM utf16 WHERE age = 30",
			want:    "alice",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			path := filepath.Join(t.TempDir(), tt.file)
			require.NoError(t, os.WriteFile(path, tt.content, 0o600))

			db, err := OpenContext(context.Background(), path)
			require.NoError(t, err)
			defer db.Close()

			var got string
			require.NoError(t, db.QueryRowContext(context.Background(), tt.query).Scan(&got))
			assert.Equal(t, tt.want, got)
		})
	}
}

// TestOpenRejectsInvalidUTF8 pins what happens to a text source that is not
// valid UTF-8. SQLite stores TEXT as UTF-8, so bytes in a legacy encoding were
// stored verbatim and came back as mojibake: the load succeeded, every string
// function operated on bytes that are not characters, and nothing said so. A
// caller can transcode before loading, but only if it is told.
func TestOpenRejectsInvalidUTF8(t *testing.T) {
	t.Parallel()

	// "名前,年齢\n田中,30\n" encoded as Shift-JIS: the header bytes are invalid
	// UTF-8, which is what a Japanese CSV exported from Excel looks like.
	shiftJIS := func(t *testing.T, s string) []byte {
		t.Helper()
		out, err := japanese.ShiftJIS.NewEncoder().Bytes([]byte(s))
		require.NoError(t, err)
		return out
	}

	tests := []struct {
		name    string
		file    string
		content []byte
	}{
		{
			name:    "shift-jis CSV",
			file:    "sjis.csv",
			content: shiftJIS(t, "名前,年齢\n田中,30\n"),
		},
		{
			name:    "shift-jis TSV",
			file:    "sjis.tsv",
			content: shiftJIS(t, "名前\t年齢\n田中\t30\n"),
		},
		{
			name:    "shift-jis LTSV",
			file:    "sjis.ltsv",
			content: shiftJIS(t, "name:田中\tage:30\n"),
		},
		{
			name:    "shift-jis JSONL",
			file:    "sjis.jsonl",
			content: shiftJIS(t, "{\"name\":\"田中\"}\n"),
		},
		{
			// A lone continuation byte is invalid wherever it appears, including
			// in the middle of an otherwise ASCII file.
			name:    "stray continuation byte in a data row",
			file:    "stray.csv",
			content: []byte("name,age\nal\x80ice,30\n"),
		},
		{
			// A truncated multi-byte sequence at end of file is invalid too: the
			// validator must not accept it just because the input ran out.
			name:    "truncated multi-byte sequence at end of file",
			file:    "truncated.csv",
			content: []byte("name\nalice\n\xe3\x81"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			path := filepath.Join(t.TempDir(), tt.file)
			require.NoError(t, os.WriteFile(path, tt.content, 0o600))

			db, err := OpenContext(context.Background(), path)
			if db != nil {
				defer db.Close()
			}
			require.Error(t, err, "loading a non-UTF-8 source must fail rather than store mojibake")
			assert.ErrorIs(t, err, ErrInvalidUTF8)
			assert.Contains(t, err.Error(), "UTF-8")
		})
	}
}

// TestOpenAcceptsValidUTF8Beyond the ASCII range keeps the validator from
// rejecting the multi-byte text it exists to protect, including a sequence that
// straddles the boundary between two reads.
func TestOpenAcceptsValidUTF8(t *testing.T) {
	t.Parallel()

	var b strings.Builder
	b.WriteString("name,note\n")
	// Enough rows that the content spans several read buffers, so a multi-byte
	// rune lands across a buffer boundary somewhere in here.
	for i := range 4000 {
		fmt.Fprintf(&b, "名前%d,日本語のテキストと絵文字🍣\n", i)
	}

	path := filepath.Join(t.TempDir(), "utf8.csv")
	require.NoError(t, os.WriteFile(path, []byte(b.String()), 0o600))

	db, err := OpenContext(context.Background(), path)
	require.NoError(t, err)
	defer db.Close()

	var got string
	require.NoError(t, db.QueryRowContext(context.Background(),
		"SELECT note FROM utf8 WHERE name = '名前3999'").Scan(&got))
	assert.Equal(t, "日本語のテキストと絵文字🍣", got)
}

// TestUTF8ValidatingReaderPassesThroughUnchanged is the property the validator
// must not break: it is a filter that judges, not one that edits. For any input
// it accepts, the bytes a parser reads are byte-for-byte the bytes it was given,
// whatever size the reads happen to be.
func TestUTF8ValidatingReaderPassesThroughUnchanged(t *testing.T) {
	t.Parallel()

	property := func(text string, chunk uint8) bool {
		if !utf8.ValidString(text) {
			return true // only accepted input has a pass-through contract
		}
		size := int(chunk)%7 + 1
		got, err := io.ReadAll(newUTF8ValidatingReader(&chunkedReader{
			data: []byte(text),
			size: size,
		}))
		return err == nil && string(got) == text
	}

	if err := quick.Check(property, &quick.Config{
		MaxCount: 500,
		Rand:     rand.New(rand.NewSource(1)), //nolint:gosec // deterministic input generation, not security
	}); err != nil {
		t.Error(err)
	}
}

// TestUTF8ValidatingReaderVerdictIndependentOfChunking pins the property a
// streaming validator is most likely to get wrong: a rune whose encoding is
// split across two reads must not be judged on its first half. The verdict has
// to match utf8.Valid over the whole input at every read size.
func TestUTF8ValidatingReaderVerdictIndependentOfChunking(t *testing.T) {
	t.Parallel()

	inputs := [][]byte{
		[]byte("日本語のテキストと絵文字🍣"),
		[]byte("ascii only"),
		{0x80},                   // lone continuation byte
		{0xE3, 0x81},             // truncated three-byte sequence
		{0xE3, 0x81, 0x82},       // complete three-byte sequence
		{0xF0, 0x9F, 0x8D},       // truncated four-byte sequence
		{0xF0, 0x9F, 0x8D, 0xA3}, // complete four-byte sequence
		{0xEF, 0xBF, 0xBD},       // an encoded U+FFFD is valid input
		[]byte("a\xC3\xA9b"),     // valid two-byte sequence between ASCII
		[]byte("a\xC3b"),         // two-byte sequence cut short mid-string
		{0xED, 0xA0, 0x80},       // surrogate half, invalid in UTF-8
		{0xC0, 0x80},             // overlong encoding of NUL
	}

	for _, input := range inputs {
		for size := 1; size <= len(input)+2; size++ {
			got, err := io.ReadAll(newUTF8ValidatingReader(&chunkedReader{
				data: input,
				size: size,
			}))
			wantValid := utf8.Valid(input)
			if wantValid != (err == nil) {
				t.Errorf("input %x read %d bytes at a time: err = %v, want valid = %v",
					input, size, err, wantValid)
				continue
			}
			if err == nil && !bytes.Equal(got, input) {
				t.Errorf("input %x read %d bytes at a time: got %x", input, size, got)
			}
			if err != nil && !errors.Is(err, ErrInvalidUTF8) {
				t.Errorf("input %x read %d bytes at a time: err = %v, want ErrInvalidUTF8", input, size, err)
			}
		}
	}
}

// chunkedReader hands out at most size bytes per Read, so a test can put a rune
// boundary wherever it needs one.
type chunkedReader struct {
	data []byte
	size int
	pos  int
}

func (r *chunkedReader) Read(p []byte) (int, error) {
	if r.pos >= len(r.data) {
		return 0, io.EOF
	}
	n := min(min(r.size, len(p)), len(r.data)-r.pos)
	copy(p, r.data[r.pos:r.pos+n])
	r.pos += n
	return n, nil
}

// TestDuplicateColumnErrorNamesTheColumn covers the message a caller acts on.
// A header with two unnamed columns is a duplicate of the empty name, and the
// message said so by printing nothing after the colon: "duplicate column name: ".
// Which column, out of a header the user cannot see the parse of, was left to
// guess.
func TestDuplicateColumnErrorNamesTheColumn(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		file    string
		content string
		want    []string
	}{
		{
			name:    "two unnamed columns",
			file:    "unnamed.csv",
			content: "a,,\n1,2,3\n",
			want:    []string{`""`, "column 3"},
		},
		{
			name:    "two columns with the same name",
			file:    "same.csv",
			content: "a,b,a\n1,2,3\n",
			want:    []string{`"a"`, "column 3"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			path := filepath.Join(t.TempDir(), tt.file)
			require.NoError(t, os.WriteFile(path, []byte(tt.content), 0o600))

			db, err := OpenContext(context.Background(), path)
			if db != nil {
				defer db.Close()
			}
			require.Error(t, err)
			assert.ErrorIs(t, err, ErrDuplicateColumn)
			for _, want := range tt.want {
				assert.Contains(t, err.Error(), want)
			}
		})
	}
}

// requireDuplicateColumnRefusal asserts the whole contract of the duplicate
// header guard: the sentinel a caller matches on, the offending name quoted so
// whitespace is visible, its 1-based position, and — the reason the guard
// exists — that the header never reached SQLite, whose refusal arrives as
// ErrDatabaseOperation carrying neither the sentinel nor the position.
func requireDuplicateColumnRefusal(t *testing.T, err error, want ...string) {
	t.Helper()

	require.Error(t, err)
	assert.ErrorIs(t, err, ErrDuplicateColumn)
	assert.NotErrorIs(t, err, ErrDatabaseOperation)
	for _, w := range want {
		assert.Contains(t, err.Error(), w)
	}
}

// TestDuplicateColumnCheckIsTheSameEverywhere pins one rule for duplicate
// column names across the formats and the loaders.
//
// The rule is CSV's: names are compared with surrounding whitespace removed, so
// "name" and " name " are the same name. Reading a CSV said so and reading a
// workbook did not, which made a header a duplicate or not depending on which
// file it arrived in.
func TestDuplicateColumnCheckIsTheSameEverywhere(t *testing.T) {
	t.Parallel()

	t.Run("csv rejects names that differ only by surrounding whitespace", func(t *testing.T) {
		t.Parallel()

		path := filepath.Join(t.TempDir(), "dup.csv")
		require.NoError(t, os.WriteFile(path, []byte("name, name \n1,2\n"), 0o600))

		db, err := OpenContext(context.Background(), path)
		if db != nil {
			defer db.Close()
		}
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrDuplicateColumn)
	})

	t.Run("xlsx rejects names that differ only by surrounding whitespace", func(t *testing.T) {
		t.Parallel()

		path := filepath.Join(t.TempDir(), "book.xlsx")
		writeXLSXHeaderFixture(t, path, []string{"name", " name "}, []string{"1", "2"})

		db, err := OpenContext(context.Background(), path)
		if db != nil {
			defer db.Close()
		}
		require.Error(t, err, "a workbook header must follow the same rule a CSV header does")
		assert.ErrorIs(t, err, ErrDuplicateColumn)
		// The quoting is what makes this message readable: the two names differ
		// only by the space the quotes show.
		assert.Contains(t, err.Error(), `" name "`)
	})

	// An exact duplicate in a workbook reached SQLite, which refused it in its
	// own words wrapped in a database-operation error. A caller matching on
	// ErrDuplicateColumn saw a workbook fail for what looked like a different
	// reason than a CSV failing the same way.
	t.Run("xlsx reports an exact duplicate as a duplicate column", func(t *testing.T) {
		t.Parallel()

		path := filepath.Join(t.TempDir(), "exact.xlsx")
		writeXLSXHeaderFixture(t, path, []string{"name", "name"}, []string{"1", "2"})

		db, err := OpenContext(context.Background(), path)
		if db != nil {
			defer db.Close()
		}
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrDuplicateColumn)
		assert.Contains(t, err.Error(), "column 2")
	})

	// SQLite compares column names without regard to case, so a header that
	// differs only in case is a duplicate to the engine. The guard compared the
	// names verbatim, so it passed them through and SQLite refused the CREATE
	// TABLE in its own words, three wraps deep, with no sentinel to match and no
	// column position — the outcome this guard exists to replace.
	t.Run("csv rejects names that differ only by case", func(t *testing.T) {
		t.Parallel()

		path := filepath.Join(t.TempDir(), "cased.csv")
		require.NoError(t, os.WriteFile(path, []byte("ID,id\n1,2\n"), 0o600))

		db, err := OpenContext(context.Background(), path)
		if db != nil {
			defer db.Close()
		}
		requireDuplicateColumnRefusal(t, err, `"id"`, "column 2")
	})

	t.Run("xlsx rejects names that differ only by case", func(t *testing.T) {
		t.Parallel()

		path := filepath.Join(t.TempDir(), "cased.xlsx")
		writeXLSXHeaderFixture(t, path, []string{"Name", "nAmE"}, []string{"1", "2"})

		db, err := OpenContext(context.Background(), path)
		if db != nil {
			defer db.Close()
		}
		requireDuplicateColumnRefusal(t, err, `"nAmE"`, "column 2")
	})

	// Whitespace and case are two rules, and a name that differs by both
	// satisfies neither: SQLite does not trim, so " NAME " and "name" are two
	// columns to it, and an import that refused them would refuse a header the
	// engine accepts.
	t.Run("csv keeps names that differ by case and whitespace at once", func(t *testing.T) {
		t.Parallel()

		path := filepath.Join(t.TempDir(), "cased_spaced.csv")
		require.NoError(t, os.WriteFile(path, []byte("name, NAME \n1,2\n"), 0o600))

		db, err := OpenContext(context.Background(), path)
		require.NoError(t, err)
		defer db.Close()

		var got string
		require.NoError(t, db.QueryRowContext(context.Background(),
			`SELECT "name" FROM cased_spaced`).Scan(&got))
		assert.Equal(t, "1", got)
	})

	// The case rule is SQLite's, and SQLite's folding stops at ASCII: "ä" and
	// "Ä" are two columns to it. Folding with strings.ToLower made them one and
	// refused a header the engine accepts.
	t.Run("csv keeps names that differ by non-ASCII case", func(t *testing.T) {
		t.Parallel()

		path := filepath.Join(t.TempDir(), "accents.csv")
		require.NoError(t, os.WriteFile(path, []byte("ä,Ä\n1,2\n"), 0o600))

		db, err := OpenContext(context.Background(), path)
		require.NoError(t, err, "SQLite tells these two apart, so the import must too")
		defer db.Close()

		var got string
		require.NoError(t, db.QueryRowContext(context.Background(),
			`SELECT "Ä" FROM accents`).Scan(&got))
		assert.Equal(t, "2", got)
	})

	// The two comparisons are separate, not one folded-and-trimmed key. " A"
	// and "a" differ by whitespace and by case, so neither rule matches on its
	// own — and SQLite, which does not trim, keeps them as two columns.
	t.Run("csv keeps names that differ by whitespace and case together", func(t *testing.T) {
		t.Parallel()

		path := filepath.Join(t.TempDir(), "both.csv")
		require.NoError(t, os.WriteFile(path, []byte(" A,a\n1,2\n"), 0o600))

		db, err := OpenContext(context.Background(), path)
		require.NoError(t, err, "neither the whitespace rule nor the case rule matches this pair")
		defer db.Close()

		var got string
		require.NoError(t, db.QueryRowContext(context.Background(),
			`SELECT "a" FROM both`).Scan(&got))
		assert.Equal(t, "2", got)
	})

	// Names that are distinct after trimming stay distinct, so the rule refuses
	// a collision rather than every header that holds a space.
	t.Run("xlsx keeps headers that differ by more than whitespace", func(t *testing.T) {
		t.Parallel()

		path := filepath.Join(t.TempDir(), "ok.xlsx")
		writeXLSXHeaderFixture(t, path, []string{"name", " other "}, []string{"1", "2"})

		db, err := OpenContext(context.Background(), path)
		require.NoError(t, err)
		defer db.Close()

		// A workbook becomes one table per sheet, named file_sheet.
		var got string
		require.NoError(t, db.QueryRowContext(context.Background(),
			`SELECT "name" FROM ok_Sheet1`).Scan(&got))
		assert.Equal(t, "1", got)
	})
}

// writeXLSXHeaderFixture writes a one-sheet workbook whose first row is headers
// and whose second row is values.
func writeXLSXHeaderFixture(t *testing.T, path string, headers, values []string) {
	t.Helper()

	f := excelize.NewFile()
	defer func() { _ = f.Close() }()
	for i, header := range headers {
		cell, err := excelize.CoordinatesToCellName(i+1, 1)
		require.NoError(t, err)
		require.NoError(t, f.SetCellValue("Sheet1", cell, header))
	}
	for i, value := range values {
		cell, err := excelize.CoordinatesToCellName(i+1, 2)
		require.NoError(t, err)
		require.NoError(t, f.SetCellValue("Sheet1", cell, value))
	}
	require.NoError(t, f.SaveAs(path))
}
