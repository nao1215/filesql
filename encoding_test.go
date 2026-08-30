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
	"strconv"
	"strings"
	"testing"
	"testing/quick"
	"unicode/utf8"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/xuri/excelize/v2"
	"golang.org/x/text/encoding/japanese"
	"golang.org/x/text/encoding/unicode"

	"github.com/nao1215/filesql/internal/reader"
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
		{
			// A program that marks text already carrying a mark writes two, and
			// re-exporting a CSV a spreadsheet marked is how a file gets there.
			// Stripping one left the second in the first column's name, which
			// is the failure stripping exists to prevent.
			name:    "two utf-8 BOMs leave the first column name plain",
			file:    "bom2.csv",
			content: append(append(append([]byte{}, utf8BOM...), utf8BOM...), []byte("name,age\nalice,30\n")...),
			query:   "SELECT name FROM bom2 WHERE age = 30",
			want:    "alice",
		},
		{
			name:    "three utf-8 BOMs leave the first column name plain",
			file:    "bom3.csv",
			content: append(append(append(append([]byte{}, utf8BOM...), utf8BOM...), utf8BOM...), []byte("name,age\nalice,30\n")...),
			query:   "SELECT name FROM bom3 WHERE age = 30",
			want:    "alice",
		},
		{
			// The mark and the character are the same code point, so a UTF-16
			// file whose first header cell opens with U+FEFF reaches the same
			// place by another route.
			name:    "utf-16 LE CSV opening with a second mark leaves the name plain",
			file:    "utf16bom.csv",
			content: utf16LE("\ufeffname,age\nalice,30\n"),
			query:   "SELECT name FROM utf16bom WHERE age = 30",
			want:    "alice",
		},
		{
			// A mark that is not at the front of the file is a character. It
			// names the column it was written in, and a value keeps it.
			name:    "a mark on a later column is part of that name",
			file:    "latebom.csv",
			content: append(append([]byte{}, utf8BOM...), []byte("name,\ufeffage\nalice,30\n")...),
			query:   "SELECT name FROM latebom WHERE \"\ufeffage\" = 30",
			want:    "alice",
		},
		{
			name:    "a mark inside a value is part of that value",
			file:    "valuebom.csv",
			content: append(append([]byte{}, utf8BOM...), []byte("name,age\n\ufeffalice,30\n")...),
			query:   "SELECT name FROM valuebom WHERE age = 30",
			want:    "\ufeffalice",
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

// TestAFileOfMarksHoldsNoTable pins that however many byte-order marks a file
// opens with, what is left is what decides whether it holds a table. A file of
// one mark and a newline was empty and a file of two was a table with a column
// named by the second mark, which is the same file to anyone reading it.
func TestAFileOfMarksHoldsNoTable(t *testing.T) {
	t.Parallel()

	const mark = "\ufeff"
	for _, content := range []string{"\n", mark + "\n", mark + mark + "\n", mark + mark + mark + "\n"} {
		t.Run(fmt.Sprintf("%q", content), func(t *testing.T) {
			t.Parallel()

			path := filepath.Join(t.TempDir(), "t.csv")
			require.NoError(t, os.WriteFile(path, []byte(content), 0o600))

			_, err := OpenContext(context.Background(), path)
			require.Error(t, err)
			assert.ErrorIs(t, err, ErrEmptyData)
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

// TestAFileWiderThanATableIsRefusedInThisPackagesWords covers the width SQLite
// cannot hold.
//
// SQLITE_MAX_COLUMN is fixed when SQLite is compiled, so a wider file cannot be
// loaded whatever this package does; what it can do is say so in its own words.
// It used to let the CREATE TABLE fail instead, which answered "SQL logic error:
// too many columns on _filesql_stage_t" -- a driver message naming the staging
// table this package builds for a reader, with no limit, no count and no
// sentinel to match. That is the outcome the duplicate-column check exists to
// replace, reached by a different route.
func TestAFileWiderThanATableIsRefusedInThisPackagesWords(t *testing.T) {
	t.Parallel()

	wide := func(columns int) string {
		names := make([]string, columns)
		values := make([]string, columns)
		for i := range names {
			names[i] = fmt.Sprintf("c%d", i)
			values[i] = "1"
		}
		return strings.Join(names, ",") + "\n" + strings.Join(values, ",") + "\n"
	}

	t.Run("a file at the limit loads", func(t *testing.T) {
		t.Parallel()

		db, err := NewBuilder().
			AddReader(strings.NewReader(wide(reader.MaxColumns)), "t", FileTypeCSV).
			Open(context.Background())
		require.NoError(t, err)
		defer func() { _ = db.Close() }()
	})

	for _, tt := range []struct {
		name string
		body string
		kind FileType
	}{
		{name: "csv", body: wide(reader.MaxColumns + 1), kind: FileTypeCSV},
		{name: "tsv", body: strings.ReplaceAll(wide(reader.MaxColumns+1), ",", "\t"), kind: FileTypeTSV},
		{name: "ltsv", body: ltsvRecordOfWidth(reader.MaxColumns + 1), kind: FileTypeLTSV},
	} {
		t.Run(tt.name+" one column past it is refused", func(t *testing.T) {
			t.Parallel()

			db, err := NewBuilder().
				AddReader(strings.NewReader(tt.body), "t", tt.kind).
				Open(context.Background())
			if db != nil {
				_ = db.Close()
			}
			require.Error(t, err)
			assert.ErrorIs(t, err, ErrUnsupportedFormat)
			assert.Contains(t, err.Error(), strconv.Itoa(reader.MaxColumns), "the message names the limit")
			assert.Contains(t, err.Error(), strconv.Itoa(reader.MaxColumns+1), "and the width the file has")
			assert.NotContains(t, err.Error(), "_filesql_", "and nothing this package built for itself")
		})
	}
}

// ltsvRecordOfWidth builds one LTSV record naming the given number of labels,
// which is where an LTSV table's columns come from.
func ltsvRecordOfWidth(labels int) string {
	pairs := make([]string, labels)
	for i := range pairs {
		pairs[i] = fmt.Sprintf("l%d:1", i)
	}
	return strings.Join(pairs, "\t") + "\n"
}

// TestBlankHeadersAreNamedByPosition covers a header row holding blank cells.
//
// A blank header names nothing, so a header with two of them has no name typed
// twice -- but two empty names are equal, and the duplicate check refused the
// whole file with a message about a name it never wrote. A spreadsheet exported
// with spacer columns, and a CSV whose header row ends in two commas, both
// arrive that way. A blank header now takes the name of its position, which is
// distinct on its own and is something a caller can write in a query.
func TestBlankHeadersAreNamedByPosition(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name    string
		file    string
		content string
		want    []string
	}{
		{
			name:    "one blank header in the middle",
			file:    "one.csv",
			content: "a,,c\n1,2,3\n",
			want:    []string{"a", "column_2", "c"},
		},
		{
			name:    "two blank headers",
			file:    "two.csv",
			content: "a,,c,,e\n1,2,3,4,5\n",
			want:    []string{"a", "column_2", "c", "column_4", "e"},
		},
		{
			name:    "a header row ending in two commas",
			file:    "trailing.csv",
			content: "a,b,,\n1,2,3,4\n",
			want:    []string{"a", "b", "column_3", "column_4"},
		},
		{
			name:    "the blank is first",
			file:    "first.csv",
			content: ",a\n1,2\n",
			want:    []string{"column_1", "a"},
		},
		{
			name:    "every header is blank",
			file:    "all.csv",
			content: ",,\n1,2,3\n",
			want:    []string{"column_1", "column_2", "column_3"},
		},
		{
			name:    "the generated name is already taken",
			file:    "taken.csv",
			content: "column_2,,c\n1,2,3\n",
			want:    []string{"column_2", "column_2_2", "c"},
		},
		{
			name:    "a header of one space is a name the file wrote",
			file:    "space.csv",
			content: "a, ,c\n1,2,3\n",
			want:    []string{"a", " ", "c"},
		},
		{
			name:    "tab-separated files follow the same rule",
			file:    "two.tsv",
			content: "a\t\tc\t\te\n1\t2\t3\t4\t5\n",
			want:    []string{"a", "column_2", "c", "column_4", "e"},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			path := filepath.Join(t.TempDir(), tt.file)
			require.NoError(t, os.WriteFile(path, []byte(tt.content), 0o600))

			db, err := OpenContext(context.Background(), path)
			require.NoError(t, err)
			defer func() { _ = db.Close() }()

			table := strings.TrimSuffix(strings.TrimSuffix(tt.file, ".csv"), ".tsv")
			rows, err := db.QueryContext(context.Background(), "SELECT * FROM "+quoteIdentifier(table)) //nolint:gosec // the name is this test's own and is quoted
			require.NoError(t, err)
			defer func() { _ = rows.Close() }()

			got, err := rows.Columns()
			require.NoError(t, err)
			require.NoError(t, rows.Err())
			assert.Equal(t, tt.want, got)

			// Every column can be named in a query, which is the point of
			// giving it a name at all.
			for _, name := range got {
				var value string
				require.NoError(t, db.QueryRowContext(context.Background(),
					"SELECT "+quoteIdentifier(name)+" FROM "+quoteIdentifier(table)).Scan(&value))
			}
		})
	}
}

// TestDuplicateColumnErrorNamesTheColumn covers the message a caller acts on.
// A header that names one column twice is refused, and the message says which
// column out of a header the user cannot see the parse of.
//
// A blank header is not a name typed twice and is not refused: it is given the
// name of its position, which TestBlankHeadersAreNamedByPosition covers.
func TestDuplicateColumnErrorNamesTheColumn(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		file    string
		content string
		want    []string
	}{
		{
			name:    "a name repeated after a blank one",
			file:    "unnamed.csv",
			content: "a,,a\n1,2,3\n",
			want:    []string{`"a"`, "column 3"},
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

// utf16FromUnits builds a UTF-16 file from code units, so a test can put a unit in
// it that a string cannot hold: an unpaired surrogate is not a rune, and Go's
// encoders replace it rather than write it.
func utf16FromUnits(littleEndian bool, units []uint16) []byte {
	out := make([]byte, 0, 2+2*len(units))
	if littleEndian {
		out = append(out, 0xFF, 0xFE)
	} else {
		out = append(out, 0xFE, 0xFF)
	}
	for _, u := range units {
		low, high := byte(u&0xFF), byte(u>>8)
		if littleEndian {
			out = append(out, low, high)
		} else {
			out = append(out, high, low)
		}
	}
	return out
}

// utf16Units is the code units of s, which for this file's inputs is one unit
// per rune: every rune used here is below U+10000.
func utf16Units(s string) []uint16 {
	units := make([]uint16, 0, len(s))
	for _, r := range s {
		units = append(units, uint16(r)) //nolint:gosec // test input stays below U+10000
	}
	return units
}

// TestOpenRejectsDamagedUTF16 pins the UTF-16 half of the rule
// TestOpenRejectsInvalidUTF8 pins for UTF-8: a text source this package cannot
// decode fails the load rather than being stored with a replacement character
// nobody can tell from one the file really held. A truncated download and an
// unpaired surrogate are the two shapes damage takes here.
func TestOpenRejectsDamagedUTF16(t *testing.T) {
	t.Parallel()

	whole := utf16Units("v\nabc\n")
	loneHigh := append(utf16Units("v\na"), 0xD800)
	loneHigh = append(loneHigh, utf16Units("b\n")...)
	loneLow := append(utf16Units("v\na"), 0xDC00)
	loneLow = append(loneLow, utf16Units("b\n")...)
	// A high surrogate at end of file is a pair whose second half never came.
	danglingHigh := append(utf16Units("v\nab\n"), 0xD800)

	// Enough rows that the damage lands well past the first read buffer, so the
	// check cannot be one that only looks at the head of the file.
	far := make([]uint16, 0, 2+20000*6+2)
	far = append(far, utf16Units("v\n")...)
	for range 20000 {
		far = append(far, utf16Units("value\n")...)
	}
	far = append(far, 0xD800)
	far = append(far, utf16Units("\n")...)

	for _, littleEndian := range []bool{true, false} {
		name := "utf-16be"
		if littleEndian {
			name = "utf-16le"
		}
		tests := []struct {
			name    string
			content []byte
		}{
			{name: "unpaired high surrogate", content: utf16FromUnits(littleEndian, loneHigh)},
			{name: "unpaired low surrogate", content: utf16FromUnits(littleEndian, loneLow)},
			{name: "high surrogate at end of file", content: utf16FromUnits(littleEndian, danglingHigh)},
			{name: "damage past the first read buffer", content: utf16FromUnits(littleEndian, far)},
			{
				name: "file cut in the middle of a unit",
				content: func() []byte {
					b := utf16FromUnits(littleEndian, whole)
					return b[:len(b)-1]
				}(),
			},
		}
		for _, tt := range tests {
			t.Run(name+" "+tt.name, func(t *testing.T) {
				t.Parallel()

				path := filepath.Join(t.TempDir(), "damaged.csv")
				require.NoError(t, os.WriteFile(path, tt.content, 0o600))

				db, err := OpenContext(context.Background(), path)
				if db != nil {
					defer db.Close()
				}
				require.Error(t, err, "damaged UTF-16 must fail rather than store a replacement character")
				assert.ErrorIs(t, err, ErrEncoding)
			})
		}
	}
}

// TestOpenAcceptsWellFormedUTF16 keeps the check above from rejecting what it
// exists to protect: a replacement character the file really holds is data, and
// a surrogate pair is one astral character rather than two broken halves.
func TestOpenAcceptsWellFormedUTF16(t *testing.T) {
	t.Parallel()

	pair := append(utf16Units("v\n"), 0xD83C, 0xDF63) // U+1F363, a sushi emoji
	pair = append(pair, utf16Units("\n")...)

	tests := []struct {
		name  string
		units []uint16
		want  string
	}{
		{name: "a replacement character in the source", units: append(utf16Units("v\na"), 0xFFFD, 'b', '\n'), want: "a�b"},
		{name: "a surrogate pair", units: pair, want: "🍣"},
		{name: "plain text", units: utf16Units("v\nabc\n"), want: "abc"},
	}

	for _, littleEndian := range []bool{true, false} {
		name := "utf-16be"
		if littleEndian {
			name = "utf-16le"
		}
		for _, tt := range tests {
			t.Run(name+" "+tt.name, func(t *testing.T) {
				t.Parallel()

				path := filepath.Join(t.TempDir(), "ok.csv")
				require.NoError(t, os.WriteFile(path, utf16FromUnits(littleEndian, tt.units), 0o600))

				db, err := OpenContext(context.Background(), path)
				require.NoError(t, err)
				defer db.Close()

				var got string
				require.NoError(t, db.QueryRowContext(context.Background(), `SELECT v FROM ok`).Scan(&got))
				assert.Equal(t, tt.want, got)
			})
		}
	}
}

// TestUTF16RoundTripKeepsAstralCharacters is the round trip the check has to
// leave working: what this package writes as UTF-16, it must read back.
func TestUTF16RoundTripKeepsAstralCharacters(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	const want = "🍣と日本語�"

	for _, enc := range []Encoding{EncodingUTF16LE, EncodingUTF16BE} {
		t.Run(enc.String(), func(t *testing.T) {
			t.Parallel()

			built, err := buildForTest(

				ctx, NewBuilder().
					AddReader(strings.NewReader("v\n"+want+"\n"), "t", FileTypeCSV))

			require.NoError(t, err)
			db, err := built.Open(ctx)
			require.NoError(t, err)

			dir := t.TempDir()
			require.NoError(t, DumpDatabase(db, dir, NewDumpOptions().WithEncoding(enc)))
			require.NoError(t, db.Close())

			reloaded, err := OpenContext(ctx, filepath.Join(dir, "t.csv"))
			require.NoError(t, err)
			defer reloaded.Close()

			var got string
			require.NoError(t, reloaded.QueryRowContext(ctx, `SELECT v FROM t`).Scan(&got))
			assert.Equal(t, want, got)
		})
	}
}

// TestUTF16ValidatingReaderVerdictIndependentOfChunking pins the property a
// streaming validator is most likely to get wrong: a code unit or a surrogate
// pair split across two reads must not be judged on its first half. The verdict
// has to be the same at every read size, and an accepted input has to come
// through byte for byte.
func TestUTF16ValidatingReaderVerdictIndependentOfChunking(t *testing.T) {
	t.Parallel()

	inputs := []struct {
		name  string
		units []uint16
		valid bool
	}{
		{name: "plain text", units: utf16Units("ab"), valid: true},
		{name: "a surrogate pair", units: []uint16{0xD83C, 0xDF63}, valid: true},
		{name: "a pair between text", units: []uint16{'a', 0xD83C, 0xDF63, 'b'}, valid: true},
		{name: "a replacement character", units: []uint16{0xFFFD}, valid: true},
		{name: "nothing but the mark", units: nil, valid: true},
		{name: "an unpaired high surrogate", units: []uint16{0xD800, 'a'}, valid: false},
		{name: "an unpaired low surrogate", units: []uint16{0xDC00, 'a'}, valid: false},
		{name: "two high surrogates", units: []uint16{0xD800, 0xD800}, valid: false},
		{name: "a high surrogate at the end", units: []uint16{'a', 0xD800}, valid: false},
	}

	for _, littleEndian := range []bool{true, false} {
		for _, input := range inputs {
			whole := utf16FromUnits(littleEndian, input.units)
			cases := []struct {
				name  string
				data  []byte
				valid bool
			}{
				{name: input.name, data: whole, valid: input.valid},
				// The same input with its last byte cut off ends in the middle of
				// a unit, which is damage whatever the units before it were.
				{name: input.name + ", cut mid unit", data: whole[:len(whole)-1], valid: false},
			}
			for _, tc := range cases {
				for size := 1; size <= len(tc.data)+2; size++ {
					got, err := io.ReadAll(newUTF16ValidatingReader(&chunkedReader{
						data: tc.data,
						size: size,
					}, littleEndian))
					if tc.valid != (err == nil) {
						t.Errorf("%s (little endian %v) read %d bytes at a time: err = %v, want valid = %v",
							tc.name, littleEndian, size, err, tc.valid)
						continue
					}
					if err == nil && !bytes.Equal(got, tc.data) {
						t.Errorf("%s (little endian %v) read %d bytes at a time: got %x", tc.name, littleEndian, size, got)
					}
					if err != nil && !errors.Is(err, ErrEncoding) {
						t.Errorf("%s (little endian %v) read %d bytes at a time: err = %v, want ErrEncoding",
							tc.name, littleEndian, size, err)
					}
				}
			}
		}
	}
}
