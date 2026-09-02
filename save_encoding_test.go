package filesql

import (
	"bytes"
	"context"
	"encoding/csv"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/text/encoding/japanese"
	"golang.org/x/text/encoding/unicode"
	"golang.org/x/text/transform"
)

// The save path could preserve a source's compression but not its text
// encoding: every text file it wrote was UTF-8. A caller that decoded a
// Shift-JIS source before loading had no way to get one back, so an in-place
// save silently changed the file's encoding on disk and the caller's own next
// run read mojibake. These tests are the round trip that was missing.

func TestDumpOptions_WithEncoding(t *testing.T) {
	t.Parallel()

	options := NewDumpOptions()
	newOptions := options.WithEncoding(EncodingShiftJIS)

	assert.Equal(t, EncodingUTF8, options.Encoding, "the original options should not be modified")
	assert.Equal(t, EncodingShiftJIS, newOptions.Encoding, "WithEncoding should update the encoding")
	assert.Equal(t, OutputFormatCSV, newOptions.Format, "WithEncoding should not change the format")
	assert.Equal(t, CompressionNone, newOptions.Compression, "WithEncoding should not change the compression")
}

func TestNewDumpOptions_DefaultsToUTF8(t *testing.T) {
	t.Parallel()

	assert.Equal(t, EncodingUTF8, NewDumpOptions().Encoding,
		"a save says nothing about encoding unless asked, and UTF-8 is what it wrote before the option existed")
}

// TestDumpDatabase_WritesTheRequestedEncoding is the round trip: a table saved
// in an encoding reads back as that encoding, values intact.
func TestDumpDatabase_WritesTheRequestedEncoding(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name     string
		encoding Encoding
		decoder  transform.Transformer
	}{
		// ソ is the classic Shift-JIS trap: its second byte is 0x5C, the ASCII
		// backslash, so a writer that is not encoding-aware corrupts the line.
		{name: "shift-jis", encoding: EncodingShiftJIS, decoder: japanese.ShiftJIS.NewDecoder()},
		{name: "euc-jp", encoding: EncodingEUCJP, decoder: japanese.EUCJP.NewDecoder()},
		{name: "utf-16le", encoding: EncodingUTF16LE, decoder: unicode.UTF16(unicode.LittleEndian, unicode.UseBOM).NewDecoder()},
		{name: "utf-16be", encoding: EncodingUTF16BE, decoder: unicode.UTF16(unicode.BigEndian, unicode.UseBOM).NewDecoder()},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			dir := t.TempDir()
			source := filepath.Join(dir, "src.csv")
			require.NoError(t, os.WriteFile(source, []byte("name,city\n山田ソ,東京\n"), 0o600))

			ctx := context.Background()
			db, err := Open(ctx, source)
			require.NoError(t, err)
			defer db.Close()

			out := filepath.Join(dir, "out")
			require.NoError(t, os.MkdirAll(out, 0o750))
			require.NoError(t, DumpDatabase(context.Background(), db, out, NewDumpOptions().WithEncoding(tt.encoding)))

			written, err := os.ReadFile(filepath.Join(out, "src.csv")) //nolint:gosec // path built by the test
			require.NoError(t, err)

			decoded, err := io.ReadAll(transform.NewReader(bytes.NewReader(written), tt.decoder))
			require.NoError(t, err)
			assert.Contains(t, string(decoded), "山田ソ", "the values should survive the encoding")
			assert.Contains(t, string(decoded), "東京")

			// Re-opening through filesql is the other half: the reader's BOM
			// handling has to recognize what the writer produced.
			if tt.encoding == EncodingUTF16LE || tt.encoding == EncodingUTF16BE {
				reopened, err := Open(ctx, filepath.Join(out, "src.csv"))
				require.NoError(t, err)
				defer reopened.Close()

				var city string
				require.NoError(t, reopened.QueryRowContext(ctx, "SELECT city FROM src").Scan(&city))
				assert.Equal(t, "東京", city)
			}
		})
	}
}

// TestDumpDatabase_RefusesAValueTheEncodingCannotCarry is the other half of the
// contract. A silent substitution is what the read side already refuses, so the
// write side refuses it too rather than writing a replacement character.
func TestDumpDatabase_RefusesAValueTheEncodingCannotCarry(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	source := filepath.Join(dir, "src.csv")
	require.NoError(t, os.WriteFile(source, []byte("name\n🎌\n"), 0o600))

	ctx := context.Background()
	db, err := Open(ctx, source)
	require.NoError(t, err)
	defer db.Close()

	out := filepath.Join(dir, "out")
	require.NoError(t, os.MkdirAll(out, 0o750))

	err = DumpDatabase(context.Background(), db, out, NewDumpOptions().WithEncoding(EncodingShiftJIS))
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrEncoding), "want ErrEncoding, got %v", err)

	// A refused save leaves nothing behind, as every other failed write does.
	entries, err := os.ReadDir(out)
	require.NoError(t, err)
	assert.Empty(t, entries, "a refused save should leave no file")
}

// TestDumpDatabase_EncodingAndCompressionCombine pins the layering: the text is
// encoded first and the bytes compressed after, so a reader decompresses and
// then decodes.
func TestDumpDatabase_EncodingAndCompressionCombine(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	source := filepath.Join(dir, "src.csv")
	require.NoError(t, os.WriteFile(source, []byte("name\n山田\n"), 0o600))

	ctx := context.Background()
	db, err := Open(ctx, source)
	require.NoError(t, err)
	defer db.Close()

	out := filepath.Join(dir, "out")
	require.NoError(t, os.MkdirAll(out, 0o750))
	require.NoError(t, DumpDatabase(context.Background(), db, out,
		NewDumpOptions().WithEncoding(EncodingShiftJIS).WithCompression(CompressionGZ)))

	written := filepath.Join(out, "src.csv.gz")
	require.FileExists(t, written)

	factory := NewCompressionFactory()
	reader, cleanup, err := factory.CreateReaderForFile(written)
	require.NoError(t, err)
	defer func() { _ = cleanup() }()

	decoded, err := io.ReadAll(transform.NewReader(reader, japanese.ShiftJIS.NewDecoder()))
	require.NoError(t, err)
	assert.Contains(t, string(decoded), "山田")
}

// TestDumpDatabase_BinaryFormatsIgnoreEncoding pins that the option applies to
// text only. Parquet and XLSX state their own encoding, so running their bytes
// through a transcoder would corrupt the container.
func TestDumpDatabase_BinaryFormatsIgnoreEncoding(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	source := filepath.Join(dir, "src.csv")
	require.NoError(t, os.WriteFile(source, []byte("name\n山田\n"), 0o600))

	ctx := context.Background()
	db, err := Open(ctx, source)
	require.NoError(t, err)
	defer db.Close()

	for _, format := range []OutputFormat{OutputFormatParquet, OutputFormatXLSX} {
		out := filepath.Join(dir, format.Extension())
		require.NoError(t, os.MkdirAll(out, 0o750))
		require.NoError(t, DumpDatabase(context.Background(), db, out,
			NewDumpOptions().WithFormat(format).WithEncoding(EncodingShiftJIS)))

		reopened, err := Open(ctx, filepath.Join(out, "src"+format.Extension()))
		require.NoError(t, err)

		var name string
		require.NoError(t, reopened.QueryRowContext(ctx, "SELECT name FROM src").Scan(&name))
		assert.Equal(t, "山田", name, "%v should keep its own encoding", format)
		require.NoError(t, reopened.Close())
	}
}

// TestDumpDatabase_ISO2022JPStaysValidCSV pins that an encoded value cannot
// break the structure of the file it sits in.
//
// The encoder wrapped the whole stream, so encoding/csv decided what to quote by
// looking at UTF-8 text and never saw the bytes the encoder introduced
// afterwards. ISO-2022-JP is seven-bit and its JIS X 0208 bytes run from 0x21 to
// 0x7E, which includes both the comma and the double quote: "が" encodes as
// ESC $ B $ , ESC ( B, and the comma in the middle of it was read as a field
// separator by every reader. "あ" carries a quote and made the record
// unparseable outright.
//
// Shift-JIS and EUC-JP escaped this by accident, their trail bytes all being at
// or above 0x80, so they are here to pin that they still write what they wrote.
func TestDumpDatabase_ISO2022JPStaysValidCSV(t *testing.T) {
	t.Parallel()

	values := map[string]string{
		"comma byte in the encoding": "が",
		"quote byte in the encoding": "あ",
		"neither":                    "日本語",
		"mixed with ascii":           "aあ1",
	}

	for name, value := range values {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			dir := t.TempDir()
			source := filepath.Join(dir, "t.csv")
			require.NoError(t, os.WriteFile(source, []byte("k,v\nrow,seed\n"), 0o600))

			db, err := Open(t.Context(), source)
			require.NoError(t, err)
			t.Cleanup(func() { _ = db.Close() })
			_, err = db.ExecContext(t.Context(), `UPDATE t SET v = ?`, value)
			require.NoError(t, err)

			out := filepath.Join(dir, "out")
			require.NoError(t, DumpDatabase(context.Background(), db, out, NewDumpOptions().WithEncoding(EncodingISO2022JP)))

			raw, err := os.ReadFile(filepath.Join(out, "t.csv")) //nolint:gosec // Test path from t.TempDir()
			require.NoError(t, err)

			decoded, err := io.ReadAll(transform.NewReader(bytes.NewReader(raw), japanese.ISO2022JP.NewDecoder()))
			require.NoError(t, err, "the file has to decode as the encoding it was written in")

			records, err := csv.NewReader(bytes.NewReader(decoded)).ReadAll()
			require.NoError(t, err, "the file has to parse as CSV")
			require.Len(t, records, 2, "a header and one row")
			require.Len(t, records[1], 2, "the row has as many fields as the table has columns")
			assert.Equal(t, value, records[1][1], "and the value survives")
		})
	}
}

// TestDumpDatabase_ISO2022JPRefusesAnEscape pins the one byte this encoding
// reads rather than writes. ISO-2022-JP switches character sets with an escape
// sequence, so an ESC held in a value or a column name went into the file as a
// designator: "x\x1b$By" came back as "x絈", the y eaten as half of a double-byte
// character, and a column named that way took the row after it into the same
// word. The dump said nothing and the decoder said nothing -- the bytes are a
// valid stream, they just say something else.
func TestDumpDatabase_ISO2022JPRefusesAnEscape(t *testing.T) {
	t.Parallel()

	t.Run("an escape in a value", func(t *testing.T) {
		t.Parallel()

		db := openWithTable(t, `CREATE TABLE t (a TEXT)`, `INSERT INTO t VALUES ('x'||CAST(x'1b2442' AS TEXT)||'y')`)

		out := filepath.Join(t.TempDir(), "out")
		err := DumpDatabase(context.Background(), db, out, NewDumpOptions().WithEncoding(EncodingISO2022JP))

		require.Error(t, err, "the file would decode as different text")
		assert.ErrorIs(t, err, ErrEncoding)
	})

	t.Run("an escape in a column name", func(t *testing.T) {
		t.Parallel()

		db := openWithTable(t, "CREATE TABLE t (\"x\x1b$By\" TEXT)", `INSERT INTO t VALUES ('1')`)

		out := filepath.Join(t.TempDir(), "out")
		err := DumpDatabase(context.Background(), db, out, NewDumpOptions().WithEncoding(EncodingISO2022JP))

		require.Error(t, err, "the header and the row would decode as one word")
		assert.ErrorIs(t, err, ErrEncoding)
	})

	t.Run("a control byte the encoding does not read is written", func(t *testing.T) {
		t.Parallel()

		// Shift out is not a designator in ISO-2022-JP, so it is data like any
		// other byte and has to keep being written.
		db := openWithTable(t, `CREATE TABLE t (a TEXT)`, `INSERT INTO t VALUES ('p'||CAST(x'0e' AS TEXT)||'q')`)

		out := filepath.Join(t.TempDir(), "out")
		require.NoError(t, DumpDatabase(context.Background(), db, out, NewDumpOptions().WithEncoding(EncodingISO2022JP)))

		raw, err := os.ReadFile(filepath.Join(out, "t.csv")) //nolint:gosec // Test path from t.TempDir()
		require.NoError(t, err)
		decoded, err := io.ReadAll(transform.NewReader(bytes.NewReader(raw), japanese.ISO2022JP.NewDecoder()))
		require.NoError(t, err)
		assert.Contains(t, string(decoded), "p\x0eq")
	})

	t.Run("another encoding writes an escape", func(t *testing.T) {
		t.Parallel()

		// Shift-JIS and the rest read ESC as an ordinary control character, so
		// the refusal belongs to this one encoding rather than to the writer.
		db := openWithTable(t, `CREATE TABLE t (a TEXT)`, `INSERT INTO t VALUES ('p'||CAST(x'1b' AS TEXT)||'q')`)

		out := filepath.Join(t.TempDir(), "out")
		require.NoError(t, DumpDatabase(context.Background(), db, out, NewDumpOptions().WithEncoding(EncodingShiftJIS)))

		raw, err := os.ReadFile(filepath.Join(out, "t.csv")) //nolint:gosec // Test path from t.TempDir()
		require.NoError(t, err)
		decoded, err := io.ReadAll(transform.NewReader(bytes.NewReader(raw), japanese.ShiftJIS.NewDecoder()))
		require.NoError(t, err)
		assert.Contains(t, string(decoded), "p\x1bq")
	})
}

// TestRefuseEscapeTransform covers the transformer on its own, including the
// short-destination path a dump does not reach: a transformer that mishandles
// it drops bytes instead of asking to be called again, which shows up as a
// truncated file rather than as an error.
func TestRefuseEscapeTransform(t *testing.T) {
	t.Parallel()

	t.Run("text with no escape passes through", func(t *testing.T) {
		t.Parallel()

		dst := make([]byte, 8)
		nDst, nSrc, err := refuseEscape{}.Transform(dst, []byte("abc"), true)
		require.NoError(t, err)
		assert.Equal(t, 3, nDst)
		assert.Equal(t, 3, nSrc)
		assert.Equal(t, "abc", string(dst[:nDst]))
	})

	t.Run("an escape fails after what precedes it", func(t *testing.T) {
		t.Parallel()

		dst := make([]byte, 8)
		nDst, nSrc, err := refuseEscape{}.Transform(dst, []byte("ab\x1bcd"), true)
		require.ErrorIs(t, err, errEscapeUnwritable)
		assert.Equal(t, 2, nDst)
		assert.Equal(t, 2, nSrc)
		assert.Equal(t, "ab", string(dst[:nDst]))
	})

	t.Run("a destination too small asks to be called again", func(t *testing.T) {
		t.Parallel()

		dst := make([]byte, 2)
		nDst, nSrc, err := refuseEscape{}.Transform(dst, []byte("abcdef"), true)
		require.ErrorIs(t, err, transform.ErrShortDst)
		assert.Equal(t, 2, nDst)
		assert.Equal(t, 2, nSrc)
	})

	t.Run("an escape at the front fails at once", func(t *testing.T) {
		t.Parallel()

		dst := make([]byte, 8)
		nDst, _, err := refuseEscape{}.Transform(dst, []byte("\x1b"), true)
		require.ErrorIs(t, err, errEscapeUnwritable)
		assert.Equal(t, 0, nDst)
	})
}

// TestOpen_NamesAnEscapeEncodedInput pins that a file filesql cannot read says
// why.
//
// ISO-2022-JP is seven-bit, so it passes the UTF-8 check and used to fail later
// as "column count mismatch": the escape sequences were read as text, so the
// record really did have the wrong number of fields as far as the reader was
// concerned. A caller saw a complaint about their data on a file filesql had
// just written from it.
func TestOpen_NamesAnEscapeEncodedInput(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	source := filepath.Join(dir, "t.csv")
	require.NoError(t, os.WriteFile(source, []byte("k,v\nrow,seed\n"), 0o600))

	db, err := Open(t.Context(), source)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	_, err = db.ExecContext(t.Context(), `UPDATE t SET v = ?`, "が")
	require.NoError(t, err)

	out := filepath.Join(dir, "out")
	require.NoError(t, DumpDatabase(context.Background(), db, out, NewDumpOptions().WithEncoding(EncodingISO2022JP)))

	_, err = Open(t.Context(), out)
	require.Error(t, err, "filesql reads UTF-8, so it cannot read what it just wrote")
	assert.ErrorIs(t, err, ErrEncoding)
	assert.Contains(t, err.Error(), "ISO-2022-JP")
}

// TestOpen_StillNamesInvalidUTF8 pins that the encodings whose bytes are not
// UTF-8 keep the clearer answer they already gave, rather than being swallowed
// by the escape check.
func TestOpen_StillNamesInvalidUTF8(t *testing.T) {
	t.Parallel()

	for _, enc := range []Encoding{EncodingShiftJIS, EncodingEUCJP} {
		t.Run(enc.String(), func(t *testing.T) {
			t.Parallel()

			dir := t.TempDir()
			source := filepath.Join(dir, "t.csv")
			require.NoError(t, os.WriteFile(source, []byte("k,v\nrow,seed\n"), 0o600))

			db, err := Open(t.Context(), source)
			require.NoError(t, err)
			t.Cleanup(func() { _ = db.Close() })
			_, err = db.ExecContext(t.Context(), `UPDATE t SET v = ?`, "日本語")
			require.NoError(t, err)

			out := filepath.Join(dir, "out")
			require.NoError(t, DumpDatabase(context.Background(), db, out, NewDumpOptions().WithEncoding(enc)))

			_, err = Open(t.Context(), out)
			require.Error(t, err)
			assert.ErrorIs(t, err, ErrInvalidUTF8)
		})
	}
}

// utf16Bytes encodes text in the given UTF-16 byte order, with a mark.
func utf16Bytes(t *testing.T, text string, order unicode.Endianness) []byte {
	t.Helper()

	encoded, _, err := transform.Bytes(unicode.UTF16(order, unicode.UseBOM).NewEncoder(), []byte(text))
	require.NoError(t, err)
	return encoded
}

// TestAutoSaveOverwriteKeepsSourceEncoding pins that a save in place writes back
// the encoding the file already used, the way it already writes back its
// compression and its line terminator.
//
// It did not: every source was written as plain UTF-8 with no mark. A UTF-16
// file — which the read side recognizes by that mark — was replaced by bytes no
// other reader of the file would take, and a UTF-8 file that carried a mark lost
// it, so a header row nobody edited changed and the spreadsheet program that
// wrote the file no longer recognized its encoding.
func TestAutoSaveOverwriteKeepsSourceEncoding(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		content func(t *testing.T, text string) []byte
	}{
		{
			name: "UTF-16LE",
			content: func(t *testing.T, text string) []byte {
				t.Helper()
				return utf16Bytes(t, text, unicode.LittleEndian)
			},
		},
		{
			name: "UTF-16BE",
			content: func(t *testing.T, text string) []byte {
				t.Helper()
				return utf16Bytes(t, text, unicode.BigEndian)
			},
		},
		{
			name: "UTF-8 with a byte-order mark",
			content: func(t *testing.T, text string) []byte {
				t.Helper()
				return append([]byte{0xEF, 0xBB, 0xBF}, text...)
			},
		},
		{
			name: "UTF-8 without one",
			content: func(t *testing.T, text string) []byte {
				t.Helper()
				return []byte(text)
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			path := filepath.Join(t.TempDir(), "data.csv")
			require.NoError(t, os.WriteFile(path, tt.content(t, "id,v\r\n1,a\r\n2,b\r\n"), 0o600))

			require.NoError(t, autoSaveOverwrite(t, []string{path}, "UPDATE data SET v='x' WHERE id=1"))

			got, err := os.ReadFile(path) //nolint:gosec // Test path from t.TempDir()
			require.NoError(t, err)
			assert.Equal(t, tt.content(t, "id,v\r\n1,x\r\n2,b\r\n"), got,
				"only the edited row may differ from what was there")
		})
	}
}

// TestAutoSaveOverwriteWithNoStatementKeepsEveryByte states the same property as
// an invariant: a database nobody wrote to has nothing to change on disk,
// whatever encoding the file is in.
func TestAutoSaveOverwriteWithNoStatementKeepsEveryByte(t *testing.T) {
	t.Parallel()

	text := "id,v\n1,a\n2,b\n"
	tests := []struct {
		name    string
		content []byte
	}{
		{name: "UTF-16LE", content: utf16Bytes(t, text, unicode.LittleEndian)},
		{name: "UTF-16BE", content: utf16Bytes(t, text, unicode.BigEndian)},
		{name: "UTF-8 with a byte-order mark", content: append([]byte{0xEF, 0xBB, 0xBF}, text...)},
		{name: "UTF-8 without one", content: []byte(text)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			path := filepath.Join(t.TempDir(), "data.csv")
			require.NoError(t, os.WriteFile(path, tt.content, 0o600))

			require.NoError(t, autoSaveOverwrite(t, []string{path}))

			got, err := os.ReadFile(path) //nolint:gosec // Test path from t.TempDir()
			require.NoError(t, err)
			assert.Equal(t, tt.content, got)
		})
	}
}

// TestAutoSaveExportIgnoresSourceEncoding pins the boundary: an export writes
// what DumpOptions says, so reading the source's encoding must not leak into it.
func TestAutoSaveExportIgnoresSourceEncoding(t *testing.T) {
	t.Parallel()

	source := filepath.Join(t.TempDir(), "data.csv")
	require.NoError(t, os.WriteFile(source, utf16Bytes(t, "id,v\n1,a\n", unicode.LittleEndian), 0o600))

	outputDir := t.TempDir()
	validated, err := buildForTest(t.Context(), NewBuilder().AddPath(source).EnableAutoSave(outputDir))
	require.NoError(t, err)
	db, err := validated.Open(t.Context())
	require.NoError(t, err)
	require.NoError(t, db.Close())

	got, err := os.ReadFile(filepath.Join(outputDir, "data.csv")) //nolint:gosec // Test path from t.TempDir()
	require.NoError(t, err)
	assert.Equal(t, "id,v\n1,a\n", string(got), "an export writes UTF-8 unless WithEncoding says otherwise")
}

// The names the encodings answer with. Each is written once here so the two
// tables below agree on what a given encoding is called.
const (
	nameUTF8      = "utf-8"
	nameShiftJIS  = "shift-jis"
	nameEUCJP     = "euc-jp"
	nameISO2022JP = "iso-2022-jp"
	nameUTF16LE   = "utf-16le"
	nameUTF16BE   = "utf-16be"
)

// TestEncoding_String pins the name of every encoding. The name is what a save
// error quotes back to the caller, so an encoding that answers with someone
// else's name misdirects whoever reads the failure.
func TestEncoding_String(t *testing.T) {
	t.Parallel()

	tests := []struct {
		encoding Encoding
		want     string
	}{
		{EncodingUTF8, nameUTF8},
		{EncodingShiftJIS, nameShiftJIS},
		{EncodingEUCJP, nameEUCJP},
		{EncodingISO2022JP, nameISO2022JP},
		{EncodingUTF16LE, nameUTF16LE},
		{EncodingUTF16BE, nameUTF16BE},
		{Encoding(99), "unknown"},
	}
	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, tt.encoding.String())
		})
	}
}

// TestEncoding_Encoder checks which encodings need a transformer. UTF-8 needs
// none because the values are already UTF-8, and an unknown value is treated the
// same way rather than being guessed at.
func TestEncoding_Encoder(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		encoding    Encoding
		wantEncoder bool
	}{
		{nameUTF8 + " needs no transformer", EncodingUTF8, false},
		{nameShiftJIS, EncodingShiftJIS, true},
		{nameEUCJP, EncodingEUCJP, true},
		{nameISO2022JP, EncodingISO2022JP, true},
		{nameUTF16LE, EncodingUTF16LE, true},
		{nameUTF16BE, EncodingUTF16BE, true},
		{"an unknown encoding needs no transformer", Encoding(99), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			transformer, ok := tt.encoding.encoder()
			assert.Equal(t, tt.wantEncoder, ok)
			if tt.wantEncoder {
				assert.NotNil(t, transformer)
				return
			}
			assert.Nil(t, transformer)
		})
	}
}

// TestEncoding_EncodingWriter covers both shapes of the writer wrapper: the
// encodings that need one get a writer whose failures are attributed to the
// encoder, and the ones that do not get their own writer back untouched.
func TestEncoding_EncodingWriter(t *testing.T) {
	t.Parallel()

	t.Run(nameUTF8+" hands back the same writer", func(t *testing.T) {
		t.Parallel()

		var buf bytes.Buffer
		w, encoded := EncodingUTF8.encodingWriter(&buf)
		assert.Nil(t, encoded, "there is nothing to attribute a failure to without an encoder")
		assert.False(t, encoded.encoderFailed(), "a nil encoded writer reports no failure")

		_, err := w.Write([]byte("hello"))
		require.NoError(t, err)
		assert.Equal(t, "hello", buf.String(), "UTF-8 values pass through unchanged")
	})

	t.Run(nameShiftJIS+" encodes what it writes", func(t *testing.T) {
		t.Parallel()

		var buf bytes.Buffer
		w, encoded := EncodingShiftJIS.encodingWriter(&buf)
		require.NotNil(t, encoded)

		_, err := w.Write([]byte("あ"))
		require.NoError(t, err)
		require.NoError(t, encoded.Close())
		assert.Equal(t, []byte{0x82, 0xa0}, buf.Bytes(), "あ is 0x82a0 in Shift-JIS")
		assert.False(t, encoded.encoderFailed())
	})
}

// TestEncodedWriter_RecordsItsOwnFailures checks the bookkeeping that separates
// "this encoding cannot write this table" from a failure to write the bytes at
// all. x/text reports an unwritable rune with an unexported error type, so the
// only exact record of it is the one taken here.
func TestEncodedWriter_RecordsItsOwnFailures(t *testing.T) {
	t.Parallel()

	t.Run("a failed write is recorded", func(t *testing.T) {
		t.Parallel()

		wantErr := errors.New("refused")
		w := &encodedWriter{
			w:      writerFunc(func([]byte) (int, error) { return 0, wantErr }),
			closer: func() error { return nil },
		}

		_, err := w.Write([]byte("あ"))
		require.ErrorIs(t, err, wantErr)
		assert.True(t, w.encoderFailed(), "the refusal must be attributed to the encoder")
	})

	t.Run("a failed close is recorded", func(t *testing.T) {
		t.Parallel()

		wantErr := errors.New("held back a partial sequence")
		w := &encodedWriter{
			w:      &bytes.Buffer{},
			closer: func() error { return wantErr },
		}

		require.ErrorIs(t, w.Close(), wantErr)
		assert.True(t, w.encoderFailed(), "a rune refused at flush time is still the encoder's refusal")
	})

	t.Run("a clean writer reports no failure", func(t *testing.T) {
		t.Parallel()

		w := &encodedWriter{w: &bytes.Buffer{}, closer: func() error { return nil }}
		_, err := w.Write([]byte("ok"))
		require.NoError(t, err)
		require.NoError(t, w.Close())
		assert.False(t, w.encoderFailed())
	})
}

// writerFunc turns a function into an io.Writer.
type writerFunc func(p []byte) (int, error)

func (f writerFunc) Write(p []byte) (int, error) { return f(p) }

// TestADumpRefusedByItsEncodingNamesTheColumnAndTheCharacter pins what a caller
// is told when the encoding they asked for cannot write something the table
// holds. The refusal used to say only that the table held such a value --
// "shift-jis cannot write a value in this table" -- so a caller with forty
// columns had to go looking, where the refusal beside it, for a value that is
// not valid UTF-8, has named its column since it was written.
func TestADumpRefusedByItsEncodingNamesTheColumnAndTheCharacter(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	src := filepath.Join(dir, "t.csv")
	if err := os.WriteFile(src, []byte("id,name,note\n1,ok,x\n2,\"smile 🙂\",x\n"), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	db, err := Open(t.Context(), src)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	for _, encoding := range []Encoding{EncodingShiftJIS, EncodingEUCJP, EncodingISO2022JP} {
		err := DumpDatabase(context.Background(), db, t.TempDir(), NewDumpOptions().WithEncoding(encoding))
		if err == nil {
			t.Errorf("a dump into %s of a table holding an emoji succeeded, want a refusal", encoding)
			continue
		}
		if !errors.Is(err, ErrEncoding) {
			t.Errorf("dump into %s = %v, want ErrEncoding", encoding, err)
		}
		for _, want := range []string{`column "name"`, "U+1F642", encoding.String()} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("dump into %s = %v, want the message to hold %q", encoding, err, want)
			}
		}
	}
}

// TestADumpRefusedByItsEncodingNamesAColumnName pins the other place the
// character can sit.
func TestADumpRefusedByItsEncodingNamesAColumnName(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	src := filepath.Join(dir, "t.csv")
	if err := os.WriteFile(src, []byte("id,smile🙂\n1,2\n"), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	db, err := Open(t.Context(), src)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	err = DumpDatabase(context.Background(), db, t.TempDir(), NewDumpOptions().WithEncoding(EncodingShiftJIS))
	if !errors.Is(err, ErrEncoding) {
		t.Fatalf("dump = %v, want ErrEncoding", err)
	}
	for _, want := range []string{"column 2", "U+1F642", "shift-jis"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("dump = %v, want the message to hold %q", err, want)
		}
	}
}

// TestADumpInAnEncodingThatHoldsTheTableStillWrites pins the sibling: an
// encoding that can write what the table holds is untouched by the check.
func TestADumpInAnEncodingThatHoldsTheTableStillWrites(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	src := filepath.Join(dir, "t.csv")
	if err := os.WriteFile(src, []byte("id,name\n1,日本語\n"), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	db, err := Open(t.Context(), src)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	for _, encoding := range []Encoding{EncodingUTF8, EncodingShiftJIS, EncodingEUCJP, EncodingUTF16LE} {
		out := t.TempDir()
		if err := DumpDatabase(context.Background(), db, out, NewDumpOptions().WithEncoding(encoding)); err != nil {
			t.Errorf("dump into %s = %v, want it to write", encoding, err)
			continue
		}
		body, err := os.ReadFile(filepath.Join(out, "t.csv")) //nolint:gosec // Test path from t.TempDir()
		if err != nil {
			t.Errorf("read the dump: %v", err)
			continue
		}
		if len(body) == 0 {
			t.Errorf("dump into %s wrote nothing", encoding)
		}
	}
}
