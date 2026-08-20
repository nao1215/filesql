package filesql

import (
	"bytes"
	"context"
	"encoding/csv"
	"errors"
	"io"
	"os"
	"path/filepath"
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
			db, err := OpenContext(ctx, source)
			require.NoError(t, err)
			defer db.Close()

			out := filepath.Join(dir, "out")
			require.NoError(t, os.MkdirAll(out, 0o750))
			require.NoError(t, DumpDatabase(db, out, NewDumpOptions().WithEncoding(tt.encoding)))

			written, err := os.ReadFile(filepath.Join(out, "src.csv")) //nolint:gosec // path built by the test
			require.NoError(t, err)

			decoded, err := io.ReadAll(transform.NewReader(bytes.NewReader(written), tt.decoder))
			require.NoError(t, err)
			assert.Contains(t, string(decoded), "山田ソ", "the values should survive the encoding")
			assert.Contains(t, string(decoded), "東京")

			// Re-opening through filesql is the other half: the reader's BOM
			// handling has to recognize what the writer produced.
			if tt.encoding == EncodingUTF16LE || tt.encoding == EncodingUTF16BE {
				reopened, err := OpenContext(ctx, filepath.Join(out, "src.csv"))
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
	db, err := OpenContext(ctx, source)
	require.NoError(t, err)
	defer db.Close()

	out := filepath.Join(dir, "out")
	require.NoError(t, os.MkdirAll(out, 0o750))

	err = DumpDatabase(db, out, NewDumpOptions().WithEncoding(EncodingShiftJIS))
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
	db, err := OpenContext(ctx, source)
	require.NoError(t, err)
	defer db.Close()

	out := filepath.Join(dir, "out")
	require.NoError(t, os.MkdirAll(out, 0o750))
	require.NoError(t, DumpDatabase(db, out,
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
	db, err := OpenContext(ctx, source)
	require.NoError(t, err)
	defer db.Close()

	for _, format := range []OutputFormat{OutputFormatParquet, OutputFormatXLSX} {
		out := filepath.Join(dir, format.Extension())
		require.NoError(t, os.MkdirAll(out, 0o750))
		require.NoError(t, DumpDatabase(db, out,
			NewDumpOptions().WithFormat(format).WithEncoding(EncodingShiftJIS)))

		reopened, err := OpenContext(ctx, filepath.Join(out, "src"+format.Extension()))
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

			db, err := OpenContext(t.Context(), source)
			require.NoError(t, err)
			t.Cleanup(func() { _ = db.Close() })
			_, err = db.ExecContext(t.Context(), `UPDATE t SET v = ?`, value)
			require.NoError(t, err)

			out := filepath.Join(dir, "out")
			require.NoError(t, DumpDatabase(db, out, NewDumpOptions().WithEncoding(EncodingISO2022JP)))

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

	db, err := OpenContext(t.Context(), source)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	_, err = db.ExecContext(t.Context(), `UPDATE t SET v = ?`, "が")
	require.NoError(t, err)

	out := filepath.Join(dir, "out")
	require.NoError(t, DumpDatabase(db, out, NewDumpOptions().WithEncoding(EncodingISO2022JP)))

	_, err = OpenContext(t.Context(), out)
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

			db, err := OpenContext(t.Context(), source)
			require.NoError(t, err)
			t.Cleanup(func() { _ = db.Close() })
			_, err = db.ExecContext(t.Context(), `UPDATE t SET v = ?`, "日本語")
			require.NoError(t, err)

			out := filepath.Join(dir, "out")
			require.NoError(t, DumpDatabase(db, out, NewDumpOptions().WithEncoding(enc)))

			_, err = OpenContext(t.Context(), out)
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
	validated, err := NewBuilder().AddPath(source).EnableAutoSave(outputDir).Build(t.Context())
	require.NoError(t, err)
	db, err := validated.Open(t.Context())
	require.NoError(t, err)
	require.NoError(t, db.Close())

	got, err := os.ReadFile(filepath.Join(outputDir, "data.csv")) //nolint:gosec // Test path from t.TempDir()
	require.NoError(t, err)
	assert.Equal(t, "id,v\n1,a\n", string(got), "an export writes UTF-8 unless WithEncoding says otherwise")
}
