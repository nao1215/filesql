package filesql

import (
	"bytes"
	"context"
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
