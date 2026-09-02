package filesql

import (
	"bytes"
	"database/sql"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/nao1215/filesql/internal/parser"
	"github.com/nao1215/filesql/prep"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fileTypePair is one format and the parser constant that names it. Only
// formats appear here: FileType no longer has a constant for a codec.
type fileTypePair struct {
	name    string
	filesql FileType
	parser  parser.FileType
}

func parserBackedFileTypePairs() []fileTypePair {
	return []fileTypePair{
		{"CSV", FileTypeCSV, parser.CSV},
		{"TSV", FileTypeTSV, parser.TSV},
		{"LTSV", FileTypeLTSV, parser.LTSV},
		{"Parquet", FileTypeParquet, parser.Parquet},
		{"XLSX", FileTypeXLSX, parser.XLSX},
		{"JSON", FileTypeJSON, parser.JSON},
		{"JSONL", FileTypeJSONL, parser.JSONL},
	}
}

func TestParserFileType(t *testing.T) {
	t.Parallel()

	for _, tt := range parserBackedFileTypePairs() {
		t.Run(tt.name, func(t *testing.T) {
			result := parserFileType(tt.filesql)
			assert.Equal(t, tt.parser, result)
		})
	}

	t.Run("Unsupported", func(t *testing.T) {
		result := parserFileType(FileTypeUnsupported)
		assert.Equal(t, parser.Unsupported, result)
	})

	t.Run("Unknown type", func(t *testing.T) {
		result := parserFileType(FileType(9999))
		assert.Equal(t, parser.Unsupported, result)
	})

	// ACH and Fedwire are filesql's own formats; the parser does not handle
	// them, so the bridge has nothing to hand it.
	for _, ft := range []FileType{FileTypeACH, FileTypeFedWire} {
		t.Run(ft.String(), func(t *testing.T) {
			assert.Equal(t, parser.Unsupported, parserFileType(ft))
		})
	}
}

// TestParserFileType_RoundTrip pins that a format survives the trip through
// the parser's enum and back.
func TestParserFileType_RoundTrip(t *testing.T) {
	t.Parallel()

	for _, tt := range parserBackedFileTypePairs() {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.filesql, filesqlFileType(parserFileType(tt.filesql)))
		})
	}
}

func TestFilesqlFileType(t *testing.T) {
	t.Parallel()

	for _, tt := range parserBackedFileTypePairs() {
		t.Run(tt.name, func(t *testing.T) {
			result := filesqlFileType(tt.parser)
			assert.Equal(t, tt.filesql, result)
		})
	}

	t.Run("Unsupported", func(t *testing.T) {
		result := filesqlFileType(parser.Unsupported)
		assert.Equal(t, FileTypeUnsupported, result)
	})

	t.Run("Unknown type", func(t *testing.T) {
		result := filesqlFileType(parser.FileType(9999))
		assert.Equal(t, FileTypeUnsupported, result)
	})
}

func TestParserColumnType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    parser.ColumnType
		expected columnType
	}{
		{"Integer", parser.TypeInteger, columnTypeInteger},
		{"Real", parser.TypeReal, columnTypeReal},
		{"Datetime", parser.TypeDatetime, columnTypeDatetime},
		{"Text", parser.TypeText, columnTypeText},
		{"Unknown defaults to Text", parser.ColumnType(9999), columnTypeText},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := parserColumnType(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestPrepReaderLoadsUnderTheFormatItReports pins the handoff between the two
// packages, which is what this bridge exists for. prep serves a JSON file as
// JSONL and an XLSX or Parquet file as CSV, so the format to declare to
// AddReader is the one the result reports and not the one that was read, and
// what the reader serves has to load back to the table the file loads as on its
// own. Documenting the reader as preserving the input format is what made a
// caller declare the wrong one.
func TestPrepReaderLoadsUnderTheFormatItReports(t *testing.T) {
	t.Parallel()

	type textRow struct {
		ID   string `json:"id"`
		Name string `json:"name"`
		Note string `json:"note"`
	}
	type jsonRow struct {
		Data string `json:"data"`
	}

	// The seed every tabular case is built from, holding the values a
	// conversion through CSV is most likely to spoil.
	seedCSV := "id,name,note\n1,007,\"a, comma\"\n2,,9223372036854775807\n3,alice,日本語\n"

	dir := t.TempDir()
	seedPath := filepath.Join(dir, "seed.csv")
	require.NoError(t, os.WriteFile(seedPath, []byte(seedCSV), 0o600))

	// tabular writes the seed out in format and returns the path, so every
	// case below is a path and the table needs no callback.
	tabular := func(format OutputFormat, ext string) string {
		db, err := Open(t.Context(), seedPath)
		require.NoError(t, err)
		defer db.Close()
		out := filepath.Join(dir, ext[1:])
		require.NoError(t, os.MkdirAll(out, 0o750))
		require.NoError(t, DumpDatabase(t.Context(), db, out, NewDumpOptions().WithFormat(format)))
		return filepath.Join(out, "seed"+ext)
	}
	write := func(name, body string) string {
		path := filepath.Join(dir, name)
		require.NoError(t, os.WriteFile(path, []byte(body), 0o600))
		return path
	}

	for _, tt := range []struct {
		name  string
		path  string
		input prep.FileType
		dest  func() any
	}{
		{name: "csv", path: seedPath, input: prep.FileTypeCSV, dest: func() any { return &[]textRow{} }},
		{name: "tsv", path: write("seed.tsv", "id\tname\tnote\n1\t007\ta comma\n2\t\t9223372036854775807\n"), input: prep.FileTypeTSV, dest: func() any { return &[]textRow{} }},
		{name: "ltsv", path: write("seed.ltsv", "id:1\tname:007\tnote:a comma\nid:2\tname:\tnote:\u65e5\u672c\u8a9e\n"), input: prep.FileTypeLTSV, dest: func() any { return &[]textRow{} }},
		{name: "json", path: write("seed.json", `[{"id":"1","name":"007"},{"id":"2","name":""}]`), input: prep.FileTypeJSON, dest: func() any { return &[]jsonRow{} }},
		{name: "jsonl", path: write("seed.jsonl", "{\"id\":\"1\"}\n{\"id\":\"2\"}\n"), input: prep.FileTypeJSONL, dest: func() any { return &[]jsonRow{} }},
		{name: "xlsx", path: tabular(OutputFormatXLSX, ".xlsx"), input: prep.FileTypeXLSX, dest: func() any { return &[]textRow{} }},
		{name: "parquet", path: tabular(OutputFormatParquet, ".parquet"), input: prep.FileTypeParquet, dest: func() any { return &[]textRow{} }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			table := strings.TrimSuffix(filepath.Base(tt.path), filepath.Ext(tt.path))

			source, err := os.Open(tt.path)
			require.NoError(t, err)
			defer source.Close()

			reader, result, err := prep.NewProcessor(tt.input).Process(source, tt.dest())
			require.NoError(t, err)
			require.Empty(t, result.Errors)
			require.Equal(t, tt.input, result.OriginalFormat, "the result has to name the format it read")

			body, err := io.ReadAll(reader)
			require.NoError(t, err)

			// The whole of the contract: the reported format is the one that
			// reads these bytes.
			declared := fileTypeOfPrepFormat(t, result.OutputFormat)
			require.NotEqual(t, FileTypeUnsupported, declared, "every format prep reports has to have a filesql name")

			throughPrep, err := NewBuilder().AddReader(bytes.NewReader(body), table, declared).Open(t.Context())
			require.NoError(t, err)
			defer throughPrep.Close()

			direct, err := Open(t.Context(), tt.path)
			require.NoError(t, err)
			defer direct.Close()

			assert.Equal(t, tableRows(t, direct, table), tableRows(t, throughPrep, table),
				"preprocessing nothing must leave the table the file loads as")
		})
	}
}

// tableRows renders a table as comparable text, so two loads of the same data
// can be compared whatever they were read from.
func tableRows(t *testing.T, db *sql.DB, table string) []string {
	t.Helper()

	rows, err := db.QueryContext(t.Context(), "SELECT * FROM "+quoteIdentifier(table)) //nolint:gosec // the table name is quoted
	require.NoError(t, err)
	defer rows.Close()

	columns, err := rows.Columns()
	require.NoError(t, err)
	rendered := []string{strings.Join(columns, "|")}
	for rows.Next() {
		values := make([]any, len(columns))
		into := make([]any, len(columns))
		for i := range values {
			into[i] = &values[i]
		}
		require.NoError(t, rows.Scan(into...))
		cells := make([]string, len(values))
		for i, value := range values {
			if value == nil {
				cells[i] = "<NULL>"
				continue
			}
			cells[i] = fmt.Sprint(value)
		}
		rendered = append(rendered, strings.Join(cells, "|"))
	}
	require.NoError(t, rows.Err())
	return rendered
}

// fileTypeOfPrepFormat is the mapping a caller writes to hand prep's output to
// this package, spelled once here rather than in every test that needs it. It
// is deliberately a switch rather than a numeric conversion: prep names its own
// formats, and two enums that happen to count in the same order today are not a
// contract.
func fileTypeOfPrepFormat(t *testing.T, format prep.FileType) FileType {
	t.Helper()
	switch format {
	case prep.FileTypeCSV:
		return FileTypeCSV
	case prep.FileTypeTSV:
		return FileTypeTSV
	case prep.FileTypeLTSV:
		return FileTypeLTSV
	case prep.FileTypeJSON:
		return FileTypeJSON
	case prep.FileTypeJSONL:
		return FileTypeJSONL
	case prep.FileTypeXLSX:
		return FileTypeXLSX
	case prep.FileTypeParquet:
		return FileTypeParquet
	default:
		return FileTypeUnsupported
	}
}

// TestPrepAndFilesqlNameTheSameFormats holds the two format vocabularies
// together. prep and this package each declare their own enum, because neither
// is the other's dependency, and a format one of them can produce that the
// other cannot name would leave a caller of the prep pipeline with a reader and
// no way to load it.
func TestPrepAndFilesqlNameTheSameFormats(t *testing.T) {
	t.Parallel()

	pairs := map[prep.FileType]FileType{
		prep.FileTypeCSV:     FileTypeCSV,
		prep.FileTypeTSV:     FileTypeTSV,
		prep.FileTypeLTSV:    FileTypeLTSV,
		prep.FileTypeJSON:    FileTypeJSON,
		prep.FileTypeJSONL:   FileTypeJSONL,
		prep.FileTypeXLSX:    FileTypeXLSX,
		prep.FileTypeParquet: FileTypeParquet,
	}
	for prepType, filesqlType := range pairs {
		assert.Equal(t, prepType.String(), filesqlType.String(),
			"prep and filesql have to spell the same format the same way")
	}
	assert.Equal(t, "Unsupported", prep.FileTypeUnsupported.String())
	assert.Equal(t, "Unsupported", FileTypeUnsupported.String())
	assert.Len(t, pairs, int(prep.FileTypeUnsupported),
		"every prep format below Unsupported needs a filesql name")
}
