package reader

import (
	"archive/zip"
	"bytes"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOpenWorkbookRejectsWrappedZipSize(t *testing.T) {
	t.Parallel()
	var data bytes.Buffer
	archive := zip.NewWriter(&data)
	_, err := archive.CreateRaw(&zip.FileHeader{
		Name: "xl/worksheets/sheet1.xml", Method: zip.Store,
		UncompressedSize64: uint64(1) << 63,
	})
	require.NoError(t, err)
	require.NoError(t, archive.Close())
	require.NotPanics(t, func() {
		book, err := OpenWorkbook(bytes.NewReader(data.Bytes()))
		if book != nil {
			require.NoError(t, book.Close())
		}
		require.Error(t, err)
	})
}

func TestWorkbookRejectsOversizedRowAfterValidRow(t *testing.T) {
	t.Parallel()
	for name, replacement := range map[string]string{
		"plain":    `r="1048577"`,
		"entity":   `r="&#49;048577"`,
		"long tag": `extra="` + strings.Repeat("x", 5000) + `" r="1048577"`,
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			data := workbookOf(t, [][]string{{"header"}, {"value"}})
			data = rewritePart(t, data, "xl/worksheets/sheet2.xml", func(s string) string {
				return strings.ReplaceAll(s, `r="2"`, replacement)
			})
			book, err := OpenWorkbook(bytes.NewReader(data))
			require.NoError(t, err)
			defer func() { require.NoError(t, book.Close()) }()
			_, err = book.Rows("people")
			require.Error(t, err)
		})
	}
}

func TestReadXLSXRejectsNegativeStyleIndices(t *testing.T) {
	t.Parallel()
	for _, attr := range []string{"fillId", "borderId", "fontId"} {
		t.Run(attr, func(t *testing.T) {
			t.Parallel()
			data := workbookOf(t, [][]string{{"header"}, {"value"}})
			data = rewritePart(t, data, "xl/styles.xml", func(s string) string {
				return strings.ReplaceAll(s, attr+`="0"`, attr+`="-1"`)
			})
			require.NotPanics(t, func() {
				_, _, err := readSheet(t, data)
				require.Error(t, err)
			})
		})
	}
}

func renameArchiveParts(t *testing.T, data []byte, rename func(string) string) []byte {
	t.Helper()
	archive, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
	require.NoError(t, err)
	var result bytes.Buffer
	out := zip.NewWriter(&result)
	for _, part := range archive.File {
		body, err := part.Open()
		require.NoError(t, err)
		content, err := io.ReadAll(body)
		require.NoError(t, err)
		require.NoError(t, body.Close())
		writer, err := out.Create(rename(part.Name))
		require.NoError(t, err)
		_, err = writer.Write(content)
		require.NoError(t, err)
	}
	require.NoError(t, out.Close())
	return result.Bytes()
}

func TestWorkbookBackslashParts(t *testing.T) {
	t.Parallel()
	data := workbookOf(t, [][]string{{"header"}, {"value"}})
	data = renameArchiveParts(t, data, func(name string) string { return strings.ReplaceAll(name, "/", "\\") })
	_, _, err := readSheet(t, data)
	require.NoError(t, err)
}

func TestWorkbookRejectsBackslashNegativeStyles(t *testing.T) {
	t.Parallel()
	data := workbookOf(t, [][]string{{"header"}, {"value"}})
	data = rewritePart(t, data, "xl/styles.xml", func(s string) string { return strings.ReplaceAll(s, `fillId="0"`, `fillId="-1"`) })
	data = renameArchiveParts(t, data, func(name string) string {
		if name == "xl/styles.xml" {
			return "xl\\styles.xml"
		}
		return name
	})
	require.NotPanics(t, func() {
		_, _, err := readSheet(t, data)
		require.Error(t, err)
	})
}

func TestWorkbookRejectsDuplicateWorksheet(t *testing.T) {
	t.Parallel()
	data := workbookOf(t, [][]string{{"header"}, {"value"}})
	archive, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
	require.NoError(t, err)
	var result bytes.Buffer
	out := zip.NewWriter(&result)
	for _, part := range archive.File {
		require.NoError(t, out.Copy(part))
	}
	writer, err := out.Create("xl/worksheets/sheet2.xml")
	require.NoError(t, err)
	_, err = io.WriteString(writer, `<worksheet><sheetData><row r="1"><c r="A1" t="inlineStr"><is><t>header</t></is></c></row><row r="1048577"/></sheetData></worksheet>`)
	require.NoError(t, err)
	require.NoError(t, out.Close())
	book, err := OpenWorkbook(bytes.NewReader(result.Bytes()))
	if book != nil {
		require.NoError(t, book.Close())
	}
	require.Error(t, err)
}

func TestWorksheetBackslashRelationship(t *testing.T) {
	t.Parallel()
	data := workbookOf(t, [][]string{{"header"}, {"value"}})
	data = rewritePart(t, data, "xl/_rels/workbook.xml.rels", func(s string) string {
		return strings.ReplaceAll(s, "worksheets/", "worksheets\\")
	})
	_, _, err := readSheet(t, data)
	require.NoError(t, err)
}
