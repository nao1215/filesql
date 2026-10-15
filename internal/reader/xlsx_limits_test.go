package reader

import (
	"archive/zip"
	"bytes"
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
