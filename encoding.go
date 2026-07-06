package filesql

import (
	"io"

	"golang.org/x/text/encoding/unicode"
	"golang.org/x/text/transform"
)

// isTextBaseType reports whether a base file type is a text format whose byte
// stream may carry a Unicode byte-order mark. Binary containers (Parquet, XLSX)
// and record formats with their own framing (ACH, Fedwire) must not be decoded.
func isTextBaseType(ft FileType) bool {
	switch ft {
	case FileTypeCSV, FileTypeTSV, FileTypeLTSV, FileTypeJSON, FileTypeJSONL:
		return true
	default:
		return false
	}
}

// decodeTextReader wraps a text reader so a leading Unicode byte-order mark is
// honored: a UTF-8 BOM is stripped and UTF-16 (LE or BE) input is transcoded to
// UTF-8. Editors and shells such as Excel, Notepad, and PowerShell prepend these
// marks, and without this a UTF-8 BOM leaks into the first column name while a
// UTF-16 file is misread as single-byte data.
//
// Input without a recognized BOM passes through unchanged, so ordinary UTF-8 is
// untouched and a non-Unicode legacy encoding (for example Shift-JIS) keeps its
// original bytes instead of being lossily replaced. Only call this for text
// formats; see isTextBaseType.
func decodeTextReader(reader io.Reader) io.Reader {
	return transform.NewReader(reader, unicode.BOMOverride(transform.Nop))
}
