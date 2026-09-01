package filesql

import (
	"bytes"
	"io"
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

// bomEncodings maps a leading byte-order mark to the encoding that wrote it.
// These are exactly the encodings the read side recognizes without being told,
// which is what makes them the ones an in-place save can put back.
var bomEncodings = []struct {
	mark     []byte
	encoding Encoding
}{
	{mark: bomUTF16LE, encoding: EncodingUTF16LE},
	{mark: bomUTF16BE, encoding: EncodingUTF16BE},
	{mark: bomUTF8, encoding: encodingUTF8BOM},
}

// The byte-order marks this package recognizes on the read side.
var (
	bomUTF16LE = []byte{0xFF, 0xFE}       //nolint:gochecknoglobals // constant-like
	bomUTF16BE = []byte{0xFE, 0xFF}       //nolint:gochecknoglobals // constant-like
	bomUTF8    = []byte{0xEF, 0xBB, 0xBF} //nolint:gochecknoglobals // constant-like
)

// detectSourceEncoding reports the text encoding path is written in, so a save
// that overwrites it can write the same one. A save that always wrote plain
// UTF-8 replaced a UTF-16 file with bytes its own reader would still accept but
// every other reader of that file would not, and dropped the mark a UTF-8 file
// carried, changing a header row nobody had edited.
//
// Only the marks decide. A file with no mark is UTF-8, because that is the only
// thing the read side accepts without one, and a binary or record format is left
// alone: nothing in it is text this package encodes.
func detectSourceEncoding(path string) Encoding {
	factory := NewCompressionFactory()
	if !isTextBaseType(factory.getBaseFileType(path)) {
		return EncodingUTF8
	}

	reader, cleanup, err := factory.CreateReaderForFile(path)
	if err != nil {
		return EncodingUTF8
	}
	defer func() {
		_ = cleanup() //nolint:errcheck // Reading for detection only; a close failure cannot affect the answer
	}()

	var head [3]byte
	n, err := io.ReadFull(reader, head[:])
	if err != nil && n == 0 {
		return EncodingUTF8
	}
	for _, candidate := range bomEncodings {
		if bytes.HasPrefix(head[:n], candidate.mark) {
			return candidate.encoding
		}
	}
	return EncodingUTF8
}
