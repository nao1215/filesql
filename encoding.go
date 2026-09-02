package filesql

import (
	"bytes"
	"io"

	"github.com/nao1215/filesql/internal/textin"
)

// isTextBaseType reports whether a base file type is a text format whose byte
// stream may carry a Unicode byte-order mark. The answer is the reader's, since
// both this package and parser ask it of the same formats; a type the reader
// has no name for -- ACH and Fedwire, which carry their own framing -- is not
// text here either.
func isTextBaseType(ft FileType) bool {
	format, supported := readerFormats[ft]
	return supported && format.IsText()
}

// bomEncodings maps a leading byte-order mark to the encoding that wrote it.
// These are exactly the encodings the read side recognizes without being told,
// which is what makes them the ones an in-place save can put back -- so the
// marks themselves come from the reader rather than being written out again
// here, where the two lists would be one rule with two answers.
var bomEncodings = []struct {
	mark     []byte
	encoding Encoding
}{
	{mark: textin.BOMUTF16LE, encoding: EncodingUTF16LE},
	{mark: textin.BOMUTF16BE, encoding: EncodingUTF16BE},
	{mark: textin.BOMUTF8, encoding: encodingUTF8BOM},
}

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
	if !isTextBaseType(baseFileType(path)) {
		return EncodingUTF8
	}

	reader, cleanup, err := openDecompressed(path)
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
