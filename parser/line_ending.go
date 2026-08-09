package parser

import (
	"bufio"
	"bytes"
	"errors"
	"io"
)

// lineEndingSniffLimit is how much of a file is inspected to decide whether it
// ends its lines with a lone carriage return. A file that uses LF has a line
// feed in its first row, so a window this size always holds the answer; it costs
// one 64 KiB buffer, which then serves as this reader's read buffer.
const lineEndingSniffLimit = 64 * 1024

// NormalizeLineEndings wraps reader so a file whose lines end with a lone
// carriage return is read as lines rather than as one very long line.
//
// CSV readers, and everything here that splits on "\n", understand LF and CRLF.
// A file written with the classic Mac OS 9 convention has neither, so without
// this the whole file parses as a single line: the data folds into the column
// names and the table comes out with zero rows, at no error.
//
// The convention is decided from the start of the file rather than by
// translating every carriage return that appears. A carriage return inside a
// quoted field is data, and a file that uses LF shows a line feed within the
// first 64 KiB, so translation happens only when that window holds a carriage
// return and no line feed at all.
func NormalizeLineEndings(reader io.Reader) io.Reader {
	return &lineEndingReader{buffered: bufio.NewReaderSize(reader, lineEndingSniffLimit)}
}

// lineEndingReader translates lone carriage returns to line feeds for a source
// that uses them as its line terminator.
type lineEndingReader struct {
	buffered  *bufio.Reader
	sniffed   bool
	translate bool
	// The error the sniff took, held until the bytes read before it have been
	// handed on. bufio.Reader.Peek consumes the source's error, so dropping the
	// peeked result would drop the error too — and a source that rejects its own
	// input, such as the UTF-8 validator, has no other way to report it.
	sniffErr error
}

// Read implements io.Reader, translating carriage returns when the sniff said to
// and replaying the source's error once the bytes read before it are handed on.
func (l *lineEndingReader) Read(p []byte) (int, error) {
	if !l.sniffed {
		l.sniff()
	}

	n, err := l.buffered.Read(p)
	if l.translate {
		for i := range n {
			if p[i] == '\r' {
				p[i] = '\n'
			}
		}
	}
	if err == nil && l.sniffErr != nil && l.buffered.Buffered() == 0 {
		err = l.sniffErr
		l.sniffErr = nil
	}
	return n, err
}

// sniff decides, once, whether this source uses carriage returns as its line
// terminator, and keeps whatever error the peek took from the source.
func (l *lineEndingReader) sniff() {
	l.sniffed = true

	window, err := l.buffered.Peek(lineEndingSniffLimit)
	// A window shorter than the limit means a file shorter than the limit, which
	// is an answer rather than a failure: the bytes peeked are the whole file.
	// Any other error belongs to the source and is replayed by Read.
	if err != nil && !errors.Is(err, io.EOF) {
		l.sniffErr = err
	}
	l.translate = !bytes.ContainsRune(window, '\n') && bytes.ContainsRune(window, '\r')
}
