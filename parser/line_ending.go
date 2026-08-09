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
// translating every carriage return that appears, because a carriage return
// inside a quoted field is data. Translation happens only for a file whose first
// 64 KiB hold a carriage return outside quotes and no line feed at all, and only
// carriage returns outside quotes are translated, so a quoted one survives in
// either kind of file. A first record longer than that window is left alone
// rather than guessed at: reading it as one line is what happens today, while a
// wrong guess would rewrite data.
func NormalizeLineEndings(reader io.Reader) io.Reader {
	return &lineEndingReader{buffered: bufio.NewReaderSize(reader, lineEndingSniffLimit)}
}

// lineEndingReader translates lone carriage returns to line feeds for a source
// that uses them as its line terminator.
type lineEndingReader struct {
	buffered  *bufio.Reader
	sniffed   bool
	translate bool
	// inQuotes tracks whether the byte about to be read sits inside a quoted
	// field, carried across reads because a field can span them. A doubled quote
	// leaves and re-enters the field, which lands on the same answer.
	inQuotes bool
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
			switch p[i] {
			case '"':
				l.inQuotes = !l.inQuotes
			case '\r':
				if !l.inQuotes {
					p[i] = '\n'
				}
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
	l.translate = usesCarriageReturnTerminator(window)
}

// usesCarriageReturnTerminator reports whether window comes from a file that
// ends its lines with a lone carriage return.
//
// Two conditions, because either alone accepts a file it should not. A line feed
// anywhere says the file already has a terminator this stack understands, so its
// carriage returns are data. A carriage return inside quotes is data as well,
// and a record long enough to fill the window can hold one, so the carriage
// return that decides has to be outside them.
func usesCarriageReturnTerminator(window []byte) bool {
	if bytes.ContainsRune(window, '\n') {
		return false
	}

	inQuotes := false
	for _, c := range window {
		switch c {
		case '"':
			inQuotes = !inQuotes
		case '\r':
			if !inQuotes {
				return true
			}
		}
	}
	return false
}
