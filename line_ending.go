package filesql

import (
	"bufio"
	"io"
)

// LineEnding is the line terminator a save writes its records with.
//
// A save already keeps a source's compression and its text encoding, but wrote
// every record with "\n" whatever the file used. A CRLF file saved in place came
// back LF throughout: every line of the file changed while the caller had edited
// one row, so the file no longer matched what a CRLF-expecting reader — or a
// repository configured for CRLF — had before. This is that decision made
// explicit, and an in-place save makes it from the file itself.
type LineEnding int

const (
	// LineEndingLF writes "\n", which is what a save wrote before this option
	// existed and remains the default.
	LineEndingLF LineEnding = iota
	// LineEndingCRLF writes "\r\n".
	LineEndingCRLF
)

// String returns the name a user types for the line ending.
func (l LineEnding) String() string {
	switch l {
	case LineEndingLF:
		return "lf"
	case LineEndingCRLF:
		return "crlf"
	default:
		return "unknown"
	}
}

// terminator returns the bytes the line ending is written as. An unknown value
// writes "\n", the same answer a zero DumpOptions gives.
func (l LineEnding) terminator() string {
	if l == LineEndingCRLF {
		return "\r\n"
	}
	return "\n"
}

// lineEndingReadSize is the buffer the detection reads through. The file is
// counted whole — a terminator that is in the minority over the first megabyte
// can be the majority over the file — but never held whole, so a file larger
// than memory costs the same as a small one.
const lineEndingReadSize = 64 << 10 // 64 KiB

// detectLineEnding reports the line terminator path already uses, so a save that
// overwrites it can write the same one. format is what the file will be written
// back as, which is what says whether a quote in it means anything.
//
// The rule is the majority of the terminators in the file, and LF on a tie or on
// a file with no line ending at all. Majority rather than first-line-wins
// because the point is to leave rows the caller did not edit byte-identical: a
// file that is LF except for one stray CRLF stays LF, where following the first
// line would rewrite every other line in it.
//
// A file this package cannot read is reported as LF, which is what a save wrote
// before this existed. The detection is an improvement on the destination's
// behalf, so failing to detect must not fail the save — and a partial count is
// not used either, because half a file is not evidence of what the whole one
// uses.
func detectLineEnding(path string, format OutputFormat) LineEnding {
	reader, cleanup, err := NewCompressionFactory().CreateReaderForFile(path)
	if err != nil {
		return LineEndingLF
	}
	defer func() {
		_ = cleanup() //nolint:errcheck // Reading for detection only; a close failure cannot affect the answer
	}()

	ending, err := countLineEndings(reader, format)
	if err != nil {
		return LineEndingLF
	}
	return ending
}

// countLineEndings is detectLineEnding's rule over a stream, reading it whole
// through a fixed buffer.
//
// A quote is honored for CSV alone. A quoted CSV field carries its own line
// breaks, and counting those as terminators is how a workbook-style export —
// CRLF between records, LF inside a quoted address — was read as an LF file and
// rewritten as one. TSV and LTSV define no quoting at all: a quote there is
// data, and tracking it would let one unmatched quote swallow the rest of the
// file's terminators.
func countLineEndings(reader io.Reader, format OutputFormat) (LineEnding, error) {
	quoted := format == OutputFormatCSV

	buffered := bufio.NewReaderSize(reader, lineEndingReadSize)
	var crlf, lf int
	inQuotes := false
	prevCR := false
	for {
		b, err := buffered.ReadByte()
		if err != nil {
			if err == io.EOF {
				break
			}
			return LineEndingLF, err
		}

		switch {
		case quoted && b == '"':
			// A doubled quote inside a field is an escaped quote, which this
			// toggling handles on its own: the pair leaves the state as it found it.
			inQuotes = !inQuotes
		case b == '\n' && !inQuotes:
			if prevCR {
				crlf++
			} else {
				lf++
			}
		}
		prevCR = b == '\r'
	}

	if crlf > lf {
		return LineEndingCRLF, nil
	}
	return LineEndingLF, nil
}
