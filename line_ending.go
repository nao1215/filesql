package filesql

import (
	"bytes"
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
		return unknownName
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

// lineEndingSampleSize is how much of a file is read to decide its line ending.
// The terminator does not change halfway through a real file, so a sample
// settles it; reading the whole of a large file to count the rest would cost the
// save a second full read.
const lineEndingSampleSize = 1 << 20 // 1 MiB

// detectLineEnding reports the line terminator path already uses, so a save that
// overwrites it can write the same one.
//
// The rule is the majority of the terminators in the sample, and LF on a tie or
// on a file with no line ending at all. Majority rather than first-line-wins
// because the point is to leave rows the caller did not edit byte-identical: a
// file that is LF except for one stray CRLF stays LF, where following the first
// line would rewrite every other line in it.
//
// A file this package cannot read is reported as LF, which is what a save wrote
// before this existed. The detection is an improvement on the destination's
// behalf, so failing to detect must not fail the save.
func detectLineEnding(path string) LineEnding {
	reader, cleanup, err := NewCompressionFactory().CreateReaderForFile(path)
	if err != nil {
		return LineEndingLF
	}
	defer func() {
		_ = cleanup() //nolint:errcheck // Reading for detection only; a close failure cannot affect the answer
	}()

	sample, err := io.ReadAll(io.LimitReader(reader, lineEndingSampleSize))
	if err != nil && len(sample) == 0 {
		return LineEndingLF
	}
	return dominantLineEnding(sample)
}

// dominantLineEnding is detectLineEnding's rule, over bytes already in hand.
func dominantLineEnding(sample []byte) LineEnding {
	crlf := bytes.Count(sample, []byte("\r\n"))
	// Every "\r\n" also contains the "\n" counted here, so the lone ones are what
	// is left after removing them.
	lf := bytes.Count(sample, []byte("\n")) - crlf
	if crlf > lf {
		return LineEndingCRLF
	}
	return LineEndingLF
}
