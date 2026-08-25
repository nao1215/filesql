package parser

import "errors"

// ErrNilReader reports a Parse called with no input to read.
var ErrNilReader = errors.New("reader cannot be nil")

// ErrUnsupportedFileType reports a FileType this package does not parse.
var ErrUnsupportedFileType = errors.New("unsupported file type")

// ErrEmptyData reports input that holds no table: a file with nothing in it, a
// workbook with no sheet or whose sheet carries no header, a Parquet file with
// no schema, or LTSV whose lines are none of them records.
//
// Every wording the read side has for that condition matches this one sentinel,
// because a caller that wants to tell "there was nothing to load" from "the file
// is broken" cannot be asked to know which of them a given format says. Matching
// the wording is what a caller had to do before, which put a list of this
// package's messages inside a package that cannot see them.
var ErrEmptyData = errors.New("input holds no table")

// emptyInputError marks err as input holding no table without changing what it says.
//
// The sentinel is reachable through errors.Is while the message stays the read
// side's own, which is the part that tells a user which file was empty and how.
// Prefixing the sentinel instead would put "input holds no table" in front of
// "empty parquet file", which says the same thing twice. The other two
// sentinels need no wrapper: they are what Parse returns for their condition,
// so their messages are already the ones a caller reads.
type emptyInputError struct{ err error }

// Error renders the underlying message, unchanged.
func (e *emptyInputError) Error() string { return e.err.Error() }

// Unwrap returns both the sentinel and the cause, so errors.Is matches
// ErrEmptyData and whatever the read side wrapped underneath.
func (e *emptyInputError) Unwrap() []error { return []error{ErrEmptyData, e.err} }
