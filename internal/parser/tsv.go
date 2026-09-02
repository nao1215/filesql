package parser

import "errors"

// ErrTSVSyntax reports tab-separated input that does not describe a table: a
// record whose field count differs from the header's. It is the counterpart of
// ErrCSVSyntax, which a caller can already match for the same shape in CSV;
// without one, the two formats reported the same fault in two ways, and TSV
// reported it as no fault at all.
var ErrTSVSyntax = errors.New("invalid TSV syntax")

// ErrTSVUnrepresentable reports a value that tab-separated values cannot hold:
// one containing a tab, which is the delimiter, or a line break, which ends the
// record. There is no escape for either.
var ErrTSVUnrepresentable = errors.New("value cannot be represented in TSV")
