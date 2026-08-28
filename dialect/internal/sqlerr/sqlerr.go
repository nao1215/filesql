// Package sqlerr holds the sentinel errors the translation pipeline returns and
// the diagnostic that carries a source position with them. The parser, the
// lowering layer and the public dialect package all report through it.
package sqlerr

import (
	"errors"
	"fmt"
)

var (
	// ErrUnsupportedSyntax indicates a construct that is valid in the source
	// dialect and that SQLite cannot express, with or without a runtime helper.
	ErrUnsupportedSyntax = errors.New("dialect: syntax not supported on SQLite backend")
	// ErrUnsupportedFeature indicates a construct outside the SQL subset this
	// package implements. The construct may be perfectly ordinary SQL; what the
	// error says is that the parser does not model it, and that it will not be
	// forwarded to SQLite untranslated.
	ErrUnsupportedFeature = errors.New("dialect: SQL feature not implemented by this package")
	// ErrInvalidSyntax indicates the query could not be read: an unterminated
	// string, a stray parenthesis, an expression that stops in the middle.
	ErrInvalidSyntax = errors.New("dialect: invalid SQL syntax")
)

// DiagnosticError is an error with the place in the query it is about. It wraps one
// of the sentinels above, so errors.Is finds the kind and the message carries
// the line and column a person needs to find the construct.
type DiagnosticError struct {
	Kind    error
	Message string
	Line    int
	Column  int
}

// Error spells the diagnostic as "<kind>: <message> at line L, column C". The
// position is left out when it is not known, which is the case for a check that
// is about the query as a whole rather than about one construct in it.
func (d *DiagnosticError) Error() string {
	if d.Line <= 0 {
		return fmt.Sprintf("%s: %s", d.Kind, d.Message)
	}
	return fmt.Sprintf("%s: %s at line %d, column %d", d.Kind, d.Message, d.Line, d.Column)
}

// Unwrap reports the sentinel, so errors.Is(err, ErrUnsupportedSyntax) holds for
// a diagnostic of that kind.
func (d *DiagnosticError) Unwrap() error { return d.Kind }

// At builds a diagnostic of the given kind at a position. A line of zero means
// the position is not known.
func At(kind error, line, column int, format string, args ...any) error {
	return &DiagnosticError{Kind: kind, Message: fmt.Sprintf(format, args...), Line: line, Column: column}
}

// Unsupportedf reports a construct SQLite cannot express, without a position.
func Unsupportedf(format string, args ...any) error {
	return &DiagnosticError{Kind: ErrUnsupportedSyntax, Message: fmt.Sprintf(format, args...)}
}
