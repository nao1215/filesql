package writer

// Kind is what a write failed on, in a form a caller can map onto its own
// sentinel. This package names no sentinel of its own: its callers have
// different ones for the same fault, and the wording each puts in front
// belongs to the caller rather than to the encoding.
type Kind int

const (
	// KindUnrepresentable is a value or a label the format has no way to hold,
	// where another text format can: a tab in a TSV value, a colon in an LTSV
	// label.
	KindUnrepresentable Kind = iota
	// KindUnrepresentableAsText is one no text format can hold, because what
	// makes it unrepresentable is where it is written rather than which
	// delimiter the format uses.
	KindUnrepresentableAsText
	// KindUnrepresentableUnnamed is a column with no name, in a format whose
	// reader gives an unnamed column the name of its position. The formats that
	// carry it are the opposite set from the one KindUnrepresentableAsText
	// leaves, since what holds an empty name is a format that writes the name
	// beside each value rather than in a header.
	KindUnrepresentableUnnamed
	// KindNotUTF8 is text that is not valid UTF-8, which no text format holds:
	// the bytes go out and the file no longer reads back as the table it came
	// from. Only a format that carries bytes rather than text can say it.
	KindNotUTF8
	// KindUnwritableInEncoding is a character the destination's text encoding
	// has no way to write. Every text format holds it and every encoding that
	// takes all of Unicode writes it, so what refuses it is the encoding the
	// caller asked for rather than the format.
	KindUnwritableInEncoding
	// KindEncode is a record that is not what the format needs.
	KindEncode
)

// Error is a write that failed, and why.
type Error struct {
	// Kind is what went wrong.
	Kind Kind
	// Msg says what went wrong, without naming a sentinel: a caller prefixes
	// its own.
	Msg string
	// Err is the cause, when the failure came from underneath.
	Err error
}

// Error renders the message and the cause under it.
func (e *Error) Error() string {
	if e.Err == nil {
		return e.Msg
	}
	return e.Msg + ": " + e.Err.Error()
}

// Unwrap returns the cause, so errors.Is reaches whatever it carries.
func (e *Error) Unwrap() error { return e.Err }
