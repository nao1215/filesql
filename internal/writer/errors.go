package writer

// Kind is what a write failed on, in a form a caller can map onto its own
// sentinel. This package names no sentinel of its own: the three callers have
// three different ones for the same fault, and the wording each puts in front
// belongs to the caller rather than to the encoding.
type Kind int

const (
	// KindUnrepresentable is a value or a label the format has no way to hold.
	KindUnrepresentable Kind = iota
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
