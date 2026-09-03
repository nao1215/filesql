package reader

import (
	"errors"
	"testing"
)

// TestFormatIsText pins the rule both doors into this module read a source by.
// A text format's bytes are characters, so a leading byte-order mark belongs to
// the encoding and what follows has to be UTF-8; a container carries its own
// framing and is read as bytes, where a text decoder would refuse every file.
//
// Every format is listed rather than only the interesting ones, so a format
// added later has to be given an answer here rather than falling into whichever
// half the default happens to be.
func TestFormatIsText(t *testing.T) {
	t.Parallel()

	want := map[Format]bool{
		FormatCSV:     true,
		FormatTSV:     true,
		FormatLTSV:    true,
		FormatJSON:    true,
		FormatJSONL:   true,
		FormatParquet: false,
		FormatXLSX:    false,
	}

	for format, isText := range want {
		if got := format.IsText(); got != isText {
			t.Errorf("%s.IsText() = %v, want %v", format, got, isText)
		}
	}
	if len(want) != int(FormatJSONL)+1 {
		t.Errorf("the table covers %d formats and there are %d", len(want), int(FormatJSONL)+1)
	}
	if Format(-1).IsText() {
		t.Error("a format this package has no name for is not text")
	}
}

// TestEverySentinelIsAKindOfItsOwn holds the mapping from a read failure's kind
// to a sentinel to being deliberate for every kind. A kind added later falls to
// ErrParsing, which is a defensible answer and not necessarily the right one:
// this fails on it, so the right one is written down rather than inherited.
func TestEverySentinelIsAKindOfItsOwn(t *testing.T) {
	t.Parallel()

	want := map[Kind]error{
		KindParse:           ErrParsing,
		KindEmpty:           ErrEmptyData,
		KindInvalidData:     ErrInvalidData,
		KindDuplicateColumn: ErrDuplicateColumn,
		KindUnsupported:     ErrUnsupportedFormat,
	}
	for kind, sentinel := range want {
		if got := SentinelFor(kind); !errors.Is(got, sentinel) {
			t.Errorf("SentinelFor(%v) = %v, want %v", kind, got, sentinel)
		}
	}

	// A kind the table above does not name is one added since it was written,
	// and it needs an answer of its own rather than the general one.
	for kind := KindParse; kind <= KindUnsupported+1; kind++ {
		_, named := want[kind]
		if !named && kind <= KindUnsupported {
			t.Errorf("Kind(%d) is a kind of this package and this test does not say what it answers", kind)
		}
	}

	// Every sentinel is a different value, since a caller tells them apart.
	seen := map[error]Kind{}
	for kind, sentinel := range want {
		if first, taken := seen[sentinel]; taken {
			t.Errorf("Kind(%d) and Kind(%d) answer the same sentinel", first, kind)
		}
		seen[sentinel] = kind
	}
}

// TestAReadErrorCarriesItsSentinel holds an Error to answering for its own kind,
// which is what lets both doors into this reader classify one document the same
// way without a mapping of their own.
func TestAReadErrorCarriesItsSentinel(t *testing.T) {
	t.Parallel()

	cause := errors.New("the cause")
	for kind, sentinel := range map[Kind]error{
		KindParse:           ErrParsing,
		KindEmpty:           ErrEmptyData,
		KindInvalidData:     ErrInvalidData,
		KindDuplicateColumn: ErrDuplicateColumn,
		KindUnsupported:     ErrUnsupportedFormat,
	} {
		err := error(&Error{Kind: kind, Msg: "what went wrong", Err: cause})
		if !errors.Is(err, sentinel) {
			t.Errorf("an error of Kind(%d) does not carry %v", kind, sentinel)
		}
		if !errors.Is(err, cause) {
			t.Errorf("an error of Kind(%d) lost its cause", kind)
		}
		withoutCause := error(&Error{Kind: kind, Msg: "what went wrong"})
		if !errors.Is(withoutCause, sentinel) {
			t.Errorf("an error of Kind(%d) with no cause does not carry %v", kind, sentinel)
		}
	}
}
