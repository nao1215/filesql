package reader

import "testing"

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
