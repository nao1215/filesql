package prep

import (
	"net/url"
	"testing"

	"github.com/nao1215/filesql/internal/infer"
)

func TestTrimPreprocessor(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		input string
		want  string
	}{
		{"trim spaces", "  hello  ", "hello"},
		{"trim tabs", "\thello\t", "hello"},
		{"trim mixed", " \t hello \t ", "hello"},
		{"no trim needed", "hello", "hello"},
		{"empty string", "", ""},
		{"only whitespace", "   ", ""},
	}

	prep := newTrimPreprocessor()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := prep.Process(tt.input); got != tt.want {
				t.Errorf("Process() = %q, want %q", got, tt.want)
			}
		})
	}

	if prep.Name() != "trim" {
		t.Errorf("Name() = %q, want %q", prep.Name(), "trim")
	}
}

func TestLtrimPreprocessor(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		input string
		want  string
	}{
		{"ltrim spaces", "  hello  ", "hello  "},
		{"ltrim tabs", "\thello\t", "hello\t"},
		{"no trim needed", "hello", "hello"},
	}

	prep := newLtrimPreprocessor()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := prep.Process(tt.input); got != tt.want {
				t.Errorf("Process() = %q, want %q", got, tt.want)
			}
		})
	}

	if prep.Name() != "ltrim" {
		t.Errorf("Name() = %q, want %q", prep.Name(), "ltrim")
	}
}

func TestRtrimPreprocessor(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		input string
		want  string
	}{
		{"rtrim spaces", "  hello  ", "  hello"},
		{"rtrim tabs", "\thello\t", "\thello"},
		{"no trim needed", "hello", "hello"},
	}

	prep := newRtrimPreprocessor()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := prep.Process(tt.input); got != tt.want {
				t.Errorf("Process() = %q, want %q", got, tt.want)
			}
		})
	}

	if prep.Name() != "rtrim" {
		t.Errorf("Name() = %q, want %q", prep.Name(), "rtrim")
	}
}

func TestLowercasePreprocessor(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		input string
		want  string
	}{
		{"uppercase to lowercase", "HELLO", "hello"},
		{"mixed case", "HeLLo WoRLd", "hello world"},
		{"already lowercase", "hello", "hello"},
		{"empty string", "", ""},
	}

	prep := newLowercasePreprocessor()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := prep.Process(tt.input); got != tt.want {
				t.Errorf("Process() = %q, want %q", got, tt.want)
			}
		})
	}

	if prep.Name() != "lowercase" {
		t.Errorf("Name() = %q, want %q", prep.Name(), "lowercase")
	}
}

func TestUppercasePreprocessor(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		input string
		want  string
	}{
		{"lowercase to uppercase", "hello", "HELLO"},
		{"mixed case", "HeLLo WoRLd", "HELLO WORLD"},
		{"already uppercase", "HELLO", "HELLO"},
		{"empty string", "", ""},
	}

	prep := newUppercasePreprocessor()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := prep.Process(tt.input); got != tt.want {
				t.Errorf("Process() = %q, want %q", got, tt.want)
			}
		})
	}

	if prep.Name() != "uppercase" {
		t.Errorf("Name() = %q, want %q", prep.Name(), "uppercase")
	}
}

func TestDefaultPreprocessor(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		defaultValue string
		input        string
		want         string
	}{
		{"empty input uses default", "default", "", "default"},
		{"whitespace only uses default", "default", "   ", "default"},
		{"non-empty input unchanged", "default", "value", "value"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			prep := newDefaultPreprocessor(tt.defaultValue)
			if got := prep.Process(tt.input); got != tt.want {
				t.Errorf("Process() = %q, want %q", got, tt.want)
			}
		})
	}

	prep := newDefaultPreprocessor("test")
	if prep.Name() != "default" {
		t.Errorf("Name() = %q, want %q", prep.Name(), "default")
	}
}

func TestPreprocessors_Process(t *testing.T) {
	t.Parallel()

	// Test chaining: trim then lowercase
	preps := preprocessors{
		newTrimPreprocessor(),
		newLowercasePreprocessor(),
	}

	input := "  HELLO WORLD  "
	want := "hello world"

	if got := preps.Process(input); got != want {
		t.Errorf("Process() = %q, want %q", got, want)
	}
}

// =============================================================================
// String Transformation Preprocessors
// =============================================================================

func TestReplacePreprocessor(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		old   string
		new   string
		input string
		want  string
	}{
		{"replace single", "foo", "bar", "foo", "bar"},
		{"replace multiple", "a", "b", "aaa", "bbb"},
		{"no match", "x", "y", "hello", "hello"},
		{"empty old string", "", "x", "hello", "xhxexlxlxox"},
		{"replace with empty", "l", "", "hello", "heo"},
		{"empty input", "a", "b", "", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			prep := newReplacePreprocessor(tt.old, tt.new)
			if got := prep.Process(tt.input); got != tt.want {
				t.Errorf("Process() = %q, want %q", got, tt.want)
			}
		})
	}

	prep := newReplacePreprocessor("a", "b")
	if prep.Name() != "replace" {
		t.Errorf("Name() = %q, want %q", prep.Name(), "replace")
	}
}

func TestPrefixPreprocessor(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		prefix string
		input  string
		want   string
	}{
		{"add prefix", "pre_", "value", "pre_value"},
		{"empty prefix", "", "value", "value"},
		{"empty input", "pre_", "", "pre_"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			prep := newPrefixPreprocessor(tt.prefix)
			if got := prep.Process(tt.input); got != tt.want {
				t.Errorf("Process() = %q, want %q", got, tt.want)
			}
		})
	}

	prep := newPrefixPreprocessor("pre_")
	if prep.Name() != "prefix" {
		t.Errorf("Name() = %q, want %q", prep.Name(), "prefix")
	}
}

func TestSuffixPreprocessor(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		suffix string
		input  string
		want   string
	}{
		{"add suffix", "_suf", "value", "value_suf"},
		{"empty suffix", "", "value", "value"},
		{"empty input", "_suf", "", "_suf"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			prep := newSuffixPreprocessor(tt.suffix)
			if got := prep.Process(tt.input); got != tt.want {
				t.Errorf("Process() = %q, want %q", got, tt.want)
			}
		})
	}

	prep := newSuffixPreprocessor("_suf")
	if prep.Name() != "suffix" {
		t.Errorf("Name() = %q, want %q", prep.Name(), "suffix")
	}
}

func TestTruncatePreprocessor(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		maxLen int
		input  string
		want   string
	}{
		{"truncate longer", 5, "hello world", "hello"},
		{"exact length", 5, "hello", "hello"},
		{"shorter than max", 10, "hello", "hello"},
		{"empty input", 5, "", ""},
		{"truncate to 1", 1, "hello", "h"},
		{"unicode support", 3, "こんにちは", "こんに"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			prep := newTruncatePreprocessor(tt.maxLen)
			if got := prep.Process(tt.input); got != tt.want {
				t.Errorf("Process() = %q, want %q", got, tt.want)
			}
		})
	}

	prep := newTruncatePreprocessor(5)
	if prep.Name() != "truncate" {
		t.Errorf("Name() = %q, want %q", prep.Name(), "truncate")
	}
}

func TestStripHTMLPreprocessor(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		input string
		want  string
	}{
		{"simple tag", "<p>hello</p>", "hello"},
		{"nested tags", "<div><span>hello</span></div>", "hello"},
		{"self-closing tag", "hello<br/>world", "helloworld"},
		{"attributes", "<a href='test'>link</a>", "link"},
		{"no tags", "hello world", "hello world"},
		{"empty input", "", ""},
		{"mixed content", "Hello <b>bold</b> world", "Hello bold world"},
	}

	prep := newStripHTMLPreprocessor()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := prep.Process(tt.input); got != tt.want {
				t.Errorf("Process() = %q, want %q", got, tt.want)
			}
		})
	}

	if prep.Name() != "strip_html" {
		t.Errorf("Name() = %q, want %q", prep.Name(), "strip_html")
	}
}

func TestStripNewlinePreprocessor(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		input string
		want  string
	}{
		{"unix newline", "hello\nworld", "helloworld"},
		{"windows newline", "hello\r\nworld", "helloworld"},
		{"old mac newline", "hello\rworld", "helloworld"},
		{"multiple newlines", "a\nb\r\nc\rd", "abcd"},
		{"no newlines", "hello world", "hello world"},
		{"empty input", "", ""},
	}

	prep := newStripNewlinePreprocessor()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := prep.Process(tt.input); got != tt.want {
				t.Errorf("Process() = %q, want %q", got, tt.want)
			}
		})
	}

	if prep.Name() != "strip_newline" {
		t.Errorf("Name() = %q, want %q", prep.Name(), "strip_newline")
	}
}

func TestCollapseSpacePreprocessor(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		input string
		want  string
	}{
		{"multiple spaces", "hello    world", "hello world"},
		{"tabs and spaces", "hello\t  \tworld", "hello world"},
		{"mixed whitespace", "a  b\t\tc   d", "a b c d"},
		{"single spaces", "hello world", "hello world"},
		{"leading trailing", "  hello  ", " hello "},
		{"empty input", "", ""},
	}

	prep := newCollapseSpacePreprocessor()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := prep.Process(tt.input); got != tt.want {
				t.Errorf("Process() = %q, want %q", got, tt.want)
			}
		})
	}

	if prep.Name() != "collapse_space" {
		t.Errorf("Name() = %q, want %q", prep.Name(), "collapse_space")
	}
}

// =============================================================================
// Character Filtering Preprocessors
// =============================================================================

func TestRemoveDigitsPreprocessor(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		input string
		want  string
	}{
		{"remove digits", "abc123def", "abcdef"},
		{"all digits", "12345", ""},
		{"no digits", "hello", "hello"},
		{"empty input", "", ""},
		{"mixed", "a1b2c3", "abc"},
	}

	prep := newRemoveDigitsPreprocessor()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := prep.Process(tt.input); got != tt.want {
				t.Errorf("Process() = %q, want %q", got, tt.want)
			}
		})
	}

	if prep.Name() != "remove_digits" {
		t.Errorf("Name() = %q, want %q", prep.Name(), "remove_digits")
	}
}

func TestRemoveAlphaPreprocessor(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		input string
		want  string
	}{
		{"remove alpha", "abc123def", "123"},
		{"all alpha", "hello", ""},
		{"no alpha", "12345", "12345"},
		{"empty input", "", ""},
		{"mixed with spaces", "a 1 b 2", " 1  2"},
	}

	prep := newRemoveAlphaPreprocessor()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := prep.Process(tt.input); got != tt.want {
				t.Errorf("Process() = %q, want %q", got, tt.want)
			}
		})
	}

	if prep.Name() != "remove_alpha" {
		t.Errorf("Name() = %q, want %q", prep.Name(), "remove_alpha")
	}
}

func TestKeepDigitsPreprocessor(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		input string
		want  string
	}{
		{"keep digits", "abc123def", "123"},
		{"all digits", "12345", "12345"},
		{"no digits", "hello", ""},
		{"empty input", "", ""},
		{"phone number", "(123) 456-7890", "1234567890"},
	}

	prep := newKeepDigitsPreprocessor()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := prep.Process(tt.input); got != tt.want {
				t.Errorf("Process() = %q, want %q", got, tt.want)
			}
		})
	}

	if prep.Name() != "keep_digits" {
		t.Errorf("Name() = %q, want %q", prep.Name(), "keep_digits")
	}
}

func TestKeepAlphaPreprocessor(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		input string
		want  string
	}{
		{"keep alpha", "abc123def", "abcdef"},
		{"all alpha", "hello", "hello"},
		{"no alpha", "12345", ""},
		{"empty input", "", ""},
		{"mixed with spaces", "a 1 b 2", "ab"},
	}

	prep := newKeepAlphaPreprocessor()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := prep.Process(tt.input); got != tt.want {
				t.Errorf("Process() = %q, want %q", got, tt.want)
			}
		})
	}

	if prep.Name() != "keep_alpha" {
		t.Errorf("Name() = %q, want %q", prep.Name(), "keep_alpha")
	}
}

func TestTrimSetPreprocessor(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		cutset string
		input  string
		want   string
	}{
		{"trim brackets", "[]", "[hello]", "hello"},
		{"trim quotes", "\"", "\"hello\"", "hello"},
		{"trim multiple chars", "abc", "abchelloabc", "hello"},
		{"no match", "xyz", "hello", "hello"},
		{"empty input", "abc", "", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			prep := newTrimSetPreprocessor(tt.cutset)
			if got := prep.Process(tt.input); got != tt.want {
				t.Errorf("Process() = %q, want %q", got, tt.want)
			}
		})
	}

	prep := newTrimSetPreprocessor("[]")
	if prep.Name() != "trim_set" {
		t.Errorf("Name() = %q, want %q", prep.Name(), "trim_set")
	}
}

// =============================================================================
// Padding Preprocessors
// =============================================================================

func TestPadLeftPreprocessor(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		length  int
		padChar rune
		input   string
		want    string
	}{
		{"pad with zeros", 5, '0', "42", "00042"},
		{"already at length", 5, '0', "12345", "12345"},
		{"longer than length", 5, '0', "123456", "123456"},
		{"pad with spaces", 10, ' ', "hello", "     hello"},
		{"empty input", 3, '0', "", "000"},
		{"unicode pad char", 5, '*', "ab", "***ab"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			prep := newPadLeftPreprocessor(tt.length, tt.padChar)
			if got := prep.Process(tt.input); got != tt.want {
				t.Errorf("Process() = %q, want %q", got, tt.want)
			}
		})
	}

	prep := newPadLeftPreprocessor(5, '0')
	if prep.Name() != "pad_left" {
		t.Errorf("Name() = %q, want %q", prep.Name(), "pad_left")
	}
}

func TestPadRightPreprocessor(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		length  int
		padChar rune
		input   string
		want    string
	}{
		{"pad with zeros", 5, '0', "42", "42000"},
		{"already at length", 5, '0', "12345", "12345"},
		{"longer than length", 5, '0', "123456", "123456"},
		{"pad with spaces", 10, ' ', "hello", "hello     "},
		{"empty input", 3, '0', "", "000"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			prep := newPadRightPreprocessor(tt.length, tt.padChar)
			if got := prep.Process(tt.input); got != tt.want {
				t.Errorf("Process() = %q, want %q", got, tt.want)
			}
		})
	}

	prep := newPadRightPreprocessor(5, '0')
	if prep.Name() != "pad_right" {
		t.Errorf("Name() = %q, want %q", prep.Name(), "pad_right")
	}
}

// =============================================================================
// Advanced Preprocessors
// =============================================================================

func TestNormalizeUnicodePreprocessor(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		input string
		want  string
	}{
		{"already NFC", "hello", "hello"},
		{"decomposed e-acute", "e\u0301", "é"},
		{"japanese dakuten", "か\u3099", "が"},
		{"empty input", "", ""},
	}

	prep := newNormalizeUnicodePreprocessor()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := prep.Process(tt.input); got != tt.want {
				t.Errorf("Process() = %q, want %q", got, tt.want)
			}
		})
	}

	if prep.Name() != "normalize_unicode" {
		t.Errorf("Name() = %q, want %q", prep.Name(), "normalize_unicode")
	}
}

func TestNullifyPreprocessor(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		nullValue string
		input     string
		want      string
	}{
		{"nullify NA", "NA", "NA", ""},
		{"nullify NULL", "NULL", "NULL", ""},
		{"no match", "NA", "hello", "hello"},
		{"case sensitive", "na", "NA", "NA"},
		{"empty input", "NA", "", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			prep := newNullifyPreprocessor(tt.nullValue)
			if got := prep.Process(tt.input); got != tt.want {
				t.Errorf("Process() = %q, want %q", got, tt.want)
			}
		})
	}

	prep := newNullifyPreprocessor("NA")
	if prep.Name() != "nullify" {
		t.Errorf("Name() = %q, want %q", prep.Name(), "nullify")
	}
}

func TestCoercePreprocessor(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		targetType string
		input      string
		want       string
	}{
		// int coercion
		{"int from float", "int", "123.0", "123"},
		{"int from int", "int", "123", "123"},
		{"int from float with decimal", "int", "123.9", "123"},
		{"int invalid", "int", "abc", "abc"},

		// float coercion
		{"float from int", "float", "123", "123"},
		{"float from float", "float", "123.45", "123.45"},
		{"float invalid", "float", "abc", "abc"},

		// bool coercion
		{"bool true", "bool", "true", "true"},
		{"bool 1", "bool", "1", "true"},
		{"bool yes", "bool", "yes", "true"},
		{"bool on", "bool", "on", "true"},
		{"bool false", "bool", "false", "false"},
		{"bool 0", "bool", "0", "false"},
		{"bool no", "bool", "no", "false"},
		{"bool off", "bool", "off", "false"},
		{"bool TRUE uppercase", "bool", "TRUE", "true"},
		{"bool invalid", "bool", "maybe", "maybe"},

		// edge cases
		{"empty input", "int", "", ""},
		{"whitespace", "int", "  123  ", "123"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			prep := newCoercePreprocessor(tt.targetType)
			if got := prep.Process(tt.input); got != tt.want {
				t.Errorf("Process() = %q, want %q", got, tt.want)
			}
		})
	}

	prep := newCoercePreprocessor("int")
	if prep.Name() != "coerce" {
		t.Errorf("Name() = %q, want %q", prep.Name(), "coerce")
	}
}

func TestFixSchemePreprocessor(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		scheme string
		input  string
		want   string
	}{
		{"add https", "https", "example.com", "https://example.com"},
		{"upgrade http to https", "https", "http://example.com", "https://example.com"},
		{"upgrade uppercase http to https", "https", "HTTP://example.com", "https://example.com"},
		{"keep https", "https", "https://example.com", "https://example.com"},
		{"keep uppercase https", "https", "HTTPS://example.com", "HTTPS://example.com"},
		{"add http", "http", "example.com", "http://example.com"},
		{"keep http when scheme is http", "http", "http://example.com", "http://example.com"},
		{"preserve non-http scheme", "https", "ftp://example.com", "ftp://example.com"},
		{"empty input", "https", "", ""},
		{"with path", "https", "example.com/path", "https://example.com/path"},

		// A scheme written without an authority is still a scheme, so the
		// value already has one and nothing is added.
		{"preserve mailto", "https", "mailto:user@example.com", "mailto:user@example.com"},
		{"preserve uppercase mailto", "https", "MAILTO:user@example.com", "MAILTO:user@example.com"},
		{"preserve tel", "https", "tel:+81-3-1234-5678", "tel:+81-3-1234-5678"},
		{"preserve urn", "https", "urn:isbn:0451450523", "urn:isbn:0451450523"},
		{"preserve data", "https", "data:text/plain,hello", "data:text/plain,hello"},
		{"preserve magnet", "https", "magnet:?xt=urn:btih:abc", "magnet:?xt=urn:btih:abc"},

		// A protocol-relative reference has an authority but no scheme, so it
		// needs the scheme and the colon and not a second pair of slashes.
		{"protocol relative", "https", "//cdn.example.com/a.png", "https://cdn.example.com/a.png"},
		{"protocol relative with query", "https", "//cdn.example.com/a?b=1", "https://cdn.example.com/a?b=1"},
		{"protocol relative with fragment", "https", "//cdn.example.com/a#top", "https://cdn.example.com/a#top"},

		// A colon followed by a bare port separates a host from its port, so
		// the text before it is a host and still needs a scheme.
		{"host with port", "https", "example.com:8080", "https://example.com:8080"},
		{"host with port and path", "https", "example.com:8080/x", "https://example.com:8080/x"},
		{"localhost with port", "https", "localhost:3000", "https://localhost:3000"},
		{"host with port and fragment", "https", "example.com:8080#top", "https://example.com:8080#top"},

		// A colon that comes after the path is part of the path.
		{"colon in path", "https", "example.com/a:b", "https://example.com/a:b"},

		// A scheme starts with a letter and holds letters, digits, "+", "-"
		// and "."; text before a colon that breaks that is not one.
		{"leading digit is not a scheme", "https", "1abc:x", "https://1abc:x"},
		{"space is not in a scheme", "https", "a b:c", "https://a b:c"},
		{"underscore is not in a scheme", "https", "a_b:c", "https://a_b:c"},
		{"leading colon", "https", ":8080", "https://:8080"},
		{"scheme with digits and signs", "https", "a1+b-c.d:x", "a1+b-c.d:x"},

		// An http scheme is upgraded however it is written, and only under
		// fix_scheme=https.
		{"upgrade opaque http", "https", "http:opaque", "https:opaque"},
		{"upgrade uppercase opaque http", "https", "HTTP:opaque", "https:opaque"},
		{"keep opaque http under fix_scheme=http", "http", "http:opaque", "http:opaque"},
		{"keep opaque https", "https", "https:opaque", "https:opaque"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			prep := newFixSchemePreprocessor(tt.scheme)
			if got := prep.Process(tt.input); got != tt.want {
				t.Errorf("Process() = %q, want %q", got, tt.want)
			}
		})
	}

	prep := newFixSchemePreprocessor("https")
	if prep.Name() != "fix_scheme" {
		t.Errorf("Name() = %q, want %q", prep.Name(), "fix_scheme")
	}
}

// TestFixSchemePreprocessorAlwaysParses states the property the table above
// spells out case by case: whatever the tag returns is a URL with a scheme.
// Both of the values that came back unparseable were caught by this alone.
func TestFixSchemePreprocessorAlwaysParses(t *testing.T) {
	t.Parallel()

	inputs := []string{
		"example.com",
		"example.com/path",
		"example.com:8080/x",
		"//cdn.example.com/a.png",
		"http://example.com",
		"https://example.com",
		"ftp://example.com",
		"mailto:user@example.com",
		"tel:+81-3-1234-5678",
		"urn:isbn:0451450523",
		"data:text/plain,hello",
		"magnet:?xt=urn:btih:abc",
	}

	for _, scheme := range []string{"http", "https"} {
		prep := newFixSchemePreprocessor(scheme)
		for _, input := range inputs {
			got := prep.Process(input)
			u, err := url.Parse(got)
			if err != nil {
				t.Errorf("fix_scheme=%s on %q produced %q, which does not parse: %v", scheme, input, got, err)
				continue
			}
			if u.Scheme == "" {
				t.Errorf("fix_scheme=%s on %q produced %q, which has no scheme", scheme, input, got)
			}
		}
	}
}

func TestRegexReplacePreprocessor(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		pattern     string
		replacement string
		input       string
		want        string
	}{
		{"simple replace", "foo", "bar", "foo", "bar"},
		{"replace digits", `\d+`, "X", "abc123def456", "abcXdefX"},
		{"capture group", `(\w+)@(\w+)`, "$1 at $2", "user@domain", "user at domain"},
		{"no match", "xyz", "abc", "hello", "hello"},
		{"empty input", "abc", "xyz", "", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			prep := newRegexReplacePreprocessor(tt.pattern, tt.replacement)
			if prep == nil {
				t.Fatal("newRegexReplacePreprocessor returned nil")
			}
			if got := prep.Process(tt.input); got != tt.want {
				t.Errorf("Process() = %q, want %q", got, tt.want)
			}
		})
	}

	// Test invalid regex
	invalidPrep := newRegexReplacePreprocessor("[invalid", "replacement")
	if invalidPrep != nil {
		t.Error("expected nil for invalid regex pattern")
	}

	prep := newRegexReplacePreprocessor("test", "replace")
	if prep.Name() != "regex_replace" {
		t.Errorf("Name() = %q, want %q", prep.Name(), "regex_replace")
	}
}

// =============================================================================
// Parser Integration Tests
// =============================================================================

func TestParsePrepTag_NewPreprocessors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		tag     string
		wantLen int
		wantErr bool
	}{
		// String transformation preprocessors
		{"replace", "replace=foo:bar", 1, false},
		{"prefix", "prefix=pre_", 1, false},
		{"suffix", "suffix=_suf", 1, false},
		{"truncate", "truncate=10", 1, false},
		{"strip_html", "strip_html", 1, false},
		{"strip_newline", "strip_newline", 1, false},
		{"collapse_space", "collapse_space", 1, false},

		// Character filtering preprocessors
		{"remove_digits", "remove_digits", 1, false},
		{"remove_alpha", "remove_alpha", 1, false},
		{"keep_digits", "keep_digits", 1, false},
		{"keep_alpha", "keep_alpha", 1, false},
		{"trim_set", "trim_set=[]", 1, false},

		// Padding preprocessors
		{"pad_left", "pad_left=5:0", 1, false},
		{"pad_right", "pad_right=5:0", 1, false},
		{"pad_left default char", "pad_left=5", 1, false},

		// Advanced preprocessors
		{"normalize_unicode", "normalize_unicode", 1, false},
		{"nullify", "nullify=NA", 1, false},
		{"coerce int", "coerce=int", 1, false},
		{"coerce float", "coerce=float", 1, false},
		{"coerce bool", "coerce=bool", 1, false},
		{"fix_scheme", "fix_scheme=https", 1, false},
		{"regex_replace", "regex_replace=\\d+:X", 1, false},

		// Combinations
		{"multiple", "trim,lowercase,prefix=pre_", 3, false},

		// Invalid cases
		{"invalid truncate", "truncate=abc", 0, false},
		{"invalid coerce", "coerce=invalid", 0, false},
		{"unknown tag", "unknown_tag", 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			preps, err := parsePrepTag(tt.tag, false)
			if (err != nil) != tt.wantErr {
				t.Errorf("parsePrepTag() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && len(preps) != tt.wantLen {
				t.Errorf("parsePrepTag() got %d preprocessors, want %d", len(preps), tt.wantLen)
			}
		})
	}
}

// TestCoerceNormalizesTheSpellingAndNotTheValue pins what a coercion may
// rewrite. A coercion normalizes how a number is written; it does not decide
// that a string of digits is a number. The three spellings the loader keeps as
// text -- a zero-padded code, a literal past int64, and Go's own number syntax
// -- were being read as numbers here, so a postal code became a smaller number
// and a column the loader would have typed TEXT became an INTEGER one under the
// caller.
//
// The boolean half is the same rule from the other side: t and f are exactly
// what strconv.ParseBool accepts, which is what the boolean validator accepts
// and how a bool struct field is filled, and they were the spellings left
// alone while on, yes, off and no were normalized.
func TestCoerceNormalizesTheSpellingAndNotTheValue(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		target string
		value  string
		want   string
	}{
		// The spelling is normalized and the quantity is unchanged.
		{"int", "1e3", "1000"},
		{"int", "1.50", "1"},
		{"int", "+1", "1"},
		{"int", " 1 ", "1"},
		{"int", "-0", "0"},
		{"float", "1.50", "1.5"},
		{"float", "1e3", "1000"},
		{"float", "+1", "1"},
		// A leading zero is a code, not a number written badly.
		{"int", "0777", "0777"},
		{"int", "007", "007"},
		{"int", "00", "00"},
		{"float", "0777", "0777"},
		{"int", "0", "0"},
		// Go's number syntax is not the file's, and SQLite's affinity does not
		// convert any of it.
		{"int", "1_000", "1_000"},
		{"int", "0x10", "0x10"},
		{"int", "0b11", "0b11"},
		{"int", "0o17", "0o17"},
		{"float", "1_000", "1_000"},
		// A literal a float64 cannot hold exactly, and one it cannot hold at
		// all. Reading the first as a float and converting back answered a
		// different number, and the second answered the smallest integer there
		// is.
		{"int", "9223372036854775808", "9223372036854775808"},
		{"int", "1e400", "1e400"},
		{"float", "1e400", "1e400"},
		{"int", "abc", "abc"},
		// The booleans strconv.ParseBool accepts, in both cases, beside the
		// four words a spreadsheet writes.
		{"bool", "t", "true"},
		{"bool", "T", "true"},
		{"bool", "f", "false"},
		{"bool", "F", "false"},
		{"bool", "1", "true"},
		{"bool", "0", "false"},
		{"bool", "TRUE", "true"},
		{"bool", "on", "true"},
		{"bool", "off", "false"},
		{"bool", "yes", "true"},
		{"bool", "no", "false"},
		{"bool", "maybe", "maybe"},
	} {
		t.Run(tt.target+" "+tt.value, func(t *testing.T) {
			t.Parallel()

			if got := newCoercePreprocessor(tt.target).Process(tt.value); got != tt.want {
				t.Errorf("coerce=%s over %q = %q, want %q", tt.target, tt.value, got, tt.want)
			}
		})
	}
}

// TestCoerceAgreesWithTheLoader is the invariant that ties the two halves
// together: a value the loader keeps as text must not come out of a coercion as
// something the loader would type as a number. Without it the column type
// changes depending on whether the caller ran prep first.
func TestCoerceAgreesWithTheLoader(t *testing.T) {
	t.Parallel()

	for _, value := range []string{
		"0777", "007", "00", "1_000", "0x10", "0b11", "0o17",
		"9223372036854775808", "1e400", "1e-400",
	} {
		t.Run(value, func(t *testing.T) {
			t.Parallel()

			if !infer.MustStayText(value) && infer.IsFloat(value) {
				t.Skipf("%q is a number to the loader, so the coercion may rewrite it", value)
			}
			for _, target := range []string{"int", "float"} {
				got := newCoercePreprocessor(target).Process(value)
				if infer.IsInteger(got) || infer.IsFloat(got) {
					t.Errorf("coerce=%s turned %q into %q, which the loader types as a number", target, value, got)
				}
			}
		})
	}
}
