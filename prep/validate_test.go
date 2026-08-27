package prep

import (
	"strings"
	"testing"
)

func TestOmitemptyValidator(t *testing.T) {
	t.Parallel()

	t.Run("omitempty with email skips validation on empty value", func(t *testing.T) {
		t.Parallel()
		vs := validators{&omitemptyValidator{}, newEmailValidator()}
		tag, msg := vs.Validate("")
		if tag != "" || msg != "" {
			t.Errorf("expected empty value to pass with omitempty, got tag=%q msg=%q", tag, msg)
		}
	})

	t.Run("omitempty with email validates non-empty value", func(t *testing.T) {
		t.Parallel()
		vs := validators{&omitemptyValidator{}, newEmailValidator()}
		tag, msg := vs.Validate("invalid")
		if tag == "" || msg == "" {
			t.Error("expected validation error for invalid email with omitempty")
		}
	})

	t.Run("omitempty with email passes valid non-empty value", func(t *testing.T) {
		t.Parallel()
		vs := validators{&omitemptyValidator{}, newEmailValidator()}
		tag, msg := vs.Validate("user@example.com")
		if tag != "" || msg != "" {
			t.Errorf("expected valid email to pass with omitempty, got tag=%q msg=%q", tag, msg)
		}
	})

	t.Run("required before omitempty still catches empty value", func(t *testing.T) {
		t.Parallel()
		vs := validators{newRequiredValidator(), &omitemptyValidator{}, newEmailValidator()}
		tag, msg := vs.Validate("")
		if tag != requiredTagValue {
			t.Errorf("expected required to catch empty value before omitempty, got tag=%q", tag)
		}
		if msg == "" {
			t.Error("expected error message for required validation failure")
		}
	})
}

func TestOmitempty_Processor(t *testing.T) {
	t.Parallel()

	type OptionalEmail struct {
		Name  string `validate:"required"`
		Email string `validate:"omitempty,email"`
	}

	t.Run("omitempty passes when email column is empty", func(t *testing.T) {
		t.Parallel()
		csvData := "name,email\nJohn,\n"
		var records []OptionalEmail
		processor := NewProcessor(FileTypeCSV)
		_, result, err := processor.Process(strings.NewReader(csvData), &records)
		if err != nil {
			t.Fatalf("Process() error = %v", err)
		}
		if len(result.Errors) != 0 {
			t.Errorf("expected 0 errors with omitempty for empty email, got %d: %v", len(result.Errors), result.Errors)
		}
	})

	t.Run("omitempty validates non-empty email", func(t *testing.T) {
		t.Parallel()
		csvData := "name,email\nJohn,invalid\n"
		var records []OptionalEmail
		processor := NewProcessor(FileTypeCSV)
		_, result, err := processor.Process(strings.NewReader(csvData), &records)
		if err != nil {
			t.Fatalf("Process() error = %v", err)
		}
		if len(result.Errors) == 0 {
			t.Error("expected validation error for invalid email with omitempty")
		}
	})

	t.Run("omitempty passes valid non-empty email", func(t *testing.T) {
		t.Parallel()
		csvData := "name,email\nJohn,john@example.com\n"
		var records []OptionalEmail
		processor := NewProcessor(FileTypeCSV)
		_, result, err := processor.Process(strings.NewReader(csvData), &records)
		if err != nil {
			t.Fatalf("Process() error = %v", err)
		}
		if len(result.Errors) != 0 {
			t.Errorf("expected 0 errors for valid email, got %d: %v", len(result.Errors), result.Errors)
		}
	})
}

func TestRequiredValidator(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		input   string
		wantErr bool
	}{
		{"non-empty value passes", "hello", false},
		{"empty value fails", "", true},
		{"space is valid", " ", false}, // Note: trim should be applied before required
	}

	v := newRequiredValidator()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			msg := v.Validate(tt.input)
			hasErr := msg != ""
			if hasErr != tt.wantErr {
				t.Errorf("Validate(%q) error = %v, wantErr %v", tt.input, msg, tt.wantErr)
			}
		})
	}

	if v.Name() != "required" {
		t.Errorf("Name() = %q, want %q", v.Name(), "required")
	}
}

func TestBooleanValidator(t *testing.T) {
	t.Parallel()

	// The accepted set is exactly what strconv.ParseBool accepts, which is
	// also what setFieldValue uses to fill a bool struct field: the validator
	// and the converter must never disagree about one value.
	tests := []struct {
		input   string
		wantErr bool
	}{
		{"true", false},
		{"false", false},
		{"0", false},
		{"1", false},
		{"True", false},
		{"False", false},
		{"TRUE", false},
		{"FALSE", false},
		{"t", false},
		{"f", false},
		{"T", false},
		{"F", false},
		{"yes", true},
		{"no", true},
		{"2", true},
		{"", true},
	}

	v := newBooleanValidator()

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			t.Parallel()
			msg := v.Validate(tt.input)
			hasErr := msg != ""
			if hasErr != tt.wantErr {
				t.Errorf("Validate(%q) error = %v, wantErr %v", tt.input, msg, tt.wantErr)
			}
		})
	}

	if v.Name() != booleanTagValue {
		t.Errorf("Name() = %q, want %q", v.Name(), booleanTagValue)
	}
}

func TestAlphaValidator(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input   string
		wantErr bool
	}{
		{"hello", false},
		{"Hello", false},
		{"ABC", false},
		{"hello123", true},
		{"hello world", true},
		{"", false},
	}

	v := newAlphaValidator()

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			t.Parallel()
			msg := v.Validate(tt.input)
			hasErr := msg != ""
			if hasErr != tt.wantErr {
				t.Errorf("Validate(%q) error = %v, wantErr %v", tt.input, msg, tt.wantErr)
			}
		})
	}
}

func TestAlphaUnicodeValidator(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input   string
		wantErr bool
	}{
		{"hello", false},
		{"Привет", false},
		{"日本語", false},
		{"hello123", true},
		{"hello world", true},
		{"", false},
	}

	v := newAlphaUnicodeValidator()

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			t.Parallel()
			msg := v.Validate(tt.input)
			hasErr := msg != ""
			if hasErr != tt.wantErr {
				t.Errorf("Validate(%q) error = %v, wantErr %v", tt.input, msg, tt.wantErr)
			}
		})
	}
}

func TestAlphaSpaceValidator(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input   string
		wantErr bool
	}{
		{"hello world", false},
		{"Hello World", false},
		{"ABC", false},
		{"hello123", true},
		{"hello-world", true},
		{"", false},
	}

	v := newAlphaSpaceValidator()

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			t.Parallel()
			msg := v.Validate(tt.input)
			hasErr := msg != ""
			if hasErr != tt.wantErr {
				t.Errorf("Validate(%q) error = %v, wantErr %v", tt.input, msg, tt.wantErr)
			}
		})
	}
}

func TestAlphanumSpaceValidator(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input   string
		wantErr bool
	}{
		{"hello world", false},
		{"Hello World 123", false},
		{"ABC123", false},
		{"hello-world", true},
		{"日本語", true},
		{"", false},
	}

	v := newAlphanumSpaceValidator()

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			t.Parallel()
			msg := v.Validate(tt.input)
			hasErr := msg != ""
			if hasErr != tt.wantErr {
				t.Errorf("Validate(%q) error = %v, wantErr %v", tt.input, msg, tt.wantErr)
			}
		})
	}
}

func TestNumericValidator(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input   string
		wantErr bool
	}{
		{"123", false},
		{"0", false},
		{"-123", false},
		{"", false},
		// numeric follows the go-playground dialect: a signed decimal is valid.
		{"12.34", false},
		{"1.5", false},
		{"45.0", false},
		{"-1.5", false},
		{"+1", false},
		{"abc", true},
		{"1e3", true},
		{"1.2.3", true},
		{".5", true},
		{"5.", true},
	}

	v := newNumericValidator()

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			t.Parallel()
			msg := v.Validate(tt.input)
			hasErr := msg != ""
			if hasErr != tt.wantErr {
				t.Errorf("Validate(%q) error = %v, wantErr %v", tt.input, msg, tt.wantErr)
			}
		})
	}
}

func TestNumberValidator(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input   string
		wantErr bool
	}{
		// number follows the go-playground dialect: digits only, no sign,
		// no decimal point.
		{"123", false},
		{"0", false},
		{"00123", false}, // Leading zeros are allowed
		{"-123", true},
		{"+123", true},
		{"-1", true},
		{"+1", true},
		{"12.34", true},
		{"-12.34", true},
		{"45.0", true},
		{"", true},
		{".5", true},
		{"5.", true},
		{"abc", true},
		{"1e3", true},
		{"1E3", true},
		{"1.5e10", true},
		{"-2.5E-3", true},
		{"+0", true},
		{"-0", true},
		{"0.0", true},
	}

	v := newNumberValidator()

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			t.Parallel()
			msg := v.Validate(tt.input)
			hasErr := msg != ""
			if hasErr != tt.wantErr {
				t.Errorf("Validate(%q) error = %v, wantErr %v", tt.input, msg, tt.wantErr)
			}
		})
	}
}

func TestAlphanumericValidator(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input   string
		wantErr bool
	}{
		{"hello123", false},
		{"ABC123", false},
		{"hello world", true},
		{"hello-world", true},
		{"", false},
	}

	v := newAlphanumericValidator(alphanumTagValue)

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			t.Parallel()
			msg := v.Validate(tt.input)
			hasErr := msg != ""
			if hasErr != tt.wantErr {
				t.Errorf("Validate(%q) error = %v, wantErr %v", tt.input, msg, tt.wantErr)
			}
		})
	}
}

func TestAlphanumericUnicodeValidator(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input   string
		wantErr bool
	}{
		{"hello123", false},
		{"Привет123", false},
		{"日本語123", false},
		// go-playground's alphanumunicode is ^[\p{L}\p{N}]+$, and \p{N} covers
		// every Unicode number category, not just decimal digits.
		{"Ⅷ", false}, // U+2167 Nl (letter number)
		{"①", false}, // U+2460 No (other number)
		{"²", false}, // U+00B2 No (other number)
		{"hello world", true},
		{"hello-world", true},
		{"", false},
	}

	v := newAlphanumericUnicodeValidator()

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			t.Parallel()
			msg := v.Validate(tt.input)
			hasErr := msg != ""
			if hasErr != tt.wantErr {
				t.Errorf("Validate(%q) error = %v, wantErr %v", tt.input, msg, tt.wantErr)
			}
		})
	}
}

func TestEqualValidator(t *testing.T) {
	t.Parallel()

	v := newEqualValidator(10)

	tests := []struct {
		input   string
		wantErr bool
	}{
		{"10", false},
		{"10.0", false},
		{"5", true},
		{"15", true},
		{"abc", true},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			t.Parallel()
			msg := v.Validate(tt.input)
			hasErr := msg != ""
			if hasErr != tt.wantErr {
				t.Errorf("Validate(%q) error = %v, wantErr %v", tt.input, msg, tt.wantErr)
			}
		})
	}
}

func TestNotEqualValidator(t *testing.T) {
	t.Parallel()

	v := newNotEqualValidator(10)

	tests := []struct {
		input   string
		wantErr bool
	}{
		{"5", false},
		{"15", false},
		{"10", true},
		{"10.0", true},
		{"abc", true},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			t.Parallel()
			msg := v.Validate(tt.input)
			hasErr := msg != ""
			if hasErr != tt.wantErr {
				t.Errorf("Validate(%q) error = %v, wantErr %v", tt.input, msg, tt.wantErr)
			}
		})
	}
}

func TestGreaterThanValidator(t *testing.T) {
	t.Parallel()

	v := newGreaterThanValidator(10)

	tests := []struct {
		input   string
		wantErr bool
	}{
		{"15", false},
		{"11", false},
		{"10", true},
		{"5", true},
		{"abc", true},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			t.Parallel()
			msg := v.Validate(tt.input)
			hasErr := msg != ""
			if hasErr != tt.wantErr {
				t.Errorf("Validate(%q) error = %v, wantErr %v", tt.input, msg, tt.wantErr)
			}
		})
	}
}

func TestGreaterThanEqualValidator(t *testing.T) {
	t.Parallel()

	v := newGreaterThanEqualValidator(10)

	tests := []struct {
		input   string
		wantErr bool
	}{
		{"15", false},
		{"10", false},
		{"9", true},
		{"5", true},
		{"abc", true},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			t.Parallel()
			msg := v.Validate(tt.input)
			hasErr := msg != ""
			if hasErr != tt.wantErr {
				t.Errorf("Validate(%q) error = %v, wantErr %v", tt.input, msg, tt.wantErr)
			}
		})
	}
}

func TestLessThanValidator(t *testing.T) {
	t.Parallel()

	v := newLessThanValidator(10)

	tests := []struct {
		input   string
		wantErr bool
	}{
		{"5", false},
		{"9", false},
		{"10", true},
		{"15", true},
		{"abc", true},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			t.Parallel()
			msg := v.Validate(tt.input)
			hasErr := msg != ""
			if hasErr != tt.wantErr {
				t.Errorf("Validate(%q) error = %v, wantErr %v", tt.input, msg, tt.wantErr)
			}
		})
	}
}

func TestLessThanEqualValidator(t *testing.T) {
	t.Parallel()

	v := newLessThanEqualValidator(10)

	tests := []struct {
		input   string
		wantErr bool
	}{
		{"5", false},
		{"10", false},
		{"11", true},
		{"15", true},
		{"abc", true},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			t.Parallel()
			msg := v.Validate(tt.input)
			hasErr := msg != ""
			if hasErr != tt.wantErr {
				t.Errorf("Validate(%q) error = %v, wantErr %v", tt.input, msg, tt.wantErr)
			}
		})
	}
}

func TestMinValidator(t *testing.T) {
	t.Parallel()

	v := newMinValidator(10)

	tests := []struct {
		input   string
		wantErr bool
	}{
		{"15", false},
		{"10", false},
		{"9", true},
		{"abc", true},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			t.Parallel()
			msg := v.Validate(tt.input)
			hasErr := msg != ""
			if hasErr != tt.wantErr {
				t.Errorf("Validate(%q) error = %v, wantErr %v", tt.input, msg, tt.wantErr)
			}
		})
	}
}

func TestMaxValidator(t *testing.T) {
	t.Parallel()

	v := newMaxValidator(10)

	tests := []struct {
		input   string
		wantErr bool
	}{
		{"5", false},
		{"10", false},
		{"11", true},
		{"abc", true},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			t.Parallel()
			msg := v.Validate(tt.input)
			hasErr := msg != ""
			if hasErr != tt.wantErr {
				t.Errorf("Validate(%q) error = %v, wantErr %v", tt.input, msg, tt.wantErr)
			}
		})
	}
}

func TestLengthValidator(t *testing.T) {
	t.Parallel()

	v := newLengthValidator(5)

	tests := []struct {
		input   string
		wantErr bool
	}{
		{"hello", false},
		{"12345", false},
		{"hi", true},
		{"toolong", true},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			t.Parallel()
			msg := v.Validate(tt.input)
			hasErr := msg != ""
			if hasErr != tt.wantErr {
				t.Errorf("Validate(%q) error = %v, wantErr %v", tt.input, msg, tt.wantErr)
			}
		})
	}
}

func TestOneOfValidator(t *testing.T) {
	t.Parallel()

	v := newOneOfValidator([]string{"red", "green", "blue"})

	tests := []struct {
		input   string
		wantErr bool
	}{
		{"red", false},
		{"green", false},
		{"blue", false},
		{"yellow", true},
		{"", true},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			t.Parallel()
			msg := v.Validate(tt.input)
			hasErr := msg != ""
			if hasErr != tt.wantErr {
				t.Errorf("Validate(%q) error = %v, wantErr %v", tt.input, msg, tt.wantErr)
			}
		})
	}
}

func TestOneOfValidator_QuotedValues(t *testing.T) {
	t.Parallel()

	// The oneof parameter follows the go-playground dialect: a single-quoted
	// run is one allowed value with its spaces intact, and the quotes are not
	// part of the value. The validator is built through the tag parser so the
	// parameter splitting is under test, not newOneOfValidator directly.
	tests := []struct {
		name    string
		tag     string
		input   string
		wantErr bool
	}{
		{"quoted multi-word value passes", "oneof='red green' blue", "red green", false},
		{"bare value beside a quoted one passes", "oneof='red green' blue", "blue", false},
		{"a word from inside a quoted value fails", "oneof='red green' blue", "red", true},
		{"the trailing quoted word fails alone", "oneof='red green' blue", "green", true},
		{"the quote is not part of the value", "oneof='red green' blue", "'red", true},
		{"first of three quoted cities passes", "oneof='New York' 'Los Angeles' Boston", "New York", false},
		{"second of three quoted cities passes", "oneof='New York' 'Los Angeles' Boston", "Los Angeles", false},
		{"bare city passes", "oneof='New York' 'Los Angeles' Boston", "Boston", false},
		{"inner word of a quoted city fails", "oneof='New York' 'Los Angeles' Boston", "York", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			vals, _, err := parseValidateTag(tt.tag, false)
			if err != nil {
				t.Fatalf("parseValidateTag(%q) error = %v", tt.tag, err)
			}
			if len(vals) != 1 {
				t.Fatalf("parseValidateTag(%q) produced %d validators, want 1", tt.tag, len(vals))
			}
			msg := vals[0].Validate(tt.input)
			hasErr := msg != ""
			if hasErr != tt.wantErr {
				t.Errorf("Validate(%q) under %q error = %v, wantErr %v", tt.input, tt.tag, msg, tt.wantErr)
			}
		})
	}
}

func TestOneOfValidator_ErrorMessagePreservesOrder(t *testing.T) {
	t.Parallel()

	// Ensure error message preserves the original order of allowed values
	// This is important for user-facing error messages even after map-based optimization
	v := newOneOfValidator([]string{"apple", "banana", "cherry"})

	msg := v.Validate("invalid")
	expected := "value must be one of: apple, banana, cherry"
	if msg != expected {
		t.Errorf("Error message mismatch:\ngot:  %q\nwant: %q", msg, expected)
	}
}

func TestOneOfValidator_EdgeCases(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		allowed []string
		input   string
		wantErr bool
	}{
		{"single value - match", []string{"only"}, "only", false},
		{"single value - no match", []string{"only"}, "other", true},
		{"empty string in allowed - match", []string{"", "valid"}, "", false},
		{"empty string in allowed - other match", []string{"", "valid"}, "valid", false},
		{"whitespace value - match", []string{"  ", "trim"}, "  ", false},
		{"case sensitive - exact match", []string{"Yes", "No"}, "Yes", false},
		{"case sensitive - wrong case", []string{"Yes", "No"}, "yes", true},
		{"many values - first", []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"}, "a", false},
		{"many values - last", []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"}, "j", false},
		{"many values - middle", []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"}, "e", false},
		{"many values - not found", []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"}, "z", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			v := newOneOfValidator(tt.allowed)
			msg := v.Validate(tt.input)
			hasErr := msg != ""
			if hasErr != tt.wantErr {
				t.Errorf("Validate(%q) with allowed=%v: error = %v, wantErr %v", tt.input, tt.allowed, msg, tt.wantErr)
			}
		})
	}
}

func TestLowercaseValidator(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input   string
		wantErr bool
	}{
		{"hello", false},
		{"hello123", false},
		{"Hello", true},
		{"HELLO", true},
		{"", false},
	}

	v := newLowercaseValidator()

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			t.Parallel()
			msg := v.Validate(tt.input)
			hasErr := msg != ""
			if hasErr != tt.wantErr {
				t.Errorf("Validate(%q) error = %v, wantErr %v", tt.input, msg, tt.wantErr)
			}
		})
	}
}

func TestUppercaseValidator(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input   string
		wantErr bool
	}{
		{"HELLO", false},
		{"HELLO123", false},
		{"Hello", true},
		{"hello", true},
		{"", false},
	}

	v := newUppercaseValidator()

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			t.Parallel()
			msg := v.Validate(tt.input)
			hasErr := msg != ""
			if hasErr != tt.wantErr {
				t.Errorf("Validate(%q) error = %v, wantErr %v", tt.input, msg, tt.wantErr)
			}
		})
	}
}

func TestASCIIValidator(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input   string
		wantErr bool
	}{
		{"hello", false},
		{"Hello123!@#", false},
		{"日本語", true},
		{"héllo", true},
		{"", false},
	}

	v := newASCIIValidator()

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			t.Parallel()
			msg := v.Validate(tt.input)
			hasErr := msg != ""
			if hasErr != tt.wantErr {
				t.Errorf("Validate(%q) error = %v, wantErr %v", tt.input, msg, tt.wantErr)
			}
		})
	}
}

func TestPrintASCIIValidator(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input   string
		wantErr bool
	}{
		{"hello", false},
		{"Hello 123!@#", false},
		{"\t", true},
		{"\n", true},
		{"", false},
	}

	v := newPrintASCIIValidator()

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			t.Parallel()
			msg := v.Validate(tt.input)
			hasErr := msg != ""
			if hasErr != tt.wantErr {
				t.Errorf("Validate(%q) error = %v, wantErr %v", tt.input, msg, tt.wantErr)
			}
		})
	}
}

func TestEmailValidator(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input   string
		wantErr bool
	}{
		{"user@example.com", false},
		{"user.name@example.co.jp", false},
		{"user+tag@example.com", false},
		// The dialect admits an internationalized address on either side of
		// the @, and the punycode spelling of a domain must agree with the
		// Unicode one.
		{"user@日本.jp", false},
		{"user@xn--wgv71a.jp", false},
		{"山田@example.com", false},
		{"user@例え.テスト", false},
		// The dialect admits the RFC 5322 atext specials in the local part.
		{"o'brien@example.com", false},
		{"a!b@example.com", false},
		{"a/b@example.com", false},
		{"a=b@example.com", false},
		{"invalid", true},
		{"@example.com", true},
		{"user@", true},
		{"", true},
	}

	v := newEmailValidator()

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			t.Parallel()
			msg := v.Validate(tt.input)
			hasErr := msg != ""
			if hasErr != tt.wantErr {
				t.Errorf("Validate(%q) error = %v, wantErr %v", tt.input, msg, tt.wantErr)
			}
		})
	}
}

func TestEmailValidator_BoundaryConditions(t *testing.T) {
	t.Parallel()

	// These tests document the current regex behavior.
	// If implementation changes (e.g., hand-written parser), these tests ensure consistency.
	tests := []struct {
		name    string
		input   string
		wantErr bool
	}{
		// Valid emails
		{"simple email", "a@b.co", false},
		{"numbers in local part", "user123@example.com", false},
		{"hyphen in domain", "user@my-domain.com", false},
		{"underscore in local", "user_name@example.com", false},
		{"percent in local", "user%tag@example.com", false},
		{"long local part", "abcdefghijklmnopqrstuvwxyz@example.com", false},
		{"subdomain", "user@mail.example.com", false},
		{"multiple subdomains", "user@a.b.c.d.example.com", false},

		// Invalid: structural issues
		{"missing @", "userexample.com", true},
		{"multiple @", "user@@example.com", true},
		{"@ at start", "@example.com", true},
		{"@ at end", "user@", true},
		{"empty local part", "@example.com", true},
		{"empty domain", "user@", true},
		{"space in local", "user name@example.com", true},
		{"space in domain", "user@exam ple.com", true},

		// Invalid: dots - trailing dot in domain
		{"trailing dot in domain", "user@example.com.", true},

		// Invalid: TLD issues
		{"single char TLD", "user@example.a", false},
		{"quoted local part", `"a b"@example.com`, false},
		{"numeric TLD", "user@example.123", true},
		{"local part is only Japanese", "山田@example.com", false},
		{"domain is only Japanese", "user@日本.jp", false},
		{"hyphen at start of domain label", "user@-example.com", true},
		{"emoji in local part", "\U0001F600@example.com", true},

		// Invalid: dot placement
		{"leading dot in local", ".user@example.com", true},
		{"trailing dot in local", "user.@example.com", true},
		{"consecutive dots in local", "user..name@example.com", true},
		{"leading dot in domain", "user@.example.com", true},
		{"only dots in local", "...@example.com", true},
	}

	v := newEmailValidator()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			msg := v.Validate(tt.input)
			hasErr := msg != ""
			if hasErr != tt.wantErr {
				t.Errorf("Validate(%q) error = %v, wantErr %v", tt.input, msg, tt.wantErr)
			}
		})
	}
}

func TestURIValidator(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input   string
		wantErr bool
	}{
		{"http://example.com", false},
		{"https://example.com/path", false},
		{"ftp://example.com", false},
		{"http://example.com#fragment", false},
		{"http://example.com#frag ment", true},
		{"", true},
		{"invalid", true},
		// A URI needs a scheme, so a relative reference is not one. The
		// dialect accepts these; prep's doc.go records the difference.
		{"/a/b", true},
		{"//example.com", true},
	}

	v := newURIValidator()

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			t.Parallel()
			msg := v.Validate(tt.input)
			hasErr := msg != ""
			if hasErr != tt.wantErr {
				t.Errorf("Validate(%q) error = %v, wantErr %v", tt.input, msg, tt.wantErr)
			}
		})
	}
}

func TestURLValidator(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input   string
		wantErr bool
	}{
		{"http://example.com", false},
		{"https://example.com/path", false},
		{"file:///path/to/file", false},
		{"", true},
		{"invalid", true},
	}

	v := newURLValidator()

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			t.Parallel()
			msg := v.Validate(tt.input)
			hasErr := msg != ""
			if hasErr != tt.wantErr {
				t.Errorf("Validate(%q) error = %v, wantErr %v", tt.input, msg, tt.wantErr)
			}
		})
	}
}

func TestHTTPURLValidator(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input   string
		wantErr bool
	}{
		{"http://example.com", false},
		{"https://example.com", false},
		{"HTTP://EXAMPLE.COM", false},
		{"ftp://example.com", true},
		{"", true},
	}

	v := newHTTPURLValidator()

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			t.Parallel()
			msg := v.Validate(tt.input)
			hasErr := msg != ""
			if hasErr != tt.wantErr {
				t.Errorf("Validate(%q) error = %v, wantErr %v", tt.input, msg, tt.wantErr)
			}
		})
	}
}

func TestHTTPSURLValidator(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input   string
		wantErr bool
	}{
		{"https://example.com", false},
		{"HTTPS://EXAMPLE.COM", false},
		{"http://example.com", true},
		{"ftp://example.com", true},
		{"", true},
	}

	v := newHTTPSURLValidator()

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			t.Parallel()
			msg := v.Validate(tt.input)
			hasErr := msg != ""
			if hasErr != tt.wantErr {
				t.Errorf("Validate(%q) error = %v, wantErr %v", tt.input, msg, tt.wantErr)
			}
		})
	}
}

func TestURLEncodedValidator(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input   string
		wantErr bool
	}{
		{"hello%20world", false},
		{"hello", false},
		{"hello%2F", false},
		{"hello%ZZ", true},
		{"hello%", true},
	}

	v := newURLEncodedValidator()

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			t.Parallel()
			msg := v.Validate(tt.input)
			hasErr := msg != ""
			if hasErr != tt.wantErr {
				t.Errorf("Validate(%q) error = %v, wantErr %v", tt.input, msg, tt.wantErr)
			}
		})
	}
}

func TestDataURIValidator(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		input   string
		wantErr bool
	}{
		{"valid text data URI", "data:text/plain;base64,SGVsbG8=", false},
		{"valid image data URI", "data:image/png;base64,iVBORw0KGgo=", false},
		{"charset parameter", "data:text/plain;charset=utf-8;base64,aGVsbG8=", false},
		{"plus in subtype with parameter", "data:image/svg+xml;charset=UTF-8;base64,PHN2Zz48L3N2Zz4=", false},
		{"two parameters", "data:text/plain;charset=utf-8;foo=bar;base64,aGVsbG8=", false},
		{"omitted media type", "data:;base64,aGVsbG8=", false},
		{"omitted media type with parameter", "data:;charset=utf-8;base64,aGVsbG8=", false},
		{"missing base64 encoding", "data:text/plain,hello", true},
		{"empty payload", "data:text/plain;base64,", true},
		{"not a data URI scheme", "notdata:text/plain;base64,aGVsbG8=", true},
		{"invalid string", "invalid", true},
		{"empty string", "", true},
		// Passes regex but fails base64 decode due to missing padding
		{"regex match but invalid base64 padding", "data:text/plain;base64,SGVsbG8", true},
	}

	v := newDataURIValidator()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			msg := v.Validate(tt.input)
			hasErr := msg != ""
			if hasErr != tt.wantErr {
				t.Errorf("Validate(%q) error = %v, wantErr %v", tt.input, msg, tt.wantErr)
			}
		})
	}
}

func TestIPAddrValidator(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input   string
		wantErr bool
	}{
		{"192.168.1.1", false},
		{"10.0.0.1", false},
		{"::1", false},
		{"2001:db8::1", false},
		{"invalid", true},
		{"", true},
	}

	v := newIPAddrValidator(ipAddrTagValue)

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			t.Parallel()
			msg := v.Validate(tt.input)
			hasErr := msg != ""
			if hasErr != tt.wantErr {
				t.Errorf("Validate(%q) error = %v, wantErr %v", tt.input, msg, tt.wantErr)
			}
		})
	}
}

func TestIP4AddrValidator(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input   string
		wantErr bool
	}{
		{"192.168.1.1", false},
		{"10.0.0.1", false},
		{"::1", true},
		{"2001:db8::1", true},
		{"invalid", true},
		{"", true},
	}

	v := newIP4AddrValidator(ip4AddrTagValue)

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			t.Parallel()
			msg := v.Validate(tt.input)
			hasErr := msg != ""
			if hasErr != tt.wantErr {
				t.Errorf("Validate(%q) error = %v, wantErr %v", tt.input, msg, tt.wantErr)
			}
		})
	}
}

func TestIP6AddrValidator(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input   string
		wantErr bool
	}{
		{"::1", false},
		{"2001:db8::1", false},
		{"192.168.1.1", true},
		{"invalid", true},
		{"", true},
	}

	v := newIP6AddrValidator(ip6AddrTagValue)

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			t.Parallel()
			msg := v.Validate(tt.input)
			hasErr := msg != ""
			if hasErr != tt.wantErr {
				t.Errorf("Validate(%q) error = %v, wantErr %v", tt.input, msg, tt.wantErr)
			}
		})
	}
}

func TestCIDRValidator(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input   string
		wantErr bool
	}{
		{"192.168.1.0/24", false},
		{"10.0.0.0/8", false},
		{"2001:db8::/32", false},
		// The general cidr tag imposes no network-address rule (unlike cidrv4),
		// so an address with host bits set is still accepted.
		{"192.168.0.1/24", false},
		{"192.168.1.1", true},
		{"invalid", true},
		{"", true},
	}

	v := newCIDRValidator()

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			t.Parallel()
			msg := v.Validate(tt.input)
			hasErr := msg != ""
			if hasErr != tt.wantErr {
				t.Errorf("Validate(%q) error = %v, wantErr %v", tt.input, msg, tt.wantErr)
			}
		})
	}
}

func TestCIDRv4Validator(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input   string
		wantErr bool
	}{
		{"192.168.1.0/24", false},
		{"10.0.0.0/8", false},
		{"0.0.0.0/0", false},
		// go-playground's cidrv4 requires the address to equal its network
		// address, so an address with host bits set is rejected.
		{"192.168.0.1/24", true},
		{"10.0.0.5/8", true},
		{"2001:db8::/32", true},
		{"", true},
	}

	v := newCIDRv4Validator()

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			t.Parallel()
			msg := v.Validate(tt.input)
			hasErr := msg != ""
			if hasErr != tt.wantErr {
				t.Errorf("Validate(%q) error = %v, wantErr %v", tt.input, msg, tt.wantErr)
			}
		})
	}
}

func TestCIDRv6Validator(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input   string
		wantErr bool
	}{
		{"2001:db8::/32", false},
		{"::1/128", false},
		{"192.168.1.0/24", true},
		{"", true},
	}

	v := newCIDRv6Validator()

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			t.Parallel()
			msg := v.Validate(tt.input)
			hasErr := msg != ""
			if hasErr != tt.wantErr {
				t.Errorf("Validate(%q) error = %v, wantErr %v", tt.input, msg, tt.wantErr)
			}
		})
	}
}

func TestUUIDValidator(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input   string
		wantErr bool
	}{
		{"123e4567-e89b-12d3-a456-426614174000", false},
		{"550e8400-e29b-41d4-a716-446655440000", false},
		{"invalid", true},
		{"123e4567-e89b-12d3-a456", true},
		{"", true},
	}

	v := newUUIDValidator()

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			t.Parallel()
			msg := v.Validate(tt.input)
			hasErr := msg != ""
			if hasErr != tt.wantErr {
				t.Errorf("Validate(%q) error = %v, wantErr %v", tt.input, msg, tt.wantErr)
			}
		})
	}
}

func TestFQDNValidator(t *testing.T) {
	t.Parallel()

	label63 := strings.Repeat("a", 63)
	// 253 bytes without the root dot is the longest name there is, and the
	// dot must not count towards it.
	name253 := label63 + "." + label63 + "." + label63 + "." + strings.Repeat("a", 61)
	name254 := label63 + "." + label63 + "." + label63 + "." + strings.Repeat("a", 62)

	tests := []struct {
		input   string
		wantErr bool
	}{
		{name253, false},
		{name253 + ".", false},
		{name254, true},
		{"example.com", false},
		{"sub.example.com", false},
		{"host.a1", false},
		// The trailing dot is what makes a name fully qualified, so it is
		// accepted and does not change the verdict.
		{"example.com.", false},
		{"sub.example.com.", false},
		{"example", true},
		{".example.com", true},
		{"example.com..", true},
		{".", true},
		// The dialect accepts a label ending in a hyphen; this package does
		// not, which prep's doc.go records as deliberate.
		{"a-.com", true},
		// go-playground requires a non-numeric top-level domain, so an
		// all-numeric dotted string (an IPv4 address, or a bare numeric TLD) is
		// not an FQDN.
		{"127.0.0.1", true},
		{"1.2.3", true},
		{"45.0", true},
		{"256.256.256.256", true},
		{"host.123", true},
		{"", true},
	}

	v := newFQDNValidator()

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			t.Parallel()
			msg := v.Validate(tt.input)
			hasErr := msg != ""
			if hasErr != tt.wantErr {
				t.Errorf("Validate(%q) error = %v, wantErr %v", tt.input, msg, tt.wantErr)
			}
		})
	}
}

func TestHostnameValidator(t *testing.T) {
	t.Parallel()

	// Generate a label exactly 63 chars (max allowed)
	label63 := strings.Repeat("a", 63)
	// Generate a label of 64 chars (too long)
	label64 := strings.Repeat("a", 64)
	// Generate a hostname exactly 253 chars total (max allowed)
	// 253 = 63 + 1 + 63 + 1 + 63 + 1 + 61 = 253 (4 labels with dots)
	hostname253 := label63 + "." + label63 + "." + label63 + "." + strings.Repeat("a", 61)
	// Generate a hostname of 254 chars (too long)
	hostname254 := label63 + "." + label63 + "." + label63 + "." + strings.Repeat("a", 62)

	tests := []struct {
		name    string
		input   string
		wantErr bool
	}{
		{"simple hostname", "example", false},
		{"capitalized hostname", "Example", false},
		{"hyphenated hostname", "example-host", false},
		{"starts with digit", "1example", true},
		{"starts with hyphen", "-example", true},
		{"empty string", "", true},
		{"label exactly 63 chars", label63, false},
		{"label 64 chars too long", label64, true},
		{"hostname exactly 253 chars", hostname253, false},
		{"hostname 254 chars too long", hostname254, true},
		{"leading dot", ".example", true},
		{"trailing dot", "example.", true},
	}

	v := newHostnameValidator()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			msg := v.Validate(tt.input)
			hasErr := msg != ""
			if hasErr != tt.wantErr {
				t.Errorf("Validate(%q) error = %v, wantErr %v", tt.input, msg, tt.wantErr)
			}
		})
	}
}

func TestHostnameRFC1123Validator(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input   string
		wantErr bool
	}{
		{"example", false},
		{"1example", false},
		{"example-host", false},
		{"-example", true},
		{"", true},
	}

	v := newHostnameRFC1123Validator()

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			t.Parallel()
			msg := v.Validate(tt.input)
			hasErr := msg != ""
			if hasErr != tt.wantErr {
				t.Errorf("Validate(%q) error = %v, wantErr %v", tt.input, msg, tt.wantErr)
			}
		})
	}
}

func TestHostnamePortValidator(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input   string
		wantErr bool
	}{
		{"localhost:8080", false},
		{"example.com:443", false},
		{"192.168.1.1:80", false},
		{"[::1]:8080", false},
		{"localhost", true},
		{"localhost:0", true},
		{"localhost:99999", true},
		{"", true},
		// A port with no host names nothing. The dialect accepts this;
		// prep's doc.go records the difference.
		{":80", true},
	}

	v := newHostnamePortValidator()

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			t.Parallel()
			msg := v.Validate(tt.input)
			hasErr := msg != ""
			if hasErr != tt.wantErr {
				t.Errorf("Validate(%q) error = %v, wantErr %v", tt.input, msg, tt.wantErr)
			}
		})
	}
}

func TestStartsWithValidator(t *testing.T) {
	t.Parallel()

	v := newStartsWithValidator("hello")

	tests := []struct {
		input   string
		wantErr bool
	}{
		{"hello world", false},
		{"hello", false},
		{"world hello", true},
		{"", true},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			t.Parallel()
			msg := v.Validate(tt.input)
			hasErr := msg != ""
			if hasErr != tt.wantErr {
				t.Errorf("Validate(%q) error = %v, wantErr %v", tt.input, msg, tt.wantErr)
			}
		})
	}
}

func TestStartsNotWithValidator(t *testing.T) {
	t.Parallel()

	v := newStartsNotWithValidator("hello")

	tests := []struct {
		input   string
		wantErr bool
	}{
		{"world hello", false},
		{"goodbye", false},
		{"hello world", true},
		{"hello", true},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			t.Parallel()
			msg := v.Validate(tt.input)
			hasErr := msg != ""
			if hasErr != tt.wantErr {
				t.Errorf("Validate(%q) error = %v, wantErr %v", tt.input, msg, tt.wantErr)
			}
		})
	}
}

func TestEndsWithValidator(t *testing.T) {
	t.Parallel()

	v := newEndsWithValidator("world")

	tests := []struct {
		input   string
		wantErr bool
	}{
		{"hello world", false},
		{"world", false},
		{"world hello", true},
		{"", true},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			t.Parallel()
			msg := v.Validate(tt.input)
			hasErr := msg != ""
			if hasErr != tt.wantErr {
				t.Errorf("Validate(%q) error = %v, wantErr %v", tt.input, msg, tt.wantErr)
			}
		})
	}
}

func TestEndsNotWithValidator(t *testing.T) {
	t.Parallel()

	v := newEndsNotWithValidator("world")

	tests := []struct {
		input   string
		wantErr bool
	}{
		{"world hello", false},
		{"goodbye", false},
		{"hello world", true},
		{"world", true},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			t.Parallel()
			msg := v.Validate(tt.input)
			hasErr := msg != ""
			if hasErr != tt.wantErr {
				t.Errorf("Validate(%q) error = %v, wantErr %v", tt.input, msg, tt.wantErr)
			}
		})
	}
}

func TestContainsValidator(t *testing.T) {
	t.Parallel()

	v := newContainsValidator("world")

	tests := []struct {
		input   string
		wantErr bool
	}{
		{"hello world", false},
		{"world", false},
		{"hello", true},
		{"", true},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			t.Parallel()
			msg := v.Validate(tt.input)
			hasErr := msg != ""
			if hasErr != tt.wantErr {
				t.Errorf("Validate(%q) error = %v, wantErr %v", tt.input, msg, tt.wantErr)
			}
		})
	}
}

func TestContainsAnyValidator(t *testing.T) {
	t.Parallel()

	v := newContainsAnyValidator("abc")

	tests := []struct {
		name    string
		input   string
		wantErr bool
	}{
		{"contains 'a' from abc", "apple", false},
		{"contains 'b' from abc", "banana", false},
		{"contains 'c' from abc", "cat", false},
		{"contains multiple chars from abc", "abc", false},
		{"no matching character", "xyz", true},
		{"empty value returns error", "", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			msg := v.Validate(tt.input)
			hasErr := msg != ""
			if hasErr != tt.wantErr {
				t.Errorf("Validate(%q) error = %v, wantErr %v", tt.input, msg, tt.wantErr)
			}
		})
	}

	t.Run("empty chars returns error for non-empty value", func(t *testing.T) {
		t.Parallel()
		emptyV := newContainsAnyValidator("")
		if msg := emptyV.Validate("hello"); msg == "" {
			t.Error("expected error for empty chars, got none")
		}
	})
}

func TestContainsRuneValidator(t *testing.T) {
	t.Parallel()

	v := newContainsRuneValidator('@')

	tests := []struct {
		input   string
		wantErr bool
	}{
		{"user@example.com", false},
		{"@", false},
		{"hello", true},
		{"", true},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			t.Parallel()
			msg := v.Validate(tt.input)
			hasErr := msg != ""
			if hasErr != tt.wantErr {
				t.Errorf("Validate(%q) error = %v, wantErr %v", tt.input, msg, tt.wantErr)
			}
		})
	}
}

func TestExcludesValidator(t *testing.T) {
	t.Parallel()

	v := newExcludesValidator("bad")

	tests := []struct {
		input   string
		wantErr bool
	}{
		{"good", false},
		{"", false},
		{"bad word", true},
		{"bad", true},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			t.Parallel()
			msg := v.Validate(tt.input)
			hasErr := msg != ""
			if hasErr != tt.wantErr {
				t.Errorf("Validate(%q) error = %v, wantErr %v", tt.input, msg, tt.wantErr)
			}
		})
	}
}

func TestExcludesAllValidator(t *testing.T) {
	t.Parallel()

	v := newExcludesAllValidator("!@#")

	tests := []struct {
		input   string
		wantErr bool
	}{
		{"hello", false},
		{"", false},
		{"hello!", true},
		{"user@example.com", true},
		{"#tag", true},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			t.Parallel()
			msg := v.Validate(tt.input)
			hasErr := msg != ""
			if hasErr != tt.wantErr {
				t.Errorf("Validate(%q) error = %v, wantErr %v", tt.input, msg, tt.wantErr)
			}
		})
	}
}

func TestExcludesRuneValidator(t *testing.T) {
	t.Parallel()

	v := newExcludesRuneValidator('@')

	tests := []struct {
		input   string
		wantErr bool
	}{
		{"hello", false},
		{"", false},
		{"user@example.com", true},
		{"@", true},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			t.Parallel()
			msg := v.Validate(tt.input)
			hasErr := msg != ""
			if hasErr != tt.wantErr {
				t.Errorf("Validate(%q) error = %v, wantErr %v", tt.input, msg, tt.wantErr)
			}
		})
	}
}

func TestMultibyteValidator(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input   string
		wantErr bool
	}{
		{"日本語", false},
		{"hello日本語", false},
		{"héllo", false},
		{"hello", true},
		{"", true},
	}

	v := newMultibyteValidator()

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			t.Parallel()
			msg := v.Validate(tt.input)
			hasErr := msg != ""
			if hasErr != tt.wantErr {
				t.Errorf("Validate(%q) error = %v, wantErr %v", tt.input, msg, tt.wantErr)
			}
		})
	}
}

func TestEqualIgnoreCaseValidator(t *testing.T) {
	t.Parallel()

	v := newEqualIgnoreCaseValidator("Hello")

	tests := []struct {
		input   string
		wantErr bool
	}{
		{"Hello", false},
		{"hello", false},
		{"HELLO", false},
		{"world", true},
		{"", true},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			t.Parallel()
			msg := v.Validate(tt.input)
			hasErr := msg != ""
			if hasErr != tt.wantErr {
				t.Errorf("Validate(%q) error = %v, wantErr %v", tt.input, msg, tt.wantErr)
			}
		})
	}
}

func TestNotEqualIgnoreCaseValidator(t *testing.T) {
	t.Parallel()

	v := newNotEqualIgnoreCaseValidator("Hello")

	tests := []struct {
		input   string
		wantErr bool
	}{
		{"world", false},
		{"", false},
		{"Hello", true},
		{"hello", true},
		{"HELLO", true},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			t.Parallel()
			msg := v.Validate(tt.input)
			hasErr := msg != ""
			if hasErr != tt.wantErr {
				t.Errorf("Validate(%q) error = %v, wantErr %v", tt.input, msg, tt.wantErr)
			}
		})
	}
}

func TestValidators_Validate(t *testing.T) {
	t.Parallel()

	vals := validators{
		newRequiredValidator(),
	}

	tests := []struct {
		name    string
		input   string
		wantTag string
		wantErr bool
	}{
		{"valid value", "hello", "", false},
		{"empty value fails required", "", "required", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tag, msg := vals.Validate(tt.input)
			hasErr := msg != ""
			if hasErr != tt.wantErr {
				t.Errorf("Validate(%q) error = %v, wantErr %v", tt.input, msg, tt.wantErr)
			}
			if tag != tt.wantTag {
				t.Errorf("Validate(%q) tag = %q, want %q", tt.input, tag, tt.wantTag)
			}
		})
	}
}

// TestValidatorNames tests that all validators return correct names
func TestValidatorNames(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		createFunc func() validator
		wantName   string
	}{
		// Basic validators
		{"required", func() validator { return newRequiredValidator() }, "required"},
		{"boolean", func() validator { return newBooleanValidator() }, "boolean"},
		{"alpha", func() validator { return newAlphaValidator() }, "alpha"},
		{"alphaunicode", func() validator { return newAlphaUnicodeValidator() }, "alphaunicode"},
		{"alphaspace", func() validator { return newAlphaSpaceValidator() }, "alphaspace"},
		{"numeric", func() validator { return newNumericValidator() }, "numeric"},
		{"number", func() validator { return newNumberValidator() }, "number"},
		{"alphanum", func() validator { return newAlphanumericValidator(alphanumTagValue) }, "alphanum"},
		{"alphanumeric", func() validator { return newAlphanumericValidator(alphanumericTagValue) }, "alphanumeric"},
		{"alphanumunicode", func() validator { return newAlphanumericUnicodeValidator() }, "alphanumunicode"},

		// Comparison validators (take float64)
		{"eq", func() validator { return newEqualValidator(5.0) }, "eq"},
		{"ne", func() validator { return newNotEqualValidator(5.0) }, "ne"},
		{"gt", func() validator { return newGreaterThanValidator(5.0) }, "gt"},
		{"gte", func() validator { return newGreaterThanEqualValidator(5.0) }, "gte"},
		{"lt", func() validator { return newLessThanValidator(5.0) }, "lt"},
		{"lte", func() validator { return newLessThanEqualValidator(5.0) }, "lte"},
		{"min", func() validator { return newMinValidator(1.0) }, "min"},
		{"max", func() validator { return newMaxValidator(10.0) }, "max"},
		{"len", func() validator { return newLengthValidator(5) }, "len"},

		// String validators
		{"oneof", func() validator { return newOneOfValidator([]string{"a", "b", "c"}) }, "oneof"},
		{"lowercase", func() validator { return newLowercaseValidator() }, "lowercase"},
		{"uppercase", func() validator { return newUppercaseValidator() }, "uppercase"},
		{"ascii", func() validator { return newASCIIValidator() }, "ascii"},
		{"printascii", func() validator { return newPrintASCIIValidator() }, "printascii"},

		// Format validators
		{"email", func() validator { return newEmailValidator() }, "email"},
		{"uri", func() validator { return newURIValidator() }, "uri"},
		{"url", func() validator { return newURLValidator() }, "url"},
		{"http_url", func() validator { return newHTTPURLValidator() }, "http_url"},
		{"https_url", func() validator { return newHTTPSURLValidator() }, "https_url"},
		{"url_encoded", func() validator { return newURLEncodedValidator() }, "url_encoded"},
		{"datauri", func() validator { return newDataURIValidator() }, "datauri"},

		// Network validators
		{"ip_addr", func() validator { return newIPAddrValidator(ipAddrTagValue) }, "ip_addr"},
		{"ip4_addr", func() validator { return newIP4AddrValidator(ip4AddrTagValue) }, "ip4_addr"},
		{"ip6_addr", func() validator { return newIP6AddrValidator(ip6AddrTagValue) }, "ip6_addr"},
		{"cidr", func() validator { return newCIDRValidator() }, "cidr"},
		{"cidrv4", func() validator { return newCIDRv4Validator() }, "cidrv4"},
		{"cidrv6", func() validator { return newCIDRv6Validator() }, "cidrv6"},

		// Identifier validators
		{"uuid", func() validator { return newUUIDValidator() }, "uuid"},
		{"fqdn", func() validator { return newFQDNValidator() }, "fqdn"},
		{"hostname", func() validator { return newHostnameValidator() }, "hostname"},
		{"hostname_rfc1123", func() validator { return newHostnameRFC1123Validator() }, "hostname_rfc1123"},
		{"hostname_port", func() validator { return newHostnamePortValidator() }, "hostname_port"},

		// String content validators
		{"startswith", func() validator { return newStartsWithValidator("pre") }, "startswith"},
		{"startsnotwith", func() validator { return newStartsNotWithValidator("pre") }, "startsnotwith"},
		{"endswith", func() validator { return newEndsWithValidator("suf") }, "endswith"},
		{"endsnotwith", func() validator { return newEndsNotWithValidator("suf") }, "endsnotwith"},
		{"contains", func() validator { return newContainsValidator("sub") }, "contains"},
		{"containsany", func() validator { return newContainsAnyValidator("abc") }, "containsany"},
		{"containsrune", func() validator { return newContainsRuneValidator('a') }, "containsrune"},

		// Exclusion validators
		{"excludes", func() validator { return newExcludesValidator("sub") }, "excludes"},
		{"excludesall", func() validator { return newExcludesAllValidator("abc") }, "excludesall"},
		{"excludesrune", func() validator { return newExcludesRuneValidator('a') }, "excludesrune"},

		// Misc validators
		{"multibyte", func() validator { return newMultibyteValidator() }, "multibyte"},
		{"eq_ignore_case", func() validator { return newEqualIgnoreCaseValidator("test") }, "eq_ignore_case"},
		{"ne_ignore_case", func() validator { return newNotEqualIgnoreCaseValidator("test") }, "ne_ignore_case"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			v := tt.createFunc()
			if got := v.Name(); got != tt.wantName {
				t.Errorf("Name() = %q, want %q", got, tt.wantName)
			}
		})
	}
}

// =============================================================================
// New Validators Tests
// =============================================================================

func TestDatetimeValidator(t *testing.T) {
	t.Parallel()

	v := newDatetimeValidator("2006-01-02")

	tests := []struct {
		input   string
		wantErr bool
	}{
		{"2023-12-25", false},
		{"2024-01-01", false},
		{"", false}, // empty is valid (use required for mandatory)
		{"25-12-2023", true},
		{"2023/12/25", true},
		{"invalid", true},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			t.Parallel()
			msg := v.Validate(tt.input)
			hasErr := msg != ""
			if hasErr != tt.wantErr {
				t.Errorf("Validate(%q) error = %v, wantErr %v", tt.input, msg, tt.wantErr)
			}
		})
	}

	if v.Name() != "datetime" {
		t.Errorf("Name() = %q, want %q", v.Name(), "datetime")
	}
}

func TestE164Validator(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input   string
		wantErr bool
	}{
		{"+12025551234", false},
		{"+819012345678", false},
		{"+1234567890123", false},
		{"", false}, // empty is valid
		// The dialect makes the leading plus optional, and a spreadsheet
		// export strips it.
		{"12025551234", false},
		{"819012345678", false},
		{"123456789012345", false}, // fifteen digits, the upper bound
		{"1234567890123456", true}, // sixteen digits
		{"+", true},
		{"++819012345678", true},
		{"+1", true},
		{"+123456", true},
		{"+0123456789", true},
		{"invalid", true},
	}

	v := newE164Validator()

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			t.Parallel()
			msg := v.Validate(tt.input)
			hasErr := msg != ""
			if hasErr != tt.wantErr {
				t.Errorf("Validate(%q) error = %v, wantErr %v", tt.input, msg, tt.wantErr)
			}
		})
	}

	if v.Name() != "e164" {
		t.Errorf("Name() = %q, want %q", v.Name(), "e164")
	}
}

func TestLatitudeValidator(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input   string
		wantErr bool
	}{
		{"0", false},
		{"35.6762", false},
		{"-35.6762", false},
		{"90", false},
		{"-90", false},
		{"90.0", false},
		{"", false}, // empty is valid
		{"91", true},
		{"-91", true},
		{"invalid", true},
	}

	v := newLatitudeValidator()

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			t.Parallel()
			msg := v.Validate(tt.input)
			hasErr := msg != ""
			if hasErr != tt.wantErr {
				t.Errorf("Validate(%q) error = %v, wantErr %v", tt.input, msg, tt.wantErr)
			}
		})
	}

	if v.Name() != "latitude" {
		t.Errorf("Name() = %q, want %q", v.Name(), "latitude")
	}
}

func TestLongitudeValidator(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input   string
		wantErr bool
	}{
		{"0", false},
		{"139.6917", false},
		{"-139.6917", false},
		{"180", false},
		{"-180", false},
		{"180.0", false},
		{"", false}, // empty is valid
		{"181", true},
		{"-181", true},
		{"invalid", true},
	}

	v := newLongitudeValidator()

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			t.Parallel()
			msg := v.Validate(tt.input)
			hasErr := msg != ""
			if hasErr != tt.wantErr {
				t.Errorf("Validate(%q) error = %v, wantErr %v", tt.input, msg, tt.wantErr)
			}
		})
	}

	if v.Name() != "longitude" {
		t.Errorf("Name() = %q, want %q", v.Name(), "longitude")
	}
}

func TestUUID3Validator(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input   string
		wantErr bool
	}{
		{"a3bb189e-8bf9-3888-9912-ace4e6543002", false},
		{"A3BB189E-8BF9-3888-9912-ACE4E6543002", false},
		{"550e8400-e29b-41d4-a716-446655440000", true}, // UUID v4
		{"a3bb189e-8bf9-3888-7912-ace4e6543002", true}, // invalid variant for UUID v3
		{"invalid", true},
		{"", true},
	}

	v := newUUID3Validator()

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			t.Parallel()
			msg := v.Validate(tt.input)
			hasErr := msg != ""
			if hasErr != tt.wantErr {
				t.Errorf("Validate(%q) error = %v, wantErr %v", tt.input, msg, tt.wantErr)
			}
		})
	}

	if v.Name() != "uuid3" {
		t.Errorf("Name() = %q, want %q", v.Name(), "uuid3")
	}
}

func TestUUID4Validator(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input   string
		wantErr bool
	}{
		{"550e8400-e29b-41d4-a716-446655440000", false},
		{"f47ac10b-58cc-4372-a567-0e02b2c3d479", false},
		// Upper case is accepted here as it is for uuid, which is where the
		// dialect is inconsistent and prep is not.
		{"F47AC10B-58CC-4372-A567-0E02B2C3D479", false},
		{"a3bb189e-8bf9-3888-9912-ace4e6543002", true}, // UUID v3
		{"invalid", true},
		{"", true},
	}

	v := newUUID4Validator()

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			t.Parallel()
			msg := v.Validate(tt.input)
			hasErr := msg != ""
			if hasErr != tt.wantErr {
				t.Errorf("Validate(%q) error = %v, wantErr %v", tt.input, msg, tt.wantErr)
			}
		})
	}

	if v.Name() != "uuid4" {
		t.Errorf("Name() = %q, want %q", v.Name(), "uuid4")
	}
}

func TestUUID5Validator(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input   string
		wantErr bool
	}{
		{"886313e1-3b8a-5372-9b90-0c9aee199e5d", false},
		{"886313E1-3B8A-5372-9B90-0C9AEE199E5D", false},
		{"550e8400-e29b-41d4-a716-446655440000", true}, // UUID v4
		{"invalid", true},
		{"", true},
	}

	v := newUUID5Validator()

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			t.Parallel()
			msg := v.Validate(tt.input)
			hasErr := msg != ""
			if hasErr != tt.wantErr {
				t.Errorf("Validate(%q) error = %v, wantErr %v", tt.input, msg, tt.wantErr)
			}
		})
	}

	if v.Name() != "uuid5" {
		t.Errorf("Name() = %q, want %q", v.Name(), "uuid5")
	}
}

func TestULIDValidator(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input   string
		wantErr bool
	}{
		{"01ARZ3NDEKTSV4RRFFQ69G5FAV", false},
		{"01arZ3NdEKTSV4RRFFQ69G5FAV", false},
		{"81ARZ3NDEKTSV4RRFFQ69G5FAV", true}, // exceeds ULID max value prefix
		{"invalid", true},
		{"01ARZ3NDEKTSV4RRFFQ69G5FA", true}, // too short
		{"", true},
	}

	v := newULIDValidator()

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			t.Parallel()
			msg := v.Validate(tt.input)
			hasErr := msg != ""
			if hasErr != tt.wantErr {
				t.Errorf("Validate(%q) error = %v, wantErr %v", tt.input, msg, tt.wantErr)
			}
		})
	}

	if v.Name() != "ulid" {
		t.Errorf("Name() = %q, want %q", v.Name(), "ulid")
	}
}

func TestHexadecimalValidator(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input   string
		wantErr bool
	}{
		{"1234567890abcdef", false},
		{"ABCDEF", false},
		{"0x1234", false},
		{"0X1234", false},
		{"", false}, // empty is valid
		{"ghij", true},
		{"0x", true},
	}

	v := newHexadecimalValidator()

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			t.Parallel()
			msg := v.Validate(tt.input)
			hasErr := msg != ""
			if hasErr != tt.wantErr {
				t.Errorf("Validate(%q) error = %v, wantErr %v", tt.input, msg, tt.wantErr)
			}
		})
	}

	if v.Name() != "hexadecimal" {
		t.Errorf("Name() = %q, want %q", v.Name(), "hexadecimal")
	}
}

func TestHexColorValidator(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input   string
		wantErr bool
	}{
		{"#fff", false},
		{"#FFF", false},
		{"#ffffff", false},
		{"#FFFFFF", false},
		{"#ffff", false},     // RGBA short
		{"#ffffffff", false}, // RRGGBBAA
		{"", false},          // empty is valid
		{"fff", true},
		{"#ff", true},
		{"#fffff", true},
	}

	v := newHexColorValidator()

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			t.Parallel()
			msg := v.Validate(tt.input)
			hasErr := msg != ""
			if hasErr != tt.wantErr {
				t.Errorf("Validate(%q) error = %v, wantErr %v", tt.input, msg, tt.wantErr)
			}
		})
	}

	if v.Name() != "hexcolor" {
		t.Errorf("Name() = %q, want %q", v.Name(), "hexcolor")
	}
}

func TestRGBValidator(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input   string
		wantErr bool
	}{
		{"rgb(0, 0, 0)", false},
		{"rgb(255, 255, 255)", false},
		{"rgb(100, 100, 100)", false},
		{"rgb(100%, 0%, 50%)", false},
		{"", false}, // empty is valid
		{"rgb(256, 0, 0)", true},
		{"rgb(-1, 0, 0)", true},
		{"rgb(255%, 0%, 0%)", true},
		{"invalid", true},
	}

	v := newRGBValidator()

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			t.Parallel()
			msg := v.Validate(tt.input)
			hasErr := msg != ""
			if hasErr != tt.wantErr {
				t.Errorf("Validate(%q) error = %v, wantErr %v", tt.input, msg, tt.wantErr)
			}
		})
	}

	if v.Name() != "rgb" {
		t.Errorf("Name() = %q, want %q", v.Name(), "rgb")
	}
}

func TestRGBAValidator(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input   string
		wantErr bool
	}{
		{"rgba(0, 0, 0, 0)", false},
		{"rgba(255, 255, 255, 1)", false},
		{"rgba(100, 100, 100, 0.5)", false},
		{"rgba(10%, 20%, 30%, 0.0)", false},
		{"", false}, // empty is valid
		{"rgba(256, 0, 0, 0)", true},
		{"rgba(255%, 0%, 0%, 0.5)", true},
		{"rgba(0, 0, 0, 0x5)", true},
		{"invalid", true},
	}

	v := newRGBAValidator()

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			t.Parallel()
			msg := v.Validate(tt.input)
			hasErr := msg != ""
			if hasErr != tt.wantErr {
				t.Errorf("Validate(%q) error = %v, wantErr %v", tt.input, msg, tt.wantErr)
			}
		})
	}

	if v.Name() != "rgba" {
		t.Errorf("Name() = %q, want %q", v.Name(), "rgba")
	}
}

func TestHSLValidator(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input   string
		wantErr bool
	}{
		{"hsl(0, 0%, 0%)", false},
		{"hsl(360, 100%, 100%)", false},
		{"hsl(180, 50%, 50%)", false},
		{"", false}, // empty is valid
		{"hsl(361, 0%, 0%)", true},
		{"invalid", true},
	}

	v := newHSLValidator()

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			t.Parallel()
			msg := v.Validate(tt.input)
			hasErr := msg != ""
			if hasErr != tt.wantErr {
				t.Errorf("Validate(%q) error = %v, wantErr %v", tt.input, msg, tt.wantErr)
			}
		})
	}

	if v.Name() != "hsl" {
		t.Errorf("Name() = %q, want %q", v.Name(), "hsl")
	}
}

func TestHSLAValidator(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input   string
		wantErr bool
	}{
		{"hsla(0, 0%, 0%, 0)", false},
		{"hsla(360, 100%, 100%, 1)", false},
		{"hsla(180, 50%, 50%, 0.5)", false},
		{"hsla(180, 50%, 50%, 0.0)", false},
		{"", false}, // empty is valid
		{"hsla(361, 0%, 0%, 0)", true},
		{"hsla(180, 50%, 50%, 0x5)", true},
		{"invalid", true},
	}

	v := newHSLAValidator()

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			t.Parallel()
			msg := v.Validate(tt.input)
			hasErr := msg != ""
			if hasErr != tt.wantErr {
				t.Errorf("Validate(%q) error = %v, wantErr %v", tt.input, msg, tt.wantErr)
			}
		})
	}

	if v.Name() != "hsla" {
		t.Errorf("Name() = %q, want %q", v.Name(), "hsla")
	}
}

func TestMACValidator(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input   string
		wantErr bool
	}{
		{"00:00:00:00:00:00", false},
		{"FF:FF:FF:FF:FF:FF", false},
		{"01:23:45:67:89:ab", false},
		{"01-23-45-67-89-AB", false},
		{"", false}, // empty is valid
		{"invalid", true},
		{"00:00:00:00:00", true},
	}

	v := newMACValidator()

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			t.Parallel()
			msg := v.Validate(tt.input)
			hasErr := msg != ""
			if hasErr != tt.wantErr {
				t.Errorf("Validate(%q) error = %v, wantErr %v", tt.input, msg, tt.wantErr)
			}
		})
	}

	if v.Name() != "mac" {
		t.Errorf("Name() = %q, want %q", v.Name(), "mac")
	}
}

// TestLengthComparisonValidators pins the character-count reading every
// comparison validator takes on a string field, at the boundary on both sides.
func TestLengthComparisonValidators(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		v       validator
		input   string
		wantErr bool
	}{
		{"gt=3 passes four runes", &greaterThanValidator{threshold: 3, measuresLength: true}, "abcd", false},
		{"gt=3 refuses three runes", &greaterThanValidator{threshold: 3, measuresLength: true}, "abc", true},
		{"gte=3 passes three runes", &greaterThanEqualValidator{threshold: 3, measuresLength: true}, "abc", false},
		{"gte=3 refuses two runes", &greaterThanEqualValidator{threshold: 3, measuresLength: true}, "ab", true},
		{"lt=3 passes two runes", &lessThanValidator{threshold: 3, measuresLength: true}, "ab", false},
		{"lt=3 refuses three runes", &lessThanValidator{threshold: 3, measuresLength: true}, "abc", true},
		{"lte=3 passes three runes", &lessThanEqualValidator{threshold: 3, measuresLength: true}, "abc", false},
		{"lte=3 refuses four runes", &lessThanEqualValidator{threshold: 3, measuresLength: true}, "abcd", true},
		{"gte=3 counts runes, not bytes", &greaterThanEqualValidator{threshold: 3, measuresLength: true}, "日本語", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			msg := tt.v.Validate(tt.input)
			if (msg != "") != tt.wantErr {
				t.Errorf("Validate(%q) = %q, wantErr %v", tt.input, msg, tt.wantErr)
			}
		})
	}
}

// TestSentinelValidatorsAnswerDirectly covers the two validators whose Validate
// never runs through the pipeline: omitempty is a marker, and an unspecialized
// eq must fail loudly rather than validate nothing.
func TestSentinelValidatorsAnswerDirectly(t *testing.T) {
	t.Parallel()

	if msg := (&omitemptyValidator{}).Validate("anything"); msg != "" {
		t.Errorf("omitempty.Validate() = %q, want no message", msg)
	}
	pending := &pendingEqualityValidator{tag: equalTagValue, param: "x"}
	if msg := pending.Validate("x"); msg == "" {
		t.Error("an unspecialized eq must report itself instead of validating nothing")
	}
}

// TestAnEmptyValueSkipsEveryValidatorExceptRequired pins the one empty-cell
// rule for the whole registry: an empty value passes every validator except
// required, and a caller who needs both presence and format writes required
// alongside the format tag. Walking validatorRegistry keeps a future validator
// from joining the wrong side unnoticed.
func TestAnEmptyValueSkipsEveryValidatorExceptRequired(t *testing.T) {
	t.Parallel()

	// Parameters for the tags that need one; every other tag builds bare.
	params := map[string]string{
		equalTagValue:              "1",
		notEqualTagValue:           "1",
		greaterThanTagValue:        "1",
		greaterThanEqualTagValue:   "1",
		lessThanTagValue:           "1",
		lessThanEqualTagValue:      "1",
		minTagValue:                "1",
		maxTagValue:                "1",
		lengthTagValue:             "1",
		oneOfTagValue:              "a b",
		startsWithTagValue:         "a",
		startsNotWithTagValue:      "a",
		endsWithTagValue:           "a",
		endsNotWithTagValue:        "a",
		containsTagValue:           "a",
		containsAnyTagValue:        "ab",
		containsRuneTagValue:       "a",
		excludesTagValue:           "a",
		excludesAllTagValue:        "ab",
		excludesRuneTagValue:       "a",
		equalIgnoreCaseTagValue:    "a",
		notEqualIgnoreCaseTagValue: "a",
		datetimeTagValue:           "2006-01-02",
	}

	for tag, builder := range validatorRegistry {
		t.Run(tag, func(t *testing.T) {
			t.Parallel()

			v, err := builder(params[tag], true)
			if err != nil {
				t.Fatalf("builder(%q) error = %v", params[tag], err)
			}
			if v == nil {
				t.Fatalf("builder(%q) returned no validator", params[tag])
			}

			gotTag, msg := validators{v}.Validate("")
			if tag == requiredTagValue {
				if msg == "" {
					t.Error("required must reject an empty value")
				}
				return
			}
			if msg != "" {
				t.Errorf("an empty value must pass %q, got tag=%q msg=%q", tag, gotTag, msg)
			}
		})
	}
}

// ip, ipv4, ipv6 and port are the spellings the go-playground dialect
// documents; prep answered only to ip_addr, ip4_addr and ip6_addr, and to no
// spelling of port at all.
func TestIPValidatorsUnderTheDialectSpellings(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input  string
		wantIP bool
		wantV4 bool
		wantV6 bool
	}{
		{"192.0.2.1", true, true, false},
		{"2001:db8::1", true, false, true},
		// An IPv4-mapped address is an IPv4 address to net.IP, so it satisfies
		// ipv4 and fails ipv6, which is what the dialect answers too.
		{"::ffff:192.0.2.1", true, true, false},
		{"256.1.1.1", false, false, false},
		{"example.com", false, false, false},
		// An empty cell passes every validator but required.
		{"", true, true, true},
	}

	ip := newIPAddrValidator(ipTagValue)
	ipv4 := newIP4AddrValidator(ipv4TagValue)
	ipv6 := newIP6AddrValidator(ipv6TagValue)

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			t.Parallel()
			for _, c := range []struct {
				tag  string
				vs   validators
				want bool
			}{
				{ipTagValue, validators{ip}, tt.wantIP},
				{ipv4TagValue, validators{ipv4}, tt.wantV4},
				{ipv6TagValue, validators{ipv6}, tt.wantV6},
			} {
				_, msg := c.vs.Validate(tt.input)
				if (msg == "") != c.want {
					t.Errorf("%s.Validate(%q) = %q, want pass = %v", c.tag, tt.input, msg, c.want)
				}
			}
		})
	}

	if got := ip.Name(); got != ipTagValue {
		t.Errorf("Name() = %q, want %q", got, ipTagValue)
	}
	if got := ipv4.Name(); got != ipv4TagValue {
		t.Errorf("Name() = %q, want %q", got, ipv4TagValue)
	}
	if got := ipv6.Name(); got != ipv6TagValue {
		t.Errorf("Name() = %q, want %q", got, ipv6TagValue)
	}
}

// port is defined on a numeric field in the dialect, so the form a cell may
// take is pinned here: ASCII digits alone that name a port from 1 to 65535.
func TestPortValidator(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input string
		want  bool
	}{
		{"1", true},
		{"80", true},
		{"65535", true},
		// Leading zeros are digits, so this is port 80.
		{"0080", true},
		{"0", false},
		{"65536", false},
		{"-1", false},
		{"+80", false},
		{"8080a", false},
		{"0x50", false},
		{"80 ", false},
		{"", true},
	}

	vs := validators{newPortValidator()}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			t.Parallel()
			_, msg := vs.Validate(tt.input)
			if (msg == "") != tt.want {
				t.Errorf("port.Validate(%q) = %q, want pass = %v", tt.input, msg, tt.want)
			}
		})
	}

	if got := newPortValidator().Name(); got != portTagValue {
		t.Errorf("Name() = %q, want %q", got, portTagValue)
	}
}
