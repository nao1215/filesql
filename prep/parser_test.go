package prep

import (
	"errors"
	"io"
	"os"
	"reflect"
	"strings"
	"testing"
)

func TestParsePrepTag(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		tag     string
		wantLen int
		wantErr bool
	}{
		{"empty tag", "", 0, false},
		{"single trim", "trim", 1, false},
		{"multiple preps", "trim,lowercase", 2, false},
		{"with default", "trim,default=N/A", 2, false},
		{"unknown tag", "unknown", 0, true},
		{"spaces in tag", " trim , lowercase ", 2, false},
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
				t.Errorf("parsePrepTag() len = %d, want %d", len(preps), tt.wantLen)
			}
		})
	}
}

func TestParseValidateTag(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		tag     string
		wantLen int
		wantErr bool
	}{
		{"empty tag", "", 0, false},
		{"required", "required", 1, false},
		{"unknown tag returns error", "unknown", 0, true},
		{"spaces in tag", " required ", 1, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			vals, _, err := parseValidateTag(tt.tag, false)
			if (err != nil) != tt.wantErr {
				t.Errorf("parseValidateTag() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && len(vals) != tt.wantLen {
				t.Errorf("parseValidateTag() len = %d, want %d", len(vals), tt.wantLen)
			}
		})
	}
}

func TestParseValidateTag_AllValidatorTypes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		tag       string
		wantVals  int
		wantCross int
		wantErr   bool
	}{
		// Basic validators
		{"boolean", "boolean", 1, 0, false},
		{"alpha", "alpha", 1, 0, false},
		{"alphaspace", "alphaspace", 1, 0, false},
		{"alphaunicode", "alphaunicode", 1, 0, false},
		{"numeric", "numeric", 1, 0, false},
		{"number", "number", 1, 0, false},
		{"alphanumeric", "alphanumeric", 1, 0, false},
		{"alphanum", "alphanum", 1, 0, false},
		{"alphanumspace", "alphanumspace", 1, 0, false},
		{"alphanumunicode", "alphanumunicode", 1, 0, false},

		// Comparison validators
		{"eq=100", "eq=100", 1, 0, false},
		{"ne=0", "ne=0", 1, 0, false},
		{"gt=0", "gt=0", 1, 0, false},
		{"gte=1", "gte=1", 1, 0, false},
		{"lt=100", "lt=100", 1, 0, false},
		{"lte=99", "lte=99", 1, 0, false},
		{"min=0", "min=0", 1, 0, false},
		{"max=100", "max=100", 1, 0, false},
		{"len=10", "len=10", 1, 0, false},

		// A threshold that must be numeric for every field kind is silently
		// skipped when it is not; eq and ne keep their parameter for
		// specializeValidator, where the field's kind decides its meaning.
		{"eq=abc (deferred to the field)", "eq=abc", 1, 0, false},
		{"ne=abc (deferred to the field)", "ne=abc", 1, 0, false},
		{"gt=abc (invalid float)", "gt=abc", 0, 0, false},
		{"len=abc (invalid int)", "len=abc", 0, 0, false},

		// String validators
		{"oneof=a b c", "oneof=a b c", 1, 0, false},
		{"lowercase", "lowercase", 1, 0, false},
		{"uppercase", "uppercase", 1, 0, false},
		{"ascii", "ascii", 1, 0, false},
		{"printascii", "printascii", 1, 0, false},

		// Format validators
		{"email", "email", 1, 0, false},
		{"uri", "uri", 1, 0, false},
		{"url", "url", 1, 0, false},
		{"http_url", "http_url", 1, 0, false},
		{"https_url", "https_url", 1, 0, false},
		{"url_encoded", "url_encoded", 1, 0, false},
		{"datauri", "datauri", 1, 0, false},
		{"datetime=2006-01-02", "datetime=2006-01-02", 1, 0, false},
		{"e164", "e164", 1, 0, false},

		// Network validators
		{"ip", "ip", 1, 0, false},
		{"ipv4", "ipv4", 1, 0, false},
		{"ipv6", "ipv6", 1, 0, false},
		{"port", "port", 1, 0, false},
		{"ip_addr", "ip_addr", 1, 0, false},
		{"ip4_addr", "ip4_addr", 1, 0, false},
		{"ip6_addr", "ip6_addr", 1, 0, false},
		{"cidr", "cidr", 1, 0, false},
		{"cidrv4", "cidrv4", 1, 0, false},
		{"cidrv6", "cidrv6", 1, 0, false},
		{"mac", "mac", 1, 0, false},

		// Identifier validators
		{"uuid", "uuid", 1, 0, false},
		{"uuid3", "uuid3", 1, 0, false},
		{"uuid4", "uuid4", 1, 0, false},
		{"uuid5", "uuid5", 1, 0, false},
		{"uuid_rfc4122", "uuid_rfc4122", 1, 0, false},
		{"uuid3_rfc4122", "uuid3_rfc4122", 1, 0, false},
		{"uuid4_rfc4122", "uuid4_rfc4122", 1, 0, false},
		{"uuid5_rfc4122", "uuid5_rfc4122", 1, 0, false},
		{"dns_rfc1035_label", "dns_rfc1035_label", 1, 0, false},
		{"ulid", "ulid", 1, 0, false},
		{"fqdn", "fqdn", 1, 0, false},
		{"hostname", "hostname", 1, 0, false},
		{"hostname_rfc1123", "hostname_rfc1123", 1, 0, false},
		{"hostname_port", "hostname_port", 1, 0, false},

		// String content validators
		{"startswith=http", "startswith=http", 1, 0, false},
		{"startsnotwith=_", "startsnotwith=_", 1, 0, false},
		{"endswith=.com", "endswith=.com", 1, 0, false},
		{"endsnotwith=.tmp", "endsnotwith=.tmp", 1, 0, false},
		{"contains=@", "contains=@", 1, 0, false},
		{"containsany=abc", "containsany=abc", 1, 0, false},
		{"containsrune=@", "containsrune=@", 1, 0, false},
		{"excludes=admin", "excludes=admin", 1, 0, false},
		{"excludesall=<>", "excludesall=<>", 1, 0, false},
		{"excludesrune=$", "excludesrune=$", 1, 0, false},

		// Misc validators
		{"multibyte", "multibyte", 1, 0, false},
		{"eq_ignore_case=yes", "eq_ignore_case=yes", 1, 0, false},
		{"ne_ignore_case=no", "ne_ignore_case=no", 1, 0, false},

		// Geolocation validators
		{"latitude", "latitude", 1, 0, false},
		{"longitude", "longitude", 1, 0, false},

		// Structured format validators
		{"json", "json", 1, 0, false},
		{"timezone", "timezone", 1, 0, false},
		{"semver", "semver", 1, 0, false},

		// RFC 4648 encoding validators
		{"base32", "base32", 1, 0, false},
		{"base64", "base64", 1, 0, false},
		{"base64url", "base64url", 1, 0, false},
		{"base64rawurl", "base64rawurl", 1, 0, false},

		// Case-insensitive membership
		{"oneofci=red green", "oneofci=red green", 1, 0, false},
		{"oneofci= (empty)", "oneofci=", 0, 0, false},

		// Checksummed identifier validators
		{"credit_card", "credit_card", 1, 0, false},
		{"luhn_checksum", "luhn_checksum", 1, 0, false},
		{"isbn", "isbn", 1, 0, false},
		{"isbn10", "isbn10", 1, 0, false},
		{"isbn13", "isbn13", 1, 0, false},
		{"issn", "issn", 1, 0, false},

		// Country and currency code validators
		{"iso3166_1_alpha2", "iso3166_1_alpha2", 1, 0, false},
		{"iso3166_1_alpha3", "iso3166_1_alpha3", 1, 0, false},
		{"iso3166_1_alpha_numeric", "iso3166_1_alpha_numeric", 1, 0, false},
		{"country_code", "country_code", 1, 0, false},
		{"iso4217", "iso4217", 1, 0, false},
		{"iso4217_numeric", "iso4217_numeric", 1, 0, false},

		// unique builds a sentinel here that parseStructType records on the
		// field and drops; see TestUniqueMarksTheField.
		{"unique", "unique", 1, 0, false},
		{"unique with a parameter is dropped in non-strict mode", "unique=x", 0, 0, false},

		// Message digest validators
		{"md5", "md5", 1, 0, false},
		{"sha256", "sha256", 1, 0, false},
		{"sha384", "sha384", 1, 0, false},
		{"sha512", "sha512", 1, 0, false},

		// Hexadecimal and color validators
		{"hexadecimal", "hexadecimal", 1, 0, false},
		{"hexcolor", "hexcolor", 1, 0, false},
		{"iscolor", "iscolor", 1, 0, false},
		{"rgb", "rgb", 1, 0, false},
		{"rgba", "rgba", 1, 0, false},
		{"hsl", "hsl", 1, 0, false},
		{"hsla", "hsla", 1, 0, false},

		// Cross-field validators
		{"eqfield=X", "eqfield=X", 0, 1, false},
		{"nefield=X", "nefield=X", 0, 1, false},
		{"gtfield=X", "gtfield=X", 0, 1, false},
		{"gtefield=X", "gtefield=X", 0, 1, false},
		{"ltfield=X", "ltfield=X", 0, 1, false},
		{"ltefield=X", "ltefield=X", 0, 1, false},
		{"fieldcontains=X", "fieldcontains=X", 0, 1, false},
		{"fieldexcludes=X", "fieldexcludes=X", 0, 1, false},

		// Conditional required validators
		{"required_if=Status active", "required_if=Status active", 0, 1, false},
		{"required_unless=Type guest", "required_unless=Type guest", 0, 1, false},
		{"required_with=Email", "required_with=Email", 0, 1, false},
		{"required_without=Phone", "required_without=Phone", 0, 1, false},

		// Conditional excluded validators
		{"excluded_if=Status active", "excluded_if=Status active", 0, 1, false},
		{"excluded_unless=Type guest", "excluded_unless=Type guest", 0, 1, false},
		{"excluded_with=Email", "excluded_with=Email", 0, 1, false},
		{"excluded_with_all=Email Phone", "excluded_with_all=Email Phone", 0, 1, false},
		{"excluded_without=Phone", "excluded_without=Phone", 0, 1, false},
		{"excluded_without_all=Email Phone", "excluded_without_all=Email Phone", 0, 1, false},

		// Empty value parameters are silently skipped
		{"startswith= (empty)", "startswith=", 0, 0, false},
		{"contains= (empty)", "contains=", 0, 0, false},
		{"excludes= (empty)", "excludes=", 0, 0, false},
		{"oneof= (empty)", "oneof=", 0, 0, false},
		{"eqfield= (empty)", "eqfield=", 0, 0, false},
		{"datetime= (empty)", "datetime=", 0, 0, false},

		// Multiple combined validators
		{"required,email,min=5", "required,email,min=5", 3, 0, false},
		{"required,eqfield=Other", "required,eqfield=Other", 1, 1, false},

		// Error case
		{"unknown_validator", "unknown_validator", 0, 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			vals, crossVals, err := parseValidateTag(tt.tag, false)
			if (err != nil) != tt.wantErr {
				t.Errorf("parseValidateTag(%q) error = %v, wantErr %v", tt.tag, err, tt.wantErr)
				return
			}
			if !tt.wantErr {
				if len(vals) != tt.wantVals {
					t.Errorf("parseValidateTag(%q) validators = %d, want %d", tt.tag, len(vals), tt.wantVals)
				}
				if len(crossVals) != tt.wantCross {
					t.Errorf("parseValidateTag(%q) crossVals = %d, want %d", tt.tag, len(crossVals), tt.wantCross)
				}
			}
		})
	}
}

// The dialect spells this tag alphanum. prep answered only to alphanumeric,
// so a struct carrying tags copied from a go-playground struct failed to parse
// on the tag the dialect uses most.
func TestAlphanumIsTheDialectSpellingOfAlphanumeric(t *testing.T) {
	t.Parallel()

	inputs := []string{"hello123", "ABC123", "hello world", "hello-world", ""}
	for _, spelling := range []string{"alphanum", "alphanumeric"} {
		vals, _, err := parseValidateTag(spelling, false)
		if err != nil {
			t.Fatalf("parseValidateTag(%q) = %v", spelling, err)
		}
		if len(vals) != 1 {
			t.Fatalf("parseValidateTag(%q) built %d validators, want 1", spelling, len(vals))
		}
		for _, in := range inputs {
			want := newAlphanumericValidator(spelling).Validate(in)
			if got := vals[0].Validate(in); got != want {
				t.Errorf("%s.Validate(%q) = %q, want %q", spelling, in, got, want)
			}
		}
	}
}

func TestParsePrepTag_AllPreprocessorTypes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		tag     string
		wantLen int
		wantErr bool
	}{
		// Basic preprocessors
		{"trim", "trim", 1, false},
		{"ltrim", "ltrim", 1, false},
		{"rtrim", "rtrim", 1, false},
		{"lowercase", "lowercase", 1, false},
		{"uppercase", "uppercase", 1, false},
		{"default=N/A", "default=N/A", 1, false},

		// String transformation preprocessors
		{"replace=foo:bar", "replace=foo:bar", 1, false},
		{"replace=no-colon (missing colon)", "replace=nocolon", 0, false},
		{"prefix=pre_", "prefix=pre_", 1, false},
		{"prefix= (empty)", "prefix=", 0, false},
		{"suffix=_suf", "suffix=_suf", 1, false},
		{"suffix= (empty)", "suffix=", 0, false},
		{"truncate=10", "truncate=10", 1, false},
		{"truncate=0 (zero)", "truncate=0", 0, false},
		{"truncate=abc (invalid)", "truncate=abc", 0, false},
		{"strip_html", "strip_html", 1, false},
		{"strip_newline", "strip_newline", 1, false},
		{"collapse_space", "collapse_space", 1, false},

		// Character filtering preprocessors
		{"remove_digits", "remove_digits", 1, false},
		{"remove_alpha", "remove_alpha", 1, false},
		{"keep_digits", "keep_digits", 1, false},
		{"keep_alpha", "keep_alpha", 1, false},
		{"trim_set=@#$", "trim_set=@#$", 1, false},
		{"trim_set= (empty)", "trim_set=", 0, false},

		// Padding preprocessors
		{"pad_left=5:0", "pad_left=5:0", 1, false},
		{"pad_right=10:x", "pad_right=10:x", 1, false},

		// Advanced preprocessors
		{"normalize_unicode", "normalize_unicode", 1, false},
		{"nullify=NA", "nullify=NA", 1, false},
		{"nullify= (empty)", "nullify=", 0, false},
		{"coerce=int", "coerce=int", 1, false},
		{"coerce=float", "coerce=float", 1, false},
		{"coerce=bool", "coerce=bool", 1, false},
		{"coerce=string (invalid)", "coerce=string", 0, false},
		{"fix_scheme=https", "fix_scheme=https", 1, false},
		{"fix_scheme= (empty)", "fix_scheme=", 0, false},
		{"regex_replace=\\d+:X", "regex_replace=\\d+:X", 1, false},

		// Multiple combined preprocessors
		{"trim,lowercase,default=N/A", "trim,lowercase,default=N/A", 3, false},

		// Unknown preprocessor
		{"unknown preprocessor", "bad_tag", 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			preps, err := parsePrepTag(tt.tag, false)
			if (err != nil) != tt.wantErr {
				t.Errorf("parsePrepTag(%q) error = %v, wantErr %v", tt.tag, err, tt.wantErr)
				return
			}
			if !tt.wantErr && len(preps) != tt.wantLen {
				t.Errorf("parsePrepTag(%q) len = %d, want %d", tt.tag, len(preps), tt.wantLen)
			}
		})
	}
}

func TestSplitTagParams(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		input string
		want  []string
	}{
		{"field and value", "Status active", []string{"Status", "active"}},
		{"field only", "Status", []string{"Status"}},
		{"empty string", "", nil},
		{"two pairs", "Kind paid Tier gold", []string{"Kind", "paid", "Tier", "gold"}},
		{"quoted value keeps its space", "Status 'on hold'", []string{"Status", "on hold"}},
		{"runs of spaces are one separator", "Status   active", []string{"Status", "active"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := splitTagParams(tt.input)
			if len(got) != len(tt.want) {
				t.Fatalf("splitTagParams(%q) = %q, want %q", tt.input, got, tt.want)
			}
			for i := range got {
				if got[i] != tt.want[i] {
					t.Errorf("splitTagParams(%q)[%d] = %q, want %q", tt.input, i, got[i], tt.want[i])
				}
			}
		})
	}
}

func TestSplitTagKeyValue(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		input     string
		wantKey   string
		wantValue string
	}{
		{"key=value", "eq=100", "eq", "100"},
		{"key only", "required", "required", ""},
		{"key=value with colon", "replace=old:new", "replace", "old:new"},
		{"key=empty value", "default=", "default", ""},
		{"=value (empty key)", "=value", "=value", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			key, value := splitTagKeyValue(tt.input)
			if key != tt.wantKey {
				t.Errorf("splitTagKeyValue(%q) key = %q, want %q", tt.input, key, tt.wantKey)
			}
			if value != tt.wantValue {
				t.Errorf("splitTagKeyValue(%q) value = %q, want %q", tt.input, value, tt.wantValue)
			}
		})
	}
}

func TestParseColonSeparatedValue(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		input     string
		wantOld   string
		wantNew   string
		wantFound bool
	}{
		{"old:new", "old:new", "old", "new", true},
		{"no colon", "nocolon", "", "", false},
		{":empty-old", ":new", "", "new", true},
		{"empty-new:", "old:", "old", "", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			oldVal, newVal, found := parseColonSeparatedValue(tt.input)
			if oldVal != tt.wantOld {
				t.Errorf("parseColonSeparatedValue(%q) old = %q, want %q", tt.input, oldVal, tt.wantOld)
			}
			if newVal != tt.wantNew {
				t.Errorf("parseColonSeparatedValue(%q) new = %q, want %q", tt.input, newVal, tt.wantNew)
			}
			if found != tt.wantFound {
				t.Errorf("parseColonSeparatedValue(%q) found = %v, want %v", tt.input, found, tt.wantFound)
			}
		})
	}
}

func TestParsePadParams(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		input      string
		wantLength int
		wantChar   rune
	}{
		{"5:0", "5:0", 5, '0'},
		{"10:x", "10:x", 10, 'x'},
		{"5 (no char)", "5", 5, ' '},
		{"abc (invalid)", "abc", 0, ' '},
		{"-5 (negative)", "-5", 0, ' '},
		{"0 (zero)", "0", 0, ' '},
		{"empty", "", 0, ' '},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			length, char := parsePadParams(tt.input)
			if length != tt.wantLength {
				t.Errorf("parsePadParams(%q) length = %d, want %d", tt.input, length, tt.wantLength)
			}
			if char != tt.wantChar {
				t.Errorf("parsePadParams(%q) char = %q, want %q", tt.input, char, tt.wantChar)
			}
		})
	}
}

func TestGetStructType(t *testing.T) {
	t.Parallel()

	type TestStruct struct {
		Name string
	}

	var nilSlicePtr *[]TestStruct

	tests := []struct {
		name    string
		input   any
		wantErr bool
	}{
		{"valid slice pointer", &[]TestStruct{}, false},
		{"non-pointer", []TestStruct{}, true},
		{"pointer to non-slice", &TestStruct{}, true},
		{"pointer to slice of non-struct", &[]string{}, true},
		{"nil interface", nil, true},
		{"nil typed pointer", nilSlicePtr, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, err := getStructType(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("getStructType() error = %v, wantErr %v", err, tt.wantErr)
			}
			// Every refusal here carries the one sentinel, whichever of the
			// shapes it is, so a caller can match on it rather than on the
			// wording that names the shape.
			if tt.wantErr && !errors.Is(err, ErrStructSlicePointer) {
				t.Errorf("getStructType() error = %v, want it to wrap ErrStructSlicePointer", err)
			}
		})
	}
}

// TestParsePrepTagInvalidFormats tests that invalid tag formats are handled gracefully
func TestParsePrepTagInvalidFormats(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		tag        string
		wantLen    int
		wantErr    bool
		errContain string
	}{
		// Invalid pad_left format - non-numeric value should be skipped (length=0)
		{"pad_left non-numeric", "pad_left=abc", 0, false, ""},
		{"pad_left negative", "pad_left=-5", 0, false, ""},
		{"pad_left empty", "pad_left=", 0, false, ""},
		{"pad_left valid", "pad_left=5:0", 1, false, ""},

		// Invalid regex_replace format - bad regex should be skipped
		{"regex_replace bad pattern", "regex_replace=bad[:X", 0, false, ""},
		{"regex_replace no colon", "regex_replace=pattern", 0, false, ""},
		{"regex_replace valid", "regex_replace=\\d+:X", 1, false, ""},

		// Invalid coerce format - wrong type should be skipped
		{"coerce invalid type", "coerce=string", 0, false, ""},
		{"coerce valid int", "coerce=int", 1, false, ""},
		{"coerce valid float", "coerce=float", 1, false, ""},
		{"coerce valid bool", "coerce=bool", 1, false, ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			preps, err := parsePrepTag(tt.tag, false)
			if (err != nil) != tt.wantErr {
				t.Errorf("parsePrepTag(%q) error = %v, wantErr %v", tt.tag, err, tt.wantErr)
				return
			}
			if tt.wantErr && tt.errContain != "" {
				if err == nil || !strings.Contains(err.Error(), tt.errContain) {
					t.Errorf("parsePrepTag(%q) error should contain %q, got %v", tt.tag, tt.errContain, err)
				}
			}
			if !tt.wantErr && len(preps) != tt.wantLen {
				t.Errorf("parsePrepTag(%q) len = %d, want %d", tt.tag, len(preps), tt.wantLen)
			}
		})
	}
}

// TestParseStructTypeWithEmbeddedStruct tests how embedded (anonymous) struct fields are handled
func TestParseStructTypeWithEmbeddedStruct(t *testing.T) {
	t.Parallel()

	type Embedded struct {
		EmbeddedField string `prep:"trim"`
	}

	type TestStruct struct {
		Embedded            // Anonymous embedded field
		RegularField string `prep:"lowercase"`
	}

	structType := reflect.TypeOf(TestStruct{})
	info, err := parseStructType(structType, false)
	if err != nil {
		t.Fatalf("parseStructType() error = %v", err)
	}

	// Embedded struct should be treated as a single field (Embedded type)
	// Regular exported fields should also be parsed
	if len(info.Fields) < 1 {
		t.Errorf("parseStructType() fields = %d, want at least 1", len(info.Fields))
	}

	// Check that RegularField is parsed correctly
	foundRegular := false
	for _, field := range info.Fields {
		if field.Name == "RegularField" {
			foundRegular = true
			if len(field.Preprocessors) != 1 {
				t.Errorf("RegularField.Preprocessors len = %d, want 1", len(field.Preprocessors))
			}
		}
	}
	if !foundRegular {
		t.Error("RegularField not found in parsed fields")
	}
}

func TestParseStructType(t *testing.T) {
	t.Parallel()

	type TestStruct struct {
		Name    string `prep:"trim" validate:"required"`
		Email   string `prep:"trim,lowercase"`
		Age     int
		private string //nolint:unused // intentionally unexported for testing
	}

	structType := reflect.TypeOf(TestStruct{})
	info, err := parseStructType(structType, false)
	if err != nil {
		t.Fatalf("parseStructType() error = %v", err)
	}

	// Should have 3 fields (private is skipped)
	if len(info.Fields) != 3 {
		t.Errorf("parseStructType() fields = %d, want 3", len(info.Fields))
	}

	// Check first field
	if len(info.Fields) > 0 {
		field := info.Fields[0]
		if field.Name != "Name" {
			t.Errorf("Field[0].Name = %q, want %q", field.Name, "Name")
		}
		if len(field.Preprocessors) != 1 {
			t.Errorf("Field[0].Preprocessors len = %d, want 1", len(field.Preprocessors))
		}
		if len(field.Validators) != 1 {
			t.Errorf("Field[0].Validators len = %d, want 1", len(field.Validators))
		}
	}

	// Check second field
	if len(info.Fields) > 1 {
		field := info.Fields[1]
		if field.Name != "Email" {
			t.Errorf("Field[1].Name = %q, want %q", field.Name, "Email")
		}
		if len(field.Preprocessors) != 2 {
			t.Errorf("Field[1].Preprocessors len = %d, want 2", len(field.Preprocessors))
		}
	}
}

// TestParseStructTypeUnknownValidateTag tests that unknown validate tags propagate
// through parseStructType with the field name included in the error message.
func TestParseStructTypeUnknownValidateTag(t *testing.T) {
	t.Parallel()

	type BadValidate struct {
		Email string `validate:"unknown_tag"`
	}

	structType := reflect.TypeOf(BadValidate{})
	_, err := parseStructType(structType, false)
	if err == nil {
		t.Fatal("parseStructType() expected error for unknown validate tag, got nil")
	}
	if !errors.Is(err, ErrInvalidTagFormat) {
		t.Errorf("parseStructType() error should wrap ErrInvalidTagFormat, got %v", err)
	}
	if !strings.Contains(err.Error(), "Email") {
		t.Errorf("parseStructType() error should contain field name \"Email\", got %q", err.Error())
	}
	if !strings.Contains(err.Error(), "unknown_tag") {
		t.Errorf("parseStructType() error should contain tag name \"unknown_tag\", got %q", err.Error())
	}
}

// TestParseStructTypeUnknownPrepTag tests that unknown prep tags propagate
// through parseStructType with the field name included in the error message.
func TestParseStructTypeUnknownPrepTag(t *testing.T) {
	t.Parallel()

	type BadPrep struct {
		Name string `prep:"bad_preprocessor"`
	}

	structType := reflect.TypeOf(BadPrep{})
	_, err := parseStructType(structType, false)
	if err == nil {
		t.Fatal("parseStructType() expected error for unknown prep tag, got nil")
	}
	if !errors.Is(err, ErrInvalidTagFormat) {
		t.Errorf("parseStructType() error should wrap ErrInvalidTagFormat, got %v", err)
	}
	if !strings.Contains(err.Error(), "Name") {
		t.Errorf("parseStructType() error should contain field name \"Name\", got %q", err.Error())
	}
}

// TestToSnakeCase tests the snake_case conversion function
func TestToSnakeCase(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input string
		want  string
	}{
		{"", ""},
		{"name", "name"},
		{"Name", "name"},
		{"UserName", "user_name"},
		{"ID", "id"},
		{"UserID", "user_id"},
		{"HTTPServer", "http_server"},
		{"XMLParser", "xml_parser"},
		{"getHTTPResponse", "get_http_response"},
		{"already_snake_case", "already_snake_case"},
		{"A", "a"},
		{"ABC", "abc"},
		{"ABCdef", "ab_cdef"},
		{"abcDEF", "abc_def"},
		{"IOReader", "io_reader"},
		{"myURLParser", "my_url_parser"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			t.Parallel()
			got := toSnakeCase(tt.input)
			if got != tt.want {
				t.Errorf("toSnakeCase(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

// TestColumnNameFromNameTag tests that name tag overrides auto-generated column name
func TestColumnNameFromNameTag(t *testing.T) {
	t.Parallel()

	type TestStruct struct {
		UserName    string `name:"user_name"`
		EmailAddr   string `name:"email_address"`
		Age         int    // No name tag - should use "age" (snake_case of "Age")
		HTTPStatus  string // No name tag - should use "http_status"
		CustomField string `name:"custom_col"`
	}

	structType := reflect.TypeOf(TestStruct{})
	info, err := parseStructType(structType, false)
	if err != nil {
		t.Fatalf("parseStructType() error = %v", err)
	}

	expected := map[string]string{
		"UserName":    "user_name",
		"EmailAddr":   "email_address",
		"Age":         "age",
		"HTTPStatus":  "http_status",
		"CustomField": "custom_col",
	}

	for _, field := range info.Fields {
		want, ok := expected[field.Name]
		if !ok {
			continue
		}
		if field.ColumnName != want {
			t.Errorf("Field %q.ColumnName = %q, want %q", field.Name, field.ColumnName, want)
		}
	}
}

// TestAutoSnakeCaseColumnNames tests automatic snake_case conversion for column names
func TestAutoSnakeCaseColumnNames(t *testing.T) {
	t.Parallel()

	type TestStruct struct {
		FirstName   string
		LastName    string
		EmailAddr   string
		PhoneNumber string
		ID          string
		UserID      string
		HTTPCode    string
		XMLData     string
	}

	structType := reflect.TypeOf(TestStruct{})
	info, err := parseStructType(structType, false)
	if err != nil {
		t.Fatalf("parseStructType() error = %v", err)
	}

	expected := map[string]string{
		"FirstName":   "first_name",
		"LastName":    "last_name",
		"EmailAddr":   "email_addr",
		"PhoneNumber": "phone_number",
		"ID":          "id",
		"UserID":      "user_id",
		"HTTPCode":    "http_code",
		"XMLData":     "xml_data",
	}

	for _, field := range info.Fields {
		want, ok := expected[field.Name]
		if !ok {
			continue
		}
		if field.ColumnName != want {
			t.Errorf("Field %q.ColumnName = %q, want %q", field.Name, field.ColumnName, want)
		}
	}
}

func TestStrictTagParsing_ValidateTag(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		tag     string
		wantErr bool
	}{
		{"eq with valid number", "eq=10", false},
		// eq and ne defer their parameter check to specializeValidator: on a
		// string field "abc" is the string to compare, so the tag alone cannot
		// be judged. See TestSpecializeValidatorJudgesADeferredParameter.
		{"eq with a string parameter parses", "eq=abc", false},
		{"ne with a string parameter parses", "ne=xyz", false},
		{"gt with invalid value", "gt=notnum", true},
		{"gte with invalid value", "gte=abc", true},
		{"lt with invalid value", "lt=abc", true},
		{"lte with invalid value", "lte=abc", true},
		{"min with invalid value", "min=abc", true},
		{"max with invalid value", "max=abc", true},
		{"len with invalid value", "len=abc", true},
		{"len with zero", "len=0", true},
		{"len with negative value", "len=-1", true},
		{"len with valid value", "len=5", false},
		{"eqfield with empty value", "eqfield=", true},
		{"required_if without expected value", "required_if=OtherField", true},
		{"required_unless without expected value", "required_unless=OtherField", true},
		{"required_if with an odd number of tokens", "required_if=A yes B", true},
		{"required_unless with an odd number of tokens", "required_unless=A yes B", true},
		{"excluded_if without expected value", "excluded_if=OtherField", true},
		{"excluded_if with an odd number of tokens", "excluded_if=A yes B", true},
		{"excluded_unless with an odd number of tokens", "excluded_unless=A yes B", true},
		{"excluded_if with two pairs", "excluded_if=A yes B no", false},
		{"excluded_with with no field", "excluded_with=", true},
		{"unique takes no parameter", "unique=x", true},
		{"unique alone", "unique", false},
		{"required_if with two pairs", "required_if=A yes B no", false},
		{"required_with naming two fields", "required_with=A B", false},
		{"required_with_all naming two fields", "required_with_all=A B", false},
		{"required_without_all naming two fields", "required_without_all=A B", false},
		{"required_with with no field", "required_with=", true},
		{"required needs no value", "required", false},
		{"email needs no value", "email", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, _, err := parseValidateTag(tt.tag, true)
			if (err != nil) != tt.wantErr {
				t.Errorf("parseValidateTag(%q, strict=true) error = %v, wantErr %v", tt.tag, err, tt.wantErr)
			}
			if tt.wantErr && err != nil && !errors.Is(err, ErrInvalidTagFormat) {
				t.Errorf("parseValidateTag(%q, strict=true) error should wrap ErrInvalidTagFormat, got %v", tt.tag, err)
			}
		})
	}
}

// TestSpecializeValidatorJudgesADeferredParameter pins where the eq and ne
// parameter check now lives: a string field takes any parameter as the string
// to compare, and a numeric field requires a number — refused in strict mode,
// dropped in non-strict mode.
func TestSpecializeValidatorJudgesADeferredParameter(t *testing.T) {
	t.Parallel()

	t.Run("a string field takes eq=abc as the string to compare", func(t *testing.T) {
		t.Parallel()
		v, err := specializeValidator(&pendingEqualityValidator{tag: equalTagValue, param: "abc"}, true, true)
		if err != nil {
			t.Fatalf("specializeValidator() error = %v", err)
		}
		if msg := v.Validate("abc"); msg != "" {
			t.Errorf("Validate(\"abc\") = %q, want a pass", msg)
		}
		if msg := v.Validate("other"); msg == "" {
			t.Error("Validate(\"other\") should fail against eq=abc")
		}
	})

	t.Run("a numeric field refuses eq=abc in strict mode", func(t *testing.T) {
		t.Parallel()
		_, err := specializeValidator(&pendingEqualityValidator{tag: equalTagValue, param: "abc"}, false, true)
		if !errors.Is(err, ErrInvalidTagFormat) {
			t.Errorf("error = %v, want ErrInvalidTagFormat", err)
		}
	})

	t.Run("a numeric field takes eq=10 as the number to compare", func(t *testing.T) {
		t.Parallel()
		v, err := specializeValidator(&pendingEqualityValidator{tag: notEqualTagValue, param: "10"}, false, true)
		if err != nil {
			t.Fatalf("specializeValidator() error = %v", err)
		}
		if msg := v.Validate("10.0"); msg == "" {
			t.Error("Validate(\"10.0\") should fail against ne=10: the quantity is equal")
		}
	})
}

func TestStrictTagParsing_PrepTag(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		tag     string
		wantErr bool
	}{
		{"truncate with valid number", "truncate=5", false},
		{"truncate with invalid value", "truncate=abc", true},
		{"truncate with zero", "truncate=0", true},
		{"coerce with valid value", "coerce=int", false},
		{"coerce with invalid value", "coerce=string", true},
		{"replace with valid format", "replace=a:b", false},
		{"replace without colon", "replace=nocolon", true},
		{"trim needs no value", "trim", false},
		{"pad_left with valid format", "pad_left=5:0", false},
		{"pad_left with invalid length", "pad_left=abc:0", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, err := parsePrepTag(tt.tag, true)
			if (err != nil) != tt.wantErr {
				t.Errorf("parsePrepTag(%q, strict=true) error = %v, wantErr %v", tt.tag, err, tt.wantErr)
			}
			if tt.wantErr && err != nil && !errors.Is(err, ErrInvalidTagFormat) {
				t.Errorf("parsePrepTag(%q, strict=true) error should wrap ErrInvalidTagFormat, got %v", tt.tag, err)
			}
		})
	}
}

func TestStrictTagParsing_NonStrictIgnoresInvalidArgs(t *testing.T) {
	t.Parallel()

	t.Run("eq=abc lands on a numeric field and is silently dropped in non-strict mode", func(t *testing.T) {
		t.Parallel()
		vals, _, err := parseValidateTag("eq=abc", false)
		if err != nil {
			t.Errorf("expected no error in non-strict mode, got %v", err)
		}
		if len(vals) != 1 {
			t.Fatalf("expected 1 deferred validator, got %d", len(vals))
		}
		specialized, err := specializeValidator(vals[0], false, false)
		if err != nil {
			t.Errorf("expected no error in non-strict mode, got %v", err)
		}
		if specialized != nil {
			t.Errorf("expected the invalid arg to be dropped, got %T", specialized)
		}
	})

	t.Run("truncate=abc is silently ignored in non-strict mode", func(t *testing.T) {
		t.Parallel()
		preps, err := parsePrepTag("truncate=abc", false)
		if err != nil {
			t.Errorf("expected no error in non-strict mode, got %v", err)
		}
		if len(preps) != 0 {
			t.Errorf("expected 0 preprocessors (invalid arg ignored), got %d", len(preps))
		}
	})

	t.Run("len=0 is silently ignored in non-strict mode", func(t *testing.T) {
		t.Parallel()
		vals, _, err := parseValidateTag("len=0", false)
		if err != nil {
			t.Errorf("expected no error in non-strict mode, got %v", err)
		}
		if len(vals) != 0 {
			t.Errorf("expected 0 validators (invalid arg ignored), got %d", len(vals))
		}
	})

	t.Run("eqfield= is silently ignored in non-strict mode", func(t *testing.T) {
		t.Parallel()
		_, crossVals, err := parseValidateTag("eqfield=", false)
		if err != nil {
			t.Errorf("expected no error in non-strict mode, got %v", err)
		}
		if len(crossVals) != 0 {
			t.Errorf("expected 0 cross-field validators (invalid arg ignored), got %d", len(crossVals))
		}
	})

	t.Run("required_if without expected value is silently ignored in non-strict mode", func(t *testing.T) {
		t.Parallel()
		_, crossVals, err := parseValidateTag("required_if=OtherField", false)
		if err != nil {
			t.Errorf("expected no error in non-strict mode, got %v", err)
		}
		if len(crossVals) != 0 {
			t.Errorf("expected 0 cross-field validators (invalid arg ignored), got %d", len(crossVals))
		}
	})

	t.Run("required_if with an odd number of tokens is silently ignored in non-strict mode", func(t *testing.T) {
		t.Parallel()
		_, crossVals, err := parseValidateTag("required_if=A yes B", false)
		if err != nil {
			t.Errorf("expected no error in non-strict mode, got %v", err)
		}
		if len(crossVals) != 0 {
			t.Errorf("expected 0 cross-field validators (invalid arg ignored), got %d", len(crossVals))
		}
	})

	t.Run("required_with with no field is silently ignored in non-strict mode", func(t *testing.T) {
		t.Parallel()
		_, crossVals, err := parseValidateTag("required_with=", false)
		if err != nil {
			t.Errorf("expected no error in non-strict mode, got %v", err)
		}
		if len(crossVals) != 0 {
			t.Errorf("expected 0 cross-field validators (invalid arg ignored), got %d", len(crossVals))
		}
	})

	t.Run("excluded_if with an odd number of tokens is silently ignored in non-strict mode", func(t *testing.T) {
		t.Parallel()
		_, crossVals, err := parseValidateTag("excluded_if=A yes B", false)
		if err != nil {
			t.Errorf("expected no error in non-strict mode, got %v", err)
		}
		if len(crossVals) != 0 {
			t.Errorf("expected 0 cross-field validators (invalid arg ignored), got %d", len(crossVals))
		}
	})
}

// The four network spellings the dialect documents parse on their own and
// beside required, and required keeps its meaning: an empty cell passes the
// format tag and is still reported by required.
func TestNetworkTagsParseAloneAndWithRequired(t *testing.T) {
	t.Parallel()

	tagsParseAloneAndWithRequired(t, ipTagValue, ipv4TagValue, ipv6TagValue, portTagValue)
}

// The format and checksum tags parse the same way.
func TestFormatAndChecksumTagsParseAloneAndWithRequired(t *testing.T) {
	t.Parallel()

	tagsParseAloneAndWithRequired(t,
		jsonTagValue, timezoneTagValue, semverTagValue,
		base32TagValue, base64TagValue, base64URLTagValue, base64RawURLTagValue,
		creditCardTagValue, luhnChecksumTagValue,
		isbnTagValue, isbn10TagValue, isbn13TagValue, issnTagValue,
		md5TagValue, sha256TagValue, sha384TagValue, sha512TagValue,
	)
}

// unique marks the field rather than building a validator, so the mark has to
// reach fieldInfo and the tag has to be readable beside the ordinary ones.
func TestUniqueMarksTheField(t *testing.T) {
	t.Parallel()

	type record struct {
		Code  string `validate:"required,unique,len=2"`
		Plain string
	}

	info, err := parseStructType(reflect.TypeOf(record{}), true)
	if err != nil {
		t.Fatalf("parseStructType() error = %v", err)
	}
	if !info.Fields[0].Unique {
		t.Error("Fields[0].Unique = false, want the unique tag to mark the field")
	}
	if got := len(info.Fields[0].Validators); got != 2 {
		t.Errorf("Fields[0].Validators = %d, want 2; unique must leave no validator behind", got)
	}
	if info.Fields[1].Unique {
		t.Error("Fields[1].Unique = true, want an untagged field to be unmarked")
	}
}

// The country and currency code tags parse the same way.
func TestCodeTagsParseAloneAndWithRequired(t *testing.T) {
	t.Parallel()

	tagsParseAloneAndWithRequired(t,
		iso3166Alpha2TagValue, iso3166Alpha3TagValue, iso3166NumericTagValue,
		countryCodeTagValue, iso4217TagValue, iso4217NumericTagValue,
	)
}

// The last dialect spellings parse the same way.
func TestAliasAndLabelTagsParseAloneAndWithRequired(t *testing.T) {
	t.Parallel()

	tagsParseAloneAndWithRequired(t,
		isColorTagValue, dnsRFC1035LabelTagValue,
		uuidRFC4122TagValue, uuid3RFC4122TagValue, uuid4RFC4122TagValue, uuid5RFC4122TagValue,
	)
}

// tagsParseAloneAndWithRequired checks that each tag builds one validator on
// its own and two beside required, and that required still reports an empty
// cell the format tag passes.
func tagsParseAloneAndWithRequired(t *testing.T, tags ...string) {
	t.Helper()

	for _, tag := range tags {
		t.Run(tag, func(t *testing.T) {
			t.Parallel()
			alone, _, err := parseValidateTag(tag, true)
			if err != nil {
				t.Fatalf("parseValidateTag(%q) error = %v", tag, err)
			}
			if len(alone) != 1 {
				t.Fatalf("parseValidateTag(%q) validators = %d, want 1", tag, len(alone))
			}
			combined, _, err := parseValidateTag("required,"+tag, true)
			if err != nil {
				t.Fatalf("parseValidateTag(%q) error = %v", "required,"+tag, err)
			}
			if len(combined) != 2 {
				t.Fatalf("parseValidateTag(%q) validators = %d, want 2", "required,"+tag, len(combined))
			}
			if tag, msg := combined.Validate(""); msg == "" || tag != requiredTagValue {
				t.Errorf("Validate(\"\") reported %q/%q, want the required tag to fail", tag, msg)
			}
		})
	}
}

func TestWithStrictTagParsing_Processor(t *testing.T) {
	t.Parallel()

	// The field is numeric: eq=abc on a string field is a legal comparison
	// against the string "abc", so only a numeric field makes the parameter
	// invalid.
	type InvalidTag struct {
		Value int `validate:"eq=abc"`
	}

	t.Run("strict mode returns error for invalid tag arguments", func(t *testing.T) {
		t.Parallel()
		csvData := "value\ntest\n"
		var records []InvalidTag
		processor := NewProcessor(FileTypeCSV, WithStrictTagParsing())
		_, _, err := processor.Process(strings.NewReader(csvData), &records)
		if err == nil {
			t.Error("expected error with strict tag parsing for eq=abc")
		}
		if !errors.Is(err, ErrInvalidTagFormat) {
			t.Errorf("expected ErrInvalidTagFormat, got %v", err)
		}
	})

	t.Run("non-strict mode ignores invalid tag arguments", func(t *testing.T) {
		t.Parallel()
		csvData := "value\ntest\n"
		var records []InvalidTag
		processor := NewProcessor(FileTypeCSV)
		_, _, err := processor.Process(strings.NewReader(csvData), &records)
		if err != nil {
			t.Errorf("expected no error in non-strict mode, got %v", err)
		}
	})
}

// TestParameterizedValidatorsNeedTheirParameter covers the tags that cannot
// work without a parameter. Dropping the tag when the parameter is missing
// leaves the column unchecked, and a datetime tag written without a layout is
// the mistake most likely to be made, so strict mode has to report it.
func TestParameterizedValidatorsNeedTheirParameter(t *testing.T) {
	t.Parallel()

	// Each tag is tried in both spellings a caller writes it in: the bare name,
	// and the name with an empty parameter after the equals sign.
	tags := []string{
		"startswith", "startsnotwith", "endswith", "endsnotwith",
		"contains", "containsany", "containsrune",
		"excludes", "excludesall", "excludesrune",
		"eq_ignore_case", "ne_ignore_case", "datetime",
	}
	for _, tag := range tags {
		for _, spelling := range []string{tag, tag + "="} {
			t.Run("strict refuses "+spelling, func(t *testing.T) {
				t.Parallel()
				_, _, err := parseValidateTag(spelling, true)
				if err == nil {
					t.Fatalf("parseValidateTag(%q, strict=true) = nil, want an error", spelling)
				}
				if !errors.Is(err, ErrInvalidTagFormat) {
					t.Fatalf("parseValidateTag(%q, strict=true) error = %v, want ErrInvalidTagFormat", spelling, err)
				}
			})
			t.Run("non-strict drops "+spelling, func(t *testing.T) {
				t.Parallel()
				validators, _, err := parseValidateTag(spelling, false)
				if err != nil {
					t.Fatalf("parseValidateTag(%q, strict=false) error = %v", spelling, err)
				}
				if len(validators) != 0 {
					t.Fatalf("parseValidateTag(%q, strict=false) built %d validators, want none", spelling, len(validators))
				}
			})
		}
	}

	// The tags whose empty parameter means something keep meaning it. eq and ne
	// compare against the empty string, and the substring tags find it in every
	// value, which is what the dialect answers for them.
	for _, tag := range []string{"eq=", "ne=", "contains=x", "datetime=2006-01-02"} {
		t.Run("strict accepts "+tag, func(t *testing.T) {
			t.Parallel()
			if _, _, err := parseValidateTag(tag, true); err != nil {
				t.Fatalf("parseValidateTag(%q, strict=true) error = %v", tag, err)
			}
		})
	}
}

// TestValidateTagSkipMarker covers the dialect's "-", which asks for no
// validation of the field it is on.
func TestValidateTagSkipMarker(t *testing.T) {
	t.Parallel()

	for _, strict := range []bool{false, true} {
		t.Run("the whole tag is a skip", func(t *testing.T) {
			t.Parallel()
			validators, crossField, err := parseValidateTag("-", strict)
			if err != nil {
				t.Fatalf("parseValidateTag(\"-\", strict=%v) error = %v", strict, err)
			}
			if len(validators) != 0 || len(crossField) != 0 {
				t.Fatalf("parseValidateTag(\"-\", strict=%v) built %d validators and %d cross-field validators, want none",
					strict, len(validators), len(crossField))
			}
		})
		t.Run("a skip among other tags is still unknown", func(t *testing.T) {
			t.Parallel()
			if _, _, err := parseValidateTag("-,required", strict); !errors.Is(err, ErrInvalidTagFormat) {
				t.Fatalf("parseValidateTag(\"-,required\", strict=%v) error = %v, want ErrInvalidTagFormat", strict, err)
			}
		})
	}
}

// TestPrepTagsAreDocumented holds the package documentation against the tags
// the parser accepts. The list in doc.go was written when the package had six
// preprocessors and did not grow with them, so a caller had nowhere to read the
// spelling of the rest.
func TestPrepTagsAreDocumented(t *testing.T) {
	t.Parallel()

	doc, err := os.ReadFile("doc.go")
	if err != nil {
		t.Fatalf("read doc.go: %v", err)
	}
	text := string(doc)
	for _, tag := range prepTagNames() {
		if !strings.Contains(text, tag) {
			t.Errorf("prep tag %q is accepted by the parser but not named in doc.go", tag)
		}
	}
}

// TestATagParameterCanHoldAColon covers the two characters that separate one
// tag from the next and a tag's parameters from each other. A colon is ordinary
// inside a regular expression -- a non-capturing group, an inline flag, a URL
// scheme, a clock time -- and splitting at the first one left a pattern the
// caller never wrote: "regex_replace=https?://:scheme-" read the pattern as
// "https?" and the replacement as "//:scheme-", and turned a URL into nonsense
// with no error anywhere.
func TestATagParameterCanHoldAColon(t *testing.T) {
	t.Parallel()

	type urlRow struct {
		V string `name:"v" prep:"regex_replace=https?\\://:scheme-"`
	}
	type groupRow struct {
		V string `name:"v" prep:"regex_replace=(?\\:foo|bar)+:X"`
	}
	type replacementRow struct {
		V string `name:"v" prep:"regex_replace=(\\d)(\\d):$1\\:$2"`
	}
	type replaceRow struct {
		V string `name:"v" prep:"replace=a\\:b:c"`
	}
	type commaRow struct {
		V string `name:"v" prep:"prefix=a\\,b"`
	}
	type defaultRow struct {
		V string `name:"v" prep:"default=x\\,y"`
	}
	type padRow struct {
		V string `name:"v" prep:"pad_left=3:\\:"`
	}
	// The separators still separate when no backslash stands in front of them.
	type unescapedRow struct {
		V string `name:"v" prep:"replace=a:b,uppercase"`
	}
	// A backslash before anything else is itself, so a pattern keeps its \d.
	type digitRow struct {
		V string `name:"v" prep:"regex_replace=\\d+:N"`
	}

	tests := []struct {
		name string
		rows any
		in   string
		want string
	}{
		{"a URL scheme in a pattern", &[]urlRow{}, "https://x.example", "scheme-x.example"},
		{"a non-capturing group in a pattern", &[]groupRow{}, "foobar", "X"},
		{"a colon in a replacement", &[]replacementRow{}, "12", "1:2"},
		{"a colon in the text replace looks for", &[]replaceRow{}, "a:b", "c"},
		{"a comma in a prefix", &[]commaRow{}, "z", `"a,bz"`},
		{"a comma in a default", &[]defaultRow{}, `""`, `"x,y"`},
		{"a colon as the padding character", &[]padRow{}, "7", "::7"},
		{"an unescaped separator still separates", &[]unescapedRow{}, "a", "B"},
		{"a backslash before anything else is itself", &[]digitRow{}, "12ab", "Nab"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			processor := NewProcessor(FileTypeCSV, WithStrictTagParsing())
			reader, result, err := processor.Process(strings.NewReader("v\n"+tt.in+"\n"), tt.rows)
			if err != nil {
				t.Fatalf("Process: %v", err)
			}
			if len(result.Errors) != 0 {
				t.Fatalf("Process reported %v", result.Errors)
			}
			out, err := io.ReadAll(reader)
			if err != nil {
				t.Fatalf("reading the output: %v", err)
			}
			if got := strings.TrimSpace(string(out)); got != "v\n"+tt.want {
				t.Errorf("output = %q, want %q", got, "v\n"+tt.want)
			}
		})
	}
}

func TestSplitUnescapedAndUnescapeTagText(t *testing.T) {
	t.Parallel()

	t.Run("splitting", func(t *testing.T) {
		t.Parallel()

		for _, tt := range []struct {
			in   string
			want []string
		}{
			{"a,b", []string{"a", "b"}},
			{`a\,b`, []string{`a\,b`}},
			{`a\,b,c`, []string{`a\,b`, "c"}},
			{",", []string{"", ""}},
			{`a\`, []string{`a\`}},
			{"", []string{""}},
		} {
			if got := splitUnescaped(tt.in, ','); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("splitUnescaped(%q) = %q, want %q", tt.in, got, tt.want)
			}
		}
	})

	t.Run("unescaping", func(t *testing.T) {
		t.Parallel()

		for _, tt := range []struct {
			in   string
			want string
		}{
			{`a\:b`, "a:b"},
			{`a\,b`, "a,b"},
			{`\d+`, `\d+`},
			{`a\\b`, `a\\b`},
			{`a\`, `a\`},
			{"plain", "plain"},
		} {
			if got := unescapeTagText(tt.in); got != tt.want {
				t.Errorf("unescapeTagText(%q) = %q, want %q", tt.in, got, tt.want)
			}
		}
	})
}
