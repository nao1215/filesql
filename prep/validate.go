//nolint:goconst // validator tag/value matching intentionally uses literal tokens.
package prep

import (
	"encoding/base64"
	"fmt"
	"net"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"
)

// Regex patterns for validation
const (
	uuidRegexPattern = `^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`
	// RFC 2397 gives a data URI as data:[<mediatype>][;base64],<data>, where
	// the media type may carry parameters and may be omitted entirely. The
	// payload is checked again by decoding it, so the class here only has to
	// find where it starts.
	dataURIRegexPattern = `^data:(?:[\w.+-]+/[\w.+-]+)?(?:;[\w.+-]+=[^;,]*)*;base64,[A-Za-z0-9+/]+={0,2}$`
	emailRegexPattern   = `^[A-Za-z0-9_%+\-]+(?:\.[A-Za-z0-9_%+\-]+)*@(?:[A-Za-z0-9](?:[A-Za-z0-9\-]{0,61}[A-Za-z0-9])?\.)+[A-Za-z]{2,}$`
	// numeric accepts an optionally signed decimal, and number accepts digits
	// alone, matching the go-playground/validator dialect prep documents.
	numericRegexPattern = `^[-+]?[0-9]+(\.[0-9]+)?$`
	numberRegexPattern  = `^[0-9]+$`
	fileScheme          = "file"

	// E.164 phone number pattern
	e164RegexPattern = `^\+[1-9][0-9]{7,14}$`
	// Latitude pattern: -90 to 90
	latitudeRegexPattern = `^[-+]?([1-8]?\d(\.\d+)?|90(\.0+)?)$`
	// Longitude pattern: -180 to 180
	longitudeRegexPattern = `^[-+]?(180(\.0+)?|((1[0-7]\d)|([1-9]?\d))(\.\d+)?)$`
	// UUID version 3 pattern
	uuid3RegexPattern = `^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-3[0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}$`
	// UUID version 4 pattern
	uuid4RegexPattern = `^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-4[0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}$`
	// UUID version 5 pattern
	uuid5RegexPattern = `^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-5[0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}$`
	// ULID pattern (26 characters, Crockford's base32)
	ulidRegexPattern = `(?i)^[0-7][0-9A-HJKMNP-TV-Z]{25}$`
	// Hexadecimal pattern
	hexadecimalRegexPattern = `^(0[xX])?[0-9a-fA-F]+$`
	// Hex color pattern (#RGB, #RGBA, #RRGGBB, #RRGGBBAA)
	hexColorRegexPattern = `^#(?:[0-9a-fA-F]{3}|[0-9a-fA-F]{4}|[0-9a-fA-F]{6}|[0-9a-fA-F]{8})$`
	// RGB component patterns
	rgbComponentRegexPattern        = `(?:0|[1-9]\d?|1\d\d|2[0-4]\d|25[0-5])`
	rgbPercentComponentRegexPattern = `(?:0|[1-9]\d?|100)%`
	alphaComponentRegexPattern      = `(?:0(?:\.\d+)?|1(?:\.0+)?)`
	hueComponentRegexPattern        = `(?:0|[1-9]\d?|[12]\d\d|3[0-5]\d|360)`
	hslPercentComponentRegexPattern = `(?:0|[1-9]\d?|100)%`
	// RGB color pattern
	rgbRegexPattern = `^rgb\(\s*(?:` +
		rgbComponentRegexPattern + `\s*,\s*` + rgbComponentRegexPattern + `\s*,\s*` + rgbComponentRegexPattern +
		`|` + rgbPercentComponentRegexPattern + `\s*,\s*` + rgbPercentComponentRegexPattern + `\s*,\s*` + rgbPercentComponentRegexPattern +
		`)\s*\)$`
	// RGBA color pattern
	rgbaRegexPattern = `^rgba\(\s*(?:` +
		rgbComponentRegexPattern + `\s*,\s*` + rgbComponentRegexPattern + `\s*,\s*` + rgbComponentRegexPattern +
		`|` + rgbPercentComponentRegexPattern + `\s*,\s*` + rgbPercentComponentRegexPattern + `\s*,\s*` + rgbPercentComponentRegexPattern +
		`)\s*,\s*` + alphaComponentRegexPattern + `\s*\)$`
	// HSL color pattern
	hslRegexPattern = `^hsl\(\s*` + hueComponentRegexPattern + `\s*,\s*` + hslPercentComponentRegexPattern + `\s*,\s*` + hslPercentComponentRegexPattern + `\s*\)$`
	// HSLA color pattern
	hslaRegexPattern = `^hsla\(\s*` + hueComponentRegexPattern + `\s*,\s*` + hslPercentComponentRegexPattern + `\s*,\s*` + hslPercentComponentRegexPattern + `\s*,\s*` + alphaComponentRegexPattern + `\s*\)$`
)

// Common error messages (to avoid goconst warnings)
const (
	errMsgValidNumber       = "value must be a valid number"
	errMsgValidURL          = "value must be a valid URL"
	errMsgValidDataURI      = "value must be a valid data URI"
	errMsgValidFQDN         = "value must be a valid FQDN"
	errMsgValidHostnamePort = "value must be a valid hostname:port"
)

// Pre-compiled regexes
var (
	uuidRegex                 = regexp.MustCompile(uuidRegexPattern)
	dataURIRegex              = regexp.MustCompile(dataURIRegexPattern)
	emailRegex                = regexp.MustCompile(emailRegexPattern)
	numericRegex              = regexp.MustCompile(numericRegexPattern)
	numberRegex               = regexp.MustCompile(numberRegexPattern)
	urlEncodedRegex           = regexp.MustCompile(`^(?:[^%]|%[0-9A-Fa-f]{2})*$`)
	fqdnLabelRegex            = regexp.MustCompile(`^[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?$`)
	fqdnTLDRegex              = regexp.MustCompile(`^[a-zA-Z][a-zA-Z0-9]{0,62}$`)
	hostnameRFC952LabelRegex  = regexp.MustCompile(`^[A-Za-z](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?$`)
	hostnameRFC1123LabelRegex = regexp.MustCompile(`^[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?$`)

	// Additional regex patterns for new validators
	e164Regex        = regexp.MustCompile(e164RegexPattern)
	latitudeRegex    = regexp.MustCompile(latitudeRegexPattern)
	longitudeRegex   = regexp.MustCompile(longitudeRegexPattern)
	uuid3Regex       = regexp.MustCompile(uuid3RegexPattern)
	uuid4Regex       = regexp.MustCompile(uuid4RegexPattern)
	uuid5Regex       = regexp.MustCompile(uuid5RegexPattern)
	ulidRegex        = regexp.MustCompile(ulidRegexPattern)
	hexadecimalRegex = regexp.MustCompile(hexadecimalRegexPattern)
	hexColorRegex    = regexp.MustCompile(hexColorRegexPattern)
	rgbRegex         = regexp.MustCompile(rgbRegexPattern)
	rgbaRegex        = regexp.MustCompile(rgbaRegexPattern)
	hslRegex         = regexp.MustCompile(hslRegexPattern)
	hslaRegex        = regexp.MustCompile(hslaRegexPattern)
)

// validator defines the interface for validating values
type validator interface {
	// Validate checks if the value is valid and returns an error message if not
	// Returns empty string if validation passes
	Validate(value string) string
	// Name returns the name of the validator for error reporting
	Name() string
}

// validators is a slice of validator
type validators []validator

// Validate applies all validators and returns the first error message.
// Returns empty strings if all validations pass.
//
// An empty value passes every validator except required: an empty cell is how
// CSV spells a missing value, so a column is optional unless required says
// otherwise. A caller who needs both presence and format writes required
// alongside the format tag. This is where this package deliberately leaves the
// go-playground dialect, which fails most validators on an empty value;
// omitempty is still accepted and is a no-op under this rule.
func (vs validators) Validate(value string) (string, string) {
	if value == "" {
		for _, v := range vs {
			if v.Name() == requiredTagValue {
				if msg := v.Validate(value); msg != "" {
					return requiredTagValue, msg
				}
			}
		}
		return "", ""
	}
	for _, v := range vs {
		if v.Name() == omitemptyTagValue {
			continue
		}
		if msg := v.Validate(value); msg != "" {
			return v.Name(), msg
		}
	}
	return "", ""
}

// omitemptyValidator is a sentinel validator that signals empty values should be skipped.
// It does not perform validation itself; its presence is detected by validators.Validate().
type omitemptyValidator struct{}

// Validate always returns empty (the omitempty logic is handled by validators.Validate)
func (v *omitemptyValidator) Validate(_ string) string {
	return ""
}

// Name returns the validator name
func (v *omitemptyValidator) Name() string {
	return omitemptyTagValue
}

// =============================================================================
// Basic Validators
// =============================================================================

// requiredValidator validates that a value is not empty
type requiredValidator struct{}

// newRequiredValidator creates a new required validator
func newRequiredValidator() *requiredValidator {
	return &requiredValidator{}
}

// Validate checks if the value is not empty
func (v *requiredValidator) Validate(value string) string {
	if value == "" {
		return "value is required"
	}
	return ""
}

// Name returns the validator name
func (v *requiredValidator) Name() string {
	return requiredTagValue
}

// booleanValidator validates that a value is a boolean.
//
// The accepted spellings are exactly what strconv.ParseBool accepts: that is
// what setFieldValue uses to fill a bool struct field, and how the
// go-playground dialect this package follows defines boolean.
type booleanValidator struct{}

// newBooleanValidator creates a new boolean validator
func newBooleanValidator() *booleanValidator {
	return &booleanValidator{}
}

// Validate checks if the value is a valid boolean
func (v *booleanValidator) Validate(value string) string {
	if _, err := strconv.ParseBool(value); err != nil {
		return "value must be a boolean (1, t, T, TRUE, true, True, 0, f, F, FALSE, false, or False)"
	}
	return ""
}

// Name returns the validator name
func (v *booleanValidator) Name() string {
	return booleanTagValue
}

// alphaValidator validates that a value contains only ASCII alphabetic characters
type alphaValidator struct{}

// newAlphaValidator creates a new alpha validator
func newAlphaValidator() *alphaValidator {
	return &alphaValidator{}
}

// Validate checks if the value contains only alphabetic characters
func (v *alphaValidator) Validate(value string) string {
	for _, r := range value {
		if !isAlpha(r) {
			return "value must contain only alphabetic characters"
		}
	}
	return ""
}

// Name returns the validator name
func (v *alphaValidator) Name() string {
	return alphaTagValue
}

// isAlpha returns true if the rune is an ASCII alphabetic character
func isAlpha(r rune) bool {
	return (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z')
}

// alphaUnicodeValidator validates that a value contains only unicode letters
type alphaUnicodeValidator struct{}

// newAlphaUnicodeValidator creates a new alphaUnicode validator
func newAlphaUnicodeValidator() *alphaUnicodeValidator {
	return &alphaUnicodeValidator{}
}

// Validate checks if the value contains only unicode letters
func (v *alphaUnicodeValidator) Validate(value string) string {
	for _, r := range value {
		if !unicode.IsLetter(r) {
			return "value must contain only unicode letters"
		}
	}
	return ""
}

// Name returns the validator name
func (v *alphaUnicodeValidator) Name() string {
	return alphaUnicodeTagValue
}

// alphaSpaceValidator validates that a value contains only alphabetic characters or spaces
type alphaSpaceValidator struct{}

// newAlphaSpaceValidator creates a new alphaSpace validator
func newAlphaSpaceValidator() *alphaSpaceValidator {
	return &alphaSpaceValidator{}
}

// Validate checks if the value contains only alphabetic characters or spaces
func (v *alphaSpaceValidator) Validate(value string) string {
	for _, r := range value {
		if !isAlpha(r) && r != ' ' {
			return "value must contain only alphabetic characters or spaces"
		}
	}
	return ""
}

// Name returns the validator name
func (v *alphaSpaceValidator) Name() string {
	return alphaSpaceTagValue
}

// numericValidator validates that a value is an optionally signed decimal,
// which is what numeric means in the go-playground/validator dialect.
type numericValidator struct{}

// newNumericValidator creates a new numeric validator
func newNumericValidator() *numericValidator {
	return &numericValidator{}
}

// Validate checks if the value is an optionally signed decimal
func (v *numericValidator) Validate(value string) string {
	if value == "" {
		return ""
	}
	if !numericRegex.MatchString(value) {
		return "value must be numeric"
	}
	return ""
}

// Name returns the validator name
func (v *numericValidator) Name() string {
	return numericTagValue
}

// numberValidator validates that a value is digits alone, which is what number
// means in the go-playground/validator dialect.
type numberValidator struct{}

// newNumberValidator creates a new number validator
func newNumberValidator() *numberValidator {
	return &numberValidator{}
}

// Validate checks if the value is a valid number
func (v *numberValidator) Validate(value string) string {
	if !numberRegex.MatchString(value) {
		return errMsgValidNumber
	}
	return ""
}

// Name returns the validator name
func (v *numberValidator) Name() string {
	return numberTagValue
}

// alphanumericValidator validates that a value contains only ASCII alphanumeric characters
type alphanumericValidator struct{}

// newAlphanumericValidator creates a new alphanumeric validator
func newAlphanumericValidator() *alphanumericValidator {
	return &alphanumericValidator{}
}

// Validate checks if the value contains only alphanumeric characters
func (v *alphanumericValidator) Validate(value string) string {
	for _, r := range value {
		if !isAlpha(r) && !isNumeric(r) {
			return "value must contain only alphanumeric characters"
		}
	}
	return ""
}

// Name returns the validator name
func (v *alphanumericValidator) Name() string {
	return alphanumericTagValue
}

// isNumeric returns true if the rune is a numeric character
func isNumeric(r rune) bool {
	return r >= '0' && r <= '9'
}

// alphanumericUnicodeValidator validates unicode alphanumeric strings
type alphanumericUnicodeValidator struct{}

// newAlphanumericUnicodeValidator creates a new alphanumericUnicode validator
func newAlphanumericUnicodeValidator() *alphanumericUnicodeValidator {
	return &alphanumericUnicodeValidator{}
}

// Validate checks if the value contains only unicode letters or numbers. The
// dialect's alphanumunicode is ^[\p{L}\p{N}]+$, and \p{N} covers every Unicode
// number category, so unicode.IsNumber is used rather than unicode.IsDigit,
// which matches decimal digits alone.
func (v *alphanumericUnicodeValidator) Validate(value string) string {
	for _, r := range value {
		if !unicode.IsLetter(r) && !unicode.IsNumber(r) {
			return "value must contain only unicode letters or digits"
		}
	}
	return ""
}

// Name returns the validator name
func (v *alphanumericUnicodeValidator) Name() string {
	return alphanumericUnicodeTagValue
}

// =============================================================================
// Comparison Validators
// =============================================================================
//
// The comparison tags follow the field they land on, which is what the
// go-playground dialect this package documents means by them: on a string
// field, eq and ne compare the string itself and gt, gte, lt, lte, min and
// max compare the character count, while on any other field all of them
// compare the numeric value and len means the value equals the parameter.
// parseStructType specializes each validator once the field's kind is known.

// pendingEqualityValidator carries an eq or ne tag whose meaning depends on
// the field it lands on: a string field compares the string itself, any other
// field compares the number, so the parameter cannot be judged before the
// field's kind is known. parseStructType replaces it; it never validates.
type pendingEqualityValidator struct {
	tag   string // equalTagValue or notEqualTagValue
	param string
}

// Validate reports the bug of running unspecialized, so a path that forgets
// specialization fails loudly instead of validating nothing.
func (v *pendingEqualityValidator) Validate(_ string) string {
	return "internal: " + v.tag + " was not specialized for its field"
}

// Name returns the validator name
func (v *pendingEqualityValidator) Name() string {
	return v.tag
}

// textEqualValidator validates that a string field's value equals the expected
// text, which is what eq means for a string in the validator dialect this
// package follows.
type textEqualValidator struct {
	expected string
}

// newTextEqualValidator creates an eq validator for a string field
func newTextEqualValidator(expected string) *textEqualValidator {
	return &textEqualValidator{expected: expected}
}

// Validate checks if the value equals the expected text
func (v *textEqualValidator) Validate(value string) string {
	if value != v.expected {
		return "value must equal '" + v.expected + "'"
	}
	return ""
}

// Name returns the validator name
func (v *textEqualValidator) Name() string {
	return equalTagValue
}

// textNotEqualValidator is the negated half of textEqualValidator.
type textNotEqualValidator struct {
	expected string
}

// newTextNotEqualValidator creates a ne validator for a string field
func newTextNotEqualValidator(expected string) *textNotEqualValidator {
	return &textNotEqualValidator{expected: expected}
}

// Validate checks if the value does not equal the expected text
func (v *textNotEqualValidator) Validate(value string) string {
	if value == v.expected {
		return "value must not equal '" + v.expected + "'"
	}
	return ""
}

// Name returns the validator name
func (v *textNotEqualValidator) Name() string {
	return notEqualTagValue
}

// equalValidator validates that a value equals the threshold
type equalValidator struct {
	threshold float64
}

// newEqualValidator creates a new equal validator
func newEqualValidator(threshold float64) *equalValidator {
	return &equalValidator{threshold: threshold}
}

// Validate checks if the value equals the threshold
func (v *equalValidator) Validate(value string) string {
	f, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return errMsgValidNumber
	}
	if f != v.threshold {
		return "value must equal " + strconv.FormatFloat(v.threshold, 'f', -1, 64)
	}
	return ""
}

// Name returns the validator name
func (v *equalValidator) Name() string {
	return equalTagValue
}

// notEqualValidator validates that a value does not equal the threshold
type notEqualValidator struct {
	threshold float64
}

// newNotEqualValidator creates a new not equal validator
func newNotEqualValidator(threshold float64) *notEqualValidator {
	return &notEqualValidator{threshold: threshold}
}

// Validate checks if the value does not equal the threshold
func (v *notEqualValidator) Validate(value string) string {
	f, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return errMsgValidNumber
	}
	if f == v.threshold {
		return "value must not equal " + strconv.FormatFloat(v.threshold, 'f', -1, 64)
	}
	return ""
}

// Name returns the validator name
func (v *notEqualValidator) Name() string {
	return notEqualTagValue
}

// greaterThanValidator validates that a value is greater than the threshold.
// It measures what minValidator measures; see there.
type greaterThanValidator struct {
	threshold      float64
	measuresLength bool
}

// newGreaterThanValidator creates a new greater than validator
func newGreaterThanValidator(threshold float64) *greaterThanValidator {
	return &greaterThanValidator{threshold: threshold}
}

// Validate checks if the value is greater than the threshold
func (v *greaterThanValidator) Validate(value string) string {
	if v.measuresLength {
		if float64(utf8.RuneCountInString(value)) <= v.threshold {
			return "value must have more than " + strconv.FormatFloat(v.threshold, 'f', -1, 64) + " characters"
		}
		return ""
	}
	f, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return errMsgValidNumber
	}
	if f <= v.threshold {
		return "value must be greater than " + strconv.FormatFloat(v.threshold, 'f', -1, 64)
	}
	return ""
}

// Name returns the validator name
func (v *greaterThanValidator) Name() string {
	return greaterThanTagValue
}

// greaterThanEqualValidator validates that a value is greater than or equal to
// the threshold. It measures what minValidator measures; see there.
type greaterThanEqualValidator struct {
	threshold      float64
	measuresLength bool
}

// newGreaterThanEqualValidator creates a new greater than or equal validator
func newGreaterThanEqualValidator(threshold float64) *greaterThanEqualValidator {
	return &greaterThanEqualValidator{threshold: threshold}
}

// Validate checks if the value is greater than or equal to the threshold
func (v *greaterThanEqualValidator) Validate(value string) string {
	if v.measuresLength {
		if float64(utf8.RuneCountInString(value)) < v.threshold {
			return "value must have at least " + strconv.FormatFloat(v.threshold, 'f', -1, 64) + " characters"
		}
		return ""
	}
	f, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return errMsgValidNumber
	}
	if f < v.threshold {
		return "value must be greater than or equal to " + strconv.FormatFloat(v.threshold, 'f', -1, 64)
	}
	return ""
}

// Name returns the validator name
func (v *greaterThanEqualValidator) Name() string {
	return greaterThanEqualTagValue
}

// lessThanValidator validates that a value is less than the threshold.
// It measures what minValidator measures; see there.
type lessThanValidator struct {
	threshold      float64
	measuresLength bool
}

// newLessThanValidator creates a new less than validator
func newLessThanValidator(threshold float64) *lessThanValidator {
	return &lessThanValidator{threshold: threshold}
}

// Validate checks if the value is less than the threshold
func (v *lessThanValidator) Validate(value string) string {
	if v.measuresLength {
		if float64(utf8.RuneCountInString(value)) >= v.threshold {
			return "value must have fewer than " + strconv.FormatFloat(v.threshold, 'f', -1, 64) + " characters"
		}
		return ""
	}
	f, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return errMsgValidNumber
	}
	if f >= v.threshold {
		return "value must be less than " + strconv.FormatFloat(v.threshold, 'f', -1, 64)
	}
	return ""
}

// Name returns the validator name
func (v *lessThanValidator) Name() string {
	return lessThanTagValue
}

// lessThanEqualValidator validates that a value is less than or equal to the
// threshold. It measures what minValidator measures; see there.
type lessThanEqualValidator struct {
	threshold      float64
	measuresLength bool
}

// newLessThanEqualValidator creates a new less than or equal validator
func newLessThanEqualValidator(threshold float64) *lessThanEqualValidator {
	return &lessThanEqualValidator{threshold: threshold}
}

// Validate checks if the value is less than or equal to the threshold
func (v *lessThanEqualValidator) Validate(value string) string {
	if v.measuresLength {
		if float64(utf8.RuneCountInString(value)) > v.threshold {
			return "value must have at most " + strconv.FormatFloat(v.threshold, 'f', -1, 64) + " characters"
		}
		return ""
	}
	f, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return errMsgValidNumber
	}
	if f > v.threshold {
		return "value must be less than or equal to " + strconv.FormatFloat(v.threshold, 'f', -1, 64)
	}
	return ""
}

// Name returns the validator name
func (v *lessThanEqualValidator) Name() string {
	return lessThanEqualTagValue
}

// minValidator validates that a value is at least the minimum.
//
// What is measured follows the field: a magnitude for a numeric field, and the
// number of characters for a string one, which is what the validator dialect
// this package follows means by min. Measuring a string as a number reported
// every name as "not a valid number", and disagreed with len, which counts
// characters in the same struct.
type minValidator struct {
	threshold      float64
	measuresLength bool
}

// newMinValidator creates a new min validator
func newMinValidator(threshold float64) *minValidator {
	return &minValidator{threshold: threshold}
}

// Validate checks if the value is at least the minimum
func (v *minValidator) Validate(value string) string {
	if v.measuresLength {
		if float64(utf8.RuneCountInString(value)) < v.threshold {
			return "value must have at least " + strconv.FormatFloat(v.threshold, 'f', -1, 64) + " characters"
		}
		return ""
	}
	f, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return errMsgValidNumber
	}
	if f < v.threshold {
		return "value must be at least " + strconv.FormatFloat(v.threshold, 'f', -1, 64)
	}
	return ""
}

// Name returns the validator name
func (v *minValidator) Name() string {
	return minTagValue
}

// maxValidator validates that a value is at most the maximum. It measures what
// minValidator measures; see there.
type maxValidator struct {
	threshold      float64
	measuresLength bool
}

// newMaxValidator creates a new max validator
func newMaxValidator(threshold float64) *maxValidator {
	return &maxValidator{threshold: threshold}
}

// Validate checks if the value is at most the maximum
func (v *maxValidator) Validate(value string) string {
	if v.measuresLength {
		if float64(utf8.RuneCountInString(value)) > v.threshold {
			return "value must have at most " + strconv.FormatFloat(v.threshold, 'f', -1, 64) + " characters"
		}
		return ""
	}
	f, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return errMsgValidNumber
	}
	if f > v.threshold {
		return "value must be at most " + strconv.FormatFloat(v.threshold, 'f', -1, 64)
	}
	return ""
}

// Name returns the validator name
func (v *maxValidator) Name() string {
	return maxTagValue
}

// specializeValidator returns the validator the field needs, once the field's
// kind is known: a comparison on a string field measures the string, and len
// on a numeric field means the value equals the parameter. Any validator that
// does not depend on the field's kind is returned as it is. Returns an error
// in strict mode when a deferred parameter turns out not to fit the field
// (eq=abc on a numeric field), and nil in non-strict mode, which drops it the
// way parse-time parameter checks already do.
func specializeValidator(v validator, isString bool, strict bool) (validator, error) {
	switch typed := v.(type) {
	case *pendingEqualityValidator:
		if isString {
			if typed.tag == equalTagValue {
				return newTextEqualValidator(typed.param), nil
			}
			return newTextNotEqualValidator(typed.param), nil
		}
		threshold, err := strconv.ParseFloat(typed.param, 64)
		if err != nil {
			if strict {
				return nil, fmt.Errorf("%w: %s requires a numeric value on a numeric field, got %q",
					ErrInvalidTagFormat, typed.tag, typed.param)
			}
			return nil, nil //nolint:nilnil // non-strict mode silently ignores invalid args
		}
		if typed.tag == equalTagValue {
			return newEqualValidator(threshold), nil
		}
		return newNotEqualValidator(threshold), nil
	case *minValidator:
		if isString {
			return &minValidator{threshold: typed.threshold, measuresLength: true}, nil
		}
	case *maxValidator:
		if isString {
			return &maxValidator{threshold: typed.threshold, measuresLength: true}, nil
		}
	case *greaterThanValidator:
		if isString {
			return &greaterThanValidator{threshold: typed.threshold, measuresLength: true}, nil
		}
	case *greaterThanEqualValidator:
		if isString {
			return &greaterThanEqualValidator{threshold: typed.threshold, measuresLength: true}, nil
		}
	case *lessThanValidator:
		if isString {
			return &lessThanValidator{threshold: typed.threshold, measuresLength: true}, nil
		}
	case *lessThanEqualValidator:
		if isString {
			return &lessThanEqualValidator{threshold: typed.threshold, measuresLength: true}, nil
		}
	case *lengthValidator:
		if !isString {
			return &lengthValidator{length: typed.length, measuresValue: true}, nil
		}
	}
	return v, nil
}

// lengthValidator validates that a value has exactly the specified length for
// a string field, and that the value is exactly the parameter for a numeric
// one — which is what the validator dialect this package follows means by len
// on a number.
type lengthValidator struct {
	length        int
	measuresValue bool
}

// newLengthValidator creates a new length validator
func newLengthValidator(length int) *lengthValidator {
	return &lengthValidator{length: length}
}

// Validate checks the length of a string, or the value of a number.
func (v *lengthValidator) Validate(value string) string {
	if v.measuresValue {
		f, err := strconv.ParseFloat(value, 64)
		if err != nil {
			return errMsgValidNumber
		}
		if f != float64(v.length) {
			return "value must be the number " + strconv.Itoa(v.length)
		}
		return ""
	}
	count := utf8.RuneCountInString(value)
	if count != v.length {
		return "value must have exactly " + strconv.Itoa(v.length) + " characters"
	}
	return ""
}

// Name returns the validator name
func (v *lengthValidator) Name() string {
	return lengthTagValue
}

// =============================================================================
// String Validators
// =============================================================================

// oneOfValidator validates that a value is one of the allowed values
type oneOfValidator struct {
	allowedSet map[string]struct{} // O(1) lookup instead of O(n) linear search
	errMsg     string              // pre-built error message
}

// newOneOfValidator creates a new oneOf validator
func newOneOfValidator(allowed []string) *oneOfValidator {
	allowedSet := make(map[string]struct{}, len(allowed))
	for _, s := range allowed {
		allowedSet[s] = struct{}{}
	}
	return &oneOfValidator{
		allowedSet: allowedSet,
		errMsg:     "value must be one of: " + strings.Join(allowed, ", "),
	}
}

// Validate checks if the value is one of the allowed values
func (v *oneOfValidator) Validate(value string) string {
	if _, ok := v.allowedSet[value]; ok {
		return ""
	}
	return v.errMsg
}

// Name returns the validator name
func (v *oneOfValidator) Name() string {
	return oneOfTagValue
}

// lowercaseValidator validates that a value is all lowercase
type lowercaseValidator struct{}

// newLowercaseValidator creates a new lowercase validator
func newLowercaseValidator() *lowercaseValidator {
	return &lowercaseValidator{}
}

// Validate checks if the value is all lowercase
func (v *lowercaseValidator) Validate(value string) string {
	if value != strings.ToLower(value) {
		return "value must be lowercase"
	}
	return ""
}

// Name returns the validator name
func (v *lowercaseValidator) Name() string {
	return lowercaseValidatorTagValue
}

// uppercaseValidator validates that a value is all uppercase
type uppercaseValidator struct{}

// newUppercaseValidator creates a new uppercase validator
func newUppercaseValidator() *uppercaseValidator {
	return &uppercaseValidator{}
}

// Validate checks if the value is all uppercase
func (v *uppercaseValidator) Validate(value string) string {
	if value != strings.ToUpper(value) {
		return "value must be uppercase"
	}
	return ""
}

// Name returns the validator name
func (v *uppercaseValidator) Name() string {
	return uppercaseValidatorTagValue
}

// asciiValidator validates that a value contains only ASCII characters
type asciiValidator struct{}

// newASCIIValidator creates a new ASCII validator
func newASCIIValidator() *asciiValidator {
	return &asciiValidator{}
}

// Validate checks if the value contains only ASCII characters
func (v *asciiValidator) Validate(value string) string {
	const maxASCII = 127
	for _, r := range value {
		if r > maxASCII {
			return "value must contain only ASCII characters"
		}
	}
	return ""
}

// Name returns the validator name
func (v *asciiValidator) Name() string {
	return asciiTagValue
}

// printASCIIValidator validates that a value contains only printable ASCII characters
type printASCIIValidator struct{}

// newPrintASCIIValidator creates a new printable ASCII validator
func newPrintASCIIValidator() *printASCIIValidator {
	return &printASCIIValidator{}
}

// Validate checks if the value contains only printable ASCII characters (0x20-0x7E)
func (v *printASCIIValidator) Validate(value string) string {
	for _, r := range value {
		if r < 0x20 || r > 0x7e {
			return "value must contain only printable ASCII characters"
		}
	}
	return ""
}

// Name returns the validator name
func (v *printASCIIValidator) Name() string {
	return printASCIITagValue
}

// =============================================================================
// Format Validators
// =============================================================================

// emailValidator validates that a value is a valid email address
type emailValidator struct{}

// newEmailValidator creates a new email validator
func newEmailValidator() *emailValidator {
	return &emailValidator{}
}

// Validate checks if the value is a valid email address
func (v *emailValidator) Validate(value string) string {
	if !emailRegex.MatchString(value) {
		return "value must be a valid email address"
	}
	return ""
}

// Name returns the validator name
func (v *emailValidator) Name() string {
	return emailTagValue
}

// uriValidator validates that a value is a valid URI
type uriValidator struct{}

// newURIValidator creates a new URI validator
func newURIValidator() *uriValidator {
	return &uriValidator{}
}

// Validate checks if the value is a valid URI
func (v *uriValidator) Validate(value string) string {
	if value == "" {
		return "value must be a valid URI"
	}

	if strings.ContainsAny(value, " \t\r\n") {
		return "value must be a valid URI"
	}

	parsed, err := url.Parse(value)
	if err != nil || parsed.Scheme == "" {
		return "value must be a valid URI"
	}

	if parsed.Host == "" && parsed.Opaque == "" && parsed.Path == "" {
		return "value must be a valid URI"
	}
	return ""
}

// Name returns the validator name
func (v *uriValidator) Name() string {
	return uriTagValue
}

// urlValidator validates that a value is a valid URL
type urlValidator struct{}

// newURLValidator creates a new URL validator
func newURLValidator() *urlValidator {
	return &urlValidator{}
}

// Validate checks if the value is a valid URL
func (v *urlValidator) Validate(value string) string {
	if value == "" {
		return errMsgValidURL
	}

	parsed, err := url.Parse(strings.ToLower(value))
	if err != nil || parsed.Scheme == "" {
		return errMsgValidURL
	}

	isFileScheme := parsed.Scheme == fileScheme
	if (isFileScheme && (parsed.Path == "" || parsed.Path == "/")) ||
		(!isFileScheme && parsed.Host == "" && parsed.Fragment == "" && parsed.Opaque == "") {
		return errMsgValidURL
	}
	return ""
}

// Name returns the validator name
func (v *urlValidator) Name() string {
	return urlTagValue
}

// httpURLValidator validates that a value is a valid HTTP or HTTPS URL
type httpURLValidator struct{}

// newHTTPURLValidator creates a new HTTP URL validator
func newHTTPURLValidator() *httpURLValidator {
	return &httpURLValidator{}
}

// Validate checks if the value is a valid HTTP or HTTPS URL
func (v *httpURLValidator) Validate(value string) string {
	parsed, err := url.Parse(strings.ToLower(value))
	if err != nil || parsed.Host == "" {
		return "value must be a valid HTTP URL"
	}

	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return "value must be a valid HTTP URL"
	}
	return ""
}

// Name returns the validator name
func (v *httpURLValidator) Name() string {
	return httpURLTagValue
}

// httpsURLValidator validates that a value is a valid HTTPS URL
type httpsURLValidator struct{}

// newHTTPSURLValidator creates a new HTTPS URL validator
func newHTTPSURLValidator() *httpsURLValidator {
	return &httpsURLValidator{}
}

// Validate checks if the value is a valid HTTPS URL
func (v *httpsURLValidator) Validate(value string) string {
	parsed, err := url.Parse(strings.ToLower(value))
	if err != nil || parsed.Host == "" || parsed.Scheme != "https" {
		return "value must be a valid HTTPS URL"
	}
	return ""
}

// Name returns the validator name
func (v *httpsURLValidator) Name() string {
	return httpsURLTagValue
}

// urlEncodedValidator validates that a value is URL encoded
type urlEncodedValidator struct{}

// newURLEncodedValidator creates a new URL encoded validator
func newURLEncodedValidator() *urlEncodedValidator {
	return &urlEncodedValidator{}
}

// Validate checks if the value is properly URL encoded
func (v *urlEncodedValidator) Validate(value string) string {
	if !urlEncodedRegex.MatchString(value) {
		return "value must be URL encoded"
	}
	return ""
}

// Name returns the validator name
func (v *urlEncodedValidator) Name() string {
	return urlEncodedTagValue
}

// dataURIValidator validates that a value is a valid data URI
type dataURIValidator struct{}

// newDataURIValidator creates a new data URI validator
func newDataURIValidator() *dataURIValidator {
	return &dataURIValidator{}
}

// Validate checks if the value is a valid data URI
func (v *dataURIValidator) Validate(value string) string {
	if !dataURIRegex.MatchString(value) {
		return errMsgValidDataURI
	}

	parts := strings.SplitN(value, ",", 2)
	if len(parts) != 2 {
		return errMsgValidDataURI
	}

	if _, err := base64.StdEncoding.DecodeString(parts[1]); err != nil {
		return errMsgValidDataURI
	}
	return ""
}

// Name returns the validator name
func (v *dataURIValidator) Name() string {
	return dataURITagValue
}

// =============================================================================
// Network Validators
// =============================================================================

// ipAddrValidator validates that a value is a valid IP address (IPv4 or IPv6)
type ipAddrValidator struct{}

// newIPAddrValidator creates a new IP address validator
func newIPAddrValidator() *ipAddrValidator {
	return &ipAddrValidator{}
}

// Validate checks if the value is a valid IP address
func (v *ipAddrValidator) Validate(value string) string {
	if value == "" || net.ParseIP(value) == nil {
		return "value must be a valid IP address"
	}
	return ""
}

// Name returns the validator name
func (v *ipAddrValidator) Name() string {
	return ipAddrTagValue
}

// ip4AddrValidator validates that a value is a valid IPv4 address
type ip4AddrValidator struct{}

// newIP4AddrValidator creates a new IPv4 address validator
func newIP4AddrValidator() *ip4AddrValidator {
	return &ip4AddrValidator{}
}

// Validate checks if the value is a valid IPv4 address
func (v *ip4AddrValidator) Validate(value string) string {
	if value == "" {
		return "value must be a valid IPv4 address"
	}
	ip := net.ParseIP(value)
	if ip == nil || ip.To4() == nil {
		return "value must be a valid IPv4 address"
	}
	return ""
}

// Name returns the validator name
func (v *ip4AddrValidator) Name() string {
	return ip4AddrTagValue
}

// ip6AddrValidator validates that a value is a valid IPv6 address
type ip6AddrValidator struct{}

// newIP6AddrValidator creates a new IPv6 address validator
func newIP6AddrValidator() *ip6AddrValidator {
	return &ip6AddrValidator{}
}

// Validate checks if the value is a valid IPv6 address
func (v *ip6AddrValidator) Validate(value string) string {
	if value == "" {
		return "value must be a valid IPv6 address"
	}
	ip := net.ParseIP(value)
	if ip == nil || ip.To4() != nil {
		return "value must be a valid IPv6 address"
	}
	return ""
}

// Name returns the validator name
func (v *ip6AddrValidator) Name() string {
	return ip6AddrTagValue
}

// cidrValidator validates that a value is a valid CIDR notation
type cidrValidator struct{}

// newCIDRValidator creates a new CIDR validator
func newCIDRValidator() *cidrValidator {
	return &cidrValidator{}
}

// Validate checks if the value is a valid CIDR notation
func (v *cidrValidator) Validate(value string) string {
	if value == "" {
		return "value must be a valid CIDR"
	}
	_, _, err := net.ParseCIDR(value)
	if err != nil {
		return "value must be a valid CIDR"
	}
	return ""
}

// Name returns the validator name
func (v *cidrValidator) Name() string {
	return cidrTagValue
}

// cidrv4Validator validates that a value is a valid IPv4 CIDR notation
type cidrv4Validator struct{}

// newCIDRv4Validator creates a new IPv4 CIDR validator
func newCIDRv4Validator() *cidrv4Validator {
	return &cidrv4Validator{}
}

// Validate checks if the value is a valid IPv4 CIDR notation. The dialect's
// cidrv4 also requires the address to be the network address itself, so an
// address with host bits set (192.168.0.1/24) is rejected where 192.168.0.0/24
// is accepted. The general cidr and cidrv6 tags do not impose this rule.
func (v *cidrv4Validator) Validate(value string) string {
	if value == "" {
		return "value must be a valid IPv4 CIDR"
	}
	ip, network, err := net.ParseCIDR(value)
	if err != nil || ip.To4() == nil || !network.IP.Equal(ip) {
		return "value must be a valid IPv4 CIDR"
	}
	return ""
}

// Name returns the validator name
func (v *cidrv4Validator) Name() string {
	return cidrv4TagValue
}

// cidrv6Validator validates that a value is a valid IPv6 CIDR notation
type cidrv6Validator struct{}

// newCIDRv6Validator creates a new IPv6 CIDR validator
func newCIDRv6Validator() *cidrv6Validator {
	return &cidrv6Validator{}
}

// Validate checks if the value is a valid IPv6 CIDR notation
func (v *cidrv6Validator) Validate(value string) string {
	if value == "" {
		return "value must be a valid IPv6 CIDR"
	}
	ip, _, err := net.ParseCIDR(value)
	if err != nil || ip.To4() != nil {
		return "value must be a valid IPv6 CIDR"
	}
	return ""
}

// Name returns the validator name
func (v *cidrv6Validator) Name() string {
	return cidrv6TagValue
}

// =============================================================================
// Identifier Validators
// =============================================================================

// uuidValidator validates that a value is a valid UUID
type uuidValidator struct{}

// newUUIDValidator creates a new UUID validator
func newUUIDValidator() *uuidValidator {
	return &uuidValidator{}
}

// Validate checks if the value is a valid UUID
func (v *uuidValidator) Validate(value string) string {
	if !uuidRegex.MatchString(value) {
		return "value must be a valid UUID"
	}
	return ""
}

// Name returns the validator name
func (v *uuidValidator) Name() string {
	return uuidTagValue
}

// fqdnValidator validates that a value is a valid fully qualified domain name
type fqdnValidator struct{}

// newFQDNValidator creates a new FQDN validator
func newFQDNValidator() *fqdnValidator {
	return &fqdnValidator{}
}

// Validate checks if the value is a valid FQDN
func (v *fqdnValidator) Validate(value string) string {
	if strings.HasPrefix(value, ".") || strings.HasSuffix(value, ".") {
		return errMsgValidFQDN
	}

	labels := strings.Split(value, ".")
	if len(labels) < 2 {
		return errMsgValidFQDN
	}

	// The dialect requires a non-numeric top-level domain, so an all-numeric
	// dotted string (an IPv4 address, or a bare numeric TLD) is not an FQDN.
	if !fqdnTLDRegex.MatchString(labels[len(labels)-1]) {
		return errMsgValidFQDN
	}

	totalLen := 0
	for _, label := range labels {
		totalLen += len(label) + 1
		if !fqdnLabelRegex.MatchString(label) {
			return errMsgValidFQDN
		}
	}

	if totalLen-1 > 253 {
		return errMsgValidFQDN
	}
	return ""
}

// Name returns the validator name
func (v *fqdnValidator) Name() string {
	return fqdnTagValue
}

// hostnameValidator validates that a value is a valid hostname (RFC 952)
type hostnameValidator struct{}

// newHostnameValidator creates a new hostname validator
func newHostnameValidator() *hostnameValidator {
	return &hostnameValidator{}
}

// Validate checks if the value is a valid hostname (RFC 952)
func (v *hostnameValidator) Validate(value string) string {
	return validateHostnameWithRegex(value, hostnameRFC952LabelRegex, "value must be a valid hostname")
}

// Name returns the validator name
func (v *hostnameValidator) Name() string {
	return hostnameTagValue
}

// hostnameRFC1123Validator validates that a value is a valid hostname (RFC 1123)
type hostnameRFC1123Validator struct{}

// newHostnameRFC1123Validator creates a new hostname RFC 1123 validator
func newHostnameRFC1123Validator() *hostnameRFC1123Validator {
	return &hostnameRFC1123Validator{}
}

// Validate checks if the value is a valid hostname (RFC 1123)
func (v *hostnameRFC1123Validator) Validate(value string) string {
	return validateHostnameWithRegex(value, hostnameRFC1123LabelRegex, "value must be a valid hostname (RFC 1123)")
}

// Name returns the validator name
func (v *hostnameRFC1123Validator) Name() string {
	return hostnameRFC1123TagValue
}

// validateHostnameWithRegex validates a hostname with the given label regex
func validateHostnameWithRegex(value string, labelRegex *regexp.Regexp, errMsg string) string {
	if strings.HasPrefix(value, ".") || strings.HasSuffix(value, ".") {
		return errMsg
	}

	labels := strings.Split(value, ".")
	if len(labels) < 1 {
		return errMsg
	}

	totalLen := 0
	for _, label := range labels {
		totalLen += len(label) + 1
		if !labelRegex.MatchString(label) {
			return errMsg
		}
	}

	if totalLen-1 > 253 {
		return errMsg
	}
	return ""
}

// hostnamePortValidator validates that a value is a valid hostname:port
type hostnamePortValidator struct{}

// newHostnamePortValidator creates a new hostname:port validator
func newHostnamePortValidator() *hostnamePortValidator {
	return &hostnamePortValidator{}
}

// Validate checks if the value is a valid hostname:port
func (v *hostnamePortValidator) Validate(value string) string {
	host, portStr, err := net.SplitHostPort(value)
	if err != nil {
		return errMsgValidHostnamePort
	}

	port, err := strconv.Atoi(portStr)
	if err != nil || port < 1 || port > 65535 {
		return errMsgValidHostnamePort
	}

	// Check if it's an IP address. SplitHostPort already stripped the
	// brackets a literal IPv6 address arrives in.
	if ip := net.ParseIP(host); ip != nil {
		return ""
	}

	// Validate as hostname
	if validateHostnameWithRegex(host, hostnameRFC1123LabelRegex, errMsgValidHostnamePort) != "" {
		return errMsgValidHostnamePort
	}
	return ""
}

// Name returns the validator name
func (v *hostnamePortValidator) Name() string {
	return hostnamePortTagValue
}

// =============================================================================
// String Content Validators
// =============================================================================

// startsWithValidator validates that a value starts with the prefix
type startsWithValidator struct {
	prefix string
}

// newStartsWithValidator creates a new startsWith validator
func newStartsWithValidator(prefix string) *startsWithValidator {
	return &startsWithValidator{prefix: prefix}
}

// Validate checks if the value starts with the prefix
func (v *startsWithValidator) Validate(value string) string {
	if !strings.HasPrefix(value, v.prefix) {
		return "value must start with '" + v.prefix + "'"
	}
	return ""
}

// Name returns the validator name
func (v *startsWithValidator) Name() string {
	return startsWithTagValue
}

// startsNotWithValidator validates that a value does not start with the prefix
type startsNotWithValidator struct {
	prefix string
}

// newStartsNotWithValidator creates a new startsNotWith validator
func newStartsNotWithValidator(prefix string) *startsNotWithValidator {
	return &startsNotWithValidator{prefix: prefix}
}

// Validate checks if the value does not start with the prefix
func (v *startsNotWithValidator) Validate(value string) string {
	if strings.HasPrefix(value, v.prefix) {
		return "value must not start with '" + v.prefix + "'"
	}
	return ""
}

// Name returns the validator name
func (v *startsNotWithValidator) Name() string {
	return startsNotWithTagValue
}

// endsWithValidator validates that a value ends with the suffix
type endsWithValidator struct {
	suffix string
}

// newEndsWithValidator creates a new endsWith validator
func newEndsWithValidator(suffix string) *endsWithValidator {
	return &endsWithValidator{suffix: suffix}
}

// Validate checks if the value ends with the suffix
func (v *endsWithValidator) Validate(value string) string {
	if !strings.HasSuffix(value, v.suffix) {
		return "value must end with '" + v.suffix + "'"
	}
	return ""
}

// Name returns the validator name
func (v *endsWithValidator) Name() string {
	return endsWithTagValue
}

// endsNotWithValidator validates that a value does not end with the suffix
type endsNotWithValidator struct {
	suffix string
}

// newEndsNotWithValidator creates a new endsNotWith validator
func newEndsNotWithValidator(suffix string) *endsNotWithValidator {
	return &endsNotWithValidator{suffix: suffix}
}

// Validate checks if the value does not end with the suffix
func (v *endsNotWithValidator) Validate(value string) string {
	if strings.HasSuffix(value, v.suffix) {
		return "value must not end with '" + v.suffix + "'"
	}
	return ""
}

// Name returns the validator name
func (v *endsNotWithValidator) Name() string {
	return endsNotWithTagValue
}

// containsValidator validates that a value contains the substring
type containsValidator struct {
	substr string
}

// newContainsValidator creates a new contains validator
func newContainsValidator(substr string) *containsValidator {
	return &containsValidator{substr: substr}
}

// Validate checks if the value contains the substring
func (v *containsValidator) Validate(value string) string {
	if !strings.Contains(value, v.substr) {
		return "value must contain '" + v.substr + "'"
	}
	return ""
}

// Name returns the validator name
func (v *containsValidator) Name() string {
	return containsTagValue
}

// containsAnyValidator validates that a value contains any of the specified characters.
// This is symmetric with excludesAllValidator and uses strings.ContainsAny for per-character checking.
type containsAnyValidator struct {
	chars string
}

// newContainsAnyValidator creates a new containsAny validator.
// Each character in chars is checked individually (e.g., "abc" checks for 'a', 'b', or 'c').
func newContainsAnyValidator(chars string) *containsAnyValidator {
	return &containsAnyValidator{chars: chars}
}

// Validate checks if the value contains any of the specified characters
func (v *containsAnyValidator) Validate(value string) string {
	if value == "" || v.chars == "" {
		return "value must contain any of: " + v.chars
	}
	if strings.ContainsAny(value, v.chars) {
		return ""
	}
	return "value must contain any of: " + v.chars
}

// Name returns the validator name
func (v *containsAnyValidator) Name() string {
	return containsAnyTagValue
}

// containsRuneValidator validates that a value contains the rune
type containsRuneValidator struct {
	r rune
}

// newContainsRuneValidator creates a new containsRune validator
func newContainsRuneValidator(r rune) *containsRuneValidator {
	return &containsRuneValidator{r: r}
}

// Validate checks if the value contains the rune
func (v *containsRuneValidator) Validate(value string) string {
	if !strings.ContainsRune(value, v.r) {
		return "value must contain character '" + string(v.r) + "'"
	}
	return ""
}

// Name returns the validator name
func (v *containsRuneValidator) Name() string {
	return containsRuneTagValue
}

// =============================================================================
// Exclusion Validators
// =============================================================================

// excludesValidator validates that a value does not contain the substring
type excludesValidator struct {
	substr string
}

// newExcludesValidator creates a new excludes validator
func newExcludesValidator(substr string) *excludesValidator {
	return &excludesValidator{substr: substr}
}

// Validate checks if the value does not contain the substring
func (v *excludesValidator) Validate(value string) string {
	if strings.Contains(value, v.substr) {
		return "value must not contain '" + v.substr + "'"
	}
	return ""
}

// Name returns the validator name
func (v *excludesValidator) Name() string {
	return excludesTagValue
}

// excludesAllValidator validates that a value does not contain any of the runes
type excludesAllValidator struct {
	chars string
}

// newExcludesAllValidator creates a new excludesAll validator
func newExcludesAllValidator(chars string) *excludesAllValidator {
	return &excludesAllValidator{chars: chars}
}

// Validate checks if the value does not contain any of the specified characters
func (v *excludesAllValidator) Validate(value string) string {
	if value == "" || v.chars == "" {
		return ""
	}
	if strings.ContainsAny(value, v.chars) {
		return "value must not contain any of: " + v.chars
	}
	return ""
}

// Name returns the validator name
func (v *excludesAllValidator) Name() string {
	return excludesAllTagValue
}

// excludesRuneValidator validates that a value does not contain the rune
type excludesRuneValidator struct {
	r rune
}

// newExcludesRuneValidator creates a new excludesRune validator
func newExcludesRuneValidator(r rune) *excludesRuneValidator {
	return &excludesRuneValidator{r: r}
}

// Validate checks if the value does not contain the rune
func (v *excludesRuneValidator) Validate(value string) string {
	if strings.ContainsRune(value, v.r) {
		return "value must not contain character '" + string(v.r) + "'"
	}
	return ""
}

// Name returns the validator name
func (v *excludesRuneValidator) Name() string {
	return excludesRuneTagValue
}

// =============================================================================
// Misc Validators
// =============================================================================

// multibyteValidator validates that a value contains multibyte characters
type multibyteValidator struct{}

// newMultibyteValidator creates a new multibyte validator
func newMultibyteValidator() *multibyteValidator {
	return &multibyteValidator{}
}

// Validate checks if the value contains at least one multibyte character
func (v *multibyteValidator) Validate(value string) string {
	if value == "" || utf8.RuneCountInString(value) == len(value) {
		return "value must contain multibyte characters"
	}
	return ""
}

// Name returns the validator name
func (v *multibyteValidator) Name() string {
	return multibyteTagValue
}

// equalIgnoreCaseValidator validates that a value equals the expected value (case insensitive)
type equalIgnoreCaseValidator struct {
	expected string
}

// newEqualIgnoreCaseValidator creates a new equalIgnoreCase validator
func newEqualIgnoreCaseValidator(expected string) *equalIgnoreCaseValidator {
	return &equalIgnoreCaseValidator{expected: expected}
}

// Validate checks if the value equals the expected value (case insensitive)
func (v *equalIgnoreCaseValidator) Validate(value string) string {
	if !strings.EqualFold(value, v.expected) {
		return "value must equal '" + v.expected + "' (case insensitive)"
	}
	return ""
}

// Name returns the validator name
func (v *equalIgnoreCaseValidator) Name() string {
	return equalIgnoreCaseTagValue
}

// notEqualIgnoreCaseValidator validates that a value does not equal the expected value (case insensitive)
type notEqualIgnoreCaseValidator struct {
	expected string
}

// newNotEqualIgnoreCaseValidator creates a new notEqualIgnoreCase validator
func newNotEqualIgnoreCaseValidator(expected string) *notEqualIgnoreCaseValidator {
	return &notEqualIgnoreCaseValidator{expected: expected}
}

// Validate checks if the value does not equal the expected value (case insensitive)
func (v *notEqualIgnoreCaseValidator) Validate(value string) string {
	if strings.EqualFold(value, v.expected) {
		return "value must not equal '" + v.expected + "' (case insensitive)"
	}
	return ""
}

// Name returns the validator name
func (v *notEqualIgnoreCaseValidator) Name() string {
	return notEqualIgnoreCaseTagValue
}

// =============================================================================
// Datetime validator
// =============================================================================

// datetimeValidator validates that a value matches the specified datetime layout
type datetimeValidator struct {
	layout string
}

// newDatetimeValidator creates a new datetime validator with the specified layout
func newDatetimeValidator(layout string) *datetimeValidator {
	return &datetimeValidator{layout: layout}
}

// Validate checks if the value matches the datetime layout
func (v *datetimeValidator) Validate(value string) string {
	if value == "" {
		return ""
	}
	if err := parseDateTimeImpl(value, v.layout); err != nil {
		return "value must be a valid datetime in format: " + v.layout
	}
	return ""
}

// Name returns the validator name
func (v *datetimeValidator) Name() string {
	return datetimeTagValue
}

// parseDateTimeImpl parses a datetime string using the specified layout
func parseDateTimeImpl(value, layout string) error {
	_, err := time.Parse(layout, value)
	return err
}

// =============================================================================
// E.164 Phone Number validator
// =============================================================================

// e164Validator validates that a value is a valid E.164 phone number
type e164Validator struct{}

// newE164Validator creates a new E.164 validator
func newE164Validator() *e164Validator {
	return &e164Validator{}
}

// Validate checks if the value is a valid E.164 phone number
func (v *e164Validator) Validate(value string) string {
	if value == "" {
		return ""
	}
	if !e164Regex.MatchString(value) {
		return "value must be a valid E.164 phone number"
	}
	return ""
}

// Name returns the validator name
func (v *e164Validator) Name() string {
	return e164TagValue
}

// =============================================================================
// Geolocation Validators
// =============================================================================

// latitudeValidator validates that a value is a valid latitude (-90 to 90)
type latitudeValidator struct{}

// newLatitudeValidator creates a new latitude validator
func newLatitudeValidator() *latitudeValidator {
	return &latitudeValidator{}
}

// Validate checks if the value is a valid latitude
func (v *latitudeValidator) Validate(value string) string {
	if value == "" {
		return ""
	}
	if !latitudeRegex.MatchString(value) {
		return "value must be a valid latitude (-90 to 90)"
	}
	return ""
}

// Name returns the validator name
func (v *latitudeValidator) Name() string {
	return latitudeTagValue
}

// longitudeValidator validates that a value is a valid longitude (-180 to 180)
type longitudeValidator struct{}

// newLongitudeValidator creates a new longitude validator
func newLongitudeValidator() *longitudeValidator {
	return &longitudeValidator{}
}

// Validate checks if the value is a valid longitude
func (v *longitudeValidator) Validate(value string) string {
	if value == "" {
		return ""
	}
	if !longitudeRegex.MatchString(value) {
		return "value must be a valid longitude (-180 to 180)"
	}
	return ""
}

// Name returns the validator name
func (v *longitudeValidator) Name() string {
	return longitudeTagValue
}

// =============================================================================
// UUID Variant Validators
// =============================================================================

// uuid3Validator validates that a value is a valid UUID version 3
type uuid3Validator struct{}

// newUUID3Validator creates a new UUID v3 validator
func newUUID3Validator() *uuid3Validator {
	return &uuid3Validator{}
}

// Validate checks if the value is a valid UUID version 3
func (v *uuid3Validator) Validate(value string) string {
	if !uuid3Regex.MatchString(value) {
		return "value must be a valid UUID version 3"
	}
	return ""
}

// Name returns the validator name
func (v *uuid3Validator) Name() string {
	return uuid3TagValue
}

// uuid4Validator validates that a value is a valid UUID version 4
type uuid4Validator struct{}

// newUUID4Validator creates a new UUID v4 validator
func newUUID4Validator() *uuid4Validator {
	return &uuid4Validator{}
}

// Validate checks if the value is a valid UUID version 4
func (v *uuid4Validator) Validate(value string) string {
	if !uuid4Regex.MatchString(value) {
		return "value must be a valid UUID version 4"
	}
	return ""
}

// Name returns the validator name
func (v *uuid4Validator) Name() string {
	return uuid4TagValue
}

// uuid5Validator validates that a value is a valid UUID version 5
type uuid5Validator struct{}

// newUUID5Validator creates a new UUID v5 validator
func newUUID5Validator() *uuid5Validator {
	return &uuid5Validator{}
}

// Validate checks if the value is a valid UUID version 5
func (v *uuid5Validator) Validate(value string) string {
	if !uuid5Regex.MatchString(value) {
		return "value must be a valid UUID version 5"
	}
	return ""
}

// Name returns the validator name
func (v *uuid5Validator) Name() string {
	return uuid5TagValue
}

// ulidValidator validates that a value is a valid ULID
type ulidValidator struct{}

// newULIDValidator creates a new ULID validator
func newULIDValidator() *ulidValidator {
	return &ulidValidator{}
}

// Validate checks if the value is a valid ULID
func (v *ulidValidator) Validate(value string) string {
	if !ulidRegex.MatchString(value) {
		return "value must be a valid ULID"
	}
	return ""
}

// Name returns the validator name
func (v *ulidValidator) Name() string {
	return ulidTagValue
}

// =============================================================================
// Hexadecimal and Color Validators
// =============================================================================

// hexadecimalValidator validates that a value is a valid hexadecimal string
type hexadecimalValidator struct{}

// newHexadecimalValidator creates a new hexadecimal validator
func newHexadecimalValidator() *hexadecimalValidator {
	return &hexadecimalValidator{}
}

// Validate checks if the value is a valid hexadecimal string
func (v *hexadecimalValidator) Validate(value string) string {
	if value == "" {
		return ""
	}
	if !hexadecimalRegex.MatchString(value) {
		return "value must be a valid hexadecimal"
	}
	return ""
}

// Name returns the validator name
func (v *hexadecimalValidator) Name() string {
	return hexadecimalTagValue
}

// hexColorValidator validates that a value is a valid hex color code
type hexColorValidator struct{}

// newHexColorValidator creates a new hex color validator
func newHexColorValidator() *hexColorValidator {
	return &hexColorValidator{}
}

// Validate checks if the value is a valid hex color code
func (v *hexColorValidator) Validate(value string) string {
	if value == "" {
		return ""
	}
	if !hexColorRegex.MatchString(value) {
		return "value must be a valid hex color"
	}
	return ""
}

// Name returns the validator name
func (v *hexColorValidator) Name() string {
	return hexColorTagValue
}

// rgbValidator validates that a value is a valid RGB color
type rgbValidator struct{}

// newRGBValidator creates a new RGB color validator
func newRGBValidator() *rgbValidator {
	return &rgbValidator{}
}

// Validate checks if the value is a valid RGB color
func (v *rgbValidator) Validate(value string) string {
	if value == "" {
		return ""
	}
	if !rgbRegex.MatchString(value) {
		return "value must be a valid RGB color"
	}
	return ""
}

// Name returns the validator name
func (v *rgbValidator) Name() string {
	return rgbTagValue
}

// rgbaValidator validates that a value is a valid RGBA color
type rgbaValidator struct{}

// newRGBAValidator creates a new RGBA color validator
func newRGBAValidator() *rgbaValidator {
	return &rgbaValidator{}
}

// Validate checks if the value is a valid RGBA color
func (v *rgbaValidator) Validate(value string) string {
	if value == "" {
		return ""
	}
	if !rgbaRegex.MatchString(value) {
		return "value must be a valid RGBA color"
	}
	return ""
}

// Name returns the validator name
func (v *rgbaValidator) Name() string {
	return rgbaTagValue
}

// hslValidator validates that a value is a valid HSL color
type hslValidator struct{}

// newHSLValidator creates a new HSL color validator
func newHSLValidator() *hslValidator {
	return &hslValidator{}
}

// Validate checks if the value is a valid HSL color
func (v *hslValidator) Validate(value string) string {
	if value == "" {
		return ""
	}
	if !hslRegex.MatchString(value) {
		return "value must be a valid HSL color"
	}
	return ""
}

// Name returns the validator name
func (v *hslValidator) Name() string {
	return hslTagValue
}

// hslaValidator validates that a value is a valid HSLA color
type hslaValidator struct{}

// newHSLAValidator creates a new HSLA color validator
func newHSLAValidator() *hslaValidator {
	return &hslaValidator{}
}

// Validate checks if the value is a valid HSLA color
func (v *hslaValidator) Validate(value string) string {
	if value == "" {
		return ""
	}
	if !hslaRegex.MatchString(value) {
		return "value must be a valid HSLA color"
	}
	return ""
}

// Name returns the validator name
func (v *hslaValidator) Name() string {
	return hslaTagValue
}

// =============================================================================
// MAC Address validator
// =============================================================================

// macValidator validates that a value is a valid MAC address
type macValidator struct{}

// newMACValidator creates a new MAC address validator
func newMACValidator() *macValidator {
	return &macValidator{}
}

// Validate checks if the value is a valid MAC address
func (v *macValidator) Validate(value string) string {
	if value == "" {
		return ""
	}
	if _, err := net.ParseMAC(value); err != nil {
		return "value must be a valid MAC address"
	}
	return ""
}

// Name returns the validator name
func (v *macValidator) Name() string {
	return macTagValue
}
