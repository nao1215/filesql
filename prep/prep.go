package prep

import (
	"math"
	"regexp"
	"strconv"
	"strings"
	"unicode"

	"golang.org/x/text/unicode/norm"

	"github.com/nao1215/filesql/internal/infer"
)

// String constants for coercion results and URL schemes
const (
	boolTrueValue   = "true"
	boolFalseValue  = "false"
	httpsSchemeType = "https"
)

// preprocessor defines the interface for preprocessing values
type preprocessor interface {
	// Process applies preprocessing to the value and returns the result
	Process(value string) string
	// Name returns the name of the preprocessor for error reporting
	Name() string
}

// trimPreprocessor removes leading and trailing whitespace
type trimPreprocessor struct{}

// newTrimPreprocessor creates a new trim preprocessor
func newTrimPreprocessor() *trimPreprocessor {
	return &trimPreprocessor{}
}

// Process removes leading and trailing whitespace
func (p *trimPreprocessor) Process(value string) string {
	return strings.TrimSpace(value)
}

// Name returns the preprocessor name
func (p *trimPreprocessor) Name() string {
	return trimTagValue
}

// ltrimPreprocessor removes leading whitespace
type ltrimPreprocessor struct{}

// newLtrimPreprocessor creates a new left trim preprocessor
func newLtrimPreprocessor() *ltrimPreprocessor {
	return &ltrimPreprocessor{}
}

// Process removes leading whitespace
func (p *ltrimPreprocessor) Process(value string) string {
	return strings.TrimLeft(value, " \t\n\r")
}

// Name returns the preprocessor name
func (p *ltrimPreprocessor) Name() string {
	return ltrimTagValue
}

// rtrimPreprocessor removes trailing whitespace
type rtrimPreprocessor struct{}

// newRtrimPreprocessor creates a new right trim preprocessor
func newRtrimPreprocessor() *rtrimPreprocessor {
	return &rtrimPreprocessor{}
}

// Process removes trailing whitespace
func (p *rtrimPreprocessor) Process(value string) string {
	return strings.TrimRight(value, " \t\n\r")
}

// Name returns the preprocessor name
func (p *rtrimPreprocessor) Name() string {
	return rtrimTagValue
}

// lowercasePreprocessor converts value to lowercase
type lowercasePreprocessor struct{}

// newLowercasePreprocessor creates a new lowercase preprocessor
func newLowercasePreprocessor() *lowercasePreprocessor {
	return &lowercasePreprocessor{}
}

// Process converts value to lowercase
func (p *lowercasePreprocessor) Process(value string) string {
	return strings.ToLower(value)
}

// Name returns the preprocessor name
func (p *lowercasePreprocessor) Name() string {
	return lowercaseTagValue
}

// uppercasePreprocessor converts value to uppercase
type uppercasePreprocessor struct{}

// newUppercasePreprocessor creates a new uppercase preprocessor
func newUppercasePreprocessor() *uppercasePreprocessor {
	return &uppercasePreprocessor{}
}

// Process converts value to uppercase
func (p *uppercasePreprocessor) Process(value string) string {
	return strings.ToUpper(value)
}

// Name returns the preprocessor name
func (p *uppercasePreprocessor) Name() string {
	return uppercaseTagValue
}

// defaultPreprocessor sets a default value if the input is empty
type defaultPreprocessor struct {
	defaultValue string
}

// newDefaultPreprocessor creates a new default value preprocessor
func newDefaultPreprocessor(defaultValue string) *defaultPreprocessor {
	return &defaultPreprocessor{defaultValue: defaultValue}
}

// Process sets the default value if input is empty
func (p *defaultPreprocessor) Process(value string) string {
	if strings.TrimSpace(value) == "" {
		return p.defaultValue
	}
	return value
}

// Name returns the preprocessor name
func (p *defaultPreprocessor) Name() string {
	return defaultTagValue
}

// preprocessors is a slice of preprocessor
type preprocessors []preprocessor

// hasDefault reports whether this field supplies a value when the input has
// none, which is what makes a field with no matching column legitimate rather
// than a typo.
func (ps preprocessors) hasDefault() bool {
	for _, pp := range ps {
		if _, ok := pp.(*defaultPreprocessor); ok {
			return true
		}
	}
	return false
}

// Process applies all preprocessors in order
func (ps preprocessors) Process(value string) string {
	result := value
	for _, p := range ps {
		result = p.Process(result)
	}
	return result
}

// =============================================================================
// String Transformation Preprocessors
// =============================================================================

// replacePreprocessor replaces occurrences of old string with new string
type replacePreprocessor struct {
	oldStr string
	newStr string
}

// newReplacePreprocessor creates a new replace preprocessor
func newReplacePreprocessor(oldStr, newStr string) *replacePreprocessor {
	return &replacePreprocessor{oldStr: oldStr, newStr: newStr}
}

// Process replaces all occurrences of old with new
func (p *replacePreprocessor) Process(value string) string {
	return strings.ReplaceAll(value, p.oldStr, p.newStr)
}

// Name returns the preprocessor name
func (p *replacePreprocessor) Name() string {
	return replaceTagValue
}

// prefixPreprocessor prepends a string to the value
type prefixPreprocessor struct {
	prefix string
}

// newPrefixPreprocessor creates a new prefix preprocessor
func newPrefixPreprocessor(prefix string) *prefixPreprocessor {
	return &prefixPreprocessor{prefix: prefix}
}

// Process prepends the prefix to the value
func (p *prefixPreprocessor) Process(value string) string {
	return p.prefix + value
}

// Name returns the preprocessor name
func (p *prefixPreprocessor) Name() string {
	return prefixTagValue
}

// suffixPreprocessor appends a string to the value
type suffixPreprocessor struct {
	suffix string
}

// newSuffixPreprocessor creates a new suffix preprocessor
func newSuffixPreprocessor(suffix string) *suffixPreprocessor {
	return &suffixPreprocessor{suffix: suffix}
}

// Process appends the suffix to the value
func (p *suffixPreprocessor) Process(value string) string {
	return value + p.suffix
}

// Name returns the preprocessor name
func (p *suffixPreprocessor) Name() string {
	return suffixTagValue
}

// truncatePreprocessor limits the value to a maximum number of characters
type truncatePreprocessor struct {
	maxLen int
}

// newTruncatePreprocessor creates a new truncate preprocessor
func newTruncatePreprocessor(maxLen int) *truncatePreprocessor {
	return &truncatePreprocessor{maxLen: maxLen}
}

// Process truncates the value to the maximum length
func (p *truncatePreprocessor) Process(value string) string {
	runes := []rune(value)
	if len(runes) <= p.maxLen {
		return value
	}
	return string(runes[:p.maxLen])
}

// Name returns the preprocessor name
func (p *truncatePreprocessor) Name() string {
	return truncateTagValue
}

// stripHTMLPreprocessor removes HTML tags from the value
type stripHTMLPreprocessor struct {
	re *regexp.Regexp
}

// newStripHTMLPreprocessor creates a new strip HTML preprocessor
func newStripHTMLPreprocessor() *stripHTMLPreprocessor {
	return &stripHTMLPreprocessor{
		re: regexp.MustCompile(`<[^>]*>`),
	}
}

// Process removes HTML tags from the value
func (p *stripHTMLPreprocessor) Process(value string) string {
	return p.re.ReplaceAllString(value, "")
}

// Name returns the preprocessor name
func (p *stripHTMLPreprocessor) Name() string {
	return stripHTMLTagValue
}

// stripNewlinePreprocessor removes newlines and CRLF from the value
type stripNewlinePreprocessor struct{}

// newStripNewlinePreprocessor creates a new strip newline preprocessor
func newStripNewlinePreprocessor() *stripNewlinePreprocessor {
	return &stripNewlinePreprocessor{}
}

// Process removes newlines from the value.
// This implementation avoids multiple string allocations.
func (p *stripNewlinePreprocessor) Process(value string) string {
	// Quick check: if no newlines, return as-is
	if !strings.ContainsAny(value, "\r\n") {
		return value
	}

	var result strings.Builder
	result.Grow(len(value))
	for _, r := range value {
		if r != '\r' && r != '\n' {
			result.WriteRune(r)
		}
	}
	return result.String()
}

// Name returns the preprocessor name
func (p *stripNewlinePreprocessor) Name() string {
	return stripNewlineTagValue
}

// collapseSpacePreprocessor collapses multiple spaces into one
type collapseSpacePreprocessor struct{}

// newCollapseSpacePreprocessor creates a new collapse space preprocessor
func newCollapseSpacePreprocessor() *collapseSpacePreprocessor {
	return &collapseSpacePreprocessor{}
}

// Process collapses multiple whitespace characters into a single space.
// This implementation avoids regexp for better performance.
func (p *collapseSpacePreprocessor) Process(value string) string {
	if value == "" {
		return value
	}

	var result strings.Builder
	result.Grow(len(value))

	inSpace := false
	for _, r := range value {
		isWhitespace := r == ' ' || r == '\t' || r == '\n' || r == '\r'
		if isWhitespace {
			if !inSpace {
				result.WriteByte(' ')
				inSpace = true
			}
		} else {
			result.WriteRune(r)
			inSpace = false
		}
	}

	return result.String()
}

// Name returns the preprocessor name
func (p *collapseSpacePreprocessor) Name() string {
	return collapseSpaceTagValue
}

// =============================================================================
// Character Filtering Preprocessors
// =============================================================================

// removeDigitsPreprocessor removes all digits from the value
type removeDigitsPreprocessor struct{}

// newRemoveDigitsPreprocessor creates a new remove digits preprocessor
func newRemoveDigitsPreprocessor() *removeDigitsPreprocessor {
	return &removeDigitsPreprocessor{}
}

// Process removes all digits from the value
func (p *removeDigitsPreprocessor) Process(value string) string {
	var result strings.Builder
	result.Grow(len(value))
	for _, r := range value {
		if !unicode.IsDigit(r) {
			result.WriteRune(r)
		}
	}
	return result.String()
}

// Name returns the preprocessor name
func (p *removeDigitsPreprocessor) Name() string {
	return removeDigitsTagValue
}

// removeAlphaPreprocessor removes all alphabetic characters from the value
type removeAlphaPreprocessor struct{}

// newRemoveAlphaPreprocessor creates a new remove alpha preprocessor
func newRemoveAlphaPreprocessor() *removeAlphaPreprocessor {
	return &removeAlphaPreprocessor{}
}

// Process removes all alphabetic characters from the value
func (p *removeAlphaPreprocessor) Process(value string) string {
	var result strings.Builder
	result.Grow(len(value))
	for _, r := range value {
		if !unicode.IsLetter(r) {
			result.WriteRune(r)
		}
	}
	return result.String()
}

// Name returns the preprocessor name
func (p *removeAlphaPreprocessor) Name() string {
	return removeAlphaTagValue
}

// keepDigitsPreprocessor keeps only digits in the value
type keepDigitsPreprocessor struct{}

// newKeepDigitsPreprocessor creates a new keep digits preprocessor
func newKeepDigitsPreprocessor() *keepDigitsPreprocessor {
	return &keepDigitsPreprocessor{}
}

// Process keeps only digits in the value
func (p *keepDigitsPreprocessor) Process(value string) string {
	var result strings.Builder
	result.Grow(len(value))
	for _, r := range value {
		if unicode.IsDigit(r) {
			result.WriteRune(r)
		}
	}
	return result.String()
}

// Name returns the preprocessor name
func (p *keepDigitsPreprocessor) Name() string {
	return keepDigitsTagValue
}

// keepAlphaPreprocessor keeps only alphabetic characters in the value
type keepAlphaPreprocessor struct{}

// newKeepAlphaPreprocessor creates a new keep alpha preprocessor
func newKeepAlphaPreprocessor() *keepAlphaPreprocessor {
	return &keepAlphaPreprocessor{}
}

// Process keeps only alphabetic characters in the value
func (p *keepAlphaPreprocessor) Process(value string) string {
	var result strings.Builder
	result.Grow(len(value))
	for _, r := range value {
		if unicode.IsLetter(r) {
			result.WriteRune(r)
		}
	}
	return result.String()
}

// Name returns the preprocessor name
func (p *keepAlphaPreprocessor) Name() string {
	return keepAlphaTagValue
}

// trimSetPreprocessor removes specified characters from both ends
type trimSetPreprocessor struct {
	cutset string
}

// newTrimSetPreprocessor creates a new trim set preprocessor
func newTrimSetPreprocessor(cutset string) *trimSetPreprocessor {
	return &trimSetPreprocessor{cutset: cutset}
}

// Process removes the specified characters from both ends
func (p *trimSetPreprocessor) Process(value string) string {
	return strings.Trim(value, p.cutset)
}

// Name returns the preprocessor name
func (p *trimSetPreprocessor) Name() string {
	return trimSetTagValue
}

// =============================================================================
// Padding Preprocessors
// =============================================================================

// padLeftPreprocessor left-pads the value to a specified length
type padLeftPreprocessor struct {
	length  int
	padChar rune
}

// newPadLeftPreprocessor creates a new left padding preprocessor
func newPadLeftPreprocessor(length int, padChar rune) *padLeftPreprocessor {
	return &padLeftPreprocessor{length: length, padChar: padChar}
}

// Process left-pads the value to the specified length
func (p *padLeftPreprocessor) Process(value string) string {
	runeCount := len([]rune(value))
	if runeCount >= p.length {
		return value
	}
	padCount := p.length - runeCount
	var result strings.Builder
	result.Grow(len(value) + padCount)
	for range padCount {
		result.WriteRune(p.padChar)
	}
	result.WriteString(value)
	return result.String()
}

// Name returns the preprocessor name
func (p *padLeftPreprocessor) Name() string {
	return padLeftTagValue
}

// padRightPreprocessor right-pads the value to a specified length
type padRightPreprocessor struct {
	length  int
	padChar rune
}

// newPadRightPreprocessor creates a new right padding preprocessor
func newPadRightPreprocessor(length int, padChar rune) *padRightPreprocessor {
	return &padRightPreprocessor{length: length, padChar: padChar}
}

// Process right-pads the value to the specified length
func (p *padRightPreprocessor) Process(value string) string {
	runeCount := len([]rune(value))
	if runeCount >= p.length {
		return value
	}
	padCount := p.length - runeCount
	var result strings.Builder
	result.Grow(len(value) + padCount)
	result.WriteString(value)
	for range padCount {
		result.WriteRune(p.padChar)
	}
	return result.String()
}

// Name returns the preprocessor name
func (p *padRightPreprocessor) Name() string {
	return padRightTagValue
}

// =============================================================================
// Advanced Preprocessors
// =============================================================================

// normalizeUnicodePreprocessor normalizes Unicode to NFC form
type normalizeUnicodePreprocessor struct{}

// newNormalizeUnicodePreprocessor creates a new Unicode normalization preprocessor
func newNormalizeUnicodePreprocessor() *normalizeUnicodePreprocessor {
	return &normalizeUnicodePreprocessor{}
}

// Process normalizes the value to NFC form
func (p *normalizeUnicodePreprocessor) Process(value string) string {
	return norm.NFC.String(value)
}

// Name returns the preprocessor name
func (p *normalizeUnicodePreprocessor) Name() string {
	return normalizeUnicodeTagValue
}

// nullifyPreprocessor treats a specific string as empty
type nullifyPreprocessor struct {
	nullValue string
}

// newNullifyPreprocessor creates a new nullify preprocessor
func newNullifyPreprocessor(nullValue string) *nullifyPreprocessor {
	return &nullifyPreprocessor{nullValue: nullValue}
}

// Process returns empty string if value matches the null value
func (p *nullifyPreprocessor) Process(value string) string {
	if value == p.nullValue {
		return ""
	}
	return value
}

// Name returns the preprocessor name
func (p *nullifyPreprocessor) Name() string {
	return nullifyTagValue
}

// coercePreprocessor performs light type coercion formatting
type coercePreprocessor struct {
	targetType string
}

// newCoercePreprocessor creates a new coerce preprocessor
func newCoercePreprocessor(targetType string) *coercePreprocessor {
	return &coercePreprocessor{targetType: targetType}
}

// Process performs light formatting based on target type
func (p *coercePreprocessor) Process(value string) string {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return value
	}

	switch p.targetType {
	case "int":
		// Read as a float first so "123.0" comes out as "123".
		f, ok := numberToRewrite(trimmed)
		if !ok || !fitsInt64(f) {
			return value
		}
		return strconv.FormatInt(int64(f), 10)
	case "float":
		if f, ok := numberToRewrite(trimmed); ok {
			return strconv.FormatFloat(f, 'f', -1, 64)
		}
	case "bool":
		// The spellings are the ones strconv.ParseBool accepts, which is what
		// the boolean validator accepts and how a bool struct field is filled,
		// plus the four words a spreadsheet writes.
		switch strings.ToLower(trimmed) {
		case boolTrueValue, "1", "t", "yes", "on":
			return boolTrueValue
		case boolFalseValue, "0", "f", "no", "off":
			return boolFalseValue
		}
	}
	return value
}

// numberToRewrite reports the number a coercion may rewrite a value into, and
// whether there is one.
//
// A coercion normalizes how a number is written; it does not decide that a
// string of digits is a number. The spellings refused here are the ones the
// loader keeps as text for that reason: a zero-padded code, whose zeros a
// number drops, a literal past int64, whose digits a float64 loses, and Go's
// own number syntax, which SQLite's affinity does not convert. Coercing them
// rewrote a postal code as a smaller number and turned a column the loader
// would have typed TEXT into an INTEGER one under the caller. A value a
// float64 cannot hold at all is refused for the same reason: it would come
// back as an infinity or as an exact zero, neither of which the file said.
func numberToRewrite(trimmed string) (float64, bool) {
	if infer.IsZeroPaddedIntegerLiteral(trimmed) ||
		infer.IsIntegerLiteralOverflowingInt64(trimmed) ||
		infer.HasGoOnlyNumericSyntax(trimmed) {
		return 0, false
	}
	f, err := strconv.ParseFloat(trimmed, 64)
	if err != nil || math.IsInf(f, 0) || math.IsNaN(f) || infer.UnderflowsFloat64(trimmed, f) {
		return 0, false
	}
	return f, true
}

// fitsInt64 reports whether a float can be converted to an int64 at all. The
// conversion is undefined for a value outside the range, and on amd64 it
// answers the smallest integer, so a number far above the range came out far
// below it.
func fitsInt64(f float64) bool {
	const limit = 1 << 63 // the first float64 above math.MaxInt64
	return f >= -limit && f < limit
}

// Name returns the preprocessor name
func (p *coercePreprocessor) Name() string {
	return coerceTagValue
}

// fixSchemePreprocessor adds or corrects URL scheme
type fixSchemePreprocessor struct {
	scheme string
}

// newFixSchemePreprocessor creates a new fix scheme preprocessor
func newFixSchemePreprocessor(scheme string) *fixSchemePreprocessor {
	return &fixSchemePreprocessor{scheme: scheme}
}

// Process adds scheme if missing, or replaces http with https if scheme is "https"
func (p *fixSchemePreprocessor) Process(value string) string {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return value
	}

	if scheme, rest, found := strings.Cut(trimmed, "://"); found {
		if strings.EqualFold(scheme, "http") {
			if p.scheme == httpsSchemeType {
				return httpsSchemeType + "://" + rest
			}
			return trimmed
		}
		if strings.EqualFold(scheme, httpsSchemeType) {
			return trimmed
		}

		// Preserve non-HTTP schemes instead of prepending another scheme.
		return trimmed
	}

	// Check if URL already has a scheme
	if strings.HasPrefix(trimmed, "http://") {
		if p.scheme == httpsSchemeType {
			return httpsSchemeType + "://" + strings.TrimPrefix(trimmed, "http://")
		}
		return trimmed
	}
	if strings.HasPrefix(trimmed, "https://") {
		return trimmed
	}

	// Add scheme if missing
	return p.scheme + "://" + trimmed
}

// Name returns the preprocessor name
func (p *fixSchemePreprocessor) Name() string {
	return fixSchemeTagValue
}

// regexReplacePreprocessor performs regex-based replacement
type regexReplacePreprocessor struct {
	re          *regexp.Regexp
	replacement string
}

// newRegexReplacePreprocessor creates a new regex replace preprocessor
// Returns nil if the pattern is invalid
func newRegexReplacePreprocessor(pattern, replacement string) *regexReplacePreprocessor {
	re, err := regexp.Compile(pattern)
	if err != nil {
		return nil
	}
	return &regexReplacePreprocessor{re: re, replacement: replacement}
}

// Process applies regex replacement to the value
func (p *regexReplacePreprocessor) Process(value string) string {
	if p.re == nil {
		return value
	}
	return p.re.ReplaceAllString(value, p.replacement)
}

// Name returns the preprocessor name
func (p *regexReplacePreprocessor) Name() string {
	return regexReplaceTagValue
}
