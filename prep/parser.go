package prep

import (
	"fmt"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// tagParamRegex splits a tag parameter the way the go-playground dialect does:
// a single-quoted run is one token, spaces and all, and an unquoted run is
// split on whitespace.
//
//nolint:gochecknoglobals // compiled once, read-only
var tagParamRegex = regexp.MustCompile(`'[^']*'|\S+`)

// splitTagParams returns the tokens a tag parameter names: split on the regex
// above, then drop the single quotes that grouped a multi-word token. oneof
// names its allowed values this way, and the conditional-required family names
// its fields and expected values the same way.
func splitTagParams(value string) []string {
	tokens := tagParamRegex.FindAllString(value, -1)
	for i, tok := range tokens {
		tokens[i] = strings.ReplaceAll(tok, "'", "")
	}
	return tokens
}

// fieldInfo contains parsed information about a struct field
type fieldInfo struct {
	Name                 string               // Struct field name
	ColumnName           string               // Expected CSV column name (from name tag or auto-converted)
	Index                int                  // Field index in struct
	ColumnIndex          int                  // Column index in CSV (resolved at runtime, -1 if not found)
	Preprocessors        preprocessors        // Preprocessing rules
	Validators           validators           // Validation rules
	CrossFieldValidators crossFieldValidators // Cross-field validation rules
	Unique               bool                 // The column's non-empty values must not repeat
}

// structInfo contains parsed information about a struct type
type structInfo struct {
	Fields []fieldInfo
}

// structInfoCacheKey combines the reflect.Type and strict flag for cache lookups.
type structInfoCacheKey struct {
	typ    reflect.Type
	strict bool
}

// structInfoCache caches parsed structInfo by (reflect.Type, strict) so that
// repeated Process calls on the same Processor avoid redundant tag parsing.
//
//nolint:gochecknoglobals // process-wide cache; safe for concurrent use via sync.Map
var structInfoCache sync.Map

// cachedParseStructType returns a cached *structInfo for the given type and
// strict flag, parsing and caching it on first access.
func cachedParseStructType(structType reflect.Type, strict bool) (*structInfo, error) {
	key := structInfoCacheKey{typ: structType, strict: strict}
	if v, ok := structInfoCache.Load(key); ok {
		return v.(*structInfo), nil //nolint:forcetypeassert,errcheck // type is guaranteed by Store below
	}
	info, err := parseStructType(structType, strict)
	if err != nil {
		return nil, err
	}
	structInfoCache.Store(key, info)
	return info, nil
}

// parseStructType parses struct tags from a struct type and returns field information
func parseStructType(structType reflect.Type, strict bool) (*structInfo, error) {
	if structType.Kind() != reflect.Struct {
		return nil, fmt.Errorf("%w: expected struct, got %s", ErrStructSlicePointer, structType.Kind())
	}

	fieldCount := structType.NumField()
	fields := make([]fieldInfo, 0, fieldCount)

	for i := range fieldCount {
		field := structType.Field(i)

		// Skip unexported fields
		if !field.IsExported() {
			continue
		}

		// Determine column name: use name tag if present, otherwise convert field name to snake_case
		columnName := field.Tag.Get(nameTagName)
		if columnName == "" {
			columnName = toSnakeCase(field.Name)
		}

		info := fieldInfo{
			Name:        field.Name,
			ColumnName:  columnName,
			Index:       i,
			ColumnIndex: -1, // Will be resolved at runtime
		}

		// Parse prep tag
		if prepTag := field.Tag.Get(prepTagName); prepTag != "" {
			preps, err := parsePrepTag(prepTag, strict)
			if err != nil {
				return nil, fmt.Errorf("field %s: %w", field.Name, err)
			}
			info.Preprocessors = preps
		}

		// Parse validate tag, then give each comparison validator the meaning
		// the field's kind decides; see specializeValidator.
		if validateTag := field.Tag.Get(validateTagName); validateTag != "" {
			vals, crossVals, err := parseValidateTag(validateTag, strict)
			if err != nil {
				return nil, fmt.Errorf("field %s: %w", field.Name, err)
			}
			isString := field.Type.Kind() == reflect.String
			specialized := make(validators, 0, len(vals))
			for _, v := range vals {
				// unique marks the field and builds no validator; the seen set
				// it needs lives per processing run, not in this cached struct.
				if _, ok := v.(*uniqueMarkerValidator); ok {
					info.Unique = true
					continue
				}
				sv, err := specializeValidator(v, isString, strict)
				if err != nil {
					return nil, fmt.Errorf("field %s: %w", field.Name, err)
				}
				if sv != nil {
					specialized = append(specialized, sv)
				}
			}
			info.Validators = specialized
			specializeCrossField(crossVals, isString)
			info.CrossFieldValidators = crossVals
		}

		fields = append(fields, info)
	}

	return &structInfo{Fields: fields}, nil
}

// parsePrepTag parses the prep tag string and returns preprocessors
// requiresParam is a builder for a validator whose parameter is the value it
// checks against, and which has nothing to check without one. An empty
// parameter is a malformed tag rather than a validator that passes every value:
// strict mode reports it, and non-strict mode drops it, which is what its
// documented behavior for an invalid tag argument is. Dropping it silently in
// both modes left a column unchecked with nothing to say so, and a datetime tag
// written without a layout is the way that is most easily reached.
func requiresParam[T validator](build func(string) T, message string) func(string, bool) (validator, error) {
	return func(value string, strict bool) (validator, error) {
		if value == "" {
			return nil, invalidValidateParam(strict, message)
		}
		return build(value), nil
	}
}

// requiresRuneParam is requiresParam for the two tags whose parameter is a
// single character; a longer parameter keeps its first character, as the
// dialect reads it.
func requiresRuneParam[T validator](build func(rune) T, message string) func(string, bool) (validator, error) {
	return func(value string, strict bool) (validator, error) {
		runes := []rune(value)
		if len(runes) == 0 {
			return nil, invalidValidateParam(strict, message)
		}
		return build(runes[0]), nil
	}
}

// invalidValidateParam is the answer to a validate tag parameter that cannot be
// used: the error in strict mode, and nothing at all in non-strict mode.
func invalidValidateParam(strict bool, message string) error {
	if !strict {
		return nil
	}
	return fmt.Errorf("%w: %s", ErrInvalidTagFormat, message)
}

// prepBuilders maps a prep tag to the preprocessor it builds. A builder answers
// a nil preprocessor when the tag needs a parameter it was not given and strict
// is off, which is how a non-strict parse drops what it cannot use; in strict
// mode the same case is an ErrInvalidTagFormat naming the tag.
//
// The table is what parsePrepTag looks a tag up in, so a tag exists exactly when
// it has an entry here — which is what lets prepTagNames report the vocabulary
// rather than repeating it.
var prepBuilders = map[string]func(value string, strict bool) (preprocessor, error){
	// Basic preprocessors.
	trimTagValue:      noParamPrep(newTrimPreprocessor),
	ltrimTagValue:     noParamPrep(newLtrimPreprocessor),
	rtrimTagValue:     noParamPrep(newRtrimPreprocessor),
	lowercaseTagValue: noParamPrep(newLowercasePreprocessor),
	uppercaseTagValue: noParamPrep(newUppercasePreprocessor),
	// A default of the empty string is a default: it says the column may be
	// missing and its cells stay empty, which is how a struct covers a column
	// the input does not have.
	defaultTagValue: func(value string, _ bool) (preprocessor, error) {
		return newDefaultPreprocessor(value), nil
	},

	// String transformation preprocessors.
	replaceTagValue: func(value string, strict bool) (preprocessor, error) {
		oldStr, newStr, found := parseColonSeparatedValue(value)
		if !found {
			return nil, invalidPrepParam(strict, "replace requires old:new format, got %q", value)
		}
		return newReplacePreprocessor(oldStr, newStr), nil
	},
	prefixTagValue: valuePrep(newPrefixPreprocessor, "prefix requires a value"),
	suffixTagValue: valuePrep(newSuffixPreprocessor, "suffix requires a value"),
	truncateTagValue: func(value string, strict bool) (preprocessor, error) {
		n, err := strconv.Atoi(value)
		if err != nil || n <= 0 {
			return nil, invalidPrepParam(strict, "truncate requires a positive integer, got %q", value)
		}
		return newTruncatePreprocessor(n), nil
	},
	stripHTMLTagValue:     noParamPrep(newStripHTMLPreprocessor),
	stripNewlineTagValue:  noParamPrep(newStripNewlinePreprocessor),
	collapseSpaceTagValue: noParamPrep(newCollapseSpacePreprocessor),

	// Character filtering preprocessors.
	removeDigitsTagValue: noParamPrep(newRemoveDigitsPreprocessor),
	removeAlphaTagValue:  noParamPrep(newRemoveAlphaPreprocessor),
	keepDigitsTagValue:   noParamPrep(newKeepDigitsPreprocessor),
	keepAlphaTagValue:    noParamPrep(newKeepAlphaPreprocessor),
	trimSetTagValue:      valuePrep(newTrimSetPreprocessor, "trim_set requires characters to trim"),

	// Padding preprocessors. The parameter is "N:char"; the comma already
	// separates one tag from the next and cannot also separate a tag's own
	// parameters.
	padLeftTagValue:  padPrep(newPadLeftPreprocessor, padLeftTagValue),
	padRightTagValue: padPrep(newPadRightPreprocessor, padRightTagValue),

	// Advanced preprocessors.
	normalizeUnicodeTagValue: noParamPrep(newNormalizeUnicodePreprocessor),
	nullifyTagValue:          valuePrep(newNullifyPreprocessor, "nullify requires a value"),
	coerceTagValue: func(value string, strict bool) (preprocessor, error) {
		if value != "int" && value != "float" && value != "bool" {
			return nil, invalidPrepParam(strict, "coerce requires int, float, or bool, got %q", value)
		}
		return newCoercePreprocessor(value), nil
	},
	fixSchemeTagValue: valuePrep(newFixSchemePreprocessor, "fix_scheme requires a scheme value"),
	regexReplaceTagValue: func(value string, strict bool) (preprocessor, error) {
		pattern, replacement, found := parseColonSeparatedValue(value)
		if !found {
			return nil, invalidPrepParam(strict, "regex_replace requires pattern:replacement format, got %q", value)
		}
		rp := newRegexReplacePreprocessor(pattern, replacement)
		if rp == nil {
			return nil, invalidPrepParam(strict, "regex_replace has invalid pattern %q", pattern)
		}
		return rp, nil
	},
}

// noParamPrep is a builder for a tag that takes no parameter.
func noParamPrep[T preprocessor](build func() T) func(string, bool) (preprocessor, error) {
	return func(_ string, _ bool) (preprocessor, error) { return build(), nil }
}

// valuePrep is a builder for a tag whose parameter is the value it works with,
// and which has nothing to do without one.
func valuePrep[T preprocessor](build func(string) T, message string) func(string, bool) (preprocessor, error) {
	return func(value string, strict bool) (preprocessor, error) {
		if value == "" {
			return nil, invalidPrepParam(strict, "%s", message)
		}
		return build(value), nil
	}
}

// padPrep is a builder for the padding tags, whose parameter is a length and an
// optional pad character.
func padPrep[T preprocessor](build func(int, rune) T, tag string) func(string, bool) (preprocessor, error) {
	return func(value string, strict bool) (preprocessor, error) {
		length, padChar := parsePadParams(value)
		if length <= 0 {
			return nil, invalidPrepParam(strict, "%s requires a positive length, got %q", tag, value)
		}
		return build(length, padChar), nil
	}
}

// invalidPrepParam is the answer to a parameter a tag cannot work with: the
// error in strict mode, and nothing at all in non-strict mode, where the
// documented behavior is to ignore what cannot be used.
func invalidPrepParam(strict bool, format string, args ...any) error {
	if !strict {
		return nil
	}
	return fmt.Errorf("%w: "+format, append([]any{ErrInvalidTagFormat}, args...)...)
}

// prepTagNames is every prep tag the parser accepts, sorted so the answer does
// not depend on map iteration order.
func prepTagNames() []string {
	names := make([]string, 0, len(prepBuilders))
	for name := range prepBuilders {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// parsePrepTag builds the preprocessors one field's prep tag asks for. The tag
// is a comma-separated list, so a tag's own parameters cannot use a comma.
func parsePrepTag(tag string, strict bool) (preprocessors, error) {
	if tag == "" {
		return nil, nil
	}

	parts := strings.Split(tag, ",")
	preps := make(preprocessors, 0, len(parts))

	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}

		key, value := splitTagKeyValue(part)
		build, known := prepBuilders[key]
		if !known {
			return nil, fmt.Errorf("%w: unknown prep tag %q", ErrInvalidTagFormat, part)
		}
		prep, err := build(value, strict)
		if err != nil {
			return nil, err
		}
		if prep != nil {
			preps = append(preps, prep)
		}
	}

	return preps, nil
}

// parseColonSeparatedValue parses "old:new" format values
// Returns old, new, and true if the format is valid
func parseColonSeparatedValue(value string) (string, string, bool) {
	idx := strings.Index(value, ":")
	if idx < 0 {
		return "", "", false
	}
	return value[:idx], value[idx+1:], true
}

// parsePadParams parses "N:char" format for padding preprocessors
// Returns length and pad character (defaults to space if not specified)
func parsePadParams(value string) (int, rune) {
	parts := strings.SplitN(value, ":", 2)
	if len(parts) == 0 {
		return 0, ' '
	}

	length, err := strconv.Atoi(parts[0])
	if err != nil || length <= 0 {
		return 0, ' '
	}

	padChar := ' '
	if len(parts) == 2 && len(parts[1]) > 0 {
		runes := []rune(parts[1])
		padChar = runes[0]
	}

	return length, padChar
}

func buildCrossFieldValidator(
	tagName string,
	value string,
	strict bool,
	factory func(string) crossFieldValidator,
) (crossFieldValidator, error) {
	if strings.TrimSpace(value) == "" {
		if strict {
			return nil, fmt.Errorf("%w: %s requires a field name", ErrInvalidTagFormat, tagName)
		}
		return nil, nil //nolint:nilnil // non-strict mode silently ignores invalid args
	}

	return factory(value), nil
}

// buildFieldListValidator builds a tag that names a list of fields, such as
// required_with. The dialect lets a tag name several fields, so the parameter
// is tokenized rather than taken whole; taking it whole named a field no struct
// has and reported a missing field on every row.
func buildFieldListValidator(
	tagName string,
	value string,
	strict bool,
	factory func([]string) crossFieldValidator,
) (crossFieldValidator, error) {
	fields := splitTagParams(value)
	if len(fields) == 0 {
		if strict {
			return nil, fmt.Errorf("%w: %s requires a field name", ErrInvalidTagFormat, tagName)
		}
		return nil, nil //nolint:nilnil // non-strict mode silently ignores invalid args
	}

	return factory(fields), nil
}

// buildConditionalCrossFieldValidator builds required_if or required_unless.
// The dialect writes their parameter as field and value pairs, and a value
// holding a space is quoted, so the parameter is tokenized the way oneof is.
func buildConditionalCrossFieldValidator(
	tagName string,
	value string,
	strict bool,
	factory func([]fieldCondition) crossFieldValidator,
) (crossFieldValidator, error) {
	tokens := splitTagParams(value)
	if len(tokens) == 0 || len(tokens)%2 != 0 {
		if strict {
			return nil, fmt.Errorf(
				"%w: %s requires pairs of \"FieldName value\", got %q",
				ErrInvalidTagFormat,
				tagName,
				value,
			)
		}
		return nil, nil //nolint:nilnil // non-strict mode silently ignores invalid args
	}

	conditions := make([]fieldCondition, 0, len(tokens)/2)
	for i := 0; i < len(tokens); i += 2 {
		conditions = append(conditions, fieldCondition{field: tokens[i], expected: tokens[i+1]})
	}

	return factory(conditions), nil
}

// validatorBuilder creates a validator from a tag value parameter.
// Returns the validator (nil if parameter is invalid in non-strict mode) and an error in strict mode.
type validatorBuilder func(value string, strict bool) (validator, error)

// crossFieldValidatorBuilder creates a crossFieldValidator from a tag value parameter.
type crossFieldValidatorBuilder func(value string) crossFieldValidator

// fieldListValidatorBuilder creates a crossFieldValidator from the field names
// a tag parameter lists.
type fieldListValidatorBuilder func(fields []string) crossFieldValidator

// buildFloatValidator is a helper for validators that require a numeric threshold parameter.
func buildFloatValidator(tagName string, value string, strict bool, factory func(float64) validator) (validator, error) {
	threshold, err := strconv.ParseFloat(value, 64)
	if err != nil {
		if strict {
			return nil, fmt.Errorf("%w: %s requires a numeric value, got %q", ErrInvalidTagFormat, tagName, value)
		}
		return nil, nil //nolint:nilnil // non-strict mode silently ignores invalid args
	}
	return factory(threshold), nil
}

// validatorRegistry maps tag names to their builder functions.
// Builders that ignore the value parameter use _ to indicate it's unused.
//
//nolint:gochecknoglobals // registry pattern requires package-level map for O(1) lookup
var validatorRegistry = map[string]validatorBuilder{
	// Sentinel
	omitemptyTagValue: func(_ string, _ bool) (validator, error) { return &omitemptyValidator{}, nil },

	// Basic validators
	requiredTagValue:     func(_ string, _ bool) (validator, error) { return newRequiredValidator(), nil },
	booleanTagValue:      func(_ string, _ bool) (validator, error) { return newBooleanValidator(), nil },
	alphaTagValue:        func(_ string, _ bool) (validator, error) { return newAlphaValidator(), nil },
	alphaSpaceTagValue:   func(_ string, _ bool) (validator, error) { return newAlphaSpaceValidator(), nil },
	alphaUnicodeTagValue: func(_ string, _ bool) (validator, error) { return newAlphaUnicodeValidator(), nil },
	numericTagValue:      func(_ string, _ bool) (validator, error) { return newNumericValidator(), nil },
	numberTagValue:       func(_ string, _ bool) (validator, error) { return newNumberValidator(), nil },
	alphanumTagValue: func(_ string, _ bool) (validator, error) {
		return newAlphanumericValidator(alphanumTagValue), nil
	},
	alphanumericTagValue: func(_ string, _ bool) (validator, error) {
		return newAlphanumericValidator(alphanumericTagValue), nil
	},
	alphanumSpaceTagValue:       func(_ string, _ bool) (validator, error) { return newAlphanumSpaceValidator(), nil },
	alphanumericUnicodeTagValue: func(_ string, _ bool) (validator, error) { return newAlphanumericUnicodeValidator(), nil },

	// Comparison validators (with threshold). eq and ne keep their raw
	// parameter here: a string field compares the string itself while any
	// other field compares the number, so the parameter can only be judged
	// once the field's kind is known, in specializeValidator.
	equalTagValue: func(v string, _ bool) (validator, error) {
		return &pendingEqualityValidator{tag: equalTagValue, param: v}, nil
	},
	notEqualTagValue: func(v string, _ bool) (validator, error) {
		return &pendingEqualityValidator{tag: notEqualTagValue, param: v}, nil
	},
	greaterThanTagValue: func(v string, s bool) (validator, error) {
		return buildFloatValidator("gt", v, s, func(t float64) validator { return newGreaterThanValidator(t) })
	},
	greaterThanEqualTagValue: func(v string, s bool) (validator, error) {
		return buildFloatValidator("gte", v, s, func(t float64) validator { return newGreaterThanEqualValidator(t) })
	},
	lessThanTagValue: func(v string, s bool) (validator, error) {
		return buildFloatValidator("lt", v, s, func(t float64) validator { return newLessThanValidator(t) })
	},
	lessThanEqualTagValue: func(v string, s bool) (validator, error) {
		return buildFloatValidator("lte", v, s, func(t float64) validator { return newLessThanEqualValidator(t) })
	},
	minTagValue: func(v string, s bool) (validator, error) {
		return buildFloatValidator("min", v, s, func(t float64) validator { return newMinValidator(t) })
	},
	maxTagValue: func(v string, s bool) (validator, error) {
		return buildFloatValidator("max", v, s, func(t float64) validator { return newMaxValidator(t) })
	},
	lengthTagValue: func(value string, strict bool) (validator, error) {
		length, err := strconv.Atoi(value)
		if err != nil || length <= 0 {
			if strict {
				return nil, fmt.Errorf("%w: len requires a positive integer value, got %q", ErrInvalidTagFormat, value)
			}
			return nil, nil //nolint:nilnil // non-strict mode silently ignores invalid args
		}
		return newLengthValidator(length), nil
	},

	// String validators
	oneOfTagValue: func(value string, _ bool) (validator, error) {
		if values := splitTagParams(value); len(values) > 0 {
			return newOneOfValidator(values), nil
		}
		return nil, nil //nolint:nilnil // empty value produces no validator
	},
	lowercaseValidatorTagValue: func(_ string, _ bool) (validator, error) { return newLowercaseValidator(), nil },
	uppercaseValidatorTagValue: func(_ string, _ bool) (validator, error) { return newUppercaseValidator(), nil },
	asciiTagValue:              func(_ string, _ bool) (validator, error) { return newASCIIValidator(), nil },
	printASCIITagValue:         func(_ string, _ bool) (validator, error) { return newPrintASCIIValidator(), nil },

	// Format validators
	emailTagValue:      func(_ string, _ bool) (validator, error) { return newEmailValidator(), nil },
	uriTagValue:        func(_ string, _ bool) (validator, error) { return newURIValidator(), nil },
	urlTagValue:        func(_ string, _ bool) (validator, error) { return newURLValidator(), nil },
	httpURLTagValue:    func(_ string, _ bool) (validator, error) { return newHTTPURLValidator(), nil },
	httpsURLTagValue:   func(_ string, _ bool) (validator, error) { return newHTTPSURLValidator(), nil },
	urlEncodedTagValue: func(_ string, _ bool) (validator, error) { return newURLEncodedValidator(), nil },
	dataURITagValue:    func(_ string, _ bool) (validator, error) { return newDataURIValidator(), nil },

	// Network validators. ip, ipv4 and ipv6 are the dialect's spellings of
	// ip_addr, ip4_addr and ip6_addr and build the same validators.
	ipTagValue:      func(_ string, _ bool) (validator, error) { return newIPAddrValidator(ipTagValue), nil },
	ipv4TagValue:    func(_ string, _ bool) (validator, error) { return newIP4AddrValidator(ipv4TagValue), nil },
	ipv6TagValue:    func(_ string, _ bool) (validator, error) { return newIP6AddrValidator(ipv6TagValue), nil },
	portTagValue:    func(_ string, _ bool) (validator, error) { return newPortValidator(), nil },
	ipAddrTagValue:  func(_ string, _ bool) (validator, error) { return newIPAddrValidator(ipAddrTagValue), nil },
	ip4AddrTagValue: func(_ string, _ bool) (validator, error) { return newIP4AddrValidator(ip4AddrTagValue), nil },
	ip6AddrTagValue: func(_ string, _ bool) (validator, error) { return newIP6AddrValidator(ip6AddrTagValue), nil },
	cidrTagValue:    func(_ string, _ bool) (validator, error) { return newCIDRValidator(), nil },
	cidrv4TagValue:  func(_ string, _ bool) (validator, error) { return newCIDRv4Validator(), nil },
	cidrv6TagValue:  func(_ string, _ bool) (validator, error) { return newCIDRv6Validator(), nil },
	macTagValue:     func(_ string, _ bool) (validator, error) { return newMACValidator(), nil },

	// Identifier validators
	uuidTagValue:            func(_ string, _ bool) (validator, error) { return newUUIDValidator(), nil },
	fqdnTagValue:            func(_ string, _ bool) (validator, error) { return newFQDNValidator(), nil },
	hostnameTagValue:        func(_ string, _ bool) (validator, error) { return newHostnameValidator(), nil },
	hostnameRFC1123TagValue: func(_ string, _ bool) (validator, error) { return newHostnameRFC1123Validator(), nil },
	hostnamePortTagValue:    func(_ string, _ bool) (validator, error) { return newHostnamePortValidator(), nil },

	// String content validators (with parameter)
	startsWithTagValue:    requiresParam(newStartsWithValidator, "startswith requires a prefix"),
	startsNotWithTagValue: requiresParam(newStartsNotWithValidator, "startsnotwith requires a prefix"),
	endsWithTagValue:      requiresParam(newEndsWithValidator, "endswith requires a suffix"),
	endsNotWithTagValue:   requiresParam(newEndsNotWithValidator, "endsnotwith requires a suffix"),
	containsTagValue:      requiresParam(newContainsValidator, "contains requires a substring"),
	containsAnyTagValue:   requiresParam(newContainsAnyValidator, "containsany requires the characters to look for"),
	containsRuneTagValue:  requiresRuneParam(newContainsRuneValidator, "containsrune requires a character"),

	// Exclusion validators (with parameter)
	excludesTagValue:     requiresParam(newExcludesValidator, "excludes requires a substring"),
	excludesAllTagValue:  requiresParam(newExcludesAllValidator, "excludesall requires the characters to refuse"),
	excludesRuneTagValue: requiresRuneParam(newExcludesRuneValidator, "excludesrune requires a character"),

	// Misc validators
	multibyteTagValue:          func(_ string, _ bool) (validator, error) { return newMultibyteValidator(), nil },
	equalIgnoreCaseTagValue:    requiresParam(newEqualIgnoreCaseValidator, "eq_ignore_case requires a value to compare"),
	notEqualIgnoreCaseTagValue: requiresParam(newNotEqualIgnoreCaseValidator, "ne_ignore_case requires a value to compare"),
	datetimeTagValue:           requiresParam(newDatetimeValidator, "datetime requires a layout"),

	// Phone number validator
	e164TagValue: func(_ string, _ bool) (validator, error) { return newE164Validator(), nil },

	// Geolocation validators
	latitudeTagValue:  func(_ string, _ bool) (validator, error) { return newLatitudeValidator(), nil },
	longitudeTagValue: func(_ string, _ bool) (validator, error) { return newLongitudeValidator(), nil },

	// UUID variant validators
	uuid3TagValue: func(_ string, _ bool) (validator, error) { return newUUID3Validator(), nil },
	uuid4TagValue: func(_ string, _ bool) (validator, error) { return newUUID4Validator(), nil },
	uuid5TagValue: func(_ string, _ bool) (validator, error) { return newUUID5Validator(), nil },
	ulidTagValue:  func(_ string, _ bool) (validator, error) { return newULIDValidator(), nil },

	// Hexadecimal and color validators
	hexadecimalTagValue: func(_ string, _ bool) (validator, error) { return newHexadecimalValidator(), nil },
	hexColorTagValue:    func(_ string, _ bool) (validator, error) { return newHexColorValidator(), nil },
	rgbTagValue:         func(_ string, _ bool) (validator, error) { return newRGBValidator(), nil },
	rgbaTagValue:        func(_ string, _ bool) (validator, error) { return newRGBAValidator(), nil },
	hslTagValue:         func(_ string, _ bool) (validator, error) { return newHSLValidator(), nil },
	hslaTagValue:        func(_ string, _ bool) (validator, error) { return newHSLAValidator(), nil },

	// Structured format validators
	jsonTagValue:     func(_ string, _ bool) (validator, error) { return newJSONValidator(), nil },
	timezoneTagValue: func(_ string, _ bool) (validator, error) { return newTimezoneValidator(), nil },
	semverTagValue:   func(_ string, _ bool) (validator, error) { return newSemverValidator(), nil },

	// RFC 4648 encoding validators
	base32TagValue:       func(_ string, _ bool) (validator, error) { return newBase32Validator(), nil },
	base64TagValue:       func(_ string, _ bool) (validator, error) { return newBase64Validator(), nil },
	base64URLTagValue:    func(_ string, _ bool) (validator, error) { return newBase64URLValidator(), nil },
	base64RawURLTagValue: func(_ string, _ bool) (validator, error) { return newBase64RawURLValidator(), nil },

	// oneofci reads its candidates the way oneof does, quoting included.
	oneOfCITagValue: func(value string, _ bool) (validator, error) {
		if values := splitTagParams(value); len(values) > 0 {
			return newOneOfCIValidator(values), nil
		}
		return nil, nil //nolint:nilnil // empty value produces no validator
	},

	// Checksummed identifier validators
	creditCardTagValue:   func(_ string, _ bool) (validator, error) { return newCreditCardValidator(), nil },
	luhnChecksumTagValue: func(_ string, _ bool) (validator, error) { return newLuhnChecksumValidator(), nil },
	isbnTagValue:         func(_ string, _ bool) (validator, error) { return newISBNValidator(), nil },
	isbn10TagValue:       func(_ string, _ bool) (validator, error) { return newISBN10Validator(), nil },
	isbn13TagValue:       func(_ string, _ bool) (validator, error) { return newISBN13Validator(), nil },
	issnTagValue:         func(_ string, _ bool) (validator, error) { return newISSNValidator(), nil },

	// unique is read at parse time; the sentinel carries the tag no further
	// than parseStructType. A parameter is the reference's form for addressing
	// a struct field inside a slice element, which has no counterpart here.
	uniqueTagValue: func(value string, strict bool) (validator, error) {
		if value != "" {
			return nil, invalidValidateParam(strict, "unique takes no parameter, got "+strconv.Quote(value))
		}
		return &uniqueMarkerValidator{}, nil
	},

	// Country and currency code validators
	iso3166Alpha2TagValue:  func(_ string, _ bool) (validator, error) { return newISO3166Alpha2Validator(), nil },
	iso3166Alpha3TagValue:  func(_ string, _ bool) (validator, error) { return newISO3166Alpha3Validator(), nil },
	iso3166NumericTagValue: func(_ string, _ bool) (validator, error) { return newISO3166NumericValidator(), nil },
	countryCodeTagValue:    func(_ string, _ bool) (validator, error) { return newCountryCodeValidator(), nil },
	iso4217TagValue:        func(_ string, _ bool) (validator, error) { return newISO4217Validator(), nil },

	// Message digest validators, which differ in nothing but their width
	md5TagValue:    hexDigest(md5TagValue, 32),
	sha256TagValue: hexDigest(sha256TagValue, 64),
	sha384TagValue: hexDigest(sha384TagValue, 96),
	sha512TagValue: hexDigest(sha512TagValue, 128),
}

// hexDigest is a builder for one of the message digest tags.
func hexDigest(tag string, length int) validatorBuilder {
	return func(_ string, _ bool) (validator, error) {
		return newHexDigestValidator(tag, length), nil
	}
}

// crossFieldValidatorRegistry maps tag names to their builder functions.
//
//nolint:gochecknoglobals // registry pattern requires package-level map for O(1) lookup
var crossFieldValidatorRegistry = map[string]crossFieldValidatorBuilder{
	eqFieldTagValue:       func(v string) crossFieldValidator { return newEqFieldValidator(v) },
	neFieldTagValue:       func(v string) crossFieldValidator { return newNeFieldValidator(v) },
	gtFieldTagValue:       func(v string) crossFieldValidator { return newGtFieldValidator(v) },
	gteFieldTagValue:      func(v string) crossFieldValidator { return newGteFieldValidator(v) },
	ltFieldTagValue:       func(v string) crossFieldValidator { return newLtFieldValidator(v) },
	lteFieldTagValue:      func(v string) crossFieldValidator { return newLteFieldValidator(v) },
	fieldContainsTagValue: func(v string) crossFieldValidator { return newFieldContainsValidator(v) },
	fieldExcludesTagValue: func(v string) crossFieldValidator { return newFieldExcludesValidator(v) },
}

// fieldListValidatorRegistry maps the tags whose parameter is a list of field
// names to their builder functions.
//
//nolint:gochecknoglobals // registry pattern requires package-level map for O(1) lookup
var fieldListValidatorRegistry = map[string]fieldListValidatorBuilder{
	requiredWithTagValue: func(f []string) crossFieldValidator {
		return newRequiredWithValidator(f, false)
	},
	requiredWithAllTagValue: func(f []string) crossFieldValidator {
		return newRequiredWithValidator(f, true)
	},
	requiredWithoutTagValue: func(f []string) crossFieldValidator {
		return newRequiredWithoutValidator(f, false)
	},
	requiredWithoutAllTagValue: func(f []string) crossFieldValidator {
		return newRequiredWithoutValidator(f, true)
	},
	excludedWithTagValue: func(f []string) crossFieldValidator {
		return newExcludedWithValidator(f, false)
	},
	excludedWithAllTagValue: func(f []string) crossFieldValidator {
		return newExcludedWithValidator(f, true)
	},
	excludedWithoutTagValue: func(f []string) crossFieldValidator {
		return newExcludedWithoutValidator(f, false)
	},
	excludedWithoutAllTagValue: func(f []string) crossFieldValidator {
		return newExcludedWithoutValidator(f, true)
	},
}

// pairTakingCrossFieldRegistry maps the tags whose parameter is field and value
// pairs to their builder functions. They are separate from the list-taking
// registry because their parameter is tokenized into pairs rather than names.
//
//nolint:gochecknoglobals // registry pattern requires package-level map for O(1) lookup
var pairTakingCrossFieldRegistry = map[string]func(conditions []fieldCondition) crossFieldValidator{
	requiredIfTagValue: func(c []fieldCondition) crossFieldValidator {
		return newRequiredIfValidator(c)
	},
	requiredUnlessTagValue: func(c []fieldCondition) crossFieldValidator {
		return newRequiredUnlessValidator(c)
	},
	excludedIfTagValue: func(c []fieldCondition) crossFieldValidator {
		return newExcludedIfValidator(c)
	},
	excludedUnlessTagValue: func(c []fieldCondition) crossFieldValidator {
		return newExcludedUnlessValidator(c)
	},
}

// parseValidateTag parses the validate tag string and returns validators and cross-field validators.
// It returns an error if an unknown validate tag is encountered.
// The registry-based approach replaces the large switch statement for easier maintenance.
func parseValidateTag(tag string, strict bool) (validators, crossFieldValidators, error) {
	if tag == "" {
		return nil, nil, nil
	}
	// The dialect writes "-" for a field it validates nothing on. Only the whole
	// tag means that: inside a list it is a tag name like any other, and an
	// unknown one, which is what a caller who wrote it there meant to be told.
	if strings.TrimSpace(tag) == validateSkipTagValue {
		return nil, nil, nil
	}

	parts := strings.Split(tag, ",")
	vals := make(validators, 0, len(parts))
	crossVals := make(crossFieldValidators, 0)

	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}

		key, value := splitTagKeyValue(part)

		// Check single-field validator registry
		if builder, ok := validatorRegistry[key]; ok {
			v, err := builder(value, strict)
			if err != nil {
				return nil, nil, err
			}
			if v != nil {
				vals = append(vals, v)
			}
			continue
		}

		// Check cross-field validator registry
		if builder, ok := crossFieldValidatorRegistry[key]; ok {
			cv, err := buildCrossFieldValidator(key, value, strict, builder)
			if err != nil {
				return nil, nil, err
			}
			if cv != nil {
				crossVals = append(crossVals, cv)
			}
			continue
		}

		// Tags whose parameter lists field names
		if builder, ok := fieldListValidatorRegistry[key]; ok {
			cv, err := buildFieldListValidator(key, value, strict, builder)
			if err != nil {
				return nil, nil, err
			}
			if cv != nil {
				crossVals = append(crossVals, cv)
			}
			continue
		}

		// Tags whose parameter is field and value pairs need their own parsing
		if builder, ok := pairTakingCrossFieldRegistry[key]; ok {
			cv, err := buildConditionalCrossFieldValidator(key, value, strict, builder)
			if err != nil {
				return nil, nil, err
			}
			if cv != nil {
				crossVals = append(crossVals, cv)
			}
			continue
		}

		return nil, nil, fmt.Errorf("%w: unknown validate tag %q", ErrInvalidTagFormat, part)
	}

	return vals, crossVals, nil
}

// splitTagKeyValue splits a tag part into key and value
// For "key=value" returns ("key", "value")
// For "key" returns ("key", "")
func splitTagKeyValue(part string) (string, string) {
	if idx := strings.Index(part, "="); idx > 0 {
		return part[:idx], part[idx+1:]
	}
	return part, ""
}

// toSnakeCase converts a CamelCase or PascalCase string to snake_case.
// Examples: "UserName" -> "user_name", "ID" -> "id", "HTTPServer" -> "http_server"
func toSnakeCase(s string) string {
	if s == "" {
		return s
	}

	var result strings.Builder
	result.Grow(len(s) + 5) // Pre-allocate with some extra space for underscores

	for i, r := range s {
		if r >= 'A' && r <= 'Z' {
			// Insert underscore before uppercase letter (except at the beginning)
			if i > 0 {
				// Check if previous char was lowercase or if next char is lowercase (for acronyms)
				prev := s[i-1]
				isAfterLower := prev >= 'a' && prev <= 'z'
				isBeforeLower := i+1 < len(s) && s[i+1] >= 'a' && s[i+1] <= 'z'
				if isAfterLower || isBeforeLower {
					result.WriteByte('_')
				}
			}
			result.WriteByte(byte(r - 'A' + 'a')) // Convert to lowercase
		} else {
			result.WriteRune(r)
		}
	}

	return result.String()
}

// getStructType extracts the struct type from a pointer to a slice of structs
func getStructType(structSlicePointer any) (reflect.Type, error) {
	if structSlicePointer == nil {
		return nil, fmt.Errorf("%w: nil pointer provided", ErrStructSlicePointer)
	}

	rv := reflect.ValueOf(structSlicePointer)
	if rv.Kind() != reflect.Pointer {
		return nil, fmt.Errorf("%w: expected pointer, got %s", ErrStructSlicePointer, rv.Kind())
	}

	if rv.IsNil() {
		return nil, fmt.Errorf("%w: nil pointer provided", ErrStructSlicePointer)
	}

	elem := rv.Elem()
	switch elem.Kind() {
	case reflect.Slice, reflect.Array:
		elemType := elem.Type().Elem()
		if elemType.Kind() != reflect.Struct {
			return nil, fmt.Errorf("%w: expected slice of structs, got slice of %s", ErrStructSlicePointer, elemType.Kind())
		}
		return elemType, nil
	default:
		return nil, fmt.Errorf("%w: expected slice or array, got %s", ErrStructSlicePointer, elem.Kind())
	}
}
