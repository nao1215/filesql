package prep

import (
	"fmt"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"sync"
)

// oneOfParamRegex splits a oneof parameter the way the go-playground dialect
// does: a single-quoted run is one value, spaces and all, and an unquoted run
// is split on whitespace.
//
//nolint:gochecknoglobals // compiled once, read-only
var oneOfParamRegex = regexp.MustCompile(`'[^']*'|\S+`)

// splitOneOfValues returns the allowed values a oneof parameter names, matching
// go-playground: split on the regex above, then drop the single quotes that
// grouped a multi-word value.
func splitOneOfValues(value string) []string {
	tokens := oneOfParamRegex.FindAllString(value, -1)
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

		// Handle preprocessors with parameters (key=value format)
		key, value := splitTagKeyValue(part)

		switch key {
		// Basic preprocessors
		case trimTagValue:
			preps = append(preps, newTrimPreprocessor())
		case ltrimTagValue:
			preps = append(preps, newLtrimPreprocessor())
		case rtrimTagValue:
			preps = append(preps, newRtrimPreprocessor())
		case lowercaseTagValue:
			preps = append(preps, newLowercasePreprocessor())
		case uppercaseTagValue:
			preps = append(preps, newUppercasePreprocessor())
		case defaultTagValue:
			preps = append(preps, newDefaultPreprocessor(value))

		// String transformation preprocessors
		case replaceTagValue:
			// replace=old:new format
			oldStr, newStr, found := parseColonSeparatedValue(value)
			if found {
				preps = append(preps, newReplacePreprocessor(oldStr, newStr))
			} else if strict {
				return nil, fmt.Errorf("%w: replace requires old:new format, got %q", ErrInvalidTagFormat, value)
			}
		case prefixTagValue:
			if value != "" {
				preps = append(preps, newPrefixPreprocessor(value))
			} else if strict {
				return nil, fmt.Errorf("%w: prefix requires a value", ErrInvalidTagFormat)
			}
		case suffixTagValue:
			if value != "" {
				preps = append(preps, newSuffixPreprocessor(value))
			} else if strict {
				return nil, fmt.Errorf("%w: suffix requires a value", ErrInvalidTagFormat)
			}
		case truncateTagValue:
			if n, err := strconv.Atoi(value); err == nil && n > 0 {
				preps = append(preps, newTruncatePreprocessor(n))
			} else if strict {
				return nil, fmt.Errorf("%w: truncate requires a positive integer, got %q", ErrInvalidTagFormat, value)
			}
		case stripHTMLTagValue:
			preps = append(preps, newStripHTMLPreprocessor())
		case stripNewlineTagValue:
			preps = append(preps, newStripNewlinePreprocessor())
		case collapseSpaceTagValue:
			preps = append(preps, newCollapseSpacePreprocessor())

		// Character filtering preprocessors
		case removeDigitsTagValue:
			preps = append(preps, newRemoveDigitsPreprocessor())
		case removeAlphaTagValue:
			preps = append(preps, newRemoveAlphaPreprocessor())
		case keepDigitsTagValue:
			preps = append(preps, newKeepDigitsPreprocessor())
		case keepAlphaTagValue:
			preps = append(preps, newKeepAlphaPreprocessor())
		case trimSetTagValue:
			if value != "" {
				preps = append(preps, newTrimSetPreprocessor(value))
			} else if strict {
				return nil, fmt.Errorf("%w: trim_set requires characters to trim", ErrInvalidTagFormat)
			}

		// Padding preprocessors
		case padLeftTagValue:
			// pad_left=N,char format
			length, padChar := parsePadParams(value)
			if length > 0 {
				preps = append(preps, newPadLeftPreprocessor(length, padChar))
			} else if strict {
				return nil, fmt.Errorf("%w: pad_left requires a positive length, got %q", ErrInvalidTagFormat, value)
			}
		case padRightTagValue:
			// pad_right=N,char format
			length, padChar := parsePadParams(value)
			if length > 0 {
				preps = append(preps, newPadRightPreprocessor(length, padChar))
			} else if strict {
				return nil, fmt.Errorf("%w: pad_right requires a positive length, got %q", ErrInvalidTagFormat, value)
			}

		// Advanced preprocessors
		case normalizeUnicodeTagValue:
			preps = append(preps, newNormalizeUnicodePreprocessor())
		case nullifyTagValue:
			if value != "" {
				preps = append(preps, newNullifyPreprocessor(value))
			} else if strict {
				return nil, fmt.Errorf("%w: nullify requires a value", ErrInvalidTagFormat)
			}
		case coerceTagValue:
			if value == "int" || value == "float" || value == "bool" {
				preps = append(preps, newCoercePreprocessor(value))
			} else if strict {
				return nil, fmt.Errorf("%w: coerce requires int, float, or bool, got %q", ErrInvalidTagFormat, value)
			}
		case fixSchemeTagValue:
			if value != "" {
				preps = append(preps, newFixSchemePreprocessor(value))
			} else if strict {
				return nil, fmt.Errorf("%w: fix_scheme requires a scheme value", ErrInvalidTagFormat)
			}
		case regexReplaceTagValue:
			// regex_replace=pattern:replacement format
			pattern, replacement, found := parseColonSeparatedValue(value)
			if found {
				rp := newRegexReplacePreprocessor(pattern, replacement)
				if rp != nil {
					preps = append(preps, rp)
				} else if strict {
					return nil, fmt.Errorf("%w: regex_replace has invalid pattern %q", ErrInvalidTagFormat, pattern)
				}
			} else if strict {
				return nil, fmt.Errorf("%w: regex_replace requires pattern:replacement format, got %q", ErrInvalidTagFormat, value)
			}

		default:
			return nil, fmt.Errorf("%w: unknown prep tag %q", ErrInvalidTagFormat, part)
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

// parseRequiredIfParams parses "FieldName value" format for required_if/required_unless
// Returns field name and expected/except value
func parseRequiredIfParams(value string) (string, string) {
	parts := strings.SplitN(value, " ", 2)
	if len(parts) == 0 {
		return "", ""
	}
	field := parts[0]
	expectedVal := ""
	if len(parts) == 2 {
		expectedVal = parts[1]
	}
	return field, expectedVal
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

func buildConditionalCrossFieldValidator(
	tagName string,
	value string,
	strict bool,
	factory func(string, string) crossFieldValidator,
) (crossFieldValidator, error) {
	field, expectedVal := parseRequiredIfParams(value)
	if field == "" || expectedVal == "" {
		if strict {
			return nil, fmt.Errorf(
				"%w: %s requires \"FieldName value\" format, got %q",
				ErrInvalidTagFormat,
				tagName,
				value,
			)
		}
		return nil, nil //nolint:nilnil // non-strict mode silently ignores invalid args
	}

	return factory(field, expectedVal), nil
}

// validatorBuilder creates a validator from a tag value parameter.
// Returns the validator (nil if parameter is invalid in non-strict mode) and an error in strict mode.
type validatorBuilder func(value string, strict bool) (validator, error)

// crossFieldValidatorBuilder creates a crossFieldValidator from a tag value parameter.
type crossFieldValidatorBuilder func(value string) crossFieldValidator

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
	requiredTagValue:            func(_ string, _ bool) (validator, error) { return newRequiredValidator(), nil },
	booleanTagValue:             func(_ string, _ bool) (validator, error) { return newBooleanValidator(), nil },
	alphaTagValue:               func(_ string, _ bool) (validator, error) { return newAlphaValidator(), nil },
	alphaSpaceTagValue:          func(_ string, _ bool) (validator, error) { return newAlphaSpaceValidator(), nil },
	alphaUnicodeTagValue:        func(_ string, _ bool) (validator, error) { return newAlphaUnicodeValidator(), nil },
	numericTagValue:             func(_ string, _ bool) (validator, error) { return newNumericValidator(), nil },
	numberTagValue:              func(_ string, _ bool) (validator, error) { return newNumberValidator(), nil },
	alphanumericTagValue:        func(_ string, _ bool) (validator, error) { return newAlphanumericValidator(), nil },
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
		if values := splitOneOfValues(value); len(values) > 0 {
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

	// Network validators
	ipAddrTagValue:  func(_ string, _ bool) (validator, error) { return newIPAddrValidator(), nil },
	ip4AddrTagValue: func(_ string, _ bool) (validator, error) { return newIP4AddrValidator(), nil },
	ip6AddrTagValue: func(_ string, _ bool) (validator, error) { return newIP6AddrValidator(), nil },
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
	startsWithTagValue: func(v string, _ bool) (validator, error) {
		if v != "" {
			return newStartsWithValidator(v), nil
		}
		return nil, nil
	}, //nolint:nlreturn,nilnil // compact builder
	startsNotWithTagValue: func(v string, _ bool) (validator, error) {
		if v != "" {
			return newStartsNotWithValidator(v), nil
		}
		return nil, nil
	}, //nolint:nlreturn,nilnil // compact builder
	endsWithTagValue: func(v string, _ bool) (validator, error) {
		if v != "" {
			return newEndsWithValidator(v), nil
		}
		return nil, nil
	}, //nolint:nlreturn,nilnil // compact builder
	endsNotWithTagValue: func(v string, _ bool) (validator, error) {
		if v != "" {
			return newEndsNotWithValidator(v), nil
		}
		return nil, nil
	}, //nolint:nlreturn,nilnil // compact builder
	containsTagValue: func(v string, _ bool) (validator, error) {
		if v != "" {
			return newContainsValidator(v), nil
		}
		return nil, nil
	}, //nolint:nlreturn,nilnil // compact builder
	containsAnyTagValue: func(v string, _ bool) (validator, error) {
		if v != "" {
			return newContainsAnyValidator(v), nil
		}
		return nil, nil
	}, //nolint:nlreturn,nilnil // compact builder
	containsRuneTagValue: func(v string, _ bool) (validator, error) {
		if v != "" {
			runes := []rune(v)
			if len(runes) > 0 {
				return newContainsRuneValidator(runes[0]), nil
			}
		}
		return nil, nil //nolint:nilnil // empty value produces no validator
	},

	// Exclusion validators (with parameter)
	excludesTagValue: func(v string, _ bool) (validator, error) {
		if v != "" {
			return newExcludesValidator(v), nil
		}
		return nil, nil
	}, //nolint:nlreturn,nilnil // compact builder
	excludesAllTagValue: func(v string, _ bool) (validator, error) {
		if v != "" {
			return newExcludesAllValidator(v), nil
		}
		return nil, nil
	}, //nolint:nlreturn,nilnil // compact builder
	excludesRuneTagValue: func(v string, _ bool) (validator, error) {
		if v != "" {
			runes := []rune(v)
			if len(runes) > 0 {
				return newExcludesRuneValidator(runes[0]), nil
			}
		}
		return nil, nil //nolint:nilnil // empty value produces no validator
	},

	// Misc validators
	multibyteTagValue: func(_ string, _ bool) (validator, error) { return newMultibyteValidator(), nil },
	equalIgnoreCaseTagValue: func(v string, _ bool) (validator, error) {
		if v != "" {
			return newEqualIgnoreCaseValidator(v), nil
		}
		return nil, nil //nolint:nlreturn,nilnil // compact builder
	},
	notEqualIgnoreCaseTagValue: func(v string, _ bool) (validator, error) {
		if v != "" {
			return newNotEqualIgnoreCaseValidator(v), nil
		}
		return nil, nil //nolint:nlreturn,nilnil // compact builder
	},

	// Datetime validator
	datetimeTagValue: func(v string, _ bool) (validator, error) {
		if v != "" {
			return newDatetimeValidator(v), nil
		}
		return nil, nil
	}, //nolint:nlreturn,nilnil // compact builder

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
}

// crossFieldValidatorRegistry maps tag names to their builder functions.
//
//nolint:gochecknoglobals // registry pattern requires package-level map for O(1) lookup
var crossFieldValidatorRegistry = map[string]crossFieldValidatorBuilder{
	eqFieldTagValue:         func(v string) crossFieldValidator { return newEqFieldValidator(v) },
	neFieldTagValue:         func(v string) crossFieldValidator { return newNeFieldValidator(v) },
	gtFieldTagValue:         func(v string) crossFieldValidator { return newGtFieldValidator(v) },
	gteFieldTagValue:        func(v string) crossFieldValidator { return newGteFieldValidator(v) },
	ltFieldTagValue:         func(v string) crossFieldValidator { return newLtFieldValidator(v) },
	lteFieldTagValue:        func(v string) crossFieldValidator { return newLteFieldValidator(v) },
	fieldContainsTagValue:   func(v string) crossFieldValidator { return newFieldContainsValidator(v) },
	fieldExcludesTagValue:   func(v string) crossFieldValidator { return newFieldExcludesValidator(v) },
	requiredWithTagValue:    func(v string) crossFieldValidator { return newRequiredWithValidator(v) },
	requiredWithoutTagValue: func(v string) crossFieldValidator { return newRequiredWithoutValidator(v) },
}

// parseValidateTag parses the validate tag string and returns validators and cross-field validators.
// It returns an error if an unknown validate tag is encountered.
// The registry-based approach replaces the large switch statement for easier maintenance.
func parseValidateTag(tag string, strict bool) (validators, crossFieldValidators, error) {
	if tag == "" {
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

		// Conditional required validators need special parsing (two-parameter format)
		switch key {
		case requiredIfTagValue:
			cv, err := buildConditionalCrossFieldValidator(
				key,
				value,
				strict,
				func(field string, expected string) crossFieldValidator {
					return newRequiredIfValidator(field, expected)
				},
			)
			if err != nil {
				return nil, nil, err
			}
			if cv != nil {
				crossVals = append(crossVals, cv)
			}
		case requiredUnlessTagValue:
			cv, err := buildConditionalCrossFieldValidator(
				key,
				value,
				strict,
				func(field string, expected string) crossFieldValidator {
					return newRequiredUnlessValidator(field, expected)
				},
			)
			if err != nil {
				return nil, nil, err
			}
			if cv != nil {
				crossVals = append(crossVals, cv)
			}
		default:
			return nil, nil, fmt.Errorf("%w: unknown validate tag %q", ErrInvalidTagFormat, part)
		}
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
