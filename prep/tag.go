package prep

// Struct tag names
const (
	// validateTagName is the struct tag name for validation rules
	validateTagName = "validate"
	// prepTagName is the struct tag name for preprocessing rules
	prepTagName = "prep"
	// nameTagName is the struct tag name for column name mapping
	nameTagName = "name"
)

// Validation tag values
const (
	// omitemptyTagValue is the tag value for skipping validation on empty values.
	// When present, subsequent validators are skipped if the value is empty.
	omitemptyTagValue = "omitempty"
	// requiredTagValue is the tag value for required validation
	requiredTagValue = "required"
	// booleanTagValue is the tag value for boolean validation
	booleanTagValue = "boolean"
	// alphaTagValue is the tag value for alpha only validation
	alphaTagValue = "alpha"
	// alphaSpaceTagValue is the tag value for alpha with spaces validation
	alphaSpaceTagValue = "alphaspace"
	// alphaUnicodeTagValue is the tag value for unicode alpha validation
	alphaUnicodeTagValue = "alphaunicode"
	// numericTagValue is the tag value for numeric validation
	numericTagValue = "numeric"
	// numberTagValue is the tag value for number (int/decimal) validation
	numberTagValue = "number"
	// alphanumericTagValue is the tag value for alphanumeric validation
	alphanumericTagValue = "alphanumeric"
	// alphanumericUnicodeTagValue is the tag value for unicode alphanumeric validation
	alphanumericUnicodeTagValue = "alphanumunicode"
	// equalTagValue is the tag value for equal validation
	equalTagValue = "eq"
	// notEqualTagValue is the tag value for not equal validation
	notEqualTagValue = "ne"
	// greaterThanTagValue is the tag value for greater than validation
	greaterThanTagValue = "gt"
	// greaterThanEqualTagValue is the tag value for greater than or equal validation
	greaterThanEqualTagValue = "gte"
	// lessThanTagValue is the tag value for less than validation
	lessThanTagValue = "lt"
	// lessThanEqualTagValue is the tag value for less than or equal validation
	lessThanEqualTagValue = "lte"
	// minTagValue is the tag value for minimum validation
	minTagValue = "min"
	// maxTagValue is the tag value for maximum validation
	maxTagValue = "max"
	// lengthTagValue is the tag value for length validation
	lengthTagValue = "len"
	// oneOfTagValue is the tag value for one of validation
	oneOfTagValue = "oneof"
	// lowercaseValidatorTagValue is the tag value for lowercase validation
	lowercaseValidatorTagValue = "lowercase"
	// uppercaseValidatorTagValue is the tag value for uppercase validation
	uppercaseValidatorTagValue = "uppercase"
	// asciiTagValue is the tag value for ASCII validation
	asciiTagValue = "ascii"
	// printASCIITagValue is the tag value for printable ASCII validation
	printASCIITagValue = "printascii"
	// emailTagValue is the tag value for email validation
	emailTagValue = "email"
	// uriTagValue is the tag value for URI validation
	uriTagValue = "uri"
	// urlTagValue is the tag value for URL validation
	urlTagValue = "url"
	// httpURLTagValue is the tag value for HTTP/HTTPS URL validation
	httpURLTagValue = "http_url"
	// httpsURLTagValue is the tag value for HTTPS-only URL validation
	httpsURLTagValue = "https_url"
	// urlEncodedTagValue is the tag value for URL encoded validation
	urlEncodedTagValue = "url_encoded"
	// dataURITagValue is the tag value for data URI validation
	dataURITagValue = "datauri"
	// ipAddrTagValue is the tag value for IP address validation
	ipAddrTagValue = "ip_addr"
	// ip4AddrTagValue is the tag value for IPv4 address validation
	ip4AddrTagValue = "ip4_addr"
	// ip6AddrTagValue is the tag value for IPv6 address validation
	ip6AddrTagValue = "ip6_addr"
	// cidrTagValue is the tag value for CIDR validation
	cidrTagValue = "cidr"
	// cidrv4TagValue is the tag value for IPv4 CIDR validation
	cidrv4TagValue = "cidrv4"
	// cidrv6TagValue is the tag value for IPv6 CIDR validation
	cidrv6TagValue = "cidrv6"
	// uuidTagValue is the tag value for UUID validation
	uuidTagValue = "uuid"
	// fqdnTagValue is the tag value for FQDN validation
	fqdnTagValue = "fqdn"
	// hostnameTagValue is the tag value for hostname (RFC 952) validation
	hostnameTagValue = "hostname"
	// hostnameRFC1123TagValue is the tag value for hostname (RFC 1123) validation
	hostnameRFC1123TagValue = "hostname_rfc1123"
	// hostnamePortTagValue is the tag value for hostname:port validation
	hostnamePortTagValue = "hostname_port"
	// startsWithTagValue is the tag value for startswith validation
	startsWithTagValue = "startswith"
	// startsNotWithTagValue is the tag value for startsnotwith validation
	startsNotWithTagValue = "startsnotwith"
	// endsWithTagValue is the tag value for endswith validation
	endsWithTagValue = "endswith"
	// endsNotWithTagValue is the tag value for endsnotwith validation
	endsNotWithTagValue = "endsnotwith"
	// containsTagValue is the tag value for contains validation
	containsTagValue = "contains"
	// containsAnyTagValue is the tag value for containsany validation
	containsAnyTagValue = "containsany"
	// containsRuneTagValue is the tag value for containsrune validation
	containsRuneTagValue = "containsrune"
	// excludesTagValue is the tag value for excludes validation
	excludesTagValue = "excludes"
	// excludesAllTagValue is the tag value for excludesall validation
	excludesAllTagValue = "excludesall"
	// excludesRuneTagValue is the tag value for excludesrune validation
	excludesRuneTagValue = "excludesrune"
	// multibyteTagValue is the tag value for multibyte validation
	multibyteTagValue = "multibyte"
	// equalIgnoreCaseTagValue is the tag value for case-insensitive equal validation
	equalIgnoreCaseTagValue = "eq_ignore_case"
	// notEqualIgnoreCaseTagValue is the tag value for case-insensitive not equal validation
	notEqualIgnoreCaseTagValue = "ne_ignore_case"

	// Conditional required validators
	// requiredIfTagValue is the tag value for required if another field equals a value
	requiredIfTagValue = "required_if"
	// requiredUnlessTagValue is the tag value for required unless another field equals a value
	requiredUnlessTagValue = "required_unless"
	// requiredWithTagValue is the tag value for required if another field is present
	requiredWithTagValue = "required_with"
	// requiredWithoutTagValue is the tag value for required if another field is not present
	requiredWithoutTagValue = "required_without"

	// Date/time validator
	// datetimeTagValue is the tag value for datetime format validation
	datetimeTagValue = "datetime"

	// Phone number validator
	// e164TagValue is the tag value for E.164 phone number validation
	e164TagValue = "e164"

	// Geolocation validators
	// latitudeTagValue is the tag value for latitude validation
	latitudeTagValue = "latitude"
	// longitudeTagValue is the tag value for longitude validation
	longitudeTagValue = "longitude"

	// UUID variant validators
	// uuid3TagValue is the tag value for UUID version 3 validation
	uuid3TagValue = "uuid3"
	// uuid4TagValue is the tag value for UUID version 4 validation
	uuid4TagValue = "uuid4"
	// uuid5TagValue is the tag value for UUID version 5 validation
	uuid5TagValue = "uuid5"
	// ulidTagValue is the tag value for ULID validation
	ulidTagValue = "ulid"

	// Hexadecimal and color validators
	// hexadecimalTagValue is the tag value for hexadecimal validation
	hexadecimalTagValue = "hexadecimal"
	// hexColorTagValue is the tag value for hex color validation
	hexColorTagValue = "hexcolor"
	// rgbTagValue is the tag value for RGB color validation
	rgbTagValue = "rgb"
	// rgbaTagValue is the tag value for RGBA color validation
	rgbaTagValue = "rgba"
	// hslTagValue is the tag value for HSL color validation
	hslTagValue = "hsl"
	// hslaTagValue is the tag value for HSLA color validation
	hslaTagValue = "hsla"

	// Network validators
	// macTagValue is the tag value for MAC address validation
	macTagValue = "mac"

	// Cross-field validation tag values
	// eqFieldTagValue is the tag value for equal to another field validation
	eqFieldTagValue = "eqfield"
	// neFieldTagValue is the tag value for not equal to another field validation
	neFieldTagValue = "nefield"
	// gtFieldTagValue is the tag value for greater than another field validation
	gtFieldTagValue = "gtfield"
	// gteFieldTagValue is the tag value for greater than or equal to another field validation
	gteFieldTagValue = "gtefield"
	// ltFieldTagValue is the tag value for less than another field validation
	ltFieldTagValue = "ltfield"
	// lteFieldTagValue is the tag value for less than or equal to another field validation
	lteFieldTagValue = "ltefield"
	// fieldContainsTagValue is the tag value for field contains another field's value validation
	fieldContainsTagValue = "fieldcontains"
	// fieldExcludesTagValue is the tag value for field excludes another field's value validation
	fieldExcludesTagValue = "fieldexcludes"
)

// Preprocessing tag values
const (
	// Basic preprocessors
	// trimTagValue is the tag value for trim preprocessing
	trimTagValue = "trim"
	// ltrimTagValue is the tag value for left trim preprocessing
	ltrimTagValue = "ltrim"
	// rtrimTagValue is the tag value for right trim preprocessing
	rtrimTagValue = "rtrim"
	// lowercaseTagValue is the tag value for lowercase preprocessing
	lowercaseTagValue = "lowercase"
	// uppercaseTagValue is the tag value for uppercase preprocessing
	uppercaseTagValue = "uppercase"
	// defaultTagValue is the tag value prefix for default value preprocessing
	defaultTagValue = "default"

	// String transformation preprocessors
	// replaceTagValue is the tag value for replace preprocessing (replace=old:new)
	replaceTagValue = "replace"
	// prefixTagValue is the tag value for prefix preprocessing (prefix=value)
	prefixTagValue = "prefix"
	// suffixTagValue is the tag value for suffix preprocessing (suffix=value)
	suffixTagValue = "suffix"
	// truncateTagValue is the tag value for truncate preprocessing (truncate=N)
	truncateTagValue = "truncate"
	// stripHTMLTagValue is the tag value for HTML tag removal preprocessing
	stripHTMLTagValue = "strip_html"
	// stripNewlineTagValue is the tag value for newline removal preprocessing
	stripNewlineTagValue = "strip_newline"
	// collapseSpaceTagValue is the tag value for collapsing multiple spaces into one
	collapseSpaceTagValue = "collapse_space"

	// Character filtering preprocessors
	// removeDigitsTagValue is the tag value for removing all digits
	removeDigitsTagValue = "remove_digits"
	// removeAlphaTagValue is the tag value for removing all alphabetic characters
	removeAlphaTagValue = "remove_alpha"
	// keepDigitsTagValue is the tag value for keeping only digits
	keepDigitsTagValue = "keep_digits"
	// keepAlphaTagValue is the tag value for keeping only alphabetic characters
	keepAlphaTagValue = "keep_alpha"
	// trimSetTagValue is the tag value for trimming specified characters (trim_set=chars)
	trimSetTagValue = "trim_set"

	// Padding preprocessors
	// padLeftTagValue is the tag value for left padding (pad_left=N,char)
	padLeftTagValue = "pad_left"
	// padRightTagValue is the tag value for right padding (pad_right=N,char)
	padRightTagValue = "pad_right"

	// Advanced preprocessors
	// normalizeUnicodeTagValue is the tag value for Unicode normalization (NFC form)
	normalizeUnicodeTagValue = "normalize_unicode"
	// nullifyTagValue is the tag value for treating specific string as empty (nullify=value)
	nullifyTagValue = "nullify"
	// coerceTagValue is the tag value for type coercion (coerce=int|float|bool)
	coerceTagValue = "coerce"
	// fixSchemeTagValue is the tag value for URL scheme completion (fix_scheme=https)
	fixSchemeTagValue = "fix_scheme"
	// regexReplaceTagValue is the tag value for regex-based replacement (regex_replace=pattern:replacement)
	regexReplaceTagValue = "regex_replace"
)
