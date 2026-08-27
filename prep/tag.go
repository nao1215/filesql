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
	// validateSkipTagValue is the whole-tag value the go-playground dialect
	// reads as "validate nothing on this field".
	validateSkipTagValue = "-"
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
	// alphanumTagValue is the tag value for alphanumeric validation, spelled
	// as the dialect spells it.
	alphanumTagValue = "alphanum"
	// alphanumericTagValue is the spelling prep accepted first. It builds the
	// same validator.
	alphanumericTagValue = "alphanumeric"
	// alphanumSpaceTagValue is the tag value for alphanumeric with spaces validation
	alphanumSpaceTagValue = "alphanumspace"
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
	// ipTagValue is the tag value for IP address validation, the spelling the
	// go-playground dialect documents first.
	ipTagValue = "ip"
	// ipv4TagValue is the tag value for IPv4 address validation, the dialect's
	// spelling of ip4_addr.
	ipv4TagValue = "ipv4"
	// ipv6TagValue is the tag value for IPv6 address validation, the dialect's
	// spelling of ip6_addr.
	ipv6TagValue = "ipv6"
	// portTagValue is the tag value for TCP/UDP port number validation
	portTagValue = "port"
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
	// requiredWithAllTagValue is the tag value for required if every named field is present
	requiredWithAllTagValue = "required_with_all"
	// requiredWithoutAllTagValue is the tag value for required if every named field is absent
	requiredWithoutAllTagValue = "required_without_all"

	// Conditional excluded validators, the negations of the required family
	// excludedIfTagValue is the tag value for forbidden if every named field equals the value paired with it
	excludedIfTagValue = "excluded_if"
	// excludedUnlessTagValue is the tag value for forbidden unless every named field equals the value paired with it
	excludedUnlessTagValue = "excluded_unless"
	// excludedWithTagValue is the tag value for forbidden if any named field is present
	excludedWithTagValue = "excluded_with"
	// excludedWithAllTagValue is the tag value for forbidden if every named field is present
	excludedWithAllTagValue = "excluded_with_all"
	// excludedWithoutTagValue is the tag value for forbidden if any named field is absent
	excludedWithoutTagValue = "excluded_without"
	// excludedWithoutAllTagValue is the tag value for forbidden if every named field is absent
	excludedWithoutAllTagValue = "excluded_without_all"

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
	// The _rfc4122 spellings the dialect defines beside the plain ones. They
	// name the same checks and build the same validators.
	// uuidRFC4122TagValue is the tag value for UUID validation
	uuidRFC4122TagValue = "uuid_rfc4122"
	// uuid3RFC4122TagValue is the tag value for UUID version 3 validation
	uuid3RFC4122TagValue = "uuid3_rfc4122"
	// uuid4RFC4122TagValue is the tag value for UUID version 4 validation
	uuid4RFC4122TagValue = "uuid4_rfc4122"
	// uuid5RFC4122TagValue is the tag value for UUID version 5 validation
	uuid5RFC4122TagValue = "uuid5_rfc4122"
	// dnsRFC1035LabelTagValue is the tag value for a single DNS label
	dnsRFC1035LabelTagValue = "dns_rfc1035_label"

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
	// isColorTagValue is the tag value for a color in any of the five
	// notations, which is how the go-playground dialect defines its alias.
	isColorTagValue = "iscolor"

	// Structured format validators
	// jsonTagValue is the tag value for JSON document validation
	jsonTagValue = "json"
	// timezoneTagValue is the tag value for IANA time zone name validation
	timezoneTagValue = "timezone"
	// semverTagValue is the tag value for Semantic Versioning 2.0.0 validation
	semverTagValue = "semver"

	// RFC 4648 encoding validators
	// base32TagValue is the tag value for base32 validation
	base32TagValue = "base32"
	// base64TagValue is the tag value for base64 validation
	base64TagValue = "base64"
	// base64URLTagValue is the tag value for URL-safe base64 validation
	base64URLTagValue = "base64url"
	// base64RawURLTagValue is the tag value for unpadded URL-safe base64 validation
	base64RawURLTagValue = "base64rawurl"

	// oneOfCITagValue is the tag value for case-insensitive one of validation
	oneOfCITagValue = "oneofci"

	// Checksummed identifier validators
	// creditCardTagValue is the tag value for credit card number validation
	creditCardTagValue = "credit_card"
	// luhnChecksumTagValue is the tag value for Luhn checksum validation
	luhnChecksumTagValue = "luhn_checksum"
	// isbnTagValue is the tag value for ISBN-10 or ISBN-13 validation
	isbnTagValue = "isbn"
	// isbn10TagValue is the tag value for ISBN-10 validation
	isbn10TagValue = "isbn10"
	// isbn13TagValue is the tag value for ISBN-13 validation
	isbn13TagValue = "isbn13"
	// issnTagValue is the tag value for ISSN validation
	issnTagValue = "issn"

	// uniqueTagValue is the tag value for column-wide uniqueness. It is read
	// at parse time and builds no validator; see uniqueMarkerValidator.
	uniqueTagValue = "unique"

	// Country and currency code validators
	// iso3166Alpha2TagValue is the tag value for ISO 3166-1 alpha-2 validation
	iso3166Alpha2TagValue = "iso3166_1_alpha2"
	// iso3166Alpha3TagValue is the tag value for ISO 3166-1 alpha-3 validation
	iso3166Alpha3TagValue = "iso3166_1_alpha3"
	// iso3166NumericTagValue is the tag value for ISO 3166-1 numeric validation
	iso3166NumericTagValue = "iso3166_1_alpha_numeric"
	// countryCodeTagValue is the tag value for a country code in any of the three forms
	countryCodeTagValue = "country_code"
	// iso4217TagValue is the tag value for ISO 4217 currency code validation
	iso4217TagValue = "iso4217"
	// iso4217NumericTagValue is the tag value for ISO 4217 numeric currency code validation
	iso4217NumericTagValue = "iso4217_numeric"

	// Message digest validators
	// md5TagValue is the tag value for MD5 digest validation
	md5TagValue = "md5"
	// sha256TagValue is the tag value for SHA-256 digest validation
	sha256TagValue = "sha256"
	// sha384TagValue is the tag value for SHA-384 digest validation
	sha384TagValue = "sha384"
	// sha512TagValue is the tag value for SHA-512 digest validation
	sha512TagValue = "sha512"

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
	// padLeftTagValue is the tag value for left padding (pad_left=N:char). The
	// comma separates one tag from the next, so a tag's own parameters cannot
	// use it.
	padLeftTagValue = "pad_left"
	// padRightTagValue is the tag value for right padding (pad_right=N:char)
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
