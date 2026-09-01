// Package prep provides preprocessing and validation for the file formats
// filesql reads: CSV, TSV, LTSV, JSON, JSONL, Parquet and Excel.
//
// prep complements filesql by providing data preprocessing before loading
// into SQLite. It uses struct tags for validation ("validate" tag) and
// preprocessing ("prep" tag).
//
// # Basic Usage
//
//	type Record struct {
//	    Name  string `prep:"trim" validate:"required"`
//	    Email string `prep:"trim,lowercase" validate:"email"`
//	    Age   int    `validate:"gte=0,lte=150"`
//	}
//
//	file, _ := os.Open("data.csv")
//	defer file.Close()
//
//	var records []Record
//	processor := prep.NewProcessor(prep.FileTypeCSV)
//	reader, result, err := processor.Process(file, &records)
//	if err != nil {
//	    log.Fatal(err)
//	}
//
//	// reader can be passed directly to filesql
//	// result.Errors contains validation errors with row/column information
//	// result.RowCount and result.ValidRowCount provide processing statistics
//
// # Streaming Output with ProcessToWriter
//
// For large datasets, ProcessToWriter writes preprocessed output directly
// to an io.Writer, avoiding the output buffer allocation:
//
//	var buf bytes.Buffer
//	result, err := processor.ProcessToWriter(file, &records, &buf)
//
// # Memory Usage
//
// prep holds the whole input in memory, so its peak scales with the size of the
// file rather than staying flat the way filesql's own chunked loading does.
// Three things are live at once: the parsed rows, one element of the struct
// slice per row, and, for Process, the buffer the preprocessed output is built
// in. ProcessToWriter removes the third of those and not the first two, since
// the struct slice is what the caller asked for and the rows are what a second
// pass over them needs.
//
// That is the design rather than an oversight: preprocessing and validation are
// separate passes over the same rows, and the row a validator reports on has to
// be somewhere when it does. A file large enough for this to matter is better
// loaded with filesql, which reads it in chunks, and validated afterwards with
// SQL over the loaded table.
//
// Format-specific limitations:
//   - XLSX: Only the first sheet is processed
//   - LTSV: Maximum line size is 10MB
//   - JSON/JSONL: Data has a single "data" column containing raw JSON strings
//
// A struct may cover a subset of the columns. A field naming a column the input
// does not have is refused with ErrUnknownColumn, since a zero-filled field
// cannot be told apart from a cell that is really empty; give such a field
// prep:"default=..." if it is meant to work without a column.
//
// # Supported File Formats
//
// prep supports the same formats as filesql:
//   - CSV (.csv)
//   - TSV (.tsv)
//   - LTSV (.ltsv)
//   - JSON (.json)
//   - JSONL (.jsonl)
//   - Parquet (.parquet)
//   - Excel (.xlsx)
//
// A compressed stream is the caller's to unwrap, the way it is for
// parser.Parse. Process reads the bytes it is given as the format it was
// constructed with, so a codec has to come off first:
//
//	f, _ := os.Open("data.csv.gz")
//	defer f.Close()
//	r, closeCodec, _ := filesql.NewCompressionHandler(filesql.CompressionGZ).CreateReader(f)
//	defer closeCodec()
//
//	reader, result, err := prep.NewProcessor(parser.CSV).Process(r, &records)
//
// filesql.NewCompressionFactory().CreateReaderForFile takes the codec from the
// name of a path instead, for a caller that has one.
//
// # Prep Tags
//
// The "prep" tag specifies preprocessing operations applied before validation.
// A comma separates one tag from the next, so a tag's own parameters are
// separated by a colon rather than a comma:
//
//   - trim, ltrim, rtrim: remove whitespace from both ends, the left, the right
//   - collapse_space: replace each run of whitespace with one space
//   - strip_newline: remove carriage returns and line feeds
//   - strip_html: remove HTML tags
//   - lowercase, uppercase: fold case
//   - normalize_unicode: normalize to NFC
//   - default=value: use value when the cell is empty
//   - nullify=value: read value as an empty cell
//   - prefix=value, suffix=value: add value at the front or the back
//   - replace=old:new: replace every occurrence of old
//   - regex_replace=pattern:replacement: replace every match of pattern
//   - truncate=N: keep the first N characters
//   - pad_left=N:char, pad_right=N:char: pad to N characters, with char or a
//     space
//   - trim_set=chars: remove any of chars from both ends
//   - keep_digits, keep_alpha: keep only digits, or only letters
//   - remove_digits, remove_alpha: remove digits, or letters
//   - coerce=int|float|bool: normalize the written form of a number or a
//     boolean. It rewrites how the value is written and not what it says, so a
//     spelling the loader keeps as text -- a leading zero, a literal past
//     int64, Go's own number syntax, a decimal a float64 cannot hold -- is left
//     alone
//   - fix_scheme=scheme: add scheme:// to a URL that has no scheme. A value
//     that names one already keeps it, however it is written, so mailto: and
//     tel: are left alone; the one exception is fix_scheme=https, which
//     rewrites an http scheme to https however that scheme is written
//
// A parameter that needs a comma or a colon of its own writes a backslash in
// front of it, which the parser drops: regex_replace=https?\://:scheme- reads
// the pattern as https?:// and the replacement as scheme-. A backslash anywhere
// else is itself, so a regular expression keeps its \d.
//
// A tag that needs a parameter and is given none is an invalid tag argument:
// WithStrictTagParsing reports it, and without that option it is ignored.
//
// # Validate Tags
//
// The "validate" tag specifies validation rules (following the
// go-playground/validator dialect):
//   - required: Field must not be empty
//   - email: Must be a valid email address
//   - url: Must be a valid URL
//   - And many more...
//
// A field tagged validate:"-" is validated by nothing, as in the dialect. Its
// prep tag still runs.
//
// An empty value passes every validator except required and the required
// family: an empty cell is how CSV spells a missing value, so a column is
// optional unless one of those tags says otherwise. This is where the dialect
// is deliberately left — that library fails most validators on an empty value.
// The rule reaches the cross-field comparisons too, on either side: a
// comparison is skipped as soon as one of the two cells is empty, since a value
// has no order against a value that is not there. The required family —
// required_if, required_unless, required_with, required_with_all,
// required_without and required_without_all — is the exception: each of those
// tags runs on an empty cell and reports it when its condition holds, since
// deciding whether an empty cell is allowed is what they are for. The excluded
// family runs on every row for the same reason, but reads the rule the other
// way: it only ever reports a cell that carries a value.
//
// The comparison tags follow the field they land on, as the dialect defines
// them: on a string field, eq and ne compare the string itself and gt, gte,
// lt, lte, min and max compare the character count, while on any other field
// all of them compare the numeric value and len means the value equals the
// parameter. Two numbers that both spell integers are compared as integers, so
// a pair past the point where a float64 stops spelling every integer exactly
// is ordered rather than rounded onto one number; anything else is compared as
// a float64. The cross-field tags eqfield, nefield, gtfield, gtefield, ltfield
// and ltefield follow the field the same way, except that on a string field
// they order the strings rather than measure them, so ltfield says a date range
// runs forwards. That is the second place this package leaves the dialect,
// which compares string lengths there and so cannot express a date range at
// all. required_if and required_unless take field and value pairs, as many as
// the tag names, and a value holding a space is written in single quotes:
// required_if=Kind paid Tier 'gold member' asks for a value only when both
// cells match, while required_unless=Kind paid Tier gold asks for one unless
// either cell matches. required_with and its three siblings take a list of
// field names instead: required_with fires when any of them carries a value
// and required_with_all when all of them do, while required_without fires when
// any of them is empty and required_without_all when all of them are.
//
// excluded_if, excluded_unless, excluded_with, excluded_with_all,
// excluded_without and excluded_without_all negate that family: each names the
// condition under which the cell must be empty, and reads its parameter the way
// the required tag it mirrors does. excluded_if=Kind free forbids a value when
// every pair matches, excluded_unless=Kind paid Tier gold forbids one unless
// either pair matches, excluded_with=Email forbids one when any named field
// carries a value and excluded_without=Email when any of them is empty, with
// the _all spellings asking for every named field rather than any.
//
// boolean accepts what strconv.ParseBool accepts, which is also how a
// bool struct field is filled. numeric accepts an optionally signed decimal
// and number accepts digits alone, as the dialect defines them. The dialect
// spells the letters-and-digits tag alphanum; alphanumeric names the same
// validator, because this package answered only to that spelling before.
//
// Some validators are deliberately stricter or wider than the dialect. uri
// requires a scheme and something after it, so neither a relative reference
// such as /a/b nor a bare http:// is a URI. fqdn, hostname and
// hostname_rfc1123 all refuse a label ending in a hyphen, which none of the
// dialect's three patterns does, and all three cap a label at 63 characters and
// a whole name at 253, as the RFCs do and as the dialect's patterns do not.
// hostname reads RFC 952, so a one-character label such as a is a hostname,
// which the dialect's pattern refuses, and every label must begin with a
// letter, so 2.example is not one. ulid requires a timestamp the format
// can hold. uuid3 requires the variant nibble that uuid4 and uuid5 require,
// and all three accept upper case, as uuid does; the uuid_rfc4122,
// uuid3_rfc4122, uuid4_rfc4122 and uuid5_rfc4122 spellings name the same four
// checks, so uuid3_rfc4122 requires the variant nibble too, where the dialect
// leaves it unconstrained. dns_rfc1035_label is the dialect's
// reading of an RFC 1035 label -- a lowercase letter, then lowercase letters,
// digits and hyphens, not ending in a hyphen -- capped at 63 characters, which
// is the RFC's own limit on a label and which the dialect's pattern leaves out.
// The RFC's grammar admits upper case and DNS compares labels without regard to
// it; the lowercase reading is the dialect's and is also what Kubernetes
// enforces on resource names, which is where such columns come from, so a
// column holding mixed case wants prep:"lowercase" before this tag. iscolor
// passes when hexcolor, rgb, rgba, hsl or hsla does, as the dialect's alias
// defines it. hostname_port takes a
// bracketed IPv6 address and refuses a bare port. ip, ipv4 and ipv6 are the
// dialect's spellings of ip_addr, ip4_addr and ip6_addr and build the same
// checks, so an IPv4-mapped address such as ::ffff:192.0.2.1 satisfies ipv4 and
// not ipv6. port is defined on a numeric field in the dialect; on a cell it is
// ASCII digits alone naming a port from 1 to 65535, so 0080 is port 80 while
// +80 and 0x50 are not ports.
//
// e164 takes the number with or without the leading plus, since the plus is a
// notation prefix rather than part of the number and a spreadsheet export
// strips it, where the dialect requires it; the first digit must be 1 through
// 9, so +0123456789, which the dialect accepts, is not an E.164 number.
//
// json accepts any JSON value, so a bare number is one. timezone names an IANA
// zone that time.LoadLocation loads, and Local is refused in every casing,
// since it names the host's own zone rather than a fixed one. semver is
// Semantic Versioning 2.0.0, so 1.2.3 is a version and v1.2.3 is not. base32,
// base64, base64url and base64rawurl check the RFC 4648 alphabet before
// decoding, because Go's decoders skip carriage returns and line feeds, which
// would otherwise let a value carrying a newline through. They then decode
// strictly, so a value whose trailing pad bits are not zero is refused although
// the dialect's patterns accept it: RFC 4648 lets a decoder reject one, and no
// conformant encoder produces one. oneofci is oneof
// compared without regard to case, and reads its candidates the same way,
// single quotes included.
//
// credit_card, luhn_checksum, isbn, isbn10, isbn13 and issn verify a check
// digit as well as a shape, which is the whole point of them: a mistyped
// identifier still looks plausible and only the check digit says otherwise.
// credit_card groups its digits with single spaces, as the dialect does, so a
// number written with hyphens is a column for prep:"keep_digits" first;
// luhn_checksum takes the digits alone; isbn10 and isbn13 remove hyphens and
// spaces themselves, at most as many as the width is printed with, so a value
// carrying more of them is not an ISBN however its digits read; and issn
// requires the hyphen the standard prints. md5,
// sha256, sha384 and sha512 are lowercase hexadecimal of exactly 32, 64, 96 and
// 128 characters, so an uppercase spelling is refused.
//
// iso3166_1_alpha2, iso3166_1_alpha3 and iso3166_1_alpha_numeric look a cell
// up in the officially assigned ISO 3166-1 codes, and country_code passes when
// any of the three does. iso4217 and iso4217_numeric look it up in the active
// ISO 4217 currency codes, alphabetic and numeric. Every lookup is exact, as it
// is in the dialect, so JP is a country code and jp is not, and a numeric code
// keeps the leading zeros the standard prints, so 032 is Argentina, 008 is the
// lek, and 32 and 8 are nothing. The user-assigned range is not
// included, so XK, widely used for Kosovo and never assigned, is not a country
// code here.
//
// unique means something different here than it does in the dialect, and
// deliberately: there it describes one slice, array or map field, and a row
// here is a flat struct, so it is read as uniqueness of a column across the
// rows of one processing run. The first occurrence of a value is valid and
// every later one is reported, naming the value and the row it first appeared
// on. An empty cell never counts as a duplicate, since an empty cell is a
// missing value and two of them are two absences rather than two equal values.
// The comparison is exact string equality on the value preprocessing produced,
// so prep:"trim,lowercase" beside it gives case-insensitive deduplication
// without unique needing options of its own. The seen values are held for the
// length of one Process call and no longer, so two calls never see each
// other's rows; memory grows with the number of distinct values in the column.
//
// Importing prep embeds the IANA time zone database through time/tzdata, which
// the timezone tag needs on a platform that ships none, and which costs a few
// hundred KB of binary size. filesql itself does not import prep.
//
// See https://pkg.go.dev/github.com/nao1215/filesql/prep for the complete list of supported validators.
package prep
