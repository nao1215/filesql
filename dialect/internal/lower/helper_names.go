package lower

import (
	"slices"
	"strconv"
	"strings"
)

// helperArity is every function the runtime registers with the driver, with the
// number of arguments it takes; -1 is a function that takes any number.
//
// Two things read it. The SAFE prefix needs the names: a name this package has a
// helper for is wrapped so its error becomes NULL, and a name it does not is
// left as the plain call SQLite will answer or refuse for itself. The counts are
// what let a call with the wrong number of arguments be refused under the name
// the caller wrote, rather than reaching the driver and failing there under the
// helper's.
//
// The table is here rather than read from the runtime package because the
// dependency runs the other way: translating a query would otherwise register
// functions with the driver. A test in lower_test holds the two together, so a
// helper added there, or given a different arity there, and not updated here is a
// failure rather than a silent change of behavior.
var helperArity = map[string]int{ //nolint:gochecknoglobals // a generated table
	"acosd":                        1,
	"addtime":                      2,
	"age":                          2,
	"ascii":                        1,
	"asind":                        1,
	"atan2d":                       2,
	"atand":                        1,
	"bin":                          1,
	"bit_count":                    1,
	"bit_length":                   1,
	"cbrt":                         1,
	"erf":                          1,
	"erfc":                         1,
	"chr":                          1,
	"clock_timestamp":              0,
	"contains_substr":              2,
	"conv":                         3,
	"convert_tz":                   3,
	"cosd":                         1,
	"cot":                          1,
	"mysql_pow":                    2,
	"postgresql_sqrt":              1,
	"postgresql_ln":                1,
	"postgresql_log":               -1,
	"postgresql_exp":               1,
	"postgresql_power":             2,
	"postgresql_acos":              1,
	"postgresql_asin":              1,
	"postgresql_acosh":             1,
	"postgresql_atanh":             1,
	"postgresql_cot":               1,
	"cotd":                         1,
	"coth":                         1,
	"crc32":                        1,
	"csc":                          1,
	"csch":                         1,
	"curdate":                      0,
	"current_datetime":             -1,
	"curtime":                      0,
	"date_bin":                     3,
	"date_diff":                    3,
	"date_format":                  2,
	"date_from_unix_date":          1,
	"date_part":                    2,
	"date_trunc":                   2,
	"date_trunc_part":              2,
	"datediff":                     2,
	"datetime_diff":                3,
	"day":                          1,
	"dayname":                      1,
	"dayofmonth":                   1,
	"dayofweek":                    1,
	"dayofyear":                    1,
	"decode":                       2,
	"dialect_replace":              3,
	"dialect_round":                2,
	"dialect_round_even":           -1,
	"div":                          2,
	"edit_distance":                -1,
	"elt":                          -1,
	"encode":                       2,
	"ends_with":                    2,
	"error":                        1,
	"export_set":                   -1,
	"factorial":                    1,
	"field":                        -1,
	"find_in_set":                  2,
	"format_date":                  2,
	"format_datetime":              2,
	"format_timestamp":             2,
	"from_base32":                  1,
	"from_base64":                  1,
	"from_days":                    1,
	"from_hex":                     1,
	"from_unixtime":                -1,
	"gcd":                          2,
	"gen_random_uuid":              0,
	"generate_uuid":                0,
	"get_byte":                     2,
	"googlesql_bit_and":            2,
	"googlesql_bit_not":            1,
	"googlesql_bit_or":             2,
	"googlesql_bit_xor":            2,
	"googlesql_shift_left":         2,
	"googlesql_shift_right":        2,
	"googlesql_cast":               2,
	"googlesql_date":               -1,
	"googlesql_date_part":          2,
	"googlesql_datetime":           -1,
	"googlesql_divide":             2,
	"googlesql_format":             -1,
	"googlesql_instr":              -1,
	"googlesql_last_day":           -1,
	"googlesql_left":               2,
	"googlesql_lpad":               -1,
	"googlesql_md5":                1,
	"googlesql_mod":                2,
	"googlesql_repeat":             2,
	"googlesql_right":              2,
	"googlesql_rpad":               -1,
	"googlesql_safe_cast":          2,
	"googlesql_sha1":               1,
	"googlesql_soundex":            1,
	"googlesql_string":             1,
	"googlesql_substr":             -1,
	"googlesql_time":               -1,
	"googlesql_timestamp":          -1,
	"greatest":                     -1,
	"hour":                         1,
	"ieee_divide":                  2,
	"if":                           3,
	"inet_aton":                    1,
	"inet_ntoa":                    1,
	"initcap":                      1,
	"interval_add":                 3,
	"interval_text_add":            3,
	"is_inf":                       1,
	"is_uuid":                      1,
	"is_ipv4":                      1,
	"is_ipv6":                      1,
	"is_nan":                       1,
	"isfinite":                     1,
	"json_contains":                -1,
	"json_length":                  -1,
	"json_unquote":                 1,
	"last_day":                     1,
	"lcase":                        1,
	"lcm":                          2,
	"least":                        -1,
	"left":                         2,
	"like_insensitive":             -1,
	"like_sensitive":               -1,
	"locate":                       -1,
	"lpad":                         -1,
	"make_date":                    3,
	"make_interval":                -1,
	"make_set":                     -1,
	"make_time":                    3,
	"make_timestamp":               6,
	"makedate":                     2,
	"maketime":                     3,
	"md5":                          1,
	"microsecond":                  1,
	"mid":                          -1,
	"min_scale":                    1,
	"minute":                       1,
	"month":                        1,
	"monthname":                    1,
	"mysql_ascii":                  1,
	"mysql_bit_and":                2,
	"mysql_bit_not":                1,
	"mysql_bit_or":                 2,
	"mysql_bit_xor":                2,
	"mysql_cast":                   2,
	"mysql_ceil":                   1,
	"mysql_char":                   -1,
	"mysql_date":                   1,
	"mysql_date_diff":              3,
	"mysql_date_part":              2,
	"mysql_divide":                 2,
	"mysql_exp":                    1,
	"mysql_floor":                  1,
	"mysql_format":                 2,
	"mysql_greatest":               -1,
	"mysql_hex":                    1,
	"mysql_insert":                 4,
	"mysql_interval":               -1,
	"mysql_interval_add":           3,
	"mysql_number":                 1,
	"mysql_interval_compound":      4,
	"mysql_json_type":              1,
	"mysql_least":                  -1,
	"mysql_left":                   2,
	"mysql_ln":                     1,
	"mysql_log10":                  1,
	"mysql_log2":                   1,
	"mysql_lpad":                   -1,
	"mysql_mod":                    2,
	"mysql_nullif":                 2,
	"mysql_ord":                    1,
	"mysql_quote":                  1,
	"mysql_regexp":                 2,
	"mysql_regexp_replace":         -1,
	"mysql_right":                  2,
	"mysql_rpad":                   -1,
	"mysql_shift_left":             2,
	"mysql_shift_right":            2,
	"mysql_sign":                   1,
	"mysql_soundex":                1,
	"mysql_sqrt":                   1,
	"mysql_substr":                 -1,
	"mysql_text":                   1,
	"mysql_time_of_day":            1,
	"mysql_timediff":               2,
	"mysql_timestamp":              -1,
	"mysql_unhex":                  1,
	"mysql_week":                   -1,
	"mysql_weekofyear":             1,
	"mysql_yearweek":               -1,
	"normalize":                    -1,
	"normalize_and_casefold":       -1,
	"now":                          0,
	"num_nonnulls":                 -1,
	"num_nulls":                    -1,
	"oct":                          1,
	"overlay":                      -1,
	"parse_date":                   2,
	"parse_datetime":               2,
	"parse_timestamp":              2,
	"period_add":                   2,
	"period_diff":                  2,
	"pg_typeof":                    1,
	"postgresql_bit_xor":           2,
	"postgresql_cast":              2,
	"postgresql_date_add":          2,
	"postgresql_date_diff":         2,
	"postgresql_divide":            2,
	"postgresql_format":            -1,
	"postgresql_greatest":          -1,
	"postgresql_json_typeof":       1,
	"postgresql_least":             -1,
	"postgresql_lpad":              -1,
	"postgresql_mod":               2,
	"postgresql_random":            0,
	"postgresql_regexp_replace":    -1,
	"postgresql_rpad":              -1,
	"postgresql_substr":            -1,
	"postgresql_substring_from":    2,
	"postgresql_to_hex":            1,
	"quarter":                      1,
	"quote_ident":                  1,
	"quote_literal":                1,
	"quote_nullable":               1,
	"rand":                         0,
	"regexp":                       2,
	"regexp_contains":              2,
	"regexp_count":                 -1,
	"regexp_extract":               2,
	"regexp_instr":                 -1,
	"regexp_like":                  -1,
	"regexp_replace":               -1,
	"regexp_substr":                -1,
	"repeat":                       2,
	"reverse":                      1,
	"right":                        2,
	"rpad":                         -1,
	"safe_add":                     2,
	"safe_convert_bytes_to_string": 1,
	"safe_call":                    -1,
	"safe_divide":                  2,
	"safe_multiply":                2,
	"safe_negate":                  1,
	"safe_subtract":                2,
	"scale":                        1,
	"sec":                          1,
	"sec_to_time":                  1,
	"sech":                         1,
	"second":                       1,
	"set_byte":                     3,
	"sha1":                         1,
	"sha2":                         2,
	"sha224":                       1,
	"sha256":                       1,
	"sha384":                       1,
	"sha512":                       1,
	"similar_substring":            3,
	"similar_to":                   2,
	"sind":                         1,
	"space":                        1,
	"split_part":                   3,
	"starts_with":                  2,
	"statement_timestamp":          0,
	"str_to_date":                  2,
	"strcmp":                       2,
	"strict_concat":                -1,
	"strpos":                       2,
	"substring_index":              3,
	"subtime":                      2,
	"tand":                         1,
	"time_add":                     3,
	"time_diff":                    3,
	"time_format":                  2,
	"time_to_sec":                  1,
	"time_trunc":                   2,
	"timeofday":                    0,
	"timestamp_diff":               3,
	"timestamp_micros":             1,
	"timestamp_millis":             1,
	"timestamp_seconds":            1,
	"to_base32":                    1,
	"to_base64":                    1,
	"to_char":                      2,
	"to_date":                      2,
	"to_days":                      1,
	"to_hex":                       1,
	"to_json_string":               1,
	"to_number":                    2,
	"to_seconds":                   1,
	"to_timestamp":                 -1,
	"transaction_timestamp":        0,
	"translate":                    3,
	"trim_scale":                   1,
	"trunc_scale":                  2,
	"truncate":                     2,
	"ucase":                        1,
	"unicode_lower":                1,
	"unicode_upper":                1,
	"unix_date":                    1,
	"unix_micros":                  1,
	"unix_millis":                  1,
	"unix_seconds":                 1,
	"unix_timestamp":               -1,
	"weekday":                      1,
	"width_bucket":                 4,
	"year":                         1,
	"sysdate":                      0,
	"utc_date":                     0,
	"utc_time":                     0,
	"utc_timestamp":                0,
}

// builtinArity is how many arguments SQLite's own functions take, for the names
// a lowering renames a call onto. Without an entry the count is unchecked, and
// SQLite then answers about the name it was given rather than the one the
// caller wrote: CHAR_LENGTH() became length() and the refusal said "wrong
// number of arguments to function length()", about a function nowhere in the
// query. The counts are what SQLite accepts, measured against it rather than
// read off the documentation.
//
// The keys are lower case because that is how the check spells the name it
// looks up. A variadic name is listed with no counts, which says it takes any:
// the listing is what TestEveryRenameTargetHasAnArity reads, so a name added to
// a lowering has to be given an answer here even when the answer is "any".
// builtinCounts is which argument counts one SQLite builtin takes, and how to
// say so in a refusal. A count is a rule rather than a list because SQLite
// states some of them that way: json_object takes an even number and json_set
// an odd one, and those are the shapes a list cannot hold.
type builtinCounts struct {
	takes     func(n int) bool
	describes string
}

var builtinArity = map[string]builtinCounts{ //nolint:gochecknoglobals // a fixed table
	"count":             exactly(0, 1),
	"instr":             exactly(2),
	"atan2":             exactly(2),
	"group_concat":      exactly(1, 2),
	"json":              exactly(1),
	"json_array_length": exactly(1, 2),
	"json_patch":        exactly(2),
	"json_quote":        exactly(1),
	"length":            exactly(1),
	"ln":                exactly(1),
	"log":               exactly(1, 2),
	"ltrim":             exactly(1, 2),
	"octet_length":      exactly(1),
	"rtrim":             exactly(1, 2),
	"trim":              exactly(1, 2),

	// SQLite states these as a shape rather than a list, and enforces them when
	// the query runs rather than when it is compiled -- so a caller who wrote
	// JSON_BUILD_OBJECT was told "json_object() requires an even number of
	// arguments", about a function nowhere in their query, and only once the
	// row was fetched.
	"json_object": {takes: func(n int) bool { return n%2 == 0 }, describes: "an even number of arguments"},
	"json_insert": {takes: func(n int) bool { return n%2 == 1 }, describes: "an odd number of arguments"},
	"json_set":    {takes: func(n int) bool { return n%2 == 1 }, describes: "an odd number of arguments"},

	// Any number, which is what SQLite takes for these.
	"json_array":   {},
	"json_extract": {},
}

// exactly names a builtin that takes one of a fixed set of counts.
func exactly(counts ...int) builtinCounts {
	described := make([]string, len(counts))
	for i, c := range counts {
		described[i] = strconv.Itoa(c)
	}
	return builtinCounts{
		takes:     func(n int) bool { return slices.Contains(counts, n) },
		describes: strings.Join(described, " or ") + " arguments",
	}
}

// registeredHelper reports whether the runtime registers a function by this
// name.
func registeredHelper(name string) bool {
	_, found := helperArity[name]
	return found
}

// helperTakesArgumentCount reports whether the function a call was renamed to
// accepts a call of n arguments. A name this package neither registers nor
// renames onto is left alone, since it is a name the caller wrote and the
// driver answers for it under that name.
//
// renamed says whether the lowering gave the call a name other than the one the
// caller wrote, which is what the builtin table is for: SQLite refusing a count
// under a name this package substituted tells the caller about a function
// nowhere in their query, while refusing under the name they wrote tells them
// about their own. A registered helper is checked either way, since a helper is
// this package's implementation whatever it is called.
func helperTakesArgumentCount(name string, n int, renamed bool) bool {
	if want, found := helperArity[name]; found {
		return want < 0 || want == n
	}
	if !renamed {
		return true
	}
	if counts, found := builtinArity[name]; found {
		// An entry with no rule is variadic and takes any count.
		return counts.takes == nil || counts.takes(n)
	}
	return true
}

// arityDescription says how many arguments a function takes, in the words the
// refusal is written in.
func arityDescription(name string) string {
	if want, found := helperArity[name]; found {
		return plural(want)
	}
	// Only a name one of the two tables holds reaches here, since a name in
	// neither is never refused for its count.
	return builtinArity[name].describes
}

// HelperNames lists the names this package believes the runtime registers, for
// the test that holds the two lists together.
func HelperNames() []string {
	names := make([]string, 0, len(helperArity))
	for name := range helperArity {
		names = append(names, name)
	}
	return names
}

// HelperArity is how many arguments this package believes a helper takes, for
// the same test.
func HelperArity(name string) (int, bool) {
	want, found := helperArity[name]
	return want, found
}
