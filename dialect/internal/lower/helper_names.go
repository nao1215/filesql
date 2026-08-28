package lower

// helperNames is every function the runtime registers with the driver. The
// SAFE prefix needs it: a name this package has a helper for is wrapped so its
// error becomes NULL, and a name it does not is left as the plain call SQLite
// will answer or refuse for itself.
//
// The list is here rather than read from the runtime package because the
// dependency runs the other way. A test in lower_test holds the two together,
// so a helper added there and not here is a failure rather than a silent
// change of behaviour.
var helperNames = map[string]bool{ //nolint:gochecknoglobals // a generated table
	"acosd": true, "addtime": true, "age": true, "ascii": true, "asind": true, "atan2d": true,
	"atand": true, "bin": true, "bit_count": true, "bit_length": true, "cbrt": true,
	"chr": true, "clock_timestamp": true, "contains_substr": true, "conv": true,
	"convert_tz": true, "cosd": true, "cot": true, "cotd": true, "coth": true, "crc32": true,
	"csc": true, "csch": true, "curdate": true, "current_datetime": true, "curtime": true,
	"date_bin": true, "date_diff": true, "date_format": true, "date_from_unix_date": true,
	"date_part": true, "date_trunc": true, "date_trunc_part": true, "datediff": true,
	"datetime_diff": true, "day": true, "dayname": true, "dayofmonth": true,
	"dayofweek": true, "dayofyear": true, "decode": true, "dialect_replace": true,
	"dialect_round": true, "dialect_round_even": true, "div": true, "edit_distance": true,
	"elt": true, "encode": true, "ends_with": true, "error": true, "export_set": true,
	"factorial": true, "field": true, "find_in_set": true, "format_date": true,
	"format_datetime": true, "format_timestamp": true, "from_base32": true,
	"from_base64": true, "from_days": true, "from_hex": true, "from_unixtime": true,
	"gcd": true, "gen_random_uuid": true, "generate_uuid": true, "get_byte": true,
	"googlesql_bit_xor": true, "googlesql_cast": true, "googlesql_date": true,
	"googlesql_date_part": true, "googlesql_datetime": true, "googlesql_divide": true,
	"googlesql_format": true, "googlesql_instr": true, "googlesql_last_day": true,
	"googlesql_left": true, "googlesql_lpad": true, "googlesql_md5": true,
	"googlesql_mod": true, "googlesql_repeat": true, "googlesql_right": true,
	"googlesql_rpad": true, "googlesql_safe_cast": true, "googlesql_sha1": true,
	"googlesql_soundex": true, "googlesql_string": true, "googlesql_substr": true,
	"googlesql_time": true, "googlesql_timestamp": true, "greatest": true, "hour": true,
	"ieee_divide": true, "if": true, "inet_aton": true, "inet_ntoa": true, "initcap": true,
	"interval_add": true, "interval_text_add": true, "is_inf": true, "is_ipv4": true,
	"is_ipv6": true, "is_nan": true, "isfinite": true, "json_contains": true,
	"json_length": true, "json_unquote": true, "last_day": true, "lcase": true, "lcm": true,
	"least": true, "left": true, "like_insensitive": true, "like_sensitive": true,
	"locate": true, "lpad": true, "make_date": true, "make_set": true, "make_time": true,
	"makedate": true, "maketime": true, "md5": true, "microsecond": true, "mid": true,
	"min_scale": true, "minute": true, "month": true, "monthname": true, "mysql_ascii": true,
	"mysql_bit_xor": true, "mysql_cast": true, "mysql_ceil": true, "mysql_char": true,
	"mysql_date_diff": true, "mysql_date_part": true, "mysql_divide": true, "mysql_exp": true,
	"mysql_floor": true, "mysql_format": true, "mysql_greatest": true, "mysql_hex": true,
	"mysql_insert": true, "mysql_interval": true, "mysql_interval_compound": true,
	"mysql_json_type": true, "mysql_least": true, "mysql_left": true, "mysql_ln": true,
	"mysql_log10": true, "mysql_log2": true, "mysql_lpad": true, "mysql_mod": true,
	"mysql_ord": true, "mysql_quote": true, "mysql_regexp": true,
	"mysql_regexp_replace": true, "mysql_right": true, "mysql_rpad": true,
	"mysql_shift_left": true, "mysql_shift_right": true, "mysql_sign": true,
	"mysql_soundex": true, "mysql_sqrt": true, "mysql_substr": true,
	"mysql_time_of_day": true, "mysql_timediff": true, "mysql_timestamp": true,
	"mysql_unhex": true, "mysql_week": true, "mysql_weekofyear": true, "mysql_yearweek": true,
	"normalize": true, "normalize_and_casefold": true, "now": true, "num_nonnulls": true,
	"num_nulls": true, "oct": true, "overlay": true, "parse_date": true,
	"parse_datetime": true, "parse_timestamp": true, "period_add": true, "period_diff": true,
	"pg_typeof": true, "postgresql_bit_xor": true, "postgresql_cast": true,
	"postgresql_date_add": true, "postgresql_date_diff": true, "postgresql_divide": true,
	"postgresql_format": true, "postgresql_greatest": true, "postgresql_json_typeof": true,
	"postgresql_least": true, "postgresql_lpad": true, "postgresql_mod": true,
	"postgresql_random": true, "postgresql_regexp_replace": true, "postgresql_rpad": true,
	"postgresql_substr": true, "postgresql_substring_from": true, "postgresql_to_hex": true,
	"quarter": true, "quote_ident": true, "quote_literal": true, "quote_nullable": true,
	"rand": true, "regexp": true, "regexp_contains": true, "regexp_count": true,
	"regexp_extract": true, "regexp_instr": true, "regexp_like": true, "regexp_replace": true,
	"regexp_substr": true, "repeat": true, "reverse": true, "right": true, "rpad": true,
	"safe_add": true, "safe_call": true, "safe_divide": true, "safe_multiply": true,
	"safe_negate": true, "safe_subtract": true, "scale": true, "sec": true,
	"sec_to_time": true, "sech": true, "second": true, "set_byte": true, "sha1": true,
	"sha2": true, "sha224": true, "sha256": true, "sha384": true, "sha512": true,
	"similar_substring": true, "similar_to": true, "sind": true, "space": true,
	"split_part": true, "starts_with": true, "statement_timestamp": true, "str_to_date": true,
	"strcmp": true, "strict_concat": true, "strpos": true, "substring_index": true,
	"subtime": true, "tand": true, "time_add": true, "time_diff": true, "time_format": true,
	"time_to_sec": true, "time_trunc": true, "timeofday": true, "timestamp_diff": true,
	"timestamp_micros": true, "timestamp_millis": true, "timestamp_seconds": true,
	"to_base32": true, "to_base64": true, "to_char": true, "to_date": true, "to_days": true,
	"to_hex": true, "to_json_string": true, "to_number": true, "to_seconds": true,
	"to_timestamp": true, "transaction_timestamp": true, "translate": true,
	"trim_scale": true, "trunc_scale": true, "truncate": true, "ucase": true,
	"unicode_lower": true, "unicode_upper": true, "unix_date": true, "unix_micros": true,
	"unix_millis": true, "unix_seconds": true, "unix_timestamp": true, "weekday": true,
	"width_bucket": true, "year": true,
}

// registeredHelper reports whether the runtime registers a function by this
// name.
func registeredHelper(name string) bool { return helperNames[name] }

// HelperNames lists the names this package believes the runtime registers, for
// the test that holds the two lists together.
func HelperNames() []string {
	names := make([]string, 0, len(helperNames))
	for name := range helperNames {
		names = append(names, name)
	}
	return names
}
