package lower

import (
	"strings"

	"github.com/nao1215/filesql/dialect/internal/ast"
	"github.com/nao1215/filesql/dialect/internal/dialects"
)

// A function a dialect defines and this package does not implement used to
// reach SQLite unchanged, and SQLite answered "no such function: NAME". That
// tells a caller a name they did write does not exist, which reads as a typo in
// their query rather than as a gap here, and it is the failure the comment on
// arrayFunctions already names. The tables below say why each name has no
// SQLite form, so the refusal names the function and the reason.
//
// A name that is not in a table still reaches SQLite, because SQLite's own
// functions are part of what a caller may write under any dialect: the tables
// are the names one engine has and SQLite has not, rather than a list of
// everything this package knows.

// Reasons, spelled once so a family of names refuses in one voice.
const (
	reasonArray        = "its result is an array and SQLite has no array type"
	reasonJSONOp       = "SQLite's JSON functions have no operation of that shape"
	reasonSession      = "it answers a fact about the connection or the server, which a database made from files does not have"
	reasonEffect       = "it has an effect rather than a value"
	reasonBytes        = "SQLite holds text as UTF-8 and has no encoding to convert between"
	reasonGeo          = "SQLite has no geography type"
	reasonRandom       = "its value is not the same twice, so a query over the same rows would not answer the same"
	reasonBinary       = "SQLite has no type for the bytes it answers"
	reasonRange        = "SQLite has no range type"
	reasonGroupingSets = "it says whether a column was rolled up, and the grouping sets it reports on are not supported"
	reasonPercentile   = "it needs the whole partition sorted, and SQLite has no function that answers a percentile"
	reasonServer       = "it answers a fact about the server or its catalog, which a database made from files does not have"
	reasonCrypto       = "SQLite has no cipher, key or digest of its own"
	reasonLargeObject  = "it reads or writes a large object held by the server, which a database made from files does not have"
	reasonXML          = "SQLite has no XML type"
	reasonTextSearch   = "SQLite has no text-search vector or query type"
	reasonRows         = "it answers rows rather than a value, and a translation of it would be a table this package does not build"
	reasonCollation    = "it answers bytes that stand for a collation's ordering, and SQLite orders text by its bytes"
	reasonNetwork      = "SQLite has no network address type"
	reasonSequence     = "it reads or moves a sequence, and a table made from a file has none"
	reasonInternalName = "it is a name the engine keeps for its own use; write the operator or the documented function instead"
)

// namePrefix refuses a family of names in one line, for a family large enough
// that listing it would be a page of the same reason. A prefix is only right
// where every name under it has that reason: "PG_" is, because everything
// PostgreSQL puts there answers a fact about a server, while "JSON" is not.
type namePrefix struct {
	prefix string
	reason string
}

// prefixReason is the reason a name's family gives, or "" when no family
// claims it.
//
// A name this package registers a helper for is never a family's: the family
// says what has no SQLite form, and a helper is what gives one to a name that
// had none. PostgreSQL's PG_TYPEOF is the case that makes this necessary --
// everything else beginning with PG_ reads a server, and that one reads a
// value -- and asking the helper table rather than listing exceptions means a
// helper added later is covered without anyone remembering to.
func prefixReason(prefixes []namePrefix, name string) string {
	if registeredHelper(strings.ToLower(name)) {
		return ""
	}
	for _, p := range prefixes {
		if strings.HasPrefix(name, p.prefix) {
			return p.reason
		}
	}
	return ""
}

// mysqlUnsupportedFunctions are the MySQL functions with no SQLite form.
var mysqlUnsupportedFunctions = map[string]string{ //nolint:gochecknoglobals // a fixed table
	"GROUPING":                      reasonGroupingSets,
	"JSON_KEYS":                     reasonArray,
	"JSON_SEARCH":                   "it answers the path a value sits at, and SQLite has no function that searches for one",
	"JSON_DEPTH":                    reasonJSONOp,
	"JSON_MERGE_PRESERVE":           reasonJSONOp,
	"JSON_OVERLAPS":                 reasonJSONOp,
	"JSON_SCHEMA_VALID":             reasonJSONOp,
	"JSON_SCHEMA_VALIDATION_REPORT": reasonJSONOp,
	"JSON_STORAGE_SIZE":             reasonJSONOp,
	"JSON_STORAGE_FREE":             reasonJSONOp,
	"BIN_TO_UUID":                   reasonBinary,
	"UUID_TO_BIN":                   reasonBinary,
	"RANDOM_BYTES":                  reasonRandom,
	"UUID":                          reasonRandom,
	"UUID_SHORT":                    reasonRandom,
	"COMPRESS":                      reasonBinary,
	"UNCOMPRESS":                    reasonBinary,
	"UNCOMPRESSED_LENGTH":           reasonBinary,
	"INET6_ATON":                    reasonBinary,
	"INET6_NTOA":                    reasonBinary,
	"STATEMENT_DIGEST":              reasonSession,
	"STATEMENT_DIGEST_TEXT":         reasonSession,
	"LAST_INSERT_ID":                reasonSession,
	"ROW_COUNT":                     reasonSession,
	"FOUND_ROWS":                    reasonSession,
	"CONNECTION_ID":                 reasonSession,
	"NAME_CONST":                    reasonSession,
	"POINT":                         reasonGeo,
	"LINESTRING":                    reasonGeo,
	"POLYGON":                       reasonGeo,
	"GEOMFROMTEXT":                  reasonGeo,
	"ASTEXT":                        reasonGeo,
	"ST_ASTEXT":                     reasonGeo,
	"ST_GEOMFROMTEXT":               reasonGeo,
	"ST_X":                          reasonGeo,
	"ST_Y":                          reasonGeo,
	"ST_DISTANCE":                   reasonGeo,
	"ST_CONTAINS":                   reasonGeo,
	"ST_SRID":                       reasonGeo,
	"MBRCONTAINS":                   reasonGeo,
	"BENCHMARK":                     reasonEffect,
	"SLEEP":                         reasonEffect,
	"GET_LOCK":                      reasonEffect,
	"RELEASE_LOCK":                  reasonEffect,
	"RELEASE_ALL_LOCKS":             reasonEffect,
	"IS_FREE_LOCK":                  reasonEffect,
	"IS_USED_LOCK":                  reasonEffect,
}

// postgresUnsupportedFunctions are the PostgreSQL functions with no SQLite form.
var postgresUnsupportedFunctions = map[string]string{ //nolint:gochecknoglobals // a fixed table
	"GROUPING":               reasonGroupingSets,
	"ARRAY_LENGTH":           reasonArray,
	"ARRAY_TO_JSON":          reasonArray,
	"ARRAY_TO_STRING":        reasonArray,
	"CARDINALITY":            reasonArray,
	"STRING_TO_ARRAY":        reasonArray,
	"PARSE_IDENT":            reasonArray,
	"REGEXP_SPLIT_TO_ARRAY":  reasonArray,
	"JSON_STRIP_NULLS":       reasonJSONOp,
	"JSONB_STRIP_NULLS":      reasonJSONOp,
	"JSONB_PRETTY":           reasonJSONOp,
	"JSONB_PATH_EXISTS":      reasonJSONOp,
	"JSONB_PATH_MATCH":       reasonJSONOp,
	"JSONB_PATH_QUERY":       reasonJSONOp,
	"JSONB_PATH_QUERY_ARRAY": reasonJSONOp,
	"JSONB_PATH_QUERY_FIRST": reasonJSONOp,
	"ROW_TO_JSON":            reasonJSONOp,
	"JUSTIFY_DAYS":           "SQLite has no interval type",
	"JUSTIFY_HOURS":          "SQLite has no interval type",
	"JUSTIFY_INTERVAL":       "SQLite has no interval type",
	"TO_ASCII":               reasonBytes,
	"ASCII_STRING":           reasonBytes,
	"CONVERT_FROM":           reasonBytes,
	"CONVERT_TO":             reasonBytes,
	"RANDOM_NORMAL":          reasonRandom,
	"INT4RANGE":              reasonRange,
	"INT8RANGE":              reasonRange,
	"NUMRANGE":               reasonRange,
	"TSRANGE":                reasonRange,
	"TSTZRANGE":              reasonRange,
	"DATERANGE":              reasonRange,
	"BOX":                    reasonGeo,
	"CIRCLE":                 reasonGeo,
	"POINT":                  reasonGeo,
	"POLYGON":                reasonGeo,
	"PG_SLEEP":               reasonEffect,
	"PG_SLEEP_FOR":           reasonEffect,
	"PG_SLEEP_UNTIL":         reasonEffect,
	"PG_BACKEND_PID":         reasonSession,
	"CURRENT_SETTING":        reasonSession,
	"SET_CONFIG":             reasonSession,
}

// googlesqlUnsupportedFunctions are the GoogleSQL functions with no SQLite form.
// The ones whose result is an array are in arrayFunctions instead, which the
// call rules already consult.
var googlesqlUnsupportedFunctions = map[string]string{ //nolint:gochecknoglobals // a fixed table
	"GROUPING":           reasonGroupingSets,
	"PERCENTILE_CONT":    reasonPercentile,
	"PERCENTILE_DISC":    reasonPercentile,
	"MAX_BY":             "it answers one column's value at the row where another is extreme, which SQLite can only express by changing the shape of the query",
	"MIN_BY":             "it answers one column's value at the row where another is extreme, which SQLite can only express by changing the shape of the query",
	"JSON_KEYS":          reasonArray,
	"JSON_STRIP_NULLS":   reasonJSONOp,
	"TO_JSON":            "SQLite has no JSON type; TO_JSON_STRING answers the same value as text",
	"BOOL":               "it reads a JSON value as a BOOL, and SQLite has no JSON type to read from",
	"INT64":              "it reads a JSON value as an INT64, and SQLite has no JSON type to read from",
	"FLOAT64":            "it reads a JSON value as a FLOAT64, and SQLite has no JSON type to read from",
	"LAX_BOOL":           "it reads a JSON value, and SQLite has no JSON type to read from",
	"LAX_INT64":          "it reads a JSON value, and SQLite has no JSON type to read from",
	"LAX_FLOAT64":        "it reads a JSON value, and SQLite has no JSON type to read from",
	"LAX_STRING":         "it reads a JSON value, and SQLite has no JSON type to read from",
	"SESSION_USER":       reasonSession,
	"VECTOR_SEARCH":      "it searches an index this package does not build",
	"COSINE_DISTANCE":    reasonArray,
	"EUCLIDEAN_DISTANCE": reasonArray,
	"ST_GEOGPOINT":       reasonGeo,
	"ST_DISTANCE":        reasonGeo,
	"ST_WITHIN":          reasonGeo,
	"ST_ASTEXT":          reasonGeo,
	"ST_GEOGFROMTEXT":    reasonGeo,
	"S2_CELLIDFROMPOINT": reasonGeo,
	"S2_COVERINGCELLIDS": reasonGeo,
}

// mysqlUnsupportedPrefixes are the MySQL families with no SQLite form, each of
// them large enough that naming every member would be a page of one reason.
// mysqlServerFunctions are the MySQL names that answer a fact about the server,
// its session or its catalog. A database made from files has none of those.
var mysqlServerFunctions = []string{ //nolint:gochecknoglobals // a fixed table
	"CHARSET", "COERCIBILITY", "COLLATION", "CURRENT_ROLE", "CURRENT_USER",
	"DATABASE", "SCHEMA", "SESSION_USER", "SYSTEM_USER", "USER", "VERSION",
	"ICU_VERSION", "ROLES_GRAPHML", "IS_VISIBLE_DD_OBJECT", "FORMAT_BYTES",
	"FORMAT_PICO_TIME", "LOAD_FILE",
}

// mysqlCryptoFunctions are the MySQL names that encrypt, sign or digest.
var mysqlCryptoFunctions = []string{ //nolint:gochecknoglobals // a fixed table
	"AES_DECRYPT", "AES_ENCRYPT", "CREATE_ASYMMETRIC_PRIV_KEY",
	"CREATE_ASYMMETRIC_PUB_KEY", "CREATE_DIGEST", "VALIDATE_PASSWORD_STRENGTH",
}

var mysqlUnsupportedPrefixes = []namePrefix{ //nolint:gochecknoglobals // a fixed table
	{"ST_", reasonGeo},
	{"MBR", reasonGeo},
	// The data dictionary: what MySQL's own INFORMATION_SCHEMA views are built
	// from. Every one of them reads a server's catalog.
	{"INTERNAL_", reasonServer},
	{"GET_DD_", reasonServer},
	{"CAN_ACCESS_", reasonServer},
	{"PS_", reasonServer},
	{"ASYMMETRIC_", reasonCrypto},
}

// postgresUnsupportedPrefixes are the same for PostgreSQL, where the catalog
// and administration functions are the bulk of what the engine defines.
var postgresUnsupportedPrefixes = []namePrefix{ //nolint:gochecknoglobals // a fixed table
	{"PG_", reasonServer},
	{"TXID_", reasonServer},
	{"BINARY_UPGRADE_", reasonServer},
	{"BRIN_", reasonServer},
	{"GIN_", reasonServer},
	{"GIST_", reasonServer},
	{"SPG", reasonServer},
	{"HAS_", reasonServer},
	{"ACL", reasonServer},
	{"TO_REG", reasonServer},
	{"LO_", reasonLargeObject},
	{"XML", reasonXML},
	{"TS_", reasonTextSearch},
	{"TSQUERY", reasonTextSearch},
	{"TSVECTOR", reasonTextSearch},
	{"MULTIRANGE", reasonRange},
	{"RANGE_", reasonRange},
}

// postgresGeometryFunctions answer facts about PostgreSQL's geometric types.
var postgresGeometryFunctions = []string{ //nolint:gochecknoglobals // a fixed table
	"AREA", "CENTER", "DIAGONAL", "DIAMETER", "HEIGHT", "WIDTH", "RADIUS",
	"SLOPE", "ISCLOSED", "ISOPEN", "ISHORIZONTAL", "ISVERTICAL", "ISPARALLEL",
	"ISPERP", "NPOINTS", "PCLOSE", "POPEN", "LINE", "LSEG",
}

// postgresNetworkFunctions read or build a network address.
var postgresNetworkFunctions = []string{ //nolint:gochecknoglobals // a fixed table
	"ABBREV", "BROADCAST", "FAMILY", "HOST", "HOSTMASK", "MASKLEN", "NETMASK",
	"NETWORK", "SET_MASKLEN", "MACADDR8_SET7BIT",
}

// postgresServerFunctions answer a fact about the server, its session or its
// catalog.
var postgresServerFunctions = []string{ //nolint:gochecknoglobals // a fixed table
	"CURRENT_DATABASE", "CURRENT_QUERY", "CURRENT_SCHEMA", "CURRENT_SCHEMAS",
	"CURRENT_USER", "SESSION_USER", "SYSTEM_USER", "VERSION",
	"GETDATABASEENCODING", "GETPGUSERNAME", "COL_DESCRIPTION", "OBJ_DESCRIPTION",
	"SHOBJ_DESCRIPTION", "FORMAT_TYPE", "ROW_SECURITY_ACTIVE", "MXID_AGE",
	"CURRTID2", "SATISFIES_HASH_PARTITION", "POSTGRESQL_FDW_VALIDATOR",
	"AMVALIDATE", "ICU_UNICODE_VERSION", "NAMECONCATOID", "INET_CLIENT_ADDR",
	"INET_CLIENT_PORT", "INET_SERVER_ADDR", "INET_SERVER_PORT", "LOREAD",
	"LOWRITE", "MAKEACLITEM", "OIDVECTORTYPES", "ENUM_SEND",
}

// postgresSequenceFunctions read or move a sequence.
var postgresSequenceFunctions = []string{ //nolint:gochecknoglobals // a fixed table
	"CURRVAL", "NEXTVAL", "SETVAL", "LASTVAL",
}

// postgresRangeFunctions answer a fact about a range or build one.
var postgresRangeFunctions = []string{ //nolint:gochecknoglobals // a fixed table
	"LOWER_INC", "LOWER_INF", "UPPER_INC", "UPPER_INF", "ISEMPTY",
	"DATEMULTIRANGE", "INT4MULTIRANGE", "INT8MULTIRANGE", "NUMMULTIRANGE",
	"TSMULTIRANGE", "TSTZMULTIRANGE",
}

// postgresTextSearchFunctions build or read a text-search value.
var postgresTextSearchFunctions = []string{ //nolint:gochecknoglobals // a fixed table
	"TO_TSQUERY", "TO_TSVECTOR", "PLAINTO_TSQUERY", "PHRASETO_TSQUERY",
	"WEBSEARCH_TO_TSQUERY", "QUERYTREE", "SETWEIGHT", "STRIP", "NUMNODE",
	"GET_CURRENT_TS_CONFIG", "ARRAY_TO_TSVECTOR", "JSON_TO_TSVECTOR",
	"JSONB_TO_TSVECTOR",
}

// postgresXMLFunctions build or read XML.
var postgresXMLFunctions = []string{ //nolint:gochecknoglobals // a fixed table
	"XPATH", "XPATH_EXISTS", "CURSOR_TO_XML", "CURSOR_TO_XMLSCHEMA",
	"DATABASE_TO_XML", "DATABASE_TO_XMLSCHEMA", "DATABASE_TO_XML_AND_XMLSCHEMA",
	"QUERY_TO_XML", "QUERY_TO_XMLSCHEMA", "QUERY_TO_XML_AND_XMLSCHEMA",
	"SCHEMA_TO_XML", "SCHEMA_TO_XMLSCHEMA", "SCHEMA_TO_XML_AND_XMLSCHEMA",
	"TABLE_TO_XML", "TABLE_TO_XMLSCHEMA", "TABLE_TO_XML_AND_XMLSCHEMA",
}

// postgresRowFunctions answer rows rather than a value.
var postgresRowFunctions = []string{ //nolint:gochecknoglobals // a fixed table
	"JSON_POPULATE_RECORD", "JSON_POPULATE_RECORDSET", "JSON_TO_RECORD",
	"JSON_TO_RECORDSET", "JSON_EACH_TEXT", "JSON_ARRAY_ELEMENTS_TEXT",
	"JSONB_POPULATE_RECORD", "JSONB_POPULATE_RECORDSET", "JSONB_TO_RECORD",
	"JSONB_TO_RECORDSET", "JSONB_EACH_TEXT", "JSONB_ARRAY_ELEMENTS_TEXT",
	"JSONB_POPULATE_RECORD_VALID",
}

// postgresArrayFunctions answer or take an array.
var postgresArrayFunctions = []string{ //nolint:gochecknoglobals // a fixed table
	"ARRAY_DIMS", "ARRAY_FILL", "ARRAY_LOWER", "ARRAY_NDIMS", "ARRAY_POSITION",
	"ARRAY_POSITIONS", "ARRAY_REMOVE", "ARRAY_REPLACE", "ARRAY_SAMPLE",
	"ARRAY_SHUFFLE", "ARRAY_UPPER", "TRIM_ARRAY", "ENUM_RANGE",
	"JSON_AGG_STRICT", "JSONB_AGG_STRICT", "JSON_OBJECT_AGG_STRICT",
	"JSON_OBJECT_AGG_UNIQUE", "JSON_OBJECT_AGG_UNIQUE_STRICT",
	"JSONB_OBJECT_AGG_STRICT", "JSONB_OBJECT_AGG_UNIQUE",
	"JSONB_OBJECT_AGG_UNIQUE_STRICT",
}

// postgresInternalFunctions are the names PostgreSQL keeps for its own
// arithmetic and for spellings it has outgrown. A caller writes the operator
// or the modern name instead.
var postgresInternalFunctions = []string{ //nolint:gochecknoglobals // a fixed table
	"NUMERIC_EXP", "NUMERIC_LN", "NUMERIC_LOG", "NUMERIC_SQRT", "NUMERIC_INC",
	"NUMERIC_DIV_TRUNC", "INT4INC", "INT8_SUM", "DEXP", "DLOG1", "DLOG10",
	"DROUND", "DTRUNC", "CASH_WORDS", "TEXTLEN", "TEXT", "TIMESTAMP",
	"TIMESTAMPTZ", "NOTLIKE", "LIKE_ESCAPE", "SIMILAR_ESCAPE",
	"SIMILAR_TO_ESCAPE", "CONVERT",
}

// unsupportedPrefixes selects a dialect's prefix table.
func unsupportedPrefixes(d dialects.Dialect) []namePrefix {
	switch d {
	case dialects.MySQL:
		return mysqlUnsupportedPrefixes
	case dialects.PostgreSQL:
		return postgresUnsupportedPrefixes
	case dialects.GoogleSQL, dialects.SQLite:
		// GoogleSQL's names are listed one by one, and SQLite is the identity
		// translation and refuses nothing here.
		return nil
	default:
		return nil
	}
}

// withFamily adds one reason for a list of names, so a family that is a list
// rather than a prefix is still written once.
func withFamily(table map[string]string, names []string, reason string) {
	for _, name := range names {
		table[name] = reason
	}
}

//nolint:gochecknoinits // the families are folded into the table once, at start
func init() {
	withFamily(mysqlUnsupportedFunctions, mysqlServerFunctions, reasonServer)
	withFamily(mysqlUnsupportedFunctions, mysqlCryptoFunctions, reasonCrypto)
	withFamily(postgresUnsupportedFunctions, postgresGeometryFunctions, reasonGeo)
	withFamily(postgresUnsupportedFunctions, postgresNetworkFunctions, reasonNetwork)
	withFamily(postgresUnsupportedFunctions, postgresServerFunctions, reasonServer)
	withFamily(postgresUnsupportedFunctions, postgresSequenceFunctions, reasonSequence)
	withFamily(postgresUnsupportedFunctions, postgresRangeFunctions, reasonRange)
	withFamily(postgresUnsupportedFunctions, postgresTextSearchFunctions, reasonTextSearch)
	withFamily(postgresUnsupportedFunctions, postgresXMLFunctions, reasonXML)
	withFamily(postgresUnsupportedFunctions, postgresRowFunctions, reasonRows)
	withFamily(postgresUnsupportedFunctions, postgresArrayFunctions, reasonArray)
	withFamily(postgresUnsupportedFunctions, postgresInternalFunctions, reasonInternalName)
}

// The names left over from the families, each with a reason of its own.
//
//nolint:gochecknoinits // one table, folded once
func init() {
	for name, reason := range map[string]string{
		"IS_IPV4_COMPAT": reasonBinary,
		"IS_IPV4_MAPPED": reasonBinary,
		"JSON_MERGE":     reasonJSONOp,
		"JSON_TABLE":     reasonRows,
		"WEIGHT_STRING":  reasonCollation,
		"GET_FORMAT": "it answers one of MySQL's own format strings, which this package does not carry; " +
			"write the format string itself, which DATE_FORMAT and STR_TO_DATE take here",
		"JSON_CONTAINS_PATH": "this package does not translate it; SQLite answers the same question with " +
			"json_type(doc, path) IS NOT NULL",
	} {
		mysqlUnsupportedFunctions[name] = reason
	}
}

// unsupportedFunctions selects a dialect's table.
func unsupportedFunctions(d dialects.Dialect) map[string]string {
	switch d {
	case dialects.MySQL:
		return mysqlUnsupportedFunctions
	case dialects.PostgreSQL:
		return postgresUnsupportedFunctions
	case dialects.GoogleSQL:
		return googlesqlUnsupportedFunctions
	case dialects.SQLite:
		// SQLite is the identity translation and refuses nothing here.
		return nil
	default:
		return nil
	}
}

// refuseUnsupportedFunction reports the refusal for a name the dialect defines
// and this package does not, and nil for every other name.
func refuseUnsupportedFunction(d dialects.Dialect, call *ast.FuncCall) error {
	name := callName(call)
	reason, found := unsupportedFunctions(d)[name]
	if !found {
		// A family answers for the names too many to list, and is asked second
		// so that a name with a reason of its own keeps it.
		if reason = prefixReason(unsupportedPrefixes(d), name); reason == "" {
			return nil
		}
	}
	return unsupported(call.Span, "%s is not supported; %s", name, reason)
}
