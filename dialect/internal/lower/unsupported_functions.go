package lower

import (
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
	reasonArray   = "its result is an array and SQLite has no array type"
	reasonJSONOp  = "SQLite's JSON functions have no operation of that shape"
	reasonSession = "it answers a fact about the connection or the server, which a database made from files does not have"
	reasonEffect  = "it has an effect rather than a value"
	reasonBytes   = "SQLite holds text as UTF-8 and has no encoding to convert between"
	reasonGeo     = "SQLite has no geography type"
	reasonRandom  = "its value is not the same twice, so a query over the same rows would not answer the same"
	reasonBinary  = "SQLite has no type for the bytes it answers"
	reasonRange   = "SQLite has no range type"
)

// mysqlUnsupportedFunctions are the MySQL functions with no SQLite form.
var mysqlUnsupportedFunctions = map[string]string{ //nolint:gochecknoglobals // a fixed table
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
	reason, found := unsupportedFunctions(d)[callName(call)]
	if !found {
		return nil
	}
	return unsupported(call.Span, "%s is not supported; %s", callName(call), reason)
}
