package dialect

import "strings"

// SQLite storage-class-ish type names used as CAST targets. SQLite applies type
// affinity from these, which is the closest equivalent to the source dialects'
// richer type systems.
const (
	sqliteInteger = "INTEGER"
	sqliteReal    = "REAL"
	sqliteText    = "TEXT"
	sqliteBlob    = "BLOB"
)

// mysqlCastTypes maps a MySQL CAST/CONVERT target type (its leading keyword,
// uppercased) to the SQLite type used in the rewritten CAST. A type not present
// here is left unchanged so SQLite can interpret it.
var mysqlCastTypes = map[string]string{
	"SIGNED":    sqliteInteger,
	"UNSIGNED":  sqliteInteger,
	"INT":       sqliteInteger,
	"INTEGER":   sqliteInteger,
	"BIGINT":    sqliteInteger,
	"SMALLINT":  sqliteInteger,
	"TINYINT":   sqliteInteger,
	"MEDIUMINT": sqliteInteger,
	"BOOL":      sqliteInteger,
	"BOOLEAN":   sqliteInteger,
	"CHAR":      sqliteText,
	"NCHAR":     sqliteText,
	"VARCHAR":   sqliteText,
	"NVARCHAR":  sqliteText,
	"TEXT":      sqliteText,
	"JSON":      sqliteText,
	"DECIMAL":   sqliteReal,
	"DEC":       sqliteReal,
	"NUMERIC":   sqliteReal,
	"FLOAT":     sqliteReal,
	"DOUBLE":    sqliteReal,
	"REAL":      sqliteReal,
	"DATE":      sqliteText,
	"DATETIME":  sqliteText,
	"TIME":      sqliteText,
	"TIMESTAMP": sqliteText,
	"YEAR":      sqliteText,
	"BINARY":    sqliteBlob,
	"VARBINARY": sqliteBlob,
	"BLOB":      sqliteBlob,
}

// pgCastTypes maps a PostgreSQL type name (uppercased) to the SQLite type used
// in a rewritten CAST or "::" cast. A type not present here is left unchanged.
var pgCastTypes = map[string]string{
	"SMALLINT":    sqliteInteger,
	"INT2":        sqliteInteger,
	"INTEGER":     sqliteInteger,
	"INT":         sqliteInteger,
	"INT4":        sqliteInteger,
	"BIGINT":      sqliteInteger,
	"INT8":        sqliteInteger,
	"SERIAL":      sqliteInteger,
	"BIGSERIAL":   sqliteInteger,
	"BOOLEAN":     sqliteInteger,
	"BOOL":        sqliteInteger,
	"REAL":        sqliteReal,
	"FLOAT4":      sqliteReal,
	"FLOAT8":      sqliteReal,
	"DOUBLE":      sqliteReal, // "DOUBLE PRECISION"; leading keyword is DOUBLE
	"NUMERIC":     sqliteReal,
	"DECIMAL":     sqliteReal,
	"MONEY":       sqliteReal,
	"TEXT":        sqliteText,
	"VARCHAR":     sqliteText,
	"CHARACTER":   sqliteText, // "CHARACTER VARYING" / CHARACTER(n)
	"CHAR":        sqliteText,
	"BPCHAR":      sqliteText,
	"NAME":        sqliteText,
	"UUID":        sqliteText,
	"JSON":        sqliteText,
	"JSONB":       sqliteText,
	"DATE":        sqliteText,
	"TIME":        sqliteText,
	"TIMESTAMP":   sqliteText,
	"TIMESTAMPTZ": sqliteText,
	"INTERVAL":    sqliteText,
	"BYTEA":       sqliteBlob,
}

// lookupCastType returns the SQLite CAST target for a source-dialect type name
// and whether a mapping exists.
func lookupCastType(m map[string]string, name string) (string, bool) {
	mapped, ok := m[strings.ToUpper(name)]
	return mapped, ok
}
