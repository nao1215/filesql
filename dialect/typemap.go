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

// commonCastTypes maps the standard SQL type names shared by the dialects to a
// SQLite type. Each dialect map holds only the names unique to that dialect;
// lookupCastType falls back to this table.
var commonCastTypes = map[string]string{
	"SMALLINT":  sqliteInteger,
	"INT":       sqliteInteger,
	"INTEGER":   sqliteInteger,
	"BIGINT":    sqliteInteger,
	"TINYINT":   sqliteInteger,
	"BOOL":      sqliteInteger,
	"BOOLEAN":   sqliteInteger,
	"REAL":      sqliteReal,
	"FLOAT":     sqliteReal,
	"DOUBLE":    sqliteReal,
	"NUMERIC":   sqliteReal,
	"DECIMAL":   sqliteReal,
	"CHAR":      sqliteText,
	"VARCHAR":   sqliteText,
	"TEXT":      sqliteText,
	"JSON":      sqliteText,
	"DATE":      sqliteText,
	"DATETIME":  sqliteText,
	"TIME":      sqliteText,
	"TIMESTAMP": sqliteText,
	"BLOB":      sqliteBlob,
}

// mysqlCastTypes holds the MySQL-only CAST target keywords (its leading keyword,
// uppercased). Standard names fall back to commonCastTypes.
var mysqlCastTypes = map[string]string{
	"SIGNED":    sqliteInteger,
	"UNSIGNED":  sqliteInteger,
	"MEDIUMINT": sqliteInteger,
	"NCHAR":     sqliteText,
	"NVARCHAR":  sqliteText,
	"DEC":       sqliteReal,
	"YEAR":      sqliteText,
	"BINARY":    sqliteBlob,
	"VARBINARY": sqliteBlob,
}

// pgCastTypes holds the PostgreSQL-only type keywords. Standard names fall back
// to commonCastTypes.
var pgCastTypes = map[string]string{
	"INT2":        sqliteInteger,
	"INT4":        sqliteInteger,
	"INT8":        sqliteInteger,
	"SERIAL":      sqliteInteger,
	"BIGSERIAL":   sqliteInteger,
	"FLOAT4":      sqliteReal,
	"FLOAT8":      sqliteReal,
	"MONEY":       sqliteReal,
	"CHARACTER":   sqliteText, // "CHARACTER VARYING" / CHARACTER(n)
	"BPCHAR":      sqliteText,
	"NAME":        sqliteText,
	"UUID":        sqliteText,
	"JSONB":       sqliteText,
	"TIMESTAMPTZ": sqliteText,
	"INTERVAL":    sqliteText,
	"BYTEA":       sqliteBlob,
}

// googlesqlCastTypes holds the GoogleSQL-only type keywords. Standard names fall
// back to commonCastTypes.
var googlesqlCastTypes = map[string]string{
	"INT64":      sqliteInteger,
	"BYTEINT":    sqliteInteger,
	"FLOAT64":    sqliteReal,
	"BIGNUMERIC": sqliteReal,
	"BIGDECIMAL": sqliteReal,
	"STRING":     sqliteText,
	"BYTES":      sqliteBlob,
}

// lookupCastType returns the SQLite CAST target for a source-dialect type name,
// checking the dialect-specific map first and then the shared table, and reports
// whether a mapping exists.
func lookupCastType(m map[string]string, name string) (string, bool) {
	upper := strings.ToUpper(name)
	if mapped, ok := m[upper]; ok {
		return mapped, true
	}
	mapped, ok := commonCastTypes[upper]
	return mapped, ok
}
