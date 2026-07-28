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

// lookupCastType returns the SQLite CAST target for a source-dialect type name
// and whether a mapping exists.
func lookupCastType(m map[string]string, name string) (string, bool) {
	mapped, ok := m[strings.ToUpper(name)]
	return mapped, ok
}
