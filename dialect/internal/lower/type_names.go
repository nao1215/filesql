package lower

// This file holds the fixed tables the type rules read. A SQL type spelled in
// three of them is three entries in three tables rather than a constant the
// package is missing, which is why the linter's repeated-string rule is turned
// off for this file.

// sqliteTypeNames maps a source dialect's type name onto the SQLite storage
// class with the affinity the source type has. A name SQLite does not know
// takes NUMERIC affinity, which is rarely what the caller meant, so the mapping
// is explicit rather than left to SQLite's rules about substrings.
var sqliteTypeNames = map[string]string{ //nolint:gochecknoglobals // a fixed table
	"INT": "INTEGER", "INTEGER": "INTEGER", "TINYINT": "INTEGER", "SMALLINT": "INTEGER",
	"MEDIUMINT": "INTEGER", "BIGINT": "INTEGER", "INT2": "INTEGER", "INT4": "INTEGER",
	"INT8": "INTEGER", "INT64": "INTEGER", "SERIAL": "INTEGER", "BIGSERIAL": "INTEGER",
	"SMALLSERIAL": "INTEGER", "SIGNED": "INTEGER", "SIGNED INTEGER": "INTEGER",
	"UNSIGNED": "INTEGER", "UNSIGNED INTEGER": "INTEGER", "YEAR": "INTEGER",

	"REAL": "REAL", "DOUBLE": "REAL", "DOUBLE PRECISION": "REAL", "FLOAT": "REAL",
	"FLOAT4": "REAL", "FLOAT8": "REAL", "FLOAT64": "REAL",

	"NUMERIC": "NUMERIC", "DECIMAL": "NUMERIC", "DEC": "NUMERIC", "FIXED": "NUMERIC",
	"BIGNUMERIC": "NUMERIC", "MONEY": "NUMERIC",

	"TEXT": "TEXT", "VARCHAR": "TEXT", "CHAR": "TEXT", "CHARACTER": "TEXT",
	"CHARACTER VARYING": "TEXT", "NCHAR": "TEXT", "NVARCHAR": "TEXT",
	"NATIONAL CHAR": "TEXT", "NATIONAL VARCHAR": "TEXT", "TINYTEXT": "TEXT",
	"MEDIUMTEXT": "TEXT", "LONGTEXT": "TEXT", "STRING": "TEXT", "CLOB": "TEXT",
	"UUID": "TEXT", "JSON": "TEXT", "JSONB": "TEXT", "ENUM": "TEXT", "SET": "TEXT",
	"DATE": "TEXT", "TIME": "TEXT", "DATETIME": "TEXT", "TIMESTAMP": "TEXT",
	"TIMESTAMPTZ": "TEXT", "TIMESTAMP WITH TIME ZONE": "TEXT",
	"TIMESTAMP WITHOUT TIME ZONE": "TEXT", "TIME WITH TIME ZONE": "TEXT",
	"TIME WITHOUT TIME ZONE": "TEXT", "INTERVAL": "TEXT", "INET": "TEXT",
	"CIDR": "TEXT", "MACADDR": "TEXT", "XML": "TEXT",

	"BLOB": "BLOB", "BYTEA": "BLOB", "BYTES": "BLOB", "BINARY": "BLOB",
	"VARBINARY": "BLOB", "TINYBLOB": "BLOB", "MEDIUMBLOB": "BLOB", "LONGBLOB": "BLOB",

	"BOOL": "BOOLEAN", "BOOLEAN": "BOOLEAN",
}
