package lower

import "github.com/nao1215/filesql/dialect/internal/dialects"

// The cast targets the runtime helpers convert to. A target outside these
// tables is left to SQLite's own CAST, which is the only other conversion there
// is; sending it to a helper would answer NULL for a type the caller wrote.
//
// The tables are here rather than read from the runtime package because the
// dependency runs the other way: lowering names helpers, and the runtime knows
// nothing about lowering. A test in lower_test holds the two lists to each
// other, so a target added to one and not the other is a failure rather than a
// silent fall-through.

var commonCastTargets = map[string]bool{ //nolint:gochecknoglobals // a fixed table
	"BIGINT": true, "BLOB": true, "INTEGER": true, "BOOL": true, "BOOLEAN": true, "CHAR": true, "DATE": true,
	"DATETIME": true, "DECIMAL": true, "DOUBLE": true, "FLOAT": true, "INT": true,
	"JSON": true, "NUMERIC": true, "REAL": true, "SMALLINT": true, "TEXT": true, "TIME": true,
	"TIMESTAMP": true, "TINYINT": true, "VARCHAR": true,
}

var commonCastTargetsExtra = map[string]bool{ //nolint:gochecknoglobals // a fixed table
	// The spellings every dialect here writes and SQLite does not name: a type
	// with a length word in it, and the standard's long forms.
	"CHARACTER VARYING": true, "DOUBLE PRECISION": true,
}

var mysqlCastTargets = map[string]bool{ //nolint:gochecknoglobals // a fixed table
	"BINARY": true, "DEC": true, "MEDIUMINT": true, "NCHAR": true, "NVARCHAR": true,
	"SIGNED": true, "UNSIGNED": true, "VARBINARY": true, "YEAR": true,
	"CHARACTER": true, "NATIONAL CHAR": true, "NATIONAL VARCHAR": true,
	"TINYTEXT": true, "MEDIUMTEXT": true, "LONGTEXT": true,
	"TINYBLOB": true, "MEDIUMBLOB": true, "LONGBLOB": true,
}

var postgresCastTargets = map[string]bool{ //nolint:gochecknoglobals // a fixed table
	"BIGSERIAL": true, "BPCHAR": true, "BYTEA": true, "CHARACTER": true, "FLOAT4": true,
	"FLOAT8": true, "INT2": true, "INT4": true, "INT8": true, "INTERVAL": true, "JSONB": true,
	"MONEY": true, "NAME": true, "SERIAL": true, "TIMESTAMPTZ": true, "UUID": true,
	typeNameSmallserial: true, "INET": true, "CIDR": true, "MACADDR": true, "XML": true,
	typeNameTimestampWithZone: true, typeNameTimestampWithoutZone: true,
	"TIME WITH TIME ZONE": true, "TIME WITHOUT TIME ZONE": true,
}

var googleCastTargets = map[string]bool{ //nolint:gochecknoglobals // a fixed table
	"BIGDECIMAL": true, "BIGNUMERIC": true, "BYTEINT": true, "BYTES": true, "FLOAT64": true,
	"INT64": true, "STRING": true, "INTERVAL": true,
}

// knownCastTarget reports whether a helper converts to the named type in a
// dialect.
func knownCastTarget(d dialects.Dialect, name string) bool {
	var own map[string]bool
	switch d {
	case dialects.MySQL:
		own = mysqlCastTargets
	case dialects.PostgreSQL:
		own = postgresCastTargets
	case dialects.GoogleSQL:
		own = googleCastTargets
	case dialects.SQLite:
		return false
	}
	return own[name] || commonCastTargets[name] || commonCastTargetsExtra[name]
}

// CastTargets lists the targets a dialect's helper converts to, for the test
// that holds this table to the runtime's.
func CastTargets(d dialects.Dialect) []string {
	var own map[string]bool
	switch d {
	case dialects.MySQL:
		own = mysqlCastTargets
	case dialects.PostgreSQL:
		own = postgresCastTargets
	case dialects.GoogleSQL:
		own = googleCastTargets
	case dialects.SQLite:
		return nil
	}
	names := make([]string, 0, len(own)+len(commonCastTargets)+len(commonCastTargetsExtra))
	for _, table := range []map[string]bool{own, commonCastTargets, commonCastTargetsExtra} {
		for name := range table {
			names = append(names, name)
		}
	}
	return names
}
