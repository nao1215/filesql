package filesql

import "errors"

// errDuplicateColumnName is returned when a file contains duplicate column names
var errDuplicateColumnName = errors.New("duplicate column name")
