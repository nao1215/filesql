// Package model provides domain model for filesql
package model

import "errors"

// ErrDuplicateColumnName is returned when a file contains duplicate column names
var ErrDuplicateColumnName = errors.New("duplicate column name")
