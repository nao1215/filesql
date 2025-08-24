package driver

import "errors"

// Predefined errors
var (
	// ErrNoPathsProvided is returned when no paths are provided
	ErrNoPathsProvided = errors.New("filesql driver: no paths provided")

	// ErrNoFilesLoaded is returned when no files were loaded
	ErrNoFilesLoaded = errors.New("filesql driver: no files were loaded")

	// ErrStmtExecContextNotSupported is returned when statement does not support ExecContext
	ErrStmtExecContextNotSupported = errors.New("filesql driver: statement does not support ExecContext")

	// ErrBeginTxNotSupported is returned when underlying connection does not support BeginTx
	ErrBeginTxNotSupported = errors.New("filesql driver: underlying connection does not support BeginTx")

	// ErrPrepareContextNotSupported is returned when underlying connection does not support PrepareContext
	ErrPrepareContextNotSupported = errors.New("filesql driver: underlying connection does not support PrepareContext")

	// ErrNotFilesqlConnection is returned when connection is not a filesql connection
	ErrNotFilesqlConnection = errors.New("filesql driver: connection is not a filesql connection")

	// ErrDuplicateColumnName is returned when a file contains duplicate column names
	ErrDuplicateColumnName = errors.New("filesql driver: duplicate column name")

	// ErrDuplicateTableName is returned when multiple files would create the same table name
	ErrDuplicateTableName = errors.New("filesql driver: duplicate table name")

	// ErrResourceExhaustion is returned when resource limits are exceeded
	ErrResourceExhaustion = errors.New("filesql driver: resource exhaustion detected")

	// ErrSecurityViolation is returned when a security policy is violated
	ErrSecurityViolation = errors.New("filesql driver: security policy violation")
)
