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
)
