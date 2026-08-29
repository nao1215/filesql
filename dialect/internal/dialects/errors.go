package dialects

import "errors"

// ErrUnknownDialect indicates a dialect name that does not map to a built-in
// dialect. It lives here rather than in sqlerr because ParseName returns it and
// sqlerr would then have to import this package for nothing.
var ErrUnknownDialect = errors.New("dialect: unknown SQL dialect")
