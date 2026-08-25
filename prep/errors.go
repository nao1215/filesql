package prep

import (
	"errors"
	"fmt"

	"github.com/nao1215/filesql/parser"
)

// Sentinel errors for fileprep
var (
	// ErrStructSlicePointer is returned when the value is not a pointer to a struct slice
	ErrStructSlicePointer = errors.New("value must be a pointer to a struct slice")
	// ErrUnsupportedFileType is returned when the file type is not supported
	ErrUnsupportedFileType = errors.New("unsupported file type")
	// ErrEmptyFile is returned when the file is empty
	ErrEmptyFile = errors.New("file is empty")
	// ErrInvalidTagFormat is returned when the tag format is invalid
	ErrInvalidTagFormat = errors.New("invalid tag format")
	// ErrInvalidJSONAfterPrep is returned when preprocessing destroys JSON structure
	// in the "data" column of a JSON/JSONL file. This is a hard error because
	// invalid JSON lines in JSONL output cause downstream parsers to fail.
	ErrInvalidJSONAfterPrep = errors.New("preprocessing produced invalid JSON")
	// ErrEmptyJSONOutput is returned when all JSON/JSONL rows are empty or invalid
	// after preprocessing, resulting in no output lines. An empty JSONL output is
	// unparseable by downstream consumers.
	ErrEmptyJSONOutput = errors.New("JSON/JSONL output has no valid rows after preprocessing")
	// ErrNilWriter is returned when a nil io.Writer is passed to ProcessToWriter.
	ErrNilWriter = errors.New("writer must not be nil")
	// ErrNilReader is returned when a nil io.Reader is passed to Process or ProcessToWriter.
	ErrNilReader = errors.New("reader must not be nil")
	// ErrUnknownColumn is returned when a struct field names a column the input
	// does not have. Such a field used to be filled with the zero value and then
	// validated, so a typo was reported as a missing value in a column that does
	// not exist, and the caller could not tell a mapping mistake from bad data.
	ErrUnknownColumn = errors.New("struct field names a column the input does not have")
)

// ValidationError represents a validation error with row and column information.
//
// Example:
//
//	for _, ve := range result.ValidationErrors() {
//	    fmt.Printf("Row %d, Column %q: %s (value=%q)\n",
//	        ve.Row, ve.Column, ve.Message, ve.Value)
//	}
type ValidationError struct {
	Row     int    // 1-based row number (excluding header)
	Column  string // Column name
	Field   string // Struct field name
	Value   string // The value that failed validation
	Tag     string // The validation tag that failed
	Message string // Human-readable error message
}

// Error implements the error interface
func (e *ValidationError) Error() string {
	return fmt.Sprintf("row %d, column %q (field %s): %s (value=%q, tag=%s)",
		e.Row, e.Column, e.Field, e.Message, e.Value, e.Tag)
}

// newValidationError creates a new ValidationError
func newValidationError(row int, column, field, value, tag, message string) *ValidationError {
	return &ValidationError{
		Row:     row,
		Column:  column,
		Field:   field,
		Value:   value,
		Tag:     tag,
		Message: message,
	}
}

// PrepError represents a preprocessing error.
//
// Example:
//
//	for _, pe := range result.PrepErrors() {
//	    fmt.Printf("Row %d, Column %q: %s (tag=%q)\n",
//	        pe.Row, pe.Column, pe.Message, pe.Tag)
//	}
//
//nolint:revive // Preserve the existing public API name from the standalone prep package.
type PrepError struct {
	Row     int    // 1-based row number
	Column  string // Column name
	Field   string // Struct field name
	Tag     string // The prep tag that failed
	Message string // Human-readable error message
}

// Error implements the error interface
func (e *PrepError) Error() string {
	return fmt.Sprintf("row %d, column %q (field %s): prep error - %s (tag=%s)",
		e.Row, e.Column, e.Field, e.Message, e.Tag)
}

// newPrepError creates a new PrepError
func newPrepError(row int, column, field, tag, message string) *PrepError {
	return &PrepError{
		Row:     row,
		Column:  column,
		Field:   field,
		Tag:     tag,
		Message: message,
	}
}

// ProcessResult contains the results of processing a file.
//
// Example:
//
//	reader, result, err := processor.Process(input, &records)
//	if result.HasErrors() {
//	    for _, ve := range result.ValidationErrors() {
//	        fmt.Printf("Row %d: %s\n", ve.Row, ve.Message)
//	    }
//	}
//	fmt.Printf("Valid: %d/%d rows\n", result.ValidRowCount, result.RowCount)
type ProcessResult struct {
	// Errors contains all validation and preprocessing errors
	Errors []error
	// RowCount is the total number of data rows processed (excluding header)
	RowCount int
	// ValidRowCount is the number of rows that passed all validations
	ValidRowCount int
	// Columns contains the column names from the header
	Columns []string
	// OriginalFormat is the file type that was processed
	OriginalFormat parser.FileType

	// validRecords holds rows that passed validation when validRowsOnly is
	// enabled. This is an internal field used between processRecords and
	// the output stage; it is cleared after output is written.
	validRecords [][]string
}

// InvalidRowCount returns the number of rows that failed validation
func (r *ProcessResult) InvalidRowCount() int {
	return r.RowCount - r.ValidRowCount
}

// HasErrors returns true if there are any errors
func (r *ProcessResult) HasErrors() bool {
	return len(r.Errors) > 0
}

// ValidationErrors returns only validation errors
func (r *ProcessResult) ValidationErrors() []*ValidationError {
	var errs []*ValidationError
	for _, err := range r.Errors {
		var ve *ValidationError
		if errors.As(err, &ve) {
			errs = append(errs, ve)
		}
	}
	return errs
}

// PrepErrors returns only preprocessing errors
func (r *ProcessResult) PrepErrors() []*PrepError {
	var errs []*PrepError
	for _, err := range r.Errors {
		var pe *PrepError
		if errors.As(err, &pe) {
			errs = append(errs, pe)
		}
	}
	return errs
}

// wrapParseError gives an error from parser.Parse the prep sentinel that
// matches it, so a caller of this package can use errors.Is against the
// sentinels this package documents.
//
// This matched the parser's message text until the parser exported a sentinel
// for each of these three conditions. Matching text meant keeping a list of the
// read side's wordings inside a package that cannot see them, and the list had
// holes: "empty XLSX file" was never on it, so a workbook with nothing in it
// reached the caller with no ErrEmptyFile on it, and a wording the reader gained
// later was not on it either.
func wrapParseError(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, parser.ErrUnsupportedFileType):
		return fmt.Errorf("%w: %w", ErrUnsupportedFileType, err)
	case errors.Is(err, parser.ErrNilReader):
		return fmt.Errorf("%w: %w", ErrNilReader, err)
	case errors.Is(err, parser.ErrEmptyData):
		return fmt.Errorf("%w: %w", ErrEmptyFile, err)
	default:
		return err
	}
}
