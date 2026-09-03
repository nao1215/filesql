package filesql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"strings"

	wireconv "github.com/nao1215/filesql/internal/parser/wire"
	filereader "github.com/nao1215/filesql/internal/reader"
)

// Fedwire file extension
const extFED = ".fed"

// isFedWireFile reports whether a path names a Fedwire file, by its extension
// and ignoring case. A name that is only the extension is not one, read from
// the base name for the reason isACHFile gives.
func isFedWireFile(path string) bool {
	base := filepath.Base(path)
	return len(base) > len(extFED) && strings.EqualFold(base[len(base)-len(extFED):], extFED)
}

// parseFedWireFile parses a Fedwire file and returns a single table along with the TableSet.
// A Fedwire file generates one table:
//   - {filename}_message: Flat table with all FEDWireMessage fields (~326 columns, 1 row)
//
// The returned TableSet can be used later for DumpFedWire to reconstruct the Fedwire file.
func parseFedWireFile(reader io.Reader, baseTableName string) ([]*table, *wireconv.TableSet, error) {
	// The library that reads a Fedwire file holds it whole, so a stream sending
	// a record with no terminator would be read however long it is. The bound is
	// the one every other record here is read against, and the one ACH already
	// holds to.
	//
	// It is read through a recorder because that library scans lines and reports
	// what the message it managed to build is missing rather than why the read
	// stopped, so a stream refused by the bound came back as a complaint about
	// an absent field. ACH needs no such wrapper: its library returns the read's
	// own error.
	source := &recordingReader{src: filereader.BoundRecords(reader)}
	tableSet, err := wireconv.ParseReader(source)
	if err != nil {
		if source.err != nil {
			err = source.err
		}
		// The parser's own error already names the file it could not read, so
		// repeating it here read as two failures rather than one.
		return nil, nil, fmt.Errorf("%w: %w", ErrWire, err)
	}
	if tableSet == nil {
		return nil, nil, fmt.Errorf("%w: failed to convert Fedwire file to tables", ErrWire)
	}

	msgTable := tableSet.GetMessageTable()
	if msgTable == nil || len(msgTable.Records) == 0 {
		return nil, nil, fmt.Errorf("%w: Fedwire file contains no data", ErrEmptyData)
	}

	t := fileParserTableDataToTable(msgTable, baseTableName+"_message")
	return []*table{t}, tableSet, nil
}

// recordingReader keeps the first error its source returned, so a caller can
// report why a read stopped after a library that swallowed it has answered with
// something else.
type recordingReader struct {
	src io.Reader
	err error
}

// Read implements io.Reader. io.EOF is the ordinary end of a stream rather than
// a reason a read stopped short, so it is not recorded.
func (r *recordingReader) Read(p []byte) (int, error) {
	n, err := r.src.Read(p)
	if err != nil && !errors.Is(err, io.EOF) && r.err == nil {
		r.err = err
	}
	return n, err
}

// isWireBaseTableName checks if a table name matches the Fedwire naming convention
// (ends with _message suffix and has a non-empty base name).
// It does NOT verify that the database was loaded from a Fedwire file; any
// table ending in _message matches.
func isWireBaseTableName(tableName string) (baseName string, isWire bool) {
	const suffix = "_message"
	if strings.HasSuffix(tableName, suffix) {
		base := strings.TrimSuffix(tableName, suffix)
		if base != "" {
			return base, true
		}
	}
	return "", false
}

// streamWireFileToDatabase streams a Fedwire file to the database as a single table.
func streamWireFileToDatabase(ctx context.Context, db dbtx, reader io.Reader, filePath, sourcePath string, replaceExisting bool) error {
	baseTableName := sanitizeTableName(tableFromFilePath(filePath))
	if err := validateTableName(baseTableName); err != nil {
		return err
	}

	tables, _, err := parseFedWireFile(reader, baseTableName)
	if err != nil {
		return err
	}

	return loadParsedTablesIntoDatabase(ctx, db, tables, baseTableName, sourcePath, sourceFormatFedWire, replaceExisting)
}

// DumpFedWire exports Fedwire tables from the database back to a Fedwire file.
// This function reconstructs the Fedwire file from the _message table that was
// created when the file was loaded.
//
// The original structure is rebuilt from the file db records as its source, so
// that file must still exist and be readable. A database loaded from an
// io.Reader has no such file; hand its bytes to DumpFedWireWithSource.
//
// The file is written from that structure rather than patched, so a tag the
// caller did not edit can come back changed: tags are written in the order the
// format defines rather than the order the file had them, and field padding is
// normalized.
//
// Parameters:
//   - ctx: Context for cancellation
//   - db: The database containing Fedwire tables
//   - baseTableName: The base name used when the Fedwire file was loaded (e.g., "payment" for payment.fed)
//   - outputPath: The path where the Fedwire file should be written
//
// Returns an error if the export fails, or ErrSourceUnavailable if the file the
// tables were loaded from cannot be read.
func DumpFedWire(ctx context.Context, db *sql.DB, baseTableName, outputPath string) error {
	if db == nil {
		return fmt.Errorf("%w: database must be a non-nil *sql.DB", ErrNilInput)
	}
	tableSet, err := wireTableSetForDump(ctx, db, baseTableName)
	if err != nil {
		return err
	}
	return dumpFedWireWithTableSet(ctx, db, baseTableName, outputPath, tableSet, nil)
}

// DumpFedWireWithSource exports Fedwire tables from the database back to a
// Fedwire file, reading the file's original structure from source.
//
// Use this function when the database has no source file to read the structure
// back from, which is the case for one loaded from an io.Reader. source must
// yield the same Fedwire file the database was loaded from: the file carries
// fields no table exposes, so the export applies the database's edits to the
// original rather than building a file out of the tables alone. DumpFedWire is
// the same export for a database that does know its source.
//
// Parameters:
//   - ctx: Context for cancellation
//   - db: The database containing Fedwire tables
//   - baseTableName: The base name used when the Fedwire file was loaded (e.g., "payment" for payment.fed)
//   - outputPath: The path where the Fedwire file should be written
//   - source: The original Fedwire file's bytes, read from the beginning
//
// Returns an error if the export fails.
func DumpFedWireWithSource(ctx context.Context, db *sql.DB, baseTableName, outputPath string, source io.Reader) error {
	if db == nil {
		return fmt.Errorf("%w: database must be a non-nil *sql.DB", ErrNilInput)
	}
	if source == nil {
		return fmt.Errorf("%w: source must be a non-nil io.Reader", ErrNilInput)
	}
	// The bound and the recorder the load path uses apply here too: the library
	// reads the whole file, and it reports what the message it built is missing
	// rather than why the read stopped, so a stream the bound refused came back
	// as a complaint about an absent field.
	recorder := &recordingReader{src: filereader.BoundRecords(source)}
	tableSet, err := wireconv.ParseReader(recorder)
	if err != nil {
		if recorder.err != nil {
			err = recorder.err
		}
		return fmt.Errorf("%w: %w", ErrWire, err)
	}
	return dumpFedWireWithTableSet(ctx, db, baseTableName, outputPath, tableSet, nil)
}

// stageFedWire is DumpFedWire writing into a set, which is how an in-place save
// of several files produces this one without putting it in place yet.
func stageFedWire(ctx context.Context, db *sql.DB, baseTableName, outputPath string, set *writeSet) error {
	tableSet, err := wireTableSetForDump(ctx, db, baseTableName)
	if err != nil {
		return err
	}
	return dumpFedWireWithTableSet(ctx, db, baseTableName, outputPath, tableSet, set)
}

func dumpFedWireWithTableSet(ctx context.Context, db *sql.DB, baseTableName, outputPath string, tableSet *wireconv.TableSet, set *writeSet) error {
	if db == nil {
		return fmt.Errorf("%w: database must be a non-nil *sql.DB", ErrNilInput)
	}
	if tableSet == nil {
		return fmt.Errorf("%w: tableSet must be a non-nil *wireconv.TableSet", ErrNilInput)
	}

	// Read updated data from database and update the TableSet
	if err := updateWireTableSetFromDB(ctx, db, baseTableName, tableSet); err != nil {
		return fmt.Errorf("%w: failed to read updated data from database: %w", ErrWire, err)
	}

	// Write the Fedwire file using WriteToWriter. The writer validates while it
	// encodes, so it can reject the data after the output has been opened.
	// Staging the write keeps a rejection from destroying the destination; the
	// previous failure path removed the output path as a "partial file", which
	// for an in-place save deleted the source it was saving.
	return set.write(outputPath, func(w io.Writer) error {
		if err := tableSet.WriteToWriter(w); err != nil {
			return fmt.Errorf("%w: failed to write Fedwire file: %w", ErrWire, err)
		}
		return nil
	})
}

// updateWireTableSetFromDB reads updated data from the database and updates the TableSet.
// Unlike ACH (which has optional tables), Fedwire has a single mandatory message table,
// so read errors are propagated to prevent exporting stale data.
func updateWireTableSetFromDB(ctx context.Context, db *sql.DB, baseTableName string, ts *wireconv.TableSet) error {
	messageTableName := baseTableName + "_message"
	messageData, err := readTableToTableData(ctx, db, messageTableName)
	if err != nil {
		return fmt.Errorf("failed to read message table %s: %w", messageTableName, err)
	}
	ts.UpdateMessageFromTableData(messageData)
	return nil
}
