package filesql

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"strings"

	wireconv "github.com/nao1215/filesql/parser/wire"
)

// Fedwire file extension
const extFED = ".fed"

// isFedWireFile checks if the file path has Fedwire extension (case-insensitive).
// Returns false for paths that are only the extension (e.g., ".fed").
// Supports both ".fed" and ".FED" extensions.
func isFedWireFile(path string) bool {
	return len(path) > len(extFED) && strings.EqualFold(path[len(path)-len(extFED):], extFED)
}

// parseFedWireFile parses a Fedwire file and returns a single table along with the TableSet.
// A Fedwire file generates one table:
//   - {filename}_message: Flat table with all FEDWireMessage fields (~326 columns, 1 row)
//
// The returned TableSet can be used later for DumpFedWire to reconstruct the Fedwire file.
func parseFedWireFile(reader io.Reader, baseTableName string) ([]*table, *wireconv.TableSet, error) {
	tableSet, err := wireconv.ParseReader(reader)
	if err != nil {
		return nil, nil, fmt.Errorf("%w: failed to parse Fedwire file: %w", ErrWire, err)
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
func streamWireFileToDatabase(ctx context.Context, db DBTX, reader io.Reader, filePath, sourcePath string, replaceExisting bool) error {
	baseTableName := sanitizeTableName(tableFromFilePath(filePath))
	if err := validateTableName(baseTableName); err != nil {
		return err
	}

	tables, _, err := parseFedWireFile(reader, baseTableName)
	if err != nil {
		return err
	}

	for _, t := range tables {
		// Check if table already exists
		var tableExists int
		err := db.QueryRowContext(ctx,
			`SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?`,
			t.getName(),
		).Scan(&tableExists)
		if err != nil {
			return fmt.Errorf("%w: failed to check table existence: %w", ErrDatabaseOperation, err)
		}

		if tableExists > 0 {
			if !replaceExisting {
				return fmt.Errorf("%w: table '%s' already exists", ErrDuplicateTable, t.getName())
			}
			// Replace mode: drop the old table so the reloaded file's tables win.
			if _, err := db.ExecContext(ctx, `DROP TABLE IF EXISTS "`+t.getName()+`"`); err != nil {
				return fmt.Errorf("%w: failed to drop existing table %s: %w", ErrDatabaseOperation, t.getName(), err)
			}
		}

		// Create table
		if err := createTableFromColumnInfo(ctx, db, t.getName(), t.columnInfo); err != nil {
			return fmt.Errorf("%w: failed to create table %s: %w", ErrDatabaseOperation, t.getName(), err)
		}

		// Insert records
		if len(t.records) > 0 {
			if err := insertRecordsIntoTable(ctx, db, t.getName(), t.header, t.records); err != nil {
				return fmt.Errorf("%w: failed to insert records into %s: %w", ErrDatabaseOperation, t.getName(), err)
			}
		}
	}

	// The source is recorded on the same DBTX as the tables, so a rolled-back
	// load leaves neither behind.
	return recordFileSource(ctx, db, baseTableName, sourcePath, sourceFormatFedWire)
}

// DumpFedWire exports Fedwire tables from the database back to a Fedwire file.
// This function reconstructs the Fedwire file from the _message table that was
// created when the file was loaded.
//
// The original structure is rebuilt from the file db records as its source, so
// that file must still exist and be readable. A database loaded from an
// io.Reader has no such file; pass the structure to DumpFedWireWithTableSet.
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
	tableSet, err := wireTableSetForDump(ctx, db, baseTableName)
	if err != nil {
		return err
	}
	return DumpFedWireWithTableSet(ctx, db, baseTableName, outputPath, tableSet)
}

// DumpFedWireWithTableSet exports Fedwire tables from the database back to a Fedwire file
// using an explicitly provided TableSet.
//
// Use this function when the database has no source file to read the structure
// back from, which is the case for one loaded from an io.Reader: parse the
// reader with parser/wire and pass the TableSet it returns. DumpFedWire is the
// same export for a database that does know its source.
//
// Parameters:
//   - ctx: Context for cancellation
//   - db: The database containing Fedwire tables
//   - baseTableName: The base name used when the Fedwire file was loaded (e.g., "payment" for payment.fed)
//   - outputPath: The path where the Fedwire file should be written
//   - tableSet: The TableSet containing the original Fedwire structure
//
// Returns an error if the export fails.
func DumpFedWireWithTableSet(ctx context.Context, db *sql.DB, baseTableName, outputPath string, tableSet *wireconv.TableSet) error {
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
	return writeFileAtomically(outputPath, func(w io.Writer) error {
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
