package filesql

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"strings"
	"sync"

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

// wireTableSetRegistry stores Fedwire TableSets for later retrieval by DumpFedWire.
// Key is the base table name (e.g., "payment" for payment.fed).
// Protected by wireRegistryMu for concurrent access safety.
var (
	wireTableSetRegistry = make(map[string]*wireconv.TableSet)
	wireRegistryMu       sync.RWMutex
)

// registerWireTableSet stores a Fedwire TableSet in the registry for later retrieval.
func registerWireTableSet(baseTableName string, ts *wireconv.TableSet) {
	wireRegistryMu.Lock()
	defer wireRegistryMu.Unlock()
	wireTableSetRegistry[baseTableName] = ts
}

// getWireTableSet retrieves a Fedwire TableSet from the registry.
func getWireTableSet(baseTableName string) *wireconv.TableSet {
	wireRegistryMu.RLock()
	defer wireRegistryMu.RUnlock()
	return wireTableSetRegistry[baseTableName]
}

// getWireBaseTableNames returns all base table names that have registered Fedwire TableSets.
// This is useful for auto-save functionality to know which Fedwire files need to be saved.
func getWireBaseTableNames() []string {
	wireRegistryMu.RLock()
	defer wireRegistryMu.RUnlock()
	names := make([]string, 0, len(wireTableSetRegistry))
	for name := range wireTableSetRegistry {
		names = append(names, name)
	}
	return names
}

// WireTableInfo represents information about Fedwire tables created from a Fedwire file.
// It provides methods to get the complete table name for the message table.
type WireTableInfo struct {
	// BaseName is the base table name derived from the Fedwire filename.
	// For example, if the file is "payment.fed", BaseName is "payment".
	BaseName string
}

// MessageTable returns the complete table name for the message table.
// Example: "payment" -> "payment_message"
func (w WireTableInfo) MessageTable() string {
	return w.BaseName + "_message"
}

// AllTableNames returns all Fedwire table names for this base name.
func (w WireTableInfo) AllTableNames() []string {
	return []string{w.MessageTable()}
}

// GetWireTableInfos returns WireTableInfo for all registered Fedwire files.
// Each WireTableInfo provides methods to get complete table names.
//
// Example:
//
//	db, _ := filesql.Open("payment.fed")
//	infos := filesql.GetWireTableInfos()
//	for _, info := range infos {
//	    fmt.Println(info.MessageTable()) // "payment_message"
//	}
func GetWireTableInfos() []WireTableInfo {
	wireRegistryMu.RLock()
	defer wireRegistryMu.RUnlock()
	infos := make([]WireTableInfo, 0, len(wireTableSetRegistry))
	for name := range wireTableSetRegistry {
		infos = append(infos, WireTableInfo{BaseName: name})
	}
	return infos
}

// UnregisterWireTableSet removes a Fedwire TableSet from the registry.
// Call this when you're done with a Fedwire file to free memory.
// This is automatically called by the auto-save connection when using EnableAutoSave.
//
// Example:
//
//	db, _ := filesql.Open("payment.fed")
//	// ... work with the database ...
//	db.Close()
//	filesql.UnregisterWireTableSet("payment") // Free the TableSet memory
func UnregisterWireTableSet(baseTableName string) {
	wireRegistryMu.Lock()
	defer wireRegistryMu.Unlock()
	delete(wireTableSetRegistry, baseTableName)
}

// ClearWireTableSetRegistry removes all Fedwire TableSets from the registry.
// Use this to reset state, typically in tests or when shutting down.
func ClearWireTableSetRegistry() {
	wireRegistryMu.Lock()
	defer wireRegistryMu.Unlock()
	wireTableSetRegistry = make(map[string]*wireconv.TableSet)
}

// IsWireBaseTableName checks if a table name matches the Fedwire naming convention
// (ends with _message suffix and has a non-empty base name).
// It does NOT verify that a TableSet is registered; callers should check
// getWireTableSet(baseName) separately if registry confirmation is needed.
func IsWireBaseTableName(tableName string) (baseName string, isWire bool) {
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
func streamWireFileToDatabase(ctx context.Context, db DBTX, reader io.Reader, filePath string, replaceExisting bool) error {
	baseTableName := sanitizeTableName(tableFromFilePath(filePath))

	tables, tableSet, err := parseFedWireFile(reader, baseTableName)
	if err != nil {
		return err
	}

	// Store the TableSet in the registry for later use by DumpFedWire
	registerWireTableSet(baseTableName, tableSet)

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

	return nil
}

// DumpFedWire exports Fedwire tables from the database back to a Fedwire file.
// This function reconstructs the Fedwire file from the _message table that was
// created when the file was loaded.
//
// The TableSet is automatically retrieved from the internal registry if the Fedwire file
// was loaded via Open() or Builder. If you have the TableSet from another source,
// use DumpFedWireWithTableSet instead.
//
// Parameters:
//   - ctx: Context for cancellation
//   - db: The database containing Fedwire tables
//   - baseTableName: The base name used when the Fedwire file was loaded (e.g., "payment" for payment.fed)
//   - outputPath: The path where the Fedwire file should be written
//
// Returns an error if the export fails or if no TableSet is found for the given base table name.
func DumpFedWire(ctx context.Context, db *sql.DB, baseTableName, outputPath string) error {
	tableSet := getWireTableSet(baseTableName)
	if tableSet == nil {
		return fmt.Errorf("%w: no Fedwire TableSet found for base table name '%s'; ensure the Fedwire file was loaded via Open() or Builder", ErrTableNotFound, baseTableName)
	}
	return DumpFedWireWithTableSet(ctx, db, baseTableName, outputPath, tableSet)
}

// DumpFedWireWithTableSet exports Fedwire tables from the database back to a Fedwire file
// using an explicitly provided TableSet.
//
// Use this function when you have the TableSet from a source other than the internal registry,
// or when you need more control over which TableSet to use.
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
