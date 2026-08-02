package filesql

// ACH File Support
//
// filesql supports reading and writing ACH (Automated Clearing House) files,
// the standard format for US bank transfers. ACH files are loaded as multiple
// SQL tables for easy querying and modification.
//
// # Tables Created
//
// When an ACH file (e.g., "payment.ach") is loaded, the following tables are created:
//
// Standard batches:
//   - {filename}_file_header: File-level header information (1 row)
//   - {filename}_batches: Batch header information
//   - {filename}_entries: Entry detail records (main transaction data)
//   - {filename}_addenda: Addenda records (types 02, 05, 98, 99)
//
// IAT (International ACH Transactions) - if present:
//   - {filename}_iat_batches: IAT batch header information
//   - {filename}_iat_entries: IAT entry detail records
//   - {filename}_iat_addenda: IAT addenda records (types 10-18, 98, 99)
//
// # Supported Operations
//
// The following modifications via SQL are supported and will be reflected
// when exporting back to ACH format:
//   - Updating field values in any table (amount, names, accounts, etc.)
//   - File header modifications (dates, identifiers, names)
//   - Batch header modifications (company info, entry descriptions)
//   - Entry modifications (transaction codes, amounts, account numbers)
//   - Addenda modifications (payment information, return codes)
//
// # Limitations (IMPORTANT)
//
// The following operations are NOT supported in the current implementation:
//   - Adding new rows (INSERT) - new entries, batches, or addenda will be ignored
//   - Deleting existing rows (DELETE) - deletions will be ignored
//   - Reordering rows - row order changes will be ignored
//
// Only UPDATE operations on existing rows are reflected in the output ACH file.
// The export uses batch_index/entry_index/addenda_index to match rows with the
// original ACH structure. Rows with indices outside the original range are ignored.
//
// These limitations exist because ACH file structure requires careful
// coordination between related records (e.g., entry count in batch control,
// addenda indicators, hash totals). Future versions may support these operations.
//
// # Exporting ACH Files
//
// ACH files can be exported in two ways:
//
// 1. Using DumpACH explicitly (recommended for single file):
//
//	filesql.DumpACH(ctx, db, "payment", "output.ach")
//
// 2. Using DumpDatabase (automatically detects and exports ACH files):
//
//	filesql.DumpDatabase(db, "./output") // ACH tables → .ach files, others → CSV/etc
//
// DumpDatabase automatically detects ACH-related tables (by suffix pattern) and
// exports them as combined ACH files when a registered TableSet exists.
//
// # Auto-Save Support
//
// ACH files support auto-save functionality in two ways:
//
// 1. Overwrite mode - saves back to original ACH file:
//
//	builder.AddPath("payment.ach").EnableAutoSave("")
//
// 2. Output directory with ACH format:
//
//	opts := NewDumpOptions().WithFormat(OutputFormatACH)
//	builder.AddPath("payment.ach").EnableAutoSave("./output", opts)
//
// Note: EnableAutoSave without OutputFormatACH will export tables as CSV, not ACH.
//
// # Concurrency
//
// The internal TableSet registry is protected by sync.RWMutex for concurrent access.
// Multiple goroutines can safely load different ACH files simultaneously.
//
// # Registry Lifecycle
//
// ACH TableSets are stored in a global registry to enable DumpACH functionality.
// Memory is managed as follows:
//
//   - When using EnableAutoSave: Registry entries are automatically cleaned up
//     when the database connection is closed.
//
//   - When using Open() without EnableAutoSave: Call UnregisterACHTableSet()
//     after closing the database to free memory:
//
//     db, _ := filesql.Open("payment.ach")
//     defer filesql.UnregisterACHTableSet("payment")
//     defer db.Close()
//
//   - For tests or shutdown: Call ClearACHTableSetRegistry() to remove all entries.
//
// # Example
//
//	// Load ACH file
//	db, _ := filesql.Open("payment.ach")
//	defer db.Close()
//
//	// Query transactions
//	rows, _ := db.Query("SELECT * FROM payment_entries WHERE amount > 10000")
//
//	// Modify existing transaction (UPDATE is supported)
//	db.Exec("UPDATE payment_entries SET amount = 50000 WHERE trace_number = '123456789'")
//
//	// NOTE: INSERT/DELETE are NOT reflected in ACH output
//	// db.Exec("INSERT INTO payment_entries ...") // This will NOT appear in output ACH
//	// db.Exec("DELETE FROM payment_entries ...") // This will NOT remove from output ACH
//
//	// Export modified ACH file - MUST use DumpACH explicitly
//	filesql.DumpACH(ctx, db, "payment", "modified.ach")

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"strings"
	"sync"

	"github.com/nao1215/filesql/parser"
	achconv "github.com/nao1215/filesql/parser/ach"
)

// ACH file extension
const extACH = ".ach"

// isACHFile checks if the file path has ACH extension (case-insensitive).
// Returns false for paths that are only the extension (e.g., ".ach").
// Supports both ".ach" and ".ACH" extensions.
func isACHFile(path string) bool {
	return len(path) > len(extACH) && strings.EqualFold(path[len(path)-len(extACH):], extACH)
}

// parseACHFile parses an ACH file and returns multiple tables along with the TableSet.
// ACH files generate the following tables for standard batches:
//   - {filename}_file_header: File header information (1 row)
//   - {filename}_batches: Batch information
//   - {filename}_entries: Entry details (main transaction data)
//   - {filename}_addenda: Addenda records (types 02, 05, 98, 99)
//
// For IAT (International ACH Transactions):
//   - {filename}_iat_batches: IAT batch headers
//   - {filename}_iat_entries: IAT entry details
//   - {filename}_iat_addenda: IAT addenda records (types 10-18, 98, 99)
//
// The returned TableSet can be used later for DumpACH to reconstruct the ACH file.
func parseACHFile(reader io.Reader, baseTableName string) ([]*table, *achconv.TableSet, error) {
	// Read ACH file using parser/ach (which encapsulates moov-io/ach)
	tableSet, err := achconv.ParseReader(reader)
	if err != nil {
		return nil, nil, fmt.Errorf("%w: failed to parse ACH file: %w", ErrACH, err)
	}
	if tableSet == nil {
		return nil, nil, fmt.Errorf("%w: failed to convert ACH file to tables", ErrACH)
	}

	var tables []*table

	// Convert file header table
	if tableSet.FileHeader != nil && len(tableSet.FileHeader.Records) > 0 {
		t := fileParserTableDataToTable(tableSet.FileHeader, baseTableName+"_file_header")
		tables = append(tables, t)
	}

	// Convert batches table
	if tableSet.Batches != nil && len(tableSet.Batches.Records) > 0 {
		t := fileParserTableDataToTable(tableSet.Batches, baseTableName+"_batches")
		tables = append(tables, t)
	}

	// Convert entries table
	if tableSet.Entries != nil && len(tableSet.Entries.Records) > 0 {
		t := fileParserTableDataToTable(tableSet.Entries, baseTableName+"_entries")
		tables = append(tables, t)
	}

	// Convert addenda table
	if tableSet.Addenda != nil && len(tableSet.Addenda.Records) > 0 {
		t := fileParserTableDataToTable(tableSet.Addenda, baseTableName+"_addenda")
		tables = append(tables, t)
	}

	// Convert IAT batches table (International ACH Transactions)
	if tableSet.IATBatches != nil && len(tableSet.IATBatches.Records) > 0 {
		t := fileParserTableDataToTable(tableSet.IATBatches, baseTableName+"_iat_batches")
		tables = append(tables, t)
	}

	// Convert IAT entries table
	if tableSet.IATEntries != nil && len(tableSet.IATEntries.Records) > 0 {
		t := fileParserTableDataToTable(tableSet.IATEntries, baseTableName+"_iat_entries")
		tables = append(tables, t)
	}

	// Convert IAT addenda table (types 10-18, 98, 99)
	if tableSet.IATAddenda != nil && len(tableSet.IATAddenda.Records) > 0 {
		t := fileParserTableDataToTable(tableSet.IATAddenda, baseTableName+"_iat_addenda")
		tables = append(tables, t)
	}

	if len(tables) == 0 {
		return nil, nil, fmt.Errorf("%w: ACH file contains no data", ErrEmptyData)
	}

	return tables, tableSet, nil
}

// fileParserTableDataToTable converts parser.TableData to filesql table
func fileParserTableDataToTable(td *parser.TableData, tableName string) *table {
	headers := newHeader(td.Headers)
	records := make([]record, len(td.Records))
	for i, rec := range td.Records {
		records[i] = newRecord(rec)
	}

	// Convert column types using parser_bridge
	// Use bounds check to handle potential mismatch between headers and column types
	columnInfos := make([]columnInfo, len(td.Headers))
	for i, name := range td.Headers {
		colType := columnTypeText // Default to text if column type is not available
		if i < len(td.ColumnTypes) {
			colType = parserColumnType(td.ColumnTypes[i])
		}
		columnInfos[i] = columnInfo{
			Name: name,
			Type: colType,
		}
	}

	return &table{
		name:       newTableName(tableName),
		header:     headers,
		records:    records,
		columnInfo: columnInfos,
	}
}

// achTableSetRegistry stores ACH TableSets for later retrieval by DumpACH.
// Key is the base table name (e.g., "payment" for payment.ach).
// Protected by achRegistryMu for concurrent access safety.
var (
	achTableSetRegistry = make(map[string]*achconv.TableSet)
	achRegistryMu       sync.RWMutex
)

// registerACHTableSet stores an ACH TableSet in the registry for later retrieval.
func registerACHTableSet(baseTableName string, ts *achconv.TableSet) {
	achRegistryMu.Lock()
	defer achRegistryMu.Unlock()
	achTableSetRegistry[baseTableName] = ts
}

// getACHTableSet retrieves an ACH TableSet from the registry.
func getACHTableSet(baseTableName string) *achconv.TableSet {
	achRegistryMu.RLock()
	defer achRegistryMu.RUnlock()
	return achTableSetRegistry[baseTableName]
}

// getACHBaseTableNames returns all base table names that have registered TableSets.
// This is useful for auto-save functionality to know which ACH files need to be saved.
func getACHBaseTableNames() []string {
	achRegistryMu.RLock()
	defer achRegistryMu.RUnlock()
	names := make([]string, 0, len(achTableSetRegistry))
	for name := range achTableSetRegistry {
		names = append(names, name)
	}
	return names
}

// ACHTableInfo represents information about ACH tables created from an ACH file.
// It provides methods to get the complete table names for each ACH table type.
type ACHTableInfo struct {
	// BaseName is the base table name derived from the ACH filename.
	// For example, if the file is "payment.ach", BaseName is "payment".
	BaseName string
}

// FileHeaderTable returns the complete table name for the file header table.
// Example: "payment" -> "payment_file_header"
func (a ACHTableInfo) FileHeaderTable() string {
	return a.BaseName + "_file_header"
}

// BatchesTable returns the complete table name for the batches table.
// Example: "payment" -> "payment_batches"
func (a ACHTableInfo) BatchesTable() string {
	return a.BaseName + "_batches"
}

// EntriesTable returns the complete table name for the entries table.
// Example: "payment" -> "payment_entries"
func (a ACHTableInfo) EntriesTable() string {
	return a.BaseName + "_entries"
}

// AddendaTable returns the complete table name for the addenda table.
// Example: "payment" -> "payment_addenda"
func (a ACHTableInfo) AddendaTable() string {
	return a.BaseName + "_addenda"
}

// IATBatchesTable returns the complete table name for the IAT batches table.
// Example: "payment" -> "payment_iat_batches"
func (a ACHTableInfo) IATBatchesTable() string {
	return a.BaseName + "_iat_batches"
}

// IATEntriesTable returns the complete table name for the IAT entries table.
// Example: "payment" -> "payment_iat_entries"
func (a ACHTableInfo) IATEntriesTable() string {
	return a.BaseName + "_iat_entries"
}

// IATAddendaTable returns the complete table name for the IAT addenda table.
// Example: "payment" -> "payment_iat_addenda"
func (a ACHTableInfo) IATAddendaTable() string {
	return a.BaseName + "_iat_addenda"
}

// AllTableNames returns all possible ACH table names for this base name.
// This includes both standard and IAT tables.
func (a ACHTableInfo) AllTableNames() []string {
	return []string{
		a.FileHeaderTable(),
		a.BatchesTable(),
		a.EntriesTable(),
		a.AddendaTable(),
		a.IATBatchesTable(),
		a.IATEntriesTable(),
		a.IATAddendaTable(),
	}
}

// GetACHTableInfos returns ACHTableInfo for all registered ACH files.
// Each ACHTableInfo provides methods to get complete table names.
//
// Example:
//
//	db, _ := filesql.Open("payment.ach", "refund.ach")
//	infos := filesql.GetACHTableInfos()
//	for _, info := range infos {
//	    fmt.Println(info.EntriesTable()) // "payment_entries", "refund_entries"
//	}
func GetACHTableInfos() []ACHTableInfo {
	achRegistryMu.RLock()
	defer achRegistryMu.RUnlock()
	infos := make([]ACHTableInfo, 0, len(achTableSetRegistry))
	for name := range achTableSetRegistry {
		infos = append(infos, ACHTableInfo{BaseName: name})
	}
	return infos
}

// UnregisterACHTableSet removes an ACH TableSet from the registry.
// Call this when you're done with an ACH file to free memory.
// This is automatically called by the auto-save connection when using EnableAutoSave.
//
// Example:
//
//	db, _ := filesql.Open("payment.ach")
//	// ... work with the database ...
//	db.Close()
//	filesql.UnregisterACHTableSet("payment") // Free the TableSet memory
func UnregisterACHTableSet(baseTableName string) {
	achRegistryMu.Lock()
	defer achRegistryMu.Unlock()
	delete(achTableSetRegistry, baseTableName)
}

// ClearACHTableSetRegistry removes all ACH TableSets from the registry.
// Use this to reset state, typically in tests or when shutting down.
func ClearACHTableSetRegistry() {
	achRegistryMu.Lock()
	defer achRegistryMu.Unlock()
	achTableSetRegistry = make(map[string]*achconv.TableSet)
}

// IsACHBaseTableName checks if a table name is an ACH-related table
// (ends with _file_header, _batches, _entries, _addenda, _iat_batches, _iat_entries, or _iat_addenda).
func IsACHBaseTableName(tableName string) (baseName string, isACH bool) {
	suffixes := []string{
		"_file_header", "_batches", "_entries", "_addenda",
		"_iat_batches", "_iat_entries", "_iat_addenda",
	}
	for _, suffix := range suffixes {
		if strings.HasSuffix(tableName, suffix) {
			return strings.TrimSuffix(tableName, suffix), true
		}
	}
	return "", false
}

// streamACHFileToDatabase streams an ACH file to the database as multiple tables
func streamACHFileToDatabase(ctx context.Context, db DBTX, reader io.Reader, filePath string, replaceExisting bool, pending *PendingRegistries) error {
	baseTableName := sanitizeTableName(tableFromFilePath(filePath))

	tables, tableSet, err := parseACHFile(reader, baseTableName)
	if err != nil {
		return err
	}

	// Keep the TableSet private until the caller commits the transaction.
	pending.addACH(baseTableName, tableSet)

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

// createTableFromColumnInfo creates a SQLite table with the given columns
func createTableFromColumnInfo(ctx context.Context, db DBTX, tableName string, columnInfo []columnInfo) error {
	columns := make([]string, 0, len(columnInfo))
	for _, col := range columnInfo {
		columns = append(columns, fmt.Sprintf(`"%s" %s`, col.Name, col.Type.string()))
	}

	query := fmt.Sprintf(
		`CREATE TABLE IF NOT EXISTS "%s" (%s)`,
		tableName,
		strings.Join(columns, ", "),
	)

	_, err := db.ExecContext(ctx, query)
	return err
}

// insertRecordsIntoTable inserts records into the specified table
func insertRecordsIntoTable(ctx context.Context, db DBTX, tableName string, headers header, records []record) error {
	placeholders := make([]string, len(headers))
	for i := range placeholders {
		placeholders[i] = "?"
	}

	query := fmt.Sprintf( //nolint:gosec // SQL table name is validated, placeholders are safe
		`INSERT INTO "%s" VALUES (%s)`,
		tableName,
		strings.Join(placeholders, ", "),
	)

	stmt, err := db.PrepareContext(ctx, query)
	if err != nil {
		return fmt.Errorf("%w: failed to prepare insert statement: %w", ErrDatabaseOperation, err)
	}
	defer stmt.Close()

	for _, record := range records {
		values := make([]any, len(record))
		for i, value := range record {
			values[i] = value
		}

		if _, err := stmt.ExecContext(ctx, values...); err != nil {
			return fmt.Errorf("%w: failed to insert record: %w", ErrDatabaseOperation, err)
		}
	}

	return nil
}

// DumpACH exports ACH tables from the database back to an ACH file.
// This function reconstructs the ACH file from the _file_header, _batches,
// _entries, and _addenda tables that were created when the file was loaded.
//
// The TableSet is automatically retrieved from the internal registry if the ACH file
// was loaded via Open() or Builder. If you have the TableSet from another source,
// use DumpACHWithTableSet instead.
//
// Parameters:
//   - ctx: Context for cancellation
//   - db: The database containing ACH tables
//   - baseTableName: The base name used when the ACH file was loaded (e.g., "payment" for payment.ach)
//   - outputPath: The path where the ACH file should be written
//
// Returns an error if the export fails or if no TableSet is found for the given base table name.
func DumpACH(ctx context.Context, db *sql.DB, baseTableName, outputPath string) error {
	tableSet := getACHTableSet(baseTableName)
	if tableSet == nil {
		return fmt.Errorf("%w: no ACH TableSet found for base table name '%s'; ensure the ACH file was loaded via Open() or Builder", ErrTableNotFound, baseTableName)
	}
	return DumpACHWithTableSet(ctx, db, baseTableName, outputPath, tableSet)
}

// DumpACHWithTableSet exports ACH tables from the database back to an ACH file
// using an explicitly provided TableSet.
//
// Use this function when you have the TableSet from a source other than the internal registry,
// or when you need more control over which TableSet to use.
//
// Parameters:
//   - ctx: Context for cancellation
//   - db: The database containing ACH tables
//   - baseTableName: The base name used when the ACH file was loaded (e.g., "payment" for payment.ach)
//   - outputPath: The path where the ACH file should be written
//   - tableSet: The TableSet containing the original ACH structure
//
// Returns an error if the export fails.
func DumpACHWithTableSet(ctx context.Context, db *sql.DB, baseTableName, outputPath string, tableSet *achconv.TableSet) error {
	if tableSet == nil {
		return fmt.Errorf("%w: tableSet must be a non-nil *achconv.TableSet", ErrNilInput)
	}

	// Read updated data from database tables and update the TableSet
	if err := updateTableSetFromDB(ctx, db, baseTableName, tableSet); err != nil {
		return fmt.Errorf("%w: failed to read updated data from database: %w", ErrACH, err)
	}

	// Write the ACH file using WriteToWriter (encapsulates moov-io/ach). The
	// writer validates while it encodes, so it can reject the data after the
	// output has been opened; staging the write keeps a rejection from destroying
	// the destination, which for an in-place save is the source file itself.
	return writeFileAtomically(outputPath, func(w io.Writer) error {
		if err := tableSet.WriteToWriter(w); err != nil {
			return fmt.Errorf("%w: failed to write ACH file: %w", ErrACH, err)
		}
		return nil
	})
}

// updateTableSetFromDB reads updated data from the database and updates the TableSet.
// The error return is kept for future extensibility even though currently it always returns nil.
//
//nolint:unparam // Error return preserved for future extensibility
func updateTableSetFromDB(ctx context.Context, db *sql.DB, baseTableName string, ts *achconv.TableSet) error {
	// Update FileHeader table from database
	fileHeaderTableName := baseTableName + "_file_header"
	fileHeaderData, err := readTableToTableData(ctx, db, fileHeaderTableName)
	if err == nil {
		ts.UpdateFileHeaderFromTableData(fileHeaderData)
	}

	// Update Batches table from database
	batchesTableName := baseTableName + "_batches"
	batchesData, err := readTableToTableData(ctx, db, batchesTableName)
	if err == nil {
		ts.UpdateBatchesFromTableData(batchesData)
	}

	// Update Entries table from database
	entriesTableName := baseTableName + "_entries"
	entriesData, err := readTableToTableData(ctx, db, entriesTableName)
	if err == nil {
		ts.UpdateEntriesFromTableData(entriesData)
	}

	// Update Addenda table from database
	addendaTableName := baseTableName + "_addenda"
	addendaData, err := readTableToTableData(ctx, db, addendaTableName)
	if err == nil {
		ts.UpdateAddendaFromTableData(addendaData)
	}

	// Update IAT Batches table from database
	iatBatchesTableName := baseTableName + "_iat_batches"
	iatBatchesData, err := readTableToTableData(ctx, db, iatBatchesTableName)
	if err == nil {
		ts.UpdateIATBatchesFromTableData(iatBatchesData)
	}

	// Update IAT Entries table from database
	iatEntriesTableName := baseTableName + "_iat_entries"
	iatEntriesData, err := readTableToTableData(ctx, db, iatEntriesTableName)
	if err == nil {
		ts.UpdateIATEntriesFromTableData(iatEntriesData)
	}

	// Update IAT Addenda table from database
	iatAddendaTableName := baseTableName + "_iat_addenda"
	iatAddendaData, err := readTableToTableData(ctx, db, iatAddendaTableName)
	if err == nil {
		ts.UpdateIATAddendaFromTableData(iatAddendaData)
	}

	return nil
}

// readTableToTableData reads a database table into a TableData structure
func readTableToTableData(ctx context.Context, db *sql.DB, tableName string) (*parser.TableData, error) {
	// Check if table exists
	var exists int
	err := db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?",
		tableName,
	).Scan(&exists)
	if err != nil || exists == 0 {
		return nil, fmt.Errorf("%w: table %s does not exist", ErrTableNotFound, tableName)
	}

	// Get columns
	query := fmt.Sprintf("PRAGMA table_info(`%s`)", tableName)
	colRows, err := db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer colRows.Close()

	var headers []string
	for colRows.Next() {
		var cid int
		var name, dataType string
		var notNull, dfltValue, pk interface{}
		if err := colRows.Scan(&cid, &name, &dataType, &notNull, &dfltValue, &pk); err != nil {
			return nil, err
		}
		headers = append(headers, name)
	}
	if err := colRows.Err(); err != nil {
		return nil, err
	}

	// Get data
	dataQuery := fmt.Sprintf("SELECT * FROM `%s`", tableName) //nolint:gosec // Table name comes from database metadata
	dataRows, err := db.QueryContext(ctx, dataQuery)
	if err != nil {
		return nil, err
	}
	defer dataRows.Close()

	var records [][]string
	values := make([]interface{}, len(headers))
	scanArgs := make([]interface{}, len(headers))
	for i := range values {
		scanArgs[i] = &values[i]
	}

	for dataRows.Next() {
		if err := dataRows.Scan(scanArgs...); err != nil {
			return nil, err
		}
		record := make([]string, len(headers))
		for i, v := range values {
			if v == nil {
				record[i] = ""
			} else {
				record[i] = fmt.Sprintf("%v", v)
			}
		}
		records = append(records, record)
	}
	if err := dataRows.Err(); err != nil {
		return nil, err
	}

	// Infer column types (simplified - all TEXT for now)
	columnTypes := make([]parser.ColumnType, len(headers))
	for i := range columnTypes {
		columnTypes[i] = parser.TypeText
	}

	return &parser.TableData{
		Headers:     headers,
		Records:     records,
		ColumnTypes: columnTypes,
	}, nil
}
