//nolint:goconst // Repeated ACH column names mirror the file schema and stay clearer as literals.
package ach

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/moov-io/ach"
	"github.com/nao1215/filesql/parser"
)

// Addenda type constants for the addenda_type column
const (
	addendaType98Refused    = "98_refused"
	addendaType99Dishonored = "99_dishonored"
	addendaType99Contested  = "99_contested"
)

// TableSet contains multiple TableData representing different aspects of an ACH file.
// This structure preserves the hierarchical nature of ACH files while enabling
// flat table-based queries.
type TableSet struct {
	// fileHeader contains file-level header information (1 row per file)
	fileHeader *parser.TableData
	// batches contains batch header information
	batches *parser.TableData
	// entries contains entry detail records (the main transaction data)
	entries *parser.TableData
	// addenda contains addenda records associated with entries
	addenda *parser.TableData

	// iatBatches contains IAT batch header information (International ACH Transactions)
	iatBatches *parser.TableData
	// iatEntries contains IAT entry detail records
	iatEntries *parser.TableData
	// iatAddenda contains IAT addenda records (types 10-18, 98, 99)
	iatAddenda *parser.TableData

	// originalFile stores the original ACH file for reconstruction
	originalFile *ach.File

	// parsedCoords holds, per table, the coordinate each row was parsed with.
	// A write compares every row against it, which is what tells a row that
	// names its own record apart from one that names another's. See
	// coordinateTracker.
	parsedCoords map[string][]string
}

// fromFile converts an ACH file to a set of TableData structures.
// It is what ParseReader hands back, and the TableSet can be used with filesql
// for SQL queries.
//
// Tables created for standard batches:
//   - file_header: File header information (1 row)
//   - batches: Batch headers with control totals
//   - entries: Individual entry details (main transaction data)
//   - addenda: Addenda records linked to entries (types 02, 05, 98, 99)
//
// Tables created for IAT (International ACH Transactions):
//   - iat_batches: IAT batch headers
//   - iat_entries: IAT entry details
//   - iat_addenda: IAT addenda records (types 10-18, 98, 99)
//
// Note: The TableSet stores a reference to the original file (not a copy), and
// toFile creates a deep copy before applying TableData modifications.
func fromFile(file *ach.File) *TableSet {
	if file == nil {
		return nil
	}

	ts := &TableSet{
		originalFile: file,
	}

	ts.fileHeader = convertFileHeader(file)
	ts.batches = convertBatches(file)
	ts.entries = convertEntries(file)
	ts.addenda = convertAddenda(file)

	// Handle IAT batches if present
	if len(file.IATBatches) > 0 {
		ts.iatBatches = convertIATBatches(file)
		ts.iatEntries = convertIATEntries(file)
		ts.iatAddenda = convertIATAddenda(file)
	}

	ts.parsedCoords = map[string][]string{
		"batches":     coordinateKeys(ts.batches, "batch_index"),
		"entries":     coordinateKeys(ts.entries, "batch_index", "entry_index"),
		"addenda":     coordinateKeys(ts.addenda, "batch_index", "entry_index", "addenda_index"),
		"iat_batches": coordinateKeys(ts.iatBatches, "batch_index"),
		"iat_entries": coordinateKeys(ts.iatEntries, "batch_index", "entry_index"),
		"iat_addenda": coordinateKeys(ts.iatAddenda, "batch_index", "entry_index", "addenda_index"),
	}

	return ts
}

// coordinateKeys reads the coordinate of every row of td, in the spelling
// coordinateTracker compares against. A nil table has no rows and no
// coordinates.
func coordinateKeys(td *parser.TableData, columns ...string) []string {
	if td == nil {
		return nil
	}
	at := make([]int, 0, len(columns))
	for _, want := range columns {
		for i, h := range td.Headers {
			if h == want {
				at = append(at, i)
				break
			}
		}
	}
	if len(at) != len(columns) {
		return nil
	}
	keys := make([]string, 0, len(td.Records))
	for _, record := range td.Records {
		values := make([]string, len(at))
		for i, idx := range at {
			if idx < len(record) {
				values[i] = record[idx]
			}
		}
		keys = append(keys, coordinateKey(columns, values))
	}
	return keys
}

// coordinateKey spells one coordinate. Both sides of the comparison go through
// it and the callers hand it numbers rather than the text they read, so a
// coordinate is compared by the record it names and not by how it was spelled:
// "0" and "00" are one position, which is what they are. That is deliberate.
// These columns are integers, and a table that has been through SQLite comes
// back with its numbers spelled the way SQLite spells them, so comparing the
// text would refuse an honest round trip for having lost a leading zero.
func coordinateKey(columns []string, values []string) string {
	parts := make([]string, len(columns))
	for i, col := range columns {
		parts[i] = col + "=" + values[i]
	}
	return strings.Join(parts, " ")
}

// convertFileHeader extracts file header information into TableData.
func convertFileHeader(file *ach.File) *parser.TableData {
	headers := []string{
		"immediate_destination",
		"immediate_origin",
		"file_creation_date",
		"file_creation_time",
		"file_id_modifier",
		"immediate_destination_name",
		"immediate_origin_name",
		"reference_code",
	}

	record := []string{
		file.Header.ImmediateDestination,
		file.Header.ImmediateOrigin,
		file.Header.FileCreationDate,
		file.Header.FileCreationTime,
		file.Header.FileIDModifier,
		file.Header.ImmediateDestinationName,
		file.Header.ImmediateOriginName,
		file.Header.ReferenceCode,
	}

	columnTypes := []parser.ColumnType{
		parser.TypeText, // immediate_destination
		parser.TypeText, // immediate_origin
		parser.TypeText, // file_creation_date (YYMMDD format)
		parser.TypeText, // file_creation_time (HHmm format)
		parser.TypeText, // file_id_modifier
		parser.TypeText, // immediate_destination_name
		parser.TypeText, // immediate_origin_name
		parser.TypeText, // reference_code
	}

	return &parser.TableData{
		Headers:     headers,
		Records:     [][]string{record},
		ColumnTypes: columnTypes,
	}
}

// convertBatches extracts batch information into TableData.
func convertBatches(file *ach.File) *parser.TableData {
	headers := []string{
		"batch_index",
		"service_class_code",
		"company_name",
		"company_discretionary_data",
		"company_identification",
		"standard_entry_class_code",
		"company_entry_description",
		"company_descriptive_date",
		"effective_entry_date",
		"originator_status_code",
		"odfi_identification",
		"batch_number",
		// Control fields
		"entry_addenda_count",
		"entry_hash",
		"total_debit",
		"total_credit",
	}

	columnTypes := []parser.ColumnType{
		parser.TypeInteger, // batch_index
		parser.TypeInteger, // service_class_code
		parser.TypeText,    // company_name
		parser.TypeText,    // company_discretionary_data
		parser.TypeText,    // company_identification
		parser.TypeText,    // standard_entry_class_code
		parser.TypeText,    // company_entry_description
		parser.TypeText,    // company_descriptive_date
		parser.TypeText,    // effective_entry_date
		parser.TypeInteger, // originator_status_code
		parser.TypeText,    // odfi_identification
		parser.TypeInteger, // batch_number
		parser.TypeInteger, // entry_addenda_count
		parser.TypeInteger, // entry_hash
		parser.TypeInteger, // total_debit
		parser.TypeInteger, // total_credit
	}

	records := make([][]string, 0, len(file.Batches))
	for i, batch := range file.Batches {
		bh := batch.GetHeader()
		bc := batch.GetControl()

		record := []string{
			strconv.Itoa(i),
			strconv.Itoa(bh.ServiceClassCode),
			strings.TrimSpace(bh.CompanyName),
			strings.TrimSpace(bh.CompanyDiscretionaryData),
			strings.TrimSpace(bh.CompanyIdentification),
			bh.StandardEntryClassCode,
			strings.TrimSpace(bh.CompanyEntryDescription),
			strings.TrimSpace(bh.CompanyDescriptiveDate),
			bh.EffectiveEntryDate,
			strconv.Itoa(bh.OriginatorStatusCode),
			bh.ODFIIdentification,
			strconv.Itoa(bh.BatchNumber),
			strconv.Itoa(bc.EntryAddendaCount),
			strconv.Itoa(bc.EntryHash),
			strconv.Itoa(bc.TotalDebitEntryDollarAmount),
			strconv.Itoa(bc.TotalCreditEntryDollarAmount),
		}
		records = append(records, record)
	}

	// Handle empty batches
	if len(records) == 0 {
		records = [][]string{}
	}

	return &parser.TableData{
		Headers:     headers,
		Records:     records,
		ColumnTypes: columnTypes,
	}
}

// convertEntries extracts entry detail records into TableData.
func convertEntries(file *ach.File) *parser.TableData {
	headers := []string{
		"batch_index",
		"entry_index",
		"transaction_code",
		"rdfi_identification",
		"check_digit",
		"dfi_account_number",
		"amount",
		"identification_number",
		"individual_name",
		"discretionary_data",
		"addenda_record_indicator",
		"trace_number",
		"category",
	}

	columnTypes := []parser.ColumnType{
		parser.TypeInteger, // batch_index
		parser.TypeInteger, // entry_index
		parser.TypeInteger, // transaction_code
		parser.TypeText,    // rdfi_identification
		parser.TypeText,    // check_digit
		parser.TypeText,    // dfi_account_number
		parser.TypeInteger, // amount (in cents)
		parser.TypeText,    // identification_number
		parser.TypeText,    // individual_name
		parser.TypeText,    // discretionary_data
		parser.TypeInteger, // addenda_record_indicator
		parser.TypeText,    // trace_number
		parser.TypeText,    // category
	}

	totalEntries := 0
	for _, batch := range file.Batches {
		totalEntries += len(batch.GetEntries())
	}
	records := make([][]string, 0, totalEntries)
	for batchIdx, batch := range file.Batches {
		for entryIdx, entry := range batch.GetEntries() {
			record := []string{
				strconv.Itoa(batchIdx),
				strconv.Itoa(entryIdx),
				strconv.Itoa(entry.TransactionCode),
				entry.RDFIIdentification,
				entry.CheckDigit,
				strings.TrimSpace(entry.DFIAccountNumber),
				strconv.Itoa(entry.Amount),
				strings.TrimSpace(entry.IdentificationNumber),
				strings.TrimSpace(entry.IndividualName),
				strings.TrimSpace(entry.DiscretionaryData),
				strconv.Itoa(entry.AddendaRecordIndicator),
				entry.TraceNumber,
				entry.Category,
			}
			records = append(records, record)
		}
	}

	// Handle empty entries
	if len(records) == 0 {
		records = [][]string{}
	}

	return &parser.TableData{
		Headers:     headers,
		Records:     records,
		ColumnTypes: columnTypes,
	}
}

// convertAddenda extracts addenda records into TableData.
// Handles multiple addenda types: Addenda02, Addenda05, Addenda98, Addenda98Refused,
// Addenda99, Addenda99Dishonored, Addenda99Contested.
func convertAddenda(file *ach.File) *parser.TableData {
	headers := []string{
		"batch_index",
		"entry_index",
		"addenda_index",
		"addenda_type", // "02", "05", "98", "98_refused", "99", "99_dishonored", "99_contested"
		"type_code",    // The actual type code from the record
		"payment_related_information",
		"sequence_number",
		"entry_detail_sequence_number",
		// Additional fields for specific addenda types
		"original_trace",            // Addenda98/99
		"original_rdfi",             // Addenda98/99
		"corrected_data",            // Addenda98/98Refused
		"change_code",               // Addenda98/98Refused
		"return_code",               // Addenda99
		"addenda_information",       // Addenda99/99Dishonored
		"trace_number",              // Addenda02/98/99
		"reference_information_one", // Addenda02
		"reference_information_two", // Addenda02
		"terminal_identification",   // Addenda02
		"transaction_serial",        // Addenda02
		"transaction_date",          // Addenda02
		"authorization_code",        // Addenda02
		"terminal_location",         // Addenda02
		"terminal_city",             // Addenda02
		"terminal_state",            // Addenda02
		// Addenda98Refused specific fields
		"refused_change_code",   // Addenda98Refused
		"trace_sequence_number", // Addenda98Refused
		// Addenda99Dishonored specific fields
		"dishonored_return_reason_code",         // Addenda99Dishonored
		"original_entry_trace_number",           // Addenda99Dishonored/Contested
		"original_receiving_dfi_identification", // Addenda99Dishonored/Contested
		"return_trace_number",                   // Addenda99Dishonored/Contested
		"return_settlement_date",                // Addenda99Dishonored/Contested
		"return_reason_code",                    // Addenda99Dishonored/Contested
		// Addenda99Contested specific fields
		"contested_return_code",                       // Addenda99Contested
		"date_original_entry_returned",                // Addenda99Contested
		"original_settlement_date",                    // Addenda99Contested
		"contested_dishonored_return_trace_number",    // Addenda99Contested
		"contested_dishonored_return_settlement_date", // Addenda99Contested
		"contested_dishonored_return_reason_code",     // Addenda99Contested
	}

	columnTypes := []parser.ColumnType{
		parser.TypeInteger, // batch_index
		parser.TypeInteger, // entry_index
		parser.TypeInteger, // addenda_index
		parser.TypeText,    // addenda_type
		parser.TypeText,    // type_code
		parser.TypeText,    // payment_related_information
		parser.TypeInteger, // sequence_number
		parser.TypeInteger, // entry_detail_sequence_number
		parser.TypeText,    // original_trace
		parser.TypeText,    // original_rdfi
		parser.TypeText,    // corrected_data
		parser.TypeText,    // change_code
		parser.TypeText,    // return_code
		parser.TypeText,    // addenda_information
		parser.TypeText,    // trace_number
		parser.TypeText,    // reference_information_one
		parser.TypeText,    // reference_information_two
		parser.TypeText,    // terminal_identification
		parser.TypeText,    // transaction_serial
		parser.TypeText,    // transaction_date
		parser.TypeText,    // authorization_code
		parser.TypeText,    // terminal_location
		parser.TypeText,    // terminal_city
		parser.TypeText,    // terminal_state
		parser.TypeText,    // refused_change_code
		parser.TypeText,    // trace_sequence_number
		parser.TypeText,    // dishonored_return_reason_code
		parser.TypeText,    // original_entry_trace_number
		parser.TypeText,    // original_receiving_dfi_identification
		parser.TypeText,    // return_trace_number
		parser.TypeText,    // return_settlement_date
		parser.TypeText,    // return_reason_code
		parser.TypeText,    // contested_return_code
		parser.TypeText,    // date_original_entry_returned
		parser.TypeText,    // original_settlement_date
		parser.TypeText,    // contested_dishonored_return_trace_number
		parser.TypeText,    // contested_dishonored_return_settlement_date
		parser.TypeText,    // contested_dishonored_return_reason_code
	}

	var records [][]string
	for batchIdx, batch := range file.Batches {
		for entryIdx, entry := range batch.GetEntries() {
			addendaIdx := 0

			// Handle Addenda02 (MTE, POS, SHR)
			if entry.Addenda02 != nil {
				record := make([]string, len(headers))
				record[0] = strconv.Itoa(batchIdx)
				record[1] = strconv.Itoa(entryIdx)
				record[2] = strconv.Itoa(addendaIdx)
				record[3] = "02"
				record[4] = entry.Addenda02.TypeCode
				record[5] = ""  // payment_related_information (not used)
				record[6] = "0" // sequence_number (not used)
				record[7] = "0" // entry_detail_sequence_number (not used)
				record[8] = ""  // original_trace
				record[9] = ""  // original_rdfi
				record[10] = "" // corrected_data
				record[11] = "" // change_code
				record[12] = "" // return_code
				record[13] = "" // addenda_information
				record[14] = entry.Addenda02.TraceNumber
				record[15] = strings.TrimSpace(entry.Addenda02.ReferenceInformationOne)
				record[16] = strings.TrimSpace(entry.Addenda02.ReferenceInformationTwo)
				record[17] = strings.TrimSpace(entry.Addenda02.TerminalIdentificationCode)
				record[18] = strings.TrimSpace(entry.Addenda02.TransactionSerialNumber)
				record[19] = strings.TrimSpace(entry.Addenda02.TransactionDate)
				record[20] = strings.TrimSpace(entry.Addenda02.AuthorizationCodeOrExpireDate)
				record[21] = strings.TrimSpace(entry.Addenda02.TerminalLocation)
				record[22] = strings.TrimSpace(entry.Addenda02.TerminalCity)
				record[23] = strings.TrimSpace(entry.Addenda02.TerminalState)
				records = append(records, record)
				addendaIdx++
			}

			// Handle Addenda05 records (most common - PPD, CCD, CTX, etc.)
			for _, addenda := range entry.Addenda05 {
				if addenda == nil {
					continue
				}
				record := make([]string, len(headers))
				record[0] = strconv.Itoa(batchIdx)
				record[1] = strconv.Itoa(entryIdx)
				record[2] = strconv.Itoa(addendaIdx)
				record[3] = "05"
				record[4] = addenda.TypeCode
				record[5] = strings.TrimSpace(addenda.PaymentRelatedInformation)
				record[6] = strconv.Itoa(addenda.SequenceNumber)
				record[7] = strconv.Itoa(addenda.EntryDetailSequenceNumber)
				// Other fields are empty for Addenda05
				for i := 8; i < len(headers); i++ {
					record[i] = ""
				}
				records = append(records, record)
				addendaIdx++
			}

			// Handle Addenda98 (Notification of Change)
			if entry.Addenda98 != nil {
				record := make([]string, len(headers))
				record[0] = strconv.Itoa(batchIdx)
				record[1] = strconv.Itoa(entryIdx)
				record[2] = strconv.Itoa(addendaIdx)
				record[3] = "98"
				record[4] = entry.Addenda98.TypeCode
				record[5] = "" // payment_related_information
				record[6] = "0"
				record[7] = "0"
				record[8] = entry.Addenda98.OriginalTrace
				record[9] = entry.Addenda98.OriginalDFI
				record[10] = strings.TrimSpace(entry.Addenda98.CorrectedData)
				record[11] = entry.Addenda98.ChangeCode
				record[12] = "" // return_code
				record[13] = "" // addenda_information
				record[14] = entry.Addenda98.TraceNumber
				for i := 15; i < len(headers); i++ {
					record[i] = ""
				}
				records = append(records, record)
				addendaIdx++
			}

			// Handle Addenda99 (Returns)
			if entry.Addenda99 != nil {
				record := make([]string, len(headers))
				record[0] = strconv.Itoa(batchIdx)
				record[1] = strconv.Itoa(entryIdx)
				record[2] = strconv.Itoa(addendaIdx)
				record[3] = "99"
				record[4] = entry.Addenda99.TypeCode
				record[5] = "" // payment_related_information
				record[6] = "0"
				record[7] = "0"
				record[8] = entry.Addenda99.OriginalTrace
				record[9] = entry.Addenda99.OriginalDFI
				record[10] = "" // corrected_data
				record[11] = "" // change_code
				record[12] = entry.Addenda99.ReturnCode
				record[13] = strings.TrimSpace(entry.Addenda99.AddendaInformation)
				record[14] = entry.Addenda99.TraceNumber
				for i := 15; i < len(headers); i++ {
					record[i] = ""
				}
				records = append(records, record)
				addendaIdx++
			}

			// Handle Addenda98Refused (Refused Notification of Change)
			if entry.Addenda98Refused != nil {
				record := make([]string, len(headers))
				record[0] = strconv.Itoa(batchIdx)
				record[1] = strconv.Itoa(entryIdx)
				record[2] = strconv.Itoa(addendaIdx)
				record[3] = addendaType98Refused
				record[4] = entry.Addenda98Refused.TypeCode
				record[5] = "" // payment_related_information
				record[6] = "0"
				record[7] = "0"
				record[8] = entry.Addenda98Refused.OriginalTrace
				record[9] = entry.Addenda98Refused.OriginalDFI
				record[10] = strings.TrimSpace(entry.Addenda98Refused.CorrectedData)
				record[11] = entry.Addenda98Refused.ChangeCode
				record[12] = "" // return_code
				record[13] = "" // addenda_information
				record[14] = entry.Addenda98Refused.TraceNumber
				for i := 15; i < 24; i++ {
					record[i] = ""
				}
				// Addenda98Refused specific fields
				record[24] = entry.Addenda98Refused.RefusedChangeCode
				record[25] = entry.Addenda98Refused.TraceSequenceNumber
				for i := 26; i < len(headers); i++ {
					record[i] = ""
				}
				records = append(records, record)
				addendaIdx++
			}

			// Handle Addenda99Dishonored (Dishonored Returns)
			if entry.Addenda99Dishonored != nil {
				record := make([]string, len(headers))
				record[0] = strconv.Itoa(batchIdx)
				record[1] = strconv.Itoa(entryIdx)
				record[2] = strconv.Itoa(addendaIdx)
				record[3] = addendaType99Dishonored
				record[4] = entry.Addenda99Dishonored.TypeCode
				record[5] = "" // payment_related_information
				record[6] = "0"
				record[7] = "0"
				record[8] = ""  // original_trace (use specific fields below)
				record[9] = ""  // original_rdfi (use specific fields below)
				record[10] = "" // corrected_data
				record[11] = "" // change_code
				record[12] = "" // return_code (use return_reason_code below)
				record[13] = strings.TrimSpace(entry.Addenda99Dishonored.AddendaInformation)
				record[14] = entry.Addenda99Dishonored.TraceNumber
				for i := 15; i < 26; i++ {
					record[i] = ""
				}
				// Addenda99Dishonored specific fields
				record[26] = entry.Addenda99Dishonored.DishonoredReturnReasonCode
				record[27] = entry.Addenda99Dishonored.OriginalEntryTraceNumber
				record[28] = entry.Addenda99Dishonored.OriginalReceivingDFIIdentification
				record[29] = entry.Addenda99Dishonored.ReturnTraceNumber
				record[30] = entry.Addenda99Dishonored.ReturnSettlementDate
				record[31] = entry.Addenda99Dishonored.ReturnReasonCode
				for i := 32; i < len(headers); i++ {
					record[i] = ""
				}
				records = append(records, record)
				addendaIdx++
			}

			// Handle Addenda99Contested (Contested Dishonored Returns)
			if entry.Addenda99Contested != nil {
				record := make([]string, len(headers))
				record[0] = strconv.Itoa(batchIdx)
				record[1] = strconv.Itoa(entryIdx)
				record[2] = strconv.Itoa(addendaIdx)
				record[3] = addendaType99Contested
				record[4] = entry.Addenda99Contested.TypeCode
				record[5] = "" // payment_related_information
				record[6] = "0"
				record[7] = "0"
				record[8] = ""  // original_trace (use specific fields below)
				record[9] = ""  // original_rdfi (use specific fields below)
				record[10] = "" // corrected_data
				record[11] = "" // change_code
				record[12] = "" // return_code (use return_reason_code below)
				record[13] = "" // addenda_information
				record[14] = entry.Addenda99Contested.TraceNumber
				for i := 15; i < 26; i++ {
					record[i] = ""
				}
				// Addenda99Contested uses some dishonored fields
				record[27] = entry.Addenda99Contested.OriginalEntryTraceNumber
				record[28] = entry.Addenda99Contested.OriginalReceivingDFIIdentification
				record[29] = entry.Addenda99Contested.ReturnTraceNumber
				record[30] = entry.Addenda99Contested.ReturnSettlementDate
				record[31] = entry.Addenda99Contested.ReturnReasonCode
				// Addenda99Contested specific fields
				record[32] = entry.Addenda99Contested.ContestedReturnCode
				record[33] = entry.Addenda99Contested.DateOriginalEntryReturned
				record[34] = entry.Addenda99Contested.OriginalSettlementDate
				record[35] = entry.Addenda99Contested.DishonoredReturnTraceNumber
				record[36] = entry.Addenda99Contested.DishonoredReturnSettlementDate
				record[37] = entry.Addenda99Contested.DishonoredReturnReasonCode
				records = append(records, record)
				// Note: addendaIdx increment omitted as this is the last addenda type processed
			}
		}
	}

	// Handle empty addenda
	if len(records) == 0 {
		records = [][]string{}
	}

	return &parser.TableData{
		Headers:     headers,
		Records:     records,
		ColumnTypes: columnTypes,
	}
}

// toFile reconstructs an ACH file from modified TableData.
// This allows round-trip editing: ACH -> TableData -> SQL modifications -> ACH
//
// The function creates a deep copy of the original ACH file and applies
// modifications from all TableData (FileHeader, Batches, Entries, Addenda).
//
// After modification, the file's control records are automatically recalculated.
func (ts *TableSet) toFile() (*ach.File, error) {
	if ts == nil || ts.originalFile == nil {
		return nil, errors.New("no original ACH file available")
	}

	// Create a true deep copy of the original file to avoid modifying it. The
	// copy goes through the library's own JSON round-trip, whose MarshalJSON
	// carries the file's validation settings and whose UnmarshalJSON restores
	// them, so the copy validates the way the original was read.
	encoded, err := json.Marshal(ts.originalFile)
	if err != nil {
		return nil, fmt.Errorf("failed to deep copy ACH file: %w", err)
	}
	var newFile ach.File
	if err := json.Unmarshal(encoded, &newFile); err != nil {
		return nil, fmt.Errorf("failed to deep copy ACH file: %w", err)
	}

	// Apply modifications from FileHeader TableData
	if ts.fileHeader != nil && len(ts.fileHeader.Records) > 0 {
		ts.applyFileHeaderModifications(&newFile)
	}

	// Apply modifications from Batches TableData
	if ts.batches != nil && len(ts.batches.Records) > 0 {
		if err := ts.applyBatchModifications(&newFile); err != nil {
			return nil, fmt.Errorf("failed to apply batch modifications: %w", err)
		}
	}

	// Apply modifications from Entries TableData
	if ts.entries != nil && len(ts.entries.Records) > 0 {
		if err := ts.applyEntryModifications(&newFile); err != nil {
			return nil, fmt.Errorf("failed to apply entry modifications: %w", err)
		}
	}

	// Apply modifications from Addenda TableData
	if ts.addenda != nil && len(ts.addenda.Records) > 0 {
		if err := ts.applyAddendaModifications(&newFile); err != nil {
			return nil, fmt.Errorf("failed to apply addenda modifications: %w", err)
		}
	}

	// Apply modifications from IATBatches TableData
	if ts.iatBatches != nil && len(ts.iatBatches.Records) > 0 {
		if err := ts.applyIATBatchModifications(&newFile); err != nil {
			return nil, fmt.Errorf("failed to apply IAT batch modifications: %w", err)
		}
	}

	// Apply modifications from IATEntries TableData
	if ts.iatEntries != nil && len(ts.iatEntries.Records) > 0 {
		if err := ts.applyIATEntryModifications(&newFile); err != nil {
			return nil, fmt.Errorf("failed to apply IAT entry modifications: %w", err)
		}
	}

	// Apply modifications from IATAddenda TableData
	if ts.iatAddenda != nil && len(ts.iatAddenda.Records) > 0 {
		if err := ts.applyIATAddendaModifications(&newFile); err != nil {
			return nil, fmt.Errorf("failed to apply IAT addenda modifications: %w", err)
		}
	}

	// Every edit is in place now, so this is where a value too wide for the
	// record it goes into can be caught — before anything is written.
	if err := validateFieldWidths(&newFile); err != nil {
		return nil, err
	}

	// Recalculate control records. Each batch first, then the file.
	//
	// File.Create builds the file control from the batch controls and does not
	// touch the batch controls themselves — that is Batch.Create's job. Without
	// this loop each batch kept the control the deep copy carried over from the
	// original file, so an edited entry amount was checked against the total the
	// source was written with and every write failed as out-of-balance. The
	// controls are derived values, so recalculating them is what makes an edit
	// writable at all.
	for i, batch := range newFile.Batches {
		if err := batch.Create(); err != nil {
			return nil, fmt.Errorf("failed to create batch control for batch %d: %w", i+1, err)
		}
	}
	for i := range newFile.IATBatches {
		if err := newFile.IATBatches[i].Create(); err != nil {
			return nil, fmt.Errorf("failed to create IAT batch control for batch %d: %w", i+1, err)
		}
	}
	if err := newFile.Create(); err != nil {
		return nil, fmt.Errorf("failed to create file control: %w", err)
	}

	return &newFile, nil
}

// validateFieldWidths reports every value that would not survive being written
// into its fixed-width record.
//
// An ACH record is a fixed-width line, and the writer's field formatters cut a
// value that does not fit instead of refusing it. That made the two halves of
// the same edit behave differently: an amount past its field fails the write
// with a FieldError, while a name past its field was quietly shortened and
// written, so the file on disk held data the session never asked for.
//
// No width is written down here. Each formatter returns exactly the number of
// characters the record holds — padding a short value, cutting a long one — so
// its length is the specification's own answer, and it stays right when the
// library's does. Only fields a caller can edit through a table are checked;
// the control records are derived and recalculated below.
func validateFieldWidths(file *ach.File) error {
	var errs []error
	check := func(column, value, formatted string) {
		width := utf8.RuneCountInString(formatted)
		got := utf8.RuneCountInString(value)
		if got <= width {
			return
		}
		errs = append(errs, fmt.Errorf(
			"%s is %d characters, but the ACH record holds %d: %q would be written as %q",
			column, got, width, value, formatted))
	}

	header := file.Header
	check("file_header.immediate_destination", header.ImmediateDestination, header.ImmediateDestinationField())
	check("file_header.immediate_origin", header.ImmediateOrigin, header.ImmediateOriginField())
	check("file_header.immediate_destination_name", header.ImmediateDestinationName, header.ImmediateDestinationNameField())
	check("file_header.immediate_origin_name", header.ImmediateOriginName, header.ImmediateOriginNameField())
	check("file_header.reference_code", header.ReferenceCode, header.ReferenceCodeField())

	for batchIdx, batch := range file.Batches {
		bh := batch.GetHeader()
		prefix := fmt.Sprintf("batches[%d].", batchIdx)
		check(prefix+"company_name", bh.CompanyName, bh.CompanyNameField())
		check(prefix+"company_discretionary_data", bh.CompanyDiscretionaryData, bh.CompanyDiscretionaryDataField())
		check(prefix+"company_identification", bh.CompanyIdentification, bh.CompanyIdentificationField())
		check(prefix+"company_entry_description", bh.CompanyEntryDescription, bh.CompanyEntryDescriptionField())
		check(prefix+"company_descriptive_date", bh.CompanyDescriptiveDate, bh.CompanyDescriptiveDateField())
		check(prefix+"effective_entry_date", bh.EffectiveEntryDate, bh.EffectiveEntryDateField())
		check(prefix+"odfi_identification", bh.ODFIIdentification, bh.ODFIIdentificationField())

		for entryIdx, entry := range batch.GetEntries() {
			prefix := fmt.Sprintf("entries[%d][%d].", batchIdx, entryIdx)
			check(prefix+"rdfi_identification", entry.RDFIIdentification, entry.RDFIIdentificationField())
			check(prefix+"dfi_account_number", entry.DFIAccountNumber, entry.DFIAccountNumberField())
			check(prefix+"identification_number", entry.IdentificationNumber, entry.IdentificationNumberField())
			check(prefix+"individual_name", entry.IndividualName, entry.IndividualNameField())
			check(prefix+"discretionary_data", entry.DiscretionaryData, entry.DiscretionaryDataField())
			check(prefix+"trace_number", entry.TraceNumber, entry.TraceNumberField())
		}
	}

	return errors.Join(errs...)
}

// applyEntryModifications updates entries in the ACH file from TableData.
// coordinateTracker refuses a record two modification rows both name.
//
// batch_index, entry_index and addenda_index say which record a row's other
// columns belong to. They are not data a caller may edit, and a row that names
// a record another row already named used to be applied anyway: the update
// landed on the other record, overwriting its account number, amount and trace
// number, while the row that was edited came back unchanged. The table then
// read as if the edit had been ignored, and a different entry had silently
// become a copy of this one -- the worst way for a payment file to be wrong.
//
// Whether a caller was told depended only on whether the new coordinate
// happened to fall outside the file, and the tables disagreed about even that:
// entries and batches reported an out-of-range coordinate while addenda and
// every IAT table skipped the row in silence. Both halves are errors now.
type coordinateTracker struct {
	table  string
	parsed []string
	row    int
	seen   map[string]bool
}

func (ts *TableSet) newCoordinateTracker(table string) *coordinateTracker {
	return &coordinateTracker{
		table:  table,
		parsed: ts.parsedCoords[table],
		seen:   make(map[string]bool),
	}
}

// claim checks the coordinate the next row names and advances to the one after
// it. A row must name the record it was parsed from: comparing against the
// parsed coordinate rather than only against the coordinates other rows have
// claimed is what catches two rows that exchange positions, where every
// coordinate is still unique, still in range, and still points at the wrong
// record.
func (c *coordinateTracker) claim(columns []string, values []int) error {
	row := c.row
	c.row++

	strs := make([]string, len(columns))
	for i, v := range values {
		strs[i] = strconv.Itoa(v)
	}
	key := coordinateKey(columns, strs)
	named := strings.Join(columns, " and ")

	if c.seen[key] {
		return fmt.Errorf(
			"%s: two rows name the same record (%s); %s give the position of the record a row updates, not a value it stores",
			c.table, key, named)
	}
	c.seen[key] = true

	// A TableSet built by hand rather than parsed has nothing to compare
	// against; the duplicate check above still applies to it.
	if c.parsed == nil {
		return nil
	}
	if row >= len(c.parsed) {
		return fmt.Errorf(
			"%s: row %d was not in the file this table was read from; rows cannot be added by a write",
			c.table, row)
	}
	if c.parsed[row] != key {
		return fmt.Errorf(
			"%s: row %d names %s but was read from %s; %s give the position of the record a row updates, not a value it stores",
			c.table, row, key, c.parsed[row], named)
	}
	return nil
}

func (ts *TableSet) applyEntryModifications(file *ach.File) error {
	// Build index mapping for quick lookup
	headerIndex := make(map[string]int)
	for i, h := range ts.entries.Headers {
		headerIndex[h] = i
	}

	coords := ts.newCoordinateTracker("entries")
	for _, record := range ts.entries.Records {
		batchIdx, err := strconv.Atoi(record[headerIndex["batch_index"]])
		if err != nil {
			return fmt.Errorf("invalid batch_index: %w", err)
		}
		entryIdx, err := strconv.Atoi(record[headerIndex["entry_index"]])
		if err != nil {
			return fmt.Errorf("invalid entry_index: %w", err)
		}

		if batchIdx < 0 || batchIdx >= len(file.Batches) {
			return fmt.Errorf("batch_index %d out of range", batchIdx)
		}

		entries := file.Batches[batchIdx].GetEntries()
		if entryIdx < 0 || entryIdx >= len(entries) {
			return fmt.Errorf("entry_index %d out of range for batch %d", entryIdx, batchIdx)
		}

		if err := coords.claim([]string{"batch_index", "entry_index"}, []int{batchIdx, entryIdx}); err != nil {
			return err
		}

		entry := entries[entryIdx]

		// Update modifiable fields
		if idx, ok := headerIndex["transaction_code"]; ok && idx < len(record) {
			if v, err := strconv.Atoi(record[idx]); err == nil {
				entry.TransactionCode = v
			}
		}
		if idx, ok := headerIndex["rdfi_identification"]; ok && idx < len(record) {
			entry.RDFIIdentification = record[idx]
		}
		if idx, ok := headerIndex["check_digit"]; ok && idx < len(record) {
			entry.CheckDigit = record[idx]
		}
		if idx, ok := headerIndex["dfi_account_number"]; ok && idx < len(record) {
			entry.DFIAccountNumber = record[idx]
		}
		if idx, ok := headerIndex["amount"]; ok && idx < len(record) {
			if v, err := strconv.Atoi(record[idx]); err == nil {
				entry.Amount = v
			}
		}
		if idx, ok := headerIndex["identification_number"]; ok && idx < len(record) {
			entry.IdentificationNumber = record[idx]
		}
		if idx, ok := headerIndex["individual_name"]; ok && idx < len(record) {
			entry.IndividualName = record[idx]
		}
		if idx, ok := headerIndex["discretionary_data"]; ok && idx < len(record) {
			entry.DiscretionaryData = record[idx]
		}
		if idx, ok := headerIndex["addenda_record_indicator"]; ok && idx < len(record) {
			if v, err := strconv.Atoi(record[idx]); err == nil {
				entry.AddendaRecordIndicator = v
			}
		}
		if idx, ok := headerIndex["trace_number"]; ok && idx < len(record) {
			entry.TraceNumber = record[idx]
		}
		if idx, ok := headerIndex["category"]; ok && idx < len(record) {
			entry.Category = record[idx]
		}
	}

	return nil
}

// applyFileHeaderModifications updates file header fields from TableData.
func (ts *TableSet) applyFileHeaderModifications(file *ach.File) {
	if len(ts.fileHeader.Records) == 0 {
		return
	}

	headerIndex := make(map[string]int)
	for i, h := range ts.fileHeader.Headers {
		headerIndex[h] = i
	}

	record := ts.fileHeader.Records[0]

	if idx, ok := headerIndex["immediate_destination"]; ok && idx < len(record) {
		file.Header.ImmediateDestination = record[idx]
	}
	if idx, ok := headerIndex["immediate_origin"]; ok && idx < len(record) {
		file.Header.ImmediateOrigin = record[idx]
	}
	if idx, ok := headerIndex["file_creation_date"]; ok && idx < len(record) {
		file.Header.FileCreationDate = record[idx]
	}
	if idx, ok := headerIndex["file_creation_time"]; ok && idx < len(record) {
		file.Header.FileCreationTime = record[idx]
	}
	if idx, ok := headerIndex["file_id_modifier"]; ok && idx < len(record) {
		file.Header.FileIDModifier = record[idx]
	}
	if idx, ok := headerIndex["immediate_destination_name"]; ok && idx < len(record) {
		file.Header.ImmediateDestinationName = record[idx]
	}
	if idx, ok := headerIndex["immediate_origin_name"]; ok && idx < len(record) {
		file.Header.ImmediateOriginName = record[idx]
	}
	if idx, ok := headerIndex["reference_code"]; ok && idx < len(record) {
		file.Header.ReferenceCode = record[idx]
	}
}

// applyBatchModifications updates batch header fields from TableData.
func (ts *TableSet) applyBatchModifications(file *ach.File) error {
	headerIndex := make(map[string]int)
	for i, h := range ts.batches.Headers {
		headerIndex[h] = i
	}

	coords := ts.newCoordinateTracker("batches")
	for _, record := range ts.batches.Records {
		batchIdxVal, ok := headerIndex["batch_index"]
		if !ok {
			continue
		}
		batchIdx, err := strconv.Atoi(record[batchIdxVal])
		if err != nil {
			return fmt.Errorf("invalid batch_index: %w", err)
		}

		if batchIdx < 0 || batchIdx >= len(file.Batches) {
			return fmt.Errorf("batch_index %d out of range", batchIdx)
		}
		if err := coords.claim([]string{"batch_index"}, []int{batchIdx}); err != nil {
			return err
		}

		bh := file.Batches[batchIdx].GetHeader()

		if idx, ok := headerIndex["service_class_code"]; ok && idx < len(record) {
			if v, err := strconv.Atoi(record[idx]); err == nil {
				bh.ServiceClassCode = v
			}
		}
		if idx, ok := headerIndex["company_name"]; ok && idx < len(record) {
			bh.CompanyName = record[idx]
		}
		if idx, ok := headerIndex["company_discretionary_data"]; ok && idx < len(record) {
			bh.CompanyDiscretionaryData = record[idx]
		}
		if idx, ok := headerIndex["company_identification"]; ok && idx < len(record) {
			bh.CompanyIdentification = record[idx]
		}
		if idx, ok := headerIndex["standard_entry_class_code"]; ok && idx < len(record) {
			bh.StandardEntryClassCode = record[idx]
		}
		if idx, ok := headerIndex["company_entry_description"]; ok && idx < len(record) {
			bh.CompanyEntryDescription = record[idx]
		}
		if idx, ok := headerIndex["company_descriptive_date"]; ok && idx < len(record) {
			bh.CompanyDescriptiveDate = record[idx]
		}
		if idx, ok := headerIndex["effective_entry_date"]; ok && idx < len(record) {
			bh.EffectiveEntryDate = record[idx]
		}
		if idx, ok := headerIndex["originator_status_code"]; ok && idx < len(record) {
			if v, err := strconv.Atoi(record[idx]); err == nil {
				bh.OriginatorStatusCode = v
			}
		}
		if idx, ok := headerIndex["odfi_identification"]; ok && idx < len(record) {
			bh.ODFIIdentification = record[idx]
		}
		if idx, ok := headerIndex["batch_number"]; ok && idx < len(record) {
			if v, err := strconv.Atoi(record[idx]); err == nil {
				bh.BatchNumber = v
			}
		}
	}

	return nil
}

// applyAddendaModifications updates addenda records from TableData.
func (ts *TableSet) applyAddendaModifications(file *ach.File) error {
	headerIndex := make(map[string]int)
	for i, h := range ts.addenda.Headers {
		headerIndex[h] = i
	}

	coords := ts.newCoordinateTracker("addenda")
	for _, record := range ts.addenda.Records {
		batchIdx, err := strconv.Atoi(record[headerIndex["batch_index"]])
		if err != nil {
			return fmt.Errorf("invalid batch_index: %w", err)
		}
		entryIdx, err := strconv.Atoi(record[headerIndex["entry_index"]])
		if err != nil {
			return fmt.Errorf("invalid entry_index: %w", err)
		}
		addendaIdx, err := strconv.Atoi(record[headerIndex["addenda_index"]])
		if err != nil {
			return fmt.Errorf("invalid addenda_index: %w", err)
		}

		if batchIdx < 0 || batchIdx >= len(file.Batches) {
			return fmt.Errorf("batch_index %d out of range", batchIdx)
		}

		entries := file.Batches[batchIdx].GetEntries()
		if entryIdx < 0 || entryIdx >= len(entries) {
			return fmt.Errorf("entry_index %d out of range for batch %d", entryIdx, batchIdx)
		}
		if err := coords.claim(
			[]string{"batch_index", "entry_index", "addenda_index"},
			[]int{batchIdx, entryIdx, addendaIdx}); err != nil {
			return err
		}

		entry := entries[entryIdx]
		addendaType := ""
		if idx, ok := headerIndex["addenda_type"]; ok && idx < len(record) {
			addendaType = record[idx]
		}

		switch addendaType {
		case "02":
			if entry.Addenda02 != nil {
				ts.applyAddenda02Modifications(entry.Addenda02, record, headerIndex)
			}
		case "05":
			addenda05Idx := addendaIdx
			if entry.Addenda02 != nil {
				addenda05Idx--
			}
			if addenda05Idx >= 0 && addenda05Idx < len(entry.Addenda05) && entry.Addenda05[addenda05Idx] != nil {
				ts.applyAddenda05Modifications(entry.Addenda05[addenda05Idx], record, headerIndex)
			}
		case "98":
			if entry.Addenda98 != nil {
				ts.applyAddenda98Modifications(entry.Addenda98, record, headerIndex)
			}
		case addendaType98Refused:
			if entry.Addenda98Refused != nil {
				ts.applyAddenda98RefusedModifications(entry.Addenda98Refused, record, headerIndex)
			}
		case "99":
			if entry.Addenda99 != nil {
				ts.applyAddenda99Modifications(entry.Addenda99, record, headerIndex)
			}
		case addendaType99Dishonored:
			if entry.Addenda99Dishonored != nil {
				ts.applyAddenda99DishonoredModifications(entry.Addenda99Dishonored, record, headerIndex)
			}
		case addendaType99Contested:
			if entry.Addenda99Contested != nil {
				ts.applyAddenda99ContestedModifications(entry.Addenda99Contested, record, headerIndex)
			}
		}
	}

	return nil
}

// applyAddenda02Modifications applies modifications to Addenda02.
func (ts *TableSet) applyAddenda02Modifications(addenda *ach.Addenda02, record []string, headerIndex map[string]int) {
	if idx, ok := headerIndex["reference_information_one"]; ok && idx < len(record) {
		addenda.ReferenceInformationOne = record[idx]
	}
	if idx, ok := headerIndex["reference_information_two"]; ok && idx < len(record) {
		addenda.ReferenceInformationTwo = record[idx]
	}
	if idx, ok := headerIndex["terminal_identification"]; ok && idx < len(record) {
		addenda.TerminalIdentificationCode = record[idx]
	}
	if idx, ok := headerIndex["transaction_serial"]; ok && idx < len(record) {
		addenda.TransactionSerialNumber = record[idx]
	}
	if idx, ok := headerIndex["transaction_date"]; ok && idx < len(record) {
		addenda.TransactionDate = record[idx]
	}
	if idx, ok := headerIndex["authorization_code"]; ok && idx < len(record) {
		addenda.AuthorizationCodeOrExpireDate = record[idx]
	}
	if idx, ok := headerIndex["terminal_location"]; ok && idx < len(record) {
		addenda.TerminalLocation = record[idx]
	}
	if idx, ok := headerIndex["terminal_city"]; ok && idx < len(record) {
		addenda.TerminalCity = record[idx]
	}
	if idx, ok := headerIndex["terminal_state"]; ok && idx < len(record) {
		addenda.TerminalState = record[idx]
	}
}

// applyAddenda05Modifications applies modifications to Addenda05.
func (ts *TableSet) applyAddenda05Modifications(addenda *ach.Addenda05, record []string, headerIndex map[string]int) {
	if idx, ok := headerIndex["payment_related_information"]; ok && idx < len(record) {
		addenda.PaymentRelatedInformation = record[idx]
	}
	if idx, ok := headerIndex["sequence_number"]; ok && idx < len(record) {
		if v, err := strconv.Atoi(record[idx]); err == nil {
			addenda.SequenceNumber = v
		}
	}
	if idx, ok := headerIndex["entry_detail_sequence_number"]; ok && idx < len(record) {
		if v, err := strconv.Atoi(record[idx]); err == nil {
			addenda.EntryDetailSequenceNumber = v
		}
	}
}

// applyAddenda98Modifications applies modifications to Addenda98.
func (ts *TableSet) applyAddenda98Modifications(addenda *ach.Addenda98, record []string, headerIndex map[string]int) {
	if idx, ok := headerIndex["change_code"]; ok && idx < len(record) {
		addenda.ChangeCode = record[idx]
	}
	if idx, ok := headerIndex["original_trace"]; ok && idx < len(record) {
		addenda.OriginalTrace = record[idx]
	}
	if idx, ok := headerIndex["original_rdfi"]; ok && idx < len(record) {
		addenda.OriginalDFI = record[idx]
	}
	if idx, ok := headerIndex["corrected_data"]; ok && idx < len(record) {
		addenda.CorrectedData = record[idx]
	}
}

// applyAddenda99Modifications applies modifications to Addenda99.
func (ts *TableSet) applyAddenda99Modifications(addenda *ach.Addenda99, record []string, headerIndex map[string]int) {
	if idx, ok := headerIndex["return_code"]; ok && idx < len(record) {
		addenda.ReturnCode = record[idx]
	}
	if idx, ok := headerIndex["original_trace"]; ok && idx < len(record) {
		addenda.OriginalTrace = record[idx]
	}
	if idx, ok := headerIndex["original_rdfi"]; ok && idx < len(record) {
		addenda.OriginalDFI = record[idx]
	}
	if idx, ok := headerIndex["addenda_information"]; ok && idx < len(record) {
		addenda.AddendaInformation = record[idx]
	}
}

// applyAddenda98RefusedModifications applies modifications to Addenda98Refused.
func (ts *TableSet) applyAddenda98RefusedModifications(addenda *ach.Addenda98Refused, record []string, headerIndex map[string]int) {
	if idx, ok := headerIndex["refused_change_code"]; ok && idx < len(record) {
		addenda.RefusedChangeCode = record[idx]
	}
	if idx, ok := headerIndex["original_trace"]; ok && idx < len(record) {
		addenda.OriginalTrace = record[idx]
	}
	if idx, ok := headerIndex["original_rdfi"]; ok && idx < len(record) {
		addenda.OriginalDFI = record[idx]
	}
	if idx, ok := headerIndex["corrected_data"]; ok && idx < len(record) {
		addenda.CorrectedData = record[idx]
	}
	if idx, ok := headerIndex["change_code"]; ok && idx < len(record) {
		addenda.ChangeCode = record[idx]
	}
	if idx, ok := headerIndex["trace_sequence_number"]; ok && idx < len(record) {
		addenda.TraceSequenceNumber = record[idx]
	}
	if idx, ok := headerIndex["trace_number"]; ok && idx < len(record) {
		addenda.TraceNumber = record[idx]
	}
}

// applyAddenda99DishonoredModifications applies modifications to Addenda99Dishonored.
func (ts *TableSet) applyAddenda99DishonoredModifications(addenda *ach.Addenda99Dishonored, record []string, headerIndex map[string]int) {
	if idx, ok := headerIndex["dishonored_return_reason_code"]; ok && idx < len(record) {
		addenda.DishonoredReturnReasonCode = record[idx]
	}
	if idx, ok := headerIndex["original_entry_trace_number"]; ok && idx < len(record) {
		addenda.OriginalEntryTraceNumber = record[idx]
	}
	if idx, ok := headerIndex["original_receiving_dfi_identification"]; ok && idx < len(record) {
		addenda.OriginalReceivingDFIIdentification = record[idx]
	}
	if idx, ok := headerIndex["return_trace_number"]; ok && idx < len(record) {
		addenda.ReturnTraceNumber = record[idx]
	}
	if idx, ok := headerIndex["return_settlement_date"]; ok && idx < len(record) {
		addenda.ReturnSettlementDate = record[idx]
	}
	if idx, ok := headerIndex["return_reason_code"]; ok && idx < len(record) {
		addenda.ReturnReasonCode = record[idx]
	}
	if idx, ok := headerIndex["addenda_information"]; ok && idx < len(record) {
		addenda.AddendaInformation = record[idx]
	}
	if idx, ok := headerIndex["trace_number"]; ok && idx < len(record) {
		addenda.TraceNumber = record[idx]
	}
}

// applyAddenda99ContestedModifications applies modifications to Addenda99Contested.
func (ts *TableSet) applyAddenda99ContestedModifications(addenda *ach.Addenda99Contested, record []string, headerIndex map[string]int) {
	if idx, ok := headerIndex["contested_return_code"]; ok && idx < len(record) {
		addenda.ContestedReturnCode = record[idx]
	}
	if idx, ok := headerIndex["original_entry_trace_number"]; ok && idx < len(record) {
		addenda.OriginalEntryTraceNumber = record[idx]
	}
	if idx, ok := headerIndex["date_original_entry_returned"]; ok && idx < len(record) {
		addenda.DateOriginalEntryReturned = record[idx]
	}
	if idx, ok := headerIndex["original_receiving_dfi_identification"]; ok && idx < len(record) {
		addenda.OriginalReceivingDFIIdentification = record[idx]
	}
	if idx, ok := headerIndex["original_settlement_date"]; ok && idx < len(record) {
		addenda.OriginalSettlementDate = record[idx]
	}
	if idx, ok := headerIndex["return_trace_number"]; ok && idx < len(record) {
		addenda.ReturnTraceNumber = record[idx]
	}
	if idx, ok := headerIndex["return_settlement_date"]; ok && idx < len(record) {
		addenda.ReturnSettlementDate = record[idx]
	}
	if idx, ok := headerIndex["return_reason_code"]; ok && idx < len(record) {
		addenda.ReturnReasonCode = record[idx]
	}
	if idx, ok := headerIndex["contested_dishonored_return_trace_number"]; ok && idx < len(record) {
		addenda.DishonoredReturnTraceNumber = record[idx]
	}
	if idx, ok := headerIndex["contested_dishonored_return_settlement_date"]; ok && idx < len(record) {
		addenda.DishonoredReturnSettlementDate = record[idx]
	}
	if idx, ok := headerIndex["contested_dishonored_return_reason_code"]; ok && idx < len(record) {
		addenda.DishonoredReturnReasonCode = record[idx]
	}
	if idx, ok := headerIndex["trace_number"]; ok && idx < len(record) {
		addenda.TraceNumber = record[idx]
	}
}

// GetEntriesTable returns the entries TableData for use with filesql.
// This is the most commonly used table for transaction analysis.
func (ts *TableSet) GetEntriesTable() *parser.TableData {
	if ts == nil {
		return nil
	}
	return ts.entries
}

// GetBatchesTable returns the batches TableData for use with filesql.
func (ts *TableSet) GetBatchesTable() *parser.TableData {
	if ts == nil {
		return nil
	}
	return ts.batches
}

// GetFileHeaderTable returns the file header TableData for use with filesql.
func (ts *TableSet) GetFileHeaderTable() *parser.TableData {
	if ts == nil {
		return nil
	}
	return ts.fileHeader
}

// GetAddendaTable returns the addenda TableData for use with filesql.
func (ts *TableSet) GetAddendaTable() *parser.TableData {
	if ts == nil {
		return nil
	}
	return ts.addenda
}

// GetIATBatchesTable returns the IAT batches TableData for use with filesql.
func (ts *TableSet) GetIATBatchesTable() *parser.TableData {
	if ts == nil {
		return nil
	}
	return ts.iatBatches
}

// GetIATEntriesTable returns the IAT entries TableData for use with filesql.
func (ts *TableSet) GetIATEntriesTable() *parser.TableData {
	if ts == nil {
		return nil
	}
	return ts.iatEntries
}

// GetIATAddendaTable returns the IAT addenda TableData for use with filesql.
func (ts *TableSet) GetIATAddendaTable() *parser.TableData {
	if ts == nil {
		return nil
	}
	return ts.iatAddenda
}

// UpdateEntriesFromTableData updates the internal entries data from modified TableData.
// Call this after making SQL modifications to prepare for WriteToWriter.
func (ts *TableSet) UpdateEntriesFromTableData(entries *parser.TableData) {
	if ts != nil {
		ts.entries = entries
	}
}

// UpdateFileHeaderFromTableData updates the internal file header data from modified TableData.
func (ts *TableSet) UpdateFileHeaderFromTableData(fileHeader *parser.TableData) {
	if ts != nil {
		ts.fileHeader = fileHeader
	}
}

// UpdateBatchesFromTableData updates the internal batches data from modified TableData.
func (ts *TableSet) UpdateBatchesFromTableData(batches *parser.TableData) {
	if ts != nil {
		ts.batches = batches
	}
}

// UpdateAddendaFromTableData updates the internal addenda data from modified TableData.
func (ts *TableSet) UpdateAddendaFromTableData(addenda *parser.TableData) {
	if ts != nil {
		ts.addenda = addenda
	}
}

// ParseReader parses an ACH file from an io.Reader and returns a TableSet.
// This function encapsulates the moov-io/ach dependency so that callers
// don't need to import moov-io/ach directly.
func ParseReader(reader io.Reader) (*TableSet, error) {
	achFile, err := ach.NewReader(reader).Read()
	if err != nil {
		return nil, fmt.Errorf("failed to parse ACH file: %w", err)
	}
	return fromFile(&achFile), nil
}

// WriteToWriter writes the ACH file from a TableSet to an io.Writer.
// This function encapsulates the moov-io/ach dependency so that callers
// don't need to import moov-io/ach directly.
func (ts *TableSet) WriteToWriter(writer io.Writer) error {
	achFile, err := ts.toFile()
	if err != nil {
		return err
	}
	w := ach.NewWriter(writer)
	return w.Write(achFile)
}

// convertIATBatches extracts IAT batch information into TableData.
func convertIATBatches(file *ach.File) *parser.TableData {
	headers := []string{
		"batch_index",
		"service_class_code",
		"iat_indicator",
		"foreign_exchange_indicator",
		"foreign_exchange_reference_indicator",
		"foreign_exchange_reference",
		"iso_destination_country_code",
		"originator_identification",
		"standard_entry_class_code",
		"company_entry_description",
		"iso_originating_currency_code",
		"iso_destination_currency_code",
		"effective_entry_date",
		"odfi_identification",
		"batch_number",
	}

	columnTypes := []parser.ColumnType{
		parser.TypeInteger, // batch_index
		parser.TypeInteger, // service_class_code
		parser.TypeText,    // iat_indicator
		parser.TypeText,    // foreign_exchange_indicator
		parser.TypeInteger, // foreign_exchange_reference_indicator
		parser.TypeText,    // foreign_exchange_reference
		parser.TypeText,    // iso_destination_country_code
		parser.TypeText,    // originator_identification
		parser.TypeText,    // standard_entry_class_code
		parser.TypeText,    // company_entry_description
		parser.TypeText,    // iso_originating_currency_code
		parser.TypeText,    // iso_destination_currency_code
		parser.TypeText,    // effective_entry_date
		parser.TypeText,    // odfi_identification
		parser.TypeInteger, // batch_number
	}

	records := make([][]string, 0, len(file.IATBatches))
	for i, batch := range file.IATBatches {
		bh := batch.Header
		record := []string{
			strconv.Itoa(i),
			strconv.Itoa(bh.ServiceClassCode),
			strings.TrimSpace(bh.IATIndicator),
			strings.TrimSpace(bh.ForeignExchangeIndicator),
			strconv.Itoa(bh.ForeignExchangeReferenceIndicator),
			strings.TrimSpace(bh.ForeignExchangeReference),
			strings.TrimSpace(bh.ISODestinationCountryCode),
			strings.TrimSpace(bh.OriginatorIdentification),
			bh.StandardEntryClassCode,
			strings.TrimSpace(bh.CompanyEntryDescription),
			strings.TrimSpace(bh.ISOOriginatingCurrencyCode),
			strings.TrimSpace(bh.ISODestinationCurrencyCode),
			bh.EffectiveEntryDate,
			bh.ODFIIdentification,
			strconv.Itoa(bh.BatchNumber),
		}
		records = append(records, record)
	}

	if len(records) == 0 {
		records = [][]string{}
	}

	return &parser.TableData{
		Headers:     headers,
		Records:     records,
		ColumnTypes: columnTypes,
	}
}

// convertIATEntries extracts IAT entry detail records into TableData.
func convertIATEntries(file *ach.File) *parser.TableData {
	headers := []string{
		"batch_index",
		"entry_index",
		"transaction_code",
		"rdfi_identification",
		"check_digit",
		"addenda_records",
		"amount",
		"dfi_account_number",
		"ofac_screening_indicator",
		"secondary_ofac_screening_indicator",
		"addenda_record_indicator",
		"trace_number",
		"category",
	}

	columnTypes := []parser.ColumnType{
		parser.TypeInteger, // batch_index
		parser.TypeInteger, // entry_index
		parser.TypeInteger, // transaction_code
		parser.TypeText,    // rdfi_identification
		parser.TypeText,    // check_digit
		parser.TypeInteger, // addenda_records
		parser.TypeInteger, // amount
		parser.TypeText,    // dfi_account_number
		parser.TypeText,    // ofac_screening_indicator
		parser.TypeText,    // secondary_ofac_screening_indicator
		parser.TypeInteger, // addenda_record_indicator
		parser.TypeText,    // trace_number
		parser.TypeText,    // category
	}

	totalEntries := 0
	for _, batch := range file.IATBatches {
		totalEntries += len(batch.Entries)
	}
	records := make([][]string, 0, totalEntries)
	for batchIdx, batch := range file.IATBatches {
		for entryIdx, entry := range batch.Entries {
			record := []string{
				strconv.Itoa(batchIdx),
				strconv.Itoa(entryIdx),
				strconv.Itoa(entry.TransactionCode),
				entry.RDFIIdentification,
				entry.CheckDigit,
				strconv.Itoa(entry.AddendaRecords),
				strconv.Itoa(entry.Amount),
				strings.TrimSpace(entry.DFIAccountNumber),
				strings.TrimSpace(entry.OFACScreeningIndicator),
				strings.TrimSpace(entry.SecondaryOFACScreeningIndicator),
				strconv.Itoa(entry.AddendaRecordIndicator),
				entry.TraceNumber,
				entry.Category,
			}
			records = append(records, record)
		}
	}

	if len(records) == 0 {
		records = [][]string{}
	}

	return &parser.TableData{
		Headers:     headers,
		Records:     records,
		ColumnTypes: columnTypes,
	}
}

// convertIATAddenda extracts IAT addenda records (types 10-18, 98, 99) into TableData.
func convertIATAddenda(file *ach.File) *parser.TableData {
	headers := []string{
		"batch_index",
		"entry_index",
		"addenda_index",
		"addenda_type",
		"type_code",
		"entry_detail_sequence_number",
		// Addenda10 fields
		"transaction_type_code",
		"foreign_payment_amount",
		"foreign_trace_number",
		"receiving_company_name",
		// Addenda11 fields
		"originator_name",
		"originator_street_address",
		// Addenda12 fields
		"originator_city_state_province",
		"originator_country_postal_code",
		// Addenda13 fields
		"odfi_name",
		"odfi_identification_number_qualifier",
		"odfi_identification",
		"odfi_branch_country_code",
		// Addenda14 fields
		"rdfi_name",
		"rdfi_identification_number_qualifier",
		"rdfi_identification",
		"rdfi_branch_country_code",
		// Addenda15 fields
		"receiver_identification_number",
		"receiver_street_address",
		// Addenda16 fields
		"receiver_city_state_province",
		"receiver_country_postal_code",
		// Addenda17/18 fields
		"payment_related_information",
		"sequence_number",
		"foreign_correspondent_bank_name",
		"foreign_correspondent_bank_id_number_qualifier",
		"foreign_correspondent_bank_id_number",
		"foreign_correspondent_bank_branch_country_code",
		// Addenda98/99 fields
		"original_trace",
		"original_rdfi",
		"corrected_data",
		"change_code",
		"return_code",
		"addenda_information",
		"trace_number",
	}

	columnTypes := make([]parser.ColumnType, len(headers))
	for i := range columnTypes {
		switch headers[i] {
		case "batch_index", "entry_index", "addenda_index", "entry_detail_sequence_number",
			"sequence_number", "foreign_payment_amount":
			columnTypes[i] = parser.TypeInteger
		default:
			columnTypes[i] = parser.TypeText
		}
	}

	var records [][]string
	for batchIdx, batch := range file.IATBatches {
		for entryIdx, entry := range batch.Entries {
			addendaIdx := 0

			// Helper to create base record
			makeRecord := func(addendaType string) []string {
				record := make([]string, len(headers))
				record[0] = strconv.Itoa(batchIdx)
				record[1] = strconv.Itoa(entryIdx)
				record[2] = strconv.Itoa(addendaIdx)
				record[3] = addendaType
				return record
			}

			// Addenda10 - Transaction Information
			if entry.Addenda10 != nil {
				record := makeRecord("10")
				record[4] = entry.Addenda10.TypeCode
				record[5] = strconv.Itoa(entry.Addenda10.EntryDetailSequenceNumber)
				record[6] = entry.Addenda10.TransactionTypeCode
				record[7] = strconv.Itoa(entry.Addenda10.ForeignPaymentAmount)
				record[8] = strings.TrimSpace(entry.Addenda10.ForeignTraceNumber)
				record[9] = strings.TrimSpace(entry.Addenda10.Name)
				records = append(records, record)
				addendaIdx++
			}

			// Addenda11 - Originator Name and Address
			if entry.Addenda11 != nil {
				record := makeRecord("11")
				record[4] = entry.Addenda11.TypeCode
				record[5] = strconv.Itoa(entry.Addenda11.EntryDetailSequenceNumber)
				record[10] = strings.TrimSpace(entry.Addenda11.OriginatorName)
				record[11] = strings.TrimSpace(entry.Addenda11.OriginatorStreetAddress)
				records = append(records, record)
				addendaIdx++
			}

			// Addenda12 - Originator City/State/Province and Country/Postal Code
			if entry.Addenda12 != nil {
				record := makeRecord("12")
				record[4] = entry.Addenda12.TypeCode
				record[5] = strconv.Itoa(entry.Addenda12.EntryDetailSequenceNumber)
				record[12] = strings.TrimSpace(entry.Addenda12.OriginatorCityStateProvince)
				record[13] = strings.TrimSpace(entry.Addenda12.OriginatorCountryPostalCode)
				records = append(records, record)
				addendaIdx++
			}

			// Addenda13 - Originating DFI Information
			if entry.Addenda13 != nil {
				record := makeRecord("13")
				record[4] = entry.Addenda13.TypeCode
				record[5] = strconv.Itoa(entry.Addenda13.EntryDetailSequenceNumber)
				record[14] = strings.TrimSpace(entry.Addenda13.ODFIName)
				record[15] = strings.TrimSpace(entry.Addenda13.ODFIIDNumberQualifier)
				record[16] = strings.TrimSpace(entry.Addenda13.ODFIIdentification)
				record[17] = strings.TrimSpace(entry.Addenda13.ODFIBranchCountryCode)
				records = append(records, record)
				addendaIdx++
			}

			// Addenda14 - Receiving DFI Information
			if entry.Addenda14 != nil {
				record := makeRecord("14")
				record[4] = entry.Addenda14.TypeCode
				record[5] = strconv.Itoa(entry.Addenda14.EntryDetailSequenceNumber)
				record[18] = strings.TrimSpace(entry.Addenda14.RDFIName)
				record[19] = strings.TrimSpace(entry.Addenda14.RDFIIDNumberQualifier)
				record[20] = strings.TrimSpace(entry.Addenda14.RDFIIdentification)
				record[21] = strings.TrimSpace(entry.Addenda14.RDFIBranchCountryCode)
				records = append(records, record)
				addendaIdx++
			}

			// Addenda15 - Receiver Identification Number and Street Address
			if entry.Addenda15 != nil {
				record := makeRecord("15")
				record[4] = entry.Addenda15.TypeCode
				record[5] = strconv.Itoa(entry.Addenda15.EntryDetailSequenceNumber)
				record[22] = strings.TrimSpace(entry.Addenda15.ReceiverIDNumber)
				record[23] = strings.TrimSpace(entry.Addenda15.ReceiverStreetAddress)
				records = append(records, record)
				addendaIdx++
			}

			// Addenda16 - Receiver City/State/Province and Country/Postal Code
			if entry.Addenda16 != nil {
				record := makeRecord("16")
				record[4] = entry.Addenda16.TypeCode
				record[5] = strconv.Itoa(entry.Addenda16.EntryDetailSequenceNumber)
				record[24] = strings.TrimSpace(entry.Addenda16.ReceiverCityStateProvince)
				record[25] = strings.TrimSpace(entry.Addenda16.ReceiverCountryPostalCode)
				records = append(records, record)
				addendaIdx++
			}

			// Addenda17 - Payment Related Information (up to 2)
			for _, addenda17 := range entry.Addenda17 {
				if addenda17 == nil {
					continue
				}
				record := makeRecord("17")
				record[4] = addenda17.TypeCode
				record[5] = strconv.Itoa(addenda17.EntryDetailSequenceNumber)
				record[26] = strings.TrimSpace(addenda17.PaymentRelatedInformation)
				record[27] = strconv.Itoa(addenda17.SequenceNumber)
				records = append(records, record)
				addendaIdx++
			}

			// Addenda18 - Foreign Correspondent Bank Information (up to 5)
			for _, addenda18 := range entry.Addenda18 {
				if addenda18 == nil {
					continue
				}
				record := makeRecord("18")
				record[4] = addenda18.TypeCode
				record[5] = strconv.Itoa(addenda18.EntryDetailSequenceNumber)
				record[27] = strconv.Itoa(addenda18.SequenceNumber)
				record[28] = strings.TrimSpace(addenda18.ForeignCorrespondentBankName)
				record[29] = strings.TrimSpace(addenda18.ForeignCorrespondentBankIDNumberQualifier)
				record[30] = strings.TrimSpace(addenda18.ForeignCorrespondentBankIDNumber)
				record[31] = strings.TrimSpace(addenda18.ForeignCorrespondentBankBranchCountryCode)
				records = append(records, record)
				addendaIdx++
			}

			// Addenda98 - Notification of Change
			if entry.Addenda98 != nil {
				record := makeRecord("98")
				record[4] = entry.Addenda98.TypeCode
				record[32] = entry.Addenda98.OriginalTrace
				record[33] = entry.Addenda98.OriginalDFI
				record[34] = strings.TrimSpace(entry.Addenda98.CorrectedData)
				record[35] = entry.Addenda98.ChangeCode
				record[38] = entry.Addenda98.TraceNumber
				records = append(records, record)
				addendaIdx++
			}

			// Addenda99 - Returns
			if entry.Addenda99 != nil {
				record := makeRecord("99")
				record[4] = entry.Addenda99.TypeCode
				record[32] = entry.Addenda99.OriginalTrace
				record[33] = entry.Addenda99.OriginalDFI
				record[36] = entry.Addenda99.ReturnCode
				record[37] = strings.TrimSpace(entry.Addenda99.AddendaInformation)
				record[38] = entry.Addenda99.TraceNumber
				records = append(records, record)
				addendaIdx++
			}
		}
	}

	if len(records) == 0 {
		records = [][]string{}
	}

	return &parser.TableData{
		Headers:     headers,
		Records:     records,
		ColumnTypes: columnTypes,
	}
}

// applyIATBatchModifications updates IAT batches in the ACH file from TableData.
func (ts *TableSet) applyIATBatchModifications(file *ach.File) error {
	headerIndex := make(map[string]int)
	for i, h := range ts.iatBatches.Headers {
		headerIndex[h] = i
	}

	coords := ts.newCoordinateTracker("iat_batches")
	for _, record := range ts.iatBatches.Records {
		batchIdx, err := strconv.Atoi(record[headerIndex["batch_index"]])
		if err != nil {
			return fmt.Errorf("invalid batch_index: %w", err)
		}

		if batchIdx < 0 || batchIdx >= len(file.IATBatches) {
			return fmt.Errorf("batch_index %d out of range", batchIdx)
		}
		if err := coords.claim([]string{"batch_index"}, []int{batchIdx}); err != nil {
			return err
		}

		bh := file.IATBatches[batchIdx].Header

		if idx, ok := headerIndex["service_class_code"]; ok {
			if v, err := strconv.Atoi(record[idx]); err == nil {
				bh.ServiceClassCode = v
			}
		}
		if idx, ok := headerIndex["iat_indicator"]; ok {
			bh.IATIndicator = record[idx]
		}
		if idx, ok := headerIndex["foreign_exchange_indicator"]; ok {
			bh.ForeignExchangeIndicator = record[idx]
		}
		if idx, ok := headerIndex["foreign_exchange_reference_indicator"]; ok {
			if v, err := strconv.Atoi(record[idx]); err == nil {
				bh.ForeignExchangeReferenceIndicator = v
			}
		}
		if idx, ok := headerIndex["foreign_exchange_reference"]; ok {
			bh.ForeignExchangeReference = record[idx]
		}
		if idx, ok := headerIndex["iso_destination_country_code"]; ok {
			bh.ISODestinationCountryCode = record[idx]
		}
		if idx, ok := headerIndex["originator_identification"]; ok {
			bh.OriginatorIdentification = record[idx]
		}
		if idx, ok := headerIndex["standard_entry_class_code"]; ok {
			bh.StandardEntryClassCode = record[idx]
		}
		if idx, ok := headerIndex["company_entry_description"]; ok {
			bh.CompanyEntryDescription = record[idx]
		}
		if idx, ok := headerIndex["iso_originating_currency_code"]; ok {
			bh.ISOOriginatingCurrencyCode = record[idx]
		}
		if idx, ok := headerIndex["iso_destination_currency_code"]; ok {
			bh.ISODestinationCurrencyCode = record[idx]
		}
		if idx, ok := headerIndex["effective_entry_date"]; ok {
			bh.EffectiveEntryDate = record[idx]
		}
		if idx, ok := headerIndex["odfi_identification"]; ok {
			bh.ODFIIdentification = record[idx]
		}
		if idx, ok := headerIndex["batch_number"]; ok {
			if v, err := strconv.Atoi(record[idx]); err == nil {
				bh.BatchNumber = v
			}
		}
	}

	return nil
}

// applyIATEntryModifications updates IAT entries in the ACH file from TableData.
func (ts *TableSet) applyIATEntryModifications(file *ach.File) error {
	headerIndex := make(map[string]int)
	for i, h := range ts.iatEntries.Headers {
		headerIndex[h] = i
	}

	coords := ts.newCoordinateTracker("iat_entries")
	for _, record := range ts.iatEntries.Records {
		batchIdx, err := strconv.Atoi(record[headerIndex["batch_index"]])
		if err != nil {
			return fmt.Errorf("invalid batch_index: %w", err)
		}
		entryIdx, err := strconv.Atoi(record[headerIndex["entry_index"]])
		if err != nil {
			return fmt.Errorf("invalid entry_index: %w", err)
		}

		if batchIdx < 0 || batchIdx >= len(file.IATBatches) {
			return fmt.Errorf("batch_index %d out of range", batchIdx)
		}
		if entryIdx < 0 || entryIdx >= len(file.IATBatches[batchIdx].Entries) {
			return fmt.Errorf("entry_index %d out of range for batch %d", entryIdx, batchIdx)
		}
		if err := coords.claim([]string{"batch_index", "entry_index"}, []int{batchIdx, entryIdx}); err != nil {
			return err
		}

		entry := file.IATBatches[batchIdx].Entries[entryIdx]

		if idx, ok := headerIndex["transaction_code"]; ok {
			if v, err := strconv.Atoi(record[idx]); err == nil {
				entry.TransactionCode = v
			}
		}
		if idx, ok := headerIndex["rdfi_identification"]; ok {
			entry.RDFIIdentification = record[idx]
		}
		if idx, ok := headerIndex["check_digit"]; ok {
			entry.CheckDigit = record[idx]
		}
		if idx, ok := headerIndex["addenda_records"]; ok {
			if v, err := strconv.Atoi(record[idx]); err == nil {
				entry.AddendaRecords = v
			}
		}
		if idx, ok := headerIndex["amount"]; ok {
			if v, err := strconv.Atoi(record[idx]); err == nil {
				entry.Amount = v
			}
		}
		if idx, ok := headerIndex["dfi_account_number"]; ok {
			entry.DFIAccountNumber = record[idx]
		}
		if idx, ok := headerIndex["ofac_screening_indicator"]; ok {
			entry.OFACScreeningIndicator = record[idx]
		}
		if idx, ok := headerIndex["secondary_ofac_screening_indicator"]; ok {
			entry.SecondaryOFACScreeningIndicator = record[idx]
		}
		if idx, ok := headerIndex["addenda_record_indicator"]; ok {
			if v, err := strconv.Atoi(record[idx]); err == nil {
				entry.AddendaRecordIndicator = v
			}
		}
		if idx, ok := headerIndex["trace_number"]; ok {
			entry.TraceNumber = record[idx]
		}
		if idx, ok := headerIndex["category"]; ok {
			entry.Category = record[idx]
		}
	}

	return nil
}

// applyIATAddendaModifications updates IAT addenda in the ACH file from TableData.
func (ts *TableSet) applyIATAddendaModifications(file *ach.File) error {
	headerIndex := make(map[string]int)
	for i, h := range ts.iatAddenda.Headers {
		headerIndex[h] = i
	}

	coords := ts.newCoordinateTracker("iat_addenda")
	for _, record := range ts.iatAddenda.Records {
		batchIdx, err := strconv.Atoi(record[headerIndex["batch_index"]])
		if err != nil {
			return fmt.Errorf("invalid batch_index: %w", err)
		}
		entryIdx, err := strconv.Atoi(record[headerIndex["entry_index"]])
		if err != nil {
			return fmt.Errorf("invalid entry_index: %w", err)
		}
		addendaIdx, err := strconv.Atoi(record[headerIndex["addenda_index"]])
		if err != nil {
			return fmt.Errorf("invalid addenda_index: %w", err)
		}

		if batchIdx < 0 || batchIdx >= len(file.IATBatches) {
			return fmt.Errorf("batch_index %d out of range", batchIdx)
		}
		if entryIdx < 0 || entryIdx >= len(file.IATBatches[batchIdx].Entries) {
			return fmt.Errorf("entry_index %d out of range for batch %d", entryIdx, batchIdx)
		}
		if err := coords.claim(
			[]string{"batch_index", "entry_index", "addenda_index"},
			[]int{batchIdx, entryIdx, addendaIdx}); err != nil {
			return err
		}

		entry := file.IATBatches[batchIdx].Entries[entryIdx]
		addendaType := record[headerIndex["addenda_type"]]

		switch addendaType {
		case "10":
			if entry.Addenda10 != nil {
				if idx, ok := headerIndex["transaction_type_code"]; ok {
					entry.Addenda10.TransactionTypeCode = record[idx]
				}
				if idx, ok := headerIndex["foreign_payment_amount"]; ok {
					if v, err := strconv.Atoi(record[idx]); err == nil {
						entry.Addenda10.ForeignPaymentAmount = v
					}
				}
				if idx, ok := headerIndex["foreign_trace_number"]; ok {
					entry.Addenda10.ForeignTraceNumber = record[idx]
				}
				if idx, ok := headerIndex["receiving_company_name"]; ok {
					entry.Addenda10.Name = record[idx]
				}
			}

		case "11":
			if entry.Addenda11 != nil {
				if idx, ok := headerIndex["originator_name"]; ok {
					entry.Addenda11.OriginatorName = record[idx]
				}
				if idx, ok := headerIndex["originator_street_address"]; ok {
					entry.Addenda11.OriginatorStreetAddress = record[idx]
				}
			}

		case "12":
			if entry.Addenda12 != nil {
				if idx, ok := headerIndex["originator_city_state_province"]; ok {
					entry.Addenda12.OriginatorCityStateProvince = record[idx]
				}
				if idx, ok := headerIndex["originator_country_postal_code"]; ok {
					entry.Addenda12.OriginatorCountryPostalCode = record[idx]
				}
			}

		case "13":
			if entry.Addenda13 != nil {
				if idx, ok := headerIndex["odfi_name"]; ok {
					entry.Addenda13.ODFIName = record[idx]
				}
				if idx, ok := headerIndex["odfi_identification_number_qualifier"]; ok {
					entry.Addenda13.ODFIIDNumberQualifier = record[idx]
				}
				if idx, ok := headerIndex["odfi_identification"]; ok {
					entry.Addenda13.ODFIIdentification = record[idx]
				}
				if idx, ok := headerIndex["odfi_branch_country_code"]; ok {
					entry.Addenda13.ODFIBranchCountryCode = record[idx]
				}
			}

		case "14":
			if entry.Addenda14 != nil {
				if idx, ok := headerIndex["rdfi_name"]; ok {
					entry.Addenda14.RDFIName = record[idx]
				}
				if idx, ok := headerIndex["rdfi_identification_number_qualifier"]; ok {
					entry.Addenda14.RDFIIDNumberQualifier = record[idx]
				}
				if idx, ok := headerIndex["rdfi_identification"]; ok {
					entry.Addenda14.RDFIIdentification = record[idx]
				}
				if idx, ok := headerIndex["rdfi_branch_country_code"]; ok {
					entry.Addenda14.RDFIBranchCountryCode = record[idx]
				}
			}

		case "15":
			if entry.Addenda15 != nil {
				if idx, ok := headerIndex["receiver_identification_number"]; ok {
					entry.Addenda15.ReceiverIDNumber = record[idx]
				}
				if idx, ok := headerIndex["receiver_street_address"]; ok {
					entry.Addenda15.ReceiverStreetAddress = record[idx]
				}
			}

		case "16":
			if entry.Addenda16 != nil {
				if idx, ok := headerIndex["receiver_city_state_province"]; ok {
					entry.Addenda16.ReceiverCityStateProvince = record[idx]
				}
				if idx, ok := headerIndex["receiver_country_postal_code"]; ok {
					entry.Addenda16.ReceiverCountryPostalCode = record[idx]
				}
			}

		case "17":
			addendaIdx, err := strconv.Atoi(record[headerIndex["addenda_index"]])
			if err != nil {
				continue
			}
			// Find the corresponding Addenda17 by sequence
			a17Idx := ts.findAddenda17Index(entry, addendaIdx)
			if a17Idx >= 0 && a17Idx < len(entry.Addenda17) && entry.Addenda17[a17Idx] != nil {
				if idx, ok := headerIndex["payment_related_information"]; ok {
					entry.Addenda17[a17Idx].PaymentRelatedInformation = record[idx]
				}
			}

		case "18":
			addendaIdx, err := strconv.Atoi(record[headerIndex["addenda_index"]])
			if err != nil {
				continue
			}
			// Find the corresponding Addenda18 by sequence
			a18Idx := ts.findAddenda18Index(entry, addendaIdx)
			if a18Idx >= 0 && a18Idx < len(entry.Addenda18) && entry.Addenda18[a18Idx] != nil {
				if idx, ok := headerIndex["foreign_correspondent_bank_name"]; ok {
					entry.Addenda18[a18Idx].ForeignCorrespondentBankName = record[idx]
				}
				if idx, ok := headerIndex["foreign_correspondent_bank_id_number_qualifier"]; ok {
					entry.Addenda18[a18Idx].ForeignCorrespondentBankIDNumberQualifier = record[idx]
				}
				if idx, ok := headerIndex["foreign_correspondent_bank_id_number"]; ok {
					entry.Addenda18[a18Idx].ForeignCorrespondentBankIDNumber = record[idx]
				}
				if idx, ok := headerIndex["foreign_correspondent_bank_branch_country_code"]; ok {
					entry.Addenda18[a18Idx].ForeignCorrespondentBankBranchCountryCode = record[idx]
				}
			}

		case "98":
			if entry.Addenda98 != nil {
				if idx, ok := headerIndex["original_trace"]; ok {
					entry.Addenda98.OriginalTrace = record[idx]
				}
				if idx, ok := headerIndex["original_rdfi"]; ok {
					entry.Addenda98.OriginalDFI = record[idx]
				}
				if idx, ok := headerIndex["corrected_data"]; ok {
					entry.Addenda98.CorrectedData = record[idx]
				}
				if idx, ok := headerIndex["change_code"]; ok {
					entry.Addenda98.ChangeCode = record[idx]
				}
				if idx, ok := headerIndex["trace_number"]; ok {
					entry.Addenda98.TraceNumber = record[idx]
				}
			}

		case "99":
			if entry.Addenda99 != nil {
				if idx, ok := headerIndex["original_trace"]; ok {
					entry.Addenda99.OriginalTrace = record[idx]
				}
				if idx, ok := headerIndex["original_rdfi"]; ok {
					entry.Addenda99.OriginalDFI = record[idx]
				}
				if idx, ok := headerIndex["return_code"]; ok {
					entry.Addenda99.ReturnCode = record[idx]
				}
				if idx, ok := headerIndex["addenda_information"]; ok {
					entry.Addenda99.AddendaInformation = record[idx]
				}
				if idx, ok := headerIndex["trace_number"]; ok {
					entry.Addenda99.TraceNumber = record[idx]
				}
			}
		}
	}

	return nil
}

// findAddenda17Index calculates the index in the Addenda17 slice based on addenda_index.
// Addenda17 starts after Addenda10-16 (7 addenda types).
func (ts *TableSet) findAddenda17Index(entry *ach.IATEntryDetail, addendaIdx int) int {
	// Count addenda before Addenda17
	offset := 0
	if entry.Addenda10 != nil {
		offset++
	}
	if entry.Addenda11 != nil {
		offset++
	}
	if entry.Addenda12 != nil {
		offset++
	}
	if entry.Addenda13 != nil {
		offset++
	}
	if entry.Addenda14 != nil {
		offset++
	}
	if entry.Addenda15 != nil {
		offset++
	}
	if entry.Addenda16 != nil {
		offset++
	}
	return addendaIdx - offset
}

// findAddenda18Index calculates the index in the Addenda18 slice based on addenda_index.
func (ts *TableSet) findAddenda18Index(entry *ach.IATEntryDetail, addendaIdx int) int {
	// Count addenda before Addenda18 (10-16 + 17s)
	offset := 0
	if entry.Addenda10 != nil {
		offset++
	}
	if entry.Addenda11 != nil {
		offset++
	}
	if entry.Addenda12 != nil {
		offset++
	}
	if entry.Addenda13 != nil {
		offset++
	}
	if entry.Addenda14 != nil {
		offset++
	}
	if entry.Addenda15 != nil {
		offset++
	}
	if entry.Addenda16 != nil {
		offset++
	}
	offset += len(entry.Addenda17)
	return addendaIdx - offset
}

// UpdateIATBatchesFromTableData updates the internal IAT batches data from modified TableData.
func (ts *TableSet) UpdateIATBatchesFromTableData(iatBatches *parser.TableData) {
	if ts != nil {
		ts.iatBatches = iatBatches
	}
}

// UpdateIATEntriesFromTableData updates the internal IAT entries data from modified TableData.
func (ts *TableSet) UpdateIATEntriesFromTableData(iatEntries *parser.TableData) {
	if ts != nil {
		ts.iatEntries = iatEntries
	}
}

// UpdateIATAddendaFromTableData updates the internal IAT addenda data from modified TableData.
func (ts *TableSet) UpdateIATAddendaFromTableData(iatAddenda *parser.TableData) {
	if ts != nil {
		ts.iatAddenda = iatAddenda
	}
}
