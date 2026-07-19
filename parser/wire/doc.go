// Package wire provides bidirectional conversion between Fedwire files and TableData.
//
// This package bridges the moov-io/wire library with parser.TableData,
// enabling SQL queries on Fedwire message data via filesql.
//
// # Security Note
//
// TableData structures expose sensitive banking information including routing numbers,
// account numbers, names, and transaction amounts. Avoid logging or exporting
// TableData contents verbatim in production environments.
//
// # Table Structure
//
// A single Fedwire file contains one FEDWireMessage, which is converted to a single-row
// flat table with approximately 326 columns. All columns are TEXT type since the wire
// format stores all values as strings.
//
// Column groups include:
//   - Mandatory fields: SenderSupplied, TypeSubType, IMAD, Amount, SenderDI, ReceiverDI, BusinessFunctionCode
//   - Financial institutions: BeneficiaryIntermediaryFI, BeneficiaryFI, OriginatorFI, InstructingFI
//   - Parties: Beneficiary, Originator, OriginatorOptionF
//   - FI-to-FI information: FIReceiverFI, FIIntermediaryFI, FIBeneficiaryFI, FIBeneficiary
//   - Advice records: FIDrawdownDebitAccountAdvice, FIIntermediaryFIAdvice, FIBeneficiaryFIAdvice, FIBeneficiaryAdvice
//   - Cover payment: OrderingCustomer, OrderingInstitution, IntermediaryInstitution, etc.
//   - Remittance: RemittanceOriginator, RemittanceBeneficiary, RelatedRemittance, etc.
//   - System: MessageDisposition, ReceiptTimeStamp, OMAD, ErrorWire, ServiceMessage
//
// # Limitations
//
// Only UPDATE operations on existing rows are supported for round-trip editing.
// INSERT/DELETE operations in SQL are not reflected in the output wire file.
// Optional message sections that were not present in the original file cannot be
// added via SQL modifications.
//
// # Usage
//
//	import (
//	    "github.com/nao1215/filesql/parser/wire"
//	    moovwire "github.com/moov-io/wire"
//	)
//
//	// Read wire file
//	f, _ := os.Open("payment.fed")
//	defer f.Close()
//	ts, _ := wire.ParseReader(f)
//
//	// Access the flat table
//	table := ts.GetMessageTable()
//	fmt.Println("Columns:", len(table.Headers))
//
//	// After SQL modifications, write back
//	out, _ := os.Create("modified.fed")
//	defer out.Close()
//	ts.WriteToWriter(out)
package wire
