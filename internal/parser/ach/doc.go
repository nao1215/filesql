// Package ach provides bidirectional conversion between ACH files and TableData.
//
// This package bridges the moov-io/ach library with parser.TableData,
// enabling SQL queries on ACH file data via filesql.
//
// # Security Note
//
// TableData structures expose sensitive banking information including account numbers,
// routing numbers, names, and transaction amounts. Avoid logging or exporting
// TableData contents verbatim in production environments.
//
// # Supported Addenda Types
//
// Standard entries (EntryDetail):
//   - Addenda02: Point-of-Sale (POS), Machine Transfer Entry (MTE), Shared Network Entry (SHR)
//   - Addenda05: Payment Related Information (PPD, CCD, CTX, WEB, etc.)
//   - Addenda98: Notification of Change (NOC)
//   - Addenda98Refused: Refused Notification of Change
//   - Addenda99: Return entries
//   - Addenda99Dishonored: Dishonored Returns
//   - Addenda99Contested: Contested Dishonored Returns
//
// IAT entries (IATEntryDetail) - International ACH Transactions:
//   - Addenda10: Transaction information (receiving company, foreign payment amount)
//   - Addenda11: Originator name and address
//   - Addenda12: Originator city, state/province, country, postal code
//   - Addenda13: Originating DFI information
//   - Addenda14: Receiving DFI information
//   - Addenda15: Receiver identification number and street address
//   - Addenda16: Receiver city, state/province, country, postal code
//   - Addenda17: Payment related information (up to 2 per entry)
//   - Addenda18: Foreign correspondent bank information (up to 5 per entry)
//   - Addenda98/99: Same as standard entries
//
// # Limitations
//
// Only UPDATE operations on existing rows are supported for round-trip editing.
// INSERT/DELETE operations in SQL are not reflected in the output ACH file.
// This is because ACH file structure requires careful coordination between
// related records (entry counts, hash totals, addenda indicators).
//
// This package uses github.com/tiendc/go-deepcopy for deep copying ACH files.
// If moov-io/ach adds new fields (especially interfaces or unexported fields),
// the deep copy may not capture them correctly. Monitor moov-io/ach releases.
//
// # Usage
//
//	import (
//	    "os"
//
//	    "github.com/nao1215/filesql/internal/parser/ach"
//	)
//
//	// Read an ACH file into TableData for SQL queries
//	f, _ := os.Open("payment.ach")
//	defer f.Close()
//	tables, _ := ach.ParseReader(f)
//
//	// After SQL modifications, write the file back out
//	out, _ := os.Create("modified.ach")
//	defer out.Close()
//	tables.WriteToWriter(out)
package ach
