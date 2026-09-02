package wire

import (
	"bytes"
	"errors"
	"fmt"
	"io"

	"github.com/moov-io/wire"
	"github.com/nao1215/filesql/internal/parser"
)

// TableSet contains a flat TableData representing all fields of a Fedwire message.
// This structure preserves the complete message while enabling flat table-based queries.
type TableSet struct {
	// message contains all flattened fields of the FEDWireMessage (1 row)
	message *parser.TableData

	// original stores the original wire file for reconstruction
	original *wire.File
}

// fromFile converts a wire.File to a TableSet with a single flat table.
// It is what ParseReader hands back, and the TableSet can be used with filesql
// for SQL queries.
//
// Note: The TableSet stores a reference to the original file (not a copy), and
// toFile creates a deep copy before applying TableData modifications.
func fromFile(file *wire.File) *TableSet {
	if file == nil {
		return nil
	}

	ts := &TableSet{
		original: file,
	}

	headers := messageHeaders()
	record := messageRecord(&file.FEDWireMessage)
	columnTypes := make([]parser.ColumnType, len(headers))
	for i := range columnTypes {
		columnTypes[i] = parser.TypeText
	}

	ts.message = &parser.TableData{
		Headers:     headers,
		Records:     [][]string{record},
		ColumnTypes: columnTypes,
	}

	return ts
}

// toFile reconstructs a wire.File from modified TableData.
// This allows round-trip editing: Wire -> TableData -> SQL modifications -> Wire
//
// The function creates a deep copy of the original wire file and applies
// modifications from the TableData. Sub-structs that were nil in the original
// file are lazily allocated when the TableData record contains non-empty values
// for their fields, enabling addition of new sections via SQL edits.
func (ts *TableSet) toFile() (*wire.File, error) {
	if ts == nil || ts.original == nil {
		return nil, errors.New("no original wire file available")
	}

	newFile := deepCopyFile(ts.original)

	if ts.message != nil && len(ts.message.Records) > 0 {
		applyModifications(&newFile.FEDWireMessage, ts.message)
		// Every edit is in place, so this is where a value too wide for the
		// record it goes into can be caught — before anything is written.
		if err := validateFieldWidths(&newFile.FEDWireMessage); err != nil {
			return nil, err
		}
	}

	return &newFile, nil
}

// ParseReader parses a wire file from an io.Reader and returns a TableSet.
// This function encapsulates the moov-io/wire dependency so that callers
// don't need to import moov-io/wire directly.
// Returns an error if reader is nil.
func ParseReader(reader io.Reader) (*TableSet, error) {
	if reader == nil {
		return nil, errors.New("wire: reader must not be nil")
	}
	wireReader := wire.NewReader(reader)
	file, err := wireReader.Read()
	if err != nil {
		return nil, fmt.Errorf("failed to parse wire file: %w", err)
	}
	return fromFile(&file), nil
}

// WriteToWriter writes the wire file from a TableSet to an io.Writer.
// This function encapsulates the moov-io/wire dependency so that callers
// don't need to import moov-io/wire directly.
// Returns an error if the TableSet or writer is nil.
func (ts *TableSet) WriteToWriter(writer io.Writer) error {
	if ts == nil {
		return errors.New("wire: TableSet must not be nil")
	}
	if writer == nil {
		return errors.New("wire: writer must not be nil")
	}
	wireFile, err := ts.toFile()
	if err != nil {
		return err
	}
	// Stage the message and read it back before any of it reaches the caller's
	// writer. A Fedwire message carries no derived totals, so every field in
	// the written file has to be the field that went into it, and reparsing is
	// the only check that covers all 326 of them at once.
	//
	// It caught a real one. moov-io/wire v0.16.1 writes the remittance
	// originator's fourth address line from its first, so a file that was read
	// and written straight back came out with line four replaced by a copy of
	// line one and the original gone, with nothing to say so. Refusing the
	// write leaves the caller holding the file they started with, which for a
	// payment file is the only safe answer: a silently wrong export is worse
	// than a failed one.
	var staged bytes.Buffer
	w := wire.NewWriter(&staged, wire.VariableLengthFields(true))
	if err := w.Write(wireFile); err != nil {
		return err
	}
	if err := verifyWritten(ts.message, staged.Bytes()); err != nil {
		return err
	}
	_, err = writer.Write(staged.Bytes())
	return err
}

// verifyWritten reparses the staged bytes and reports the first column whose
// value the write did not preserve. want is the table the write was made from,
// so a column that differs is one the caller asked for and did not get.
//
// A message that cannot be reparsed at all is reported the same way: bytes this
// package cannot read back are bytes it should not hand out.
func verifyWritten(want *parser.TableData, staged []byte) error {
	if want == nil || len(want.Records) == 0 {
		return nil
	}
	back, err := ParseReader(bytes.NewReader(staged))
	if err != nil {
		return fmt.Errorf("the written message cannot be read back: %w", err)
	}
	got := back.message
	if got == nil || len(got.Records) == 0 {
		return errors.New("the written message came back with no rows")
	}
	for i, column := range want.Headers {
		if i >= len(want.Records[0]) || i >= len(got.Records[0]) {
			break
		}
		if want.Records[0][i] != got.Records[0][i] {
			return fmt.Errorf(
				"the written message would hold %q in %s, not %q; the file was not written",
				got.Records[0][i], column, want.Records[0][i])
		}
	}
	return nil
}

// GetMessageTable returns the message TableData for use with filesql.
func (ts *TableSet) GetMessageTable() *parser.TableData {
	if ts == nil {
		return nil
	}
	return ts.message
}

// UpdateMessageFromTableData updates the internal message data from modified TableData.
// Call this after making SQL modifications to prepare for WriteToWriter.
func (ts *TableSet) UpdateMessageFromTableData(td *parser.TableData) {
	if ts != nil {
		ts.message = td
	}
}

// messageHeaders returns all column names for the flat message table.
func messageHeaders() []string {
	h := make([]string, 0, 330)

	// SenderSupplied (4)
	h = append(h,
		"sender_supplied_format_version",
		"sender_supplied_user_request_correlation",
		"sender_supplied_test_production_code",
		"sender_supplied_message_duplication_code",
	)

	// TypeSubType (2)
	h = append(h, "type_code", "sub_type_code")

	// InputMessageAccountabilityData (3)
	h = append(h,
		"imad_input_cycle_date",
		"imad_input_source",
		"imad_input_sequence_number",
	)

	// Amount (1)
	h = append(h, "amount")

	// SenderDepositoryInstitution (2)
	h = append(h, "sender_di_routing_number", "sender_di_short_name")

	// ReceiverDepositoryInstitution (2)
	h = append(h, "receiver_di_routing_number", "receiver_di_short_name")

	// BusinessFunctionCode (2)
	h = append(h, "business_function_code", "transaction_type_code")

	// SenderReference (1)
	h = append(h, "sender_reference")

	// PreviousMessageIdentifier (1)
	h = append(h, "previous_message_identifier")

	// LocalInstrument (2)
	h = append(h, "local_instrument_code", "local_instrument_proprietary_code")

	// PaymentNotification (7)
	h = append(h,
		"payment_notification_indicator",
		"payment_notification_electronic_address",
		"payment_notification_contact_name",
		"payment_notification_contact_phone_number",
		"payment_notification_contact_mobile_number",
		"payment_notification_contact_fax_number",
		"payment_notification_end_to_end_identification",
	)

	// Charges (5)
	h = append(h,
		"charges_details",
		"charges_senders_one",
		"charges_senders_two",
		"charges_senders_three",
		"charges_senders_four",
	)

	// InstructedAmount (2)
	h = append(h, "instructed_amount_currency_code", "instructed_amount_amount")

	// ExchangeRate (1)
	h = append(h, "exchange_rate")

	// BeneficiaryIntermediaryFI (6)
	h = append(h, identifiedEntityHeaders("beneficiary_intermediary_fi")...)

	// BeneficiaryFI (6)
	h = append(h, identifiedEntityHeaders("beneficiary_fi")...)

	// Beneficiary (6)
	h = append(h, identifiedEntityHeaders("beneficiary")...)

	// BeneficiaryReference (1)
	h = append(h, "beneficiary_reference")

	// AccountDebitedDrawdown (6)
	h = append(h, identifiedEntityHeaders("account_debited_drawdown")...)

	// Originator (6)
	h = append(h, identifiedEntityHeaders("originator")...)

	// OriginatorOptionF (5)
	h = append(h,
		"originator_option_f_party_identifier",
		"originator_option_f_name",
		"originator_option_f_line_one",
		"originator_option_f_line_two",
		"originator_option_f_line_three",
	)

	// OriginatorFI (6)
	h = append(h, identifiedEntityHeaders("originator_fi")...)

	// InstructingFI (6)
	h = append(h, identifiedEntityHeaders("instructing_fi")...)

	// AccountCreditedDrawdown (1)
	h = append(h, "account_credited_drawdown_number")

	// OriginatorToBeneficiary (4)
	h = append(h, lineHeaders("originator_to_beneficiary", 4)...)

	// FIReceiverFI (6)
	h = append(h, lineHeaders("fi_receiver_fi", 6)...)

	// FIDrawdownDebitAccountAdvice (7)
	h = append(h, adviceHeaders("fi_drawdown_debit_account_advice")...)

	// FIIntermediaryFI (6)
	h = append(h, lineHeaders("fi_intermediary_fi", 6)...)

	// FIIntermediaryFIAdvice (7)
	h = append(h, adviceHeaders("fi_intermediary_fi_advice")...)

	// FIBeneficiaryFI (6)
	h = append(h, lineHeaders("fi_beneficiary_fi", 6)...)

	// FIBeneficiaryFIAdvice (7)
	h = append(h, adviceHeaders("fi_beneficiary_fi_advice")...)

	// FIBeneficiary (6)
	h = append(h, lineHeaders("fi_beneficiary", 6)...)

	// FIBeneficiaryAdvice (7)
	h = append(h, adviceHeaders("fi_beneficiary_advice")...)

	// FIPaymentMethodToBeneficiary (2)
	h = append(h,
		"fi_payment_method_to_beneficiary_payment_method",
		"fi_payment_method_to_beneficiary_additional_information",
	)

	// FIAdditionalFIToFI (6)
	h = append(h, lineHeaders("fi_additional_fi_to_fi", 6)...)

	// CurrencyInstructedAmount (2)
	h = append(h, "currency_instructed_amount_swift_field_tag", "currency_instructed_amount_amount")

	// OrderingCustomer (7)
	h = append(h, coverPaymentHeaders("ordering_customer")...)

	// OrderingInstitution (7)
	h = append(h, coverPaymentHeaders("ordering_institution")...)

	// IntermediaryInstitution (7)
	h = append(h, coverPaymentHeaders("intermediary_institution")...)

	// InstitutionAccount (7)
	h = append(h, coverPaymentHeaders("institution_account")...)

	// BeneficiaryCustomer (7)
	h = append(h, coverPaymentHeaders("beneficiary_customer")...)

	// Remittance (7)
	h = append(h, coverPaymentHeaders("remittance")...)

	// SenderToReceiver (7)
	h = append(h, coverPaymentHeaders("sender_to_receiver")...)

	// UnstructuredAddenda (2)
	h = append(h, "unstructured_addenda_length", "unstructured_addenda")

	// RelatedRemittance (22)
	h = append(h,
		"related_remittance_identification",
		"related_remittance_location_method",
		"related_remittance_location_electronic_address",
	)
	h = append(h, remittanceDataHeaders("related_remittance")...)

	// RemittanceOriginator (29)
	h = append(h,
		"remittance_originator_identification_type",
		"remittance_originator_identification_code",
		"remittance_originator_identification_number",
		"remittance_originator_identification_number_issuer",
		"remittance_originator_contact_name",
		"remittance_originator_contact_phone_number",
		"remittance_originator_contact_mobile_number",
		"remittance_originator_contact_fax_number",
		"remittance_originator_contact_electronic_address",
		"remittance_originator_contact_other",
	)
	h = append(h, remittanceDataHeaders("remittance_originator")...)

	// RemittanceBeneficiary (23)
	h = append(h,
		"remittance_beneficiary_identification_type",
		"remittance_beneficiary_identification_code",
		"remittance_beneficiary_identification_number",
		"remittance_beneficiary_identification_number_issuer",
	)
	h = append(h, remittanceDataHeaders("remittance_beneficiary")...)

	// PrimaryRemittanceDocument (4)
	h = append(h, remittanceDocumentHeaders("primary_remittance_document")...)

	// ActualAmountPaid (2)
	h = append(h, remittanceAmountHeaders("actual_amount_paid")...)

	// GrossAmountRemittanceDocument (2)
	h = append(h, remittanceAmountHeaders("gross_amount_remittance_document")...)

	// AmountNegotiatedDiscount (2)
	h = append(h, remittanceAmountHeaders("amount_negotiated_discount")...)

	// Adjustment (5)
	h = append(h,
		"adjustment_reason_code",
		"adjustment_credit_debit_indicator",
	)
	h = append(h, remittanceAmountHeaders("adjustment")...)
	h = append(h, "adjustment_additional_info")

	// DateRemittanceDocument (1)
	h = append(h, "date_remittance_document")

	// SecondaryRemittanceDocument (4)
	h = append(h, remittanceDocumentHeaders("secondary_remittance_document")...)

	// RemittanceFreeText (3)
	h = append(h, lineHeaders("remittance_free_text", 3)...)

	// ServiceMessage (12)
	h = append(h, lineHeaders("service_message", 12)...)

	// MessageDisposition (4)
	h = append(h,
		"message_disposition_format_version",
		"message_disposition_test_production_code",
		"message_disposition_message_duplication_code",
		"message_disposition_message_status_indicator",
	)

	// ReceiptTimeStamp (3)
	h = append(h,
		"receipt_time_stamp_receipt_date",
		"receipt_time_stamp_receipt_time",
		"receipt_time_stamp_receipt_application_identification",
	)

	// OutputMessageAccountabilityData (6)
	h = append(h,
		"omad_output_cycle_date",
		"omad_output_destination_id",
		"omad_output_sequence_number",
		"omad_output_date",
		"omad_output_time",
		"omad_output_frb_application_identification",
	)

	// ErrorWire (3)
	h = append(h,
		"error_wire_category",
		"error_wire_code",
		"error_wire_description",
	)

	return h
}

// messageRecord extracts all field values from a FEDWireMessage as a flat string slice.
func messageRecord(fwm *wire.FEDWireMessage) []string {
	var r []string

	// SenderSupplied (4)
	if ss := fwm.SenderSupplied; ss != nil {
		r = append(r, ss.FormatVersion, ss.UserRequestCorrelation, ss.TestProductionCode, ss.MessageDuplicationCode)
	} else {
		r = appendEmpty(r, 4)
	}

	// TypeSubType (2)
	if ts := fwm.TypeSubType; ts != nil {
		r = append(r, ts.TypeCode, ts.SubTypeCode)
	} else {
		r = appendEmpty(r, 2)
	}

	// InputMessageAccountabilityData (3)
	if imad := fwm.InputMessageAccountabilityData; imad != nil {
		r = append(r, imad.InputCycleDate, imad.InputSource, imad.InputSequenceNumber)
	} else {
		r = appendEmpty(r, 3)
	}

	// Amount (1)
	if a := fwm.Amount; a != nil {
		r = append(r, a.Amount)
	} else {
		r = append(r, "")
	}

	// SenderDepositoryInstitution (2)
	if sdi := fwm.SenderDepositoryInstitution; sdi != nil {
		r = append(r, sdi.SenderABANumber, sdi.SenderShortName)
	} else {
		r = appendEmpty(r, 2)
	}

	// ReceiverDepositoryInstitution (2)
	if rdi := fwm.ReceiverDepositoryInstitution; rdi != nil {
		r = append(r, rdi.ReceiverABANumber, rdi.ReceiverShortName)
	} else {
		r = appendEmpty(r, 2)
	}

	// BusinessFunctionCode (2)
	if bfc := fwm.BusinessFunctionCode; bfc != nil {
		r = append(r, bfc.BusinessFunctionCode, bfc.TransactionTypeCode)
	} else {
		r = appendEmpty(r, 2)
	}

	// SenderReference (1)
	if sr := fwm.SenderReference; sr != nil {
		r = append(r, sr.SenderReference)
	} else {
		r = append(r, "")
	}

	// PreviousMessageIdentifier (1)
	if pmi := fwm.PreviousMessageIdentifier; pmi != nil {
		r = append(r, pmi.PreviousMessageIdentifier)
	} else {
		r = append(r, "")
	}

	// LocalInstrument (2)
	if li := fwm.LocalInstrument; li != nil {
		r = append(r, li.LocalInstrumentCode, li.ProprietaryCode)
	} else {
		r = appendEmpty(r, 2)
	}

	// PaymentNotification (7)
	if pn := fwm.PaymentNotification; pn != nil {
		r = append(r,
			pn.PaymentNotificationIndicator,
			pn.ContactNotificationElectronicAddress,
			pn.ContactName,
			pn.ContactPhoneNumber,
			pn.ContactMobileNumber,
			pn.ContactFaxNumber,
			pn.EndToEndIdentification,
		)
	} else {
		r = appendEmpty(r, 7)
	}

	// Charges (5)
	if ch := fwm.Charges; ch != nil {
		r = append(r,
			ch.ChargeDetails,
			ch.SendersChargesOne,
			ch.SendersChargesTwo,
			ch.SendersChargesThree,
			ch.SendersChargesFour,
		)
	} else {
		r = appendEmpty(r, 5)
	}

	// InstructedAmount (2)
	if ia := fwm.InstructedAmount; ia != nil {
		r = append(r, ia.CurrencyCode, ia.Amount)
	} else {
		r = appendEmpty(r, 2)
	}

	// ExchangeRate (1)
	if er := fwm.ExchangeRate; er != nil {
		r = append(r, er.ExchangeRate)
	} else {
		r = append(r, "")
	}

	// BeneficiaryIntermediaryFI (6)
	if bifi := fwm.BeneficiaryIntermediaryFI; bifi != nil {
		r = appendFinancialInstitution(r, bifi.FinancialInstitution)
	} else {
		r = appendEmpty(r, 6)
	}

	// BeneficiaryFI (6)
	if bfi := fwm.BeneficiaryFI; bfi != nil {
		r = appendFinancialInstitution(r, bfi.FinancialInstitution)
	} else {
		r = appendEmpty(r, 6)
	}

	// Beneficiary (6)
	if b := fwm.Beneficiary; b != nil {
		r = appendPersonal(r, b.Personal)
	} else {
		r = appendEmpty(r, 6)
	}

	// BeneficiaryReference (1)
	if br := fwm.BeneficiaryReference; br != nil {
		r = append(r, br.BeneficiaryReference)
	} else {
		r = append(r, "")
	}

	// AccountDebitedDrawdown (6)
	if add := fwm.AccountDebitedDrawdown; add != nil {
		r = append(r, add.IdentificationCode, add.Identifier, add.Name,
			add.Address.AddressLineOne, add.Address.AddressLineTwo, add.Address.AddressLineThree)
	} else {
		r = appendEmpty(r, 6)
	}

	// Originator (6)
	if o := fwm.Originator; o != nil {
		r = appendPersonal(r, o.Personal)
	} else {
		r = appendEmpty(r, 6)
	}

	// OriginatorOptionF (5)
	if oof := fwm.OriginatorOptionF; oof != nil {
		r = append(r, oof.PartyIdentifier, oof.Name, oof.LineOne, oof.LineTwo, oof.LineThree)
	} else {
		r = appendEmpty(r, 5)
	}

	// OriginatorFI (6)
	if ofi := fwm.OriginatorFI; ofi != nil {
		r = appendFinancialInstitution(r, ofi.FinancialInstitution)
	} else {
		r = appendEmpty(r, 6)
	}

	// InstructingFI (6)
	if ifi := fwm.InstructingFI; ifi != nil {
		r = appendFinancialInstitution(r, ifi.FinancialInstitution)
	} else {
		r = appendEmpty(r, 6)
	}

	// AccountCreditedDrawdown (1)
	if acd := fwm.AccountCreditedDrawdown; acd != nil {
		r = append(r, acd.DrawdownCreditAccountNumber)
	} else {
		r = append(r, "")
	}

	// OriginatorToBeneficiary (4)
	if otb := fwm.OriginatorToBeneficiary; otb != nil {
		r = append(r, otb.LineOne, otb.LineTwo, otb.LineThree, otb.LineFour)
	} else {
		r = appendEmpty(r, 4)
	}

	// FIReceiverFI (6)
	if firf := fwm.FIReceiverFI; firf != nil {
		r = appendFIToFI(r, firf.FIToFI)
	} else {
		r = appendEmpty(r, 6)
	}

	// FIDrawdownDebitAccountAdvice (7)
	if fiddaa := fwm.FIDrawdownDebitAccountAdvice; fiddaa != nil {
		r = appendAdvice(r, fiddaa.Advice)
	} else {
		r = appendEmpty(r, 7)
	}

	// FIIntermediaryFI (6)
	if fiif := fwm.FIIntermediaryFI; fiif != nil {
		r = appendFIToFI(r, fiif.FIToFI)
	} else {
		r = appendEmpty(r, 6)
	}

	// FIIntermediaryFIAdvice (7)
	if fiifa := fwm.FIIntermediaryFIAdvice; fiifa != nil {
		r = appendAdvice(r, fiifa.Advice)
	} else {
		r = appendEmpty(r, 7)
	}

	// FIBeneficiaryFI (6)
	if fibfi := fwm.FIBeneficiaryFI; fibfi != nil {
		r = appendFIToFI(r, fibfi.FIToFI)
	} else {
		r = appendEmpty(r, 6)
	}

	// FIBeneficiaryFIAdvice (7)
	if fibfia := fwm.FIBeneficiaryFIAdvice; fibfia != nil {
		r = appendAdvice(r, fibfia.Advice)
	} else {
		r = appendEmpty(r, 7)
	}

	// FIBeneficiary (6)
	if fib := fwm.FIBeneficiary; fib != nil {
		r = appendFIToFI(r, fib.FIToFI)
	} else {
		r = appendEmpty(r, 6)
	}

	// FIBeneficiaryAdvice (7)
	if fiba := fwm.FIBeneficiaryAdvice; fiba != nil {
		r = appendAdvice(r, fiba.Advice)
	} else {
		r = appendEmpty(r, 7)
	}

	// FIPaymentMethodToBeneficiary (2)
	if fipmtb := fwm.FIPaymentMethodToBeneficiary; fipmtb != nil {
		r = append(r, fipmtb.PaymentMethod, fipmtb.AdditionalInformation)
	} else {
		r = appendEmpty(r, 2)
	}

	// FIAdditionalFIToFI (6)
	if fiafi := fwm.FIAdditionalFIToFI; fiafi != nil {
		f := fiafi.AdditionalFIToFI
		r = append(r, f.LineOne, f.LineTwo, f.LineThree, f.LineFour, f.LineFive, f.LineSix)
	} else {
		r = appendEmpty(r, 6)
	}

	// CurrencyInstructedAmount (2)
	if cia := fwm.CurrencyInstructedAmount; cia != nil {
		r = append(r, cia.SwiftFieldTag, cia.Amount)
	} else {
		r = appendEmpty(r, 2)
	}

	// OrderingCustomer (7)
	if oc := fwm.OrderingCustomer; oc != nil {
		r = appendCoverPayment(r, oc.CoverPayment)
	} else {
		r = appendEmpty(r, 7)
	}

	// OrderingInstitution (7)
	if oi := fwm.OrderingInstitution; oi != nil {
		r = appendCoverPayment(r, oi.CoverPayment)
	} else {
		r = appendEmpty(r, 7)
	}

	// IntermediaryInstitution (7)
	if ii := fwm.IntermediaryInstitution; ii != nil {
		r = appendCoverPayment(r, ii.CoverPayment)
	} else {
		r = appendEmpty(r, 7)
	}

	// InstitutionAccount (7)
	if ia := fwm.InstitutionAccount; ia != nil {
		r = appendCoverPayment(r, ia.CoverPayment)
	} else {
		r = appendEmpty(r, 7)
	}

	// BeneficiaryCustomer (7)
	if bc := fwm.BeneficiaryCustomer; bc != nil {
		r = appendCoverPayment(r, bc.CoverPayment)
	} else {
		r = appendEmpty(r, 7)
	}

	// Remittance (7)
	if rem := fwm.Remittance; rem != nil {
		r = appendCoverPayment(r, rem.CoverPayment)
	} else {
		r = appendEmpty(r, 7)
	}

	// SenderToReceiver (7)
	if str := fwm.SenderToReceiver; str != nil {
		r = appendCoverPayment(r, str.CoverPayment)
	} else {
		r = appendEmpty(r, 7)
	}

	// UnstructuredAddenda (2)
	if ua := fwm.UnstructuredAddenda; ua != nil {
		r = append(r, ua.AddendaLength, ua.Addenda)
	} else {
		r = appendEmpty(r, 2)
	}

	// RelatedRemittance (22)
	if rr := fwm.RelatedRemittance; rr != nil {
		r = append(r, rr.RemittanceIdentification, rr.RemittanceLocationMethod, rr.RemittanceLocationElectronicAddress)
		r = appendRemittanceData(r, rr.RemittanceData)
	} else {
		r = appendEmpty(r, 22)
	}

	// RemittanceOriginator (29)
	if ro := fwm.RemittanceOriginator; ro != nil {
		r = append(r,
			ro.IdentificationType, ro.IdentificationCode,
			ro.IdentificationNumber, ro.IdentificationNumberIssuer,
			ro.ContactName, ro.ContactPhoneNumber,
			ro.ContactMobileNumber, ro.ContactFaxNumber,
			ro.ContactElectronicAddress, ro.ContactOther,
		)
		r = appendRemittanceData(r, ro.RemittanceData)
	} else {
		r = appendEmpty(r, 29)
	}

	// RemittanceBeneficiary (23)
	if rb := fwm.RemittanceBeneficiary; rb != nil {
		r = append(r,
			rb.IdentificationType, rb.IdentificationCode,
			rb.IdentificationNumber, rb.IdentificationNumberIssuer,
		)
		r = appendRemittanceData(r, rb.RemittanceData)
	} else {
		r = appendEmpty(r, 23)
	}

	// PrimaryRemittanceDocument (4)
	if prd := fwm.PrimaryRemittanceDocument; prd != nil {
		r = append(r, prd.DocumentTypeCode, prd.ProprietaryDocumentTypeCode, prd.DocumentIdentificationNumber, prd.Issuer)
	} else {
		r = appendEmpty(r, 4)
	}

	// ActualAmountPaid (2)
	if aap := fwm.ActualAmountPaid; aap != nil {
		r = appendRemittanceAmount(r, aap.RemittanceAmount)
	} else {
		r = appendEmpty(r, 2)
	}

	// GrossAmountRemittanceDocument (2)
	if gard := fwm.GrossAmountRemittanceDocument; gard != nil {
		r = appendRemittanceAmount(r, gard.RemittanceAmount)
	} else {
		r = appendEmpty(r, 2)
	}

	// AmountNegotiatedDiscount (2)
	if and := fwm.AmountNegotiatedDiscount; and != nil {
		r = appendRemittanceAmount(r, and.RemittanceAmount)
	} else {
		r = appendEmpty(r, 2)
	}

	// Adjustment (5)
	if adj := fwm.Adjustment; adj != nil {
		r = append(r, adj.AdjustmentReasonCode, adj.CreditDebitIndicator)
		r = appendRemittanceAmount(r, adj.RemittanceAmount)
		r = append(r, adj.AdditionalInfo)
	} else {
		r = appendEmpty(r, 5)
	}

	// DateRemittanceDocument (1)
	if drd := fwm.DateRemittanceDocument; drd != nil {
		r = append(r, drd.DateRemittanceDocument)
	} else {
		r = append(r, "")
	}

	// SecondaryRemittanceDocument (4)
	if srd := fwm.SecondaryRemittanceDocument; srd != nil {
		r = append(r, srd.DocumentTypeCode, srd.ProprietaryDocumentTypeCode, srd.DocumentIdentificationNumber, srd.Issuer)
	} else {
		r = appendEmpty(r, 4)
	}

	// RemittanceFreeText (3)
	if rft := fwm.RemittanceFreeText; rft != nil {
		r = append(r, rft.LineOne, rft.LineTwo, rft.LineThree)
	} else {
		r = appendEmpty(r, 3)
	}

	// ServiceMessage (12)
	if sm := fwm.ServiceMessage; sm != nil {
		r = append(r,
			sm.LineOne, sm.LineTwo, sm.LineThree, sm.LineFour,
			sm.LineFive, sm.LineSix, sm.LineSeven, sm.LineEight,
			sm.LineNine, sm.LineTen, sm.LineEleven, sm.LineTwelve,
		)
	} else {
		r = appendEmpty(r, 12)
	}

	// MessageDisposition (4)
	if md := fwm.MessageDisposition; md != nil {
		r = append(r, md.FormatVersion, md.TestProductionCode, md.MessageDuplicationCode, md.MessageStatusIndicator)
	} else {
		r = appendEmpty(r, 4)
	}

	// ReceiptTimeStamp (3)
	if rts := fwm.ReceiptTimeStamp; rts != nil {
		r = append(r, rts.ReceiptDate, rts.ReceiptTime, rts.ReceiptApplicationIdentification)
	} else {
		r = appendEmpty(r, 3)
	}

	// OutputMessageAccountabilityData (6)
	if omad := fwm.OutputMessageAccountabilityData; omad != nil {
		r = append(r,
			omad.OutputCycleDate, omad.OutputDestinationID,
			omad.OutputSequenceNumber, omad.OutputDate,
			omad.OutputTime, omad.OutputFRBApplicationIdentification,
		)
	} else {
		r = appendEmpty(r, 6)
	}

	// ErrorWire (3)
	if ew := fwm.ErrorWire; ew != nil {
		r = append(r, ew.ErrorCategory, ew.ErrorCode, ew.ErrorDescription)
	} else {
		r = appendEmpty(r, 3)
	}

	return r
}

// ensureNonNilSubStructs allocates zero-value sub-structs for any nil pointer fields
// in the FEDWireMessage when the TableData record contains at least one non-empty value
// for that section. This allows applyModifications to write values into sections that
// were absent in the original message.
func ensureNonNilSubStructs(fwm *wire.FEDWireMessage, headerIndex map[string]int, record []string) {
	if fwm.SenderSupplied == nil && hasNonEmptyField(headerIndex, record,
		"sender_supplied_format_version", "sender_supplied_user_request_correlation",
		"sender_supplied_test_production_code", "sender_supplied_message_duplication_code") {
		fwm.SenderSupplied = &wire.SenderSupplied{}
	}
	if fwm.TypeSubType == nil && hasNonEmptyField(headerIndex, record,
		"type_code", "sub_type_code") {
		fwm.TypeSubType = &wire.TypeSubType{}
	}
	if fwm.InputMessageAccountabilityData == nil && hasNonEmptyField(headerIndex, record,
		"imad_input_cycle_date", "imad_input_source", "imad_input_sequence_number") {
		fwm.InputMessageAccountabilityData = &wire.InputMessageAccountabilityData{}
	}
	if fwm.Amount == nil && hasNonEmptyField(headerIndex, record, "amount") {
		fwm.Amount = &wire.Amount{}
	}
	if fwm.SenderDepositoryInstitution == nil && hasNonEmptyField(headerIndex, record,
		"sender_di_routing_number", "sender_di_short_name") {
		fwm.SenderDepositoryInstitution = &wire.SenderDepositoryInstitution{}
	}
	if fwm.ReceiverDepositoryInstitution == nil && hasNonEmptyField(headerIndex, record,
		"receiver_di_routing_number", "receiver_di_short_name") {
		fwm.ReceiverDepositoryInstitution = &wire.ReceiverDepositoryInstitution{}
	}
	if fwm.BusinessFunctionCode == nil && hasNonEmptyField(headerIndex, record,
		"business_function_code", "transaction_type_code") {
		fwm.BusinessFunctionCode = &wire.BusinessFunctionCode{}
	}
	if fwm.SenderReference == nil && hasNonEmptyField(headerIndex, record, "sender_reference") {
		fwm.SenderReference = &wire.SenderReference{}
	}
	if fwm.PreviousMessageIdentifier == nil && hasNonEmptyField(headerIndex, record, "previous_message_identifier") {
		fwm.PreviousMessageIdentifier = &wire.PreviousMessageIdentifier{}
	}
	if fwm.LocalInstrument == nil && hasNonEmptyField(headerIndex, record,
		"local_instrument_code", "local_instrument_proprietary_code") {
		fwm.LocalInstrument = &wire.LocalInstrument{}
	}
	if fwm.PaymentNotification == nil && hasNonEmptyField(headerIndex, record,
		"payment_notification_indicator", "payment_notification_electronic_address",
		"payment_notification_contact_name", "payment_notification_contact_phone_number",
		"payment_notification_contact_mobile_number", "payment_notification_contact_fax_number",
		"payment_notification_end_to_end_identification") {
		fwm.PaymentNotification = &wire.PaymentNotification{}
	}
	if fwm.Charges == nil && hasNonEmptyField(headerIndex, record,
		"charges_details", "charges_senders_one", "charges_senders_two",
		"charges_senders_three", "charges_senders_four") {
		fwm.Charges = &wire.Charges{}
	}
	if fwm.InstructedAmount == nil && hasNonEmptyField(headerIndex, record,
		"instructed_amount_currency_code", "instructed_amount_amount") {
		fwm.InstructedAmount = &wire.InstructedAmount{}
	}
	if fwm.ExchangeRate == nil && hasNonEmptyField(headerIndex, record, "exchange_rate") {
		fwm.ExchangeRate = &wire.ExchangeRate{}
	}
	if fwm.BeneficiaryIntermediaryFI == nil && hasNonEmptyField(headerIndex, record, identifiedEntityHeaders("beneficiary_intermediary_fi")...) {
		fwm.BeneficiaryIntermediaryFI = &wire.BeneficiaryIntermediaryFI{}
	}
	if fwm.BeneficiaryFI == nil && hasNonEmptyField(headerIndex, record, identifiedEntityHeaders("beneficiary_fi")...) {
		fwm.BeneficiaryFI = &wire.BeneficiaryFI{}
	}
	if fwm.Beneficiary == nil && hasNonEmptyField(headerIndex, record, identifiedEntityHeaders("beneficiary")...) {
		fwm.Beneficiary = &wire.Beneficiary{}
	}
	if fwm.BeneficiaryReference == nil && hasNonEmptyField(headerIndex, record, "beneficiary_reference") {
		fwm.BeneficiaryReference = &wire.BeneficiaryReference{}
	}
	if fwm.AccountDebitedDrawdown == nil && hasNonEmptyField(headerIndex, record,
		"account_debited_drawdown_id_code", "account_debited_drawdown_identifier",
		"account_debited_drawdown_name", "account_debited_drawdown_address_line_one",
		"account_debited_drawdown_address_line_two", "account_debited_drawdown_address_line_three") {
		fwm.AccountDebitedDrawdown = &wire.AccountDebitedDrawdown{}
	}
	if fwm.Originator == nil && hasNonEmptyField(headerIndex, record, identifiedEntityHeaders("originator")...) {
		fwm.Originator = &wire.Originator{}
	}
	if fwm.OriginatorOptionF == nil && hasNonEmptyField(headerIndex, record,
		"originator_option_f_party_identifier", "originator_option_f_name",
		"originator_option_f_line_one", "originator_option_f_line_two", "originator_option_f_line_three") {
		fwm.OriginatorOptionF = &wire.OriginatorOptionF{}
	}
	if fwm.OriginatorFI == nil && hasNonEmptyField(headerIndex, record, identifiedEntityHeaders("originator_fi")...) {
		fwm.OriginatorFI = &wire.OriginatorFI{}
	}
	if fwm.InstructingFI == nil && hasNonEmptyField(headerIndex, record, identifiedEntityHeaders("instructing_fi")...) {
		fwm.InstructingFI = &wire.InstructingFI{}
	}
	if fwm.AccountCreditedDrawdown == nil && hasNonEmptyField(headerIndex, record, "account_credited_drawdown_number") {
		fwm.AccountCreditedDrawdown = &wire.AccountCreditedDrawdown{}
	}
	if fwm.OriginatorToBeneficiary == nil && hasNonEmptyField(headerIndex, record, lineHeaders("originator_to_beneficiary", 4)...) {
		fwm.OriginatorToBeneficiary = &wire.OriginatorToBeneficiary{}
	}
	if fwm.FIReceiverFI == nil && hasNonEmptyField(headerIndex, record, lineHeaders("fi_receiver_fi", 6)...) {
		fwm.FIReceiverFI = &wire.FIReceiverFI{}
	}
	if fwm.FIDrawdownDebitAccountAdvice == nil && hasNonEmptyField(headerIndex, record, adviceHeaders("fi_drawdown_debit_account_advice")...) {
		fwm.FIDrawdownDebitAccountAdvice = &wire.FIDrawdownDebitAccountAdvice{}
	}
	if fwm.FIIntermediaryFI == nil && hasNonEmptyField(headerIndex, record, lineHeaders("fi_intermediary_fi", 6)...) {
		fwm.FIIntermediaryFI = &wire.FIIntermediaryFI{}
	}
	if fwm.FIIntermediaryFIAdvice == nil && hasNonEmptyField(headerIndex, record, adviceHeaders("fi_intermediary_fi_advice")...) {
		fwm.FIIntermediaryFIAdvice = &wire.FIIntermediaryFIAdvice{}
	}
	if fwm.FIBeneficiaryFI == nil && hasNonEmptyField(headerIndex, record, lineHeaders("fi_beneficiary_fi", 6)...) {
		fwm.FIBeneficiaryFI = &wire.FIBeneficiaryFI{}
	}
	if fwm.FIBeneficiaryFIAdvice == nil && hasNonEmptyField(headerIndex, record, adviceHeaders("fi_beneficiary_fi_advice")...) {
		fwm.FIBeneficiaryFIAdvice = &wire.FIBeneficiaryFIAdvice{}
	}
	if fwm.FIBeneficiary == nil && hasNonEmptyField(headerIndex, record, lineHeaders("fi_beneficiary", 6)...) {
		fwm.FIBeneficiary = &wire.FIBeneficiary{}
	}
	if fwm.FIBeneficiaryAdvice == nil && hasNonEmptyField(headerIndex, record, adviceHeaders("fi_beneficiary_advice")...) {
		fwm.FIBeneficiaryAdvice = &wire.FIBeneficiaryAdvice{}
	}
	if fwm.FIPaymentMethodToBeneficiary == nil && hasNonEmptyField(headerIndex, record,
		"fi_payment_method_to_beneficiary_payment_method", "fi_payment_method_to_beneficiary_additional_information") {
		fwm.FIPaymentMethodToBeneficiary = &wire.FIPaymentMethodToBeneficiary{}
	}
	if fwm.FIAdditionalFIToFI == nil && hasNonEmptyField(headerIndex, record, lineHeaders("fi_additional_fi_to_fi", 6)...) {
		fwm.FIAdditionalFIToFI = &wire.FIAdditionalFIToFI{}
	}
	if fwm.CurrencyInstructedAmount == nil && hasNonEmptyField(headerIndex, record,
		"currency_instructed_amount_swift_field_tag", "currency_instructed_amount_amount") {
		fwm.CurrencyInstructedAmount = &wire.CurrencyInstructedAmount{}
	}
	if fwm.OrderingCustomer == nil && hasNonEmptyField(headerIndex, record, coverPaymentHeaders("ordering_customer")...) {
		fwm.OrderingCustomer = &wire.OrderingCustomer{}
	}
	if fwm.OrderingInstitution == nil && hasNonEmptyField(headerIndex, record, coverPaymentHeaders("ordering_institution")...) {
		fwm.OrderingInstitution = &wire.OrderingInstitution{}
	}
	if fwm.IntermediaryInstitution == nil && hasNonEmptyField(headerIndex, record, coverPaymentHeaders("intermediary_institution")...) {
		fwm.IntermediaryInstitution = &wire.IntermediaryInstitution{}
	}
	if fwm.InstitutionAccount == nil && hasNonEmptyField(headerIndex, record, coverPaymentHeaders("institution_account")...) {
		fwm.InstitutionAccount = &wire.InstitutionAccount{}
	}
	if fwm.BeneficiaryCustomer == nil && hasNonEmptyField(headerIndex, record, coverPaymentHeaders("beneficiary_customer")...) {
		fwm.BeneficiaryCustomer = &wire.BeneficiaryCustomer{}
	}
	if fwm.Remittance == nil && hasNonEmptyField(headerIndex, record, coverPaymentHeaders("remittance")...) {
		fwm.Remittance = &wire.Remittance{}
	}
	if fwm.SenderToReceiver == nil && hasNonEmptyField(headerIndex, record, coverPaymentHeaders("sender_to_receiver")...) {
		fwm.SenderToReceiver = &wire.SenderToReceiver{}
	}
	if fwm.UnstructuredAddenda == nil && hasNonEmptyField(headerIndex, record,
		"unstructured_addenda_length", "unstructured_addenda") {
		fwm.UnstructuredAddenda = &wire.UnstructuredAddenda{}
	}
	if fwm.RelatedRemittance == nil {
		fields := make([]string, 0, 3+len(remittanceDataHeaders("related_remittance")))
		fields = append(fields, "related_remittance_identification", "related_remittance_location_method", "related_remittance_location_electronic_address")
		fields = append(fields, remittanceDataHeaders("related_remittance")...)
		if hasNonEmptyField(headerIndex, record, fields...) {
			fwm.RelatedRemittance = &wire.RelatedRemittance{}
		}
	}
	if fwm.RemittanceOriginator == nil {
		fields := make([]string, 0, 10+len(remittanceDataHeaders("remittance_originator")))
		fields = append(fields,
			"remittance_originator_identification_type", "remittance_originator_identification_code",
			"remittance_originator_identification_number", "remittance_originator_identification_number_issuer",
			"remittance_originator_contact_name", "remittance_originator_contact_phone_number",
			"remittance_originator_contact_mobile_number", "remittance_originator_contact_fax_number",
			"remittance_originator_contact_electronic_address", "remittance_originator_contact_other",
		)
		fields = append(fields, remittanceDataHeaders("remittance_originator")...)
		if hasNonEmptyField(headerIndex, record, fields...) {
			fwm.RemittanceOriginator = &wire.RemittanceOriginator{}
		}
	}
	if fwm.RemittanceBeneficiary == nil {
		fields := make([]string, 0, 4+len(remittanceDataHeaders("remittance_beneficiary")))
		fields = append(fields,
			"remittance_beneficiary_identification_type", "remittance_beneficiary_identification_code",
			"remittance_beneficiary_identification_number", "remittance_beneficiary_identification_number_issuer",
		)
		fields = append(fields, remittanceDataHeaders("remittance_beneficiary")...)
		if hasNonEmptyField(headerIndex, record, fields...) {
			fwm.RemittanceBeneficiary = &wire.RemittanceBeneficiary{}
		}
	}
	if fwm.PrimaryRemittanceDocument == nil && hasNonEmptyField(headerIndex, record, remittanceDocumentHeaders("primary_remittance_document")...) {
		fwm.PrimaryRemittanceDocument = &wire.PrimaryRemittanceDocument{}
	}
	if fwm.ActualAmountPaid == nil && hasNonEmptyField(headerIndex, record, remittanceAmountHeaders("actual_amount_paid")...) {
		fwm.ActualAmountPaid = &wire.ActualAmountPaid{}
	}
	if fwm.GrossAmountRemittanceDocument == nil && hasNonEmptyField(headerIndex, record, remittanceAmountHeaders("gross_amount_remittance_document")...) {
		fwm.GrossAmountRemittanceDocument = &wire.GrossAmountRemittanceDocument{}
	}
	if fwm.AmountNegotiatedDiscount == nil && hasNonEmptyField(headerIndex, record, remittanceAmountHeaders("amount_negotiated_discount")...) {
		fwm.AmountNegotiatedDiscount = &wire.AmountNegotiatedDiscount{}
	}
	if fwm.Adjustment == nil {
		fields := make([]string, 0, 3+len(remittanceAmountHeaders("adjustment")))
		fields = append(fields, "adjustment_reason_code", "adjustment_credit_debit_indicator", "adjustment_additional_info")
		fields = append(fields, remittanceAmountHeaders("adjustment")...)
		if hasNonEmptyField(headerIndex, record, fields...) {
			fwm.Adjustment = &wire.Adjustment{}
		}
	}
	if fwm.DateRemittanceDocument == nil && hasNonEmptyField(headerIndex, record, "date_remittance_document") {
		fwm.DateRemittanceDocument = &wire.DateRemittanceDocument{}
	}
	if fwm.SecondaryRemittanceDocument == nil && hasNonEmptyField(headerIndex, record, remittanceDocumentHeaders("secondary_remittance_document")...) {
		fwm.SecondaryRemittanceDocument = &wire.SecondaryRemittanceDocument{}
	}
	if fwm.RemittanceFreeText == nil && hasNonEmptyField(headerIndex, record, lineHeaders("remittance_free_text", 3)...) {
		fwm.RemittanceFreeText = &wire.RemittanceFreeText{}
	}
	if fwm.ServiceMessage == nil && hasNonEmptyField(headerIndex, record, lineHeaders("service_message", 12)...) {
		fwm.ServiceMessage = &wire.ServiceMessage{}
	}
	if fwm.MessageDisposition == nil && hasNonEmptyField(headerIndex, record,
		"message_disposition_format_version", "message_disposition_test_production_code",
		"message_disposition_message_duplication_code", "message_disposition_message_status_indicator") {
		fwm.MessageDisposition = &wire.MessageDisposition{}
	}
	if fwm.ReceiptTimeStamp == nil && hasNonEmptyField(headerIndex, record,
		"receipt_time_stamp_receipt_date", "receipt_time_stamp_receipt_time",
		"receipt_time_stamp_receipt_application_identification") {
		fwm.ReceiptTimeStamp = &wire.ReceiptTimeStamp{}
	}
	if fwm.OutputMessageAccountabilityData == nil && hasNonEmptyField(headerIndex, record,
		"omad_output_cycle_date", "omad_output_destination_id", "omad_output_sequence_number",
		"omad_output_date", "omad_output_time", "omad_output_frb_application_identification") {
		fwm.OutputMessageAccountabilityData = &wire.OutputMessageAccountabilityData{}
	}
	if fwm.ErrorWire == nil && hasNonEmptyField(headerIndex, record,
		"error_wire_category", "error_wire_code", "error_wire_description") {
		fwm.ErrorWire = &wire.ErrorWire{}
	}
}

// applyModifications writes all field values from the TableData record back to a FEDWireMessage.
// Nil sub-structs are lazily allocated when the record contains non-empty values for their fields.
func applyModifications(fwm *wire.FEDWireMessage, td *parser.TableData) {
	if len(td.Records) == 0 {
		return
	}

	headerIndex := make(map[string]int, len(td.Headers))
	for i, h := range td.Headers {
		headerIndex[h] = i
	}

	record := td.Records[0]

	// Lazily allocate nil sub-structs that have non-empty values in the record.
	ensureNonNilSubStructs(fwm, headerIndex, record)

	// SenderSupplied
	if fwm.SenderSupplied != nil {
		setField(headerIndex, record, "sender_supplied_format_version", &fwm.SenderSupplied.FormatVersion)
		setField(headerIndex, record, "sender_supplied_user_request_correlation", &fwm.SenderSupplied.UserRequestCorrelation)
		setField(headerIndex, record, "sender_supplied_test_production_code", &fwm.SenderSupplied.TestProductionCode)
		setField(headerIndex, record, "sender_supplied_message_duplication_code", &fwm.SenderSupplied.MessageDuplicationCode)
	}

	// TypeSubType
	if fwm.TypeSubType != nil {
		setField(headerIndex, record, "type_code", &fwm.TypeSubType.TypeCode)
		setField(headerIndex, record, "sub_type_code", &fwm.TypeSubType.SubTypeCode)
	}

	// InputMessageAccountabilityData
	if fwm.InputMessageAccountabilityData != nil {
		setField(headerIndex, record, "imad_input_cycle_date", &fwm.InputMessageAccountabilityData.InputCycleDate)
		setField(headerIndex, record, "imad_input_source", &fwm.InputMessageAccountabilityData.InputSource)
		setField(headerIndex, record, "imad_input_sequence_number", &fwm.InputMessageAccountabilityData.InputSequenceNumber)
	}

	// Amount
	if fwm.Amount != nil {
		setField(headerIndex, record, "amount", &fwm.Amount.Amount)
	}

	// SenderDepositoryInstitution
	if fwm.SenderDepositoryInstitution != nil {
		setField(headerIndex, record, "sender_di_routing_number", &fwm.SenderDepositoryInstitution.SenderABANumber)
		setField(headerIndex, record, "sender_di_short_name", &fwm.SenderDepositoryInstitution.SenderShortName)
	}

	// ReceiverDepositoryInstitution
	if fwm.ReceiverDepositoryInstitution != nil {
		setField(headerIndex, record, "receiver_di_routing_number", &fwm.ReceiverDepositoryInstitution.ReceiverABANumber)
		setField(headerIndex, record, "receiver_di_short_name", &fwm.ReceiverDepositoryInstitution.ReceiverShortName)
	}

	// BusinessFunctionCode
	if fwm.BusinessFunctionCode != nil {
		setField(headerIndex, record, "business_function_code", &fwm.BusinessFunctionCode.BusinessFunctionCode)
		setField(headerIndex, record, "transaction_type_code", &fwm.BusinessFunctionCode.TransactionTypeCode)
	}

	// SenderReference
	if fwm.SenderReference != nil {
		setField(headerIndex, record, "sender_reference", &fwm.SenderReference.SenderReference)
	}

	// PreviousMessageIdentifier
	if fwm.PreviousMessageIdentifier != nil {
		setField(headerIndex, record, "previous_message_identifier", &fwm.PreviousMessageIdentifier.PreviousMessageIdentifier)
	}

	// LocalInstrument
	if fwm.LocalInstrument != nil {
		setField(headerIndex, record, "local_instrument_code", &fwm.LocalInstrument.LocalInstrumentCode)
		setField(headerIndex, record, "local_instrument_proprietary_code", &fwm.LocalInstrument.ProprietaryCode)
	}

	// PaymentNotification
	if fwm.PaymentNotification != nil {
		setField(headerIndex, record, "payment_notification_indicator", &fwm.PaymentNotification.PaymentNotificationIndicator)
		setField(headerIndex, record, "payment_notification_electronic_address", &fwm.PaymentNotification.ContactNotificationElectronicAddress)
		setField(headerIndex, record, "payment_notification_contact_name", &fwm.PaymentNotification.ContactName)
		setField(headerIndex, record, "payment_notification_contact_phone_number", &fwm.PaymentNotification.ContactPhoneNumber)
		setField(headerIndex, record, "payment_notification_contact_mobile_number", &fwm.PaymentNotification.ContactMobileNumber)
		setField(headerIndex, record, "payment_notification_contact_fax_number", &fwm.PaymentNotification.ContactFaxNumber)
		setField(headerIndex, record, "payment_notification_end_to_end_identification", &fwm.PaymentNotification.EndToEndIdentification)
	}

	// Charges
	if fwm.Charges != nil {
		setField(headerIndex, record, "charges_details", &fwm.Charges.ChargeDetails)
		setField(headerIndex, record, "charges_senders_one", &fwm.Charges.SendersChargesOne)
		setField(headerIndex, record, "charges_senders_two", &fwm.Charges.SendersChargesTwo)
		setField(headerIndex, record, "charges_senders_three", &fwm.Charges.SendersChargesThree)
		setField(headerIndex, record, "charges_senders_four", &fwm.Charges.SendersChargesFour)
	}

	// InstructedAmount
	if fwm.InstructedAmount != nil {
		setField(headerIndex, record, "instructed_amount_currency_code", &fwm.InstructedAmount.CurrencyCode)
		setField(headerIndex, record, "instructed_amount_amount", &fwm.InstructedAmount.Amount)
	}

	// ExchangeRate
	if fwm.ExchangeRate != nil {
		setField(headerIndex, record, "exchange_rate", &fwm.ExchangeRate.ExchangeRate)
	}

	// BeneficiaryIntermediaryFI
	if fwm.BeneficiaryIntermediaryFI != nil {
		applyFinancialInstitution(headerIndex, record, &fwm.BeneficiaryIntermediaryFI.FinancialInstitution, "beneficiary_intermediary_fi")
	}

	// BeneficiaryFI
	if fwm.BeneficiaryFI != nil {
		applyFinancialInstitution(headerIndex, record, &fwm.BeneficiaryFI.FinancialInstitution, "beneficiary_fi")
	}

	// Beneficiary
	if fwm.Beneficiary != nil {
		applyPersonal(headerIndex, record, &fwm.Beneficiary.Personal, "beneficiary")
	}

	// BeneficiaryReference
	if fwm.BeneficiaryReference != nil {
		setField(headerIndex, record, "beneficiary_reference", &fwm.BeneficiaryReference.BeneficiaryReference)
	}

	// AccountDebitedDrawdown
	if fwm.AccountDebitedDrawdown != nil {
		setField(headerIndex, record, "account_debited_drawdown_id_code", &fwm.AccountDebitedDrawdown.IdentificationCode)
		setField(headerIndex, record, "account_debited_drawdown_identifier", &fwm.AccountDebitedDrawdown.Identifier)
		setField(headerIndex, record, "account_debited_drawdown_name", &fwm.AccountDebitedDrawdown.Name)
		setField(headerIndex, record, "account_debited_drawdown_address_line_one", &fwm.AccountDebitedDrawdown.Address.AddressLineOne)
		setField(headerIndex, record, "account_debited_drawdown_address_line_two", &fwm.AccountDebitedDrawdown.Address.AddressLineTwo)
		setField(headerIndex, record, "account_debited_drawdown_address_line_three", &fwm.AccountDebitedDrawdown.Address.AddressLineThree)
	}

	// Originator
	if fwm.Originator != nil {
		applyPersonal(headerIndex, record, &fwm.Originator.Personal, "originator")
	}

	// OriginatorOptionF
	if fwm.OriginatorOptionF != nil {
		setField(headerIndex, record, "originator_option_f_party_identifier", &fwm.OriginatorOptionF.PartyIdentifier)
		setField(headerIndex, record, "originator_option_f_name", &fwm.OriginatorOptionF.Name)
		setField(headerIndex, record, "originator_option_f_line_one", &fwm.OriginatorOptionF.LineOne)
		setField(headerIndex, record, "originator_option_f_line_two", &fwm.OriginatorOptionF.LineTwo)
		setField(headerIndex, record, "originator_option_f_line_three", &fwm.OriginatorOptionF.LineThree)
	}

	// OriginatorFI
	if fwm.OriginatorFI != nil {
		applyFinancialInstitution(headerIndex, record, &fwm.OriginatorFI.FinancialInstitution, "originator_fi")
	}

	// InstructingFI
	if fwm.InstructingFI != nil {
		applyFinancialInstitution(headerIndex, record, &fwm.InstructingFI.FinancialInstitution, "instructing_fi")
	}

	// AccountCreditedDrawdown
	if fwm.AccountCreditedDrawdown != nil {
		setField(headerIndex, record, "account_credited_drawdown_number", &fwm.AccountCreditedDrawdown.DrawdownCreditAccountNumber)
	}

	// OriginatorToBeneficiary
	if fwm.OriginatorToBeneficiary != nil {
		applyLines(headerIndex, record, "originator_to_beneficiary",
			&fwm.OriginatorToBeneficiary.LineOne, &fwm.OriginatorToBeneficiary.LineTwo,
			&fwm.OriginatorToBeneficiary.LineThree, &fwm.OriginatorToBeneficiary.LineFour)
	}

	// FIReceiverFI
	if fwm.FIReceiverFI != nil {
		applyFIToFI(headerIndex, record, &fwm.FIReceiverFI.FIToFI, "fi_receiver_fi")
	}

	// FIDrawdownDebitAccountAdvice
	if fwm.FIDrawdownDebitAccountAdvice != nil {
		applyAdvice(headerIndex, record, &fwm.FIDrawdownDebitAccountAdvice.Advice, "fi_drawdown_debit_account_advice")
	}

	// FIIntermediaryFI
	if fwm.FIIntermediaryFI != nil {
		applyFIToFI(headerIndex, record, &fwm.FIIntermediaryFI.FIToFI, "fi_intermediary_fi")
	}

	// FIIntermediaryFIAdvice
	if fwm.FIIntermediaryFIAdvice != nil {
		applyAdvice(headerIndex, record, &fwm.FIIntermediaryFIAdvice.Advice, "fi_intermediary_fi_advice")
	}

	// FIBeneficiaryFI
	if fwm.FIBeneficiaryFI != nil {
		applyFIToFI(headerIndex, record, &fwm.FIBeneficiaryFI.FIToFI, "fi_beneficiary_fi")
	}

	// FIBeneficiaryFIAdvice
	if fwm.FIBeneficiaryFIAdvice != nil {
		applyAdvice(headerIndex, record, &fwm.FIBeneficiaryFIAdvice.Advice, "fi_beneficiary_fi_advice")
	}

	// FIBeneficiary
	if fwm.FIBeneficiary != nil {
		applyFIToFI(headerIndex, record, &fwm.FIBeneficiary.FIToFI, "fi_beneficiary")
	}

	// FIBeneficiaryAdvice
	if fwm.FIBeneficiaryAdvice != nil {
		applyAdvice(headerIndex, record, &fwm.FIBeneficiaryAdvice.Advice, "fi_beneficiary_advice")
	}

	// FIPaymentMethodToBeneficiary
	if fwm.FIPaymentMethodToBeneficiary != nil {
		setField(headerIndex, record, "fi_payment_method_to_beneficiary_payment_method", &fwm.FIPaymentMethodToBeneficiary.PaymentMethod)
		setField(headerIndex, record, "fi_payment_method_to_beneficiary_additional_information", &fwm.FIPaymentMethodToBeneficiary.AdditionalInformation)
	}

	// FIAdditionalFIToFI
	if fwm.FIAdditionalFIToFI != nil {
		f := &fwm.FIAdditionalFIToFI.AdditionalFIToFI
		applyLines(headerIndex, record, "fi_additional_fi_to_fi",
			&f.LineOne, &f.LineTwo, &f.LineThree, &f.LineFour, &f.LineFive, &f.LineSix)
	}

	// CurrencyInstructedAmount
	if fwm.CurrencyInstructedAmount != nil {
		setField(headerIndex, record, "currency_instructed_amount_swift_field_tag", &fwm.CurrencyInstructedAmount.SwiftFieldTag)
		setField(headerIndex, record, "currency_instructed_amount_amount", &fwm.CurrencyInstructedAmount.Amount)
	}

	// OrderingCustomer
	if fwm.OrderingCustomer != nil {
		applyCoverPayment(headerIndex, record, &fwm.OrderingCustomer.CoverPayment, "ordering_customer")
	}

	// OrderingInstitution
	if fwm.OrderingInstitution != nil {
		applyCoverPayment(headerIndex, record, &fwm.OrderingInstitution.CoverPayment, "ordering_institution")
	}

	// IntermediaryInstitution
	if fwm.IntermediaryInstitution != nil {
		applyCoverPayment(headerIndex, record, &fwm.IntermediaryInstitution.CoverPayment, "intermediary_institution")
	}

	// InstitutionAccount
	if fwm.InstitutionAccount != nil {
		applyCoverPayment(headerIndex, record, &fwm.InstitutionAccount.CoverPayment, "institution_account")
	}

	// BeneficiaryCustomer
	if fwm.BeneficiaryCustomer != nil {
		applyCoverPayment(headerIndex, record, &fwm.BeneficiaryCustomer.CoverPayment, "beneficiary_customer")
	}

	// Remittance
	if fwm.Remittance != nil {
		applyCoverPayment(headerIndex, record, &fwm.Remittance.CoverPayment, "remittance")
	}

	// SenderToReceiver
	if fwm.SenderToReceiver != nil {
		applyCoverPayment(headerIndex, record, &fwm.SenderToReceiver.CoverPayment, "sender_to_receiver")
	}

	// UnstructuredAddenda
	if fwm.UnstructuredAddenda != nil {
		setField(headerIndex, record, "unstructured_addenda_length", &fwm.UnstructuredAddenda.AddendaLength)
		setField(headerIndex, record, "unstructured_addenda", &fwm.UnstructuredAddenda.Addenda)
	}

	// RelatedRemittance
	if fwm.RelatedRemittance != nil {
		setField(headerIndex, record, "related_remittance_identification", &fwm.RelatedRemittance.RemittanceIdentification)
		setField(headerIndex, record, "related_remittance_location_method", &fwm.RelatedRemittance.RemittanceLocationMethod)
		setField(headerIndex, record, "related_remittance_location_electronic_address", &fwm.RelatedRemittance.RemittanceLocationElectronicAddress)
		applyRemittanceData(headerIndex, record, &fwm.RelatedRemittance.RemittanceData, "related_remittance")
	}

	// RemittanceOriginator
	if fwm.RemittanceOriginator != nil {
		setField(headerIndex, record, "remittance_originator_identification_type", &fwm.RemittanceOriginator.IdentificationType)
		setField(headerIndex, record, "remittance_originator_identification_code", &fwm.RemittanceOriginator.IdentificationCode)
		setField(headerIndex, record, "remittance_originator_identification_number", &fwm.RemittanceOriginator.IdentificationNumber)
		setField(headerIndex, record, "remittance_originator_identification_number_issuer", &fwm.RemittanceOriginator.IdentificationNumberIssuer)
		setField(headerIndex, record, "remittance_originator_contact_name", &fwm.RemittanceOriginator.ContactName)
		setField(headerIndex, record, "remittance_originator_contact_phone_number", &fwm.RemittanceOriginator.ContactPhoneNumber)
		setField(headerIndex, record, "remittance_originator_contact_mobile_number", &fwm.RemittanceOriginator.ContactMobileNumber)
		setField(headerIndex, record, "remittance_originator_contact_fax_number", &fwm.RemittanceOriginator.ContactFaxNumber)
		setField(headerIndex, record, "remittance_originator_contact_electronic_address", &fwm.RemittanceOriginator.ContactElectronicAddress)
		setField(headerIndex, record, "remittance_originator_contact_other", &fwm.RemittanceOriginator.ContactOther)
		applyRemittanceData(headerIndex, record, &fwm.RemittanceOriginator.RemittanceData, "remittance_originator")
	}

	// RemittanceBeneficiary
	if fwm.RemittanceBeneficiary != nil {
		setField(headerIndex, record, "remittance_beneficiary_identification_type", &fwm.RemittanceBeneficiary.IdentificationType)
		setField(headerIndex, record, "remittance_beneficiary_identification_code", &fwm.RemittanceBeneficiary.IdentificationCode)
		setField(headerIndex, record, "remittance_beneficiary_identification_number", &fwm.RemittanceBeneficiary.IdentificationNumber)
		setField(headerIndex, record, "remittance_beneficiary_identification_number_issuer", &fwm.RemittanceBeneficiary.IdentificationNumberIssuer)
		applyRemittanceData(headerIndex, record, &fwm.RemittanceBeneficiary.RemittanceData, "remittance_beneficiary")
	}

	// PrimaryRemittanceDocument
	if fwm.PrimaryRemittanceDocument != nil {
		applyRemittanceDocument(headerIndex, record, fwm.PrimaryRemittanceDocument, "primary_remittance_document")
	}

	// ActualAmountPaid
	if fwm.ActualAmountPaid != nil {
		applyRemittanceAmount(headerIndex, record, &fwm.ActualAmountPaid.RemittanceAmount, "actual_amount_paid")
	}

	// GrossAmountRemittanceDocument
	if fwm.GrossAmountRemittanceDocument != nil {
		applyRemittanceAmount(headerIndex, record, &fwm.GrossAmountRemittanceDocument.RemittanceAmount, "gross_amount_remittance_document")
	}

	// AmountNegotiatedDiscount
	if fwm.AmountNegotiatedDiscount != nil {
		applyRemittanceAmount(headerIndex, record, &fwm.AmountNegotiatedDiscount.RemittanceAmount, "amount_negotiated_discount")
	}

	// Adjustment
	if fwm.Adjustment != nil {
		setField(headerIndex, record, "adjustment_reason_code", &fwm.Adjustment.AdjustmentReasonCode)
		setField(headerIndex, record, "adjustment_credit_debit_indicator", &fwm.Adjustment.CreditDebitIndicator)
		applyRemittanceAmount(headerIndex, record, &fwm.Adjustment.RemittanceAmount, "adjustment")
		setField(headerIndex, record, "adjustment_additional_info", &fwm.Adjustment.AdditionalInfo)
	}

	// DateRemittanceDocument
	if fwm.DateRemittanceDocument != nil {
		setField(headerIndex, record, "date_remittance_document", &fwm.DateRemittanceDocument.DateRemittanceDocument)
	}

	// SecondaryRemittanceDocument
	if fwm.SecondaryRemittanceDocument != nil {
		applyRemittanceDocument(headerIndex, record, fwm.SecondaryRemittanceDocument, "secondary_remittance_document")
	}

	// RemittanceFreeText
	if fwm.RemittanceFreeText != nil {
		applyLines(headerIndex, record, "remittance_free_text",
			&fwm.RemittanceFreeText.LineOne, &fwm.RemittanceFreeText.LineTwo, &fwm.RemittanceFreeText.LineThree)
	}

	// ServiceMessage
	if fwm.ServiceMessage != nil {
		sm := fwm.ServiceMessage
		applyLines(headerIndex, record, "service_message",
			&sm.LineOne, &sm.LineTwo, &sm.LineThree, &sm.LineFour,
			&sm.LineFive, &sm.LineSix, &sm.LineSeven, &sm.LineEight,
			&sm.LineNine, &sm.LineTen, &sm.LineEleven, &sm.LineTwelve)
	}

	// MessageDisposition
	if fwm.MessageDisposition != nil {
		setField(headerIndex, record, "message_disposition_format_version", &fwm.MessageDisposition.FormatVersion)
		setField(headerIndex, record, "message_disposition_test_production_code", &fwm.MessageDisposition.TestProductionCode)
		setField(headerIndex, record, "message_disposition_message_duplication_code", &fwm.MessageDisposition.MessageDuplicationCode)
		setField(headerIndex, record, "message_disposition_message_status_indicator", &fwm.MessageDisposition.MessageStatusIndicator)
	}

	// ReceiptTimeStamp
	if fwm.ReceiptTimeStamp != nil {
		setField(headerIndex, record, "receipt_time_stamp_receipt_date", &fwm.ReceiptTimeStamp.ReceiptDate)
		setField(headerIndex, record, "receipt_time_stamp_receipt_time", &fwm.ReceiptTimeStamp.ReceiptTime)
		setField(headerIndex, record, "receipt_time_stamp_receipt_application_identification", &fwm.ReceiptTimeStamp.ReceiptApplicationIdentification)
	}

	// OutputMessageAccountabilityData
	if fwm.OutputMessageAccountabilityData != nil {
		setField(headerIndex, record, "omad_output_cycle_date", &fwm.OutputMessageAccountabilityData.OutputCycleDate)
		setField(headerIndex, record, "omad_output_destination_id", &fwm.OutputMessageAccountabilityData.OutputDestinationID)
		setField(headerIndex, record, "omad_output_sequence_number", &fwm.OutputMessageAccountabilityData.OutputSequenceNumber)
		setField(headerIndex, record, "omad_output_date", &fwm.OutputMessageAccountabilityData.OutputDate)
		setField(headerIndex, record, "omad_output_time", &fwm.OutputMessageAccountabilityData.OutputTime)
		setField(headerIndex, record, "omad_output_frb_application_identification", &fwm.OutputMessageAccountabilityData.OutputFRBApplicationIdentification)
	}

	// ErrorWire
	if fwm.ErrorWire != nil {
		setField(headerIndex, record, "error_wire_category", &fwm.ErrorWire.ErrorCategory)
		setField(headerIndex, record, "error_wire_code", &fwm.ErrorWire.ErrorCode)
		setField(headerIndex, record, "error_wire_description", &fwm.ErrorWire.ErrorDescription)
	}
}

// --- Header helper functions ---

// identifiedEntityHeaders returns column names for structs with id_code, identifier, name, and 3 address lines.
// Used for FinancialInstitution-based and Personal-based types.
func identifiedEntityHeaders(prefix string) []string {
	return []string{
		prefix + "_id_code",
		prefix + "_identifier",
		prefix + "_name",
		prefix + "_address_line_one",
		prefix + "_address_line_two",
		prefix + "_address_line_three",
	}
}

// lineHeaders returns column names for structs with numbered line fields.
func lineHeaders(prefix string, count int) []string {
	names := []string{
		"_line_one", "_line_two", "_line_three", "_line_four",
		"_line_five", "_line_six", "_line_seven", "_line_eight",
		"_line_nine", "_line_ten", "_line_eleven", "_line_twelve",
	}
	result := make([]string, count)
	for i := range count {
		result[i] = prefix + names[i]
	}
	return result
}

// adviceHeaders returns column names for Advice-based structs (advice_code + 6 lines).
func adviceHeaders(prefix string) []string {
	result := make([]string, 0, 7)
	result = append(result, prefix+"_advice_code")
	result = append(result, lineHeaders(prefix, 6)...)
	return result
}

// coverPaymentHeaders returns column names for CoverPayment-based structs.
func coverPaymentHeaders(prefix string) []string {
	return []string{
		prefix + "_swift_field_tag",
		prefix + "_swift_line_one",
		prefix + "_swift_line_two",
		prefix + "_swift_line_three",
		prefix + "_swift_line_four",
		prefix + "_swift_line_five",
		prefix + "_swift_line_six",
	}
}

// remittanceDataHeaders returns column names for RemittanceData fields.
func remittanceDataHeaders(prefix string) []string {
	return []string{
		prefix + "_name",
		prefix + "_date_birth_place",
		prefix + "_address_type",
		prefix + "_department",
		prefix + "_sub_department",
		prefix + "_street_name",
		prefix + "_building_number",
		prefix + "_post_code",
		prefix + "_town_name",
		prefix + "_country_sub_division_state",
		prefix + "_country",
		prefix + "_address_line_one",
		prefix + "_address_line_two",
		prefix + "_address_line_three",
		prefix + "_address_line_four",
		prefix + "_address_line_five",
		prefix + "_address_line_six",
		prefix + "_address_line_seven",
		prefix + "_country_of_residence",
	}
}

// remittanceAmountHeaders returns column names for RemittanceAmount fields.
func remittanceAmountHeaders(prefix string) []string {
	return []string{
		prefix + "_currency_code",
		prefix + "_amount",
	}
}

// remittanceDocumentHeaders returns column names for remittance document structs.
func remittanceDocumentHeaders(prefix string) []string {
	return []string{
		prefix + "_type_code",
		prefix + "_proprietary_code",
		prefix + "_identification_number",
		prefix + "_issuer",
	}
}

// --- Record append helper functions ---

// appendEmpty appends count empty strings to the slice.
func appendEmpty(r []string, count int) []string {
	for range count {
		r = append(r, "")
	}
	return r
}

// appendFinancialInstitution appends FinancialInstitution fields to the record.
func appendFinancialInstitution(r []string, fi wire.FinancialInstitution) []string {
	return append(r, fi.IdentificationCode, fi.Identifier, fi.Name,
		fi.Address.AddressLineOne, fi.Address.AddressLineTwo, fi.Address.AddressLineThree)
}

// appendPersonal appends Personal fields to the record.
func appendPersonal(r []string, p wire.Personal) []string {
	return append(r, p.IdentificationCode, p.Identifier, p.Name,
		p.Address.AddressLineOne, p.Address.AddressLineTwo, p.Address.AddressLineThree)
}

// appendFIToFI appends FIToFI fields to the record.
func appendFIToFI(r []string, f wire.FIToFI) []string {
	return append(r, f.LineOne, f.LineTwo, f.LineThree, f.LineFour, f.LineFive, f.LineSix)
}

// appendAdvice appends Advice fields to the record.
func appendAdvice(r []string, a wire.Advice) []string {
	return append(r, a.AdviceCode, a.LineOne, a.LineTwo, a.LineThree, a.LineFour, a.LineFive, a.LineSix)
}

// appendCoverPayment appends CoverPayment fields to the record.
func appendCoverPayment(r []string, cp wire.CoverPayment) []string {
	return append(r, cp.SwiftFieldTag, cp.SwiftLineOne, cp.SwiftLineTwo,
		cp.SwiftLineThree, cp.SwiftLineFour, cp.SwiftLineFive, cp.SwiftLineSix)
}

// appendRemittanceData appends RemittanceData fields to the record.
func appendRemittanceData(r []string, rd wire.RemittanceData) []string {
	return append(r,
		rd.Name, rd.DateBirthPlace, rd.AddressType, rd.Department, rd.SubDepartment,
		rd.StreetName, rd.BuildingNumber, rd.PostCode, rd.TownName,
		rd.CountrySubDivisionState, rd.Country,
		rd.AddressLineOne, rd.AddressLineTwo, rd.AddressLineThree, rd.AddressLineFour,
		rd.AddressLineFive, rd.AddressLineSix, rd.AddressLineSeven, rd.CountryOfResidence,
	)
}

// appendRemittanceAmount appends RemittanceAmount fields to the record.
func appendRemittanceAmount(r []string, ra wire.RemittanceAmount) []string {
	return append(r, ra.CurrencyCode, ra.Amount)
}

// --- Modification helper functions ---

// setField sets a string field from the record using the header index.
func setField(headerIndex map[string]int, record []string, name string, target *string) {
	if idx, ok := headerIndex[name]; ok && idx < len(record) {
		*target = record[idx]
	}
}

// hasNonEmptyField returns true if at least one of the named columns has a non-empty value in the record.
func hasNonEmptyField(headerIndex map[string]int, record []string, names ...string) bool {
	for _, name := range names {
		if idx, ok := headerIndex[name]; ok && idx < len(record) && record[idx] != "" {
			return true
		}
	}
	return false
}

// applyFinancialInstitution applies modifications to a FinancialInstitution.
func applyFinancialInstitution(headerIndex map[string]int, record []string, fi *wire.FinancialInstitution, prefix string) {
	setField(headerIndex, record, prefix+"_id_code", &fi.IdentificationCode)
	setField(headerIndex, record, prefix+"_identifier", &fi.Identifier)
	setField(headerIndex, record, prefix+"_name", &fi.Name)
	setField(headerIndex, record, prefix+"_address_line_one", &fi.Address.AddressLineOne)
	setField(headerIndex, record, prefix+"_address_line_two", &fi.Address.AddressLineTwo)
	setField(headerIndex, record, prefix+"_address_line_three", &fi.Address.AddressLineThree)
}

// applyPersonal applies modifications to a Personal.
func applyPersonal(headerIndex map[string]int, record []string, p *wire.Personal, prefix string) {
	setField(headerIndex, record, prefix+"_id_code", &p.IdentificationCode)
	setField(headerIndex, record, prefix+"_identifier", &p.Identifier)
	setField(headerIndex, record, prefix+"_name", &p.Name)
	setField(headerIndex, record, prefix+"_address_line_one", &p.Address.AddressLineOne)
	setField(headerIndex, record, prefix+"_address_line_two", &p.Address.AddressLineTwo)
	setField(headerIndex, record, prefix+"_address_line_three", &p.Address.AddressLineThree)
}

// applyFIToFI applies modifications to a FIToFI.
func applyFIToFI(headerIndex map[string]int, record []string, f *wire.FIToFI, prefix string) {
	applyLines(headerIndex, record, prefix,
		&f.LineOne, &f.LineTwo, &f.LineThree, &f.LineFour, &f.LineFive, &f.LineSix)
}

// applyAdvice applies modifications to an Advice.
func applyAdvice(headerIndex map[string]int, record []string, a *wire.Advice, prefix string) {
	setField(headerIndex, record, prefix+"_advice_code", &a.AdviceCode)
	applyLines(headerIndex, record, prefix,
		&a.LineOne, &a.LineTwo, &a.LineThree, &a.LineFour, &a.LineFive, &a.LineSix)
}

// applyCoverPayment applies modifications to a CoverPayment.
func applyCoverPayment(headerIndex map[string]int, record []string, cp *wire.CoverPayment, prefix string) {
	setField(headerIndex, record, prefix+"_swift_field_tag", &cp.SwiftFieldTag)
	setField(headerIndex, record, prefix+"_swift_line_one", &cp.SwiftLineOne)
	setField(headerIndex, record, prefix+"_swift_line_two", &cp.SwiftLineTwo)
	setField(headerIndex, record, prefix+"_swift_line_three", &cp.SwiftLineThree)
	setField(headerIndex, record, prefix+"_swift_line_four", &cp.SwiftLineFour)
	setField(headerIndex, record, prefix+"_swift_line_five", &cp.SwiftLineFive)
	setField(headerIndex, record, prefix+"_swift_line_six", &cp.SwiftLineSix)
}

// applyRemittanceData applies modifications to a RemittanceData.
func applyRemittanceData(headerIndex map[string]int, record []string, rd *wire.RemittanceData, prefix string) {
	setField(headerIndex, record, prefix+"_name", &rd.Name)
	setField(headerIndex, record, prefix+"_date_birth_place", &rd.DateBirthPlace)
	setField(headerIndex, record, prefix+"_address_type", &rd.AddressType)
	setField(headerIndex, record, prefix+"_department", &rd.Department)
	setField(headerIndex, record, prefix+"_sub_department", &rd.SubDepartment)
	setField(headerIndex, record, prefix+"_street_name", &rd.StreetName)
	setField(headerIndex, record, prefix+"_building_number", &rd.BuildingNumber)
	setField(headerIndex, record, prefix+"_post_code", &rd.PostCode)
	setField(headerIndex, record, prefix+"_town_name", &rd.TownName)
	setField(headerIndex, record, prefix+"_country_sub_division_state", &rd.CountrySubDivisionState)
	setField(headerIndex, record, prefix+"_country", &rd.Country)
	setField(headerIndex, record, prefix+"_address_line_one", &rd.AddressLineOne)
	setField(headerIndex, record, prefix+"_address_line_two", &rd.AddressLineTwo)
	setField(headerIndex, record, prefix+"_address_line_three", &rd.AddressLineThree)
	setField(headerIndex, record, prefix+"_address_line_four", &rd.AddressLineFour)
	setField(headerIndex, record, prefix+"_address_line_five", &rd.AddressLineFive)
	setField(headerIndex, record, prefix+"_address_line_six", &rd.AddressLineSix)
	setField(headerIndex, record, prefix+"_address_line_seven", &rd.AddressLineSeven)
	setField(headerIndex, record, prefix+"_country_of_residence", &rd.CountryOfResidence)
}

// applyRemittanceAmount applies modifications to a RemittanceAmount.
func applyRemittanceAmount(headerIndex map[string]int, record []string, ra *wire.RemittanceAmount, prefix string) {
	setField(headerIndex, record, prefix+"_currency_code", &ra.CurrencyCode)
	setField(headerIndex, record, prefix+"_amount", &ra.Amount)
}

// applyRemittanceDocument applies modifications to a remittance document struct.
// Works with both PrimaryRemittanceDocument and SecondaryRemittanceDocument
// since they share the same field layout.
func applyRemittanceDocument(headerIndex map[string]int, record []string, doc interface{}, prefix string) {
	switch d := doc.(type) {
	case *wire.PrimaryRemittanceDocument:
		setField(headerIndex, record, prefix+"_type_code", &d.DocumentTypeCode)
		setField(headerIndex, record, prefix+"_proprietary_code", &d.ProprietaryDocumentTypeCode)
		setField(headerIndex, record, prefix+"_identification_number", &d.DocumentIdentificationNumber)
		setField(headerIndex, record, prefix+"_issuer", &d.Issuer)
	case *wire.SecondaryRemittanceDocument:
		setField(headerIndex, record, prefix+"_type_code", &d.DocumentTypeCode)
		setField(headerIndex, record, prefix+"_proprietary_code", &d.ProprietaryDocumentTypeCode)
		setField(headerIndex, record, prefix+"_identification_number", &d.DocumentIdentificationNumber)
		setField(headerIndex, record, prefix+"_issuer", &d.Issuer)
	}
}

// applyLines applies numbered line modifications using the standard naming convention.
func applyLines(headerIndex map[string]int, record []string, prefix string, targets ...*string) {
	names := []string{
		"_line_one", "_line_two", "_line_three", "_line_four",
		"_line_five", "_line_six", "_line_seven", "_line_eight",
		"_line_nine", "_line_ten", "_line_eleven", "_line_twelve",
	}
	for i, target := range targets {
		if i < len(names) {
			setField(headerIndex, record, prefix+names[i], target)
		}
	}
}

// --- Deep copy ---

// deepCopyFile creates a deep copy of a wire.File.
// All pointer fields in FEDWireMessage are individually copied to avoid shared state.
func deepCopyFile(src *wire.File) wire.File {
	dst := wire.File{
		ID: src.ID,
	}
	fwm := &src.FEDWireMessage
	d := &dst.FEDWireMessage

	// Copy the ID field
	d.ID = fwm.ID

	if fwm.MessageDisposition != nil {
		v := *fwm.MessageDisposition
		d.MessageDisposition = &v
	}
	if fwm.ReceiptTimeStamp != nil {
		v := *fwm.ReceiptTimeStamp
		d.ReceiptTimeStamp = &v
	}
	if fwm.OutputMessageAccountabilityData != nil {
		v := *fwm.OutputMessageAccountabilityData
		d.OutputMessageAccountabilityData = &v
	}
	if fwm.ErrorWire != nil {
		v := *fwm.ErrorWire
		d.ErrorWire = &v
	}
	if fwm.SenderSupplied != nil {
		v := *fwm.SenderSupplied
		d.SenderSupplied = &v
	}
	if fwm.TypeSubType != nil {
		v := *fwm.TypeSubType
		d.TypeSubType = &v
	}
	if fwm.InputMessageAccountabilityData != nil {
		v := *fwm.InputMessageAccountabilityData
		d.InputMessageAccountabilityData = &v
	}
	if fwm.Amount != nil {
		v := *fwm.Amount
		d.Amount = &v
	}
	if fwm.SenderDepositoryInstitution != nil {
		v := *fwm.SenderDepositoryInstitution
		d.SenderDepositoryInstitution = &v
	}
	if fwm.ReceiverDepositoryInstitution != nil {
		v := *fwm.ReceiverDepositoryInstitution
		d.ReceiverDepositoryInstitution = &v
	}
	if fwm.BusinessFunctionCode != nil {
		v := *fwm.BusinessFunctionCode
		d.BusinessFunctionCode = &v
	}
	if fwm.SenderReference != nil {
		v := *fwm.SenderReference
		d.SenderReference = &v
	}
	if fwm.PreviousMessageIdentifier != nil {
		v := *fwm.PreviousMessageIdentifier
		d.PreviousMessageIdentifier = &v
	}
	if fwm.LocalInstrument != nil {
		v := *fwm.LocalInstrument
		d.LocalInstrument = &v
	}
	if fwm.PaymentNotification != nil {
		v := *fwm.PaymentNotification
		d.PaymentNotification = &v
	}
	if fwm.Charges != nil {
		v := *fwm.Charges
		d.Charges = &v
	}
	if fwm.InstructedAmount != nil {
		v := *fwm.InstructedAmount
		d.InstructedAmount = &v
	}
	if fwm.ExchangeRate != nil {
		v := *fwm.ExchangeRate
		d.ExchangeRate = &v
	}
	if fwm.BeneficiaryIntermediaryFI != nil {
		v := *fwm.BeneficiaryIntermediaryFI
		d.BeneficiaryIntermediaryFI = &v
	}
	if fwm.BeneficiaryFI != nil {
		v := *fwm.BeneficiaryFI
		d.BeneficiaryFI = &v
	}
	if fwm.Beneficiary != nil {
		v := *fwm.Beneficiary
		d.Beneficiary = &v
	}
	if fwm.BeneficiaryReference != nil {
		v := *fwm.BeneficiaryReference
		d.BeneficiaryReference = &v
	}
	if fwm.AccountDebitedDrawdown != nil {
		v := *fwm.AccountDebitedDrawdown
		d.AccountDebitedDrawdown = &v
	}
	if fwm.Originator != nil {
		v := *fwm.Originator
		d.Originator = &v
	}
	if fwm.OriginatorOptionF != nil {
		v := *fwm.OriginatorOptionF
		d.OriginatorOptionF = &v
	}
	if fwm.OriginatorFI != nil {
		v := *fwm.OriginatorFI
		d.OriginatorFI = &v
	}
	if fwm.InstructingFI != nil {
		v := *fwm.InstructingFI
		d.InstructingFI = &v
	}
	if fwm.AccountCreditedDrawdown != nil {
		v := *fwm.AccountCreditedDrawdown
		d.AccountCreditedDrawdown = &v
	}
	if fwm.OriginatorToBeneficiary != nil {
		v := *fwm.OriginatorToBeneficiary
		d.OriginatorToBeneficiary = &v
	}
	if fwm.FIReceiverFI != nil {
		v := *fwm.FIReceiverFI
		d.FIReceiverFI = &v
	}
	if fwm.FIDrawdownDebitAccountAdvice != nil {
		v := *fwm.FIDrawdownDebitAccountAdvice
		d.FIDrawdownDebitAccountAdvice = &v
	}
	if fwm.FIIntermediaryFI != nil {
		v := *fwm.FIIntermediaryFI
		d.FIIntermediaryFI = &v
	}
	if fwm.FIIntermediaryFIAdvice != nil {
		v := *fwm.FIIntermediaryFIAdvice
		d.FIIntermediaryFIAdvice = &v
	}
	if fwm.FIBeneficiaryFI != nil {
		v := *fwm.FIBeneficiaryFI
		d.FIBeneficiaryFI = &v
	}
	if fwm.FIBeneficiaryFIAdvice != nil {
		v := *fwm.FIBeneficiaryFIAdvice
		d.FIBeneficiaryFIAdvice = &v
	}
	if fwm.FIBeneficiary != nil {
		v := *fwm.FIBeneficiary
		d.FIBeneficiary = &v
	}
	if fwm.FIBeneficiaryAdvice != nil {
		v := *fwm.FIBeneficiaryAdvice
		d.FIBeneficiaryAdvice = &v
	}
	if fwm.FIPaymentMethodToBeneficiary != nil {
		v := *fwm.FIPaymentMethodToBeneficiary
		d.FIPaymentMethodToBeneficiary = &v
	}
	if fwm.FIAdditionalFIToFI != nil {
		v := *fwm.FIAdditionalFIToFI
		d.FIAdditionalFIToFI = &v
	}
	if fwm.CurrencyInstructedAmount != nil {
		v := *fwm.CurrencyInstructedAmount
		d.CurrencyInstructedAmount = &v
	}
	if fwm.OrderingCustomer != nil {
		v := *fwm.OrderingCustomer
		d.OrderingCustomer = &v
	}
	if fwm.OrderingInstitution != nil {
		v := *fwm.OrderingInstitution
		d.OrderingInstitution = &v
	}
	if fwm.IntermediaryInstitution != nil {
		v := *fwm.IntermediaryInstitution
		d.IntermediaryInstitution = &v
	}
	if fwm.InstitutionAccount != nil {
		v := *fwm.InstitutionAccount
		d.InstitutionAccount = &v
	}
	if fwm.BeneficiaryCustomer != nil {
		v := *fwm.BeneficiaryCustomer
		d.BeneficiaryCustomer = &v
	}
	if fwm.Remittance != nil {
		v := *fwm.Remittance
		d.Remittance = &v
	}
	if fwm.SenderToReceiver != nil {
		v := *fwm.SenderToReceiver
		d.SenderToReceiver = &v
	}
	if fwm.UnstructuredAddenda != nil {
		v := *fwm.UnstructuredAddenda
		d.UnstructuredAddenda = &v
	}
	if fwm.RelatedRemittance != nil {
		v := *fwm.RelatedRemittance
		d.RelatedRemittance = &v
	}
	if fwm.RemittanceOriginator != nil {
		v := *fwm.RemittanceOriginator
		d.RemittanceOriginator = &v
	}
	if fwm.RemittanceBeneficiary != nil {
		v := *fwm.RemittanceBeneficiary
		d.RemittanceBeneficiary = &v
	}
	if fwm.PrimaryRemittanceDocument != nil {
		v := *fwm.PrimaryRemittanceDocument
		d.PrimaryRemittanceDocument = &v
	}
	if fwm.ActualAmountPaid != nil {
		v := *fwm.ActualAmountPaid
		d.ActualAmountPaid = &v
	}
	if fwm.GrossAmountRemittanceDocument != nil {
		v := *fwm.GrossAmountRemittanceDocument
		d.GrossAmountRemittanceDocument = &v
	}
	if fwm.AmountNegotiatedDiscount != nil {
		v := *fwm.AmountNegotiatedDiscount
		d.AmountNegotiatedDiscount = &v
	}
	if fwm.Adjustment != nil {
		v := *fwm.Adjustment
		d.Adjustment = &v
	}
	if fwm.DateRemittanceDocument != nil {
		v := *fwm.DateRemittanceDocument
		d.DateRemittanceDocument = &v
	}
	if fwm.SecondaryRemittanceDocument != nil {
		v := *fwm.SecondaryRemittanceDocument
		d.SecondaryRemittanceDocument = &v
	}
	if fwm.RemittanceFreeText != nil {
		v := *fwm.RemittanceFreeText
		d.RemittanceFreeText = &v
	}
	if fwm.ServiceMessage != nil {
		v := *fwm.ServiceMessage
		d.ServiceMessage = &v
	}
	if fwm.ValidateOptions != nil {
		v := *fwm.ValidateOptions
		d.ValidateOptions = &v
	}

	return dst
}
