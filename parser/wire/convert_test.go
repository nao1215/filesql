package wire

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/moov-io/wire"
	"github.com/nao1215/filesql/parser"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFromFile_NilFile(t *testing.T) {
	t.Parallel()
	ts := fromFile(nil)
	assert.Nil(t, ts)
}

func TestParseReader_Nil(t *testing.T) {
	t.Parallel()
	ts, err := ParseReader(nil)
	assert.Nil(t, ts)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "reader must not be nil")
}

func TestWriteToWriter_NilReceiver(t *testing.T) {
	t.Parallel()
	var ts *TableSet
	err := ts.WriteToWriter(&bytes.Buffer{})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "TableSet must not be nil")
}

func TestWriteToWriter_NilWriter(t *testing.T) {
	t.Parallel()
	file := readTestFile(t, filepath.Join("testdata", "fedWireMessage-CustomerTransfer.fed"))
	ts := fromFile(file)
	err := ts.WriteToWriter(nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "writer must not be nil")
}

func TestFromFile_CustomerTransfer(t *testing.T) {
	t.Parallel()

	file := readTestFile(t, filepath.Join("testdata", "fedWireMessage-CustomerTransfer.fed"))
	ts := fromFile(file)

	require.NotNil(t, ts)
	require.NotNil(t, ts.Message)
	assert.Len(t, ts.Message.Records, 1, "should have exactly 1 row")

	headers := ts.Message.Headers
	record := ts.Message.Records[0]
	assert.Equal(t, len(headers), len(record), "header count should match record column count")

	// Verify all column types are TEXT
	for i, ct := range ts.Message.ColumnTypes {
		assert.Equal(t, parser.TypeText, ct, "column %d (%s) should be TypeText", i, headers[i])
	}

	// Verify mandatory fields are populated
	headerIndex := buildHeaderIndex(headers)

	assertField(t, record, headerIndex, "sender_supplied_format_version", "30")
	assertField(t, record, headerIndex, "type_code", "10")
	assertField(t, record, headerIndex, "sub_type_code", "00")
	assertField(t, record, headerIndex, "imad_input_cycle_date", "20190410")
	assertField(t, record, headerIndex, "imad_input_source", "Source08")
	assertField(t, record, headerIndex, "imad_input_sequence_number", "000001")
	assertField(t, record, headerIndex, "amount", "000001234567")
	assertField(t, record, headerIndex, "sender_di_routing_number", "121042882")
	assertFieldContains(t, record, headerIndex, "sender_di_short_name", "Wells Fargo")
	assertField(t, record, headerIndex, "receiver_di_routing_number", "231380104")
	assertFieldContains(t, record, headerIndex, "receiver_di_short_name", "Citadel")
	assertField(t, record, headerIndex, "business_function_code", "CTR")

	// Verify optional fields
	assertFieldContains(t, record, headerIndex, "sender_reference", "Sender Reference")
	assertFieldContains(t, record, headerIndex, "previous_message_identifier", "Previous Message Ident")

	// Verify Beneficiary fields
	assertFieldNonEmpty(t, record, headerIndex, "beneficiary_id_code")
	assertFieldNonEmpty(t, record, headerIndex, "beneficiary_name")

	// Verify Originator fields
	assertFieldNonEmpty(t, record, headerIndex, "originator_id_code")
	assertFieldNonEmpty(t, record, headerIndex, "originator_name")

	// Verify OriginatorToBeneficiary lines
	assertFieldNonEmpty(t, record, headerIndex, "originator_to_beneficiary_line_one")
}

func TestFromFile_BankTransfer(t *testing.T) {
	t.Parallel()

	file := readTestFile(t, filepath.Join("testdata", "fedWireMessage-BankTransfer.fed"))
	ts := fromFile(file)

	require.NotNil(t, ts)
	require.NotNil(t, ts.Message)

	headerIndex := buildHeaderIndex(ts.Message.Headers)
	record := ts.Message.Records[0]

	assertField(t, record, headerIndex, "business_function_code", "BTR")
}

func TestFromFile_CustomerTransferPlusCOVS(t *testing.T) {
	t.Parallel()

	file := readTestFile(t, filepath.Join("testdata", "fedWireMessage-CustomerTransferPlusCOVS.fed"))
	ts := fromFile(file)

	require.NotNil(t, ts)
	require.NotNil(t, ts.Message)

	headerIndex := buildHeaderIndex(ts.Message.Headers)
	record := ts.Message.Records[0]

	assertField(t, record, headerIndex, "business_function_code", "CTP")

	// COVS messages should have CoverPayment fields
	assertFieldNonEmpty(t, record, headerIndex, "ordering_customer_swift_field_tag")
}

func TestMessageHeadersConsistency(t *testing.T) {
	t.Parallel()

	headers := messageHeaders()

	// Verify no duplicate headers
	seen := make(map[string]bool, len(headers))
	for _, h := range headers {
		assert.False(t, seen[h], "duplicate header found: %s", h)
		seen[h] = true
	}

	// Verify all headers are non-empty
	for i, h := range headers {
		assert.NotEmpty(t, h, "header at index %d is empty", i)
	}
}

func TestMessageRecordLength(t *testing.T) {
	t.Parallel()

	headers := messageHeaders()

	// Test with empty FEDWireMessage
	fwm := &wire.FEDWireMessage{}
	record := messageRecord(fwm)
	assert.Equal(t, len(headers), len(record), "record length should match header count for empty message")

	// Test with populated file
	file := readTestFile(t, filepath.Join("testdata", "fedWireMessage-CustomerTransfer.fed"))
	record = messageRecord(&file.FEDWireMessage)
	assert.Equal(t, len(headers), len(record), "record length should match header count for populated message")
}

func TestToFile_NilTableSet(t *testing.T) {
	t.Parallel()

	var ts *TableSet
	_, err := ts.toFile()
	assert.Error(t, err)
}

func TestToFile_NoOriginal(t *testing.T) {
	t.Parallel()

	ts := &TableSet{}
	_, err := ts.toFile()
	assert.Error(t, err)
}

func TestRoundTrip_CustomerTransfer(t *testing.T) {
	t.Parallel()

	file := readTestFile(t, filepath.Join("testdata", "fedWireMessage-CustomerTransfer.fed"))
	ts := fromFile(file)
	require.NotNil(t, ts)

	// Convert back to wire.File
	result, err := ts.toFile()
	require.NoError(t, err)
	require.NotNil(t, result)

	// Verify key fields are preserved
	fwm := &result.FEDWireMessage

	require.NotNil(t, fwm.SenderSupplied)
	assert.Equal(t, file.FEDWireMessage.SenderSupplied.FormatVersion, fwm.SenderSupplied.FormatVersion)

	require.NotNil(t, fwm.TypeSubType)
	assert.Equal(t, file.FEDWireMessage.TypeSubType.TypeCode, fwm.TypeSubType.TypeCode)
	assert.Equal(t, file.FEDWireMessage.TypeSubType.SubTypeCode, fwm.TypeSubType.SubTypeCode)

	require.NotNil(t, fwm.Amount)
	assert.Equal(t, file.FEDWireMessage.Amount.Amount, fwm.Amount.Amount)

	require.NotNil(t, fwm.SenderDepositoryInstitution)
	assert.Equal(t, file.FEDWireMessage.SenderDepositoryInstitution.SenderABANumber, fwm.SenderDepositoryInstitution.SenderABANumber)

	require.NotNil(t, fwm.ReceiverDepositoryInstitution)
	assert.Equal(t, file.FEDWireMessage.ReceiverDepositoryInstitution.ReceiverABANumber, fwm.ReceiverDepositoryInstitution.ReceiverABANumber)

	require.NotNil(t, fwm.BusinessFunctionCode)
	assert.Equal(t, "CTR", fwm.BusinessFunctionCode.BusinessFunctionCode)
}

func TestRoundTrip_Modification(t *testing.T) {
	t.Parallel()

	file := readTestFile(t, filepath.Join("testdata", "fedWireMessage-CustomerTransfer.fed"))
	ts := fromFile(file)
	require.NotNil(t, ts)

	// Modify the amount via TableData
	headerIndex := buildHeaderIndex(ts.Message.Headers)
	amountIdx, ok := headerIndex["amount"]
	require.True(t, ok)

	originalAmount := ts.Message.Records[0][amountIdx]
	newAmount := "000009999999"
	ts.Message.Records[0][amountIdx] = newAmount

	// Convert back
	result, err := ts.toFile()
	require.NoError(t, err)

	// Verify modification was applied
	assert.Equal(t, newAmount, result.FEDWireMessage.Amount.Amount)
	assert.NotEqual(t, originalAmount, result.FEDWireMessage.Amount.Amount)

	// Verify original file was not modified (deep copy)
	assert.Equal(t, originalAmount, file.FEDWireMessage.Amount.Amount)
}

func TestParseReader(t *testing.T) {
	t.Parallel()

	data, err := os.ReadFile(filepath.Join("testdata", "fedWireMessage-CustomerTransfer.fed"))
	require.NoError(t, err)

	ts, err := ParseReader(bytes.NewReader(data))
	require.NoError(t, err)
	require.NotNil(t, ts)
	require.NotNil(t, ts.Message)
	assert.Len(t, ts.Message.Records, 1)

	headerIndex := buildHeaderIndex(ts.Message.Headers)
	assertField(t, ts.Message.Records[0], headerIndex, "business_function_code", "CTR")
}

func TestParseReader_InvalidData(t *testing.T) {
	t.Parallel()

	_, err := ParseReader(strings.NewReader("invalid wire data"))
	assert.Error(t, err)
}

func TestWriteToWriter(t *testing.T) {
	t.Parallel()

	file := readTestFile(t, filepath.Join("testdata", "fedWireMessage-CustomerTransfer.fed"))
	ts := fromFile(file)
	require.NotNil(t, ts)

	var buf bytes.Buffer
	err := ts.WriteToWriter(&buf)
	require.NoError(t, err)
	assert.NotEmpty(t, buf.String())

	// Verify the output can be re-parsed
	ts2, err := ParseReader(bytes.NewReader(buf.Bytes()))
	require.NoError(t, err)
	require.NotNil(t, ts2)

	headerIndex := buildHeaderIndex(ts2.Message.Headers)
	assertField(t, ts2.Message.Records[0], headerIndex, "business_function_code", "CTR")
	assertField(t, ts2.Message.Records[0], headerIndex, "amount", "000001234567")
}

func TestGetMessageTable(t *testing.T) {
	t.Parallel()

	// Nil TableSet
	var ts *TableSet
	assert.Nil(t, ts.GetMessageTable())

	// Valid TableSet
	file := readTestFile(t, filepath.Join("testdata", "fedWireMessage-CustomerTransfer.fed"))
	ts = fromFile(file)
	table := ts.GetMessageTable()
	require.NotNil(t, table)
	assert.Equal(t, ts.Message, table)
}

func TestUpdateMessageFromTableData(t *testing.T) {
	t.Parallel()

	file := readTestFile(t, filepath.Join("testdata", "fedWireMessage-CustomerTransfer.fed"))
	ts := fromFile(file)
	require.NotNil(t, ts)

	newTD := &parser.TableData{
		Headers: []string{"amount"},
		Records: [][]string{{"000000000001"}},
	}
	ts.UpdateMessageFromTableData(newTD)
	assert.Equal(t, newTD, ts.Message)
}

func TestDeepCopyFile(t *testing.T) {
	t.Parallel()

	file := readTestFile(t, filepath.Join("testdata", "fedWireMessage-CustomerTransfer.fed"))

	copied := deepCopyFile(file)

	// Verify fields are equal
	assert.Equal(t, file.ID, copied.ID)
	assert.Equal(t, file.FEDWireMessage.Amount.Amount, copied.FEDWireMessage.Amount.Amount)

	// Verify they are independent (modifying copy doesn't affect original)
	copied.FEDWireMessage.Amount.Amount = "MODIFIED"
	assert.NotEqual(t, file.FEDWireMessage.Amount.Amount, copied.FEDWireMessage.Amount.Amount)
}

func TestDeepCopyFile_NilFields(t *testing.T) {
	t.Parallel()

	file := &wire.File{
		ID: "test",
		FEDWireMessage: wire.FEDWireMessage{
			SenderSupplied: &wire.SenderSupplied{
				FormatVersion: "30",
			},
		},
	}

	copied := deepCopyFile(file)
	assert.Equal(t, "30", copied.FEDWireMessage.SenderSupplied.FormatVersion)
	assert.Nil(t, copied.FEDWireMessage.TypeSubType)
	assert.Nil(t, copied.FEDWireMessage.Amount)
	assert.Nil(t, copied.FEDWireMessage.Beneficiary)
}

func TestHelperHeaders(t *testing.T) {
	t.Parallel()

	t.Run("identifiedEntityHeaders", func(t *testing.T) {
		t.Parallel()
		h := identifiedEntityHeaders("test")
		assert.Len(t, h, 6)
		assert.Equal(t, "test_id_code", h[0])
		assert.Equal(t, "test_address_line_three", h[5])
	})

	t.Run("lineHeaders", func(t *testing.T) {
		t.Parallel()
		h := lineHeaders("test", 4)
		assert.Len(t, h, 4)
		assert.Equal(t, "test_line_one", h[0])
		assert.Equal(t, "test_line_four", h[3])
	})

	t.Run("adviceHeaders", func(t *testing.T) {
		t.Parallel()
		h := adviceHeaders("test")
		assert.Len(t, h, 7)
		assert.Equal(t, "test_advice_code", h[0])
		assert.Equal(t, "test_line_six", h[6])
	})

	t.Run("coverPaymentHeaders", func(t *testing.T) {
		t.Parallel()
		h := coverPaymentHeaders("test")
		assert.Len(t, h, 7)
		assert.Equal(t, "test_swift_field_tag", h[0])
		assert.Equal(t, "test_swift_line_six", h[6])
	})

	t.Run("remittanceDataHeaders", func(t *testing.T) {
		t.Parallel()
		h := remittanceDataHeaders("test")
		assert.Len(t, h, 19)
		assert.Equal(t, "test_name", h[0])
		assert.Equal(t, "test_country_of_residence", h[18])
	})

	t.Run("remittanceAmountHeaders", func(t *testing.T) {
		t.Parallel()
		h := remittanceAmountHeaders("test")
		assert.Len(t, h, 2)
		assert.Equal(t, "test_currency_code", h[0])
		assert.Equal(t, "test_amount", h[1])
	})

	t.Run("remittanceDocumentHeaders", func(t *testing.T) {
		t.Parallel()
		h := remittanceDocumentHeaders("test")
		assert.Len(t, h, 4)
		assert.Equal(t, "test_type_code", h[0])
		assert.Equal(t, "test_issuer", h[3])
	})
}

func TestAppendEmpty(t *testing.T) {
	t.Parallel()

	var r []string
	r = appendEmpty(r, 3)
	assert.Equal(t, []string{"", "", ""}, r)
}

func TestSetField(t *testing.T) {
	t.Parallel()

	headerIndex := map[string]int{"amount": 0, "name": 1}
	record := []string{"100", "Test"}

	var target string
	setField(headerIndex, record, "amount", &target)
	assert.Equal(t, "100", target)

	// Missing field
	var missing string
	setField(headerIndex, record, "nonexistent", &missing)
	assert.Equal(t, "", missing)
}

func TestRoundTrip_WriteAndReparse(t *testing.T) {
	t.Parallel()

	// Parse original
	data, err := os.ReadFile(filepath.Join("testdata", "fedWireMessage-CustomerTransfer.fed"))
	require.NoError(t, err)

	ts1, err := ParseReader(bytes.NewReader(data))
	require.NoError(t, err)

	// Write to buffer
	var buf bytes.Buffer
	err = ts1.WriteToWriter(&buf)
	require.NoError(t, err)

	// Re-parse
	ts2, err := ParseReader(bytes.NewReader(buf.Bytes()))
	require.NoError(t, err)

	// Compare key fields
	h1 := buildHeaderIndex(ts1.Message.Headers)
	h2 := buildHeaderIndex(ts2.Message.Headers)

	fieldsToCheck := []string{
		"amount",
		"business_function_code",
		"sender_di_routing_number",
		"receiver_di_routing_number",
		"type_code",
		"sub_type_code",
	}

	for _, field := range fieldsToCheck {
		idx1, ok1 := h1[field]
		idx2, ok2 := h2[field]
		require.True(t, ok1, "field %s not found in ts1", field)
		require.True(t, ok2, "field %s not found in ts2", field)
		assert.Equal(t, ts1.Message.Records[0][idx1], ts2.Message.Records[0][idx2],
			"field %s differs after round-trip", field)
	}
}

func TestFromFile_StructuredRemittance(t *testing.T) {
	t.Parallel()

	file := readTestFile(t, filepath.Join("testdata", "fedWireMessage-StructuredRemittance.fed"))
	ts := fromFile(file)
	require.NotNil(t, ts)

	headerIndex := buildHeaderIndex(ts.Message.Headers)
	record := ts.Message.Records[0]

	assertField(t, record, headerIndex, "business_function_code", "CTP")

	// Structured remittance should have remittance originator/beneficiary fields
	if file.FEDWireMessage.RemittanceOriginator != nil {
		assertFieldNonEmpty(t, record, headerIndex, "remittance_originator_identification_type")
	}
	if file.FEDWireMessage.PrimaryRemittanceDocument != nil {
		assertFieldNonEmpty(t, record, headerIndex, "primary_remittance_document_type_code")
	}
}

func TestFromFile_RelatedRemittance(t *testing.T) {
	t.Parallel()

	// Build a wire.File programmatically with RelatedRemittance populated.
	file := &wire.File{
		FEDWireMessage: wire.FEDWireMessage{},
	}
	file.FEDWireMessage.SenderSupplied = &wire.SenderSupplied{
		FormatVersion:          "30",
		UserRequestCorrelation: "T00001",
		TestProductionCode:     "T",
		MessageDuplicationCode: " ",
	}
	file.FEDWireMessage.TypeSubType = &wire.TypeSubType{TypeCode: "10", SubTypeCode: "00"}
	file.FEDWireMessage.InputMessageAccountabilityData = &wire.InputMessageAccountabilityData{
		InputCycleDate: "20190410", InputSource: "Source08", InputSequenceNumber: "000001",
	}
	file.FEDWireMessage.Amount = &wire.Amount{Amount: "000001234567"}
	file.FEDWireMessage.SenderDepositoryInstitution = &wire.SenderDepositoryInstitution{
		SenderABANumber: "121042882", SenderShortName: "Wells Fargo NA",
	}
	file.FEDWireMessage.ReceiverDepositoryInstitution = &wire.ReceiverDepositoryInstitution{
		ReceiverABANumber: "231380104", ReceiverShortName: "Citadel",
	}
	file.FEDWireMessage.BusinessFunctionCode = &wire.BusinessFunctionCode{
		BusinessFunctionCode: "CTP", TransactionTypeCode: "   ",
	}
	file.FEDWireMessage.RelatedRemittance = &wire.RelatedRemittance{
		RemittanceIdentification:            "RelRemitID",
		RemittanceLocationMethod:            "EDIC",
		RemittanceLocationElectronicAddress: "http://example.com",
	}

	ts := fromFile(file)
	require.NotNil(t, ts)

	headerIndex := buildHeaderIndex(ts.Message.Headers)
	record := ts.Message.Records[0]
	assertField(t, record, headerIndex, "related_remittance_identification", "RelRemitID")
	assertField(t, record, headerIndex, "related_remittance_location_method", "EDIC")
	assertField(t, record, headerIndex, "related_remittance_location_electronic_address", "http://example.com")
}

func TestRoundTrip_StructuredRemittance(t *testing.T) {
	t.Parallel()

	file := readTestFile(t, filepath.Join("testdata", "fedWireMessage-StructuredRemittance.fed"))
	ts := fromFile(file)
	require.NotNil(t, ts)

	result, err := ts.toFile()
	require.NoError(t, err)

	// Verify remittance fields are preserved
	if file.FEDWireMessage.RemittanceOriginator != nil {
		require.NotNil(t, result.FEDWireMessage.RemittanceOriginator)
		assert.Equal(t,
			file.FEDWireMessage.RemittanceOriginator.IdentificationType,
			result.FEDWireMessage.RemittanceOriginator.IdentificationType,
		)
	}
	if file.FEDWireMessage.PrimaryRemittanceDocument != nil {
		require.NotNil(t, result.FEDWireMessage.PrimaryRemittanceDocument)
		assert.Equal(t,
			file.FEDWireMessage.PrimaryRemittanceDocument.DocumentTypeCode,
			result.FEDWireMessage.PrimaryRemittanceDocument.DocumentTypeCode,
		)
	}
	if file.FEDWireMessage.ActualAmountPaid != nil {
		require.NotNil(t, result.FEDWireMessage.ActualAmountPaid)
		assert.Equal(t,
			file.FEDWireMessage.ActualAmountPaid.RemittanceAmount.Amount,
			result.FEDWireMessage.ActualAmountPaid.RemittanceAmount.Amount,
		)
	}
}

func TestRoundTrip_COVSModification(t *testing.T) {
	t.Parallel()

	file := readTestFile(t, filepath.Join("testdata", "fedWireMessage-CustomerTransferPlusCOVS.fed"))
	ts := fromFile(file)
	require.NotNil(t, ts)

	// Modify a CoverPayment field
	headerIndex := buildHeaderIndex(ts.Message.Headers)
	if idx, ok := headerIndex["ordering_customer_swift_line_one"]; ok {
		ts.Message.Records[0][idx] = "MODIFIED LINE ONE"
	}

	result, err := ts.toFile()
	require.NoError(t, err)

	if result.FEDWireMessage.OrderingCustomer != nil {
		assert.Equal(t, "MODIFIED LINE ONE", result.FEDWireMessage.OrderingCustomer.CoverPayment.SwiftLineOne)
	}
}

func TestAppendHelpers(t *testing.T) {
	t.Parallel()

	t.Run("appendFinancialInstitution", func(t *testing.T) {
		t.Parallel()
		fi := wire.FinancialInstitution{
			IdentificationCode: "D",
			Identifier:         "123",
			Name:               "Test Bank",
			Address: wire.Address{
				AddressLineOne:   "Addr1",
				AddressLineTwo:   "Addr2",
				AddressLineThree: "Addr3",
			},
		}
		r := appendFinancialInstitution(nil, fi)
		assert.Equal(t, []string{"D", "123", "Test Bank", "Addr1", "Addr2", "Addr3"}, r)
	})

	t.Run("appendPersonal", func(t *testing.T) {
		t.Parallel()
		p := wire.Personal{
			IdentificationCode: "1",
			Identifier:         "456",
			Name:               "John Doe",
		}
		r := appendPersonal(nil, p)
		assert.Len(t, r, 6)
		assert.Equal(t, "John Doe", r[2])
	})

	t.Run("appendFIToFI", func(t *testing.T) {
		t.Parallel()
		f := wire.FIToFI{
			LineOne: "L1", LineTwo: "L2", LineThree: "L3",
			LineFour: "L4", LineFive: "L5", LineSix: "L6",
		}
		r := appendFIToFI(nil, f)
		assert.Equal(t, []string{"L1", "L2", "L3", "L4", "L5", "L6"}, r)
	})

	t.Run("appendAdvice", func(t *testing.T) {
		t.Parallel()
		a := wire.Advice{AdviceCode: "LTR", LineOne: "L1"}
		r := appendAdvice(nil, a)
		assert.Len(t, r, 7)
		assert.Equal(t, "LTR", r[0])
		assert.Equal(t, "L1", r[1])
	})

	t.Run("appendCoverPayment", func(t *testing.T) {
		t.Parallel()
		cp := wire.CoverPayment{
			SwiftFieldTag: "/TAG/",
			SwiftLineOne:  "Line1",
			SwiftLineSix:  "Line6",
		}
		r := appendCoverPayment(nil, cp)
		assert.Len(t, r, 7)
		assert.Equal(t, "/TAG/", r[0])
		assert.Equal(t, "Line6", r[6])
	})

	t.Run("appendRemittanceData", func(t *testing.T) {
		t.Parallel()
		rd := wire.RemittanceData{
			Name:                    "Originator Name",
			Country:                 "US",
			CountryOfResidence:      "US",
			CountrySubDivisionState: "PA",
		}
		r := appendRemittanceData(nil, rd)
		assert.Len(t, r, 19)
		assert.Equal(t, "Originator Name", r[0])
		assert.Equal(t, "US", r[18])
	})

	t.Run("appendRemittanceAmount", func(t *testing.T) {
		t.Parallel()
		ra := wire.RemittanceAmount{CurrencyCode: "USD", Amount: "1234,56"}
		r := appendRemittanceAmount(nil, ra)
		assert.Equal(t, []string{"USD", "1234,56"}, r)
	})
}

func TestApplyHelpers(t *testing.T) {
	t.Parallel()

	t.Run("applyFinancialInstitution", func(t *testing.T) {
		t.Parallel()
		fi := &wire.FinancialInstitution{}
		headerIndex := map[string]int{
			"test_id_code":            0,
			"test_identifier":         1,
			"test_name":               2,
			"test_address_line_one":   3,
			"test_address_line_two":   4,
			"test_address_line_three": 5,
		}
		record := []string{"D", "123", "Bank", "A1", "A2", "A3"}
		applyFinancialInstitution(headerIndex, record, fi, "test")
		assert.Equal(t, "D", fi.IdentificationCode)
		assert.Equal(t, "A3", fi.Address.AddressLineThree)
	})

	t.Run("applyPersonal", func(t *testing.T) {
		t.Parallel()
		p := &wire.Personal{}
		headerIndex := map[string]int{
			"test_id_code":            0,
			"test_identifier":         1,
			"test_name":               2,
			"test_address_line_one":   3,
			"test_address_line_two":   4,
			"test_address_line_three": 5,
		}
		record := []string{"1", "456", "Jane", "A1", "A2", "A3"}
		applyPersonal(headerIndex, record, p, "test")
		assert.Equal(t, "Jane", p.Name)
	})

	t.Run("applyFIToFI", func(t *testing.T) {
		t.Parallel()
		f := &wire.FIToFI{}
		headerIndex := map[string]int{
			"test_line_one": 0, "test_line_two": 1, "test_line_three": 2,
			"test_line_four": 3, "test_line_five": 4, "test_line_six": 5,
		}
		record := []string{"L1", "L2", "L3", "L4", "L5", "L6"}
		applyFIToFI(headerIndex, record, f, "test")
		assert.Equal(t, "L6", f.LineSix)
	})

	t.Run("applyAdvice", func(t *testing.T) {
		t.Parallel()
		a := &wire.Advice{}
		headerIndex := map[string]int{
			"test_advice_code": 0,
			"test_line_one":    1, "test_line_two": 2, "test_line_three": 3,
			"test_line_four": 4, "test_line_five": 5, "test_line_six": 6,
		}
		record := []string{"LTR", "L1", "L2", "L3", "L4", "L5", "L6"}
		applyAdvice(headerIndex, record, a, "test")
		assert.Equal(t, "LTR", a.AdviceCode)
		assert.Equal(t, "L6", a.LineSix)
	})

	t.Run("applyCoverPayment", func(t *testing.T) {
		t.Parallel()
		cp := &wire.CoverPayment{}
		headerIndex := map[string]int{
			"test_swift_field_tag": 0,
			"test_swift_line_one":  1, "test_swift_line_two": 2, "test_swift_line_three": 3,
			"test_swift_line_four": 4, "test_swift_line_five": 5, "test_swift_line_six": 6,
		}
		record := []string{"/TAG/", "L1", "L2", "L3", "L4", "L5", "L6"}
		applyCoverPayment(headerIndex, record, cp, "test")
		assert.Equal(t, "/TAG/", cp.SwiftFieldTag)
		assert.Equal(t, "L6", cp.SwiftLineSix)
	})

	t.Run("applyRemittanceData", func(t *testing.T) {
		t.Parallel()
		rd := &wire.RemittanceData{}
		headerIndex := map[string]int{
			"test_name": 0, "test_date_birth_place": 1, "test_address_type": 2,
			"test_department": 3, "test_sub_department": 4, "test_street_name": 5,
			"test_building_number": 6, "test_post_code": 7, "test_town_name": 8,
			"test_country_sub_division_state": 9, "test_country": 10,
			"test_address_line_one": 11, "test_address_line_two": 12,
			"test_address_line_three": 13, "test_address_line_four": 14,
			"test_address_line_five": 15, "test_address_line_six": 16,
			"test_address_line_seven": 17, "test_country_of_residence": 18,
		}
		record := make([]string, 19)
		record[0] = "TestName"
		record[10] = "US"
		record[18] = "US"
		applyRemittanceData(headerIndex, record, rd, "test")
		assert.Equal(t, "TestName", rd.Name)
		assert.Equal(t, "US", rd.Country)
		assert.Equal(t, "US", rd.CountryOfResidence)
	})

	t.Run("applyRemittanceAmount", func(t *testing.T) {
		t.Parallel()
		ra := &wire.RemittanceAmount{}
		headerIndex := map[string]int{"test_currency_code": 0, "test_amount": 1}
		record := []string{"USD", "1000,00"}
		applyRemittanceAmount(headerIndex, record, ra, "test")
		assert.Equal(t, "USD", ra.CurrencyCode)
		assert.Equal(t, "1000,00", ra.Amount)
	})

	t.Run("applyRemittanceDocument_Primary", func(t *testing.T) {
		t.Parallel()
		doc := &wire.PrimaryRemittanceDocument{}
		headerIndex := map[string]int{
			"test_type_code": 0, "test_proprietary_code": 1,
			"test_identification_number": 2, "test_issuer": 3,
		}
		record := []string{"AROI", "PROP", "DOC123", "Issuer"}
		applyRemittanceDocument(headerIndex, record, doc, "test")
		assert.Equal(t, "AROI", doc.DocumentTypeCode)
		assert.Equal(t, "Issuer", doc.Issuer)
	})

	t.Run("applyRemittanceDocument_Secondary", func(t *testing.T) {
		t.Parallel()
		doc := &wire.SecondaryRemittanceDocument{}
		headerIndex := map[string]int{
			"test_type_code": 0, "test_proprietary_code": 1,
			"test_identification_number": 2, "test_issuer": 3,
		}
		record := []string{"AROI", "", "DOC456", ""}
		applyRemittanceDocument(headerIndex, record, doc, "test")
		assert.Equal(t, "AROI", doc.DocumentTypeCode)
		assert.Equal(t, "DOC456", doc.DocumentIdentificationNumber)
	})
}

func TestDeepCopyFile_AllFields(t *testing.T) {
	t.Parallel()

	// Create a file with many optional fields populated
	file := &wire.File{
		ID: "test-all",
		FEDWireMessage: wire.FEDWireMessage{
			SenderSupplied: &wire.SenderSupplied{FormatVersion: "30"},
			TypeSubType:    &wire.TypeSubType{TypeCode: "10", SubTypeCode: "00"},
			InputMessageAccountabilityData: &wire.InputMessageAccountabilityData{
				InputCycleDate: "20200101", InputSource: "SRC", InputSequenceNumber: "000001",
			},
			Amount:                        &wire.Amount{Amount: "000001000000"},
			SenderDepositoryInstitution:   &wire.SenderDepositoryInstitution{SenderABANumber: "121042882"},
			ReceiverDepositoryInstitution: &wire.ReceiverDepositoryInstitution{ReceiverABANumber: "231380104"},
			BusinessFunctionCode:          &wire.BusinessFunctionCode{BusinessFunctionCode: "CTP"},
			SenderReference:               &wire.SenderReference{SenderReference: "REF123"},
			PreviousMessageIdentifier:     &wire.PreviousMessageIdentifier{PreviousMessageIdentifier: "PREV"},
			LocalInstrument:               &wire.LocalInstrument{LocalInstrumentCode: "ANSI"},
			PaymentNotification:           &wire.PaymentNotification{PaymentNotificationIndicator: "1"},
			Charges:                       &wire.Charges{ChargeDetails: "B"},
			InstructedAmount:              &wire.InstructedAmount{CurrencyCode: "USD", Amount: "100,00"},
			ExchangeRate:                  &wire.ExchangeRate{ExchangeRate: "1,2345"},
			BeneficiaryReference:          &wire.BeneficiaryReference{BeneficiaryReference: "BREF"},
			AccountCreditedDrawdown:       &wire.AccountCreditedDrawdown{DrawdownCreditAccountNumber: "ACCT123"},
			FIPaymentMethodToBeneficiary:  &wire.FIPaymentMethodToBeneficiary{PaymentMethod: "CHECK"},
			CurrencyInstructedAmount:      &wire.CurrencyInstructedAmount{SwiftFieldTag: "TAG", Amount: "200,00"},
			UnstructuredAddenda:           &wire.UnstructuredAddenda{AddendaLength: "10", Addenda: "TEST"},
			DateRemittanceDocument:        &wire.DateRemittanceDocument{DateRemittanceDocument: "20200115"},
			ServiceMessage:                &wire.ServiceMessage{LineOne: "SVC1", LineTwo: "SVC2"},
			MessageDisposition:            &wire.MessageDisposition{FormatVersion: "30"},
			ReceiptTimeStamp:              &wire.ReceiptTimeStamp{ReceiptDate: "0110"},
			OutputMessageAccountabilityData: &wire.OutputMessageAccountabilityData{
				OutputCycleDate: "20200101",
			},
			ErrorWire: &wire.ErrorWire{ErrorCategory: "E", ErrorCode: "001"},
		},
	}

	copied := deepCopyFile(file)

	// Verify all copied fields
	assert.NotNil(t, copied.FEDWireMessage.SenderReference)
	assert.Equal(t, "REF123", copied.FEDWireMessage.SenderReference.SenderReference)
	assert.NotNil(t, copied.FEDWireMessage.PreviousMessageIdentifier)
	assert.NotNil(t, copied.FEDWireMessage.LocalInstrument)
	assert.NotNil(t, copied.FEDWireMessage.PaymentNotification)
	assert.NotNil(t, copied.FEDWireMessage.Charges)
	assert.NotNil(t, copied.FEDWireMessage.InstructedAmount)
	assert.NotNil(t, copied.FEDWireMessage.ExchangeRate)
	assert.NotNil(t, copied.FEDWireMessage.BeneficiaryReference)
	assert.NotNil(t, copied.FEDWireMessage.AccountCreditedDrawdown)
	assert.NotNil(t, copied.FEDWireMessage.FIPaymentMethodToBeneficiary)
	assert.NotNil(t, copied.FEDWireMessage.CurrencyInstructedAmount)
	assert.NotNil(t, copied.FEDWireMessage.UnstructuredAddenda)
	assert.NotNil(t, copied.FEDWireMessage.DateRemittanceDocument)
	assert.NotNil(t, copied.FEDWireMessage.ServiceMessage)
	assert.NotNil(t, copied.FEDWireMessage.MessageDisposition)
	assert.NotNil(t, copied.FEDWireMessage.ReceiptTimeStamp)
	assert.NotNil(t, copied.FEDWireMessage.OutputMessageAccountabilityData)
	assert.NotNil(t, copied.FEDWireMessage.ErrorWire)

	// Verify independence
	copied.FEDWireMessage.SenderReference.SenderReference = "MODIFIED"
	assert.Equal(t, "REF123", file.FEDWireMessage.SenderReference.SenderReference)
}

func TestHasNonEmptyField(t *testing.T) {
	t.Parallel()

	headerIndex := map[string]int{"a": 0, "b": 1, "c": 2}

	t.Run("returns true when at least one field is non-empty", func(t *testing.T) {
		t.Parallel()
		record := []string{"", "value", ""}
		assert.True(t, hasNonEmptyField(headerIndex, record, "a", "b", "c"))
	})

	t.Run("returns false when all fields are empty", func(t *testing.T) {
		t.Parallel()
		record := []string{"", "", ""}
		assert.False(t, hasNonEmptyField(headerIndex, record, "a", "b", "c"))
	})

	t.Run("returns false for unknown field names", func(t *testing.T) {
		t.Parallel()
		record := []string{"x", "y", "z"}
		assert.False(t, hasNonEmptyField(headerIndex, record, "unknown"))
	})

	t.Run("returns false when record is shorter than index", func(t *testing.T) {
		t.Parallel()
		record := []string{"x"}
		assert.False(t, hasNonEmptyField(headerIndex, record, "c"))
	})
}

func TestEnsureNonNilSubStructs_AllocatesOnNonEmptyValues(t *testing.T) {
	t.Parallel()

	// Start with a minimal FEDWireMessage where most sub-structs are nil
	fwm := &wire.FEDWireMessage{}

	headers := messageHeaders()
	headerIndex := buildHeaderIndex(headers)
	record := make([]string, len(headers))

	// Set a non-empty value for Charges (originally nil)
	if idx, ok := headerIndex["charges_details"]; ok {
		record[idx] = "B"
	}
	// Set a non-empty value for LocalInstrument (originally nil)
	if idx, ok := headerIndex["local_instrument_code"]; ok {
		record[idx] = "ANSI"
	}
	// Set a non-empty value for Beneficiary (originally nil)
	if idx, ok := headerIndex["beneficiary_name"]; ok {
		record[idx] = "Jane Doe"
	}

	ensureNonNilSubStructs(fwm, headerIndex, record)

	// Sub-structs with non-empty values should be allocated
	assert.NotNil(t, fwm.Charges, "Charges should be allocated when record has non-empty charges_details")
	assert.NotNil(t, fwm.LocalInstrument, "LocalInstrument should be allocated when record has non-empty local_instrument_code")
	assert.NotNil(t, fwm.Beneficiary, "Beneficiary should be allocated when record has non-empty beneficiary_name")

	// Sub-structs with all-empty values should remain nil
	assert.Nil(t, fwm.SenderReference, "SenderReference should remain nil when record has empty sender_reference")
	assert.Nil(t, fwm.ExchangeRate, "ExchangeRate should remain nil when record has empty exchange_rate")
	assert.Nil(t, fwm.ServiceMessage, "ServiceMessage should remain nil when all service_message lines are empty")
}

func TestApplyModifications_NilSubStructsReceiveValues(t *testing.T) {
	t.Parallel()

	// Create a FEDWireMessage where Charges and LocalInstrument are nil
	fwm := &wire.FEDWireMessage{
		SenderSupplied: &wire.SenderSupplied{FormatVersion: "30"},
		TypeSubType:    &wire.TypeSubType{TypeCode: "10", SubTypeCode: "00"},
		InputMessageAccountabilityData: &wire.InputMessageAccountabilityData{
			InputCycleDate: "20200101", InputSource: "SRC", InputSequenceNumber: "000001",
		},
		Amount:                        &wire.Amount{Amount: "000001000000"},
		SenderDepositoryInstitution:   &wire.SenderDepositoryInstitution{SenderABANumber: "121042882"},
		ReceiverDepositoryInstitution: &wire.ReceiverDepositoryInstitution{ReceiverABANumber: "231380104"},
		BusinessFunctionCode:          &wire.BusinessFunctionCode{BusinessFunctionCode: "CTR"},
		// Charges is intentionally nil
		// LocalInstrument is intentionally nil
	}

	// Build TableData from the message (nil sub-structs become empty strings)
	headers := messageHeaders()
	record := messageRecord(fwm)

	headerIndex := buildHeaderIndex(headers)

	// Simulate user editing: set values for originally-nil sub-structs
	if idx, ok := headerIndex["charges_details"]; ok {
		record[idx] = "B"
	}
	if idx, ok := headerIndex["charges_senders_one"]; ok {
		record[idx] = "USD10,00"
	}
	if idx, ok := headerIndex["local_instrument_code"]; ok {
		record[idx] = "ANSI"
	}

	td := &parser.TableData{
		Headers: headers,
		Records: [][]string{record},
	}

	// Apply modifications — previously this would silently drop the edits
	applyModifications(fwm, td)

	// Verify the previously-nil sub-structs now have the user's values
	require.NotNil(t, fwm.Charges, "Charges should be allocated after applying non-empty values")
	assert.Equal(t, "B", fwm.Charges.ChargeDetails)
	assert.Equal(t, "USD10,00", fwm.Charges.SendersChargesOne)

	require.NotNil(t, fwm.LocalInstrument, "LocalInstrument should be allocated after applying non-empty values")
	assert.Equal(t, "ANSI", fwm.LocalInstrument.LocalInstrumentCode)
}

func TestToFile_RoundTripWithNewSections(t *testing.T) {
	t.Parallel()

	// Create a minimal wire file without Charges
	file := &wire.File{
		ID: "test-roundtrip",
		FEDWireMessage: wire.FEDWireMessage{
			SenderSupplied: &wire.SenderSupplied{FormatVersion: "30"},
			TypeSubType:    &wire.TypeSubType{TypeCode: "10", SubTypeCode: "00"},
			InputMessageAccountabilityData: &wire.InputMessageAccountabilityData{
				InputCycleDate: "20200101", InputSource: "SRC", InputSequenceNumber: "000001",
			},
			Amount:                        &wire.Amount{Amount: "000001000000"},
			SenderDepositoryInstitution:   &wire.SenderDepositoryInstitution{SenderABANumber: "121042882"},
			ReceiverDepositoryInstitution: &wire.ReceiverDepositoryInstitution{ReceiverABANumber: "231380104"},
			BusinessFunctionCode:          &wire.BusinessFunctionCode{BusinessFunctionCode: "CTR"},
		},
	}
	assert.Nil(t, file.FEDWireMessage.Charges, "precondition: Charges should be nil")

	// Convert to TableSet
	ts := fromFile(file)
	require.NotNil(t, ts)

	// Simulate SQL edit: set charges in the TableData
	headerIndex := buildHeaderIndex(ts.Message.Headers)
	if idx, ok := headerIndex["charges_details"]; ok {
		ts.Message.Records[0][idx] = "B"
	}

	// Round-trip back to wire.File
	newFile, err := ts.toFile()
	require.NoError(t, err)

	// The new file should have a Charges section with the user's value
	require.NotNil(t, newFile.FEDWireMessage.Charges, "Charges should be present in reconstructed file")
	assert.Equal(t, "B", newFile.FEDWireMessage.Charges.ChargeDetails)

	// Original file should be unmodified
	assert.Nil(t, file.FEDWireMessage.Charges, "original file should remain unmodified")
}

// --- Test helpers ---

func readTestFile(t *testing.T, path string) *wire.File {
	t.Helper()

	f, err := os.Open(path) //nolint:gosec // Test file path is from test constants
	require.NoError(t, err)
	defer f.Close()

	reader := wire.NewReader(f)
	file, err := reader.Read()
	require.NoError(t, err)
	return &file
}

func buildHeaderIndex(headers []string) map[string]int {
	idx := make(map[string]int, len(headers))
	for i, h := range headers {
		idx[h] = i
	}
	return idx
}

func assertField(t *testing.T, record []string, headerIndex map[string]int, field, expected string) {
	t.Helper()
	idx, ok := headerIndex[field]
	require.True(t, ok, "field %s not found in headers", field)
	require.Less(t, idx, len(record), "field %s index out of range", field)
	assert.Equal(t, expected, record[idx], "field %s", field)
}

func assertFieldContains(t *testing.T, record []string, headerIndex map[string]int, field, substring string) {
	t.Helper()
	idx, ok := headerIndex[field]
	require.True(t, ok, "field %s not found in headers", field)
	require.Less(t, idx, len(record), "field %s index out of range", field)
	assert.Contains(t, record[idx], substring, "field %s should contain %q", field, substring)
}

func assertFieldNonEmpty(t *testing.T, record []string, headerIndex map[string]int, field string) {
	t.Helper()
	idx, ok := headerIndex[field]
	require.True(t, ok, "field %s not found in headers", field)
	require.Less(t, idx, len(record), "field %s index out of range", field)
	assert.NotEmpty(t, record[idx], "field %s should not be empty", field)
}

// TestWriteToWriter_RefusesAMessageItCannotWriteFaithfully drives the fault a
// write-back used to have: a field written from the wrong source, with nothing
// to say so.
//
// moov-io/wire v0.16.1 writes the remittance originator's fourth address line
// from its first, so a file that was read and written straight back came out
// with line four replaced by a copy of line one and the original gone. The
// stock fixture hid it by repeating "Address Line One" in the line-four
// position, so the wrong value happened to equal the right one.
//
// filesql cannot make that write correct without patching another package's
// output, so it refuses it: the caller keeps the file they started with and is
// told which field could not be written. When the dependency is fixed this test
// starts failing, which is the signal to delete it along with whatever is left
// of the workaround.
func TestWriteToWriter_RefusesAMessageItCannotWriteFaithfully(t *testing.T) {
	t.Parallel()

	// Give every address line of the remittance originator a value of its own,
	// so a field written from its neighbor cannot pass unnoticed.
	raw, err := os.ReadFile(filepath.Join("testdata", "fedWireMessage-StructuredRemittance.fed"))
	require.NoError(t, err)

	lines := strings.Split(string(raw), "\n")
	for i, line := range lines {
		if !strings.HasPrefix(line, "{8300}") {
			continue
		}
		parts := strings.Split(line, "*")
		seen := 0
		for j, p := range parts {
			if !strings.HasPrefix(p, "Address Line ") {
				continue
			}
			seen++
			parts[j] = fmt.Sprintf("LINE%d", seen)
		}
		lines[i] = strings.Join(parts, "*")
	}

	ts, err := ParseReader(strings.NewReader(strings.Join(lines, "\n")))
	require.NoError(t, err)

	cell := func(ts *TableSet, column string) string {
		idx := buildHeaderIndex(ts.Message.Headers)
		i, ok := idx[column]
		require.True(t, ok, "no such column: %s", column)
		return ts.Message.Records[0][i]
	}

	// Reading is unaffected: every line comes back in its own column.
	for column, value := range map[string]string{
		"remittance_originator_address_line_one":   "LINE1",
		"remittance_originator_address_line_two":   "LINE2",
		"remittance_originator_address_line_three": "LINE3",
		"remittance_originator_address_line_four":  "LINE4",
		"remittance_originator_address_line_five":  "LINE5",
		"remittance_originator_address_line_six":   "LINE6",
		"remittance_originator_address_line_seven": "LINE7",
	} {
		require.Equal(t, value, cell(ts, column), "the fixture was not read as intended: %s", column)
	}

	var buf bytes.Buffer
	err = ts.WriteToWriter(&buf)
	require.Error(t, err, "a write that cannot keep a field must be refused, not silently wrong")
	assert.Contains(t, err.Error(), "remittance_originator_address_line_four",
		"the error must name the field that could not be written")
	assert.Zero(t, buf.Len(), "nothing may reach the caller's writer when the write is refused")
}

// TestWriteToWriter_KeepsEveryFieldOfEveryFixture is the invariant the check
// above exists to serve, held over every Fedwire fixture in the repository:
// parse, write with no edits, parse again, and every one of the 326 columns
// still holds what the file held.
//
// The fixtures pass it today. What they did not do before is fail when a field
// was written from the wrong source, because one of them repeats the same text
// in two address positions -- so the invariant is only worth as much as the
// distinctness of the values it runs on, which is what the test above adds.
func TestWriteToWriter_KeepsEveryFieldOfEveryFixture(t *testing.T) {
	t.Parallel()

	entries, err := os.ReadDir("testdata")
	require.NoError(t, err)

	for _, entry := range entries {
		if filepath.Ext(entry.Name()) != ".fed" {
			continue
		}
		t.Run(entry.Name(), func(t *testing.T) {
			t.Parallel()

			raw, readErr := os.ReadFile(filepath.Join("testdata", entry.Name()))
			require.NoError(t, readErr)

			ts, parseErr := ParseReader(bytes.NewReader(raw))
			require.NoError(t, parseErr, "every fixture in testdata must be a message this package reads")

			var buf bytes.Buffer
			require.NoError(t, ts.WriteToWriter(&buf))

			back, err := ParseReader(bytes.NewReader(buf.Bytes()))
			require.NoError(t, err)
			require.Equal(t, ts.Message.Headers, back.Message.Headers)
			for i, column := range ts.Message.Headers {
				assert.Equal(t, ts.Message.Records[0][i], back.Message.Records[0][i],
					"%s did not survive a write with no edits", column)
			}
		})
	}
}
