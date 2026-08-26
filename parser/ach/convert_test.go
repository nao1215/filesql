package ach

import (
	"bytes"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/moov-io/ach"
	"github.com/nao1215/filesql/parser"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func findHeaderIndex(t *testing.T, headers []string, name string) int {
	t.Helper()

	for i, header := range headers {
		if header == name {
			return i
		}
	}

	t.Fatalf("header %q not found", name)
	return -1
}

func TestFromFile_NilFile(t *testing.T) {
	ts := fromFile(nil)
	assert.Nil(t, ts)
}

func TestFromFile_EmptyFile(t *testing.T) {
	file := ach.NewFile()
	file.Header.ImmediateDestination = "231380104"
	file.Header.ImmediateOrigin = "121042882"
	file.Header.FileCreationDate = "190624"
	file.Header.FileCreationTime = "0000"
	file.Header.FileIDModifier = "A"

	ts := fromFile(file)
	require.NotNil(t, ts)

	// File header should have 1 row
	assert.Len(t, ts.FileHeader.Records, 1)
	assert.Equal(t, "231380104", ts.FileHeader.Records[0][0])

	// Batches should be empty
	assert.Len(t, ts.Batches.Records, 0)

	// Entries should be empty
	assert.Len(t, ts.Entries.Records, 0)
}

func TestFromFile_WithBatchAndEntry(t *testing.T) {
	file := createTestACHFile(t)

	ts := fromFile(file)
	require.NotNil(t, ts)

	// Check file header
	assert.Len(t, ts.FileHeader.Records, 1)
	assert.Equal(t, "231380104", ts.FileHeader.Records[0][0])

	// Check batches
	assert.Len(t, ts.Batches.Records, 1)
	batchRecord := ts.Batches.Records[0]
	assert.Equal(t, "0", batchRecord[0])               // batch_index
	assert.Equal(t, "225", batchRecord[1])             // service_class_code (DebitsOnly)
	assert.Equal(t, "Name on Account", batchRecord[2]) // company_name
	assert.Equal(t, "PPD", batchRecord[5])             // standard_entry_class_code

	// Check entries
	assert.Len(t, ts.Entries.Records, 1)
	entryRecord := ts.Entries.Records[0]
	assert.Equal(t, "0", entryRecord[0])                     // batch_index
	assert.Equal(t, "0", entryRecord[1])                     // entry_index
	assert.Equal(t, "27", entryRecord[2])                    // transaction_code (CheckingDebit)
	assert.Equal(t, "100000000", entryRecord[6])             // amount (in cents)
	assert.Equal(t, "Receiver Account Name", entryRecord[8]) // individual_name
}

func TestToFile_NilTableSet(t *testing.T) {
	var ts *TableSet
	file, err := ts.toFile()
	assert.Error(t, err)
	assert.Nil(t, file)
}

func TestToFile_RoundTrip(t *testing.T) {
	// Create original file
	originalFile := createTestACHFile(t)

	// Convert to TableSet
	ts := fromFile(originalFile)
	require.NotNil(t, ts)

	// Convert back to file
	newFile, err := ts.toFile()
	require.NoError(t, err)
	require.NotNil(t, newFile)

	// Verify structure is preserved
	assert.Equal(t, originalFile.Header.ImmediateDestination, newFile.Header.ImmediateDestination)
	assert.Equal(t, originalFile.Header.ImmediateOrigin, newFile.Header.ImmediateOrigin)
	assert.Len(t, newFile.Batches, len(originalFile.Batches))
}

func TestToFile_ModifyAmount(t *testing.T) {
	// Create original file
	originalFile := createTestACHFile(t)

	// Convert to TableSet
	ts := fromFile(originalFile)
	require.NotNil(t, ts)

	// Modify amount in entries
	require.Len(t, ts.Entries.Records, 1)
	// Find amount column index
	amountIdx := -1
	for i, h := range ts.Entries.Headers {
		if h == "amount" {
			amountIdx = i
			break
		}
	}
	require.NotEqual(t, -1, amountIdx, "amount column not found")

	// Change amount from 100000000 to 50000000
	ts.Entries.Records[0][amountIdx] = "50000000"

	// Convert back to file
	newFile, err := ts.toFile()
	require.NoError(t, err)
	require.NotNil(t, newFile)

	// Verify amount was changed
	entries := newFile.Batches[0].GetEntries()
	require.Len(t, entries, 1)
	assert.Equal(t, 50000000, entries[0].Amount)
}

// TestToFile_ModifiedAmountIsWritable is the half TestToFile_ModifyAmount above
// does not reach. That test reads the amount back off the returned file, which
// the deep copy already carried; the writer is where the batch control is
// checked against the entries, and it was still the control the original file
// arrived with. Every amount edit was therefore unwritable: the value changed,
// the control did not, and the write failed as out-of-balance.
func TestToFile_ModifiedAmountIsWritable(t *testing.T) {
	original := createTestACHFile(t)
	ts := fromFile(original)
	require.NotNil(t, ts)

	amountIdx := findHeaderIndex(t, ts.Entries.Headers, "amount")

	const newAmount = 50000000
	ts.Entries.Records[0][amountIdx] = strconv.Itoa(newAmount)

	var buf bytes.Buffer
	require.NoError(t, ts.WriteToWriter(&buf))

	// Re-parsing is what proves the file is valid ACH rather than merely bytes:
	// the reader runs the same balance check the writer does.
	reparsed, err := ParseReader(bytes.NewReader(buf.Bytes()))
	require.NoError(t, err)

	entries := reparsed.originalFile.Batches[0].GetEntries()
	require.Len(t, entries, 1)
	assert.Equal(t, newAmount, entries[0].Amount)

	// The controls are derived from the entries, so they follow the edit rather
	// than keeping the value the source file was written with.
	control := reparsed.originalFile.Batches[0].GetControl()
	require.NotNil(t, control)
	assert.Equal(t, newAmount, control.TotalDebitEntryDollarAmount)
	assert.Equal(t, newAmount, reparsed.originalFile.Control.TotalDebitEntryDollarAmountInFile)
}

// TestToFile_ModifiedIATAmountIsWritable is the IAT half of the recalculation.
// IAT batches carry their own control record and are rebuilt by IATBatch.Create,
// which File.Create does not call either, so they had the same defect.
func TestToFile_ModifiedIATAmountIsWritable(t *testing.T) {
	file, err := ach.ReadFile(findTestFile(t, "iat-credit.ach"))
	require.NoError(t, err)

	ts := fromFile(file)
	require.NotNil(t, ts)
	require.NotNil(t, ts.IATEntries)
	require.NotEmpty(t, ts.IATEntries.Records)

	amountIdx := findHeaderIndex(t, ts.IATEntries.Headers, "amount")
	const newAmount = 50000
	ts.IATEntries.Records[0][amountIdx] = strconv.Itoa(newAmount)

	var buf bytes.Buffer
	require.NoError(t, ts.WriteToWriter(&buf))

	reparsed, err := ParseReader(bytes.NewReader(buf.Bytes()))
	require.NoError(t, err)
	entries := reparsed.originalFile.IATBatches[0].GetEntries()
	require.NotEmpty(t, entries)
	assert.Equal(t, newAmount, entries[0].Amount)
}

func TestToFile_RejectsNegativeBatchIndexInBatches(t *testing.T) {
	file := createTestACHFile(t)
	ts := fromFile(file)
	require.NotNil(t, ts)
	require.Len(t, ts.Batches.Records, 1)

	batchIdx := findHeaderIndex(t, ts.Batches.Headers, "batch_index")
	ts.Batches.Records[0][batchIdx] = "-1"

	newFile, err := ts.toFile()

	require.Error(t, err)
	assert.Nil(t, newFile)
	assert.Contains(t, err.Error(), "batch_index -1 out of range")
}

func TestToFile_RejectsNegativeBatchIndexInEntries(t *testing.T) {
	file := createTestACHFile(t)
	ts := fromFile(file)
	require.NotNil(t, ts)
	require.Len(t, ts.Entries.Records, 1)

	batchIdx := findHeaderIndex(t, ts.Entries.Headers, "batch_index")
	ts.Entries.Records[0][batchIdx] = "-1"

	newFile, err := ts.toFile()

	require.Error(t, err)
	assert.Nil(t, newFile)
	assert.Contains(t, err.Error(), "batch_index -1 out of range")
}

func TestGetters(t *testing.T) {
	file := createTestACHFile(t)
	ts := fromFile(file)
	require.NotNil(t, ts)

	assert.NotNil(t, ts.GetEntriesTable())
	assert.NotNil(t, ts.GetBatchesTable())
	assert.NotNil(t, ts.GetFileHeaderTable())
	assert.NotNil(t, ts.GetAddendaTable())

	// Test nil TableSet
	var nilTs *TableSet
	assert.Nil(t, nilTs.GetEntriesTable())
	assert.Nil(t, nilTs.GetBatchesTable())
	assert.Nil(t, nilTs.GetFileHeaderTable())
	assert.Nil(t, nilTs.GetAddendaTable())
}

func TestToFile_DeepCopy(t *testing.T) {
	// This test verifies that toFile creates a true deep copy
	// and does not modify the original file
	file := createTestACHFile(t)
	originalAmount := file.Batches[0].GetEntries()[0].Amount

	ts := fromFile(file)
	require.NotNil(t, ts)

	// Modify amount in entries TableData
	amountIdx := -1
	for i, h := range ts.Entries.Headers {
		if h == "amount" {
			amountIdx = i
			break
		}
	}
	require.NotEqual(t, -1, amountIdx)

	// Change amount to a different value
	newAmount := originalAmount + 1000000
	ts.Entries.Records[0][amountIdx] = strconv.Itoa(newAmount)

	// Convert to new file
	newFile, err := ts.toFile()
	require.NoError(t, err)

	// Verify new file has modified amount
	assert.Equal(t, newAmount, newFile.Batches[0].GetEntries()[0].Amount)

	// Verify ORIGINAL file is NOT modified (deep copy worked)
	assert.Equal(t, originalAmount, file.Batches[0].GetEntries()[0].Amount,
		"Original file should not be modified - deep copy failed")
}

func TestFromFile_RealACHFile(t *testing.T) {
	// Find the test ACH file
	testFile := findTestACHFile(t)
	if testFile == "" {
		t.Skip("No test ACH file found")
	}

	file, err := ach.ReadFile(testFile)
	require.NoError(t, err)

	ts := fromFile(file)
	require.NotNil(t, ts)

	// Basic structure checks
	assert.NotEmpty(t, ts.FileHeader.Headers)
	assert.Len(t, ts.FileHeader.Records, 1)

	t.Logf("Batches: %d", len(ts.Batches.Records))
	t.Logf("Entries: %d", len(ts.Entries.Records))
	t.Logf("Addenda: %d", len(ts.Addenda.Records))
}

// TestFromFile_WithAddenda99Return tests Addenda99 return entries
func TestFromFile_WithAddenda99Return(t *testing.T) {
	testFile := findTestFile(t, "return-WEB.ach")
	if testFile == "" {
		t.Skip("return-WEB.ach not found")
	}

	file, err := ach.ReadFile(testFile)
	require.NoError(t, err)

	ts := fromFile(file)
	require.NotNil(t, ts)

	// Should have addenda records
	assert.NotEmpty(t, ts.Addenda.Records, "expected addenda records for return file")

	// Check addenda type
	for _, record := range ts.Addenda.Records {
		addendaType := record[3] // addenda_type column
		assert.Equal(t, "99", addendaType, "expected Addenda99 type for return")
	}
}

// TestFromFile_WithAddenda98COR tests Addenda98 Notification of Change
func TestFromFile_WithAddenda98COR(t *testing.T) {
	testFile := findTestFile(t, "cor-example.ach")
	if testFile == "" {
		t.Skip("cor-example.ach not found")
	}

	file, err := ach.ReadFile(testFile)
	require.NoError(t, err)

	ts := fromFile(file)
	require.NotNil(t, ts)

	// Should have addenda records
	assert.NotEmpty(t, ts.Addenda.Records, "expected addenda records for COR file")

	// Check addenda type is 98
	for _, record := range ts.Addenda.Records {
		addendaType := record[3] // addenda_type column
		assert.Equal(t, "98", addendaType, "expected Addenda98 type for COR")
	}
}

// TestFromFile_WithIATBatch tests IAT (International ACH Transaction) support
func TestFromFile_WithIATBatch(t *testing.T) {
	testFile := findTestFile(t, "iat-credit.ach")
	if testFile == "" {
		t.Skip("iat-credit.ach not found")
	}

	file, err := ach.ReadFile(testFile)
	require.NoError(t, err)

	ts := fromFile(file)
	require.NotNil(t, ts)

	// IAT batches should be present
	assert.NotNil(t, ts.IATBatches, "IATBatches should not be nil")
	assert.NotEmpty(t, ts.IATBatches.Records, "expected IAT batch records")

	// IAT entries should be present
	assert.NotNil(t, ts.IATEntries, "IATEntries should not be nil")
	assert.NotEmpty(t, ts.IATEntries.Records, "expected IAT entry records")

	// IAT addenda should be present (types 10-18)
	assert.NotNil(t, ts.IATAddenda, "IATAddenda should not be nil")
	assert.NotEmpty(t, ts.IATAddenda.Records, "expected IAT addenda records")

	// Verify we have different addenda types (10, 11, 12, 13, 14, 15, 16, 17, 18)
	addendaTypes := make(map[string]bool)
	for _, record := range ts.IATAddenda.Records {
		addendaType := record[3] // addenda_type column
		addendaTypes[addendaType] = true
	}

	// Should have multiple addenda types
	assert.True(t, len(addendaTypes) > 1, "expected multiple IAT addenda types, got: %v", addendaTypes)
}

// TestIATRoundTrip tests IAT file round-trip conversion
func TestIATRoundTrip(t *testing.T) {
	testFile := findTestFile(t, "iat-credit.ach")
	if testFile == "" {
		t.Skip("iat-credit.ach not found")
	}

	file, err := ach.ReadFile(testFile)
	require.NoError(t, err)

	ts := fromFile(file)
	require.NotNil(t, ts)

	// Round-trip
	newFile, err := ts.toFile()
	require.NoError(t, err)
	require.NotNil(t, newFile)

	// Verify IAT structure preserved
	assert.Len(t, newFile.IATBatches, len(file.IATBatches))
	if len(file.IATBatches) > 0 {
		assert.Len(t, newFile.IATBatches[0].Entries, len(file.IATBatches[0].Entries))
	}
}

// TestParseReader tests parsing ACH from io.Reader
func TestParseReader(t *testing.T) {
	testFile := findTestACHFile(t)
	if testFile == "" {
		t.Skip("No test ACH file found")
	}

	data, err := os.ReadFile(testFile) //nolint:gosec // testFile is a hardcoded test file path
	require.NoError(t, err)

	ts, err := ParseReader(bytes.NewReader(data))
	require.NoError(t, err)
	require.NotNil(t, ts)

	assert.NotEmpty(t, ts.FileHeader.Records)
	assert.NotEmpty(t, ts.Batches.Records)
	assert.NotEmpty(t, ts.Entries.Records)
}

// TestWriteToWriter tests writing ACH to io.Writer
func TestWriteToWriter(t *testing.T) {
	testFile := findTestACHFile(t)
	if testFile == "" {
		t.Skip("No test ACH file found")
	}

	file, err := ach.ReadFile(testFile)
	require.NoError(t, err)

	ts := fromFile(file)
	require.NotNil(t, ts)

	var buf bytes.Buffer
	err = ts.WriteToWriter(&buf)
	require.NoError(t, err)

	assert.NotEmpty(t, buf.String())
	// ACH files have specific line lengths
	assert.True(t, len(buf.String()) > 0, "expected non-empty output")
}

// TestUpdateFromTableData tests updating internal TableData
func TestUpdateFromTableData(t *testing.T) {
	file := createTestACHFile(t)
	ts := fromFile(file)
	require.NotNil(t, ts)

	// Create modified TableData
	newEntries := &parser.TableData{
		Headers: ts.Entries.Headers,
		Records: ts.Entries.Records,
	}
	newBatches := &parser.TableData{
		Headers: ts.Batches.Headers,
		Records: ts.Batches.Records,
	}
	newFileHeader := &parser.TableData{
		Headers: ts.FileHeader.Headers,
		Records: ts.FileHeader.Records,
	}
	newAddenda := &parser.TableData{
		Headers: ts.Addenda.Headers,
		Records: ts.Addenda.Records,
	}

	// Update TableData
	ts.UpdateEntriesFromTableData(newEntries)
	ts.UpdateBatchesFromTableData(newBatches)
	ts.UpdateFileHeaderFromTableData(newFileHeader)
	ts.UpdateAddendaFromTableData(newAddenda)

	// Verify updates
	assert.Equal(t, newEntries, ts.Entries)
	assert.Equal(t, newBatches, ts.Batches)
	assert.Equal(t, newFileHeader, ts.FileHeader)
	assert.Equal(t, newAddenda, ts.Addenda)
}

// TestUpdateIATFromTableData tests updating IAT TableData
func TestUpdateIATFromTableData(t *testing.T) {
	testFile := findTestFile(t, "iat-credit.ach")
	if testFile == "" {
		t.Skip("iat-credit.ach not found")
	}

	file, err := ach.ReadFile(testFile)
	require.NoError(t, err)

	ts := fromFile(file)
	require.NotNil(t, ts)
	require.NotNil(t, ts.IATBatches)

	// Create modified TableData
	newIATBatches := &parser.TableData{
		Headers: ts.IATBatches.Headers,
		Records: ts.IATBatches.Records,
	}
	newIATEntries := &parser.TableData{
		Headers: ts.IATEntries.Headers,
		Records: ts.IATEntries.Records,
	}
	newIATAddenda := &parser.TableData{
		Headers: ts.IATAddenda.Headers,
		Records: ts.IATAddenda.Records,
	}

	// Update TableData
	ts.UpdateIATBatchesFromTableData(newIATBatches)
	ts.UpdateIATEntriesFromTableData(newIATEntries)
	ts.UpdateIATAddendaFromTableData(newIATAddenda)

	// Verify updates
	assert.Equal(t, newIATBatches, ts.IATBatches)
	assert.Equal(t, newIATEntries, ts.IATEntries)
	assert.Equal(t, newIATAddenda, ts.IATAddenda)
}

// TestModifyBatchHeader tests modifying batch header fields
func TestModifyBatchHeader(t *testing.T) {
	file := createTestACHFile(t)
	ts := fromFile(file)
	require.NotNil(t, ts)
	require.NotEmpty(t, ts.Batches.Records)

	// Find company_name column
	companyNameIdx := -1
	for i, h := range ts.Batches.Headers {
		if h == "company_name" {
			companyNameIdx = i
			break
		}
	}
	require.NotEqual(t, -1, companyNameIdx)

	// Modify company name
	originalName := ts.Batches.Records[0][companyNameIdx]
	newName := "Modified Company"
	ts.Batches.Records[0][companyNameIdx] = newName

	// Convert back
	newFile, err := ts.toFile()
	require.NoError(t, err)

	// Verify change
	bh := newFile.Batches[0].GetHeader()
	assert.Equal(t, newName, strings.TrimSpace(bh.CompanyName))
	assert.NotEqual(t, originalName, strings.TrimSpace(bh.CompanyName))
}

// TestModifyFileHeader tests modifying file header fields
func TestModifyFileHeader(t *testing.T) {
	file := createTestACHFile(t)
	ts := fromFile(file)
	require.NotNil(t, ts)
	require.Len(t, ts.FileHeader.Records, 1)

	// Find immediate_destination_name column
	destNameIdx := -1
	for i, h := range ts.FileHeader.Headers {
		if h == "immediate_destination_name" {
			destNameIdx = i
			break
		}
	}
	require.NotEqual(t, -1, destNameIdx)

	// Modify destination name
	newName := "New Destination"
	ts.FileHeader.Records[0][destNameIdx] = newName

	// Convert back
	newFile, err := ts.toFile()
	require.NoError(t, err)

	// Verify change
	assert.Equal(t, newName, strings.TrimSpace(newFile.Header.ImmediateDestinationName))
}

// TestModifyAddenda05 tests modifying Addenda05 records
func TestModifyAddenda05(t *testing.T) {
	// Create a file with Addenda05
	file := ach.NewFile()
	file.Header.ImmediateDestination = "231380104"
	file.Header.ImmediateOrigin = "121042882"
	file.Header.FileCreationDate = "190624"
	file.Header.FileCreationTime = "0000"
	file.Header.FileIDModifier = "A"

	bh := ach.NewBatchHeader()
	bh.ServiceClassCode = ach.DebitsOnly
	bh.CompanyName = "Test Company"
	bh.CompanyIdentification = "121042882"
	bh.StandardEntryClassCode = ach.PPD
	bh.CompanyEntryDescription = "PAYMENT"
	bh.EffectiveEntryDate = "190625"
	bh.ODFIIdentification = "12104288"

	entry := ach.NewEntryDetail()
	entry.TransactionCode = ach.CheckingDebit
	entry.SetRDFI("231380104")
	entry.DFIAccountNumber = "12345678"
	entry.Amount = 100000
	entry.IndividualName = "Test Person"
	entry.SetTraceNumber("12104288", 1)
	entry.AddendaRecordIndicator = 1

	// Add Addenda05
	addenda05 := ach.NewAddenda05()
	addenda05.PaymentRelatedInformation = "Original Payment Info"
	addenda05.SequenceNumber = 1
	addenda05.EntryDetailSequenceNumber = 1
	entry.AddAddenda05(addenda05)

	batch, err := ach.NewBatch(bh)
	require.NoError(t, err)
	batch.AddEntry(entry)
	require.NoError(t, batch.Create())

	file.AddBatch(batch)
	require.NoError(t, file.Create())

	// Convert to TableSet
	ts := fromFile(file)
	require.NotNil(t, ts)
	require.NotEmpty(t, ts.Addenda.Records)

	// Find payment_related_information column
	paymentInfoIdx := -1
	for i, h := range ts.Addenda.Headers {
		if h == "payment_related_information" {
			paymentInfoIdx = i
			break
		}
	}
	require.NotEqual(t, -1, paymentInfoIdx)

	// Modify payment info
	newPaymentInfo := "Modified Payment Info"
	ts.Addenda.Records[0][paymentInfoIdx] = newPaymentInfo

	// Convert back
	newFile, err := ts.toFile()
	require.NoError(t, err)

	// Verify change
	entries := newFile.Batches[0].GetEntries()
	require.Len(t, entries, 1)
	require.Len(t, entries[0].Addenda05, 1)
	assert.Equal(t, newPaymentInfo, entries[0].Addenda05[0].PaymentRelatedInformation)
}

func TestApplyAddendaModifications_AdjustsAddenda05IndexAfterAddenda02(t *testing.T) {
	file := ach.NewFile()

	bh := ach.NewBatchHeader()
	bh.ServiceClassCode = ach.DebitsOnly
	bh.CompanyName = "Test Company"
	bh.CompanyIdentification = "121042882"
	bh.StandardEntryClassCode = ach.PPD
	bh.CompanyEntryDescription = "PAYMENT"
	bh.EffectiveEntryDate = "190625"
	bh.ODFIIdentification = "12104288"

	batcher, err := ach.NewBatch(bh)
	require.NoError(t, err)

	entry := ach.NewEntryDetail()
	entry.TransactionCode = ach.CheckingDebit
	entry.SetRDFI("231380104")
	entry.DFIAccountNumber = "12345678"
	entry.Amount = 100000
	entry.IndividualName = "Test Person"
	entry.SetTraceNumber("12104288", 1)
	entry.AddendaRecordIndicator = 1

	addenda02 := ach.NewAddenda02()
	addenda02.TypeCode = "02"
	addenda02.ReferenceInformationOne = "POSREF1"
	entry.Addenda02 = addenda02

	addenda05 := ach.NewAddenda05()
	addenda05.PaymentRelatedInformation = "Original Payment Info"
	addenda05.SequenceNumber = 1
	addenda05.EntryDetailSequenceNumber = 1
	entry.AddAddenda05(addenda05)

	batcher.AddEntry(entry)
	file.AddBatch(batcher)

	ts := fromFile(file)
	require.NotNil(t, ts)
	require.Len(t, ts.Addenda.Records, 2)

	paymentInfoIdx := findHeaderIndex(t, ts.Addenda.Headers, "payment_related_information")
	require.Equal(t, "02", ts.Addenda.Records[0][3])
	require.Equal(t, "05", ts.Addenda.Records[1][3])

	ts.Addenda.Records[1][paymentInfoIdx] = "Updated Payment Info"

	err = ts.applyAddendaModifications(file)
	require.NoError(t, err)

	entries := file.Batches[0].GetEntries()
	require.Len(t, entries, 1)
	require.Len(t, entries[0].Addenda05, 1)
	assert.Equal(t, "Updated Payment Info", entries[0].Addenda05[0].PaymentRelatedInformation)
}

// TestColumnTypes tests that column types are correctly set
func TestColumnTypes(t *testing.T) {
	file := createTestACHFile(t)
	ts := fromFile(file)
	require.NotNil(t, ts)

	// Check entries column types
	assert.NotEmpty(t, ts.Entries.ColumnTypes)

	// Amount should be integer
	amountIdx := -1
	for i, h := range ts.Entries.Headers {
		if h == "amount" {
			amountIdx = i
			break
		}
	}
	require.NotEqual(t, -1, amountIdx)
	assert.Equal(t, parser.TypeInteger, ts.Entries.ColumnTypes[amountIdx])

	// individual_name should be text
	nameIdx := -1
	for i, h := range ts.Entries.Headers {
		if h == "individual_name" {
			nameIdx = i
			break
		}
	}
	require.NotEqual(t, -1, nameIdx)
	assert.Equal(t, parser.TypeText, ts.Entries.ColumnTypes[nameIdx])
}

// TestFileHeaderColumnTypes tests file header column types
func TestFileHeaderColumnTypes(t *testing.T) {
	file := createTestACHFile(t)
	ts := fromFile(file)
	require.NotNil(t, ts)

	assert.NotEmpty(t, ts.FileHeader.ColumnTypes)
	assert.Len(t, ts.FileHeader.ColumnTypes, len(ts.FileHeader.Headers))

	// All file header fields should be text
	for _, ct := range ts.FileHeader.ColumnTypes {
		assert.Equal(t, parser.TypeText, ct)
	}
}

// TestBatchColumnTypes tests batch column types
func TestBatchColumnTypes(t *testing.T) {
	file := createTestACHFile(t)
	ts := fromFile(file)
	require.NotNil(t, ts)

	assert.NotEmpty(t, ts.Batches.ColumnTypes)
	assert.Len(t, ts.Batches.ColumnTypes, len(ts.Batches.Headers))

	// batch_index should be integer
	batchIdxCol := -1
	for i, h := range ts.Batches.Headers {
		if h == "batch_index" {
			batchIdxCol = i
			break
		}
	}
	require.NotEqual(t, -1, batchIdxCol)
	assert.Equal(t, parser.TypeInteger, ts.Batches.ColumnTypes[batchIdxCol])
}

// TestEmptyAddenda tests file with no addenda records
func TestEmptyAddenda(t *testing.T) {
	file := createTestACHFile(t)
	ts := fromFile(file)
	require.NotNil(t, ts)

	// Addenda should be initialized but may be empty
	assert.NotNil(t, ts.Addenda)
	assert.NotNil(t, ts.Addenda.Headers)
	// Empty records should be an empty slice, not nil
	assert.NotNil(t, ts.Addenda.Records)
}

// TestMultipleBatches tests file with multiple batches
func TestMultipleBatches(t *testing.T) {
	// Create a file with multiple batches
	file := ach.NewFile()
	file.Header.ImmediateDestination = "231380104"
	file.Header.ImmediateOrigin = "121042882"
	file.Header.FileCreationDate = "190624"
	file.Header.FileCreationTime = "0000"
	file.Header.FileIDModifier = "A"

	// Create two batches
	for batchNum := range []int{0, 1} {
		bh := ach.NewBatchHeader()
		bh.ServiceClassCode = ach.DebitsOnly
		bh.CompanyName = "Company " + strconv.Itoa(batchNum)
		bh.CompanyIdentification = "121042882"
		bh.StandardEntryClassCode = ach.PPD
		bh.CompanyEntryDescription = "PAYMENT"
		bh.EffectiveEntryDate = "190625"
		bh.ODFIIdentification = "12104288"
		bh.BatchNumber = batchNum + 1

		entry := ach.NewEntryDetail()
		entry.TransactionCode = ach.CheckingDebit
		entry.SetRDFI("231380104")
		entry.DFIAccountNumber = "12345678"
		entry.Amount = 100000 * (batchNum + 1)
		entry.IndividualName = "Person " + strconv.Itoa(batchNum)
		entry.SetTraceNumber("12104288", batchNum+1)

		batch, err := ach.NewBatch(bh)
		require.NoError(t, err)
		batch.AddEntry(entry)
		require.NoError(t, batch.Create())

		file.AddBatch(batch)
	}
	require.NoError(t, file.Create())

	// Convert to TableSet
	ts := fromFile(file)
	require.NotNil(t, ts)

	// Should have 2 batch records
	assert.Len(t, ts.Batches.Records, 2)

	// Should have 2 entry records
	assert.Len(t, ts.Entries.Records, 2)

	// Verify batch indices
	assert.Equal(t, "0", ts.Entries.Records[0][0]) // First entry in batch 0
	assert.Equal(t, "1", ts.Entries.Records[1][0]) // Second entry in batch 1
}

// TestToFile_NilOriginalFile tests toFile with nil original file
func TestToFile_NilOriginalFile(t *testing.T) {
	ts := &TableSet{
		originalFile: nil,
	}

	file, err := ts.toFile()
	assert.Error(t, err)
	assert.Nil(t, file)
	assert.Contains(t, err.Error(), "no original ACH file")
}

// TestModifyIATBatchHeader tests modifying IAT batch header fields
func TestModifyIATBatchHeader(t *testing.T) {
	testFile := findTestFile(t, "iat-credit.ach")
	if testFile == "" {
		t.Skip("iat-credit.ach not found")
	}

	file, err := ach.ReadFile(testFile)
	require.NoError(t, err)

	ts := fromFile(file)
	require.NotNil(t, ts)
	require.NotEmpty(t, ts.IATBatches.Records)

	// Find company_entry_description column
	descIdx := -1
	for i, h := range ts.IATBatches.Headers {
		if h == "company_entry_description" {
			descIdx = i
			break
		}
	}
	require.NotEqual(t, -1, descIdx)

	// Modify description
	newDesc := "NEWPAYMENT"
	ts.IATBatches.Records[0][descIdx] = newDesc

	// Convert back
	newFile, err := ts.toFile()
	require.NoError(t, err)

	// Verify change
	assert.Equal(t, newDesc, strings.TrimSpace(newFile.IATBatches[0].Header.CompanyEntryDescription))
}

// TestModifyIATEntry tests modifying IAT entry fields
func TestModifyIATEntry(t *testing.T) {
	testFile := findTestFile(t, "iat-credit.ach")
	if testFile == "" {
		t.Skip("iat-credit.ach not found")
	}

	file, err := ach.ReadFile(testFile)
	require.NoError(t, err)

	ts := fromFile(file)
	require.NotNil(t, ts)
	require.NotEmpty(t, ts.IATEntries.Records)

	// Find amount column
	amountIdx := -1
	for i, h := range ts.IATEntries.Headers {
		if h == "amount" {
			amountIdx = i
			break
		}
	}
	require.NotEqual(t, -1, amountIdx)

	// Modify amount
	newAmount := 200000
	ts.IATEntries.Records[0][amountIdx] = strconv.Itoa(newAmount)

	// Convert back
	newFile, err := ts.toFile()
	require.NoError(t, err)

	// Verify change
	assert.Equal(t, newAmount, newFile.IATBatches[0].Entries[0].Amount)
}

// TestModifyIATAddenda tests modifying IAT addenda fields
func TestModifyIATAddenda(t *testing.T) {
	testFile := findTestFile(t, "iat-credit.ach")
	if testFile == "" {
		t.Skip("iat-credit.ach not found")
	}

	file, err := ach.ReadFile(testFile)
	require.NoError(t, err)

	ts := fromFile(file)
	require.NotNil(t, ts)
	require.NotEmpty(t, ts.IATAddenda.Records)

	// Find a record with addenda_type "11" (Addenda11 - Originator Name)
	var addenda11RecordIdx = -1
	addendaTypeIdx := -1
	for i, h := range ts.IATAddenda.Headers {
		if h == "addenda_type" {
			addendaTypeIdx = i
			break
		}
	}
	require.NotEqual(t, -1, addendaTypeIdx)

	for i, record := range ts.IATAddenda.Records {
		if record[addendaTypeIdx] == "11" {
			addenda11RecordIdx = i
			break
		}
	}
	require.NotEqual(t, -1, addenda11RecordIdx, "Addenda11 record not found")

	// Find originator_name column
	originatorNameIdx := -1
	for i, h := range ts.IATAddenda.Headers {
		if h == "originator_name" {
			originatorNameIdx = i
			break
		}
	}
	require.NotEqual(t, -1, originatorNameIdx)

	// Modify originator name
	newName := "Modified Originator"
	ts.IATAddenda.Records[addenda11RecordIdx][originatorNameIdx] = newName

	// Convert back
	newFile, err := ts.toFile()
	require.NoError(t, err)

	// Verify change
	require.NotNil(t, newFile.IATBatches[0].Entries[0].Addenda11)
	assert.Equal(t, newName, newFile.IATBatches[0].Entries[0].Addenda11.OriginatorName)
}

// TestAddenda02Handling tests Addenda02 (POS, MTE, SHR) records
func TestAddenda02Handling(t *testing.T) {
	testFile := findTestFile(t, "pos-debit.ach")
	if testFile == "" {
		t.Skip("pos-debit.ach not found")
	}

	file, err := ach.ReadFile(testFile)
	require.NoError(t, err)

	ts := fromFile(file)
	require.NotNil(t, ts)

	// Check if there are Addenda02 records
	addenda02Found := false
	for _, record := range ts.Addenda.Records {
		if record[3] == "02" { // addenda_type column
			addenda02Found = true
			break
		}
	}

	if !addenda02Found {
		t.Skip("No Addenda02 records in test file")
	}

	// Verify round-trip
	newFile, err := ts.toFile()
	require.NoError(t, err)
	require.NotNil(t, newFile)
}

// TestEmptyFileHeaderRecords tests edge case with empty file header
func TestEmptyFileHeaderRecords(t *testing.T) {
	file := createTestACHFile(t)
	ts := fromFile(file)
	require.NotNil(t, ts)

	// Clear file header records
	ts.FileHeader.Records = [][]string{}

	// Convert back - should not panic
	newFile, err := ts.toFile()
	require.NoError(t, err)
	require.NotNil(t, newFile)
}

// createTestACHFile creates a minimal valid ACH file for testing.
// Uses an actual test file from the moov-io/ach repository.
func createTestACHFile(t *testing.T) *ach.File {
	t.Helper()

	testFile := findTestACHFile(t)
	if testFile == "" {
		t.Skip("No test ACH file found")
	}

	file, err := ach.ReadFile(testFile)
	require.NoError(t, err)

	return file
}

// findTestACHFile returns the path to a test ACH file in testdata directory.
func findTestACHFile(t *testing.T) string {
	t.Helper()
	return filepath.Join("testdata", "ppd-debit.ach")
}

// findTestFile returns the path to a specific test file in testdata directory.
func findTestFile(t *testing.T, filename string) string {
	t.Helper()
	return filepath.Join("testdata", filename)
}

// TestModifyAddenda98 tests modifying Addenda98 (Notification of Change) records
func TestModifyAddenda98(t *testing.T) {
	testFile := findTestFile(t, "cor-example.ach")
	if testFile == "" {
		t.Skip("cor-example.ach not found")
	}

	file, err := ach.ReadFile(testFile)
	require.NoError(t, err)

	ts := fromFile(file)
	require.NotNil(t, ts)
	require.NotEmpty(t, ts.Addenda.Records)

	// Find Addenda98 record
	var addenda98RecordIdx = -1
	addendaTypeIdx := -1
	for i, h := range ts.Addenda.Headers {
		if h == "addenda_type" {
			addendaTypeIdx = i
			break
		}
	}
	require.NotEqual(t, -1, addendaTypeIdx)

	for i, record := range ts.Addenda.Records {
		if record[addendaTypeIdx] == "98" {
			addenda98RecordIdx = i
			break
		}
	}
	require.NotEqual(t, -1, addenda98RecordIdx, "Addenda98 record not found")

	// Find change_code column
	changeCodeIdx := -1
	for i, h := range ts.Addenda.Headers {
		if h == "change_code" {
			changeCodeIdx = i
			break
		}
	}
	require.NotEqual(t, -1, changeCodeIdx)

	// Find corrected_data column
	correctedDataIdx := -1
	for i, h := range ts.Addenda.Headers {
		if h == "corrected_data" {
			correctedDataIdx = i
			break
		}
	}
	require.NotEqual(t, -1, correctedDataIdx)

	// Modify change code and corrected data
	newChangeCode := "C02"
	newCorrectedData := "999999999"
	ts.Addenda.Records[addenda98RecordIdx][changeCodeIdx] = newChangeCode
	ts.Addenda.Records[addenda98RecordIdx][correctedDataIdx] = newCorrectedData

	// Convert back
	newFile, err := ts.toFile()
	require.NoError(t, err)

	// Find the entry with Addenda98 and verify changes
	entries := newFile.Batches[0].GetEntries()
	require.NotEmpty(t, entries)

	var foundAddenda98 *ach.Addenda98
	for _, entry := range entries {
		if entry.Addenda98 != nil {
			foundAddenda98 = entry.Addenda98
			break
		}
	}
	require.NotNil(t, foundAddenda98, "Addenda98 not found in output file")
	assert.Equal(t, newChangeCode, foundAddenda98.ChangeCode)
	assert.Equal(t, newCorrectedData, strings.TrimSpace(foundAddenda98.CorrectedData))
}

// TestModifyAddenda99 tests modifying Addenda99 (Return) records
func TestModifyAddenda99(t *testing.T) {
	testFile := findTestFile(t, "return-WEB.ach")
	if testFile == "" {
		t.Skip("return-WEB.ach not found")
	}

	file, err := ach.ReadFile(testFile)
	require.NoError(t, err)

	ts := fromFile(file)
	require.NotNil(t, ts)
	require.NotEmpty(t, ts.Addenda.Records)

	// Find Addenda99 record
	var addenda99RecordIdx = -1
	addendaTypeIdx := -1
	for i, h := range ts.Addenda.Headers {
		if h == "addenda_type" {
			addendaTypeIdx = i
			break
		}
	}
	require.NotEqual(t, -1, addendaTypeIdx)

	for i, record := range ts.Addenda.Records {
		if record[addendaTypeIdx] == "99" {
			addenda99RecordIdx = i
			break
		}
	}
	require.NotEqual(t, -1, addenda99RecordIdx, "Addenda99 record not found")

	// Find return_code column
	returnCodeIdx := -1
	for i, h := range ts.Addenda.Headers {
		if h == "return_code" {
			returnCodeIdx = i
			break
		}
	}
	require.NotEqual(t, -1, returnCodeIdx)

	// Find addenda_information column
	addendaInfoIdx := -1
	for i, h := range ts.Addenda.Headers {
		if h == "addenda_information" {
			addendaInfoIdx = i
			break
		}
	}
	require.NotEqual(t, -1, addendaInfoIdx)

	// Modify return code and addenda information
	newReturnCode := "R02"
	newAddendaInfo := "MODIFIED INFO"
	ts.Addenda.Records[addenda99RecordIdx][returnCodeIdx] = newReturnCode
	ts.Addenda.Records[addenda99RecordIdx][addendaInfoIdx] = newAddendaInfo

	// Convert back
	newFile, err := ts.toFile()
	require.NoError(t, err)

	// Find the entry with Addenda99 and verify changes
	var foundAddenda99 *ach.Addenda99
	for _, batch := range newFile.Batches {
		for _, entry := range batch.GetEntries() {
			if entry.Addenda99 != nil {
				foundAddenda99 = entry.Addenda99
				break
			}
		}
		if foundAddenda99 != nil {
			break
		}
	}
	require.NotNil(t, foundAddenda99, "Addenda99 not found in output file")
	assert.Equal(t, newReturnCode, foundAddenda99.ReturnCode)
	assert.Equal(t, newAddendaInfo, strings.TrimSpace(foundAddenda99.AddendaInformation))
}

// TestModifyAddenda98Refused tests modifying Addenda98Refused records
func TestModifyAddenda98Refused(t *testing.T) {
	// Create a file with Addenda98Refused programmatically
	file := ach.NewFile()
	file.Header.ImmediateDestination = "231380104"
	file.Header.ImmediateOrigin = "121042882"
	file.Header.FileCreationDate = "190624"
	file.Header.FileCreationTime = "0000"
	file.Header.FileIDModifier = "A"

	bh := ach.NewBatchHeader()
	bh.ServiceClassCode = ach.CreditsOnly
	bh.CompanyName = "Test Company"
	bh.CompanyIdentification = "121042882"
	bh.StandardEntryClassCode = ach.COR
	bh.CompanyEntryDescription = "REFUSED"
	bh.EffectiveEntryDate = "190625"
	bh.ODFIIdentification = "12104288"

	entry := ach.NewEntryDetail()
	entry.TransactionCode = ach.CheckingReturnNOCCredit
	entry.SetRDFI("231380104")
	entry.DFIAccountNumber = "12345678"
	entry.Amount = 0
	entry.IndividualName = "Test Person"
	entry.SetTraceNumber("12104288", 1)
	entry.AddendaRecordIndicator = 1

	// Add Addenda98Refused
	addenda98Refused := ach.NewAddenda98Refused()
	addenda98Refused.RefusedChangeCode = "C01"
	addenda98Refused.OriginalTrace = "121042880000001"
	addenda98Refused.OriginalDFI = "12104288"
	addenda98Refused.CorrectedData = "123456789"
	addenda98Refused.ChangeCode = "C01"
	addenda98Refused.TraceSequenceNumber = "0000001"
	addenda98Refused.TraceNumber = "091012980000001"
	entry.Addenda98Refused = addenda98Refused
	entry.Category = ach.CategoryNOC

	batch, err := ach.NewBatch(bh)
	require.NoError(t, err)
	batch.AddEntry(entry)
	require.NoError(t, batch.Create())

	file.AddBatch(batch)
	require.NoError(t, file.Create())

	// Convert to TableSet
	ts := fromFile(file)
	require.NotNil(t, ts)
	require.NotEmpty(t, ts.Addenda.Records)

	// Find Addenda98Refused record
	var addenda98RefusedRecordIdx = -1
	addendaTypeIdx := -1
	for i, h := range ts.Addenda.Headers {
		if h == "addenda_type" {
			addendaTypeIdx = i
			break
		}
	}
	require.NotEqual(t, -1, addendaTypeIdx)

	for i, record := range ts.Addenda.Records {
		if record[addendaTypeIdx] == "98_refused" {
			addenda98RefusedRecordIdx = i
			break
		}
	}
	require.NotEqual(t, -1, addenda98RefusedRecordIdx, "Addenda98Refused record not found")

	// Find refused_change_code column
	refusedChangeCodeIdx := -1
	for i, h := range ts.Addenda.Headers {
		if h == "refused_change_code" {
			refusedChangeCodeIdx = i
			break
		}
	}
	require.NotEqual(t, -1, refusedChangeCodeIdx)

	// Find corrected_data column
	correctedDataIdx := -1
	for i, h := range ts.Addenda.Headers {
		if h == "corrected_data" {
			correctedDataIdx = i
			break
		}
	}
	require.NotEqual(t, -1, correctedDataIdx)

	// Modify refused change code and corrected data
	newRefusedChangeCode := "C02"
	newCorrectedData := "987654321"
	ts.Addenda.Records[addenda98RefusedRecordIdx][refusedChangeCodeIdx] = newRefusedChangeCode
	ts.Addenda.Records[addenda98RefusedRecordIdx][correctedDataIdx] = newCorrectedData

	// Convert back
	newFile, err := ts.toFile()
	require.NoError(t, err)

	// Find the entry with Addenda98Refused and verify changes
	entries := newFile.Batches[0].GetEntries()
	require.NotEmpty(t, entries)

	var foundAddenda98Refused *ach.Addenda98Refused
	for _, entry := range entries {
		if entry.Addenda98Refused != nil {
			foundAddenda98Refused = entry.Addenda98Refused
			break
		}
	}
	require.NotNil(t, foundAddenda98Refused, "Addenda98Refused not found in output file")
	assert.Equal(t, newRefusedChangeCode, foundAddenda98Refused.RefusedChangeCode)
	assert.Equal(t, newCorrectedData, strings.TrimSpace(foundAddenda98Refused.CorrectedData))
}

// TestModifyAddenda99Dishonored tests modifying Addenda99Dishonored records
func TestModifyAddenda99Dishonored(t *testing.T) {
	// Create a file with Addenda99Dishonored programmatically
	file := ach.NewFile()
	file.Header.ImmediateDestination = "231380104"
	file.Header.ImmediateOrigin = "121042882"
	file.Header.FileCreationDate = "190624"
	file.Header.FileCreationTime = "0000"
	file.Header.FileIDModifier = "A"

	bh := ach.NewBatchHeader()
	bh.ServiceClassCode = ach.DebitsOnly
	bh.CompanyName = "Test Company"
	bh.CompanyIdentification = "121042882"
	bh.StandardEntryClassCode = ach.PPD
	bh.CompanyEntryDescription = "DISHONORED"
	bh.EffectiveEntryDate = "190625"
	bh.ODFIIdentification = "12104288"

	entry := ach.NewEntryDetail()
	entry.TransactionCode = ach.CheckingDebit
	entry.SetRDFI("231380104")
	entry.DFIAccountNumber = "12345678"
	entry.Amount = 100000
	entry.IndividualName = "Test Person"
	entry.SetTraceNumber("12104288", 1)
	entry.AddendaRecordIndicator = 1

	// Add Addenda99Dishonored
	addenda99Dishonored := ach.NewAddenda99Dishonored()
	addenda99Dishonored.DishonoredReturnReasonCode = "R69"
	addenda99Dishonored.OriginalEntryTraceNumber = "121042880000001"
	addenda99Dishonored.OriginalReceivingDFIIdentification = "23138010"
	addenda99Dishonored.ReturnTraceNumber = "091000010000001"
	addenda99Dishonored.ReturnSettlementDate = "190"
	addenda99Dishonored.ReturnReasonCode = "R01"
	addenda99Dishonored.AddendaInformation = "ORIGINAL INFO"
	addenda99Dishonored.TraceNumber = "091012980000001"
	entry.Addenda99Dishonored = addenda99Dishonored
	entry.Category = ach.CategoryDishonoredReturn

	batch, err := ach.NewBatch(bh)
	require.NoError(t, err)
	batch.AddEntry(entry)
	require.NoError(t, batch.Create())

	file.AddBatch(batch)
	require.NoError(t, file.Create())

	// Convert to TableSet
	ts := fromFile(file)
	require.NotNil(t, ts)
	require.NotEmpty(t, ts.Addenda.Records)

	// Find Addenda99Dishonored record
	var addenda99DishonoredRecordIdx = -1
	addendaTypeIdx := -1
	for i, h := range ts.Addenda.Headers {
		if h == "addenda_type" {
			addendaTypeIdx = i
			break
		}
	}
	require.NotEqual(t, -1, addendaTypeIdx)

	for i, record := range ts.Addenda.Records {
		if record[addendaTypeIdx] == "99_dishonored" {
			addenda99DishonoredRecordIdx = i
			break
		}
	}
	require.NotEqual(t, -1, addenda99DishonoredRecordIdx, "Addenda99Dishonored record not found")

	// Find return_reason_code column (unique to Addenda99Dishonored)
	returnReasonCodeIdx := -1
	for i, h := range ts.Addenda.Headers {
		if h == "return_reason_code" {
			returnReasonCodeIdx = i
			break
		}
	}
	require.NotEqual(t, -1, returnReasonCodeIdx)

	// Find addenda_information column
	addendaInfoIdx := -1
	for i, h := range ts.Addenda.Headers {
		if h == "addenda_information" {
			addendaInfoIdx = i
			break
		}
	}
	require.NotEqual(t, -1, addendaInfoIdx)

	// Modify return reason code and addenda information
	newReturnReasonCode := "R02"
	newAddendaInfo := "MODIFIED DISHONORED INFO"
	ts.Addenda.Records[addenda99DishonoredRecordIdx][returnReasonCodeIdx] = newReturnReasonCode
	ts.Addenda.Records[addenda99DishonoredRecordIdx][addendaInfoIdx] = newAddendaInfo

	// Convert back
	newFile, err := ts.toFile()
	require.NoError(t, err)

	// Find the entry with Addenda99Dishonored and verify changes
	entries := newFile.Batches[0].GetEntries()
	require.NotEmpty(t, entries)

	var foundAddenda99Dishonored *ach.Addenda99Dishonored
	for _, entry := range entries {
		if entry.Addenda99Dishonored != nil {
			foundAddenda99Dishonored = entry.Addenda99Dishonored
			break
		}
	}
	require.NotNil(t, foundAddenda99Dishonored, "Addenda99Dishonored not found in output file")
	assert.Equal(t, newReturnReasonCode, foundAddenda99Dishonored.ReturnReasonCode)
	assert.Equal(t, newAddendaInfo, strings.TrimSpace(foundAddenda99Dishonored.AddendaInformation))
}

// TestModifyAddenda99Contested tests modifying Addenda99Contested records
func TestModifyAddenda99Contested(t *testing.T) {
	// Create a file with Addenda99Contested programmatically
	file := ach.NewFile()
	file.Header.ImmediateDestination = "231380104"
	file.Header.ImmediateOrigin = "121042882"
	file.Header.FileCreationDate = "190624"
	file.Header.FileCreationTime = "0000"
	file.Header.FileIDModifier = "A"

	bh := ach.NewBatchHeader()
	bh.ServiceClassCode = ach.DebitsOnly
	bh.CompanyName = "Test Company"
	bh.CompanyIdentification = "121042882"
	bh.StandardEntryClassCode = ach.PPD
	bh.CompanyEntryDescription = "CONTESTED"
	bh.EffectiveEntryDate = "190625"
	bh.ODFIIdentification = "12104288"

	entry := ach.NewEntryDetail()
	entry.TransactionCode = ach.CheckingDebit
	entry.SetRDFI("231380104")
	entry.DFIAccountNumber = "12345678"
	entry.Amount = 100000
	entry.IndividualName = "Test Person"
	entry.SetTraceNumber("12104288", 1)
	entry.AddendaRecordIndicator = 1

	// Add Addenda99Contested
	addenda99Contested := ach.NewAddenda99Contested()
	addenda99Contested.ContestedReturnCode = "R71"
	addenda99Contested.OriginalEntryTraceNumber = "121042880000001"
	addenda99Contested.DateOriginalEntryReturned = "190624"
	addenda99Contested.OriginalReceivingDFIIdentification = "23138010"
	addenda99Contested.OriginalSettlementDate = "189"
	addenda99Contested.ReturnTraceNumber = "091000010000001"
	addenda99Contested.ReturnSettlementDate = "190"
	addenda99Contested.ReturnReasonCode = "R01"
	addenda99Contested.DishonoredReturnTraceNumber = "091000020000001"
	addenda99Contested.DishonoredReturnSettlementDate = "191"
	addenda99Contested.DishonoredReturnReasonCode = "R69"
	addenda99Contested.TraceNumber = "091012980000001"
	entry.Addenda99Contested = addenda99Contested
	entry.Category = ach.CategoryDishonoredReturnContested

	batch, err := ach.NewBatch(bh)
	require.NoError(t, err)
	batch.AddEntry(entry)
	require.NoError(t, batch.Create())

	file.AddBatch(batch)
	require.NoError(t, file.Create())

	// Convert to TableSet
	ts := fromFile(file)
	require.NotNil(t, ts)
	require.NotEmpty(t, ts.Addenda.Records)

	// Find Addenda99Contested record
	var addenda99ContestedRecordIdx = -1
	addendaTypeIdx := -1
	for i, h := range ts.Addenda.Headers {
		if h == "addenda_type" {
			addendaTypeIdx = i
			break
		}
	}
	require.NotEqual(t, -1, addendaTypeIdx)

	for i, record := range ts.Addenda.Records {
		if record[addendaTypeIdx] == "99_contested" {
			addenda99ContestedRecordIdx = i
			break
		}
	}
	require.NotEqual(t, -1, addenda99ContestedRecordIdx, "Addenda99Contested record not found")

	// Find contested_return_code column
	contestedReturnCodeIdx := -1
	for i, h := range ts.Addenda.Headers {
		if h == "contested_return_code" {
			contestedReturnCodeIdx = i
			break
		}
	}
	require.NotEqual(t, -1, contestedReturnCodeIdx)

	// Find original_settlement_date column (unique to Addenda99Contested)
	originalSettlementDateIdx := -1
	for i, h := range ts.Addenda.Headers {
		if h == "original_settlement_date" {
			originalSettlementDateIdx = i
			break
		}
	}
	require.NotEqual(t, -1, originalSettlementDateIdx)

	// Modify contested return code and original settlement date
	newContestedReturnCode := "R72"
	newOriginalSettlementDate := "200"
	ts.Addenda.Records[addenda99ContestedRecordIdx][contestedReturnCodeIdx] = newContestedReturnCode
	ts.Addenda.Records[addenda99ContestedRecordIdx][originalSettlementDateIdx] = newOriginalSettlementDate

	// Convert back
	newFile, err := ts.toFile()
	require.NoError(t, err)

	// Find the entry with Addenda99Contested and verify changes
	entries := newFile.Batches[0].GetEntries()
	require.NotEmpty(t, entries)

	var foundAddenda99Contested *ach.Addenda99Contested
	for _, entry := range entries {
		if entry.Addenda99Contested != nil {
			foundAddenda99Contested = entry.Addenda99Contested
			break
		}
	}
	require.NotNil(t, foundAddenda99Contested, "Addenda99Contested not found in output file")
	assert.Equal(t, newContestedReturnCode, foundAddenda99Contested.ContestedReturnCode)
	assert.Equal(t, newOriginalSettlementDate, foundAddenda99Contested.OriginalSettlementDate)
}

// TestApplyModifications_RefusesAMovedCoordinate drives the fault the index
// columns had. batch_index, entry_index and addenda_index say which record a
// row updates; they are not values the row stores. Nothing stopped a caller
// from changing them, and a changed one retargeted the update: a row pointed at
// another row's coordinates overwrote that record's account number, amount and
// trace number while the row that was edited came back unchanged, so the table
// read as if the edit had been ignored and a different entry had silently
// become a copy of this one.
//
// The same mistake was already an error when the new coordinate happened to be
// out of range, so whether a caller was told depended only on how many batches
// the file had.
func TestApplyModifications_RefusesAMovedCoordinate(t *testing.T) {
	t.Parallel()

	load := func(t *testing.T) ([]byte, *TableSet) {
		t.Helper()
		// return-WEB.ach carries two batches with one entry each, which is
		// what makes the retargeted coordinate land in range.
		raw, err := os.ReadFile(filepath.Join("testdata", "return-WEB.ach"))
		require.NoError(t, err)
		ts, err := ParseReader(bytes.NewReader(raw))
		require.NoError(t, err)
		return raw, ts
	}

	column := func(td *parser.TableData, name string) int {
		for i, h := range td.Headers {
			if h == name {
				return i
			}
		}
		return -1
	}

	t.Run("an entry pointed at another entry is refused", func(t *testing.T) {
		t.Parallel()

		_, ts := load(t)
		require.GreaterOrEqual(t, len(ts.Entries.Records), 2, "the fixture must have two entries")

		batchIdx := column(ts.Entries, "batch_index")
		entryIdx := column(ts.Entries, "entry_index")
		before := append([]string(nil), ts.Entries.Records[0]...)

		// No value column is touched. Only the coordinates move.
		ts.Entries.Records[1][batchIdx] = ts.Entries.Records[0][batchIdx]
		ts.Entries.Records[1][entryIdx] = ts.Entries.Records[0][entryIdx]
		ts.UpdateEntriesFromTableData(ts.Entries)

		var buf bytes.Buffer
		err := ts.WriteToWriter(&buf)
		require.Error(t, err, "a row that names another row's record must be refused")
		assert.Contains(t, err.Error(), "batch_index", "the error must name the column")

		// Nothing may have been written, so the entry that was not edited is
		// still what it was.
		if buf.Len() > 0 {
			back, parseErr := ParseReader(bytes.NewReader(buf.Bytes()))
			require.NoError(t, parseErr)
			assert.Equal(t, before, back.Entries.Records[0],
				"the entry nobody edited must not be overwritten")
		}
	})

	t.Run("an out-of-range coordinate keeps reporting its own error", func(t *testing.T) {
		t.Parallel()

		_, ts := load(t)
		batchIdx := column(ts.Entries, "batch_index")
		ts.Entries.Records[0][batchIdx] = "99"
		ts.UpdateEntriesFromTableData(ts.Entries)

		var buf bytes.Buffer
		err := ts.WriteToWriter(&buf)
		require.Error(t, err, "a coordinate outside the file must stay an error")
		assert.Contains(t, err.Error(), "batch_index")
	})

	t.Run("editing a value column is still allowed", func(t *testing.T) {
		t.Parallel()

		_, ts := load(t)
		nameIdx := column(ts.Entries, "individual_name")
		require.GreaterOrEqual(t, nameIdx, 0)
		ts.Entries.Records[0][nameIdx] = "EDITED NAME"
		ts.UpdateEntriesFromTableData(ts.Entries)

		var buf bytes.Buffer
		require.NoError(t, ts.WriteToWriter(&buf), "the ordinary edit must keep working")

		back, err := ParseReader(bytes.NewReader(buf.Bytes()))
		require.NoError(t, err)
		assert.Equal(t, "EDITED NAME", back.Entries.Records[0][nameIdx])
	})

	t.Run("two rows that exchange coordinates are refused", func(t *testing.T) {
		t.Parallel()

		// Every coordinate stays unique and in range here, so nothing but the
		// row's own parsed position tells the swap apart from an honest edit.
		// The damage is real: each row's values land on the other's record, so
		// two payments trade account numbers and amounts.
		_, ts := load(t)
		require.GreaterOrEqual(t, len(ts.Entries.Records), 2)

		batchIdx := column(ts.Entries, "batch_index")
		entryIdx := column(ts.Entries, "entry_index")
		first := []string{ts.Entries.Records[0][batchIdx], ts.Entries.Records[0][entryIdx]}
		ts.Entries.Records[0][batchIdx] = ts.Entries.Records[1][batchIdx]
		ts.Entries.Records[0][entryIdx] = ts.Entries.Records[1][entryIdx]
		ts.Entries.Records[1][batchIdx] = first[0]
		ts.Entries.Records[1][entryIdx] = first[1]
		ts.UpdateEntriesFromTableData(ts.Entries)

		var buf bytes.Buffer
		err := ts.WriteToWriter(&buf)
		require.Error(t, err, "two rows trading positions must be refused")
		assert.Contains(t, err.Error(), "batch_index")
		assert.Zero(t, buf.Len(), "nothing may be written when the write is refused")
	})

	t.Run("an addenda_index naming no addenda is refused", func(t *testing.T) {
		t.Parallel()

		// A singleton addenda ignores addenda_index when it applies the row, so
		// before the coordinate was checked an out-of-range value still updated
		// the record it happened to land beside.
		raw, err := os.ReadFile(filepath.Join("testdata", "cor-example.ach"))
		require.NoError(t, err)
		ts, err := ParseReader(bytes.NewReader(raw))
		require.NoError(t, err)
		require.NotEmpty(t, ts.Addenda.Records)

		at := column(ts.Addenda, "addenda_index")
		require.GreaterOrEqual(t, at, 0)
		ts.Addenda.Records[0][at] = "9"
		ts.UpdateAddendaFromTableData(ts.Addenda)

		var buf bytes.Buffer
		writeErr := ts.WriteToWriter(&buf)
		require.Error(t, writeErr, "an addenda_index naming no addenda must be refused")
		assert.Contains(t, writeErr.Error(), "addenda_index")
	})

	t.Run("a row added to the table is refused", func(t *testing.T) {
		t.Parallel()

		// Adding rows was never supported; it used to be applied anyway, to
		// whatever record the new row's coordinates named.
		_, ts := load(t)
		ts.Entries.Records = append(ts.Entries.Records, append([]string(nil), ts.Entries.Records[0]...))
		ts.UpdateEntriesFromTableData(ts.Entries)

		var buf bytes.Buffer
		assert.Error(t, ts.WriteToWriter(&buf), "a row the file did not have must be refused")
	})

	t.Run("every table guards its own coordinates", func(t *testing.T) {
		t.Parallel()

		raw, err := os.ReadFile(filepath.Join("testdata", "iat-credit.ach"))
		require.NoError(t, err)

		for _, tt := range []struct {
			name  string
			get   func(*TableSet) *parser.TableData
			apply func(*TableSet, *parser.TableData)
			coord string
			bogus string
		}{
			{"Batches", func(ts *TableSet) *parser.TableData { return ts.Batches },
				func(ts *TableSet, td *parser.TableData) { ts.UpdateBatchesFromTableData(td) }, "batch_index", "7"},
			{"IATBatches", func(ts *TableSet) *parser.TableData { return ts.IATBatches },
				func(ts *TableSet, td *parser.TableData) { ts.UpdateIATBatchesFromTableData(td) }, "batch_index", "7"},
			{"IATEntries", func(ts *TableSet) *parser.TableData { return ts.IATEntries },
				func(ts *TableSet, td *parser.TableData) { ts.UpdateIATEntriesFromTableData(td) }, "entry_index", "7"},
			{"IATAddenda", func(ts *TableSet) *parser.TableData { return ts.IATAddenda },
				func(ts *TableSet, td *parser.TableData) { ts.UpdateIATAddendaFromTableData(td) }, "addenda_index", "7"},
		} {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()

				ts, parseErr := ParseReader(bytes.NewReader(raw))
				require.NoError(t, parseErr)
				td := tt.get(ts)
				if td == nil || len(td.Records) == 0 {
					t.Skipf("the fixture has no %s rows", tt.name)
				}
				at := column(td, tt.coord)
				require.GreaterOrEqual(t, at, 0, "%s has no %s column", tt.name, tt.coord)

				td.Records[0][at] = tt.bogus
				tt.apply(ts, td)

				var buf bytes.Buffer
				assert.Error(t, ts.WriteToWriter(&buf),
					"%s must refuse a row whose %s names no record it was parsed with", tt.name, tt.coord)
			})
		}
	})
}

// TestCoordinateKey_ComparesThePositionNotTheSpelling pins the rule the
// coordinate check compares by. These columns are integers, and a table that
// has been through SQLite comes back with its numbers spelled the way SQLite
// spells them, so a coordinate is the record it names rather than the text it
// arrived as: "0" and "00" are one position and a round trip does not lose a
// row for having lost a leading zero.
func TestCoordinateKey_ComparesThePositionNotTheSpelling(t *testing.T) {
	t.Parallel()

	raw, err := os.ReadFile(filepath.Join("testdata", "ppd-debit.ach"))
	require.NoError(t, err)
	ts, err := ParseReader(bytes.NewReader(raw))
	require.NoError(t, err)
	require.NotEmpty(t, ts.Entries.Records)

	at := findHeaderIndex(t, ts.Entries.Headers, "batch_index")
	require.Equal(t, "0", ts.Entries.Records[0][at], "the fixture must start at batch 0")
	ts.Entries.Records[0][at] = "00"
	ts.UpdateEntriesFromTableData(ts.Entries)

	var buf bytes.Buffer
	assert.NoError(t, ts.WriteToWriter(&buf),
		"a coordinate respelled as the same number names the same record")
}
