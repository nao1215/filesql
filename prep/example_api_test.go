package prep_test

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"log"
	"strings"

	"github.com/nao1215/filesql/prep"
)

func ExampleWithStrictTagParsing() {
	// eq on a numeric field needs a numeric parameter; on a string field the
	// same tag would compare the string "abc" itself.
	type record struct {
		Value int `validate:"eq=abc"`
	}

	processor := prep.NewProcessor(prep.FileTypeCSV, prep.WithStrictTagParsing())
	var records []record

	_, _, err := processor.Process(strings.NewReader("value\ntest\n"), &records)
	fmt.Println(errors.Is(err, prep.ErrInvalidTagFormat))
	// Output:
	// true
}

func ExampleWithValidRowsOnly() {
	type user struct {
		Name  string `prep:"trim" validate:"required"`
		Email string `prep:"trim,lowercase" validate:"required,email"`
	}

	csvData := "name,email\n Alice ,ALICE@EXAMPLE.COM\n,bad-email\n Bob ,BOB@EXAMPLE.COM\n"
	processor := prep.NewProcessor(prep.FileTypeCSV, prep.WithValidRowsOnly())
	var users []user

	reader, result, err := processor.Process(strings.NewReader(csvData), &users)
	if err != nil {
		log.Fatal(err)
	}

	output, err := io.ReadAll(reader)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("valid=%d invalid=%d\n", result.ValidRowCount, result.InvalidRowCount())
	fmt.Print(string(output))
	// Output:
	// valid=2 invalid=1
	// name,email
	// Alice,alice@example.com
	// Bob,bob@example.com
}

func ExampleProcessor_Process() {
	type user struct {
		Name  string `prep:"trim" validate:"required"`
		Email string `prep:"trim,lowercase" validate:"required,email"`
	}

	processor := prep.NewProcessor(prep.FileTypeCSV)
	var users []user

	reader, result, err := processor.Process(strings.NewReader("name,email\n Alice ,ALICE@EXAMPLE.COM\n"), &users)
	if err != nil {
		log.Fatal(err)
	}

	output, err := io.ReadAll(reader)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("%s %d\n", users[0].Email, result.ValidRowCount)
	fmt.Print(string(output))
	// Output:
	// alice@example.com 1
	// name,email
	// Alice,alice@example.com
}

func ExampleProcessor_Process_json() {
	type record struct {
		Data string `name:"data" prep:"trim"`
	}

	jsonData := `[{"id":1},{"id":2}]`
	processor := prep.NewProcessor(prep.FileTypeJSON)
	var records []record

	reader, result, err := processor.Process(strings.NewReader(jsonData), &records)
	if err != nil {
		log.Fatal(err)
	}

	output, err := io.ReadAll(reader)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("rows=%d format=%q\n", result.RowCount, string(output))
	// Output:
	// rows=2 format="{\"id\":1}\n{\"id\":2}\n"
}

func ExampleProcessor_ProcessToWriter() {
	type user struct {
		Name string `prep:"trim"`
	}

	processor := prep.NewProcessor(prep.FileTypeCSV)
	var users []user
	var buf bytes.Buffer

	result, err := processor.ProcessToWriter(strings.NewReader("name\n Alice \n"), &users, &buf)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("valid=%d output=%q\n", result.ValidRowCount, buf.String())
	// Output:
	// valid=1 output="name\nAlice\n"
}

func ExampleProcessResult_InvalidRowCount() {
	type user struct {
		Email string `validate:"email"`
	}

	processor := prep.NewProcessor(prep.FileTypeCSV)
	var users []user

	_, result, err := processor.Process(strings.NewReader("email\nbad\nok@example.com\n"), &users)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(result.InvalidRowCount())
	// Output:
	// 1
}

func ExampleProcessResult_HasErrors() {
	type user struct {
		Email string `validate:"email"`
	}

	processor := prep.NewProcessor(prep.FileTypeCSV)
	var users []user

	_, result, err := processor.Process(strings.NewReader("email\nnot-an-email\n"), &users)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(result.HasErrors())
	// Output:
	// true
}

func ExampleProcessResult_ValidationErrors() {
	type user struct {
		Email string `validate:"email"`
	}

	processor := prep.NewProcessor(prep.FileTypeCSV)
	var users []user

	_, result, err := processor.Process(strings.NewReader("email\nnot-an-email\n"), &users)
	if err != nil {
		log.Fatal(err)
	}

	validationErrors := result.ValidationErrors()
	fmt.Printf("%s: %s\n", validationErrors[0].Column, validationErrors[0].Message)
	// Output:
	// email: value must be a valid email address
}

func ExampleProcessResult_PrepErrors() {
	type record struct {
		Data string `name:"data" prep:"nullify={}"`
	}

	processor := prep.NewProcessor(prep.FileTypeJSON)
	var records []record

	_, result, err := processor.Process(strings.NewReader(`[{}, {"id": 2}]`), &records)
	if err != nil {
		log.Fatal(err)
	}

	prepErrors := result.PrepErrors()
	fmt.Printf("%s %d\n", prepErrors[0].Tag, result.ValidRowCount)
	// Output:
	// empty_json_data 1
}

func ExampleStream_Format() {
	type user struct {
		Name string
	}

	processor := prep.NewProcessor(prep.FileTypeCSV)
	var users []user

	reader, _, err := processor.Process(strings.NewReader("name\nAlice\n"), &users)
	if err != nil {
		log.Fatal(err)
	}

	stream, ok := reader.(prep.Stream)
	if !ok {
		log.Fatalf("reader has type %T, want prep.Stream", reader)
	}
	fmt.Println(stream.Format())
	// Output:
	// CSV
}

func ExampleStream_OriginalFormat() {
	type record struct {
		Data string `name:"data"`
	}

	processor := prep.NewProcessor(prep.FileTypeJSON)
	var records []record

	reader, _, err := processor.Process(strings.NewReader(`[{"id":1}]`), &records)
	if err != nil {
		log.Fatal(err)
	}

	stream, ok := reader.(prep.Stream)
	if !ok {
		log.Fatalf("reader has type %T, want prep.Stream", reader)
	}
	fmt.Printf("output=%s original=%s\n", stream.Format(), stream.OriginalFormat())
	// Output:
	// output=JSONL original=JSON
}

func Example_streamLen() {
	type user struct {
		Name string
	}

	processor := prep.NewProcessor(prep.FileTypeCSV)
	var users []user

	reader, _, err := processor.Process(strings.NewReader("name\nAlice\n"), &users)
	if err != nil {
		log.Fatal(err)
	}

	stream, ok := reader.(interface {
		prep.Stream
		Len() int
	})
	if !ok {
		log.Fatalf("reader has type %T, want prep.Stream with Len", reader)
	}
	fmt.Println(stream.Len() > 0)
	// Output:
	// true
}

func Example_streamSeek() {
	type user struct {
		Name string
	}

	processor := prep.NewProcessor(prep.FileTypeCSV)
	var users []user

	reader, _, err := processor.Process(strings.NewReader("name\nAlice\n"), &users)
	if err != nil {
		log.Fatal(err)
	}

	stream, ok := reader.(interface {
		prep.Stream
		io.Seeker
	})
	if !ok {
		log.Fatalf("reader has type %T, want prep.Stream with io.Seeker", reader)
	}
	buf := make([]byte, 4)
	if _, err := io.ReadFull(stream, buf); err != nil {
		log.Fatal(err)
	}
	fmt.Println(string(buf))

	if _, err := stream.Seek(0, io.SeekStart); err != nil {
		log.Fatal(err)
	}
	if _, err := io.ReadFull(stream, buf); err != nil {
		log.Fatal(err)
	}
	fmt.Println(string(buf))
	// Output:
	// name
	// name
}

// ExampleProcessor_Process_unknownColumn shows what happens when a struct field
// names a column the input does not have. The error names the field, the column
// it looked for, and the columns that exist, so a typo reads as a typo rather
// than as a row with a missing value.
func ExampleProcessor_Process_unknownColumn() {
	type user struct {
		Name   string `prep:"trim"`
		Emails string // the column is "email", singular
	}

	var users []user
	_, _, err := prep.NewProcessor(prep.FileTypeCSV).
		Process(strings.NewReader("name,email\nAlice,alice@example.com\n"), &users)

	fmt.Println(errors.Is(err, prep.ErrUnknownColumn))
	fmt.Println(err)
	// Output:
	// true
	// struct field names a column the input does not have: Emails (column "emails"); the input has [name email]
}

// ExampleProcessor_Process_defaultForAbsentColumn shows the field that is meant
// to work without a column: prep:"default=..." says where its value comes from,
// so it is accepted rather than refused.
func ExampleProcessor_Process_defaultForAbsentColumn() {
	type row struct {
		Comment string
		Status  string `prep:"default=active"`
	}

	var rows []row
	_, _, err := prep.NewProcessor(prep.FileTypeCSV).
		Process(strings.NewReader("comment\nhello\n"), &rows)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("%+v\n", rows[0])
	// Output:
	// {Comment:hello Status:active}
}

// ExampleProcessor_Process_crossField shows a comparison between two columns,
// which reads the cells as the field it lands on says they are: the dates are
// strings and compare as text, so the range runs forwards, while the quantities
// are numbers and compare as numbers, so 007 is not more than 7.
func ExampleProcessor_Process_conditionalRequired() {
	type order struct {
		// Required when both other columns hold the value named beside them.
		Invoice string `validate:"required_if=Kind paid Tier 'gold member'"`
		// Required as soon as either named column holds a value.
		Address string `validate:"required_with=City Zip"`
		Kind    string
		Tier    string
		City    string
		Zip     string
	}

	var rows []order
	_, result, err := prep.NewProcessor(prep.FileTypeCSV).Process(strings.NewReader(
		"invoice,address,kind,tier,city,zip\n"+
			",,paid,gold member,,\n"+
			",,paid,silver,Kyoto,\n"+
			"INV-1,1 Main St,paid,gold member,Kyoto,600-8216\n"), &rows)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(result.ValidRowCount)
	for _, e := range result.ValidationErrors() {
		fmt.Printf("row %d: %s %s\n", e.Row, e.Column, e.Message)
	}
	// Output:
	// 1
	// row 1: invoice value is required when Kind is paid and Tier is gold member
	// row 2: address value is required when City or Zip is present
}

func ExampleProcessor_Process_conditionalExcluded() {
	type account struct {
		// Forbidden as soon as the named column holds a value.
		PersonalTaxID string `validate:"excluded_with=CompanyTaxID"`
		CompanyTaxID  string
		// Forbidden unless the named column holds the value beside it.
		Coupon string `validate:"excluded_unless=Kind promo"`
		Kind   string
	}

	var rows []account
	_, result, err := prep.NewProcessor(prep.FileTypeCSV).Process(strings.NewReader(
		"personal_tax_id,company_tax_id,coupon,kind\n"+
			"P-1,C-1,,promo\n"+
			",C-1,SAVE10,regular\n"+
			",C-1,SAVE10,promo\n"), &rows)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(result.ValidRowCount)
	for _, e := range result.ValidationErrors() {
		fmt.Printf("row %d: %s %s\n", e.Row, e.Column, e.Message)
	}
	// Output:
	// 1
	// row 1: personal_tax_id value must be empty when CompanyTaxID is present
	// row 2: coupon value must be empty unless Kind is promo
}

func ExampleProcessor_Process_networkColumns() {
	type endpoint struct {
		Host string `validate:"ip"`
		Port string `validate:"port"`
	}

	var rows []endpoint
	_, result, err := prep.NewProcessor(prep.FileTypeCSV).Process(strings.NewReader(
		"host,port\n"+
			"192.0.2.1,0080\n"+
			"2001:db8::1,65535\n"+
			"example.com,80\n"+
			"192.0.2.1,65536\n"), &rows)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(result.ValidRowCount)
	for _, e := range result.ValidationErrors() {
		fmt.Printf("row %d: %s %s\n", e.Row, e.Column, e.Message)
	}
	// Output:
	// 2
	// row 3: host value must be a valid IP address
	// row 4: port value must be a valid port number
}

func ExampleProcessor_Process_encodedColumns() {
	type payload struct {
		Config   string `validate:"json"`
		Zone     string `validate:"timezone"`
		Version  string `validate:"semver"`
		Checksum string `validate:"sha256"`
	}

	var rows []payload
	_, result, err := prep.NewProcessor(prep.FileTypeCSV).Process(strings.NewReader(
		"config,zone,version,checksum\n"+
			`"{""retries"":3}",Asia/Tokyo,1.4.0,`+strings.Repeat("ab", 32)+"\n"+
			`"{""retries"":3}",JST,1.4,`+strings.ToUpper(strings.Repeat("ab", 32))+"\n"), &rows)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(result.ValidRowCount)
	for _, e := range result.ValidationErrors() {
		fmt.Printf("row %d: %s %s\n", e.Row, e.Column, e.Message)
	}
	// Output:
	// 1
	// row 2: zone value must be a valid IANA time zone name
	// row 2: version value must be a valid semantic version
	// row 2: checksum value must be a valid sha256 hash
}

func ExampleProcessor_Process_checksummedIdentifiers() {
	type item struct {
		ISBN string `validate:"isbn"`
		Card string `validate:"credit_card"`
	}

	var rows []item
	_, result, err := prep.NewProcessor(prep.FileTypeCSV).Process(strings.NewReader(
		"isbn,card\n"+
			"978-0-13-110362-7,4242 4242 4242 4242\n"+
			"978-0-13-110362-8,4242 4242 4242 4243\n"), &rows)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(result.ValidRowCount)
	for _, e := range result.ValidationErrors() {
		fmt.Printf("row %d: %s %s\n", e.Row, e.Column, e.Message)
	}
	// Output:
	// 1
	// row 2: isbn value must be a valid ISBN
	// row 2: card value must be a valid credit card number
}

func ExampleProcessor_Process_codeColumns() {
	type sale struct {
		Country  string `validate:"iso3166_1_alpha2"`
		Currency string `validate:"iso4217"`
	}

	var rows []sale
	_, result, err := prep.NewProcessor(prep.FileTypeCSV).Process(strings.NewReader(
		"country,currency\n"+
			"JP,JPY\n"+
			"jp,YEN\n"), &rows)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(result.ValidRowCount)
	for _, e := range result.ValidationErrors() {
		fmt.Printf("row %d: %s %s\n", e.Row, e.Column, e.Message)
	}
	// Output:
	// 1
	// row 2: country value must be an ISO 3166-1 alpha-2 country code
	// row 2: currency value must be an active ISO 4217 currency code
}

func ExampleProcessor_Process_uniqueColumn() {
	type member struct {
		// prep runs before validation, so uniqueness is decided on the
		// trimmed, folded value.
		Email string `prep:"trim,lowercase" validate:"unique"`
		Name  string
	}

	var rows []member
	_, result, err := prep.NewProcessor(prep.FileTypeCSV).Process(strings.NewReader(
		"email,name\n"+
			"a@example.com,Ada\n"+
			"b@example.com,Bob\n"+
			" A@Example.com ,Ada again\n"), &rows)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(result.ValidRowCount)
	for _, e := range result.ValidationErrors() {
		fmt.Printf("row %d: %s %s\n", e.Row, e.Column, e.Message)
	}
	// Output:
	// 2
	// row 3: email value "a@example.com" already appeared in row 1
}

func ExampleProcessor_Process_crossField() {
	type shipment struct {
		ShippedOn string `validate:"ltfield=DueOn"`
		DueOn     string
		Packed    int `validate:"ltefield=Ordered"`
		Ordered   int
	}

	var rows []shipment
	_, result, err := prep.NewProcessor(prep.FileTypeCSV).Process(strings.NewReader(
		"shipped_on,due_on,packed,ordered\n"+
			"2024-01-05,2024-01-31,007,7\n"+
			"2024-02-10,2024-02-01,9,7\n"), &rows)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(result.ValidRowCount)
	for _, e := range result.ValidationErrors() {
		fmt.Printf("row %d: %s %s\n", e.Row, e.Column, e.Message)
	}
	// Output:
	// 1
	// row 2: shipped_on value must be less than field DueOn
	// row 2: packed value must be less than or equal to field Ordered
}
