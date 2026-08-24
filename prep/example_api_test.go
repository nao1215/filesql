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

func ExampleIsCompressed() {
	fmt.Println(prep.IsCompressed(prep.DetectFileType("users.csv.gz")))
	// Output:
	// true
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
