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
	type record struct {
		Value string `validate:"eq=abc"`
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

	stream := reader.(prep.Stream)
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

	stream := reader.(prep.Stream)
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

	stream := reader.(interface {
		prep.Stream
		Len() int
	})
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

	stream := reader.(interface {
		prep.Stream
		io.Seeker
	})
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
