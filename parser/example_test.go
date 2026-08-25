package parser_test

import (
	"fmt"
	"strings"
	"time"

	"github.com/nao1215/filesql/parser"
	"github.com/xuri/excelize/v2"
)

func ExampleParse_csv() {
	csvData := `name,age,score
Alice,30,85.5
Bob,25,92.0
Charlie,35,78.5`

	result, err := parser.Parse(strings.NewReader(csvData), parser.CSV)
	if err != nil {
		fmt.Println("Error:", err)
		return
	}

	fmt.Println("Headers:", result.Headers)
	fmt.Println("Records:", len(result.Records))
	fmt.Println("First row:", result.Records[0])
	// Output:
	// Headers: [name age score]
	// Records: 3
	// First row: [Alice 30 85.5]
}

func ExampleParse_tsv() {
	tsvData := `id	product	price
1	Laptop	999.99
2	Mouse	29.99
3	Keyboard	79.99`

	result, err := parser.Parse(strings.NewReader(tsvData), parser.TSV)
	if err != nil {
		fmt.Println("Error:", err)
		return
	}

	fmt.Println("Headers:", result.Headers)
	fmt.Println("Records:", len(result.Records))
	// Output:
	// Headers: [id product price]
	// Records: 3
}

func ExampleParse_ltsv() {
	ltsvData := `host:192.168.1.1	method:GET	path:/index.html
host:192.168.1.2	method:POST	path:/api/users`

	result, err := parser.Parse(strings.NewReader(ltsvData), parser.LTSV)
	if err != nil {
		fmt.Println("Error:", err)
		return
	}

	fmt.Println("Headers:", result.Headers)
	fmt.Println("First row:", result.Records[0])
	// Output:
	// Headers: [host method path]
	// First row: [192.168.1.1 GET /index.html]
}

func ExampleDetectFileType() {
	paths := []string{
		"data.csv",
		"data.csv.gz",
		"report.xlsx",
		"logs.ltsv.zst",
		"analytics.parquet",
	}

	for _, path := range paths {
		ft := parser.DetectFileType(path)
		fmt.Printf("%s -> %s\n", path, ft)
	}
	// Output:
	// data.csv -> CSV
	// data.csv.gz -> CSV (gzip)
	// report.xlsx -> XLSX
	// logs.ltsv.zst -> LTSV (zstd)
	// analytics.parquet -> Parquet
}

func ExampleIsCompressed() {
	types := []parser.FileType{
		parser.CSV,
		parser.CSVGZ,
		parser.Parquet,
		parser.ParquetZSTD,
	}

	for _, ft := range types {
		fmt.Printf("%s compressed: %v\n", ft, parser.IsCompressed(ft))
	}
	// Output:
	// CSV compressed: false
	// CSV (gzip) compressed: true
	// Parquet compressed: false
	// Parquet (zstd) compressed: true
}

func ExampleBaseFileType() {
	types := []parser.FileType{
		parser.CSV,
		parser.CSVGZ,
		parser.TSVBZ2,
		parser.ParquetZSTD,
	}

	for _, ft := range types {
		base := parser.BaseFileType(ft)
		fmt.Printf("%s -> %s\n", ft, base)
	}
	// Output:
	// CSV -> CSV
	// CSV (gzip) -> CSV
	// TSV (bzip2) -> TSV
	// Parquet (zstd) -> Parquet
}

func ExampleParseValue() {
	// Integer column
	intVal := parser.ParseValue("42", parser.TypeInteger)
	fmt.Printf("Integer: %v (%T)\n", intVal, intVal)

	// Real column
	realVal := parser.ParseValue("3.14", parser.TypeReal)
	fmt.Printf("Real: %v (%T)\n", realVal, realVal)

	// Text column
	textVal := parser.ParseValue("hello", parser.TypeText)
	fmt.Printf("Text: %v (%T)\n", textVal, textVal)

	// Empty value returns nil
	nilVal := parser.ParseValue("", parser.TypeInteger)
	fmt.Printf("Empty: %v\n", nilVal)
	// Output:
	// Integer: 42 (int64)
	// Real: 3.14 (float64)
	// Text: hello (string)
	// Empty: <nil>
}

func ExampleTableData_columnTypes() {
	csvData := `id,name,score,date
1,Alice,85.5,2024-01-15
2,Bob,92.0,2024-01-16
3,Charlie,78.5,2024-01-17`

	result, err := parser.Parse(strings.NewReader(csvData), parser.CSV)
	if err != nil {
		fmt.Println("Error:", err)
		return
	}

	for i, header := range result.Headers {
		fmt.Printf("%s: %s\n", header, result.ColumnTypes[i])
	}
	// Output:
	// id: INTEGER
	// name: TEXT
	// score: REAL
	// date: DATETIME
}

func ExampleNewCSVReader() {
	// A CRLF inside a quoted field is data, and this reader keeps it. That is
	// the one difference from encoding/csv, which would hand back "x\ny".
	records, err := parser.NewCSVReader(strings.NewReader("note\n\"x\r\ny\"\n")).ReadAll()
	if err != nil {
		fmt.Println("Error:", err)
		return
	}

	fmt.Printf("%q\n", records)
	// Output:
	// [["note"] ["x\r\ny"]]
}

func ExampleNewTSVReader() {
	// TSV has no quoting, so every byte between two tabs is data: a double
	// quote is an ordinary character rather than the start of a quoted field.
	// A CSV reader handed the same input fails with `bare " in non-quoted-field`.
	records, err := parser.NewTSVReader(strings.NewReader("name\theight\nalice\t5'9\" tall\n")).ReadAll()
	if err != nil {
		fmt.Println("Error:", err)
		return
	}

	fmt.Printf("%q\n", records)
	// Output:
	// [["name" "height"] ["alice" "5'9\" tall"]]
}

func ExampleNormalizeLineEndings() {
	// A file written the classic Mac OS way ends its lines with a lone carriage
	// return, which nothing downstream reads as a line break.
	normalized := parser.NormalizeLineEndings(strings.NewReader("name,age\rAlice,30\r"))

	result, err := parser.Parse(normalized, parser.CSV)
	if err != nil {
		fmt.Println("Error:", err)
		return
	}

	fmt.Println(result.Headers, result.Records)
	// Output:
	// [name age] [[Alice 30]]
}

// workbook is one sheet of a workbook as sheet selection reads it: the names in
// the order the file stores them, and whether each is shown.
type workbook struct {
	names   []string
	visible map[string]bool
}

func (w workbook) GetSheetList() []string { return w.names }

func (w workbook) GetSheetVisible(sheet string) (bool, error) { return w.visible[sheet], nil }

func ExampleExcelSheets() {
	sheets, err := parser.ExcelSheets(workbook{
		names:   []string{"summary", "scratch"},
		visible: map[string]bool{"summary": true, "scratch": false},
	})
	if err != nil {
		fmt.Println("Error:", err)
		return
	}

	for _, sheet := range sheets {
		fmt.Println(sheet.Name, sheet.Visible)
	}
	// Output:
	// summary true
	// scratch false
}

func ExampleSelectExcelSheets() {
	loaded, skipped, err := parser.SelectExcelSheets(workbook{
		names:   []string{"summary", "scratch"},
		visible: map[string]bool{"summary": true, "scratch": false},
	}, parser.ExcelSheetPolicyVisibleOnly)
	if err != nil {
		fmt.Println("Error:", err)
		return
	}

	fmt.Println("loaded:", loaded)
	fmt.Println("skipped:", skipped)
	// Output:
	// loaded: [summary]
	// skipped: [scratch]
}

func ExampleNormalizeXLSXDates() {
	f := excelize.NewFile()
	defer f.Close()

	style, err := f.NewStyle(&excelize.Style{NumFmt: 15}) // d-mmm-yy
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	if err := f.SetCellStr("Sheet1", "A1", "when"); err != nil {
		fmt.Println("Error:", err)
		return
	}
	if err := f.SetCellValue("Sheet1", "A2", time.Date(2023, 3, 15, 0, 0, 0, 0, time.UTC)); err != nil {
		fmt.Println("Error:", err)
		return
	}
	if err := f.SetCellStyle("Sheet1", "A2", "A2", style); err != nil {
		fmt.Println("Error:", err)
		return
	}

	rows, err := f.GetRows("Sheet1")
	if err != nil {
		fmt.Println("Error:", err)
		return
	}

	fmt.Println("as the sheet renders it:", rows[1][0])
	fmt.Println("as the inference reads it:", parser.NormalizeXLSXDates(f, "Sheet1", rows)[1][0])
	// Output:
	// as the sheet renders it: 15-Mar-23
	// as the inference reads it: 2023-03-15
}
