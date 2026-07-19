package parser_test

import (
	"fmt"
	"strings"

	"github.com/nao1215/filesql/parser"
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
