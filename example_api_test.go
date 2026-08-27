package filesql_test

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"log"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/nao1215/filesql"
	"github.com/nao1215/filesql/dialect"
	achconv "github.com/nao1215/filesql/parser/ach"
	wireconv "github.com/nao1215/filesql/parser/wire"
	"github.com/xuri/excelize/v2"
	_ "modernc.org/sqlite"
)

func createFilesqlExampleDir(files map[string]string) string {
	dir, err := os.MkdirTemp("", "filesql-api-example")
	if err != nil {
		log.Fatal(err)
	}
	for name, body := range files {
		path := filepath.Join(dir, name)
		if err := os.MkdirAll(filepath.Dir(path), 0750); err != nil {
			log.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(strings.TrimLeft(body, "\n")), 0600); err != nil {
			log.Fatal(err)
		}
	}
	return dir
}

// exampleTempDir is a directory of its own for an example to write into, so
// that no example touches a path another program may already be using.
func exampleTempDir() string {
	dir, err := os.MkdirTemp("", "filesql-api-example")
	if err != nil {
		log.Fatal(err)
	}
	return dir
}

func openExampleSQLiteDB() *sql.DB {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		log.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	return db
}

func ExampleLoadInto() {
	dir := createFilesqlExampleDir(map[string]string{
		"users.csv": `
id,name
1,Alice
2,Bob
`,
	})
	defer os.RemoveAll(dir)

	db := openExampleSQLiteDB()
	defer db.Close()

	ctx := context.Background()

	if _, err := db.ExecContext(ctx, `CREATE TABLE notes (body TEXT)`); err != nil {
		log.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO notes VALUES ('kept')`); err != nil {
		log.Fatal(err)
	}

	if err := filesql.LoadInto(ctx, db, filepath.Join(dir, "users.csv")); err != nil {
		log.Fatal(err)
	}

	var users, notes int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM users`).Scan(&users); err != nil {
		log.Fatal(err)
	}
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM notes`).Scan(&notes); err != nil {
		log.Fatal(err)
	}

	fmt.Printf("users=%d notes=%d\n", users, notes)
	// Output:
	// users=2 notes=1
}

func ExampleDBBuilder_SetDefaultChunkSize() {
	builder := filesql.NewBuilder().
		AddReader(strings.NewReader("id,name\n1,Alice\n2,Bob\n3,Cora\n"), "users", filesql.FileTypeCSV).
		SetDefaultChunkSize(2)

	validated, err := builder.Build(context.Background())
	if err != nil {
		log.Fatal(err)
	}

	db, err := validated.Open(context.Background())
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	ctx := context.Background()
	var rows int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM users`).Scan(&rows); err != nil {
		log.Fatal(err)
	}

	fmt.Printf("chunk=2 rows=%d\n", rows)
	// Output:
	// chunk=2 rows=3
}

func ExampleDBBuilder_WithMalformedRowPolicy() {
	csvData := "id,name\n1,Alice\n2\n3,Cora,extra\n4,Dave\n"

	validated, err := filesql.NewBuilder().
		AddReader(strings.NewReader(csvData), "users", filesql.FileTypeCSV).
		WithMalformedRowPolicy(filesql.MalformedRowSkip).
		Build(context.Background())
	if err != nil {
		log.Fatal(err)
	}

	db, err := validated.Open(context.Background())
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	ctx := context.Background()
	var rows int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM users`).Scan(&rows); err != nil {
		log.Fatal(err)
	}

	fmt.Printf("loaded rows=%d\n", rows)
	// Output:
	// loaded rows=2
}

func ExampleDBBuilder_WithDialect() {
	validated, err := filesql.NewBuilder().
		AddReader(strings.NewReader("id,shipped_at\n1,2024-12-31 09:07:00\n"), "orders", filesql.FileTypeCSV).
		WithDialect(dialect.MySQL).
		Build(context.Background())
	if err != nil {
		log.Fatal(err)
	}

	db, err := validated.Open(context.Background())
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// MySQL syntax: DATE_FORMAT with the 24-hour time and the ISO week its
	// year belongs to.
	var shipped string
	if err := db.QueryRowContext(context.Background(),
		`SELECT DATE_FORMAT(shipped_at, '%T on week %v of %x') FROM orders`).Scan(&shipped); err != nil {
		log.Fatal(err)
	}

	fmt.Println(shipped)
	// Output:
	// 09:07:00 on week 01 of 2025
}

func ExampleDBBuilder_WithLogger() {
	ctx := context.Background()

	// A *slog.Logger is what the builder takes, so where the records go and
	// which levels are kept are the handler's business. This one drops the
	// timestamp so the example's output is stable.
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{
		Level: slog.LevelInfo,
		ReplaceAttr: func(_ []string, attr slog.Attr) slog.Attr {
			if attr.Key == slog.TimeKey {
				return slog.Attr{}
			}
			return attr
		},
	}))

	validated, err := filesql.NewBuilder().
		WithLogger(logger).
		AddReader(strings.NewReader("id,name\n1,Alice\n"), "users", filesql.FileTypeCSV).
		Build(ctx)
	if err != nil {
		log.Fatal(err)
	}

	db, err := validated.Open(ctx)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	fmt.Println(lines[0])
	fmt.Println(lines[len(lines)-1])
	// Output:
	// level=INFO msg="build completed" collected_paths=0 readers=1
	// level=INFO msg="database opened successfully"
}

func ExampleDBBuilder_OpenReadOnly() {
	ctx := context.Background()

	validated, err := filesql.NewBuilder().
		AddReader(strings.NewReader("id,name\n1,Alice\n2,Bob\n"), "users", filesql.FileTypeCSV).
		Build(ctx)
	if err != nil {
		log.Fatal(err)
	}

	// The result is an ordinary *sql.DB whose connections carry SQLite's
	// query_only pragma, so a write is refused by the engine rather than by
	// this package.
	db, err := validated.OpenReadOnly(ctx)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	var rows int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM users`).Scan(&rows); err != nil {
		log.Fatal(err)
	}

	_, err = db.ExecContext(ctx, `INSERT INTO users VALUES (3, 'Cora')`)
	fmt.Printf("rows=%d write_blocked=%t\n", rows, err != nil)
	// Output:
	// rows=2 write_blocked=true
}

func ExampleDBBuilder_LoadInto() {
	db := openExampleSQLiteDB()
	defer db.Close()

	ctx := context.Background()

	if _, err := db.ExecContext(ctx, `CREATE TABLE notes (body TEXT)`); err != nil {
		log.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO notes VALUES ('ready')`); err != nil {
		log.Fatal(err)
	}

	validated, err := filesql.NewBuilder().
		AddReader(strings.NewReader("id,name\n1,Alice\n2,Bob\n"), "users", filesql.FileTypeCSV).
		Build(context.Background())
	if err != nil {
		log.Fatal(err)
	}

	if err := validated.LoadInto(ctx, db); err != nil {
		log.Fatal(err)
	}

	var users, notes int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM users`).Scan(&users); err != nil {
		log.Fatal(err)
	}
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM notes`).Scan(&notes); err != nil {
		log.Fatal(err)
	}

	fmt.Printf("users=%d notes=%d\n", users, notes)
	// Output:
	// users=2 notes=1
}

func ExampleDBBuilder_LoadIntoTx() {
	db := openExampleSQLiteDB()
	defer db.Close()

	ctx := context.Background()

	validated, err := filesql.NewBuilder().
		AddReader(strings.NewReader("id,name\n1,Alice\n2,Bob\n"), "users", filesql.FileTypeCSV).
		Build(ctx)
	if err != nil {
		log.Fatal(err)
	}

	// The caller owns the transaction: the load lands only on Commit, and a
	// Rollback discards it.
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		log.Fatal(err)
	}
	if err := validated.LoadIntoTx(ctx, tx); err != nil {
		log.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		log.Fatal(err)
	}

	var users int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM users`).Scan(&users); err != nil {
		log.Fatal(err)
	}

	fmt.Printf("users=%d\n", users)
	// Output:
	// users=2
}

func ExampleNewCompressionHandler() {
	handler := filesql.NewCompressionHandler(filesql.CompressionGZ)

	var compressed bytes.Buffer
	writer, closeWriter, err := handler.CreateWriter(&compressed)
	if err != nil {
		log.Fatal(err)
	}
	if _, err := io.WriteString(writer, "hello"); err != nil {
		log.Fatal(err)
	}
	if err := closeWriter(); err != nil {
		log.Fatal(err)
	}

	reader, closeReader, err := handler.CreateReader(bytes.NewReader(compressed.Bytes()))
	if err != nil {
		log.Fatal(err)
	}
	defer closeReader()

	plain, err := io.ReadAll(reader)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(string(plain))
	// Output:
	// hello
}

func ExampleNewCompressionFactory() {
	factory := filesql.NewCompressionFactory()
	fmt.Println(factory.CreateHandlerForFile("orders.csv.zst").Extension())
	// Output:
	// .zst
}

func ExampleCompressionFactory_DetectCompressionType() {
	factory := filesql.NewCompressionFactory()
	fmt.Println(factory.DetectCompressionType("orders.csv.xz"))
	// Output:
	// xz
}

func ExampleCompressionFactory_RemoveCompressionExtension() {
	factory := filesql.NewCompressionFactory()
	fmt.Println(factory.RemoveCompressionExtension("orders.csv.gz"))
	// Output:
	// orders.csv
}

func ExampleCompressionFactory_GetBaseFileType() {
	factory := filesql.NewCompressionFactory()
	fmt.Println(factory.GetBaseFileType("orders.csv.gz"))
	// Output:
	// CSV
}

func ExampleNewDumpOptions() {
	opts := filesql.NewDumpOptions()
	fmt.Printf("%s %q\n", opts.Format, opts.FileExtension())
	// Output:
	// csv ".csv"
}

func ExampleDumpOptions_WithFormat() {
	opts := filesql.NewDumpOptions().WithFormat(filesql.OutputFormatTSV)
	fmt.Println(opts.FileExtension())
	// Output:
	// .tsv
}

func ExampleDumpOptions_WithCompression() {
	opts := filesql.NewDumpOptions().WithCompression(filesql.CompressionGZ)
	fmt.Println(opts.FileExtension())
	// Output:
	// .csv.gz
}

func ExampleDumpOptions_WithEncoding() {
	// Output is UTF-8 unless asked otherwise. A value the encoding cannot write
	// fails the save rather than being replaced.
	opts := filesql.NewDumpOptions().WithEncoding(filesql.EncodingShiftJIS)
	fmt.Println(opts.Encoding)
	// Output:
	// shift-jis
}

func ExampleDumpOptions_WithLineEnding() {
	// Output ends its records with "\n" unless asked otherwise. A save that
	// overwrites a file it loaded from a path keeps that file's own terminator
	// without being told.
	opts := filesql.NewDumpOptions().WithLineEnding(filesql.LineEndingCRLF)
	fmt.Println(opts.LineEnding)
	// Output:
	// crlf
}

func ExampleOutputFormat_Extension() {
	fmt.Println(filesql.OutputFormatParquet.Extension())
	// Output:
	// .parquet
}

func ExampleCompressionType_Extension() {
	fmt.Println(filesql.CompressionXZ.Extension())
	// Output:
	// .xz
}

func ExampleMalformedRowPolicy_String() {
	fmt.Println(filesql.MalformedRowStop)
	fmt.Println(filesql.MalformedRowSkip)
	fmt.Println(filesql.MalformedRowFill)
	// Output:
	// stop
	// skip
	// fill
}

// exampleWorkbook writes a workbook holding one shown sheet and one hidden
// sheet, and returns its path.
func exampleWorkbook(dir string) string {
	f := excelize.NewFile()
	defer func() { _ = f.Close() }()

	for _, sheet := range []string{"Sales", "Scratch"} {
		if sheet != "Sheet1" {
			if _, err := f.NewSheet(sheet); err != nil {
				log.Fatal(err)
			}
		}
		if err := f.SetCellValue(sheet, "A1", "region"); err != nil {
			log.Fatal(err)
		}
		if err := f.SetCellValue(sheet, "A2", sheet); err != nil {
			log.Fatal(err)
		}
	}
	if err := f.DeleteSheet("Sheet1"); err != nil {
		log.Fatal(err)
	}
	if err := f.SetSheetVisible("Scratch", false); err != nil {
		log.Fatal(err)
	}

	path := filepath.Join(dir, "book.xlsx")
	if err := f.SaveAs(path); err != nil {
		log.Fatal(err)
	}
	return path
}

func ExampleDBBuilder_WithExcelSheetPolicy() {
	dir := exampleTempDir()
	defer os.RemoveAll(dir)

	path := exampleWorkbook(dir)

	validated, err := filesql.NewBuilder().
		AddPath(path).
		WithExcelSheetPolicy(filesql.ExcelSheetPolicyVisibleOnly).
		Build(context.Background())
	if err != nil {
		log.Fatal(err)
	}

	db, err := validated.Open(context.Background())
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	rows, err := db.QueryContext(context.Background(),
		`SELECT name FROM sqlite_master WHERE type='table' ORDER BY name`)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			log.Fatal(err)
		}
		fmt.Println(name)
	}
	if err := rows.Err(); err != nil {
		log.Fatal(err)
	}
	// Output:
	// book_Sales
}

func ExampleExcelSheetsInFile() {
	dir := exampleTempDir()
	defer os.RemoveAll(dir)

	path := exampleWorkbook(dir)

	sheets, err := filesql.ExcelSheetsInFile(path)
	if err != nil {
		log.Fatal(err)
	}
	for _, sheet := range sheets {
		fmt.Printf("%s visible=%t\n", sheet.Name, sheet.Visible)
	}
	// Output:
	// Sales visible=true
	// Scratch visible=false
}

// ExampleLoadInto_dumpDatabase completes the cycle a caller-managed database is
// for: load files into a database you already own, query across what was there
// and what arrived, then write the result back out.
func ExampleLoadInto_dumpDatabase() {
	dir := createFilesqlExampleDir(map[string]string{
		"users.csv": `
id,name
1,Alice
2,Bob
`,
	})
	defer os.RemoveAll(dir)

	// openExampleSQLiteDB pins the pool to one connection, which SQLite's
	// ":memory:" requires: the database is private per connection, so a second
	// connection would not see the loaded tables.
	db := openExampleSQLiteDB()
	defer db.Close()

	ctx := context.Background()
	if err := filesql.LoadInto(ctx, db, filepath.Join(dir, "users.csv")); err != nil {
		log.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `UPDATE users SET name = 'Carol' WHERE id = 2`); err != nil {
		log.Fatal(err)
	}

	out := filepath.Join(dir, "out")
	if err := filesql.DumpDatabase(db, out); err != nil {
		log.Fatal(err)
	}

	saved, err := os.ReadFile(filepath.Join(out, "users.csv")) //nolint:gosec // path built from this example's own temp directory
	if err != nil {
		log.Fatal(err)
	}
	fmt.Print(string(saved))
	// Output:
	// id,name
	// 1,Alice
	// 2,Carol
}

// ExampleExcelSheetsInReader is ExampleExcelSheetsInFile for a workbook that
// arrived as bytes rather than as a file.
func ExampleExcelSheetsInReader() {
	dir := exampleTempDir()
	defer os.RemoveAll(dir)

	path := exampleWorkbook(dir)

	// A workbook a caller holds in memory, downloaded or embedded rather than
	// read from disk. The reader must yield the workbook itself: a codec around
	// it has no name to be detected from.
	body, err := os.ReadFile(path) //nolint:gosec // path built by this example
	if err != nil {
		log.Fatal(err)
	}

	sheets, err := filesql.ExcelSheetsInReader(bytes.NewReader(body))
	if err != nil {
		log.Fatal(err)
	}
	for _, sheet := range sheets {
		fmt.Printf("%s visible=%t\n", sheet.Name, sheet.Visible)
	}
	// Output:
	// Sales visible=true
	// Scratch visible=false
}

func ExampleExcelSheetTableNames() {
	tables, err := filesql.ExcelSheetTableNames("book.xlsx", []string{"Sales", "Q1 sales"})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(tables)

	// "Q1 sales" and "Q1.sales" both sanitize to one identifier, so the second
	// sheet loaded would replace the first with nothing said about it. Asking
	// first is how a caller refuses the workbook before loading any of it.
	_, err = filesql.ExcelSheetTableNames("book.xlsx", []string{"Q1 sales", "Q1.sales"})
	fmt.Println(errors.Is(err, filesql.ErrDuplicateTable))
	// Output:
	// [book_Sales book_Q1_sales]
	// true
}

// ExampleDBBuilder_SkippedRows shows what MalformedRowSkip discarded. The
// policy is an instruction from the caller, but one dropped row and most of the
// file dropped look alike without the counts.
func ExampleDBBuilder_SkippedRows() {
	csvData := "id,name\n1,Alice\n2\n3,Cora,extra\n4,Dave\n"

	validated, err := filesql.NewBuilder().
		AddReader(strings.NewReader(csvData), "users", filesql.FileTypeCSV).
		WithMalformedRowPolicy(filesql.MalformedRowSkip).
		Build(context.Background())
	if err != nil {
		log.Fatal(err)
	}

	db, err := validated.Open(context.Background())
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	for _, skipped := range validated.SkippedRows() {
		fmt.Printf("%s: %d of %d rows skipped\n", skipped.Table, skipped.Count, skipped.Total)
	}
	// Output:
	// users: 2 of 4 rows skipped
}

func ExampleFileType_String() {
	fmt.Println(filesql.FileTypeCSV, filesql.FileTypeLTSV, filesql.FileTypeParquet)
	// Output: CSV LTSV Parquet
}

func ExampleCompressionType_String() {
	fmt.Println(filesql.CompressionNone, filesql.CompressionGZ, filesql.CompressionZSTD)
	// Output: none gz zstd
}

func ExampleEncoding_String() {
	fmt.Println(filesql.EncodingUTF8, filesql.EncodingShiftJIS, filesql.EncodingEUCJP)
	// Output: utf-8 shift-jis euc-jp
}

func ExampleLineEnding_String() {
	fmt.Println(filesql.LineEndingLF, filesql.LineEndingCRLF)
	// Output: lf crlf
}

func ExampleOutputFormat_String() {
	fmt.Println(filesql.OutputFormatCSV, filesql.OutputFormatTSV, filesql.OutputFormatXLSX)
	// Output: csv tsv xlsx
}

// ExampleDumpACH loads an ACH file, edits one entry, and writes the file back.
//
// The write rebuilds the file from the source it was loaded from, so that file
// must still be readable. Control records are derived rather than stored: an
// edited amount is balanced by the write, and an edit to a control column is
// overwritten by the recalculation.
func ExampleDumpACH() {
	dir := exampleTempDir()
	defer os.RemoveAll(dir)

	ctx := context.Background()
	db, err := filesql.Open("testdata/ppd-debit.ach")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	if _, err := db.ExecContext(ctx,
		`UPDATE ppd_debit_entries SET individual_name = 'Alice Smith'`); err != nil {
		log.Fatal(err)
	}

	out := filepath.Join(dir, "edited.ach")
	if err := filesql.DumpACH(ctx, db, "ppd_debit", out); err != nil {
		log.Fatal(err)
	}

	reloaded, err := filesql.Open(out)
	if err != nil {
		log.Fatal(err)
	}
	defer reloaded.Close()

	var name string
	var amount int64
	if err := reloaded.QueryRowContext(ctx,
		`SELECT individual_name, amount FROM edited_entries`).Scan(&name, &amount); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("%s %d\n", name, amount)
	// Output:
	// Alice Smith 100000000
}

// ExampleDumpFedWire loads a Fedwire file, edits the message, and writes it back.
//
// Tags are written in the order the format defines rather than the order the
// file had them, so a diff after a write-back shows lines nobody edited.
func ExampleDumpFedWire() {
	dir := exampleTempDir()
	defer os.RemoveAll(dir)

	ctx := context.Background()
	db, err := filesql.Open("testdata/customer-transfer.fed")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	if _, err := db.ExecContext(ctx,
		`UPDATE customer_transfer_message SET amount = '000000012500'`); err != nil {
		log.Fatal(err)
	}

	out := filepath.Join(dir, "edited.fed")
	if err := filesql.DumpFedWire(ctx, db, "customer_transfer", out); err != nil {
		log.Fatal(err)
	}

	reloaded, err := filesql.Open(out)
	if err != nil {
		log.Fatal(err)
	}
	defer reloaded.Close()

	var amount string
	if err := reloaded.QueryRowContext(ctx,
		`SELECT amount FROM edited_message`).Scan(&amount); err != nil {
		log.Fatal(err)
	}
	fmt.Println(amount)
	// Output:
	// 000000012500
}

// ExampleDumpACHWithTableSet writes back a database loaded from an io.Reader.
// Such a database has no file to rebuild from, so DumpACH refuses it with
// ErrSourceUnavailable; parse the same bytes with parser/ach and hand over the
// structure instead.
func ExampleDumpACHWithTableSet() {
	dir := exampleTempDir()
	defer os.RemoveAll(dir)

	body, err := os.ReadFile("testdata/ppd-debit.ach")
	if err != nil {
		log.Fatal(err)
	}

	ctx := context.Background()
	validated, err := filesql.NewBuilder().
		AddReader(bytes.NewReader(body), "payment", filesql.FileTypeACH).
		Build(ctx)
	if err != nil {
		log.Fatal(err)
	}
	db, err := validated.Open(ctx)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	out := filepath.Join(dir, "payment.ach")
	err = filesql.DumpACH(ctx, db, "payment", out)
	fmt.Println("without the structure:", errors.Is(err, filesql.ErrSourceUnavailable))

	tableSet, err := achconv.ParseReader(bytes.NewReader(body))
	if err != nil {
		log.Fatal(err)
	}
	if _, err := db.ExecContext(ctx,
		`UPDATE payment_entries SET individual_name = 'Alice Smith'`); err != nil {
		log.Fatal(err)
	}
	if err := filesql.DumpACHWithTableSet(ctx, db, "payment", out, tableSet); err != nil {
		log.Fatal(err)
	}

	reloaded, err := filesql.Open(out)
	if err != nil {
		log.Fatal(err)
	}
	defer reloaded.Close()

	var name string
	if err := reloaded.QueryRowContext(ctx,
		`SELECT individual_name FROM payment_entries`).Scan(&name); err != nil {
		log.Fatal(err)
	}
	fmt.Println(name)
	// Output:
	// without the structure: true
	// Alice Smith
}

// ExampleDumpFedWireWithTableSet is ExampleDumpACHWithTableSet for Fedwire: a
// database loaded from an io.Reader is written back through the structure
// parser/wire returns for the same bytes.
func ExampleDumpFedWireWithTableSet() {
	dir := exampleTempDir()
	defer os.RemoveAll(dir)

	body, err := os.ReadFile("testdata/customer-transfer.fed")
	if err != nil {
		log.Fatal(err)
	}

	ctx := context.Background()
	validated, err := filesql.NewBuilder().
		AddReader(bytes.NewReader(body), "transfer", filesql.FileTypeFedWire).
		Build(ctx)
	if err != nil {
		log.Fatal(err)
	}
	db, err := validated.Open(ctx)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	tableSet, err := wireconv.ParseReader(bytes.NewReader(body))
	if err != nil {
		log.Fatal(err)
	}
	if _, err := db.ExecContext(ctx,
		`UPDATE transfer_message SET amount = '000000012500'`); err != nil {
		log.Fatal(err)
	}

	out := filepath.Join(dir, "transfer.fed")
	if err := filesql.DumpFedWireWithTableSet(ctx, db, "transfer", out, tableSet); err != nil {
		log.Fatal(err)
	}

	reloaded, err := filesql.Open(out)
	if err != nil {
		log.Fatal(err)
	}
	defer reloaded.Close()

	var amount string
	if err := reloaded.QueryRowContext(ctx,
		`SELECT amount FROM transfer_message`).Scan(&amount); err != nil {
		log.Fatal(err)
	}
	fmt.Println(amount)
	// Output:
	// 000000012500
}

// ExampleDumpDatabaseContext exports under a deadline, which is what a handler
// serving a download wants: the export stops when the request it belongs to
// does.
func ExampleDumpDatabaseContext() {
	dir, err := os.MkdirTemp("", "filesql-dump-context")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)

	source := filepath.Join(dir, "orders.csv")
	if err := os.WriteFile(source, []byte("id,total\n1,120\n2,340\n"), 0o600); err != nil {
		log.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	db, err := filesql.OpenContext(ctx, source)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	out := filepath.Join(dir, "out")
	if err := filesql.DumpDatabaseContext(ctx, db, out); err != nil {
		log.Fatal(err)
	}

	exported, err := os.ReadFile(filepath.Join(out, "orders.csv")) //nolint:gosec // Path built from this example's own temporary directory.
	if err != nil {
		log.Fatal(err)
	}
	fmt.Print(string(exported))

	// A context that is already done stops the export before it writes anything.
	done, stop := context.WithCancel(context.Background())
	stop()
	fmt.Println(errors.Is(filesql.DumpDatabaseContext(done, db, filepath.Join(dir, "none")), context.Canceled))

	// Output:
	// id,total
	// 1,120
	// 2,340
	// true
}
