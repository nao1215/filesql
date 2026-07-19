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

	"github.com/nao1215/filesql"
	_ "modernc.org/sqlite"
)

type exampleLogger struct {
	messages []string
}

func (l *exampleLogger) Debug(msg string, _ ...any) { l.messages = append(l.messages, msg) }
func (l *exampleLogger) Info(msg string, _ ...any)  { l.messages = append(l.messages, msg) }
func (l *exampleLogger) Warn(msg string, _ ...any)  { l.messages = append(l.messages, msg) }
func (l *exampleLogger) Error(msg string, _ ...any) { l.messages = append(l.messages, msg) }
func (l *exampleLogger) With(_ ...any) filesql.Logger {
	return l
}

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

func ExampleDBBuilder_WithLogger() {
	recorder := &exampleLogger{}

	validated, err := filesql.NewBuilder().
		WithLogger(recorder).
		AddReader(strings.NewReader("id,name\n1,Alice\n"), "users", filesql.FileTypeCSV).
		Build(context.Background())
	if err != nil {
		log.Fatal(err)
	}

	db, err := validated.Open(context.Background())
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	fmt.Println(recorder.messages[0])
	fmt.Println(recorder.messages[len(recorder.messages)-1])
	// Output:
	// starting build
	// database opened successfully
}

func ExampleDBBuilder_DisableAutoSave() {
	dir := createFilesqlExampleDir(map[string]string{
		"users.csv": `
id,name
1,Alice
`,
	})
	defer os.RemoveAll(dir)

	outputDir := filepath.Join(dir, "backup")
	validated, err := filesql.NewBuilder().
		AddPath(filepath.Join(dir, "users.csv")).
		EnableAutoSave(outputDir).
		DisableAutoSave().
		Build(context.Background())
	if err != nil {
		log.Fatal(err)
	}

	db, err := validated.Open(context.Background())
	if err != nil {
		log.Fatal(err)
	}
	ctx := context.Background()
	if _, err := db.ExecContext(ctx, `INSERT INTO users VALUES (2, 'Bob')`); err != nil {
		log.Fatal(err)
	}
	if err := db.Close(); err != nil {
		log.Fatal(err)
	}

	_, err = os.Stat(filepath.Join(outputDir, "users.csv"))
	fmt.Printf("autosaved=%t\n", err == nil)
	// Output:
	// autosaved=false
}

func ExampleDBBuilder_OpenReadOnly() {
	validated, err := filesql.NewBuilder().
		AddReader(strings.NewReader("id,name\n1,Alice\n2,Bob\n"), "users", filesql.FileTypeCSV).
		Build(context.Background())
	if err != nil {
		log.Fatal(err)
	}

	rodb, err := validated.OpenReadOnly(context.Background())
	if err != nil {
		log.Fatal(err)
	}
	defer rodb.Close()

	var rows int
	if err := rodb.QueryRow(`SELECT COUNT(*) FROM users`).Scan(&rows); err != nil {
		log.Fatal(err)
	}

	_, err = rodb.Exec(`INSERT INTO users VALUES (3, 'Cora')`)
	fmt.Printf("rows=%d write_blocked=%t\n", rows, errors.Is(err, filesql.ErrReadOnly))
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

func ExampleNewErrorContext() {
	err := filesql.NewErrorContext("import", "users.csv").
		WithTable("users").
		WithDetails("duplicate column").
		Error(filesql.ErrDuplicateColumn)
	fmt.Println(err)
	// Output:
	// filesql: import failed, file: users.csv, table: users, details: duplicate column: filesql: duplicate column name
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

func ExampleNewSlogAdapter() {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{ReplaceAttr: func(_ []string, attr slog.Attr) slog.Attr {
		if attr.Key == slog.TimeKey {
			return slog.Attr{}
		}
		return attr
	}}))

	filesql.NewSlogAdapter(logger).Info("loaded", "rows", 2)
	fmt.Println(strings.Contains(buf.String(), "loaded"))
	// Output:
	// true
}
