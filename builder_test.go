package filesql

import (
	"bytes"
	"context"
	"database/sql"
	"embed"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"testing/fstest"
	"time"

	"github.com/nao1215/filesql/dialect"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"modernc.org/sqlite"
)

//go:embed testdata/embed_test/*.csv testdata/embed_test/*.tsv
var testFS embed.FS

// buildForTest runs the validation that Open, OpenReadOnly, LoadInto and
// LoadIntoTx run before they load anything, and hands the builder back. The
// tests below check that validation on its own, which the terminal methods no
// longer let a caller do separately.
func buildForTest(ctx context.Context, b *DBBuilder) (*DBBuilder, error) {
	if err := b.build(ctx); err != nil {
		return nil, err
	}
	return b, nil
}

func TestNewBuilder(t *testing.T) {
	t.Parallel()

	builder := NewBuilder()
	require.NotNil(t, builder, "NewBuilder() should not return nil")
	assert.Len(t, builder.paths, 0, "NewBuilder() should have empty paths slice")
	assert.Len(t, builder.filesystems, 0, "NewBuilder() should have empty filesystems slice")
}

func TestDBBuilder_AddPath(t *testing.T) {
	t.Parallel()

	t.Run("single path", func(t *testing.T) {
		t.Parallel()
		builder := NewBuilder().AddPath("test.csv")
		assert.Len(t, builder.paths, 1, "should have 1 path")
		assert.Equal(t, "test.csv", builder.paths[0], "first path should be test.csv")
	})

	t.Run("chain multiple paths", func(t *testing.T) {
		t.Parallel()
		builder := NewBuilder().
			AddPath("test1.csv").
			AddPath("test2.tsv")
		assert.Len(t, builder.paths, 2, "should have 2 paths after chaining")
	})
}

func TestDBBuilder_AddPaths(t *testing.T) {
	t.Parallel()

	builder := NewBuilder().AddPaths("test1.csv", "test2.tsv", "test3.ltsv")
	assert.Len(t, builder.paths, 3, "should have 3 paths after AddPaths")
}

func TestDBBuilder_AddFS(t *testing.T) {
	t.Parallel()

	t.Run("add filesystem", func(t *testing.T) {
		t.Parallel()
		mockFS := fstest.MapFS{
			"data.csv": &fstest.MapFile{Data: []byte("col1,col2\nval1,val2\n")},
		}

		builder := NewBuilder().AddFS(mockFS)
		assert.Len(t, builder.filesystems, 1, "should have 1 filesystem")
	})

	t.Run("add multiple filesystems", func(t *testing.T) {
		t.Parallel()
		mockFS1 := fstest.MapFS{
			"data1.csv": &fstest.MapFile{Data: []byte("col1,col2\nval1,val2\n")},
		}
		mockFS2 := fstest.MapFS{
			"data2.csv": &fstest.MapFile{Data: []byte("col1,col2\nval3,val4\n")},
		}

		builder := NewBuilder().AddFS(mockFS1).AddFS(mockFS2)
		assert.Len(t, builder.filesystems, 2, "should have 2 filesystems")
	})
}

// TestDBBuilder_AddFS_Compressed loads a genuinely compressed file out of an
// fs.FS. AddFS hands the file over still wrapped, so the codec has to travel
// alongside the format on the reader input: the file name is the only place it
// is written down, and nothing downstream sees that name.
func TestDBBuilder_AddFS_Compressed(t *testing.T) {
	t.Parallel()

	csvData := "name,age\nAlice,30\nBob,25\n"

	codecs := []struct {
		compression CompressionType
		ext         string
	}{
		{CompressionGZ, ".gz"},
		{CompressionXZ, ".xz"},
		{CompressionZSTD, ".zst"},
		{CompressionZLIB, ".z"},
		{CompressionSNAPPY, ".snappy"},
		{CompressionS2, ".s2"},
		{CompressionLZ4, ".lz4"},
	}

	for _, codec := range codecs {
		t.Run(codec.compression.String(), func(t *testing.T) {
			t.Parallel()

			var compressed bytes.Buffer
			w, closeWriter, err := NewCompressionHandler(codec.compression).CreateWriter(&compressed)
			require.NoError(t, err)
			_, err = w.Write([]byte(csvData))
			require.NoError(t, err)
			require.NoError(t, closeWriter())

			mockFS := fstest.MapFS{
				"users.csv" + codec.ext: &fstest.MapFile{Data: compressed.Bytes()},
			}

			ctx := context.Background()
			validated, err := buildForTest(ctx, NewBuilder().AddFS(mockFS))
			require.NoError(t, err, "build() failed for %s", codec.compression)

			db, err := validated.Open(ctx)
			require.NoError(t, err, "Open(context.Background(), ) failed for %s", codec.compression)
			defer db.Close()

			var name string
			var age int
			err = db.QueryRowContext(ctx, "SELECT name, age FROM users ORDER BY age").Scan(&name, &age)
			require.NoError(t, err, "query failed for %s", codec.compression)
			assert.Equal(t, "Bob", name)
			assert.Equal(t, 25, age)
		})
	}
}

// TestDBBuilder_AddFS_CompressedHeaderOnly loads a compressed file that has a
// header and no rows. The chunked reader and the header-only fallback are two
// different paths over the same reader, and only one of them can consume it, so
// the case is worth pinning separately from the file that has rows.
func TestDBBuilder_AddFS_CompressedHeaderOnly(t *testing.T) {
	t.Parallel()

	for _, codec := range []CompressionType{CompressionNone, CompressionGZ, CompressionZSTD, CompressionLZ4} {
		t.Run(codec.String(), func(t *testing.T) {
			t.Parallel()

			var compressed bytes.Buffer
			w, closeWriter, err := NewCompressionHandler(codec).CreateWriter(&compressed)
			require.NoError(t, err)
			_, err = w.Write([]byte("id,name,email\n"))
			require.NoError(t, err)
			require.NoError(t, closeWriter())

			mockFS := fstest.MapFS{
				"empty" + extCSV + codec.Extension(): &fstest.MapFile{Data: compressed.Bytes()},
			}

			ctx := context.Background()
			validated, err := buildForTest(ctx, NewBuilder().AddFS(mockFS))
			require.NoError(t, err, "build() failed for %s", codec)

			db, err := validated.Open(ctx)
			require.NoError(t, err, "Open(context.Background(), ) failed for %s", codec)
			defer db.Close()

			// The table exists with the header's columns and holds no rows.
			var count int
			require.NoError(t, db.QueryRowContext(ctx, "SELECT COUNT(*) FROM empty").Scan(&count))
			assert.Equal(t, 0, count, "a header-only file should load no rows")

			rows, err := db.QueryContext(ctx, "SELECT id, name, email FROM empty")
			require.NoError(t, err, "the header's columns should exist for %s", codec)
			defer rows.Close()
			cols, err := rows.Columns()
			require.NoError(t, err)
			assert.Equal(t, []string{"id", "name", "email"}, cols)
			require.NoError(t, rows.Err())
		})
	}
}

func TestDBBuilder_AddReader(t *testing.T) {
	t.Parallel()

	t.Run("add CSV reader", func(t *testing.T) {
		t.Parallel()
		data := "name,age\nAlice,30\nBob,25\n"
		reader := bytes.NewReader([]byte(data))

		builder := NewBuilder().AddReader(reader, "users", FileTypeCSV)
		assert.Len(t, builder.readers, 1, "should have 1 reader")
		assert.Equal(t, "users", builder.readers[0].tableName, "table name should be users")
		assert.Equal(t, FileTypeCSV, builder.readers[0].fileType, "file type should be CSV")
		// No compression fields to check since FileTypeCSV is uncompressed
	})

	t.Run("add TSV reader", func(t *testing.T) {
		t.Parallel()
		data := "col1\tcol2\nval1\tval2\n"
		reader := bytes.NewReader([]byte(data))

		builder := NewBuilder().AddReader(reader, "data", FileTypeTSV)
		assert.Len(t, builder.readers, 1, "should have 1 reader")
		assert.Equal(t, FileTypeTSV, builder.readers[0].fileType, "file type should be TSV")
	})

	t.Run("add compressed CSV reader", func(t *testing.T) {
		t.Parallel()
		data := []byte{} // Empty data for test
		reader := bytes.NewReader(data)

		builder := NewBuilder().AddReader(reader, "logs", FileTypeCSV, WithCompression(CompressionGZ))
		assert.Len(t, builder.readers, 1, "should have 1 reader")
		assert.Equal(t, FileTypeCSV, builder.readers[0].fileType, "file type should be CSV")
		assert.Equal(t, CompressionGZ, builder.readers[0].compression, "compression should be gzip")
	})

	t.Run("add multiple readers", func(t *testing.T) {
		t.Parallel()
		reader1 := bytes.NewReader([]byte("col1,col2\nval1,val2\n"))
		reader2 := bytes.NewReader([]byte("col3\tcol4\nval3\tval4\n"))

		builder := NewBuilder().
			AddReader(reader1, "table1", FileTypeCSV).
			AddReader(reader2, "table2", FileTypeTSV)

		assert.Len(t, builder.readers, 2, "should have 2 readers")
	})
}

func TestDBBuilder_SetDefaultChunkSize(t *testing.T) {
	t.Parallel()

	t.Run("set custom chunk size", func(t *testing.T) {
		t.Parallel()
		customSize := 2000 // 2000 rows
		builder := NewBuilder().SetDefaultChunkSize(customSize)

		assert.Equal(t, customSize, builder.defaultChunkSize, "default chunk size should be set to custom size")
	})

	t.Run("zero or negative size ignored", func(t *testing.T) {
		t.Parallel()
		defaultSize := defaultChunkSizeRows
		builder := NewBuilder()

		// Zero should be ignored
		builder.SetDefaultChunkSize(0)
		assert.Equal(t, defaultSize, builder.defaultChunkSize, "chunk size should not change when set to zero")

		// Negative should be ignored
		builder.SetDefaultChunkSize(-1)
		assert.Equal(t, defaultSize, builder.defaultChunkSize, "chunk size should not change when set to negative")
	})

	t.Run("the configured size is the size the loader reads in", func(t *testing.T) {
		t.Parallel()

		const rows = 10
		body := chunkCountingCSV(rows)
		dir := t.TempDir()
		path := filepath.Join(dir, "counted.csv")
		require.NoError(t, os.WriteFile(path, []byte(body), 0o600))

		ctx := context.Background()
		for _, size := range []int{1, 2, 5, 10, defaultChunkSizeRows} {
			want := (rows + size - 1) / size

			counter := &chunkCountingHandler{}
			validated, err := buildForTest(

				ctx, NewBuilder().
					AddPath(path).
					SetDefaultChunkSize(size).
					WithLogger(slog.New(counter)))

			require.NoError(t, err)
			db, err := validated.Open(ctx)
			require.NoError(t, err)
			require.NoError(t, db.Close())
			assert.Equal(t, want, counter.count(), "chunk size %d over %d rows", size, rows)

			// A reader is loaded by the same processor, so it has to chunk the
			// same way a path does.
			counter = &chunkCountingHandler{}
			validated, err = buildForTest(

				ctx, NewBuilder().
					AddReader(strings.NewReader(body), "counted", FileTypeCSV).
					SetDefaultChunkSize(size).
					WithLogger(slog.New(counter)))

			require.NoError(t, err)
			db, err = validated.Open(ctx)
			require.NoError(t, err)
			require.NoError(t, db.Close())
			assert.Equal(t, want, counter.count(), "chunk size %d over %d rows, from a reader", size, rows)
		}
	})

	t.Run("the loaded table is the same whatever the chunk size", func(t *testing.T) {
		t.Parallel()

		body := chunkCountingCSV(20)
		ctx := context.Background()

		var want string
		for _, size := range []int{1, 2, 7, 20, defaultChunkSizeRows} {
			validated, err := buildForTest(

				ctx, NewBuilder().
					AddReader(strings.NewReader(body), "counted", FileTypeCSV).
					SetDefaultChunkSize(size))

			require.NoError(t, err)
			db, err := validated.Open(ctx)
			require.NoError(t, err)
			got := describeTableForChunkTest(t, db, "counted")
			require.NoError(t, db.Close())

			if want == "" {
				want = got
				continue
			}
			assert.Equal(t, want, got, "chunk size %d loaded a different table", size)
		}
	})
}

// chunkCountingCSV builds a CSV of n rows whose columns cover the three types
// the loader infers, so a chunk-size difference shows up in the schema as well
// as in the row count.
func chunkCountingCSV(n int) string {
	var b strings.Builder
	b.WriteString("id,amount,name\n")
	for i := 1; i <= n; i++ {
		fmt.Fprintf(&b, "%d,%d.5,name%d\n", i, i, i)
	}
	return b.String()
}

// chunkCountingHandler counts the chunks the loader reports inserting, which is
// how a caller can observe the chunk size from outside the package. It is a
// slog.Handler because that is where a caller's own logging plugs in now that
// the builder takes a *slog.Logger.
type chunkCountingHandler struct {
	mu     sync.Mutex
	chunks int
}

func (h *chunkCountingHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h *chunkCountingHandler) Handle(_ context.Context, record slog.Record) error {
	if record.Message == "inserting chunk" {
		h.mu.Lock()
		h.chunks++
		h.mu.Unlock()
	}
	return nil
}

func (h *chunkCountingHandler) WithAttrs([]slog.Attr) slog.Handler { return h }

func (h *chunkCountingHandler) WithGroup(string) slog.Handler { return h }

func (h *chunkCountingHandler) count() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.chunks
}

// describeTableForChunkTest renders a table's declared types and every value it
// holds, including the Go type each value scanned as, so two loads can be
// compared for more than their row count.
func describeTableForChunkTest(t *testing.T, db *sql.DB, tableName string) string {
	t.Helper()

	ctx := context.Background()
	var out strings.Builder

	schema, err := db.QueryContext(ctx, `SELECT name, type FROM pragma_table_info(?)`, tableName)
	require.NoError(t, err)
	for schema.Next() {
		var name, columnType string
		require.NoError(t, schema.Scan(&name, &columnType))
		fmt.Fprintf(&out, "%s:%s ", name, columnType)
	}
	require.NoError(t, schema.Err())
	require.NoError(t, schema.Close())
	out.WriteString("\n")

	rows, err := db.QueryContext(ctx, fmt.Sprintf(`SELECT * FROM %q`, tableName))
	require.NoError(t, err)
	defer rows.Close()
	columns, err := rows.Columns()
	require.NoError(t, err)
	for rows.Next() {
		values := make([]any, len(columns))
		pointers := make([]any, len(columns))
		for i := range values {
			pointers[i] = &values[i]
		}
		require.NoError(t, rows.Scan(pointers...))
		for _, v := range values {
			fmt.Fprintf(&out, "%T(%v)|", v, v)
		}
		out.WriteString("\n")
	}
	require.NoError(t, rows.Err())

	return out.String()
}

func TestDBBuilder_build(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	t.Run("no inputs error", func(t *testing.T) {
		t.Parallel()
		builder := NewBuilder()
		_, err := buildForTest(ctx, builder)
		assert.Error(t, err, "build() should return error for no inputs")
	})

	t.Run("reader with nil reader error", func(t *testing.T) {
		t.Parallel()
		builder := NewBuilder()
		builder.readers = append(builder.readers, readerInput{
			reader:    nil,
			tableName: "test",
			fileType:  FileTypeCSV,
		})

		_, err := buildForTest(ctx, builder)
		assert.Error(t, err, "build() should return error for nil reader")
		assert.Contains(t, err.Error(), "reader cannot be nil", "error message should mention nil reader")
	})

	t.Run("reader with empty table name error", func(t *testing.T) {
		t.Parallel()
		reader := bytes.NewReader([]byte("test"))
		builder := NewBuilder()
		builder.readers = append(builder.readers, readerInput{
			reader:    reader,
			tableName: "",
			fileType:  FileTypeCSV,
		})

		_, err := buildForTest(ctx, builder)
		assert.Error(t, err, "build() should return error for empty table name")
		assert.Contains(t, err.Error(), "table name must be specified", "error message should mention table name requirement")
	})

	t.Run("reader with unsupported file type error", func(t *testing.T) {
		t.Parallel()
		reader := bytes.NewReader([]byte("test"))
		builder := NewBuilder()
		builder.readers = append(builder.readers, readerInput{
			reader:    reader,
			tableName: "test",
			fileType:  FileTypeUnsupported,
		})

		_, err := buildForTest(ctx, builder)
		assert.Error(t, err, "build() should return error for unsupported file type")
		assert.Contains(t, err.Error(), "file type must be specified", "error message should mention file type requirement")
	})

	t.Run("reader with valid CSV data", func(t *testing.T) {
		t.Parallel()
		data := "name,age\nAlice,30\nBob,25\n"
		reader := bytes.NewReader([]byte(data))
		builder := NewBuilder().AddReader(reader, "users", FileTypeCSV)

		validatedBuilder, err := buildForTest(ctx, builder)
		assert.NoError(t, err, "build() should succeed with valid CSV data")
		require.NotNil(t, validatedBuilder, "build() should not return nil builder")
		// Readers don't create temp files anymore - they use direct streaming
		assert.Len(t, validatedBuilder.readers, 1, "build() should have 1 reader input")

		// Clean up temp files
	})

	t.Run("reader with compressed type specification", func(t *testing.T) {
		t.Parallel()
		// Note: Use regular CSV data since we're testing the type system, not actual compression
		data := []byte("col1,col2\nval1,val2\n")
		reader := bytes.NewReader(data)
		builder := NewBuilder().AddReader(reader, "logs", FileTypeCSV)

		validatedBuilder, err := buildForTest(ctx, builder)
		assert.NoError(t, err, "build() should succeed with compressed type")
		assert.NotNil(t, validatedBuilder, "build() should not return nil builder")

		// Clean up temp files
	})

	t.Run("multiple readers", func(t *testing.T) {
		t.Parallel()
		reader1 := bytes.NewReader([]byte("col1,col2\nval1,val2\n"))
		reader2 := bytes.NewReader([]byte("col3\tcol4\nval3\tval4\n"))

		builder := NewBuilder().
			AddReader(reader1, "table1", FileTypeCSV).
			AddReader(reader2, "table2", FileTypeTSV)

		validatedBuilder, err := buildForTest(ctx, builder)
		assert.NoError(t, err, "build() should succeed with multiple readers")
		require.NotNil(t, validatedBuilder, "build() should not return nil builder")
		// Readers don't create temp files anymore - they use direct streaming
		assert.Len(t, validatedBuilder.readers, 2, "build() should have 2 reader inputs")

		// Clean up temp files
	})

	t.Run("invalid path error", func(t *testing.T) {
		t.Parallel()
		builder := NewBuilder().AddPath(filepath.Join("nonexistent", "file.csv"))
		_, err := buildForTest(ctx, builder)
		assert.Error(t, err, "build() should return error for nonexistent path")
	})

	t.Run("unsupported file type error", func(t *testing.T) {
		t.Parallel()
		// Create a temporary unsupported file
		tempDir := t.TempDir()
		unsupportedFile := filepath.Join(tempDir, "test.txt")
		err := os.WriteFile(unsupportedFile, []byte("test"), 0600)
		require.NoError(t, err, "should create test file")

		builder := NewBuilder().AddPath(unsupportedFile)
		_, err = buildForTest(ctx, builder)
		assert.Error(t, err, "build() should return error for unsupported file type")
	})

	t.Run("valid CSV file", func(t *testing.T) {
		t.Parallel()
		// Create a temporary CSV file
		tempDir := t.TempDir()
		csvFile := filepath.Join(tempDir, "test.csv")
		content := "col1,col2\nval1,val2\n"
		err := os.WriteFile(csvFile, []byte(content), 0600)
		require.NoError(t, err, "should create CSV file")

		builder := NewBuilder().AddPath(csvFile)
		validatedBuilder, err := buildForTest(ctx, builder)
		assert.NoError(t, err, "build() should succeed with valid CSV file")
		assert.NotNil(t, validatedBuilder, "build() should not return nil builder")
	})

	t.Run("valid directory", func(t *testing.T) {
		t.Parallel()
		tempDir := t.TempDir()

		// Create a valid CSV file in the temp directory
		csvFile := filepath.Join(tempDir, "test.csv")
		csvContent := "id,name,age\n1,John,30\n2,Jane,25\n"
		err := os.WriteFile(csvFile, []byte(csvContent), 0600)
		require.NoError(t, err, "Failed to create test CSV file")

		builder := NewBuilder().AddPath(tempDir)
		validatedBuilder, err := buildForTest(ctx, builder)
		assert.NoError(t, err, "build() should succeed with valid directory")
		assert.NotNil(t, validatedBuilder, "build() should not return nil builder")
	})

	t.Run("FS with valid files", func(t *testing.T) {
		t.Parallel()
		mockFS := fstest.MapFS{
			"data.csv":     &fstest.MapFile{Data: []byte("col1,col2\nval1,val2\n")},
			"products.tsv": &fstest.MapFile{Data: []byte("id\tname\n1\tLaptop\n")},
			"logs.ltsv":    &fstest.MapFile{Data: []byte("time:2023-01-01T00:00:00Z\tlevel:info\n")},
			"readme.txt":   &fstest.MapFile{Data: []byte("This is not a supported file\n")}, // Should be ignored
		}

		builder := NewBuilder().AddFS(mockFS)
		validatedBuilder, err := buildForTest(ctx, builder)
		assert.NoError(t, err, "build() should succeed with FS containing valid files")
		require.NotNil(t, validatedBuilder, "build() should not return nil builder")
		// Should have found 3 files (csv, tsv, ltsv) and ignored txt
		// fs.FS files become readers of the build's own rather than paths.
		assert.Len(t, validatedBuilder.derivedReaders, 3, "build() should have 3 readers from fs.FS")
		assert.Empty(t, validatedBuilder.readers, "a filesystem adds nothing to the caller's own readers")
	})

	t.Run("FS with nil filesystem error", func(t *testing.T) {
		t.Parallel()
		builder := NewBuilder()
		builder.filesystems = append(builder.filesystems, nil)

		_, err := buildForTest(ctx, builder)
		assert.Error(t, err, "build() should return error for nil FS")
	})

	t.Run("FS with no supported files error", func(t *testing.T) {
		t.Parallel()
		mockFS := fstest.MapFS{
			"readme.txt": &fstest.MapFile{Data: []byte("Not supported\n")},
			"data.xml":   &fstest.MapFile{Data: []byte("<data/>\n")},
		}

		builder := NewBuilder().AddFS(mockFS)
		_, err := buildForTest(ctx, builder)
		assert.Error(t, err, "build() should return error for FS with no supported files")
	})
}

func TestDBBuilder_ChunkedReading(t *testing.T) {
	t.Parallel()

	t.Run("large data chunked reading", func(t *testing.T) {
		t.Parallel()

		// Skip this test in local development, only run on GitHub Actions
		if os.Getenv("GITHUB_ACTIONS") == "" {
			t.Skip("Skipping large data chunked reading test in local development")
		}

		// Create a dataset that would benefit from chunked reading
		var data bytes.Buffer
		data.WriteString("id,name,value\n")
		for i := range 10000 { // Full test on GitHub Actions
			fmt.Fprintf(&data, "%d,name_%d,%d\n", i, i, i*10)
		}

		reader := bytes.NewReader(data.Bytes())
		chunkSize := 1024 // Small chunk for testing
		builder := NewBuilder().
			SetDefaultChunkSize(chunkSize).
			AddReader(reader, "large_table", FileTypeCSV)

		ctx := context.Background()
		validatedBuilder, err := buildForTest(ctx, builder)
		require.NoError(t, err, "build() should succeed")

		db, err := validatedBuilder.Open(ctx)
		assert.NoError(t, err, "Open(context.Background(), ) should succeed")
		require.NotNil(t, db, "Open(context.Background(), ) should not return nil database")
		// Verify the data was loaded correctly
		var count int
		err = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM large_table").Scan(&count)
		assert.NoError(t, err, "Count query should succeed")
		assert.Equal(t, 10000, count, "Should have 10000 rows")
		_ = db.Close()

		// Clean up temp files
	})
}

func TestDBBuilder_Open_WithReader(t *testing.T) {
	ctx := context.Background()

	t.Run("successful open with reader", func(t *testing.T) {
		data := "name,age\nAlice,30\nBob,25\n"
		reader := bytes.NewReader([]byte(data))
		builder := NewBuilder().AddReader(reader, "users", FileTypeCSV)

		validatedBuilder, err := buildForTest(ctx, builder)
		require.NoError(t, err, "build() should succeed")

		db, err := validatedBuilder.Open(ctx)
		assert.NoError(t, err, "Open(context.Background(), ) should succeed")
		require.NotNil(t, db, "Open(context.Background(), ) should not return nil database")
		// Verify we can query the data
		rows, err := db.QueryContext(ctx, "SELECT * FROM users")
		assert.NoError(t, err, "Query should succeed")
		defer rows.Close()
		assert.NoError(t, rows.Err(), "Rows should not have errors")
		_ = db.Close()

		// Clean up temp files
	})

	t.Run("mixed inputs - reader and file", func(t *testing.T) {
		// Create a temporary CSV file
		tempDir := t.TempDir()
		csvFile := filepath.Join(tempDir, "orders.csv")
		fileContent := "order_id,amount\n1,100\n2,200\n"
		err := os.WriteFile(csvFile, []byte(fileContent), 0600)
		require.NoError(t, err, "should create orders CSV file")

		// Create a reader with different data
		readerData := "product_id,name\n1,Laptop\n2,Mouse\n"
		reader := bytes.NewReader([]byte(readerData))

		builder := NewBuilder().
			AddPath(csvFile).
			AddReader(reader, "products", FileTypeCSV)

		validatedBuilder, err := buildForTest(ctx, builder)
		require.NoError(t, err, "build() should succeed with mixed inputs")

		db, err := validatedBuilder.Open(ctx)
		assert.NoError(t, err, "Open(context.Background(), ) should succeed")
		require.NotNil(t, db, "Open(context.Background(), ) should not return nil database")
		// Verify both tables exist
		for _, table := range []string{"orders", "products"} {
			rows, err := db.QueryContext(ctx, "SELECT * FROM "+table) // #nosec G202 -- table name is safe
			assert.NoError(t, err, "Query %s should succeed", table)
			assert.NoError(t, rows.Err(), "Rows should not have errors for %s", table)
			_ = rows.Close() // Close immediately in the loop
		}
		_ = db.Close()

		// Clean up temp files
	})
}

func TestDBBuilder_Open(t *testing.T) {
	ctx := context.Background()

	t.Run("open without build reports what is wrong with the input", func(t *testing.T) {
		builder := NewBuilder().AddPath("test.csv")
		// Open validates on its own, so the caller hears about the missing
		// file rather than about a step they did not take.
		db, err := builder.Open(ctx)
		if db != nil {
			_ = db.Close()
		}
		require.Error(t, err, "Open(context.Background(), ) should refuse a path that does not exist")
		assert.True(t, errors.Is(err, ErrFileNotFound), "error should be ErrFileNotFound, got %v", err)
		assert.Contains(t, err.Error(), "test.csv", "error should name the file")
	})

	t.Run("open without build refuses a builder with no input", func(t *testing.T) {
		db, err := NewBuilder().Open(ctx)
		if db != nil {
			_ = db.Close()
		}
		require.Error(t, err, "Open(context.Background(), ) should refuse a builder with nothing added")
		assert.True(t, errors.Is(err, ErrNoFiles), "error should be ErrNoFiles, got %v", err)
	})

	t.Run("validating then opening loads an added filesystem once", func(t *testing.T) {
		// The validation derives a reader per file of the filesystem, so
		// running it twice would register every one of them twice.
		fsys := fstest.MapFS{
			"users.csv": &fstest.MapFile{Data: []byte("id,name\n1,Alice\n")},
		}

		validated, err := buildForTest(ctx, NewBuilder().AddFS(fsys))
		require.NoError(t, err, "build() should succeed")

		db, err := validated.Open(ctx)
		require.NoError(t, err, "Open(context.Background(), ) after the validation should succeed")
		defer func() { _ = db.Close() }()

		var rows int
		require.NoError(t, db.QueryRowContext(ctx, "SELECT COUNT(*) FROM users").Scan(&rows))
		assert.Equal(t, 1, rows, "the file must be loaded once, not once per validation")
	})

	t.Run("open after a failed open loads an added filesystem once", func(t *testing.T) {
		// The first open fails on the auto-save target, which is a check that
		// runs after the build has derived its readers from the filesystem.
		// Those readers must not stay behind: registering them a second time
		// loads the file twice.
		dir := t.TempDir()
		jsonPath := filepath.Join(dir, "orders.json")
		require.NoError(t, os.WriteFile(jsonPath, []byte(`[{"id":1}]`), 0600))

		fsys := fstest.MapFS{
			"users.csv": &fstest.MapFile{Data: []byte("id,name\n1,Alice\n")},
		}
		// Overwrite mode cannot write a JSON source back to itself.
		builder := NewBuilder().AddFS(fsys).AddPath(jsonPath).EnableAutoSave("")

		db, err := builder.Open(ctx)
		if db != nil {
			_ = db.Close()
		}
		require.Error(t, err, "Open(context.Background(), ) should refuse to overwrite a source it cannot write")

		// Correct the configuration and try again.
		db, err = builder.EnableAutoSave(filepath.Join(dir, "out")).Open(ctx)
		require.NoError(t, err, "Open(context.Background(), ) should succeed once auto-save has an output directory")
		defer func() { _ = db.Close() }()

		var rows int
		require.NoError(t, db.QueryRowContext(ctx, "SELECT COUNT(*) FROM users").Scan(&rows))
		assert.Equal(t, 1, rows, "the filesystem's file must be loaded once, not once per attempt")
	})

	t.Run("successful open with CSV file", func(t *testing.T) {
		// Create a temporary CSV file
		tempDir := t.TempDir()
		csvFile := filepath.Join(tempDir, "test.csv")
		content := "col1,col2\nval1,val2\n"
		err := os.WriteFile(csvFile, []byte(content), 0600)
		require.NoError(t, err, "should create CSV file")

		builder := NewBuilder().AddPath(csvFile)
		validatedBuilder, err := buildForTest(ctx, builder)
		require.NoError(t, err, "build() should succeed")

		db, err := validatedBuilder.Open(ctx)
		assert.NoError(t, err, "Open(context.Background(), ) should succeed")
		assert.NotNil(t, db, "Open(context.Background(), ) should not return nil database")
		if db != nil {
			_ = db.Close()
		}
	})

	t.Run("successful open with FS", func(t *testing.T) {
		mockFS := fstest.MapFS{
			"data.csv": &fstest.MapFile{Data: []byte("col1,col2\nval1,val2\n")},
		}

		builder := NewBuilder().AddFS(mockFS)
		validatedBuilder, err := buildForTest(ctx, builder)
		require.NoError(t, err, "build() should succeed")

		db, err := validatedBuilder.Open(ctx)
		assert.NoError(t, err, "Open(context.Background(), ) should succeed")
		assert.NotNil(t, db, "Open(context.Background(), ) should not return nil database")
		if db != nil {
			_ = db.Close()
			// Clean up temp files
		}
	})

	t.Run("successful open with glob pattern", func(t *testing.T) {
		mockFS := fstest.MapFS{
			"data1.csv": &fstest.MapFile{Data: []byte("col1,col2\nval1,val2\n")},
			"data2.csv": &fstest.MapFile{Data: []byte("col1,col2\nval3,val4\n")},
		}

		builder := NewBuilder().AddFS(mockFS)
		validatedBuilder, err := buildForTest(ctx, builder)
		require.NoError(t, err, "build() should succeed")

		db, err := validatedBuilder.Open(ctx)
		assert.NoError(t, err, "Open(context.Background(), ) should succeed")
		assert.NotNil(t, db, "Open(context.Background(), ) should not return nil database")
		if db != nil {
			_ = db.Close()
			// Clean up temp files
		}
	})
}

func TestDBBuilder_processFSInput(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	t.Run("multiple supported files", func(t *testing.T) {
		t.Parallel()
		mockFS := fstest.MapFS{
			"data.csv":     &fstest.MapFile{Data: []byte("col1,col2\nval1,val2\n")},
			"products.tsv": &fstest.MapFile{Data: []byte("id\tname\n1\tLaptop\n")},
			"logs.ltsv":    &fstest.MapFile{Data: []byte("time:2023-01-01T00:00:00Z\tlevel:info\n")},
			"readme.txt":   &fstest.MapFile{Data: []byte("Not supported\n")}, // Should be ignored
		}

		builder := NewBuilder()

		readers, err := builder.fileProcessor.processFSToReaders(ctx, mockFS)
		assert.NoError(t, err, "processFSToReaders() should succeed")
		assert.Len(t, readers, 3, "should return 3 readers")

		// Close all readers
		for _, reader := range readers {
			if closer, ok := reader.reader.(io.Closer); ok {
				_ = closer.Close()
			}
		}
	})

	t.Run("compressed files", func(t *testing.T) {
		t.Parallel()
		mockFS := fstest.MapFS{
			"data.csv.gz":   &fstest.MapFile{Data: []byte("compressed csv data")},
			"logs.ltsv.bz2": &fstest.MapFile{Data: []byte("compressed ltsv data")},
		}

		builder := NewBuilder()

		readers, err := builder.fileProcessor.processFSToReaders(ctx, mockFS)
		assert.NoError(t, err, "processFSToReaders() should succeed with compressed files")
		assert.Len(t, readers, 2, "should return 2 readers for compressed files")

		// Close all readers
		for _, reader := range readers {
			if closer, ok := reader.reader.(io.Closer); ok {
				_ = closer.Close()
			}
		}
	})
}

func TestIntegrationWithEmbedFS(t *testing.T) {
	ctx := context.Background()

	// Use embedded test data from embed_test subdirectory
	subFS, err := fs.Sub(testFS, "testdata/embed_test")
	require.NoError(t, err, "should create sub filesystem")

	// Test loading all supported files from embedded FS
	builder := NewBuilder().AddFS(subFS)

	validatedBuilder, err := buildForTest(ctx, builder)
	require.NoError(t, err, "build() should succeed with embedded FS")

	db, err := validatedBuilder.Open(ctx)
	assert.NoError(t, err, "Open(context.Background(), ) with embed.FS should succeed")
	require.NotNil(t, db, "Open(context.Background(), ) with embed.FS should not return nil database")
	// Verify we can query the database
	rows, err := db.QueryContext(ctx, "SELECT name FROM sqlite_master WHERE type='table'")
	assert.NoError(t, err, "should be able to query database")
	defer rows.Close()
	assert.NoError(t, rows.Err(), "rows should not have errors")

	_ = db.Close()
	// Clean up temp files
}

func TestAutoSave_OnClose(t *testing.T) {
	// Create temporary directory
	tmpDir := t.TempDir()

	// Create test CSV file
	csvPath := filepath.Join(tmpDir, "test.csv")
	csvContent := "name,age\nAlice,25\nBob,30\n"
	err := os.WriteFile(csvPath, []byte(csvContent), 0600)
	require.NoError(t, err, "Failed to write test CSV")

	// Create output directory
	outputDir := filepath.Join(tmpDir, "output")
	err = os.MkdirAll(outputDir, 0750)
	require.NoError(t, err, "Failed to create output dir")

	// Build database with auto-save on close
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	builder := NewBuilder().
		AddPath(csvPath).
		EnableAutoSave(outputDir)

	validatedBuilder, err := buildForTest(ctx, builder)
	require.NoError(t, err, "Build should succeed")

	db, err := validatedBuilder.Open(ctx)
	require.NoError(t, err, "Open should succeed")

	// Modify data
	_, err = db.ExecContext(ctx, "INSERT INTO test (name, age) VALUES ('Charlie', 35)")
	require.NoError(t, err, "Insert should succeed")

	// Close database (should trigger auto-save)
	err = db.Close()
	require.NoError(t, err, "Close should succeed")

	// Check if file was saved
	outputFile := filepath.Join(outputDir, "test.csv")
	_, err = os.Stat(outputFile)
	assert.False(t, os.IsNotExist(err), "Auto-save file should be created: %s", outputFile)

	// Verify content includes the new record
	content, err := os.ReadFile(outputFile) //nolint:gosec // Test file path is safe
	require.NoError(t, err, "should be able to read output file")

	assert.Contains(t, string(content), "Charlie", "Auto-saved file should contain inserted data")
}

func TestAutoSave_OnCommit(t *testing.T) {
	// Create temporary directory
	tmpDir := t.TempDir()

	// Create test CSV file
	csvPath := filepath.Join(tmpDir, "test.csv")
	csvContent := "name,age\nAlice,25\n"
	err := os.WriteFile(csvPath, []byte(csvContent), 0600)
	require.NoError(t, err, "Failed to write test CSV")

	// Create output directory
	outputDir := filepath.Join(tmpDir, "output")
	err = os.MkdirAll(outputDir, 0750)
	require.NoError(t, err, "Failed to create output dir")

	// Build database with auto-save on commit
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	builder := NewBuilder().
		AddPath(csvPath).
		EnableAutoSaveOnCommit(outputDir)

	validatedBuilder, err := buildForTest(ctx, builder)
	if err != nil {
		require.NoError(t, err, "Build should succeed")
	}

	db, err := validatedBuilder.Open(ctx)
	if err != nil {
		require.NoError(t, err, "Open should succeed")
	}
	defer db.Close()

	// Start transaction
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		require.NoError(t, err, "Begin transaction should succeed")
	}

	// Modify data within transaction
	_, err = tx.ExecContext(ctx, "INSERT INTO test (name, age) VALUES ('David', 40)")
	if err != nil {
		require.NoError(t, err, "Insert should succeed")
	}

	// Commit transaction (should trigger auto-save)
	err = tx.Commit()
	require.NoError(t, err, "Commit should succeed")

	// Check if file was saved
	outputFile := filepath.Join(outputDir, "test.csv")
	if _, err := os.Stat(outputFile); os.IsNotExist(err) {
		assert.FileExists(t, outputFile, "Auto-save file should be created")
	}

	// Verify content includes the new record
	content, err := os.ReadFile(outputFile) //nolint:gosec // Test file path is safe
	if err != nil {
		require.NoError(t, err, "should be able to read output file")
	}

	assert.Contains(t, string(content), "David", "Auto-saved file should contain committed data")
}

func TestAutoSave_OffByDefault(t *testing.T) {
	// Create temporary directory
	tmpDir := t.TempDir()

	// Create test CSV file
	csvPath := filepath.Join(tmpDir, "test.csv")
	csvContent := "name,age\nAlice,25\n"
	err := os.WriteFile(csvPath, []byte(csvContent), 0600)
	require.NoError(t, err, "Failed to write test CSV")

	// Create output directory
	outputDir := filepath.Join(tmpDir, "output")
	err = os.MkdirAll(outputDir, 0750)
	require.NoError(t, err, "Failed to create output dir")

	// Build database without auto-save (the default)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	builder := NewBuilder().
		AddPath(csvPath)
	// Note: No EnableAutoSave() call

	validatedBuilder, err := buildForTest(ctx, builder)
	if err != nil {
		require.NoError(t, err, "Build should succeed")
	}

	db, err := validatedBuilder.Open(ctx)
	if err != nil {
		require.NoError(t, err, "Open should succeed")
	}

	// Modify data
	_, err = db.ExecContext(ctx, "INSERT INTO test (name, age) VALUES ('Echo', 45)")
	if err != nil {
		require.NoError(t, err, "Insert should succeed")
	}

	// Close database (should NOT trigger auto-save)
	if err := db.Close(); err != nil {
		require.NoError(t, err, "Close should succeed")
	}

	// Check that no output file was created
	outputFile := filepath.Join(outputDir, "test.csv")
	if _, err := os.Stat(outputFile); !os.IsNotExist(err) {
		assert.NoFileExists(t, outputFile, "Auto-save file should not have been created when auto-save was never enabled")
	}
}

func TestAutoSave_MultipleCommitsOverwrite(t *testing.T) {
	// This test verifies that multiple commits properly overwrite the same file
	t.Parallel()

	tmpDir := t.TempDir()

	// Create test CSV file
	csvPath := filepath.Join(tmpDir, "test.csv")
	csvContent := "name,count\nInitial,1\n"
	err := os.WriteFile(csvPath, []byte(csvContent), 0600)
	require.NoError(t, err, "Failed to write test CSV")

	// Create output directory
	outputDir := filepath.Join(tmpDir, "output")
	err = os.MkdirAll(outputDir, 0750)
	require.NoError(t, err, "Failed to create output dir")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Build database with auto-save on commit
	builder := NewBuilder().
		AddPath(csvPath).
		EnableAutoSaveOnCommit(outputDir)

	validatedBuilder, err := buildForTest(ctx, builder)
	if err != nil {
		require.NoError(t, err, "Build should succeed")
	}

	db, err := validatedBuilder.Open(ctx)
	if err != nil {
		require.NoError(t, err, "Open should succeed")
	}
	defer db.Close()

	outputFile := filepath.Join(outputDir, "test.csv")

	// First commit: Add first record
	tx1, err := db.BeginTx(ctx, nil)
	if err != nil {
		require.NoError(t, err, "Begin first transaction should succeed")
	}

	_, err = tx1.ExecContext(ctx, "INSERT INTO test (name, count) VALUES ('First', 100)")
	if err != nil {
		require.NoError(t, err, "First insert should succeed")
	}

	if err := tx1.Commit(); err != nil {
		require.NoError(t, err, "First commit should succeed")
	}

	// Check first commit saved the file
	if _, err := os.Stat(outputFile); os.IsNotExist(err) {
		assert.FileExists(t, outputFile, "Auto-save file should be created after first commit")
	}

	// Read content after first commit
	content1, err := os.ReadFile(outputFile) //nolint:gosec // Test file path is safe
	if err != nil {
		require.NoError(t, err, "should be able to read output file after first commit")
	}

	assert.Contains(t, string(content1), "First", "File should contain first commit data")

	// Second commit: Add second record (should overwrite)
	tx2, err := db.BeginTx(ctx, nil)
	if err != nil {
		require.NoError(t, err, "Begin second transaction should succeed")
	}

	_, err = tx2.ExecContext(ctx, "INSERT INTO test (name, count) VALUES ('Second', 200)")
	if err != nil {
		require.NoError(t, err, "Second insert should succeed")
	}

	if err := tx2.Commit(); err != nil {
		require.NoError(t, err, "Second commit should succeed")
	}

	// Read content after second commit
	content2, err := os.ReadFile(outputFile) //nolint:gosec // Test file path is safe
	if err != nil {
		require.NoError(t, err, "should be able to read output file after second commit")
	}

	// Verify the file was overwritten and contains both records
	assert.Contains(t, string(content2), "First", "File should still contain first commit data after second commit")

	assert.Contains(t, string(content2), "Second", "File should contain second commit data")

	// Verify the file was actually overwritten (not just appended)
	// Count lines to make sure we have header + original + two new records
	lines := strings.Split(string(content2), "\n")
	nonEmptyLines := 0
	for _, line := range lines {
		if strings.TrimSpace(line) != "" {
			nonEmptyLines++
		}
	}

	// Should have: header + Initial + First + Second = 4 lines
	assert.Equal(t, 4, nonEmptyLines, "Expected 4 lines in overwritten file, got %d. Content: %s", nonEmptyLines, string(content2))

	// Third commit: Update existing record
	tx3, err := db.BeginTx(ctx, nil)
	if err != nil {
		require.NoError(t, err, "Begin third transaction should succeed")
	}

	_, err = tx3.ExecContext(ctx, "UPDATE test SET count = 999 WHERE name = 'Initial'")
	if err != nil {
		require.NoError(t, err, "Update should succeed")
	}

	if err := tx3.Commit(); err != nil {
		require.NoError(t, err, "Third commit should succeed")
	}

	// Read content after third commit
	content3, err := os.ReadFile(outputFile) //nolint:gosec // Test file path is safe
	if err != nil {
		require.NoError(t, err, "should be able to read output file after third commit")
	}

	// Verify the update was saved
	assert.Contains(t, string(content3), "999", "File should contain updated count (999)")

	// Verify original count (1) was overwritten
	assert.NotContains(t, string(content3), "Initial,1", "File should not contain old count (1) after update")
}

func TestBuilder_ErrorCases(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	t.Run("build with no inputs", func(t *testing.T) {
		t.Parallel()
		builder := NewBuilder()
		_, err := buildForTest(ctx, builder)
		if err == nil {
			assert.Error(t, err, "build() with no inputs should return error")
		}
	})

	t.Run("build with empty path", func(t *testing.T) {
		t.Parallel()
		builder := NewBuilder().AddPath("")
		_, err := buildForTest(ctx, builder)
		if err == nil {
			assert.Error(t, err, "build() with empty path should return error")
		}
	})

	t.Run("build with non-existent path", func(t *testing.T) {
		t.Parallel()
		builder := NewBuilder().AddPath(filepath.Join("non", "existent", "file.csv"))
		_, err := buildForTest(ctx, builder)
		if err == nil {
			assert.Error(t, err, "build() with non-existent path should return error")
		}
	})

	t.Run("auto-save with empty output directory", func(t *testing.T) {
		t.Parallel()
		tmpDir := t.TempDir()
		csvPath := filepath.Join(tmpDir, "test.csv")
		if err := os.WriteFile(csvPath, []byte("col1\nval1\n"), 0600); err != nil {
			require.NoError(t, err, "operation should succeed")
		}

		// Test with empty string for output directory - should use overwrite mode
		builder := NewBuilder().
			AddPath(csvPath).
			EnableAutoSave("") // Empty string should work for overwrite mode

		_, err := buildForTest(ctx, builder)
		if err != nil {
			t.Errorf("build() with empty output directory should not error, got: %v", err)
		}
	})

	t.Run("auto-save on commit with empty output directory", func(t *testing.T) {
		t.Parallel()
		tmpDir := t.TempDir()
		csvPath := filepath.Join(tmpDir, "test.csv")
		if err := os.WriteFile(csvPath, []byte("col1\nval1\n"), 0600); err != nil {
			require.NoError(t, err, "operation should succeed")
		}

		// Test with empty string for output directory - should use overwrite mode
		builder := NewBuilder().
			AddPath(csvPath).
			EnableAutoSaveOnCommit("") // Empty string should work for overwrite mode

		_, err := buildForTest(ctx, builder)
		if err != nil {
			t.Errorf("build() with empty output directory for auto-save on commit should not error, got: %v", err)
		}
	})

	t.Run("invalid reader data", func(t *testing.T) {
		t.Parallel()

		// Test with malformed CSV data that might cause parsing issues
		invalidCSV := "name,age\n\"unclosed quote,30\nvalid,25\n"
		reader := strings.NewReader(invalidCSV)

		builder := NewBuilder().AddReader(reader, "invalid", FileTypeCSV)
		_, err := buildForTest(ctx, builder)

		// Should handle malformed CSV gracefully or return meaningful error
		if err == nil {
			t.Log("Build succeeded with malformed CSV - parser handled it gracefully")
		}
	})

	t.Run("empty reader", func(t *testing.T) {
		t.Parallel()

		// The build does not read the stream, so the refusal comes from Open,
		// in the words of the format that read it.
		builder := NewBuilder().AddReader(strings.NewReader(""), "empty", FileTypeCSV)
		built, err := buildForTest(ctx, builder)
		require.NoError(t, err, "the build does not read the stream")

		_, err = built.Open(ctx)
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrEmptyData)
		assert.Contains(t, err.Error(), "empty CSV data")
	})

	t.Run("extremely small chunk size", func(t *testing.T) {
		t.Parallel()

		reader := strings.NewReader("name,age\nAlice,30\n")
		// Test with very small chunk size
		builder := NewBuilder().
			AddReader(reader, "test", FileTypeCSV).
			SetDefaultChunkSize(1) // Very small chunk size

		_, err := buildForTest(ctx, builder)
		if err != nil {
			assert.NoError(t, err, "Build should handle small chunk size")
		}
	})
}

func TestBuilder_AddPaths_ErrorCases(t *testing.T) {
	t.Parallel()

	t.Run("add multiple paths", func(t *testing.T) {
		t.Parallel()
		builder := NewBuilder().AddPaths("file1.csv", "file2.tsv", "file3.ltsv")
		if len(builder.paths) != 3 {
			assert.Len(t, builder.paths, 3, "AddPaths should add all paths")
		}
		expectedPaths := []string{"file1.csv", "file2.tsv", "file3.ltsv"}
		for i, expectedPath := range expectedPaths {
			if builder.paths[i] != expectedPath {
				t.Errorf("AddPaths should preserve path order, got %s at index %d, expected %s", builder.paths[i], i, expectedPath)
			}
		}
	})

	t.Run("add paths with empty string", func(t *testing.T) {
		t.Parallel()
		builder := NewBuilder().AddPaths("valid.csv", "", "another.csv")
		if len(builder.paths) != 3 {
			assert.Len(t, builder.paths, 3, "AddPaths should add all paths including empty ones")
		}
		if builder.paths[1] != "" {
			t.Errorf("AddPaths should preserve empty string, got %s", builder.paths[1])
		}
	})

	t.Run("add no paths", func(t *testing.T) {
		t.Parallel()
		builder := NewBuilder().AddPaths()
		if len(builder.paths) != 0 {
			assert.Len(t, builder.paths, 0, "AddPaths() with no arguments should not add any paths")
		}
	})
}

func TestDBBuilder_StreamDirectoryToSQLite(t *testing.T) {
	t.Parallel()

	t.Run("directory with supported files", func(t *testing.T) {
		t.Parallel()

		// Create temporary directory with test files
		tempDir := t.TempDir()
		csvContent := "id,name,age\n1,Alice,30\n2,Bob,25\n"
		tsvContent := "id\tname\tage\n3\tCharlie\t35\n4\tDiana\t28\n"

		csvFile := filepath.Join(tempDir, "test1.csv")
		tsvFile := filepath.Join(tempDir, "test2.tsv")

		err := os.WriteFile(csvFile, []byte(csvContent), 0600)
		if err != nil {
			t.Fatalf("Failed to create test CSV file: %v", err)
		}

		err = os.WriteFile(tsvFile, []byte(tsvContent), 0600)
		if err != nil {
			t.Fatalf("Failed to create test TSV file: %v", err)
		}

		// Test the streaming directory function
		db, err := Open(context.Background(), tempDir)
		if err != nil {
			t.Fatalf("Failed to open directory: %v", err)
		}
		defer db.Close()

		// Verify tables were created
		ctx := context.Background()
		rows, err := db.QueryContext(ctx, "SELECT name FROM sqlite_master WHERE type='table'")
		if err != nil {
			t.Fatalf("Failed to query tables: %v", err)
		}
		defer rows.Close()

		var tableNames []string
		for rows.Next() {
			var name string
			if err := rows.Scan(&name); err != nil {
				t.Fatalf("Failed to scan table name: %v", err)
			}
			tableNames = append(tableNames, name)
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("Error during rows iteration: %v", err)
		}

		expectedTables := []string{"test1", "test2"}
		if len(tableNames) != len(expectedTables) {
			t.Errorf("Expected %d tables, got %d: %v", len(expectedTables), len(tableNames), tableNames)
		}
	})

	t.Run("directory with unsupported files only", func(t *testing.T) {
		t.Parallel()

		// Create temporary directory with unsupported files
		tempDir := t.TempDir()
		txtFile := filepath.Join(tempDir, "test.txt")

		err := os.WriteFile(txtFile, []byte("some text content"), 0600)
		if err != nil {
			t.Fatalf("Failed to create test txt file: %v", err)
		}

		// Test should return error for no supported files
		_, err = Open(context.Background(), tempDir)
		if err == nil {
			t.Error("Expected error for directory with no supported files")
		}

		expectedError := "no supported files found in directory"
		if !strings.Contains(err.Error(), expectedError) {
			t.Errorf("Expected error to contain '%s', got: %v", expectedError, err)
		}
	})

	t.Run("empty directory", func(t *testing.T) {
		t.Parallel()

		tempDir := t.TempDir()

		// Test should return error for empty directory
		_, err := Open(context.Background(), tempDir)
		if err == nil {
			t.Error("Expected error for empty directory")
		}

		expectedError := "no supported files found in directory"
		if !strings.Contains(err.Error(), expectedError) {
			t.Errorf("Expected error to contain '%s', got: %v", expectedError, err)
		}
	})
}

func TestDriverMethods(t *testing.T) {
	t.Parallel()

	t.Run("directConnector Driver method", func(t *testing.T) {
		t.Parallel()

		connector := &directConnector{}
		driver := connector.Driver()
		if driver == nil {
			assert.NotNil(t, driver, "Expected non-nil driver")
		}
	})

	t.Run("autoSaveConnector Driver method", func(t *testing.T) {
		t.Parallel()

		// The connector hands back the driver instance the loader used, not a
		// fresh one: the dialect helper functions are registered per instance,
		// so a new driver would not see them.
		want := &sqlite.Driver{}
		connector := &autoSaveConnector{drv: want}
		assert.Same(t, want, connector.Driver())
	})
}

// TestTransactionMethods tests transaction operations for coverage
func TestTransactionMethods(t *testing.T) {
	t.Parallel()

	t.Run("Begin and Rollback transaction", func(t *testing.T) {
		t.Parallel()

		// Create a separate temp directory and CSV file for this test
		testTempDir := t.TempDir()
		testCsvFile := filepath.Join(testTempDir, "test.csv")
		csvContent := "id,name\n1,Alice\n2,Bob\n"
		if err := os.WriteFile(testCsvFile, []byte(csvContent), 0600); err != nil {
			require.NoError(t, err, "operation should succeed")
		}

		validatedBuilder, err := buildForTest(

			context.Background(), NewBuilder().
				AddPath(testCsvFile).
				EnableAutoSaveOnCommit(testTempDir))

		if err != nil {
			require.NoError(t, err, "operation should succeed")
		}

		db, err := validatedBuilder.Open(context.Background())
		if err != nil {
			require.NoError(t, err, "operation should succeed")
		}
		defer db.Close()

		ctx := context.Background()
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			require.NoError(t, err, "operation should succeed")
		}

		_, err = tx.ExecContext(ctx, "UPDATE test SET name = 'Charlie' WHERE id = 1")
		if err != nil {
			require.NoError(t, err, "operation should succeed")
		}

		err = tx.Rollback()
		if err != nil {
			require.NoError(t, err, "operation should succeed")
		}
	})

	t.Run("autoSaveConnection Begin method", func(t *testing.T) {
		t.Parallel()

		// Create a separate temp directory and CSV file for this test
		testTempDir := t.TempDir()
		testCsvFile := filepath.Join(testTempDir, "test.csv")
		csvContent := "id,name\n1,Alice\n2,Bob\n"
		if err := os.WriteFile(testCsvFile, []byte(csvContent), 0600); err != nil {
			require.NoError(t, err, "operation should succeed")
		}

		validatedBuilder, err := buildForTest(

			context.Background(), NewBuilder().
				AddPath(testCsvFile).
				EnableAutoSaveOnCommit(testTempDir))

		if err != nil {
			require.NoError(t, err, "operation should succeed")
		}

		db, err := validatedBuilder.Open(context.Background())
		if err != nil {
			require.NoError(t, err, "operation should succeed")
		}
		defer db.Close()

		// Test the Begin method (0% coverage)
		ctx := context.Background()
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			require.NoError(t, err, "operation should succeed")
		}
		defer tx.Rollback()
	})

	t.Run("overwriteOriginalFiles path", func(t *testing.T) {
		t.Parallel()

		// Create a separate temp directory and CSV file for this test
		testTempDir := t.TempDir()
		testCsvFile := filepath.Join(testTempDir, "test.csv")
		csvContent := "id,name\n1,Alice\n2,Bob\n"
		if err := os.WriteFile(testCsvFile, []byte(csvContent), 0600); err != nil {
			require.NoError(t, err, "operation should succeed")
		}

		validatedBuilder, err := buildForTest(

			context.Background(), NewBuilder().
				AddPath(testCsvFile).
				EnableAutoSaveOnCommit(""))

		if err != nil {
			require.NoError(t, err, "operation should succeed")
		}

		db, err := validatedBuilder.Open(context.Background())
		if err != nil {
			require.NoError(t, err, "operation should succeed")
		}
		defer db.Close()

		ctx := context.Background()
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			require.NoError(t, err, "operation should succeed")
		}

		_, err = tx.ExecContext(ctx, "UPDATE test SET name = 'Diana' WHERE id = 1")
		if err != nil {
			_ = tx.Rollback() //nolint:errcheck
			require.NoError(t, err, "operation should succeed")
		}

		// This should trigger overwriteOriginalFiles
		err = tx.Commit()
		if err != nil {
			require.NoError(t, err, "operation should succeed")
		}
	})
}

// TestAutoSavePaths tests auto-save functionality for coverage
func TestAutoSavePaths(t *testing.T) {
	t.Parallel()

	t.Run("Close connection with auto-save", func(t *testing.T) {
		t.Parallel()

		// Create a separate temp directory and CSV file for this test
		testTempDir := t.TempDir()
		testCsvFile := filepath.Join(testTempDir, "test.csv")
		csvContent := "id,name\n1,Alice\n2,Bob\n"
		if err := os.WriteFile(testCsvFile, []byte(csvContent), 0600); err != nil {
			require.NoError(t, err, "operation should succeed")
		}

		validatedBuilder, err := buildForTest(

			context.Background(), NewBuilder().
				AddPath(testCsvFile).
				EnableAutoSave(testTempDir))

		if err != nil {
			require.NoError(t, err, "operation should succeed")
		}

		db, err := validatedBuilder.Open(context.Background())
		if err != nil {
			require.NoError(t, err, "operation should succeed")
		}

		ctx := context.Background()
		_, err = db.ExecContext(ctx, "UPDATE test SET name = 'Eve' WHERE id = 1")
		if err != nil {
			require.NoError(t, err, "operation should succeed")
		}

		// Close should trigger auto-save
		err = db.Close()
		if err != nil {
			require.NoError(t, err, "operation should succeed")
		}
	})

	t.Run("createEmptyTable coverage", func(t *testing.T) {
		t.Parallel()

		// Test with header-only reader to trigger createEmptyTable path
		validatedBuilder, err := buildForTest(

			context.Background(), NewBuilder().
				AddReader(strings.NewReader("col1,col2\n"), "empty_test", FileTypeCSV))

		if err != nil {
			require.NoError(t, err, "operation should succeed")
		}

		db, err := validatedBuilder.Open(context.Background())
		if err != nil {
			require.NoError(t, err, "operation should succeed")
		}
		defer db.Close()

		// Verify the empty table was created correctly
		var count int
		err = db.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM empty_test").Scan(&count)
		if err != nil {
			require.NoError(t, err, "operation should succeed")
		}
		if count != 0 {
			t.Errorf("Expected empty table, got %d rows", count)
		}
	})

	t.Run("createEmptyTable successful parse", func(t *testing.T) {
		t.Parallel()

		// Test with minimal CSV that would parse successfully but have no data
		// This should trigger the createEmptyTable happy path
		validCSV := "id,name,email\n1,test,test@example.com\n"
		reader := strings.NewReader(validCSV)

		// Create a custom reader input that forces empty table creation
		// We'll simulate this by creating a very small chunk size that reads only headers
		validatedBuilder, err := buildForTest(

			context.Background(), NewBuilder().
				AddReader(reader, "parsed_empty", FileTypeCSV).
				SetDefaultChunkSize(1))

		if err != nil {
			require.NoError(t, err, "operation should succeed")
		}

		db, err := validatedBuilder.Open(context.Background())
		if err != nil {
			require.NoError(t, err, "operation should succeed")
		}
		defer db.Close()

		// Check table was created
		rows, err := db.QueryContext(context.Background(), "SELECT name FROM sqlite_master WHERE type='table' AND name='parsed_empty'")
		if err != nil {
			require.NoError(t, err, "operation should succeed")
		}
		defer rows.Close()

		hasTable := false
		for rows.Next() {
			hasTable = true
		}
		if err := rows.Err(); err != nil {
			require.NoError(t, err, "operation should succeed")
		}

		if !hasTable {
			t.Error("Expected table to be created")
		}
	})

	t.Run("createEmptyTable with duplicate columns", func(t *testing.T) {
		t.Parallel()

		// Test with duplicate column names
		duplicateCSV := "id,name,id\n"
		validatedBuilder, err := buildForTest(

			context.Background(), NewBuilder().
				AddReader(strings.NewReader(duplicateCSV), "duplicate_cols", FileTypeCSV))

		if err != nil {
			require.NoError(t, err, "operation should succeed")
		}

		_, err = validatedBuilder.Open(context.Background())
		if err == nil {
			t.Error("Expected error for duplicate column names")
		}
		if !strings.Contains(err.Error(), "duplicate column") {
			t.Errorf("Expected 'duplicate column' error, got: %v", err)
		}
	})

	t.Run("createEmptyTable fallback to createTableFromHeaders", func(t *testing.T) {
		t.Parallel()

		// Test the fallback path when parseFromReader fails
		// Use a reader that would cause parsing to fail but still have readable content
		brokenCSV := "id,name,email\n" // Header only, no data, should trigger fallback
		validatedBuilder, err := buildForTest(

			context.Background(), NewBuilder().
				AddReader(strings.NewReader(brokenCSV), "fallback_test", FileTypeCSV))

		if err != nil {
			require.NoError(t, err, "operation should succeed")
		}

		// This should not fail but use the createTableFromHeaders fallback
		db, err := validatedBuilder.Open(context.Background())
		if err != nil {
			require.NoError(t, err, "operation should succeed")
		}
		defer db.Close()

		// Check table exists
		rows, err := db.QueryContext(context.Background(), "SELECT name FROM sqlite_master WHERE type='table' AND name='fallback_test'")
		if err != nil {
			require.NoError(t, err, "operation should succeed")
		}
		defer rows.Close()

		hasTable := false
		for rows.Next() {
			hasTable = true
		}
		if err := rows.Err(); err != nil {
			require.NoError(t, err, "operation should succeed")
		}

		if !hasTable {
			t.Error("Expected table to be created via fallback")
		}
	})
}

// csvFixture writes a small CSV for a test and returns its path.
func csvFixture(t *testing.T) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), "users.csv")
	require.NoError(t, os.WriteFile(path, []byte("id,name\n1,Alice\n"), 0o600))
	return path
}

// canceledContext returns a context that is already done.
func canceledContext(t *testing.T) context.Context {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	return ctx
}

// expiredContext returns a context whose deadline has passed, which is the
// other way a caller's context ends and the one that answers a different
// sentinel.
func expiredContext(t *testing.T) context.Context {
	t.Helper()

	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Millisecond))
	t.Cleanup(cancel)
	return ctx
}

// endedContext is one way a caller's context can be done, with the error it has
// to produce. A load that answered the wrong one would tell a caller their
// deadline passed when they had canceled, or the reverse.
type endedContext struct {
	name string
	// make builds the context when the case runs, so a parallel subtest gets
	// one of its own rather than sharing a value built here.
	make func(*testing.T) context.Context
	want error
}

// endedContexts is every way a caller's context can be done.
func endedContexts() []endedContext {
	return []endedContext{
		{"canceled", canceledContext, context.Canceled},
		{"past a deadline", expiredContext, context.DeadlineExceeded},
	}
}

// builtBuilder returns a builder that has collected path, which is the state a
// load starts from.
func builtBuilder(t *testing.T, path string) *DBBuilder {
	t.Helper()

	builder, err := buildForTest(context.Background(), NewBuilder().AddPath(path))
	require.NoError(t, err)
	return builder
}

// TestBuilderEntryPoints_EndedContext checks that each way of loading stops on a
// context that is already done, before it opens files or writes tables, and
// answers the sentinel that says which way it ended. A load that ignored
// cancellation would leave half the tables of an abandoned request behind, and
// one that answered the wrong sentinel would tell a caller their deadline
// passed when they had canceled.
//
// The godoc names six entry points that take a context. The four that belong to
// a builder are here; Open is covered in filesql_test.go and
// DumpDatabase in dump_test.go, each beside the code it calls.
func TestBuilderEntryPoints_EndedContext(t *testing.T) {
	t.Parallel()

	path := csvFixture(t)

	for _, tc := range endedContexts() {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			t.Run("Open", func(t *testing.T) {
				t.Parallel()

				db, err := builtBuilder(t, path).Open(tc.make(t))
				if db != nil {
					_ = db.Close()
				}
				assert.ErrorIs(t, err, tc.want)
			})

			t.Run("OpenReadOnly", func(t *testing.T) {
				t.Parallel()

				db, err := builtBuilder(t, path).OpenReadOnly(tc.make(t))
				if db != nil {
					_ = db.Close()
				}
				assert.ErrorIs(t, err, tc.want)
			})

			t.Run("LoadInto", func(t *testing.T) {
				t.Parallel()

				err := builtBuilder(t, path).LoadInto(tc.make(t), openTestDB(t))
				assert.ErrorIs(t, err, tc.want)
			})

			t.Run("LoadIntoTx", func(t *testing.T) {
				t.Parallel()

				db := openTestDB(t)
				tx, err := db.BeginTx(context.Background(), nil)
				require.NoError(t, err)
				defer func() { _ = tx.Rollback() }()

				assert.ErrorIs(t, builtBuilder(t, path).LoadIntoTx(tc.make(t), tx), tc.want)
			})
		})
	}
}

// TestLoadIntoTx_Refusals covers what LoadIntoTx cannot do. The caller owns the
// transaction, so there is nothing for auto-save to attach its close to, and a
// nil transaction has to be named rather than panicking.
func TestLoadIntoTx_Refusals(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	path := csvFixture(t)

	t.Run("a nil transaction", func(t *testing.T) {
		t.Parallel()

		err := builtBuilder(t, path).LoadIntoTx(ctx, nil)
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrDatabaseOperation)
	})

	t.Run("auto-save", func(t *testing.T) {
		t.Parallel()

		db := openTestDB(t)
		tx, err := db.BeginTx(ctx, nil)
		require.NoError(t, err)
		defer func() { _ = tx.Rollback() }()

		err = builtBuilder(t, path).EnableAutoSave(t.TempDir()).LoadIntoTx(ctx, tx)
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrDatabaseOperation)
	})

	t.Run("no input at all", func(t *testing.T) {
		t.Parallel()

		db := openTestDB(t)
		tx, err := db.BeginTx(ctx, nil)
		require.NoError(t, err)
		defer func() { _ = tx.Rollback() }()

		assert.Error(t, NewBuilder().LoadIntoTx(ctx, tx), "a builder with no input has nothing to load")
	})
}

// TestOpenReadOnly_PassesTheOpenFailureThrough checks that a read-only open
// does not swallow the failure of the load it performs.
func TestOpenReadOnly_PassesTheOpenFailureThrough(t *testing.T) {
	t.Parallel()

	rodb, err := NewBuilder().OpenReadOnly(context.Background())
	require.Error(t, err, "a builder with no input has nothing to open")
	assert.Nil(t, rodb)
}

// TestValidateDatabaseConnection covers the health check a load runs before it
// hands the database back.
func TestValidateDatabaseConnection(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	builder := NewBuilder()

	t.Run("a working database passes", func(t *testing.T) {
		t.Parallel()
		assert.NoError(t, builder.validateDatabaseConnection(ctx, openTestDB(t)))
	})

	t.Run("a closed database is reported", func(t *testing.T) {
		t.Parallel()

		db := openTestDB(t)
		require.NoError(t, db.Close())

		assert.Error(t, builder.validateDatabaseConnection(ctx, db))
	})
}

// tableNamesOf reports the tables a database holds, so a test can say which
// sources reached it.
func tableNamesOf(t *testing.T, db *sql.DB) []string {
	t.Helper()

	rows, err := db.QueryContext(context.Background(), "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
	require.NoError(t, err)
	defer rows.Close()

	names := make([]string, 0, 2)
	for rows.Next() {
		var name string
		require.NoError(t, rows.Scan(&name))
		names = append(names, name)
	}
	require.NoError(t, rows.Err())
	return names
}

// TestBuilderHonorsWhatItIsGivenAfterABuild covers the order two calls are made
// in. A builder caches what its build derived, and everything it was given
// afterwards used to fall into one of two halves: a path or a filesystem was
// dropped without a word while a reader was loaded, and the checks that live in
// the build did not run at all, so the pair the builder refuses -- a dialect
// together with auto-save -- was accepted when the dialect came second.
func TestBuilderHonorsWhatItIsGivenAfterABuild(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	newCSV := func(t *testing.T, dir, name string) string {
		t.Helper()
		path := filepath.Join(dir, name)
		require.NoError(t, os.WriteFile(path, []byte("id\n1\n"), 0o600))
		return path
	}

	// openedOnce hands back a builder whose build has run and succeeded, which
	// is the state every case below starts from.
	openedOnce := func(t *testing.T, b *DBBuilder) *DBBuilder {
		t.Helper()
		db, err := b.Open(ctx)
		require.NoError(t, err)
		require.NoError(t, db.Close())
		return b
	}

	t.Run("a path added afterwards is loaded", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()
		first := newCSV(t, dir, "aaa.csv")
		second := newCSV(t, dir, "bbb.csv")

		builder := openedOnce(t, NewBuilder().AddPath(first))
		builder.AddPath(second)

		db, err := builder.Open(ctx)
		require.NoError(t, err)
		defer db.Close()

		assert.Equal(t, []string{"aaa", "bbb"}, tableNamesOf(t, db))
	})

	t.Run("paths added afterwards are loaded", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()
		first := newCSV(t, dir, "aaa.csv")
		second := newCSV(t, dir, "bbb.csv")

		builder := openedOnce(t, NewBuilder().AddPath(first))
		builder.AddPaths(second)

		db, err := builder.Open(ctx)
		require.NoError(t, err)
		defer db.Close()

		assert.Equal(t, []string{"aaa", "bbb"}, tableNamesOf(t, db))
	})

	t.Run("a filesystem added afterwards is loaded", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()
		builder := openedOnce(t, NewBuilder().AddPath(newCSV(t, dir, "aaa.csv")))
		builder.AddFS(fstest.MapFS{"fromfs.csv": &fstest.MapFile{Data: []byte("id\n2\n")}})

		db, err := builder.Open(ctx)
		require.NoError(t, err)
		defer db.Close()

		assert.Equal(t, []string{"aaa", "fromfs"}, tableNamesOf(t, db))
	})

	t.Run("a reader added afterwards is loaded", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()
		builder := openedOnce(t, NewBuilder().AddPath(newCSV(t, dir, "aaa.csv")))
		builder.AddReader(strings.NewReader("id\n3\n"), "later", FileTypeCSV)

		db, err := builder.Open(ctx)
		require.NoError(t, err)
		defer db.Close()

		assert.Equal(t, []string{"aaa", "later"}, tableNamesOf(t, db))
	})

	t.Run("a path that does not exist is refused", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()
		builder := openedOnce(t, NewBuilder().AddPath(newCSV(t, dir, "aaa.csv")))
		builder.AddPath(filepath.Join(dir, "no-such-file.csv"))

		db, err := builder.Open(ctx)
		if db != nil {
			defer db.Close()
		}
		assert.Error(t, err, "a path that does not exist is refused whenever it was added")
	})

	t.Run("a dialect added afterwards cannot be combined with auto-save", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()
		out := filepath.Join(dir, "out")
		require.NoError(t, os.MkdirAll(out, 0o750))

		builder := openedOnce(t, NewBuilder().AddPath(newCSV(t, dir, "aaa.csv")).EnableAutoSave(out))
		builder.WithDialect(dialect.MySQL)

		db, err := builder.Open(ctx)
		if db != nil {
			defer db.Close()
		}
		assert.Error(t, err, "the pair the builder refuses is refused in either order")
	})

	t.Run("an auto-save directory added afterwards is checked", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()
		notADir := filepath.Join(dir, "not-a-dir")
		require.NoError(t, os.WriteFile(notADir, []byte("x"), 0o600))

		builder := openedOnce(t, NewBuilder().AddPath(newCSV(t, dir, "aaa.csv")))
		builder.EnableAutoSave(notADir)

		db, err := builder.Open(ctx)
		if db != nil {
			defer db.Close()
		}
		assert.Error(t, err, "a destination that cannot be written is reported by Open, not by Close")
	})

	t.Run("every option clears the cached build", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()
		path := newCSV(t, dir, "aaa.csv")

		// Naming each mutator here is what keeps the next one added to the
		// builder from quietly joining the half that is dropped.
		mutators := map[string]func(*DBBuilder){
			"AddPath":                func(b *DBBuilder) { b.AddPath(path) },
			"AddPaths":               func(b *DBBuilder) { b.AddPaths(path) },
			"AddReader":              func(b *DBBuilder) { b.AddReader(strings.NewReader("id\n1\n"), "r", FileTypeCSV) },
			"AddFS":                  func(b *DBBuilder) { b.AddFS(fstest.MapFS{}) },
			"SetDefaultChunkSize":    func(b *DBBuilder) { b.SetDefaultChunkSize(7) },
			"WithMalformedRowPolicy": func(b *DBBuilder) { b.WithMalformedRowPolicy(MalformedRowSkip) },
			"WithExcelSheetPolicy":   func(b *DBBuilder) { b.WithExcelSheetPolicy(ExcelSheetPolicyVisibleOnly) },
			"WithLogger":             func(b *DBBuilder) { b.WithLogger(slog.New(slog.DiscardHandler)) },
			"EnableAutoSave":         func(b *DBBuilder) { b.EnableAutoSave(dir) },
			"EnableAutoSaveOnCommit": func(b *DBBuilder) { b.EnableAutoSaveOnCommit(dir) },
			"WithDialect":            func(b *DBBuilder) { b.WithDialect(dialect.MySQL) },
		}

		for name, mutate := range mutators {
			t.Run(name, func(t *testing.T) {
				t.Parallel()

				builder := NewBuilder().AddPath(path)
				require.NoError(t, builder.build(ctx))
				require.True(t, builder.built)

				mutate(builder)
				assert.False(t, builder.built, "%s left the build cached, so what it said is dropped", name)
			})
		}
	})
}
