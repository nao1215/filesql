package reader

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math"
	"math/big"
	"strconv"
	"strings"

	"github.com/nao1215/filesql/internal/infer"
	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/format"
)

// The reader is github.com/parquet-go/parquet-go rather than the Arrow one it
// replaced, because a Parquet file this package did not write is untrusted
// input and the Arrow page decoder was unbounded on some of it: a 433-byte
// file held a load forever while it allocated hundreds of megabytes a second,
// it did not check the context it was given, and Go has no way to stop a
// goroutine that is allocating. parquet-go refuses the same files in
// microseconds, with time and memory bounded by the file's own size.

// parquetMagic is the four bytes a Parquet file begins and ends with. The
// format defines both, and checking the leading one before handing the bytes
// to a library is what keeps arbitrary non-Parquet input out of a decoder:
// fuzzing the previous reader with the check in place ran 1.4 million inputs
// without a hang or a panic; without it, a worker died within thirty seconds.
var parquetMagic = []byte("PAR1") //nolint:gochecknoglobals // constant-like

// errNotParquet reports bytes that do not begin the way the format says.
func errNotParquet(head []byte) error {
	return parseError(nil, "not a parquet file: it begins %q rather than %q", head, parquetMagic)
}

// parquetFooterFits refuses a file whose declared footer is larger than the
// file holding it.
//
// The last eight bytes of a Parquet file are the footer's length and the magic,
// and the library allocates that length before checking the file is that big:
// eight bytes reading "PAR1PAR1" declare a footer of 826364240 bytes, and
// opening them allocated 789 MiB before failing with "negative offset". A file
// costs no more than its own size, which is the rule this reader was chosen
// for, so the declared number is checked against the bytes that are there
// before the library is handed anything.
func parquetFooterFits(at io.ReaderAt, size int64) error {
	// Four bytes of footer length and four of magic sit at the end, after the
	// footer itself, and the leading magic sits in front of it.
	const trailer = 8
	if size < int64(len(parquetMagic))+trailer {
		return parseError(nil, "parquet file is %d bytes, too short to hold a footer", size)
	}
	tail := make([]byte, trailer)
	if _, err := at.ReadAt(tail, size-trailer); err != nil {
		return parseError(err, "failed to read parquet footer")
	}
	// The declared length is compared in int64, which holds every uint32 and
	// every size a file can have, so neither side can wrap.
	declared := int64(binary.LittleEndian.Uint32(tail))
	if declared+int64(len(parquetMagic))+trailer > size {
		return parseError(nil, "parquet footer declares %d bytes in a file of %d", declared, size)
	}
	return nil
}

// parquetPagesFit refuses a file whose column chunks or page headers reach
// past the bytes the file holds.
//
// A page header is a thrift compact structure inside a column chunk, and the
// library decodes it by allocating what the structure declares before checking
// those bytes are there: a 473-byte file whose page header declared a 98 MiB
// statistic allocated 98 MiB and then failed with "unexpected EOF", and paid
// it again on every open of the same file. A file costs no more than its own
// size, which is the rule this reader was chosen for, so the declared lengths
// are read here first, against the bytes the chunk actually holds.
//
// Nothing is allocated for what a field declares. A field is walked past by
// arithmetic where the format allows it and read only where its own length has
// to be read, so the walk costs the header bytes and nothing else.
//
// This comes out if parquet-go grows an option bounding what a decoded page
// header may declare, the way the check above the footer would.
func parquetPagesFit(at io.ReaderAt, size int64, meta *format.FileMetaData) error {
	if meta == nil {
		return nil
	}
	for g := range meta.RowGroups {
		group := &meta.RowGroups[g]
		for c := range group.Columns {
			column := &group.Columns[c].MetaData
			// A chunk stored in another file is not this file's to check, and
			// this reader does not follow one.
			if group.Columns[c].FilePath != "" {
				continue
			}
			start := column.DataPageOffset
			if column.DictionaryPageOffset > 0 && column.DictionaryPageOffset < start {
				start = column.DictionaryPageOffset
			}
			length := column.TotalCompressedSize
			if start < 0 || length < 0 || start > size || length > size-start {
				return parseError(nil, "parquet column chunk %d of row group %d claims %d bytes at offset %d in a file of %d",
					c, g, length, start, size)
			}
			if err := parquetChunkPagesFit(at, start, start+length, g, c); err != nil {
				return err
			}
		}
	}
	return nil
}

// parquetChunkPagesFit walks the page headers of one column chunk, refusing
// the first one that declares more than the chunk holds.
func parquetChunkPagesFit(at io.ReaderAt, start, end int64, group, column int) error {
	// A chunk records no page count, so the walk ends where the chunk does. A
	// header that declares a page reaching past that end is the failure this
	// looks for; a chunk of no bytes has no page to walk.
	cursor := &thriftCursor{at: at}
	for off := start; off < end; {
		cursor.reset(off, end)
		compressed, err := cursor.pageHeader()
		if err != nil {
			return parseError(nil, "parquet page header at offset %d of column chunk %d in row group %d is damaged: %v",
				off, column, group, err)
		}
		next := off + cursor.read() + compressed
		if next <= off || next > end {
			return parseError(nil, "parquet page at offset %d of column chunk %d in row group %d claims %d bytes, past the chunk's end at %d",
				off, column, group, compressed, end)
		}
		off = next
	}
	return nil
}

// thriftCursor reads one thrift compact structure out of a bounded window of
// the file. It exists to answer how much a structure declares without
// allocating what it declares, which is the whole reason this walk is here.
type thriftCursor struct {
	at     io.ReaderAt
	reader *bufio.Reader
	base   int64
	limit  int64
	n      int64
}

// pageHeaderReadAhead is what the cursor buffers. A page header is a few dozen
// bytes plus whatever statistics the writer put in it, and the bytes after it
// are the page's own, so reading far past the header would be reading the data
// this walk exists to avoid touching.
const pageHeaderReadAhead = 512

// thrift compact field types, as the protocol writes them in the low nibble of
// a field header byte.
const (
	thriftStop         byte = 0x0
	thriftBooleanTrue  byte = 0x1
	thriftBooleanFalse byte = 0x2
	thriftByte         byte = 0x3
	thriftI16          byte = 0x4
	thriftI32          byte = 0x5
	thriftI64          byte = 0x6
	thriftDouble       byte = 0x7
	thriftBinary       byte = 0x8
	thriftList         byte = 0x9
	thriftSet          byte = 0xA
	thriftMap          byte = 0xB
	thriftStruct       byte = 0xC
)

// thriftMaxDepth bounds how deeply a structure may nest. A page header nests
// three deep; anything past this is damaged input, and a walk that followed it
// would grow the stack on a file that declares it.
const thriftMaxDepth = 32

// reset points the cursor at the window beginning at off and ending at limit.
func (c *thriftCursor) reset(off, limit int64) {
	c.base = off
	c.limit = limit
	c.n = 0
	section := io.NewSectionReader(c.at, off, limit-off)
	if c.reader == nil {
		c.reader = bufio.NewReaderSize(section, pageHeaderReadAhead)
		return
	}
	c.reader.Reset(section)
}

// read reports how many bytes of the window the cursor has consumed.
func (c *thriftCursor) read() int64 { return c.n }

// remaining reports how many bytes of the window are left.
func (c *thriftCursor) remaining() int64 { return c.limit - c.base - c.n }

// pageHeader walks one PageHeader structure and answers its
// compressed_page_size, which is field 3 and the only one the walk needs: it
// says where the next header begins.
func (c *thriftCursor) pageHeader() (int64, error) {
	compressed := int64(-1)
	var id int16
	for {
		typ, next, err := c.field(id)
		if err != nil {
			return 0, err
		}
		if typ == thriftStop {
			break
		}
		id = next
		if id == 3 && typ == thriftI32 {
			value, err := c.zigzag()
			if err != nil {
				return 0, err
			}
			compressed = value
			continue
		}
		if err := c.skip(typ, 0); err != nil {
			return 0, err
		}
	}
	if compressed < 0 {
		return 0, errors.New("it states no compressed page size")
	}
	return compressed, nil
}

// field reads one field header, answering the field's type and id. The type is
// thriftStop at the end of a structure, where no id follows.
func (c *thriftCursor) field(prev int16) (byte, int16, error) {
	head, err := c.byteAt()
	if err != nil {
		return 0, 0, err
	}
	typ := head & 0x0F
	if typ == thriftStop {
		return thriftStop, 0, nil
	}
	delta := int16(head >> 4)
	if delta != 0 {
		return typ, prev + delta, nil
	}
	// A field whose id does not follow the previous one writes the id itself,
	// as a zigzag varint.
	id, err := c.zigzag()
	if err != nil {
		return 0, 0, err
	}
	if id < math.MinInt16 || id > math.MaxInt16 {
		return 0, 0, errors.New("it names a field id no thrift structure has")
	}
	return typ, int16(id), nil
}

// skip walks past one value of the given type without allocating what the
// value declares.
func (c *thriftCursor) skip(typ byte, depth int) error {
	if depth > thriftMaxDepth {
		return errors.New("it nests deeper than any page header does")
	}
	switch typ {
	case thriftBooleanTrue, thriftBooleanFalse:
		// The value is the type itself, so there is nothing to walk past.
		return nil
	case thriftByte:
		return c.discard(1)
	case thriftI16, thriftI32, thriftI64:
		_, err := c.zigzag()
		return err
	case thriftDouble:
		return c.discard(8)
	case thriftBinary:
		n, err := c.length()
		if err != nil {
			return err
		}
		return c.discard(n)
	case thriftList, thriftSet:
		return c.skipList(depth)
	case thriftMap:
		return c.skipMap(depth)
	case thriftStruct:
		return c.skipStruct(depth)
	default:
		return fmt.Errorf("it names a field type thrift has no value for: %#x", typ)
	}
}

// skipStruct walks past one nested structure.
func (c *thriftCursor) skipStruct(depth int) error {
	var id int16
	for {
		typ, next, err := c.field(id)
		if err != nil {
			return err
		}
		if typ == thriftStop {
			return nil
		}
		id = next
		if err := c.skip(typ, depth+1); err != nil {
			return err
		}
	}
}

// skipList walks past one list or set.
func (c *thriftCursor) skipList(depth int) error {
	head, err := c.byteAt()
	if err != nil {
		return err
	}
	typ := head & 0x0F
	size := int64(head >> 4)
	if size == 0x0F {
		// A list of fifteen elements or more writes its length separately.
		if size, err = c.length(); err != nil {
			return err
		}
	}
	// Every element takes at least a byte, so a list longer than the window
	// has declared more than the file holds, and saying so here is what keeps
	// the walk from running the length of a number the file made up.
	if size > c.remaining() {
		return fmt.Errorf("it declares a list of %d elements in %d bytes", size, c.remaining())
	}
	for range size {
		// A boolean in a list is written as a byte of its own, unlike a
		// boolean field, whose value is its type.
		if typ == thriftBooleanTrue || typ == thriftBooleanFalse {
			if err := c.discard(1); err != nil {
				return err
			}
			continue
		}
		if err := c.skip(typ, depth+1); err != nil {
			return err
		}
	}
	return nil
}

// skipMap walks past one map.
func (c *thriftCursor) skipMap(depth int) error {
	size, err := c.length()
	if err != nil {
		return err
	}
	if size == 0 {
		return nil
	}
	types, err := c.byteAt()
	if err != nil {
		return err
	}
	if size > c.remaining() {
		return fmt.Errorf("it declares a map of %d entries in %d bytes", size, c.remaining())
	}
	keyType, valueType := types>>4, types&0x0F
	for range size {
		if err := c.skip(keyType, depth+1); err != nil {
			return err
		}
		if err := c.skip(valueType, depth+1); err != nil {
			return err
		}
	}
	return nil
}

// byteAt reads one byte of the window.
func (c *thriftCursor) byteAt() (byte, error) {
	b, err := c.reader.ReadByte()
	if err != nil {
		return 0, err
	}
	c.n++
	return b, nil
}

// discard walks past n bytes of the window, refusing a count the window cannot
// hold before any of it is read.
func (c *thriftCursor) discard(n int64) error {
	if n < 0 || n > c.remaining() {
		return fmt.Errorf("it declares %d bytes in %d", n, c.remaining())
	}
	skipped, err := c.reader.Discard(int(n))
	c.n += int64(skipped)
	return err
}

// length reads a length or a collection size, which the protocol writes as an
// unsigned varint.
func (c *thriftCursor) length() (int64, error) {
	value, err := c.uvarint()
	if err != nil {
		return 0, err
	}
	if value > math.MaxInt32 {
		return 0, fmt.Errorf("it declares a length of %d, past what thrift writes", value)
	}
	return int64(value), nil
}

// uvarint reads one unsigned varint.
func (c *thriftCursor) uvarint() (uint64, error) {
	var value uint64
	for shift := uint(0); ; shift += 7 {
		if shift >= 64 {
			return 0, errors.New("it writes a number longer than a varint holds")
		}
		b, err := c.byteAt()
		if err != nil {
			return 0, err
		}
		value |= uint64(b&0x7F) << shift
		if b < 0x80 {
			return value, nil
		}
	}
}

// zigzag reads one signed varint, which the protocol writes zigzag-encoded.
func (c *thriftCursor) zigzag() (int64, error) {
	value, err := c.uvarint()
	if err != nil {
		return 0, err
	}
	return int64(value>>1) ^ -int64(value&1), nil
}

// parquetBytes gives the reader something it can read at both ends and then by
// column chunk, which is what the format asks for.
//
// A file named by path arrives here as the *os.File itself, and a file already
// serves reads at an offset, so nothing is copied: the load holds the rows it
// has read and the pages the operating system was holding anyway, rather than
// a second copy of the compressed bytes. Anything else -- a reader passed to
// AddReader, a decompressed stream -- has to be buffered, because the format is
// read back to front and a stream cannot go back.
func parquetBytes(src io.Reader) (io.ReaderAt, int64, error) {
	if at, size, ok := wholeFileAt(src); ok {
		if size == 0 {
			return nil, 0, emptyError("empty parquet file")
		}
		return at, size, nil
	}
	data, err := io.ReadAll(src)
	if err != nil {
		return nil, 0, parseError(err, "failed to read parquet data")
	}
	if len(data) == 0 {
		return nil, 0, emptyError("empty parquet file")
	}
	return bytes.NewReader(data), int64(len(data)), nil
}

// wholeFileAt reports whether src is a whole file this read may address
// directly, and how large it is. A source that has already been read from is
// refused, because what the format needs is the file from its start and this
// cannot know what the bytes already taken were.
func wholeFileAt(src io.Reader) (io.ReaderAt, int64, bool) {
	at, ok := src.(io.ReaderAt)
	if !ok {
		return nil, 0, false
	}
	seeker, ok := src.(io.Seeker)
	if !ok {
		return nil, 0, false
	}
	here, err := seeker.Seek(0, io.SeekCurrent)
	if err != nil || here != 0 {
		return nil, 0, false
	}
	size, err := seeker.Seek(0, io.SeekEnd)
	if err != nil {
		return nil, 0, false
	}
	if _, err := seeker.Seek(0, io.SeekStart); err != nil {
		return nil, 0, false
	}
	return at, size, true
}

// parquetBeginsWithMagic refuses bytes that do not begin the way the format
// says, before the library is handed anything.
func parquetBeginsWithMagic(at io.ReaderAt, size int64) error {
	head := make([]byte, min(size, int64(len(parquetMagic))))
	if _, err := at.ReadAt(head, 0); err != nil {
		return parseError(err, "failed to read parquet header")
	}
	if !bytes.Equal(head, parquetMagic) {
		return errNotParquet(head)
	}
	return nil
}

// readParquet reads a Parquet file in chunks. The whole file is buffered first
// because the format is read back to front: its metadata is at the end.
func readParquet(src io.Reader, opts Options, emit Emit) (Result, error) {
	at, size, err := parquetBytes(src)
	if err != nil {
		return Result{}, err
	}
	if err := parquetBeginsWithMagic(at, size); err != nil {
		return Result{}, err
	}
	if err := parquetFooterFits(at, size); err != nil {
		return Result{}, err
	}

	file, err := openParquet(at, size)
	if err != nil {
		return Result{}, err
	}
	if err := parquetPagesFit(at, size, file.Metadata()); err != nil {
		return Result{}, err
	}

	header, columns, leafField, flat, err := parquetSchemaLayout(file)
	if err != nil {
		return Result{}, err
	}
	// A Parquet file carries its own column names, which SQLite folds by case and
	// filesql trims, so two of them can be one column downstream. The header is
	// validated here, the way the delimited and XLSX readers validate theirs, so
	// the clash is the classified duplicate-column error rather than a raw
	// CREATE TABLE failure later.
	if err := ValidateColumnNames(header); err != nil {
		return Result{}, err
	}
	// Parquet declares the type of every column, so the schema is read rather
	// than inferred from the rendered values: inference cannot tell a STRING
	// column of digits from an INT64 one, and would turn a zip code into a
	// number.
	types := make([]infer.Type, len(columns))
	for i, col := range columns {
		types[i] = col.columnType(opts.Rendering)
	}
	result := Result{Header: header, Types: types}

	// A file with a schema and no rows still names its columns.
	if file.NumRows() == 0 {
		return result, emit(&Chunk{Header: header, Types: types})
	}

	chunkSize := chunkSizeOf(opts)
	rows := newTypedChunker(header, types, opts, emit).reserve(min(chunkSize, 1024))

	buf := make([]parquet.Row, min(chunkSize, 1024))
	for _, rowGroup := range file.RowGroups() {
		group, err := openParquetRows(rowGroup)
		if err != nil {
			return Result{}, err
		}
		readErr := func() error {
			defer group.Close()
			for {
				n, err := readParquetRows(group, buf)
				for _, parquetRow := range buf[:n] {
					row, nullRow := renderParquetRow(parquetRow, columns, leafField, flat, opts.Rendering)
					if err := rows.addWithNulls(row, nullRow); err != nil {
						return err
					}
				}
				if errors.Is(err, io.EOF) {
					return nil
				}
				if err != nil {
					return parseError(err, "failed to read table")
				}
				// No rows, no error, and no EOF is a read that made no
				// progress; looping on it would never return. A healthy read
				// into a non-empty buffer always does one of the three.
				if n == 0 {
					return parseError(nil, "parquet data is damaged: a read returned no rows and no error")
				}
			}
		}()
		if readErr != nil {
			return Result{}, readErr
		}
	}
	if err := rows.finish(); err != nil {
		return Result{}, err
	}
	result.Rows = rows.rows
	result.Total = rows.rows
	return result, nil
}

// openParquet opens the file's metadata, with any panic the library raises on
// damaged input turned into an error: a caller loading a file chosen by
// someone else cannot defend against a panic, and every other malformed input
// here is an error.
func openParquet(at io.ReaderAt, size int64) (file *parquet.File, err error) {
	defer func() {
		if r := recover(); r != nil {
			file = nil
			err = parseError(nil, "parquet data is damaged: %v", r)
		}
	}()
	// The page index and bloom filters are for pruning reads; this read scans
	// every row, so neither is worth decoding.
	file, err = parquet.OpenFile(at, size,
		parquet.SkipPageIndex(true), parquet.SkipBloomFilters(true))
	if err != nil {
		return nil, parseError(err, "failed to create parquet reader")
	}
	return file, nil
}

// openParquetRows is RowGroup.Rows with the panic a damaged schema can raise
// turned into an error: building the row reader asks every leaf type its kind,
// which panics on the same inconsistent metadata parquetSchemaLayout guards
// against, this time inside the library's own walk.
func openParquetRows(rowGroup parquet.RowGroup) (rows parquet.Rows, err error) {
	defer func() {
		if r := recover(); r != nil {
			rows = nil
			err = parseError(nil, "parquet data is damaged: %v", r)
		}
	}()
	return rowGroup.Rows(), nil
}

// readParquetRows is Rows.ReadRows with the panic a damaged page can raise
// turned into an error.
func readParquetRows(rows parquet.Rows, buf []parquet.Row) (n int, err error) {
	defer func() {
		if r := recover(); r != nil {
			n = 0
			err = fmt.Errorf("parquet data is damaged: %v", r)
		}
	}()
	return rows.ReadRows(buf)
}

// parquetColumn is one top-level field of the schema and how its values are
// rendered.
type parquetColumn struct {
	leaf     bool // a single non-repeated primitive, the common case
	kind     parquet.Kind
	unsigned bool  // INT(bits, false): the physical int carries an unsigned value
	float16  bool  // FLOAT16: two bytes carrying a half-precision float
	decimal  bool  // DECIMAL(precision, scale) over an int or fixed bytes
	scale    int32 // the DECIMAL scale
	uuid     bool  // UUID: sixteen bytes rendered in the canonical hex form
}

// parquetSchemaLayout reads the file's schema into the header, the column
// descriptions, and the leaf-to-field mapping, with any panic the library
// raises on a damaged schema turned into an error: the type of a leaf whose
// metadata is inconsistent -- a MAP annotation on a node with a physical type,
// found by fuzzing -- panics when asked its kind.
func parquetSchemaLayout(file *parquet.File) (header []string, columns []parquetColumn, leafField []int, flat bool, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = parseError(nil, "parquet data is damaged: %v", r)
		}
	}()
	fields := file.Schema().Fields()
	header = make([]string, len(fields))
	for i, field := range fields {
		header[i] = field.Name()
	}
	columns = parquetColumns(file)
	leafField, flat = parquetLeafFields(file.Schema(), columns)
	return header, columns, leafField, flat, nil
}

// parquetColumns describes each top-level field of the file's schema.
//
// The FLOAT16 annotation is read from the file's raw metadata rather than from
// the parsed schema, because the library's schema normalization does not carry
// it: the parsed leaf presents as a plain fixed-length byte array.
func parquetColumns(file *parquet.File) []parquetColumn {
	halfFloats := float16Leaves(file.Metadata().Schema)
	fields := file.Schema().Fields()
	columns := make([]parquetColumn, len(fields))
	leafIndex := 0
	for i, field := range fields {
		leaves := countParquetLeaves(field)
		col := columnOfNode(field)
		if col.leaf {
			col.float16 = col.float16 || halfFloats[leafIndex]
		}
		columns[i] = col
		leafIndex += leaves
	}
	return columns
}

// columnOfNode reads what a node's own type says about how its values render.
// The file's metadata can say more -- a FLOAT16 annotation the schema carries
// where the node does not -- which is why the caller with a file in hand adds
// to this rather than replacing it.
func columnOfNode(node parquet.Node) parquetColumn {
	col := parquetColumn{leaf: node.Leaf() && !node.Repeated()}
	if !col.leaf {
		return col
	}
	typ := node.Type()
	if lt := typ.LogicalType(); lt != nil {
		switch v := lt.Value.(type) {
		case *format.IntType:
			col.unsigned = !v.IsSigned
		case *format.DecimalType:
			col.decimal = true
			col.scale = v.Scale
		case *format.UUIDType:
			col.uuid = true
		case *format.Float16Type:
			// The format allows FLOAT16 on two fixed bytes alone; on any other
			// shape the annotation is inconsistent metadata and the bytes are
			// left as they are.
			col.float16 = typ.Kind() == parquet.FixedLenByteArray && typ.Length() == 2
		case *format.MapType, *format.ListType:
			// A group annotation on a node with a physical type is inconsistent
			// metadata; asking such a type its kind panics. The values still
			// render, by their own kind.
			col.leaf = false
		}
	}
	if col.leaf {
		col.kind = typ.Kind()
	}
	return col
}

// ParquetRenderer renders values of one field the way a load of that field
// renders them.
//
// A save writing a Parquet file back to itself checks its work against this:
// the text it starts from came out of a load, so a value it rebuilds from that
// text has to render as the same text again.
//
// It is built once per field rather than once per value: what a field renders
// as follows from its type alone, and a field of a million values would
// otherwise answer the same question a million times.
type ParquetRenderer struct {
	column parquetColumn
}

// NewParquetRenderer reads what a field renders as, reporting false for a field
// it cannot render: a group, a repeated field, or one whose metadata is
// inconsistent enough that asking its type panics.
func NewParquetRenderer(node parquet.Node) (renderer ParquetRenderer, ok bool) {
	defer func() {
		if r := recover(); r != nil {
			renderer, ok = ParquetRenderer{}, false
		}
	}()
	column := columnOfNode(node)
	if !column.leaf {
		return ParquetRenderer{}, false
	}
	return ParquetRenderer{column: column}, true
}

// Text is what a load renders for a value of the field, and whether the load
// holds it at all: a Parquet null and a NaN are both cells a load leaves empty.
func (r ParquetRenderer) Text(value parquet.Value) (string, bool) {
	if parquetCellIsNull(value, r.column, RenderSQLite) {
		return "", false
	}
	return renderParquetValue(value, r.column, RenderSQLite), true
}

// float16Leaves reports which leaf columns a file's own metadata annotates as
// FLOAT16, by leaf index. The format allows the annotation on two fixed bytes
// alone, so a length of anything else is inconsistent metadata and the column
// is left as the bytes it holds rather than declared a real number it cannot
// render.
func float16Leaves(elements []format.SchemaElement) map[int]bool {
	out := map[int]bool{}
	leaf := 0
	for _, element := range elements {
		if !element.Type.Valid {
			continue // a group node has no physical type and holds no column
		}
		if _, ok := element.LogicalType.Value.(*format.Float16Type); ok &&
			element.Type.V == format.FixedLenByteArray &&
			element.TypeLength.Valid && element.TypeLength.V == 2 {
			out[leaf] = true
		}
		leaf++
	}
	return out
}

// countParquetLeaves is how many leaf columns sit under a field.
func countParquetLeaves(field parquet.Field) int {
	if field.Leaf() {
		return 1
	}
	total := 0
	for _, child := range field.Fields() {
		total += countParquetLeaves(child)
	}
	return total
}

// parquetLeafFields maps each leaf column index onto the top-level field it
// belongs to, and reports whether the schema is flat: every field one
// non-repeated leaf, so a row's values line up with the fields one to one.
func parquetLeafFields(schema *parquet.Schema, columns []parquetColumn) ([]int, bool) {
	position := make(map[string]int, len(columns))
	for i, field := range schema.Fields() {
		position[field.Name()] = i
	}
	paths := schema.Columns()
	leafField := make([]int, len(paths))
	for i, path := range paths {
		leafField[i] = position[path[0]]
	}
	flat := len(paths) == len(columns)
	for _, col := range columns {
		flat = flat && col.leaf
	}
	return leafField, flat
}

// columnType is the column type one field calls for. It has to agree with what
// renderParquetValue renders under the same rendering, because a value is read
// back by parsing that string -- and, for a load, because SQLite applies the
// column's affinity to it. A mismatch is worse than text: it would store a
// value the column claims not to hold.
//
// The temporal annotations (DATE, TIME, TIMESTAMP) render as the raw count the
// file stores -- days, or ticks of the schema's unit -- which is an integer. A
// nested or repeated field, and any leaf not named here, stays text, the safe
// answer for a shape this function does not know.
func (c parquetColumn) columnType(rendering Rendering) infer.Type {
	switch {
	case !c.leaf, c.decimal, c.uuid:
		return infer.Text
	// A UINT64 column's upper half is past int64, and SQLite's INTEGER
	// affinity converts such a literal to the nearest REAL, silently a
	// different number. TEXT holds the whole range exactly, the same trade
	// DECIMAL makes. The narrower unsigned types fit in int64 and stay
	// INTEGER below.
	case c.unsigned && c.kind == parquet.Int64:
		return infer.Text
	case c.float16:
		return infer.Real
	}
	switch c.kind {
	case parquet.Boolean:
		// A boolean renders as 1 or 0 for SQLite, which is an integer there,
		// and as "true" or "false" otherwise, which is not.
		if rendering == RenderSQLite {
			return infer.Integer
		}
		return infer.Text
	case parquet.Int32, parquet.Int64, parquet.Int96:
		return infer.Integer
	case parquet.Float, parquet.Double:
		return infer.Real
	default:
		return infer.Text
	}
}

// renderParquetRow renders one row into its cells and their null marks. In a
// flat schema the row's values line up with the fields; otherwise the values
// are gathered per field, and a field whose values are all null is a null cell
// while any other set renders bracketed, the way a list prints.
func renderParquetRow(row parquet.Row, columns []parquetColumn, leafField []int, flat bool, rendering Rendering) ([]string, []bool) {
	cells := make([]string, len(columns))
	nullRow := make([]bool, len(columns))
	if flat {
		for i, value := range row {
			if i >= len(columns) {
				break
			}
			if parquetCellIsNull(value, columns[i], rendering) {
				nullRow[i] = true
				continue
			}
			cells[i] = renderParquetValue(value, columns[i], rendering)
		}
		return cells, nullRow
	}

	parts := make([][]string, len(columns))
	seen := make([]bool, len(columns))
	for _, value := range row {
		leaf := value.Column()
		if leaf < 0 || leaf >= len(leafField) {
			continue
		}
		i := leafField[leaf]
		if parquetCellIsNull(value, columns[i], rendering) {
			parts[i] = append(parts[i], "")
			continue
		}
		seen[i] = true
		parts[i] = append(parts[i], renderParquetValue(value, columns[i], rendering))
	}
	for i, col := range columns {
		switch {
		case !seen[i]:
			nullRow[i] = true
		case col.leaf && len(parts[i]) == 1:
			cells[i] = parts[i][0]
		default:
			cells[i] = "[" + strings.Join(parts[i], " ") + "]"
		}
	}
	return cells, nullRow
}

// parquetCellIsNull reports whether a cell has no value the destination can
// store: a Parquet null always, and under RenderSQLite a NaN as well, which
// SQLite has no representation for at all -- a computed NaN is NULL there, so
// NULL is what the value already means. Left as text it would sit in a column
// declared REAL as the word "NaN".
func parquetCellIsNull(v parquet.Value, col parquetColumn, rendering Rendering) bool {
	if v.IsNull() {
		return true
	}
	if rendering != RenderSQLite {
		return false
	}
	switch v.Kind() {
	case parquet.Float:
		return math.IsNaN(float64(v.Float()))
	case parquet.Double:
		return math.IsNaN(v.Double())
	case parquet.FixedLenByteArray:
		if b := v.ByteArray(); col.float16 && len(b) == 2 {
			return math.IsNaN(float64(float16To32(uint16(b[0]) | uint16(b[1])<<8)))
		}
	}
	return false
}

// renderParquetValue renders one leaf value as the text its column type calls
// for.
func renderParquetValue(v parquet.Value, col parquetColumn, rendering Rendering) string {
	switch v.Kind() {
	case parquet.Boolean:
		return boolText(v.Boolean(), rendering)
	case parquet.Int32:
		if col.decimal {
			return decimalText(big.NewInt(int64(v.Int32())), col.scale)
		}
		if col.unsigned {
			return strconv.FormatUint(uint64(uint32(v.Int32())), 10) //nolint:gosec // the unsigned column stores its value in the physical int's bits
		}
		return strconv.FormatInt(int64(v.Int32()), 10)
	case parquet.Int64:
		if col.decimal {
			return decimalText(big.NewInt(v.Int64()), col.scale)
		}
		if col.unsigned {
			return strconv.FormatUint(uint64(v.Int64()), 10) //nolint:gosec // the unsigned column stores its value in the physical int's bits
		}
		return strconv.FormatInt(v.Int64(), 10)
	case parquet.Int96:
		return strconv.FormatInt(int96EpochNanos([3]uint32(v.Int96())), 10)
	case parquet.Float:
		return floatText(float64(v.Float()), 32, rendering)
	case parquet.Double:
		return floatText(v.Double(), 64, rendering)
	case parquet.ByteArray, parquet.FixedLenByteArray:
		b := v.ByteArray()
		switch {
		// A half float is rendered at 32 bits, the narrowest width Go can
		// format it at. The bytes are little-endian per the format.
		case col.float16 && len(b) == 2:
			return floatText(float64(float16To32(uint16(b[0])|uint16(b[1])<<8)), 32, rendering)
		case col.decimal:
			return decimalText(twosComplementBig(b), col.scale)
		case col.uuid && len(b) == 16:
			return uuidText(b)
		}
		return string(b)
	default:
		return v.String()
	}
}

// int96EpochNanos converts the deprecated INT96 timestamp -- nanoseconds of
// the day in its low eight bytes and a Julian day in its high four -- to
// nanoseconds since the Unix epoch, which is what the Arrow reader rendered
// for the same column.
func int96EpochNanos(v [3]uint32) int64 {
	const julianUnixEpoch = 2440588
	const nanosPerDay = 24 * 60 * 60 * 1_000_000_000
	nanos := int64(uint64(v[0]) | uint64(v[1])<<32) //nolint:gosec // a damaged count wraps to a wrong number, never past the slice it renders into
	days := int64(v[2]) - julianUnixEpoch
	return days*nanosPerDay + nanos
}

// float16To32 widens an IEEE 754 half-precision float to single precision.
func float16To32(bits uint16) float32 {
	sign := uint32(bits>>15) << 31
	exponent := uint32(bits >> 10 & 0x1f)
	mantissa := uint32(bits & 0x3ff)
	switch exponent {
	case 0:
		if mantissa == 0 {
			return math.Float32frombits(sign) // ±0
		}
		// A subnormal half is a normal float32: shift the mantissa up to its
		// implicit leading bit, lowering the exponent as it goes.
		exponent = 1
		for mantissa&0x400 == 0 {
			mantissa <<= 1
			exponent--
		}
		mantissa &= 0x3ff
	case 0x1f: // infinities and NaN
		return math.Float32frombits(sign | 0x7f800000 | mantissa<<13)
	}
	return math.Float32frombits(sign | (exponent+112)<<23 | mantissa<<13)
}

// decimalText renders a DECIMAL's unscaled integer at its scale: "123.45" for
// 12345 at scale 2, with a leading zero when the value is smaller than its
// scale.
func decimalText(unscaled *big.Int, scale int32) string {
	if scale <= 0 {
		if scale < 0 {
			unscaled = new(big.Int).Mul(unscaled,
				new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(-scale)), nil))
		}
		return unscaled.String()
	}
	sign := ""
	digits := unscaled.String()
	if strings.HasPrefix(digits, "-") {
		sign, digits = "-", digits[1:]
	}
	if len(digits) <= int(scale) {
		digits = strings.Repeat("0", int(scale)-len(digits)+1) + digits
	}
	point := len(digits) - int(scale)
	return sign + digits[:point] + "." + digits[point:]
}

// twosComplementBig reads a big-endian two's-complement integer, which is how
// DECIMAL stores its unscaled value in a byte array.
func twosComplementBig(b []byte) *big.Int {
	n := new(big.Int).SetBytes(b)
	if len(b) > 0 && b[0]&0x80 != 0 {
		n.Sub(n, new(big.Int).Lsh(big.NewInt(1), uint(len(b)*8)))
	}
	return n
}

// uuidText renders sixteen bytes in the canonical 8-4-4-4-12 form.
func uuidText(b []byte) string {
	h := hex.EncodeToString(b)
	return h[:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:]
}

// SQLiteFloatText renders a float at bitSize so SQLite's REAL affinity converts
// it back to the same number, which "%g" does not for the three values that have
// no decimal spelling. It is what a load binds and what a dump writes, so a
// value that survives one survives the other.
//
// The column is declared REAL from the Parquet schema, and SQLite applies that
// affinity to the text an import binds: "+Inf" is not a number to it, so the
// cell was stored as TEXT inside a REAL column and typeof() answered "text" for
// a value the file held as a double. "9e999" overflows to infinity when SQLite
// parses it, which is the only spelling that survives.
//
// NaN renders as empty, the same as a null, because SQLite has no NaN at all: a
// computed one becomes NULL there, so NULL is what the value already means in
// the destination. Keeping the word would leave the same TEXT-in-a-REAL-column
// mismatch this exists to remove.
func SQLiteFloatText(f float64, bitSize int) string {
	return floatText(f, bitSize, RenderSQLite)
}

// floatText is SQLiteFloatText with the ".0" suffix left off for a caller that
// renders a value rather than storing it in a typed column.
func floatText(f float64, bitSize int, rendering Rendering) string {
	// A literal SQLite overflows to an infinity while parsing it. There is no
	// spelling of the value itself that its REAL affinity accepts.
	const infinityLiteral = "9e999"
	switch {
	case math.IsInf(f, 1):
		return infinityLiteral
	case math.IsInf(f, -1):
		return "-" + infinityLiteral
	case math.IsNaN(f):
		return ""
	}
	text := formatFloatDigits(f, bitSize)
	// A whole number renders with neither a point nor an exponent, and read back
	// that spelling is an integer. The suffix is what keeps a loaded column REAL;
	// a caller that only renders the value has no column to keep.
	if rendering == RenderSQLite && !strings.ContainsAny(text, ".eE") {
		text += ".0"
	}
	return text
}

// formatFloatDigits writes a finite float the way SQLite writes it, which is
// the plain form from a ten-thousandth up to a hundred quadrillion and the
// shortest exponent form outside that.
//
// Go's shortest 'g' leaves the plain form as soon as the decimal exponent
// reaches six, so a load and a dump with nothing in between rewrote 2500000 as
// 2.5e+06 -- a notation the source file never used, in a range that is ordinary
// for the files this library is for. The same database disagreed with itself
// about the value, since CAST(c AS TEXT) answers SQLite's own rendering.
func formatFloatDigits(f float64, bitSize int) string {
	const (
		plainLow  = 1e-4
		plainHigh = 1e17
	)
	if abs := math.Abs(f); abs == 0 || (abs >= plainLow && abs < plainHigh) {
		return strconv.FormatFloat(f, 'f', -1, bitSize)
	}
	return strconv.FormatFloat(f, 'g', -1, bitSize)
}

// boolText spells a boolean the way its column is declared: 1 and 0 for the
// INTEGER column a load declares, and the words otherwise.
func boolText(v bool, rendering Rendering) string {
	if rendering == RenderSQLite {
		if v {
			return "1"
		}
		return "0"
	}
	if v {
		return "true"
	}
	return "false"
}
