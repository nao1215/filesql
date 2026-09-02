package reader

import (
	"archive/zip"
	"bytes"
	"encoding/xml"
	"errors"
	"io"
	"path"
	"strings"
)

// The normalization has two ways to find the cells whose drawing is not the
// value behind them, and they differ only in how they reach a cell's style and
// type.
//
// Asking the library, one cell at a time, is what normalizeXLSXDates and
// normalizeXLSXNumbers do. It works on any open workbook, and it costs: the
// first such question makes excelize unmarshal the whole worksheet into its
// object model, 1470 MB for an 18.5 MB workbook of 200,000 rows, against 267 MB
// for the streaming row read that produced the rows being normalized.
//
// Reading the sheet's own XML, which is what this file does, costs what the
// rewritten cells cost and nothing for the rest, and the style and the type are
// attributes of the cell it is already looking at. It needs the workbook's
// bytes, which the loader has because the format is a zip and was buffered to
// open it at all, and it is used when those bytes are there. The other way
// stays for a workbook that arrived without them.

// sheetCell is a cell of a sheet, numbered the way the rows a read produces are.
type sheetCell struct {
	row, col int
}

// numberFormatStyles are the styles whose number format draws something other
// than the number a cell stores: a calendar day, which becomes ISO 8601, and a
// time of day or an elapsed duration, which keep what the sheet drew.
type numberFormatStyles struct {
	dates  map[int]bool
	clocks map[int]bool
}

// cellValuesFromXML returns what every cell of a sheet loads as, for the cells
// whose drawing is not the value behind them: a date as ISO 8601 and any other
// number as the text the file stores. It is read from the sheet's own XML.
//
// It reports false when the workbook's parts do not say where the sheet is,
// which is the signal to fall back to asking the library cell by cell rather
// than to answer with a sheet that is missing cells.
func cellValuesFromXML(data []byte, sheet string, styles numberFormatStyles, date1904 bool) (map[sheetCell]string, bool) {
	archive, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return nil, false
	}
	part, ok := sheetPart(archive, sheet)
	if !ok {
		return nil, false
	}
	file, err := part.Open()
	if err != nil {
		return nil, false
	}
	defer file.Close()

	values := make(map[sheetCell]string)
	if err := scanSheetValues(file, styles, date1904, values); err != nil {
		return nil, false
	}
	return values, true
}

// scanSheetValues walks a worksheet's XML and records what each cell whose
// drawing is not its value loads as.
func scanSheetValues(src io.Reader, styles numberFormatStyles, date1904 bool, into map[sheetCell]string) error {
	decoder := xml.NewDecoder(src)
	row, col := 0, 0
	// dated says the cell now open is one to convert: a number, which is what a
	// date is stored as, wearing one of the styles that renders a calendar day.
	// A shared string or an inline string is text whatever its style says.
	dated := false
	// stored says the cell now open is a number the sheet draws as a quantity,
	// so the number it stores is what it loads as.
	stored := false
	for {
		token, err := decoder.Token()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
		start, ok := token.(xml.StartElement)
		if !ok {
			continue
		}
		switch start.Name.Local {
		case "row":
			// A row carries its number, and rows without one follow the row
			// before, which is what a writer that omits them means.
			if n, found := attrInt(start.Attr, "r"); found {
				row = n
			} else {
				row++
			}
			col = 0
		case "c":
			col++
			if ref, found := attr(start.Attr, "r"); found {
				if c, r, valid := cellRef(ref); valid {
					col, row = c, r
				}
			}
			style, _ := attrInt(start.Attr, "s")
			kind, _ := attr(start.Attr, "t")
			number := kind == "" || kind == "n"
			dated = number && styles.dates[style]
			// A boolean is stored as 1 or 0 and drawn as a word, so it loads as
			// the number it stores, like any other cell whose drawing is not
			// its value.
			stored = (number && !dated && !styles.clocks[style]) || kind == "b"
		case "v":
			if !dated && !stored {
				continue
			}
			var raw string
			if err := decoder.DecodeElement(&raw, &start); err != nil {
				return err
			}
			switch {
			case dated:
				if iso, ok := isoFromRaw(raw, date1904); ok {
					into[sheetCell{row: row, col: col}] = iso
				}
			case isPlainNumber(raw):
				into[sheetCell{row: row, col: col}] = raw
			}
			dated, stored = false, false
		}
	}
}

// rowsHoldingCellsFromXML returns the numbers of the rows of a sheet that hold
// at least one cell, read from the sheet's own XML.
//
// It exists because the library drops a cell whose value is the empty string,
// so a row whose cells are all empty arrives with no cells at all and cannot be
// told from a row that is not in the file. The two mean opposite things: the
// first is a record whose values are empty, and the second is the space under a
// sheet's data, which a workbook with a stray cell near the bottom has by the
// million. The file says which is which, so the file is asked.
//
// It reports false when the workbook's parts do not say where the sheet is,
// which is the signal to fall back to the reading that cannot tell them apart
// rather than to answer with a sheet that is missing rows.
func rowsHoldingCellsFromXML(data []byte, sheet string) (*rowSet, bool) {
	archive, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return nil, false
	}
	part, ok := sheetPart(archive, sheet)
	if !ok {
		return nil, false
	}
	file, err := part.Open()
	if err != nil {
		return nil, false
	}
	defer file.Close()

	rows := &rowSet{}
	if err := scanSheetRows(file, rows); err != nil {
		return nil, false
	}
	return rows, true
}

// rowSet is the set of row numbers a sheet holds a cell in. It is a bitmap
// rather than a map because a dense sheet holds every row from one to its last:
// a workbook of 200,000 rows costs 25 KB here and a map of the same rows costs
// several megabytes, against the 267 MB such a workbook costs to read.
type rowSet struct {
	words []uint64
	last  int
}

// add records that a row holds a cell.
func (s *rowSet) add(row int) {
	if row < 1 {
		return
	}
	word := (row - 1) / 64
	for len(s.words) <= word {
		s.words = append(s.words, 0)
	}
	s.words[word] |= 1 << uint((row-1)%64)
	if row > s.last {
		s.last = row
	}
}

// has reports whether a row holds a cell. A nil set answers false for every
// row, which is the reading that came before this could be asked.
func (s *rowSet) has(row int) bool {
	if s == nil || row < 1 {
		return false
	}
	word := (row - 1) / 64
	if word >= len(s.words) {
		return false
	}
	return s.words[word]&(1<<uint((row-1)%64)) != 0
}

// lastRow is the highest row the sheet holds a cell in, or zero for a nil set.
func (s *rowSet) lastRow() int {
	if s == nil {
		return 0
	}
	return s.last
}

// scanSheetRows fills into with the number of every row of a sheet that holds a
// cell.
//
// The scan reads bytes rather than XML tokens because it asks one question --
// which rows have a cell -- and asking it through encoding/xml costs about as
// much again as reading the sheet did in the first place. A cell carries its own
// reference, "A3", which names its row, and a row element carries its number for
// the cells that leave theirs out.
func scanSheetRows(src io.Reader, into *rowSet) error {
	// The window carries the tail of one read into the next, so a tag split
	// across the boundary is still seen whole. A tag of this kind is far
	// shorter than the carry.
	const (
		bufferSize = 64 << 10
		carrySize  = 256
	)
	buf := make([]byte, bufferSize+carrySize)
	carried := 0
	row := 0
	inComment := false
	for {
		n, err := src.Read(buf[carried : carried+bufferSize])
		if n > 0 {
			window := buf[:carried+n]
			consumed := scanRowWindow(window, &row, &inComment, into, true)
			// A tag that has run longer than the carry is not a row or a cell,
			// which are short; a comment or a damaged file can hold one. It is
			// let go rather than carried, since carrying it would leave the
			// next read no room in the buffer.
			if len(window)-consumed > carrySize {
				consumed = len(window)
			}
			carried = copy(buf, window[consumed:])
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				if carried > 0 {
					scanRowWindow(buf[:carried], &row, &inComment, into, false)
				}
				return nil
			}
			return err
		}
	}
}

// scanRowWindow records the rows the cells in one window belong to and answers
// how much of the window it consumed. When more is coming, a tag that runs past
// the end is left for the next window.
//
// A comment is passed over whole, however many windows it spans, because what
// it holds is not markup: a comment quoting a row would otherwise count as one.
// inComment carries that state from one window to the next.
func scanRowWindow(window []byte, row *int, inComment *bool, into *rowSet, more bool) int {
	at := 0
	for {
		if *inComment {
			end := bytes.Index(window[at:], []byte("-->"))
			if end < 0 {
				// The comment runs on. The last two bytes may be the start of
				// its end, so they are carried into the next window.
				if more && len(window)-at >= 2 {
					return len(window) - 2
				}
				return len(window)
			}
			*inComment = false
			at += end + len("-->")
			continue
		}
		next := bytes.IndexByte(window[at:], '<')
		if next < 0 {
			return len(window)
		}
		start := at + next
		tag := window[start:]
		if bytes.HasPrefix(tag, []byte("<!--")) {
			*inComment = true
			at = start + len("<!--")
			continue
		}
		end := bytes.IndexByte(tag, '>')
		if end < 0 {
			if more {
				return start
			}
			return len(window)
		}
		tag = tag[:end]
		name, attrs := startTag(tag)
		switch string(name) {
		case "row":
			// A row carries its number, and one without follows the row before,
			// which is what a writer that omits them means.
			if n, ok := rowNumberAttr(attrs); ok {
				*row = n
			} else {
				*row++
			}
		case "c":
			// A cell carries the row it is in, which is what says where it is
			// when its row left its own number out.
			if n, ok := rowNumberAttr(attrs); ok {
				*row = n
			}
			into.add(*row)
		}
		at = start + end + 1
	}
}

// startTag splits a tag into the local name of the element it opens and the
// attributes after the name. An end tag, a comment and a processing
// instruction open nothing and are answered with no name.
//
// The name is read whole and then compared, rather than matched by prefix, so
// "<row" is a row and "<rowBreaks" is not; and a namespace prefix on it is
// dropped, since a writer may put every element behind one -- "<x:row" -- and
// the parser this scan stands in for compares the local name.
func startTag(tag []byte) (name, attrs []byte) {
	if len(tag) < 2 || tag[0] != '<' {
		return nil, nil
	}
	body := tag[1:]
	end := 0
	for end < len(body) && !isXMLSpace(body[end]) && body[end] != '/' && body[end] != '>' {
		end++
	}
	name = body[:end]
	if len(name) == 0 || name[0] == '?' || name[0] == '!' {
		return nil, nil
	}
	if colon := bytes.LastIndexByte(name, ':'); colon >= 0 {
		name = name[colon+1:]
	}
	return name, body[end:]
}

// isXMLSpace reports whether a byte is what XML counts as whitespace.
func isXMLSpace(c byte) bool {
	return c == ' ' || c == '\t' || c == '\r' || c == '\n'
}

// attrValue finds an attribute by name among a start tag's attributes, reading
// them as the parser accepts them: a name, whitespace or none, an equals sign,
// whitespace or none, and a value in either kind of quote. A writer that quotes
// with single quotes, breaks a line before an attribute or spaces out the
// equals sign is writing XML, and the parser this scan stands in for reads it.
func attrValue(attrs []byte, name string) ([]byte, bool) {
	at := 0
	for {
		for at < len(attrs) && isXMLSpace(attrs[at]) {
			at++
		}
		if at >= len(attrs) || attrs[at] == '/' || attrs[at] == '>' {
			return nil, false
		}
		start := at
		for at < len(attrs) && !isXMLSpace(attrs[at]) && attrs[at] != '=' && attrs[at] != '/' {
			at++
		}
		key := attrs[start:at]
		for at < len(attrs) && isXMLSpace(attrs[at]) {
			at++
		}
		if at >= len(attrs) || attrs[at] != '=' {
			return nil, false
		}
		at++
		for at < len(attrs) && isXMLSpace(attrs[at]) {
			at++
		}
		if at >= len(attrs) || (attrs[at] != '"' && attrs[at] != '\'') {
			return nil, false
		}
		quote := attrs[at]
		at++
		end := bytes.IndexByte(attrs[at:], quote)
		if end < 0 {
			return nil, false
		}
		if string(key) == name {
			return attrs[at : at+end], true
		}
		at += end + 1
	}
}

// rowNumberAttr reads the row a tag's r attribute names: the number itself on a
// row element, and the digits of a reference such as "A3" on a cell.
func rowNumberAttr(attrs []byte) (int, bool) {
	value, ok := attrValue(attrs, "r")
	if !ok {
		return 0, false
	}
	digits := value
	for len(digits) > 0 && isASCIILetter(digits[0]) {
		digits = digits[1:]
	}
	if len(digits) == 0 {
		return 0, false
	}
	n := 0
	for _, c := range digits {
		if c < '0' || c > '9' {
			return 0, false
		}
		n = n*10 + int(c-'0')
		if n > maxSheetRow {
			return 0, false
		}
	}
	if n == 0 {
		return 0, false
	}
	return n, true
}

// isASCIILetter reports whether a byte is a column letter of a cell reference,
// in either case, since "a3" names the same cell as "A3".
func isASCIILetter(c byte) bool {
	return (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z')
}

// maxSheetRow bounds a row number read out of a sheet, which is what keeps a
// damaged file from asking for a bitmap the size of its own digits.
const maxSheetRow = 1 << 24

// sheetPart finds the worksheet part a sheet name belongs to, following the
// package's own bookkeeping rather than guessing at a file name: the root
// relationships say where the workbook's main part is, the main part's
// relationships say where each sheet is, and a workbook whose sheets were
// reordered or deleted does not name its parts in order.
func sheetPart(archive *zip.Reader, sheet string) (*zip.File, bool) {
	workbook := workbookPart(archive)
	relID, ok := sheetRelationshipID(archive, workbook, sheet)
	if !ok {
		return nil, false
	}
	target, ok := relationshipTarget(archive, relsPartOf(workbook), relID)
	if !ok {
		return nil, false
	}
	name := resolvePart(path.Dir(workbook), target)
	for _, file := range archive.File {
		if file.Name == name {
			return file, true
		}
	}
	return nil, false
}

// workbookPart is the name of the workbook's main part. Excel writes it at
// xl/workbook.xml, and the format lets a writer put it anywhere the package's
// root relationships name; a package whose root relationships are missing,
// unreadable or silent is answered with Excel's name, the only one left to
// guess.
func workbookPart(archive *zip.Reader) string {
	const excelWrites = "xl/workbook.xml"
	body, ok := partOf(archive, "_rels/.rels")
	if !ok {
		return excelWrites
	}
	var rels struct {
		Relationship []struct {
			Type   string `xml:"Type,attr"`
			Target string `xml:"Target,attr"`
		} `xml:"Relationship"`
	}
	if err := xml.Unmarshal(body, &rels); err != nil {
		return excelWrites
	}
	for _, r := range rels.Relationship {
		// The transitional and the strict schema name the relationship under
		// different namespaces and the same last word.
		if strings.HasSuffix(r.Type, "/officeDocument") && r.Target != "" {
			return resolvePart("", r.Target)
		}
	}
	return excelWrites
}

// relsPartOf names the part holding a part's relationships, which sits in a
// _rels directory beside it under the part's own name with .rels appended.
func relsPartOf(part string) string {
	dir, base := path.Split(part)
	return dir + "_rels/" + base + ".rels"
}

// resolvePart resolves a relationship target against the directory of the part
// that names it. A target beginning with a slash is written from the root.
func resolvePart(dir, target string) string {
	if strings.HasPrefix(target, "/") {
		return path.Clean(strings.TrimPrefix(target, "/"))
	}
	return path.Join(dir, target)
}

// sheetRelationshipID reads the workbook's main part for the relationship id
// of a sheet.
func sheetRelationshipID(archive *zip.Reader, part, sheet string) (string, bool) {
	body, ok := partOf(archive, part)
	if !ok {
		return "", false
	}
	var workbook struct {
		Sheets struct {
			Sheet []struct {
				Name string `xml:"name,attr"`
				ID   string `xml:"id,attr"`
			} `xml:"sheet"`
		} `xml:"sheets"`
	}
	if err := xml.Unmarshal(body, &workbook); err != nil {
		return "", false
	}
	for _, s := range workbook.Sheets.Sheet {
		if s.Name == sheet {
			return s.ID, s.ID != ""
		}
	}
	return "", false
}

// relationshipTarget reads a relationships part for what a relationship id
// points at.
func relationshipTarget(archive *zip.Reader, part, relID string) (string, bool) {
	body, ok := partOf(archive, part)
	if !ok {
		return "", false
	}
	var rels struct {
		Relationship []struct {
			ID     string `xml:"Id,attr"`
			Target string `xml:"Target,attr"`
		} `xml:"Relationship"`
	}
	if err := xml.Unmarshal(body, &rels); err != nil {
		return "", false
	}
	for _, r := range rels.Relationship {
		if r.ID == relID {
			return r.Target, r.Target != ""
		}
	}
	return "", false
}

// partOf reads one part of the archive whole. The parts read this way are the
// workbook's own bookkeeping, which is small; the sheet is streamed instead.
func partOf(archive *zip.Reader, name string) ([]byte, bool) {
	for _, file := range archive.File {
		if file.Name != name {
			continue
		}
		body, err := file.Open()
		if err != nil {
			return nil, false
		}
		defer body.Close()
		content, err := io.ReadAll(body)
		if err != nil {
			return nil, false
		}
		return content, true
	}
	return nil, false
}

// attr returns an attribute's value.
func attr(attrs []xml.Attr, name string) (string, bool) {
	for _, a := range attrs {
		if a.Name.Local == name {
			return a.Value, true
		}
	}
	return "", false
}

// attrInt returns an attribute's value as a number.
func attrInt(attrs []xml.Attr, name string) (int, bool) {
	value, found := attr(attrs, name)
	if !found {
		return 0, false
	}
	return parseDigits(value)
}

// cellRef splits a cell reference such as "AB12" into its column and row, both
// numbered from one.
func cellRef(ref string) (col, row int, ok bool) {
	letters := 0
	for letters < len(ref) {
		c := ref[letters]
		switch {
		case c >= 'A' && c <= 'Z':
			col = col*26 + int(c-'A') + 1
		case c >= 'a' && c <= 'z':
			col = col*26 + int(c-'a') + 1
		default:
			// The letters have ended; what follows is the row.
			row, ok = parseDigits(ref[letters:])
			return col, row, ok && col != 0
		}
		letters++
	}
	// All letters and no row is not a cell reference.
	return 0, 0, false
}

// parseDigits reads a run of ASCII digits, and reports false for anything else,
// including an empty one.
func parseDigits(s string) (int, bool) {
	if s == "" {
		return 0, false
	}
	n := 0
	for i := range len(s) {
		c := s[i]
		if c < '0' || c > '9' {
			return 0, false
		}
		n = n*10 + int(c-'0')
	}
	return n, true
}
