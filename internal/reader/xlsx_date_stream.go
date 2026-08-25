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

// The date normalization has two ways to find the date cells of a sheet, and
// they differ only in how they reach a cell's style.
//
// Asking the library, one cell at a time, is what NormalizeXLSXDates does. It
// works on any open workbook, and it costs: the first such question makes
// excelize unmarshal the whole worksheet into its object model, 1470 MB for an
// 18.5 MB workbook of 200,000 rows, against 267 MB for the streaming row read
// that produced the rows being normalized.
//
// Reading the sheet's own XML, which is what this file does, costs what the
// date cells cost and nothing for the rest. It needs the workbook's bytes,
// which the loader has because the format is a zip and was buffered to open it
// at all, and it is used when those bytes are there. The other way stays for
// the exported function, which is handed an open workbook and nothing else.

// datedCell is a cell of a sheet, numbered the way the rows a read produces are.
type datedCell struct {
	row, col int
}

// dateCellsFromXML returns the ISO 8601 rendering of every cell of a sheet that
// wears one of the given styles, read from the sheet's own XML.
//
// It reports false when the workbook's parts do not say where the sheet is,
// which is the signal to fall back to asking the library cell by cell rather
// than to answer with a sheet that is missing cells.
func dateCellsFromXML(data []byte, sheet string, dateStyles map[int]bool, date1904 bool) (map[datedCell]string, bool) {
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

	dates := make(map[datedCell]string)
	if err := scanSheetDates(file, dateStyles, date1904, dates); err != nil {
		return nil, false
	}
	return dates, true
}

// scanSheetDates walks a worksheet's XML and records the date cells it holds.
func scanSheetDates(src io.Reader, dateStyles map[int]bool, date1904 bool, into map[datedCell]string) error {
	decoder := xml.NewDecoder(src)
	row, col := 0, 0
	// dated says the cell now open is one to convert: a number, which is what a
	// date is stored as, wearing one of the styles that renders a calendar day.
	// A shared string or an inline string is text whatever its style says.
	dated := false
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
			dated = (kind == "" || kind == "n") && dateStyles[style]
		case "v":
			if !dated {
				continue
			}
			var raw string
			if err := decoder.DecodeElement(&raw, &start); err != nil {
				return err
			}
			if iso, ok := isoFromRaw(raw, date1904); ok {
				into[datedCell{row: row, col: col}] = iso
			}
			dated = false
		}
	}
}

// sheetPart finds the worksheet part a sheet name belongs to, following the
// workbook's relationships rather than guessing at a file name: a workbook
// whose sheets were reordered or deleted does not name its parts in order.
func sheetPart(archive *zip.Reader, sheet string) (*zip.File, bool) {
	relID, ok := sheetRelationshipID(archive, sheet)
	if !ok {
		return nil, false
	}
	target, ok := relationshipTarget(archive, relID)
	if !ok {
		return nil, false
	}
	// A target is relative to the part that names it, which is xl/workbook.xml,
	// unless it is written from the root.
	name := path.Join("xl", target)
	if strings.HasPrefix(target, "/") {
		name = strings.TrimPrefix(target, "/")
	}
	for _, file := range archive.File {
		if file.Name == name {
			return file, true
		}
	}
	return nil, false
}

// sheetRelationshipID reads xl/workbook.xml for the relationship id of a sheet.
func sheetRelationshipID(archive *zip.Reader, sheet string) (string, bool) {
	body, ok := partOf(archive, "xl/workbook.xml")
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

// relationshipTarget reads xl/_rels/workbook.xml.rels for what a relationship
// id points at.
func relationshipTarget(archive *zip.Reader, relID string) (string, bool) {
	body, ok := partOf(archive, "xl/_rels/workbook.xml.rels")
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
