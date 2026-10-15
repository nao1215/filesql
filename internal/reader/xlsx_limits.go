package reader

import (
	"archive/zip"
	"bytes"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"strconv"

	"github.com/xuri/excelize/v2"
)

// workbookArchive checks sizes before Excelize converts them to signed values
// (GHSA-fw94-4wwp-w8pw).
// It also rejects non-ZIP input before the encrypted OLE/CFB parser can allocate
// from untrusted directory entries. This API has no password option and cannot
// open encrypted workbooks.
func workbookArchive(data []byte) (*zip.Reader, error) {
	archive, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return nil, err
	}
	var total uint64
	seen := make(map[string]bool, len(archive.File))
	// Match Excelize's default limit, but compare in unsigned space before
	// reading any XML or allowing a size to wrap through an int64 conversion.
	const unzipLimit = 1000 << 24
	for _, file := range archive.File {
		name := normalizePartName(file.Name)
		if seen[name] {
			return nil, fmt.Errorf("duplicate XLSX package part %q", name)
		}
		seen[name] = true
		if file.UncompressedSize64 > unzipLimit-total {
			return nil, errors.New("XLSX uncompressed size exceeds the supported range")
		}
		total += file.UncompressedSize64
	}
	for _, file := range archive.File {
		// Excelize v2.11.0 reads styles from this fixed package path.
		if normalizePartName(file.Name) == "xl/styles.xml" {
			if err := checkXMLAttributes(file, checkStyleIndices); err != nil {
				return nil, err
			}
		}
	}
	return archive, nil
}

// checkSheetRowLimits runs before GetRows, whose look-ahead can otherwise set
// an unchecked row number and iterate once per missing row (GHSA-jw42-f3rr-4cc3).
func checkSheetRowLimits(data []byte, sheet string) (err error) {
	archive, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return err
	}
	part, ok := sheetPart(archive, sheet)
	if !ok {
		return fmt.Errorf("cannot locate worksheet %q", sheet)
	}
	return checkXMLAttributes(part, checkRowNumber)
}

const worksheetRowElement = "row"

func checkRowNumber(element xml.StartElement) error {
	if element.Name.Local != worksheetRowElement {
		return nil
	}
	for _, attr := range element.Attr {
		if attr.Name.Local != "r" {
			continue
		}
		row, err := strconv.ParseUint(attr.Value, 10, 64)
		if err != nil {
			return fmt.Errorf("invalid worksheet row %q: %w", attr.Value, err)
		}
		if row > excelize.TotalRows {
			return fmt.Errorf("worksheet row %d exceeds %d", row, excelize.TotalRows)
		}
	}
	return nil
}

// checkStyleIndices blocks negative indices before number-format lookup reaches
// GetStyle, which only bounds their upper end (GHSA-5h23-36rv-pm65).
func checkStyleIndices(element xml.StartElement) error {
	if element.Name.Local != "xf" {
		return nil
	}
	for _, attr := range element.Attr {
		switch attr.Name.Local {
		case "fillId", "borderId", "fontId":
			if _, err := strconv.ParseUint(attr.Value, 10, 32); err != nil {
				return fmt.Errorf("invalid style %s %q: %w", attr.Name.Local, attr.Value, err)
			}
		}
	}
	return nil
}

func checkXMLAttributes(part *zip.File, check func(xml.StartElement) error) (err error) {
	body, err := part.Open()
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, body.Close()) }()
	decoder := xml.NewDecoder(body)
	for {
		token, err := decoder.Token()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		if element, ok := token.(xml.StartElement); ok {
			if err := check(element); err != nil {
				return err
			}
		}
	}
}
