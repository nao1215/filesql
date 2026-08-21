package reader

import (
	"bytes"
	"io"

	"github.com/nao1215/filesql/internal/infer"
	"github.com/xuri/excelize/v2"
)

// Workbook is an open Excel workbook.
//
// A workbook holds several sheets and a table holds one, so opening and reading
// are separate: a caller that maps every sheet onto its own table opens once and
// reads each sheet, and one that wants a single table reads the first sheet its
// policy admits.
type Workbook struct {
	file *excelize.File
}

// OpenWorkbook reads a workbook from src.
//
// The bytes are buffered whole because the format is a zip archive that is read
// by random access, which a stream cannot provide.
func OpenWorkbook(src io.Reader) (*Workbook, error) {
	data, err := io.ReadAll(src)
	if err != nil {
		return nil, parseError(err, "failed to read XLSX data")
	}
	if len(data) == 0 {
		return nil, emptyError("empty XLSX file")
	}
	file, err := excelize.OpenReader(bytes.NewReader(data))
	if err != nil {
		return nil, parseError(err, "failed to open XLSX file")
	}
	return &Workbook{file: file}, nil
}

// Close releases the workbook.
func (w *Workbook) Close() error {
	return w.file.Close()
}

// Select returns the sheets policy admits, in workbook order, and the names it
// left out.
func (w *Workbook) Select(policy ExcelSheetPolicy) (loaded, skipped []string, err error) {
	return SelectExcelSheets(w.file, policy)
}

// Source is the workbook as sheet selection sees it, for a caller that reports
// on the sheets rather than reading them.
func (w *Workbook) Source() ExcelSheetSource {
	return w.file
}

// NoExcelSheetsError explains a workbook that contributed nothing. It separates
// one with no sheets at all from one whose sheets were all left out by the
// policy, because the two need different things done about them: the first file
// is broken, the second is a setting the caller chose.
func NoExcelSheetsError(f ExcelSheetSource, policy ExcelSheetPolicy) error {
	if policy == ExcelSheetPolicyVisibleOnly && len(f.GetSheetList()) > 0 {
		return emptyError("no visible sheets found in XLSX file")
	}
	return emptyError("no sheets found in XLSX file")
}

// ReadSheet reads one sheet of the workbook in chunks.
//
// A sheet holding nothing a table can be made from -- no rows, or no row that
// names any column -- is reported with Kind KindEmpty, so a caller loading every
// sheet can pass over it while one loading a single sheet can refuse.
func (w *Workbook) ReadSheet(name string, opts Options, emit Emit) (Result, error) {
	rows, err := w.file.GetRows(name)
	if err != nil {
		return Result{}, parseError(err, "failed to read sheet %s", name)
	}
	// A workbook stores a date as a serial number and a number format, so the
	// cells that hold one are rewritten into the ISO 8601 the datetime inference
	// recognizes. Without this a column of dates is text in whatever shape the
	// sheet was formatted, and ORDER BY sorts it lexically.
	rows = NormalizeXLSXDates(w.file, name, rows)

	if len(rows) == 0 {
		return Result{}, emptyError("empty XLSX sheet")
	}

	// A sheet may begin with rows holding no cell at all, which name no column.
	for len(rows) > 0 && len(rows[0]) == 0 {
		rows = rows[1:]
	}
	if len(rows) == 0 || len(rows[0]) == 0 {
		return Result{}, emptyError("no headers found in XLSX")
	}

	header := rows[0]
	if err := ValidateColumnNames(header); err != nil {
		return Result{}, err
	}

	evidence := make([]infer.Evidence, len(header))
	chunkSize := chunkSizeOf(opts)
	result := Result{Header: header}

	var chunk [][]string
	emitted := false
	for i, row := range rows[1:] {
		// A workbook stores no cell for a trailing empty one, so a row ending in
		// blanks arrives short and means what the padding says. More cells than
		// the header has means the opposite -- there is data in a column the
		// header does not name -- and truncating it dropped that data with no
		// error and no count to say it had happened.
		if len(row) > len(header) {
			return Result{}, parseError(nil, "row %d has %d cells where the header has %d", i+2, len(row), len(header))
		}
		record := make([]string, len(header))
		copy(record, row)

		addEvidence(evidence, record)
		chunk = append(chunk, record)
		result.Total++
		if len(chunk) >= chunkSize {
			result.Rows += len(chunk)
			if err := emit(&Chunk{Header: header, Records: chunk, Types: typesOf(evidence)}); err != nil {
				return Result{}, err
			}
			chunk = nil
			emitted = true
		}
	}

	// A sheet that is nothing but a header still names its columns.
	if len(chunk) > 0 || !emitted {
		result.Rows += len(chunk)
		if err := emit(&Chunk{Header: header, Records: chunk, Types: typesOf(evidence)}); err != nil {
			return Result{}, err
		}
	}
	result.Types = typesOf(evidence)
	return result, nil
}

// readXLSX reads the first sheet the policy admits, which is the one table a
// caller with no way to name a sheet can mean.
func readXLSX(src io.Reader, opts Options, emit Emit) (result Result, err error) {
	workbook, err := OpenWorkbook(src)
	if err != nil {
		return Result{}, err
	}
	defer func() {
		if closeErr := workbook.Close(); closeErr != nil && err == nil {
			err = parseError(closeErr, "failed to close XLSX")
		}
	}()

	sheets, _, err := workbook.Select(opts.ExcelSheetPolicy)
	if err != nil {
		return Result{}, err
	}
	if len(sheets) == 0 {
		return Result{}, NoExcelSheetsError(workbook.Source(), opts.ExcelSheetPolicy)
	}
	return workbook.ReadSheet(sheets[0], opts, emit)
}
