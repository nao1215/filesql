package parser

import (
	"bytes"
	"errors"
	"fmt"
	"io"

	"github.com/xuri/excelize/v2"
)

// parseXLSX parses Excel XLSX data into one table, taking the first sheet the
// policy admits. A workbook holds several sheets and TableData holds one table,
// so a choice has to be made; under ExcelSheetPolicyVisibleOnly the choice
// matches what the workbook presents rather than what happens to be stored
// first.
func parseXLSX(reader io.Reader, policy ExcelSheetPolicy) (table *TableData, err error) {
	// Read all data into memory (excelize requires this)
	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, fmt.Errorf("failed to read XLSX data: %w", err)
	}

	f, err := excelize.OpenReader(bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("failed to open XLSX: %w", err)
	}
	defer func() {
		if closeErr := f.Close(); closeErr != nil && err == nil {
			err = fmt.Errorf("failed to close XLSX: %w", closeErr)
		}
	}()

	// Get the first sheet the policy admits
	sheets, _, err := SelectExcelSheets(f, policy)
	if err != nil {
		return nil, err
	}
	if len(sheets) == 0 {
		if policy == ExcelSheetPolicyVisibleOnly && len(f.GetSheetList()) > 0 {
			return nil, errors.New("no visible sheets found in XLSX file")
		}
		return nil, errors.New("no sheets found in XLSX file")
	}

	sheetName := sheets[0]
	rows, err := f.GetRows(sheetName)
	if err != nil {
		return nil, fmt.Errorf("failed to read sheet %s: %w", sheetName, err)
	}

	if len(rows) == 0 {
		return nil, errors.New("empty XLSX sheet")
	}

	headers := rows[0]
	if len(headers) == 0 {
		return nil, errors.New("no headers found in XLSX")
	}

	if err := validateColumnNames(headers); err != nil {
		return nil, err
	}

	// Normalize records to match header length
	records := make([][]string, 0, len(rows)-1)
	for i := 1; i < len(rows); i++ {
		row := rows[i]
		// Pad or truncate to match header length
		normalizedRow := make([]string, len(headers))
		for j := range headers {
			if j < len(row) {
				normalizedRow[j] = row[j]
			}
		}
		records = append(records, normalizedRow)
	}

	// Infer column types
	columnTypes := inferColumnTypes(headers, records)

	return &TableData{
		Headers:     headers,
		Records:     records,
		ColumnTypes: columnTypes,
	}, nil
}
