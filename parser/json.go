package parser

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
)

// jsonDataHeader is the column name for JSON data storage.
// JSON data is stored as raw JSON strings in a single TEXT column.
// Users can query fields using SQLite's json_extract() function.
//
// Example SQL:
//
//	SELECT json_extract(data, '$.name') AS name FROM my_json_table;
const jsonDataHeader = "data"

// parseJSON parses JSON data from reader.
// Array root: each element becomes a row with raw JSON in the "data" column.
// Object root: single row with the entire object as raw JSON.
// Primitive root (string, number, boolean, null): single row with the value as raw JSON.
//
// This approach stores raw JSON and relies on SQLite's json_extract() for field access,
// making it robust against arbitrarily nested or complex JSON structures.
//
// Example usage with SQLite:
//
//	SELECT json_extract(data, '$.name') FROM my_json_table;
//	SELECT json_extract(data, '$.address.city') FROM my_json_table;
func parseJSON(reader io.Reader) (*TableData, error) {
	content, err := io.ReadAll(reader)
	if err != nil {
		return nil, fmt.Errorf("failed to read JSON: %w", err)
	}

	trimmed := strings.TrimSpace(string(content))
	if trimmed == "" {
		return nil, errors.New("empty JSON data")
	}

	headers := []string{jsonDataHeader}
	columnTypes := []ColumnType{TypeText}

	// Try to parse as array first
	var arr []json.RawMessage
	if err := json.Unmarshal([]byte(trimmed), &arr); err == nil {
		if len(arr) == 0 {
			return nil, errors.New("empty JSON array")
		}
		records := make([][]string, 0, len(arr))
		for _, elem := range arr {
			records = append(records, []string{string(elem)})
		}
		return &TableData{
			Headers:     headers,
			Records:     records,
			ColumnTypes: columnTypes,
		}, nil
	}

	// Try as single value (object, string, number, boolean, null)
	var obj json.RawMessage
	if err := json.Unmarshal([]byte(trimmed), &obj); err != nil {
		return nil, fmt.Errorf("failed to parse JSON: %w", err)
	}

	return &TableData{
		Headers:     headers,
		Records:     [][]string{{string(obj)}},
		ColumnTypes: columnTypes,
	}, nil
}

// parseJSONL parses JSON Lines data from reader.
// Each non-empty line must be valid JSON and becomes a row with raw JSON in "data" column.
// Empty lines are silently skipped.
//
// JSONL (JSON Lines) format stores one JSON value per line, making it ideal for
// streaming and append-only log files. Each line is independently valid JSON.
//
// Example usage with SQLite:
//
//	SELECT json_extract(data, '$.status') FROM my_jsonl_table
//	WHERE json_extract(data, '$.code') = 200;
func parseJSONL(reader io.Reader) (*TableData, error) {
	br := bufio.NewReader(reader)

	headers := []string{jsonDataHeader}
	columnTypes := []ColumnType{TypeText}
	var records [][]string
	lineNum := 0

	for {
		rawLine, err := br.ReadBytes('\n')
		// Process whatever we got before checking the error.
		// ReadBytes returns data even when err == io.EOF.
		lineNum++
		line := strings.TrimSpace(string(rawLine))
		if line != "" {
			if !json.Valid([]byte(line)) {
				return nil, fmt.Errorf("invalid JSON on line %d: %s", lineNum, truncateLine(line, 100))
			}
			records = append(records, []string{line})
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, fmt.Errorf("failed to read JSONL: %w", err)
		}
	}

	if len(records) == 0 {
		return nil, errors.New("empty JSONL data")
	}

	return &TableData{
		Headers:     headers,
		Records:     records,
		ColumnTypes: columnTypes,
	}, nil
}

// truncateLine truncates a string to maxLen characters for error messages.
func truncateLine(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen] + "..."
}
