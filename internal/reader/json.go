package reader

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"strings"

	"github.com/nao1215/filesql/internal/infer"
)

// JSONDataColumn is the one column a JSON or JSONL table has: the raw JSON of
// each element, held as text.
//
// The document is stored as it was written and read back with SQLite's
// json_extract(), which is what makes an arbitrarily nested document queryable
// without a schema:
//
//	SELECT json_extract(data, '$.address.city') FROM my_json_table;
const JSONDataColumn = "data"

// jsonHeader is the header every JSON and JSONL table has.
func jsonHeader() ([]string, []infer.Type) {
	return []string{JSONDataColumn}, []infer.Type{infer.Text}
}

// readJSON reads one JSON document into the "data" column. An array is streamed
// element by element; any other value is one row.
func readJSON(src io.Reader, opts Options, emit Emit) (Result, error) {
	buffered := bufio.NewReader(src)
	header, types := jsonHeader()
	result := Result{Header: header, Types: types}

	isArray, empty, err := peekJSONIsArray(buffered)
	if err != nil {
		return Result{}, err
	}
	if empty {
		// A document holding nothing is a table with no rows. Only the caller
		// knows whether that is a failure, so it is reported rather than refused.
		result.EmptyInput = true
		return result, emit(&Chunk{Header: header, Types: types})
	}

	if isArray {
		decoder := json.NewDecoder(buffered)
		if _, err := decoder.Token(); err != nil {
			return Result{}, jsonError(err, "failed to parse JSON array")
		}
		rows, err := readJSONArray(decoder, opts, emit)
		if err != nil {
			return Result{}, err
		}
		result.Rows = rows
		result.Total = rows
		return result, nil
	}

	content, err := io.ReadAll(buffered)
	if err != nil {
		return Result{}, parseError(err, "failed to read JSON")
	}
	var value json.RawMessage
	if err := json.Unmarshal(bytes.TrimSpace(content), &value); err != nil {
		return Result{}, invalidError(err, "failed to parse JSON")
	}
	result.Rows = 1
	result.Total = 1
	return result, emit(&Chunk{Header: header, Records: [][]string{{string(value)}}, Types: types})
}

// jsonError classifies a failure the JSON decoder reported, by what failed
// rather than by which branch found it.
//
// A document that is not JSON is invalid data whatever value it opens with. The
// array branch used to report every failure as a parse error, so a caller
// writing errors.Is(err, ErrInvalidData) to mean "this file is not JSON" matched
// an unterminated object and missed an unterminated array, with nothing in the
// message to explain the difference.
//
// Swapping the two outright would be wrong in the other direction: the decoder
// also hands back whatever the underlying reader said, and a disk that went away
// is not a document that is not JSON. A syntax error, or an input that ended in
// the middle of a value, is the format's fault; anything else came from
// underneath and stays a parse failure.
func jsonError(cause error, message string) error {
	var syntax *json.SyntaxError
	if errors.As(cause, &syntax) || errors.Is(cause, io.EOF) || errors.Is(cause, io.ErrUnexpectedEOF) {
		return invalidError(cause, "%s", message)
	}
	return parseError(cause, "%s", message)
}

// peekJSONIsArray reports whether the document opens with '[', and whether it
// holds no value at all. Leading whitespace is skipped.
//
// What the document opens with decides the shape, rather than whether it
// unmarshals into a slice: "null" unmarshals into one as the empty slice, and
// answering "empty JSON array" for a document holding no array named something
// the caller had not written.
func peekJSONIsArray(buffered *bufio.Reader) (isArray, empty bool, err error) {
	for {
		b, err := buffered.ReadByte()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return false, true, nil
			}
			return false, false, parseError(err, "failed to read JSON")
		}
		if b == ' ' || b == '\t' || b == '\n' || b == '\r' {
			continue
		}
		// Put the byte back so the decoder or ReadAll can consume it.
		if err := buffered.UnreadByte(); err != nil {
			return false, false, parseError(err, "failed to read JSON")
		}
		return b == '[', false, nil
	}
}

// readJSONArray streams the elements of an array whose opening bracket the
// decoder has already consumed, and returns how many rows it emitted.
func readJSONArray(decoder *json.Decoder, opts Options, emit Emit) (int, error) {
	header, types := jsonHeader()
	elements := newTypedChunker(header, types, opts, emit)
	for decoder.More() {
		var element json.RawMessage
		if err := decoder.Decode(&element); err != nil {
			return 0, jsonError(err, "failed to decode JSON array element")
		}
		if err := elements.add([]string{string(element)}); err != nil {
			return 0, err
		}
	}

	// Consume the closing bracket, then refuse anything after it.
	if _, err := decoder.Token(); err != nil {
		return 0, jsonError(err, "failed to read JSON array end")
	}
	// A complete document has no token left, and the decoder says so with
	// io.EOF. More() cannot answer this: it reports whether the container being
	// iterated holds another element, so it answers false for ']' and '}',
	// which are exactly the two bytes a stray container close is written with
	// and were the two this read passed over rather than refused.
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		if err != nil {
			return 0, jsonError(err, "unexpected data after JSON array")
		}
		return 0, invalidError(nil, "unexpected data after JSON array")
	}

	if err := elements.finish(); err != nil {
		return 0, err
	}
	return elements.rows, nil
}

// readJSONL reads one JSON value per line into the "data" column. A blank line
// is not a value and is skipped.
func readJSONL(src io.Reader, opts Options, emit Emit) (Result, error) {
	buffered := bufio.NewReader(src)
	header, types := jsonHeader()
	result := Result{Header: header, Types: types}
	values := newTypedChunker(header, types, opts, emit)
	limit := recordLimitOf(opts)
	lineNum := 0
	for {
		raw, err := readLine(buffered, limit, lineNum+1)
		if errors.Is(err, ErrRecordTooLong) {
			return Result{}, err
		}
		// Whatever arrived is processed before the error is checked: ReadBytes
		// returns the last line together with io.EOF.
		lineNum++
		line := strings.TrimSpace(string(raw))
		if line != "" {
			if !json.Valid([]byte(line)) {
				return Result{}, invalidError(nil, "invalid JSON on line %d: %s", lineNum, truncateLine(line, 100))
			}
			if err := values.add([]string{line}); err != nil {
				return Result{}, err
			}
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return Result{}, parseError(err, "failed to read JSONL")
		}
	}

	// An input with no lines is a table with no rows. Only the caller knows
	// whether that is a failure.
	if err := values.finish(); err != nil {
		return Result{}, err
	}
	result.Rows = values.rows
	result.Total = result.Rows
	result.EmptyInput = result.Rows == 0
	return result, nil
}

// readLine reads one line, bounded the way a delimited record is: a line has to
// be complete before it can be parsed, so its length is what the read costs, and
// one line with no terminator would otherwise make that the whole stream.
//
// Whatever was read before the bound was passed is returned with the error, the
// way bufio does, so a caller that reports the line has something to report.
func readLine(src *bufio.Reader, limit, number int) ([]byte, error) {
	var line []byte
	for {
		chunk, err := src.ReadSlice('\n')
		line = append(line, chunk...)
		if len(line) > limit {
			return line, recordTooLongError(number, limit)
		}
		if !errors.Is(err, bufio.ErrBufferFull) {
			return line, err
		}
	}
}

// truncateLine shortens a line for an error message.
func truncateLine(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen] + "..."
}
