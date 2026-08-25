package writer

import (
	"fmt"
	"io"
	"strings"
)

// TSVRecord writes one record as a line of tab-separated fields, and reports a
// field the format cannot hold rather than writing something else.
//
// TSV has no quoting: IANA's text/tab-separated-values says a field is the
// bytes between two tabs, so there is no escape for a tab or a line break and a
// field holding one cannot be written. A CSV writer would quote such a field
// instead, and to a TSV reader a quote is data, so what came back would carry
// the quotes the writer added.
//
// An empty lineEnding writes "\n".
func TSVRecord(dst io.Writer, record []string, lineEnding string) error {
	for _, field := range record {
		if i := strings.IndexAny(field, "\t\n\r"); i >= 0 {
			return &Error{
				Kind: KindUnrepresentable,
				Msg:  fmt.Sprintf("field %q contains %q", field, field[i:i+1]),
			}
		}
	}

	if lineEnding == "" {
		lineEnding = "\n"
	}
	if _, err := io.WriteString(dst, strings.Join(record, "\t")+lineEnding); err != nil {
		return fmt.Errorf("failed to write TSV record: %w", err)
	}
	return nil
}
