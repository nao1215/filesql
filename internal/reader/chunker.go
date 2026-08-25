package reader

import "github.com/nao1215/filesql/internal/infer"

// chunker collects records and hands them to emit a chunk at a time.
//
// Every format reads its rows differently and then does the same three things
// with them: fold each into the column evidence, flush when the chunk is full,
// and flush what is left at the end. The last step carries a rule that is not
// obvious and that each format had its own copy of: a read that emitted nothing
// still emits one empty chunk, so an input that is nothing but a header, or
// whose records were all skipped, makes a table with the columns the header
// names rather than no table at all.
type chunker struct {
	header   []string
	size     int
	emit     Emit
	evidence []infer.Evidence
	declared []infer.Type
	room     int

	chunk   [][]string
	nulls   [][]bool
	emitted bool
	rows    int
}

// newChunker returns a chunker for a format that infers its types, widening the
// evidence as it reads. Each chunk carries the types every row up to and
// including it requires, so the last chunk's are final.
func newChunker(header []string, opts Options, emit Emit) *chunker {
	return &chunker{
		header:   header,
		size:     chunkSizeOf(opts),
		emit:     emit,
		evidence: make([]infer.Evidence, len(header)),
	}
}

// newTypedChunker returns a chunker for a format that states its own types, so
// every chunk carries the same ones and no evidence is gathered.
func newTypedChunker(header []string, types []infer.Type, opts Options, emit Emit) *chunker {
	return &chunker{
		header:   header,
		size:     chunkSizeOf(opts),
		emit:     emit,
		declared: types,
	}
}

// reserve asks for room for n records ahead of each chunk, for a format that
// knows how many are coming. It returns the chunker so a constructor call can
// carry it.
func (c *chunker) reserve(n int) *chunker {
	c.room = n
	c.chunk = make([][]string, 0, n)
	return c
}

// add takes one record, folding it into the evidence when the format infers its
// types, and flushes the chunk if this record filled it.
func (c *chunker) add(record []string) error {
	return c.addWithNulls(record, nil)
}

// addWithNulls is add for a format that knows which of a record's cells hold SQL
// NULL rather than text. Passing nil leaves the chunk's mask nil, which is what
// a format with no null of its own means.
func (c *chunker) addWithNulls(record []string, nulls []bool) error {
	if c.evidence != nil {
		addEvidence(c.evidence, record)
	}
	c.chunk = append(c.chunk, record)
	if nulls != nil {
		if c.nulls == nil {
			c.nulls = make([][]bool, 0, c.room)
		}
		c.nulls = append(c.nulls, nulls)
	}
	if len(c.chunk) < c.size {
		return nil
	}
	return c.flush()
}

// finish flushes the records the last full chunk left behind, or one empty chunk
// when nothing has been emitted at all.
func (c *chunker) finish() error {
	if len(c.chunk) == 0 && c.emitted {
		return nil
	}
	return c.flush()
}

// flush hands the collected records over and starts a new chunk. The records are
// not reused, since emit may keep them.
func (c *chunker) flush() error {
	c.rows += len(c.chunk)
	err := c.emit(&Chunk{Header: c.header, Records: c.chunk, Types: c.types(), Nulls: c.nulls})
	c.chunk = nil
	c.nulls = nil
	if c.room > 0 {
		c.chunk = make([][]string, 0, c.room)
	}
	c.emitted = true
	return err
}

// types is what the columns require as things stand.
func (c *chunker) types() []infer.Type {
	if c.evidence == nil {
		return c.declared
	}
	return typesOf(c.evidence)
}

// addEvidence folds one record into the evidence of the columns it covers. A
// record shorter than the header leaves the columns it does not reach alone,
// which is what a missing cell means.
func addEvidence(evidence []infer.Evidence, record []string) {
	for i, value := range record {
		if i >= len(evidence) {
			return
		}
		evidence[i].Add(value)
	}
}

// typesOf is the type each column's evidence requires.
func typesOf(evidence []infer.Evidence) []infer.Type {
	types := make([]infer.Type, len(evidence))
	for i := range evidence {
		types[i] = evidence[i].Type()
	}
	return types
}
