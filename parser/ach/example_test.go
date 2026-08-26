package ach_test

import (
	"fmt"
	"os"

	"github.com/nao1215/filesql/parser/ach"
)

func ExampleParseReader() {
	file, err := os.Open("testdata/ppd-debit.ach")
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	defer file.Close()

	tableSet, err := ach.ParseReader(file)
	if err != nil {
		fmt.Println("Error:", err)
		return
	}

	// One ACH file becomes several tables, one per record kind.
	fmt.Println("file_header rows:", len(tableSet.GetFileHeaderTable().Records))
	fmt.Println("batches rows:", len(tableSet.GetBatchesTable().Records))
	fmt.Println("entries rows:", len(tableSet.GetEntriesTable().Records))
	fmt.Println("addenda rows:", len(tableSet.GetAddendaTable().Records))
	fmt.Println("entry columns:", len(tableSet.GetEntriesTable().Headers))
	// Output:
	// file_header rows: 1
	// batches rows: 1
	// entries rows: 1
	// addenda rows: 0
	// entry columns: 13
}
