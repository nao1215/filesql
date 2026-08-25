package wire_test

import (
	"fmt"
	"os"

	moovwire "github.com/moov-io/wire"
	"github.com/nao1215/filesql/parser/wire"
)

func ExampleParseReader() {
	file, err := os.Open("testdata/fedWireMessage-CustomerTransfer.fed")
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	defer file.Close()

	tableSet, err := wire.ParseReader(file)
	if err != nil {
		fmt.Println("Error:", err)
		return
	}

	// A Fedwire message is one row of one wide table rather than a set of
	// tables: every tag the format defines is a column, present or not.
	message := tableSet.GetMessageTable()
	fmt.Println("rows:", len(message.Records))
	fmt.Println("columns:", len(message.Headers))
	fmt.Println("first columns:", message.Headers[:3])
	// Output:
	// rows: 1
	// columns: 326
	// first columns: [sender_supplied_format_version sender_supplied_user_request_correlation sender_supplied_test_production_code]
}

func ExampleFromFile() {
	// FromFile is ParseReader for a caller who already has the moov-io value,
	// having read or built it themselves.
	file, err := os.Open("testdata/fedWireMessage-CustomerTransfer.fed")
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	defer file.Close()

	parsed, err := moovwire.NewReader(file).Read()
	if err != nil {
		fmt.Println("Error:", err)
		return
	}

	tableSet := wire.FromFile(&parsed)

	fmt.Println("rows:", len(tableSet.GetMessageTable().Records))
	// Output:
	// rows: 1
}
