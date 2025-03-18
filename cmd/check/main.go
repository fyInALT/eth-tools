package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"unsafe"

	"github.com/ethereum/go-ethereum/core/state"
)

// DumpData represents the full dump in a collected format, as one large map.
type DumpData struct {
	Root     string                       `json:"root"`
	Accounts map[string]state.DumpAccount `json:"accounts"`
	Next     []byte                       `json:"next,omitempty"` // nil if no more accounts
}

// readDump reads and parses the JSON file into a DumpData struct.
func readDump(filePath string) (*DumpData, error) {
	data, err := ioutil.ReadFile(filePath)
	if err != nil {
		return nil, err
	}

	var dump DumpData
	err = json.Unmarshal(data, &dump)
	if err != nil {
		return nil, err
	}

	return &dump, nil
}

// main function to handle command line arguments and process the JSON file.
func main() {
	filePath := flag.String("file", "", "Path to the JSON file")
	flag.Parse()

	if *filePath == "" {
		fmt.Println("Please provide the path to the JSON file using -file flag.")
		os.Exit(1)
	}

	dump, err := readDump(*filePath)
	if err != nil {
		fmt.Printf("Error reading or parsing the JSON file: %v\n", err)
		os.Exit(1)
	}

	// Count the number of accounts
	accountCount := len(dump.Accounts)
	fmt.Printf("Number of accounts: %d\n", accountCount)

	// Calculate the memory size of the DumpData struct
	memSize := unsafe.Sizeof(*dump)
	fmt.Printf("Memory size of the DumpData struct: %d bytes\n", memSize)
}
