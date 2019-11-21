package main

import (
	"encoding/csv"
	"fmt"
	"os"

	"github.com/stellar/go/support/errors"
	"github.com/stellar/go/xdr"
)

// csvMap maintains a mapping from ledger entry type to csv file
type csvMap struct {
	files   map[xdr.LedgerEntryType]*os.File
	writers map[xdr.LedgerEntryType]*csv.Writer
}

// newCSVMap constructs an empty csvMap instance
func newCSVMap() csvMap {
	return csvMap{
		files:   map[xdr.LedgerEntryType]*os.File{},
		writers: map[xdr.LedgerEntryType]*csv.Writer{},
	}
}

// put creates a new file with the given file name and links that file to the
// given ledger entry type
func (c csvMap) put(entryType xdr.LedgerEntryType, fileName string) error {
	if _, ok := c.files[entryType]; ok {
		return fmt.Errorf("entry type %s is already present in the file set", fileName)
	}

	file, err := os.Create(fileName)
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf("could not open file %s", fileName))
	}

	c.files[entryType] = file
	c.writers[entryType] = csv.NewWriter(file)

	return nil
}

// get returns a csv writer for the given ledger entry type if it exists in the mapping
func (c csvMap) get(entryType xdr.LedgerEntryType) (*csv.Writer, bool) {
	writer, ok := c.writers[entryType]
	return writer, ok
}

// close will close all files contained in the mapping
func (c csvMap) close() {
	for entryType, file := range c.files {
		if err := file.Close(); err != nil {
			fmt.Printf("could not close file for entry type: %s\n", entryType.String())
		}
		delete(c.files, entryType)
		delete(c.writers, entryType)
	}
}
