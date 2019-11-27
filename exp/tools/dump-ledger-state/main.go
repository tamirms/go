package main

import (
	"context"
	"flag"
	"fmt"
	stdio "io"
	"os"
	"strconv"
	"time"

	"github.com/stellar/go/support/errors"

	"github.com/stellar/go/exp/ingest/io"
	"github.com/stellar/go/exp/ingest/processors"
	"github.com/stellar/go/support/historyarchive"
	"github.com/stellar/go/xdr"
)

// I'm not sure if this wrapper is necessary but every ingestion processor
// seems to have this if statement:
//
// if entryChange.Type != xdr.LedgerEntryChangeTypeLedgerEntryState {
// 	return entryChange, errors.New("unexpected entry change type")
// }
//
// It seems strange that a StateReader would return anything other than a
// xdr.LedgerEntryChangeTypeLedgerEntryState entry since that is what the
// name "StateReader" implies.
//
// Maybe this check should be moved to io.SingleLedgerStateReader?
type onlyState struct {
	io.StateReader
}

func (r onlyState) Read() (xdr.LedgerEntryChange, error) {
	entryChange, err := r.StateReader.Read()
	if err != nil {
		return entryChange, err
	}

	if entryChange.Type != xdr.LedgerEntryChangeTypeLedgerEntryState {
		return entryChange, errors.New("unexpected entry change type")
	}

	return entryChange, nil
}

func dumpStateToCSV(reader io.StateReader, writers csvMap) error {
	reader = onlyState{reader}
	var entryChange xdr.LedgerEntryChange
	var err error

	for {
		entryChange, err = reader.Read()
		if err != nil {
			break
		}

		writer, ok := writers.get(entryChange.EntryType())
		if !ok {
			continue
		}

		if err = processors.EntryChangeStateToCSV(writer, entryChange); err != nil {
			return err
		}
	}

	if err == stdio.EOF {
		return nil
	}
	return err
}

func main() {
	startTime := time.Now()
	testnet := flag.Bool("testnet", false, "connect to the Stellar test network")
	flag.Parse()

	archive, err := archive(*testnet)
	if err != nil {
		panic(err)
	}
	ledgerSequence, err := strconv.Atoi(os.Getenv("LATEST_LEDGER"))
	if err != nil {
		panic(err)
	}

	reader, err := io.NewStateReaderForLedger(
		context.Background(),
		archive,
		&io.MemoryTempSet{},
		uint32(ledgerSequence),
	)
	if err != nil {
		panic(err)
	}
	defer reader.Close()

	writers := newCSVMap()
	defer writers.close()

	for entryType, fileName := range map[xdr.LedgerEntryType]string{
		xdr.LedgerEntryTypeAccount:   "./accounts.csv",
		xdr.LedgerEntryTypeData:      "./accountdata.csv",
		xdr.LedgerEntryTypeOffer:     "./offers.csv",
		xdr.LedgerEntryTypeTrustline: "./trustlines.csv",
	} {
		if err := writers.put(entryType, fileName); err != nil {
			panic(err)
		}
	}

	if err := dumpStateToCSV(reader, writers); err != nil {
		panic(err)
	} else {
		elapsedTime := time.Since(startTime)
		fmt.Printf("Session finished without errors: %v\n", elapsedTime)
	}

	// Remove sorted files
	sortedFiles := []string{
		"./accounts_sorted.csv",
		"./accountdata_sorted.csv",
		"./offers_sorted.csv",
		"./trustlines_sorted.csv",
	}
	for _, file := range sortedFiles {
		// Ignore not exist errors
		if err := os.Remove(file); err != nil && !os.IsNotExist(err) {
			panic(err)
		}
	}
}

func archive(testnet bool) (*historyarchive.Archive, error) {
	if testnet {
		return historyarchive.Connect(
			"https://history.stellar.org/prd/core-testnet/core_testnet_001",
			historyarchive.ConnectOptions{},
		)
	}

	return historyarchive.Connect(
		fmt.Sprintf("https://history.stellar.org/prd/core-live/core_live_001/"),
		historyarchive.ConnectOptions{},
	)
}
