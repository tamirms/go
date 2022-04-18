package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"github.com/stellar/go/xdr"

	"github.com/stellar/go/historyarchive"
	"github.com/stellar/go/xdr"

	"github.com/stellar/go/ingest/ledgerbackend"
	supportlog "github.com/stellar/go/support/log"
)

var logger = supportlog.New()

func main() {
	targetUrl := flag.String("target", "gcs://horizon-archive-poc", "history archive url to write txmeta files")
	stellarCoreBinaryPath := flag.String("stellar-core-binary-path", os.Getenv("STELLAR_CORE_BINARY_PATH"), "path to the stellar core binary")
	networkPassphrase := flag.String("network-passphrase", "Test SDF Network ; September 2015", "network passphrase")
	historyArchiveUrls := flag.String("history-archive-urls", "https://history.stellar.org/prd/core-testnet/core_testnet_001", "comma-separated list of history archive urls to read from")
	flag.Parse()

	logger.SetLevel(supportlog.InfoLevel)

	params := ledgerbackend.CaptiveCoreTomlParams{
		NetworkPassphrase:  *networkPassphrase,
		HistoryArchiveURLs: strings.Split(*historyArchiveUrls, ","),
	}
	captiveCoreToml, err := ledgerbackend.NewCaptiveCoreTomlFromFile(os.Getenv("CAPTIVE_CORE_TOML_PATH"), params)
	if err != nil {
		logger.WithError(err).Fatal("Invalid captive core toml")
	}

	captiveConfig := ledgerbackend.CaptiveCoreConfig{
		BinaryPath:          *stellarCoreBinaryPath,
		NetworkPassphrase:   params.NetworkPassphrase,
		HistoryArchiveURLs:  params.HistoryArchiveURLs,
		CheckpointFrequency: 64,
		Log:                 logger.WithField("subservice", "stellar-core"),
		Toml:                captiveCoreToml,
	}
	core, err := ledgerbackend.NewCaptive(captiveConfig)
	if err != nil {
		logger.WithError(err).Fatal("Could not create captive core instance")
	}

	target, err := historyarchive.ConnectBackend(
		*targetUrl,
		historyarchive.ConnectOptions{
			Context:           context.Background(),
			NetworkPassphrase: params.NetworkPassphrase,
		},
	)
	defer target.Close()

	var latestLedger uint32
	latestLedger, err = readLatestLedger(target)
	if err != nil {
		logger.WithError(err).Fatal("could not read latest ledger")
	}

	//startTime := time.Now()
	//target := time.Unix(1639647957, 0)
	//seq, err := timeToLedger(gcsBucket, target)
	//duration := time.Now().Sub(startTime)
	//if err != nil {
	//	logger.WithError(err).WithField("duration", duration).Fatal("could not find ledger for timestamp")
	//}
	//fmt.Printf("duration %v latest %v timestampToLedger %v\n", duration, latestLedger, seq)

	nextLedger := latestLedger + 1
	if err := core.PrepareRange(context.Background(), ledgerbackend.UnboundedRange(latestLedger)); err != nil {
		logger.WithError(err).Fatalf("could not prepare unbounded range %v", nextLedger)
	}

	for {
		leddger, err := core.GetLedger(context.Background(), nextLedger)
		if err != nil {
			logger.WithError(err).Warnf("could not fetch ledger %v, will retry", nextLedger)
			time.Sleep(time.Second)
			continue
		}

		if err = writeLedger(target, leddger); err != nil {
			continue
		}

		if err = writeLatestLedger(target, nextLedger); err != nil {
			logger.WithError(err).Warnf("could not write latest ledger %v", nextLedger)
		}

		nextLedger++
	}

}

func readLatestLedger(backend historyarchive.ArchiveBackend) (uint32, error) {
	r, err := backend.GetFile("latest")
	if err == os.ErrNotExist {
		return 2, nil
	} else if err != nil {
		return 0, err
	} else {
		defer r.Close()
		var buf bytes.Buffer
		if _, err := io.Copy(&buf, r); err != nil {
			return 0, err
		}
		if parsed, err := strconv.ParseUint(buf.String(), 10, 32); err != nil {
			return 0, err
		} else {
			return uint32(parsed), nil
		}
	}
}

func timeToLedger(gcsBucket *storage.BucketHandle, timestamp time.Time) (uint32, error) {
	latest, err := readLatestLedger(gcsBucket)
	if err != nil {
		return 0, err
	}
	if latest < 3 {
		return 0, fmt.Errorf("not enough ledgers")
	}
	upper := int(latest - 2)
	backend := ledgerbackend.GCSBackend{Bucket: gcsBucket}
	var lcm xdr.LedgerCloseMeta
	count := 0
	result := sort.Search(upper, func(i int) bool {
		if err != nil {
			return false
		}
		count++
		ledger := uint32(i + 3)
		lcm, err = backend.GetLedger(context.Background(), ledger)
		if err != nil {
			return false
		}
		closeTime := time.Unix(int64(lcm.MustV0().LedgerHeader.Header.ScpValue.CloseTime), 0).UTC()
		return !closeTime.Before(timestamp)
	})
	if err != nil {
		return 0, err
	}
	if result >= upper {
		return 0, fmt.Errorf("timestamp is after latest sequence")
	}
	if result == 0 {
		lcm, err = backend.GetLedger(context.Background(), 3)
		if err != nil {
			return 0, err
		}
		count++
		closeTime := time.Unix(int64(lcm.MustV0().LedgerHeader.Header.ScpValue.CloseTime), 0).UTC()
		if closeTime.After(timestamp) {
			return 0, fmt.Errorf("timestamp is before genesis ledger")
		}
	}

	fmt.Printf("number of get leger calls %v\n", count)
	return uint32(result + 3), err
}

func writeLedger(backend historyarchive.ArchiveBackend, leddger xdr.LedgerCloseMeta) error {
	blob, err := leddger.MarshalBinary()
	if err != nil {
		logger.WithError(err).Fatalf("could not serialize ledger %v", uint64(leddger.LedgerSequence()))
	}
	err = backend.PutFile(
		"ledgers/"+strconv.FormatUint(uint64(leddger.LedgerSequence()), 10),
		ioutil.NopCloser(bytes.NewReader(blob)),
	)
	if err != nil {
		logger.WithError(err).Warnf("could not write ledger object %v, will retry", uint64(leddger.LedgerSequence()))
	}
	return err
}

func writeLatestLedger(backend historyarchive.ArchiveBackend, ledger uint32) error {
	return backend.PutFile(
		"latest",
		ioutil.NopCloser(
			bytes.NewBufferString(
				strconv.FormatUint(uint64(ledger), 10),
			),
		),
	)
}
