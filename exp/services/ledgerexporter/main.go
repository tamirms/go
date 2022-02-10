package main

import (
	"bytes"
	"context"
	"fmt"
	"github.com/stellar/go/xdr"
	"io"
	"os"
	"sort"
	"strconv"
	"time"

	"cloud.google.com/go/storage"

	"github.com/stellar/go/ingest/ledgerbackend"
	supportlog "github.com/stellar/go/support/log"
)

const (
	bucket = "horizon-archive-poc"
)
var logger = supportlog.New()

func main() {
	logger.SetLevel(supportlog.InfoLevel)
	binaryPath := os.Getenv("STELLAR_CORE_BINARY_PATH")

	params := ledgerbackend.CaptiveCoreTomlParams{
		NetworkPassphrase:  "Test SDF Network ; September 2015",
		HistoryArchiveURLs: []string{"https://history.stellar.org/prd/core-testnet/core_testnet_001"},
	}
	captiveCoreToml, err := ledgerbackend.NewCaptiveCoreTomlFromFile(os.Getenv("CAPTIVE_CORE_TOML_PATH"),params)
	if err != nil {
		logger.WithError(err).Fatal("Invalid captive core toml")
	}

	captiveConfig := ledgerbackend.CaptiveCoreConfig{
		BinaryPath:          binaryPath,
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

	client, err := storage.NewClient(context.Background())
	if err != nil {
		logger.WithError(err).Fatal("Could not create GCS client")
	}
	defer client.Close()

	var latestLedger uint32
	gcsBucket := client.Bucket(bucket)
	latestLedger, err = readLatestLedger(gcsBucket)
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

		if err = writeLedger(gcsBucket, leddger); err != nil {
			continue
		}

		if err = writeLatestLedger(gcsBucket, nextLedger); err != nil {
			logger.WithError(err).Warnf("could not write latest ledger %v", nextLedger)
		}

		nextLedger++
	}

}

func readLatestLedger(gcsBucket *storage.BucketHandle) (uint32, error) {
	r, err := gcsBucket.Object("latest").NewReader(context.Background())
	if err == storage.ErrObjectNotExist {
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
	upper := int(latest-2)
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
	return uint32(result+3), err
}

func writeLedger(gcsBucket *storage.BucketHandle, leddger xdr.LedgerCloseMeta) error {
	writer := gcsBucket.Object("ledgers/" + strconv.FormatUint(uint64(leddger.LedgerSequence()), 10)).NewWriter(context.Background())
	blob, err := leddger.MarshalBinary()
	if err != nil {
		logger.WithError(err).Fatalf("could not serialize ledger %v", uint64(leddger.LedgerSequence()))
	}
	if _, err = io.Copy(writer, bytes.NewReader(blob)); err != nil {
		logger.WithError(err).Warnf("could not write ledger object %v, will retry", uint64(leddger.LedgerSequence()))
		return err
	}
	if err = writer.Close(); err != nil {
		logger.WithError(err).Warnf("could not close ledger object %v, will retry", uint64(leddger.LedgerSequence()))
		return err
	}
	return nil
}

func writeLatestLedger(gcsBucket *storage.BucketHandle, ledger uint32) error {
	w := gcsBucket.Object("latest").NewWriter(context.Background())
	if _, err := io.Copy(w, bytes.NewBufferString(strconv.FormatUint(uint64(ledger), 10))); err != nil {
		return nil
	}
	return w.Close()
}
