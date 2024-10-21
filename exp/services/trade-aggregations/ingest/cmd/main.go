package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/stellar/go/exp/services/trade-aggregations/ingest"
	"github.com/stellar/go/exp/services/trade-aggregations/store"
	"github.com/stellar/go/ingest/ledgerbackend"
	"github.com/stellar/go/network"
	"github.com/stellar/go/support/datastore"
	"github.com/stellar/go/support/log"
)

const minute = 60
const hour = 60 * minute
const week = 7 * 24 * hour

func main() {
	project := flag.String("project", "", "GCP project")
	instance := flag.String("instance", "", "bigtable instance name")
	ledgersBucket := flag.String("ledgers-bucket", "", "GCS bucket containing ledgers")
	progressId := flag.String("progress-id", "progress", "id uses to track ingestion progress")
	backfillDays := flag.Int("backfill-days", 1, "number of days to backfill")
	flag.Parse()

	log.SetLevel(log.InfoLevel)
	ctx, cancel := context.WithCancel(context.Background())

	ledgerStore, err := datastore.NewGCSDataStore(
		ctx,
		*ledgersBucket,
		datastore.DataStoreSchema{
			LedgersPerFile:    1,
			FilesPerPartition: 64000,
		},
	)
	if err != nil {
		log.Fatalf("Error initializing ledgerStore: %v", err)
	}

	intervalConfig := ingest.IntervalConfig{
		AggregationInterval: minute,
		BucketInterval:      hour,
		RowKeyInterval:      week,
	}
	config := ingest.Config{
		LedgerStore: ledgerStore,
		Schema: store.BigTableSchema{
			Project:      *project,
			Instance:     *instance,
			Table:        "weekly-trade-aggregations",
			ColumnFamily: "hours",
			RowKeyer: func(assetPair string, timestamp int64) string {
				return strings.Join(
					[]string{
						"trade-aggregations",
						"hours",
						assetPair,
						fmt.Sprintf("%013d", ingest.RoundDown(timestamp, intervalConfig.RowKeyInterval)),
					},
					"#",
				)
			},
			ColKeyer: func(timestamp int64) string {
				return fmt.Sprintf("%013d", timestamp)
			},
			ProgressColumnFamily: "progress",
			ProgressRowKey: strings.Join(
				[]string{
					"trade-aggregations",
					*progressId,
					"hours",
				},
				"#",
			),
			ProgressColumn: "",
		},
		RoundingSlippageFilter: 1000,
		IntervalConfig:         intervalConfig,
		BufferedStorageBackendConfig: ledgerbackend.BufferedStorageBackendConfig{
			BufferSize: 1000,
			NumWorkers: 100,
			RetryLimit: 5,
			RetryWait:  time.Second,
		},
		BackfillAmount:    time.Hour * 24 * time.Duration(*backfillDays),
		NetworkPassphrase: network.PublicNetworkPassphrase,
		HistoryArchiveURL: "http://history.stellar.org/prd/core-live/core_live_001",
	}

	// Handle OS signals to gracefully terminate the service
	sigCh := make(chan os.Signal, 1)
	defer close(sigCh)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig, ok := <-sigCh
		if ok {
			log.Infof("Received termination signal: %v", sig)
			cancel()
		}
	}()

	if err := ingest.Run(ctx, config); err != nil && !errors.Is(err, context.Canceled) {
		log.Errorf("could not run ingest service: %v", err)
	}
}
