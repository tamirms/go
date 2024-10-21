package ingest

import (
	"context"
	"fmt"
	"io"
	"runtime"
	"sort"
	"time"

	"cloud.google.com/go/bigtable"

	"github.com/stellar/go/exp/services/trade-aggregations/metrics"
	"github.com/stellar/go/exp/services/trade-aggregations/store"
	"github.com/stellar/go/historyarchive"
	"github.com/stellar/go/ingest"
	"github.com/stellar/go/ingest/ledgerbackend"
	"github.com/stellar/go/support/datastore"
	"github.com/stellar/go/support/log"
	"github.com/stellar/go/xdr"
)

const sizeThreshold = 50 * 1024 * 1024 // 50 mb
const loggingThreshold = 1000

type Config struct {
	LedgerStore                  datastore.DataStore
	Schema                       store.BigTableSchema
	IntervalConfig               IntervalConfig
	RoundingSlippageFilter       int
	BufferedStorageBackendConfig ledgerbackend.BufferedStorageBackendConfig
	BackfillAmount               time.Duration
	NetworkPassphrase            string
	HistoryArchiveURL            string
}

func Run(ctx context.Context, config Config) error {
	ctx, cancel := context.WithCancelCause(ctx)

	client, err := bigtable.NewClient(ctx, config.Schema.Project, config.Schema.Instance)
	if err != nil {
		cancel(err)
		return fmt.Errorf("error initializing bigtable client: %v", err)
	}

	bucketsSink := newSink(store.NewBigTableWriter(client, config.Schema), 20)

	ledgerRange, err := getLedgerRange(ctx, config, client)
	if err != nil {
		cancel(err)
		return fmt.Errorf("error getting ledger range: %w", err)
	}

	backend, err := ledgerbackend.NewBufferedStorageBackend(
		config.BufferedStorageBackendConfig,
		config.LedgerStore,
	)
	if err != nil {
		cancel(err)
		return fmt.Errorf("error creating buffered storage backend: %w", err)
	}

	err = backend.PrepareRange(ctx, ledgerRange)
	if err != nil {
		cancel(err)
		return fmt.Errorf("error preparing range: %w", err)
	}

	processor, err := NewTradeAggregationProcessor(config.RoundingSlippageFilter, config.IntervalConfig)
	if err != nil {
		cancel(err)
		return fmt.Errorf("error creating aggregation processor: %w", err)
	}

	fetchLedger := metrics.NewSample("backend.GetLedger", loggingThreshold, loggingThreshold)
	processLedger := metrics.NewSample("processLedger", loggingThreshold, loggingThreshold)

	for cur := ledgerRange.From(); !ledgerRange.Bounded() || cur <= ledgerRange.To(); cur++ {
		var ledger xdr.LedgerCloseMeta
		var fetchLedgerErr error
		fetchLedger.Measure(func() {
			ledger, fetchLedgerErr = backend.GetLedger(ctx, cur)
		})
		if fetchLedgerErr != nil {
			cancel(fetchLedgerErr)
			return fmt.Errorf("error fetching ledger: %w", fetchLedgerErr)
		}

		var processLedgerErr error
		processLedger.Measure(func() {
			reader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(config.NetworkPassphrase, ledger)
			if err != nil {
				processLedgerErr = fmt.Errorf("error creating ledger transaction reader: %w", err)
				return
			}

			for {
				tx, err := reader.Read()
				if err == io.EOF {
					break
				}
				if err != nil {
					processLedgerErr = fmt.Errorf("error reading transaction: %w", err)
					return
				}

				if err = processor.ProcessTransaction(ledger, tx); err != nil {
					processLedgerErr = fmt.Errorf("error processing transaction: %w", err)
					return
				}
			}
		})
		if processLedgerErr != nil {
			cancel(processLedgerErr)
			return fmt.Errorf("error processing ledger: %w", processLedgerErr)
		}

		ledgerIsRecent := time.Since(time.Unix(ledger.LedgerCloseTime(), 0).UTC()).Minutes() < 5
		if ledgerIsRecent {
			log.Infof("ingested ledger %d", cur)
			fetchLedger.Report()
			processLedger.Report()
		} else if (cur-ledgerRange.From())%loggingThreshold == 0 {
			log.Infof("ingested ledger %d", cur)
		}

		if ledgerIsRecent || processor.Size() > sizeThreshold {
			flushBuckets(ctx, cancel, processor, bucketsSink)
		}
	}

	fetchLedger.Report()
	processLedger.Report()
	flushBuckets(ctx, cancel, processor, bucketsSink)

	bucketsSink.wait()

	if err := client.Close(); err != nil {
		cancel(err)
		return fmt.Errorf("error closing bigtable client: %w", err)
	}
	if err := backend.Close(); err != nil {
		cancel(err)
		return fmt.Errorf("error closing buffered storage backend: %w", err)
	}

	return ctx.Err()
}

func getLedgerRange(ctx context.Context, config Config, client *bigtable.Client) (ledgerbackend.Range, error) {
	now := time.Now().UTC()
	startTime := RoundDown(now.Add(-config.BackfillAmount).Unix(), config.IntervalConfig.RowKeyInterval)

	lastTimestamp, ok, err := store.GetProgress(ctx, client, config.Schema)
	if err != nil {
		return ledgerbackend.Range{}, fmt.Errorf("error getting progress from bigtable: %w", err)
	}
	if ok {
		if lastTimestamp != RoundDown(lastTimestamp, config.IntervalConfig.BucketInterval) {
			return ledgerbackend.Range{}, fmt.Errorf(
				"progress timestamp: %d is not aligned with bucket interval: %d",
				lastTimestamp,
				config.IntervalConfig.BucketInterval,
			)
		}

		startTime = lastTimestamp + config.IntervalConfig.BucketInterval
	}

	archive, err := historyarchive.Connect(config.HistoryArchiveURL,
		historyarchive.ArchiveOptions{
			NetworkPassphrase: config.NetworkPassphrase,
		},
	)
	if err != nil {
		return ledgerbackend.Range{}, fmt.Errorf("could not connect to history archive: %w", err)
	}

	end, err := archive.GetLatestLedgerSequence()
	if err != nil {
		return ledgerbackend.Range{}, fmt.Errorf("could not get latest ledger sequence: %w", err)
	}

	var searchErr error
	start := uint32(sort.Search(int(end-1), func(i int) bool {
		if searchErr != nil {
			return false
		}
		seq := uint32(i + 2)
		metadata, err := config.LedgerStore.GetFileMetadata(
			ctx,
			config.LedgerStore.GetSchema().GetObjectKeyFromSequenceNumber(seq),
		)
		if err != nil {
			searchErr = fmt.Errorf("could not get file metadata: %w", err)
			return false
		}
		parsed, err := datastore.NewMetaDataFromMap(metadata)
		if err != nil {
			searchErr = fmt.Errorf("could not parse file metadata: %w", err)
			return false
		}
		closeTime := time.Unix(parsed.StartLedgerCloseTime, 0).UTC().Unix()
		return closeTime >= startTime
	}) + 2)

	return ledgerbackend.UnboundedRange(start), searchErr
}

func flushBuckets(ctx context.Context, cancel context.CancelCauseFunc, processor *TradeAggregationProcessor, bucketsSink sink) {
	buckets, size := processor.PopBuckets()
	if len(buckets) == 0 {
		return
	}
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	log.WithFields(log.F{
		"trading pairs": len(buckets),
		"buckets-size":  bToMb(uint64(size)),
		"Sys":           bToMb(m.Sys),
		"HeapAlloc":     bToMb(m.HeapAlloc),
		"HeapSys":       bToMb(m.HeapSys),
		"HeapInuse":     bToMb(m.HeapInuse),
		"HeapIdle":      bToMb(m.HeapIdle),
		"HeapReleased":  bToMb(m.HeapReleased),
		"NumGC":         m.NumGC,
	}).Info("memstats")
	bucketsSink.enqueueBuckets(ctx, cancel, buckets)
}

func bToMb(b uint64) uint64 {
	return b / 1024 / 1024
}
