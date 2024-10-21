package ingest

import (
	"context"
	"fmt"

	"github.com/stellar/go/exp/services/trade-aggregations/metrics"
	"github.com/stellar/go/exp/services/trade-aggregations/store"
)

type sink struct {
	writer              store.BigTableWriter
	bucketWritersGroup  workerGroup
	bucketWriteDuration *metrics.Sample
	progress            *progressMarker
}

func newSink(writer store.BigTableWriter, maxParallelWrites int) sink {
	return sink{
		writer:              writer,
		bucketWritersGroup:  newWorkerGroup(maxParallelWrites),
		bucketWriteDuration: metrics.NewSample("writeBuckets", 100, 1),
		progress:            newProgressMarker(writer),
	}
}

func maxTimestamp(buckets []store.BucketEntry) int64 {
	var lastTimestamp int64
	for _, entry := range buckets {
		if entry.Timestamp > lastTimestamp {
			lastTimestamp = entry.Timestamp
		}
	}
	return lastTimestamp
}

func (s sink) enqueueBuckets(ctx context.Context, onError func(error), buckets []store.BucketEntry) {
	s.progress.markInProgress(maxTimestamp(buckets))
	s.bucketWritersGroup.run(func() {
		var err error
		s.bucketWriteDuration.Measure(func() {
			err = s.writer.WriteBuckets(ctx, buckets)
		})
		if err != nil {
			onError(fmt.Errorf("could not write buckets: %w", err))
			return
		}
		s.progress.markCompleted(ctx, onError, maxTimestamp(buckets))
	})
}

func (s sink) wait() {
	s.bucketWritersGroup.wait()
	s.progress.wait()
}
