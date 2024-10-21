package store

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"time"

	"cloud.google.com/go/bigtable"
)

const mutationBatchSize = 90000

type BucketEntry struct {
	AssetPair string
	Timestamp int64
	Payload   []byte
}

type BigTableSchema struct {
	Project              string
	Instance             string
	Table                string
	ColumnFamily         string
	RowKeyer             func(assetPair string, timestamp int64) string
	ColKeyer             func(timestamp int64) string
	ProgressColumnFamily string
	ProgressRowKey       string
	ProgressColumn       string
}
type BigTableWriter struct {
	client *bigtable.Client
	schema BigTableSchema
}

func NewBigTableWriter(client *bigtable.Client, schema BigTableSchema) BigTableWriter {
	return BigTableWriter{client: client, schema: schema}
}

func (b BigTableWriter) WriteBuckets(ctx context.Context, entries []BucketEntry) error {
	if len(entries) == 0 {
		return nil
	}

	table := b.client.Open(b.schema.Table)
	sort.Slice(entries, func(i, j int) bool {
		r1 := b.schema.RowKeyer(entries[i].AssetPair, entries[i].Timestamp)
		r2 := b.schema.RowKeyer(entries[j].AssetPair, entries[j].Timestamp)
		if r1 < r2 {
			return true
		} else if r1 > r2 {
			return false
		}
		if b.schema.ColKeyer(entries[i].Timestamp) < b.schema.ColKeyer(entries[j].Timestamp) {
			return true
		}
		return false
	})
	var keys []string
	var mutations []*bigtable.Mutation
	var curKey string
	var curMutation *bigtable.Mutation

	for _, entry := range entries {
		key := b.schema.RowKeyer(entry.AssetPair, entry.Timestamp)
		if curKey != key {
			if curKey != "" {
				keys = append(keys, curKey)
				mutations = append(mutations, curMutation)
				if len(mutations) >= mutationBatchSize {
					errs, err := table.ApplyBulk(ctx, keys, mutations)
					if err != nil {
						return err
					}
					if len(errs) > 0 {
						return errors.Join(errs...)
					}
					keys = keys[:0]
					mutations = mutations[:0]
				}
			}
			curKey = key
			curMutation = bigtable.NewMutation()
		}
		curMutation.Set(
			b.schema.ColumnFamily,
			b.schema.ColKeyer(entry.Timestamp),
			bigtable.Time(time.Unix(entry.Timestamp, 0).UTC()),
			entry.Payload,
		)
	}

	keys = append(keys, curKey)
	mutations = append(mutations, curMutation)
	errs, err := table.ApplyBulk(ctx, keys, mutations)
	if err != nil {
		return err
	}
	return errors.Join(errs...)
}

func (b BigTableWriter) WriteProgress(ctx context.Context, timestamp int64) error {
	table := b.client.Open(b.schema.Table)
	mutation := bigtable.NewMutation()
	mutation.Set(b.schema.ProgressColumnFamily, b.schema.ProgressColumn, 1, []byte(fmt.Sprintf("%013d", timestamp)))
	return table.Apply(ctx, b.schema.ProgressRowKey, mutation)
}

func GetProgress(ctx context.Context, client *bigtable.Client, schema BigTableSchema) (int64, bool, error) {
	table := client.Open(schema.Table)
	row, err := table.ReadRow(ctx, schema.ProgressRowKey)
	if err != nil {
		return 0, false, fmt.Errorf("could not read row from big table %w", err)
	}
	cols := row[schema.ProgressColumnFamily]
	if len(cols) > 0 {
		if len(cols) != 1 {
			return 0, true, fmt.Errorf("expected exactly one progress column but got %d", len(cols))
		}
		val := string(cols[0].Value)
		lastTimestamp, err := strconv.ParseInt(val, 10, 64)
		if err != nil {
			return 0, true, fmt.Errorf("could not parse progress value: %s %w", val, err)
		}

		return lastTimestamp, true, nil
	}
	return 0, false, nil
}
