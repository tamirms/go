package store

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"cloud.google.com/go/bigtable"
	"google.golang.org/protobuf/proto"

	"github.com/stellar/go/exp/services/trade-aggregations/tradesproto"
)

type BigTableReader struct {
	client *bigtable.Client
	schema BigTableSchema
}

type Query struct {
	AssetPair  string
	Start      int64
	End        int64
	Limit      int64
	Descending bool
}

func NewBigTableReader(client *bigtable.Client, schema BigTableSchema) BigTableReader {
	return BigTableReader{client: client, schema: schema}
}

func (r *BigTableReader) GetRows(ctx context.Context, q Query, f func(*tradesproto.Bucket) bool) error {
	table := r.client.Open(r.schema.Table)
	options := []bigtable.ReadOption{bigtable.LimitRows(q.Limit)}
	rowSet := bigtable.NewClosedRange(
		r.schema.RowKeyer(q.AssetPair, q.Start),
		r.schema.RowKeyer(q.AssetPair, q.End),
	)
	if q.Descending {
		options = append(options, bigtable.ReverseScan())
	}

	var rowErr error
	err := table.ReadRows(ctx, rowSet, func(row bigtable.Row) bool {
		items := row[r.schema.ColumnFamily]
		if q.Descending {
			slices.Reverse(items)
		}
		for _, item := range items {
			var bucket tradesproto.Bucket
			if err := proto.Unmarshal(item.Value, &bucket); err != nil {
				rowErr = fmt.Errorf("failed to unmarshal bucket: %w", err)
				return false
			}

			if !f(&bucket) {
				return false
			}
		}

		return true
	}, options...)

	return errors.Join(err, rowErr)
}
