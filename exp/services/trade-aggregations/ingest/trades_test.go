package ingest

import (
	"bytes"
	"fmt"
	"math/big"
	"math/rand"
	"testing"

	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/stellar/go/exp/services/trade-aggregations/tradesproto"
)

const minutesPerWeek = 10080

func TestPBSize(t *testing.T) {
	key := fmt.Sprintf("%013d", 1)
	t.Log(key, len(key))
	bucket := &tradesproto.Bucket{}
	for i := 0; i < minutesPerWeek; i++ {
		aggregation := &tradesproto.Aggregation{
			Timestamp:     rand.Int63(),
			Count:         rand.Int63n(12),
			BaseVolume:    big.NewInt(rand.Int63()).Bytes(),
			CounterVolume: big.NewInt(rand.Int63()).Bytes(),
			High: &tradesproto.Fraction{
				Numerator:   rand.Int63n(10000),
				Denominator: rand.Int63n(10000),
			},
			Low: &tradesproto.Fraction{
				Numerator:   rand.Int63n(10000),
				Denominator: rand.Int63n(10000),
			},
			Open: &tradesproto.Fraction{
				Numerator:   rand.Int63n(10000),
				Denominator: rand.Int63n(10000),
			},
			Close: &tradesproto.Fraction{
				Numerator:   rand.Int63n(10000),
				Denominator: rand.Int63n(10000),
			},
		}
		bucket.Aggregations = append(bucket.Aggregations, aggregation)
	}
	out, err := proto.Marshal(bucket)
	require.NoError(t, err)
	t.Log(len(out))

	var compressed bytes.Buffer
	writer, err := zstd.NewWriter(&compressed)
	require.NoError(t, err)
	_, err = writer.Write(out)
	require.NoError(t, err)
	require.NoError(t, writer.Close())
	t.Log(compressed.Len())
}
