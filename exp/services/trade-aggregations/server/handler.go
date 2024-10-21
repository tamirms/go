package server

import (
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"
	gTime "time"

	"golang.org/x/exp/slices"

	"github.com/stellar/go/amount"
	"github.com/stellar/go/exp/services/trade-aggregations/ingest"
	"github.com/stellar/go/exp/services/trade-aggregations/store"
	"github.com/stellar/go/exp/services/trade-aggregations/tradesproto"
	"github.com/stellar/go/price"
	protocol "github.com/stellar/go/protocols/horizon"
	"github.com/stellar/go/support/log"
	"github.com/stellar/go/support/render/problem"
	"github.com/stellar/go/support/time"
	"github.com/stellar/go/xdr"
)

const yearMs = 60 * 60 * 24 * 365 * 1000

var AllowedResolutions = map[gTime.Duration]struct{}{
	gTime.Minute:        {}, //1 minute
	gTime.Minute * 5:    {}, //5 minutes
	gTime.Minute * 15:   {}, //15 minutes
	gTime.Hour:          {}, //1 hour
	gTime.Hour * 24:     {}, //day
	gTime.Hour * 24 * 7: {}, //week
}

// TradeAssetsQueryParams represents the base and counter assets on trade related end-points.
type TradeAssetsQueryParams struct {
	BaseAssetType      string `schema:"base_asset_type" valid:"assetType,optional"`
	BaseAssetIssuer    string `schema:"base_asset_issuer" valid:"accountID,optional"`
	BaseAssetCode      string `schema:"base_asset_code" valid:"-"`
	CounterAssetType   string `schema:"counter_asset_type" valid:"assetType,optional"`
	CounterAssetIssuer string `schema:"counter_asset_issuer" valid:"accountID,optional"`
	CounterAssetCode   string `schema:"counter_asset_code" valid:"-"`
}

// Base returns an xdr.Asset representing the base side of the trade.
func (q TradeAssetsQueryParams) Base() (*xdr.Asset, error) {
	if len(q.BaseAssetType) == 0 {
		return nil, nil
	}

	base, err := xdr.BuildAsset(
		q.BaseAssetType,
		q.BaseAssetIssuer,
		q.BaseAssetCode,
	)

	if err != nil {
		return nil, problem.MakeInvalidFieldProblem(
			"base_asset",
			fmt.Errorf("invalid base_asset: %s", err.Error()),
		)
	}

	return &base, nil
}

// Counter returns an *xdr.Asset representing the counter asset side of the trade.
func (q TradeAssetsQueryParams) Counter() (*xdr.Asset, error) {
	if len(q.CounterAssetType) == 0 {
		return nil, nil
	}

	counter, err := xdr.BuildAsset(
		q.CounterAssetType,
		q.CounterAssetIssuer,
		q.CounterAssetCode,
	)

	if err != nil {
		return nil, problem.MakeInvalidFieldProblem(
			"counter_asset",
			fmt.Errorf("invalid counter_asset: %s", err.Error()),
		)
	}

	return &counter, nil
}

// TradeAggregationsQuery query struct for trade_aggregations end-point
type TradeAggregationsQuery struct {
	OffsetFilter           uint64      `schema:"offset" valid:"-"`
	StartTimeFilter        time.Millis `schema:"start_time" valid:"-"`
	EndTimeFilter          time.Millis `schema:"end_time" valid:"-"`
	ResolutionFilter       uint64      `schema:"resolution" valid:"-"`
	TradeAssetsQueryParams `valid:"optional"`
}

// Validate runs validations on tradeAggregationsQuery
func (q TradeAggregationsQuery) Validate() error {
	base, err := q.Base()
	if err != nil {
		return err
	}
	if base == nil {
		return problem.MakeInvalidFieldProblem(
			"base_asset_type",
			fmt.Errorf("Missing required field"),
		)
	}
	counter, err := q.Counter()
	if err != nil {
		return err
	}
	if counter == nil {
		return problem.MakeInvalidFieldProblem(
			"counter_asset_type",
			fmt.Errorf("Missing required field"),
		)
	}

	//check if resolution is legal
	resolutionDuration := gTime.Duration(q.ResolutionFilter) * gTime.Millisecond
	if _, ok := AllowedResolutions[resolutionDuration]; !ok {
		return problem.MakeInvalidFieldProblem(
			"resolution",
			fmt.Errorf("illegal or missing resolution. "+
				"allowed resolutions are: 1 minute (60000), 5 minutes (300000), 15 minutes (900000), 1 hour (3600000), "+
				"1 day (86400000) and 1 week (604800000)"),
		)
	}
	// check if offset is legal
	offsetDuration := gTime.Duration(q.OffsetFilter) * gTime.Millisecond
	if offsetDuration%gTime.Hour != 0 || offsetDuration >= gTime.Hour*24 || offsetDuration > resolutionDuration {
		return problem.MakeInvalidFieldProblem(
			"offset",
			fmt.Errorf("illegal or missing offset. offset must be a multiple of an"+
				" hour, less than or equal to the resolution, and less than 24 hours"),
		)
	}

	return nil
}

type Handler struct {
	intervalConfig ingest.IntervalConfig
	reader         store.BigTableReader
}

func NewHandler(intervalConfig ingest.IntervalConfig, reader store.BigTableReader) Handler {
	return Handler{
		intervalConfig: intervalConfig,
		reader:         reader,
	}
}

func (h Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	pq, err := GetPageQuery(r)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(err.Error()))
		return
	}

	qp := TradeAggregationsQuery{}
	if err = getParams(&qp, r); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(err.Error()))
		return
	}

	baseAsset, err := qp.Base()
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(err.Error()))
		return
	}

	counterAsset, err := qp.Counter()
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(err.Error()))
		return
	}

	//set time range if supplied
	if !qp.StartTimeFilter.IsNil() {
		offsetMillis := time.MillisFromInt64(int64(qp.OffsetFilter))
		var adjustedStartTime time.Millis
		// Round up to offset if the provided start time is less than the offset.
		if qp.StartTimeFilter < offsetMillis {
			adjustedStartTime = offsetMillis
		} else {
			adjustedStartTime = (qp.StartTimeFilter - offsetMillis).RoundUp(int64(qp.ResolutionFilter)) + offsetMillis
		}
		qp.StartTimeFilter = adjustedStartTime
	}
	if !qp.EndTimeFilter.IsNil() {
		offsetMillis := time.MillisFromInt64(int64(qp.OffsetFilter))
		var adjustedEndTime time.Millis
		// the end time isn't allowed to be less than the offset
		if qp.EndTimeFilter < offsetMillis {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte("end time is not allowed"))
			return
		} else {
			adjustedEndTime = (qp.EndTimeFilter - offsetMillis).RoundDown(int64(qp.ResolutionFilter)) + offsetMillis
		}
		qp.EndTimeFilter = adjustedEndTime
	}

	if !qp.StartTimeFilter.IsNil() && !qp.EndTimeFilter.IsNil() && qp.StartTimeFilter > qp.EndTimeFilter {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("start time is greater than end time"))
		return
	}

	if !qp.EndTimeFilter.IsNil() && qp.EndTimeFilter < qp.StartTimeFilter {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("end time is less than start time"))
		return
	}

	if qp.EndTimeFilter.IsNil() {
		qp.EndTimeFilter = (time.Now() + yearMs).RoundDown(int64(qp.ResolutionFilter)) + time.MillisFromInt64(int64(qp.OffsetFilter))
	}

	var storeQuery store.Query
	baseAssetString := baseAsset.StringCanonical()
	counterAssetString := counterAsset.StringCanonical()
	var reverse bool
	if baseAssetString > counterAssetString {
		storeQuery.AssetPair = counterAssetString + "," + baseAssetString
		reverse = true
	} else {
		storeQuery.AssetPair = baseAssetString + "," + counterAssetString
	}
	storeQuery.Descending = pq.Order == OrderDescending
	storeQuery.Limit = int64(pq.Limit)
	storeQuery.Start = int64(qp.StartTimeFilter) / 1000
	storeQuery.End = int64(qp.EndTimeFilter) / 1000
	resolution := int64(qp.ResolutionFilter) / 1000

	var result []*tradesproto.Aggregation
	var prev *tradesproto.Aggregation
	err = h.reader.GetRows(ctx, storeQuery, func(bucket *tradesproto.Bucket) bool {
		if storeQuery.Descending {
			slices.Reverse(bucket.Aggregations)
		}
		for _, cur := range bucket.Aggregations {
			if cur.Timestamp >= storeQuery.End {
				if storeQuery.Descending {
					continue
				} else {
					return false
				}
			}
			if cur.Timestamp < storeQuery.Start {
				if storeQuery.Descending {
					return false
				} else {
					continue
				}
			}
			cur.Timestamp = storeQuery.Start + ingest.RoundDown(cur.Timestamp-storeQuery.Start, resolution)
			if prev == nil {
				prev = cur
			} else if prev.Timestamp == cur.Timestamp {
				prev.Count += cur.Count
				prev.BaseVolume = sumBigInts(prev.BaseVolume, cur.BaseVolume)
				prev.CounterVolume = sumBigInts(prev.CounterVolume, cur.CounterVolume)
				if storeQuery.Descending {
					prev.Open = cur.Open
				} else {
					prev.Close = cur.Close
				}
				if ingest.CmpFrac(toFrac(prev.High), toFrac(cur.High)) < 0 {
					prev.High = cur.High
				}
				if ingest.CmpFrac(toFrac(prev.Low), toFrac(cur.Low)) > 0 {
					prev.Low = cur.Low
				}
			} else {
				result = append(result, prev)
				prev = cur
				if len(result) >= int(storeQuery.Limit) {
					return false
				}
			}
		}
		return true
	})
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
		return
	}
	if prev != nil && len(result) < int(storeQuery.Limit) {
		result = append(result, prev)
	}

	if reverse {
		for _, cur := range result {
			cur.BaseVolume, cur.CounterVolume = cur.CounterVolume, cur.BaseVolume
			cur.Low, cur.High = cur.High, cur.Low
			invert(cur.Low)
			invert(cur.High)
			invert(cur.Open)
			invert(cur.Close)
		}
	}

	response := []protocol.TradeAggregation{}
	for _, cur := range result {
		item, err := toResponse(cur)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(err.Error()))
			return
		}
		response = append(response, item)
	}

	if err := json.NewEncoder(w).Encode(response); err != nil {
		log.Errorf("error encoding response: %v", err)
	}
}

func invert(f *tradesproto.Fraction) {
	f.Numerator, f.Denominator = f.Denominator, f.Numerator
}

func toFrac(a *tradesproto.Fraction) [2]int64 {
	return [2]int64{a.Numerator, a.Denominator}
}

func sumBigInts(a, b []byte) []byte {
	bigA := (&big.Int{}).SetBytes(a)
	bigB := (&big.Int{}).SetBytes(b)
	return bigA.Add(bigA, bigB).Bytes()
}

func toResponse(
	src *tradesproto.Aggregation,
) (protocol.TradeAggregation, error) {
	var dest protocol.TradeAggregation
	var err error
	baseVolume := (&big.Int{}).SetBytes(src.BaseVolume)
	counterVolume := (&big.Int{}).SetBytes(src.CounterVolume)

	dest.Timestamp = src.Timestamp * 1000
	dest.TradeCount = src.Count
	dest.BaseVolume, err = amount.IntStringToAmount(baseVolume.String())
	if err != nil {
		return dest, err
	}
	dest.CounterVolume, err = amount.IntStringToAmount(counterVolume.String())
	if err != nil {
		return dest, err
	}
	avg, _ := (&big.Rat{}).SetFrac(counterVolume, baseVolume).Float64()
	dest.Average = price.StringFromFloat64(avg)
	dest.HighR = protocol.TradePrice{
		N: src.High.Numerator,
		D: src.High.Denominator,
	}
	dest.High = dest.HighR.String()
	dest.LowR = protocol.TradePrice{
		N: src.Low.Numerator,
		D: src.Low.Denominator,
	}
	dest.Low = dest.LowR.String()
	dest.OpenR = protocol.TradePrice{
		N: src.Open.Numerator,
		D: src.Open.Denominator,
	}
	dest.Open = dest.OpenR.String()
	dest.CloseR = protocol.TradePrice{
		N: src.Close.Numerator,
		D: src.Close.Denominator,
	}
	dest.Close = dest.CloseR.String()
	return dest, err
}
