package ingest

import (
	"fmt"
	"math"
	"math/big"
	"time"

	"github.com/guregu/null"
	"google.golang.org/protobuf/proto"

	"github.com/stellar/go/exp/orderbook"
	"github.com/stellar/go/exp/services/trade-aggregations/store"
	"github.com/stellar/go/exp/services/trade-aggregations/tradesproto"
	"github.com/stellar/go/ingest"
	"github.com/stellar/go/support/errors"
	"github.com/stellar/go/xdr"
)

// run `protoc --go_out=. trades.proto` to generate go protobuf bindings

type tradeAggregation struct {
	count         int
	baseVolume    *big.Int
	counterVolume *big.Int
	High          [2]int64
	Low           [2]int64
	Open          [2]int64
	Close         [2]int64
}

func CmpFrac(a, b [2]int64) int {
	return big.NewRat(a[0], a[1]).Cmp(big.NewRat(b[0], b[1]))
}

type IntervalConfig struct {
	AggregationInterval int64
	BucketInterval      int64
	RowKeyInterval      int64
}

type TradeAggregationProcessor struct {
	intervalConfig         IntervalConfig
	roundingSlippageFilter int

	curAggregationTimestamp int64
	curBucketTimestamp      int64
	size                    int64

	curAggregation map[string]*tradeAggregation
	curBucket      map[string]*tradesproto.Bucket
	buckets        []store.BucketEntry
}

func NewTradeAggregationProcessor(roundingSlippageFilter int, intervalConfig IntervalConfig) (*TradeAggregationProcessor, error) {
	if roundingSlippageFilter <= 0 {
		return nil, fmt.Errorf("invalid RoundingSlippageFilter: %d", roundingSlippageFilter)
	}
	if intervalConfig.AggregationInterval <= 0 {
		return nil, fmt.Errorf("invalid AggregationInterval: %d", intervalConfig.AggregationInterval)
	}
	if intervalConfig.BucketInterval < intervalConfig.AggregationInterval {
		return nil, fmt.Errorf(
			"BucketInterval: %d is less than AggregationInterval: %d",
			intervalConfig.BucketInterval,
			intervalConfig.AggregationInterval,
		)
	}
	return &TradeAggregationProcessor{
		intervalConfig:         intervalConfig,
		roundingSlippageFilter: roundingSlippageFilter,
		curAggregation:         map[string]*tradeAggregation{},
		curBucket:              map[string]*tradesproto.Bucket{},
	}, nil
}

func RoundDown(timestamp, interval int64) int64 {
	return timestamp - (timestamp % interval)
}

// ProcessTransaction process the given transaction
func (p *TradeAggregationProcessor) ProcessTransaction(lcm xdr.LedgerCloseMeta, transaction ingest.LedgerTransaction) error {
	closeTime := time.Unix(lcm.LedgerCloseTime(), 0).UTC().Unix()
	aggregationAligned := RoundDown(closeTime, p.intervalConfig.AggregationInterval)
	bucketAligned := RoundDown(closeTime, p.intervalConfig.BucketInterval)

	if aggregationAligned < p.curAggregationTimestamp {
		return fmt.Errorf("ledger timestamp %d is decreasing %d", aggregationAligned, p.curAggregationTimestamp)
	} else if aggregationAligned > p.curAggregationTimestamp {
		for key, aggregation := range p.curAggregation {
			bucket, ok := p.curBucket[key]
			if !ok {
				bucket = &tradesproto.Bucket{}
				p.curBucket[key] = bucket
			}
			bucket.Aggregations = append(bucket.Aggregations, &tradesproto.Aggregation{
				Timestamp:     p.curAggregationTimestamp,
				Count:         int64(aggregation.count),
				BaseVolume:    aggregation.baseVolume.Bytes(),
				CounterVolume: aggregation.counterVolume.Bytes(),
				High: &tradesproto.Fraction{
					Numerator:   aggregation.High[0],
					Denominator: aggregation.High[1],
				},
				Low: &tradesproto.Fraction{
					Numerator:   aggregation.Low[0],
					Denominator: aggregation.Low[1],
				},
				Open: &tradesproto.Fraction{
					Numerator:   aggregation.Open[0],
					Denominator: aggregation.Open[1],
				},
				Close: &tradesproto.Fraction{
					Numerator:   aggregation.Close[0],
					Denominator: aggregation.Close[1],
				},
			})
		}
		p.curAggregation = map[string]*tradeAggregation{}
		p.curAggregationTimestamp = aggregationAligned

		if bucketAligned > p.curBucketTimestamp {
			for assetPair, bucket := range p.curBucket {
				out, err := proto.Marshal(bucket)
				if err != nil {
					return err
				}
				p.buckets = append(p.buckets, store.BucketEntry{
					AssetPair: assetPair,
					Timestamp: p.curBucketTimestamp,
					Payload:   out,
				})
				p.size += int64(len(out))
			}
			p.curBucket = map[string]*tradesproto.Bucket{}
			p.curBucketTimestamp = bucketAligned
		}
	}

	if err := p.extractTrades(transaction); err != nil {
		return err
	}

	return nil
}

func (p *TradeAggregationProcessor) Size() int64 {
	return p.size
}

func (p *TradeAggregationProcessor) PopBuckets() ([]store.BucketEntry, int64) {
	results := p.buckets
	size := p.size
	p.buckets = []store.BucketEntry{}
	p.size = 0
	return results, size
}

func (p *TradeAggregationProcessor) findTradeSellPrice(
	transaction ingest.LedgerTransaction,
	opidx int,
	trade xdr.ClaimAtom,
) (int64, int64, error) {
	if trade.Type == xdr.ClaimAtomTypeClaimAtomTypeLiquidityPool {
		return int64(trade.AmountBought()), int64(trade.AmountSold()), nil
	}

	key := xdr.LedgerKey{}
	if err := key.SetOffer(trade.SellerId(), uint64(trade.OfferId())); err != nil {
		return 0, 0, errors.Wrap(err, "Could not create offer ledger key")
	}

	change, err := p.findOperationChange(transaction, opidx, key)
	if err != nil {
		return 0, 0, errors.Wrap(err, "could not find change for trade offer")
	}

	return int64(change.Pre.Data.Offer.Price.N), int64(change.Pre.Data.Offer.Price.D), nil
}

func (p *TradeAggregationProcessor) findOperationChange(tx ingest.LedgerTransaction, opidx int, key xdr.LedgerKey) (ingest.Change, error) {
	changes, err := tx.GetOperationChanges(uint32(opidx))
	if err != nil {
		return ingest.Change{}, errors.Wrap(err, "could not determine changes for operation")
	}

	var change ingest.Change
	for i := len(changes) - 1; i >= 0; i-- {
		change = changes[i]
		if change.Pre != nil {
			preKey, err := change.Pre.LedgerKey()
			if err != nil {
				return ingest.Change{}, errors.Wrap(err, "could not determine ledger key for change")
			}
			if key.Equals(preKey) {
				return change, nil
			}
		}
	}
	return ingest.Change{}, errors.Errorf("could not find operation for key %v", key)
}

func (p *TradeAggregationProcessor) liquidityPoolChange(
	transaction ingest.LedgerTransaction,
	opidx int,
	trade xdr.ClaimAtom,
) (*ingest.Change, error) {
	if trade.Type != xdr.ClaimAtomTypeClaimAtomTypeLiquidityPool {
		return nil, nil
	}

	poolID := trade.LiquidityPool.LiquidityPoolId

	key := xdr.LedgerKey{}
	if err := key.SetLiquidityPool(poolID); err != nil {
		return nil, errors.Wrap(err, "Could not create liquidity pool ledger key")
	}

	change, err := p.findOperationChange(transaction, opidx, key)
	if err != nil {
		return nil, errors.Wrap(err, "could not find change for liquidity pool")
	}
	return &change, nil
}

func (p *TradeAggregationProcessor) liquidityPoolReserves(trade xdr.ClaimAtom, change *ingest.Change) (int64, int64) {
	pre := change.Pre.Data.MustLiquidityPool().Body.ConstantProduct
	a := int64(pre.ReserveA)
	b := int64(pre.ReserveB)
	if !trade.AssetSold().Equals(pre.Params.AssetA) {
		a, b = b, a
	}
	return a, b
}

func (p *TradeAggregationProcessor) roundingSlippage(
	transaction ingest.LedgerTransaction,
	opidx int,
	trade xdr.ClaimAtom,
	change *ingest.Change,
) (null.Int, error) {
	disbursedReserves, depositedReserves := p.liquidityPoolReserves(trade, change)

	pre := change.Pre.Data.MustLiquidityPool().Body.ConstantProduct

	op, found := transaction.GetOperation(uint32(opidx))
	if !found {
		return null.Int{}, errors.New("could not find operation")
	}

	amountDeposited := trade.AmountBought()
	amountDisbursed := trade.AmountSold()

	switch op.Body.Type {
	case xdr.OperationTypePathPaymentStrictReceive:
		// User specified the disbursed amount
		_, roundingSlippageBips, ok := orderbook.CalculatePoolExpectation(
			xdr.Int64(depositedReserves),
			xdr.Int64(disbursedReserves),
			amountDisbursed,
			pre.Params.Fee,
			true,
		)
		if !ok {
			// Temporary workaround for https://github.com/stellar/go/issues/4203
			// Given strict receives that would underflow here, set maximum
			// slippage so they get excluded.
			roundingSlippageBips = xdr.Int64(math.MaxInt64)
		}
		return null.IntFrom(int64(roundingSlippageBips)), nil
	case xdr.OperationTypePathPaymentStrictSend:
		// User specified the disbursed amount
		_, roundingSlippageBips, ok := orderbook.CalculatePoolPayout(
			xdr.Int64(depositedReserves),
			xdr.Int64(disbursedReserves),
			amountDeposited,
			pre.Params.Fee,
			true,
		)
		if !ok {
			// Temporary workaround for https://github.com/stellar/go/issues/4203
			// Given strict sends that would overflow here, set maximum slippage
			// so they get excluded.
			roundingSlippageBips = xdr.Int64(math.MaxInt64)
		}
		return null.IntFrom(int64(roundingSlippageBips)), nil
	default:
		return null.Int{}, fmt.Errorf("unexpected trade operation type: %v", op.Body.Type)
	}
}

func (p *TradeAggregationProcessor) findPoolFee(
	transaction ingest.LedgerTransaction,
	opidx int,
	poolID xdr.PoolId,
) (uint32, error) {
	key := xdr.LedgerKey{}
	if err := key.SetLiquidityPool(poolID); err != nil {
		return 0, errors.Wrap(err, "Could not create liquidity pool ledger key")

	}

	change, err := p.findOperationChange(transaction, opidx, key)
	if err != nil {
		return 0, errors.Wrap(err, "could not find change for liquidity pool")
	}

	return uint32(change.Pre.Data.MustLiquidityPool().Body.MustConstantProduct().Params.Fee), nil
}

func (p *TradeAggregationProcessor) extractTrades(transaction ingest.LedgerTransaction) error {
	if !transaction.Result.Successful() {
		return nil
	}

	opResults, ok := transaction.Result.OperationResults()
	if !ok {
		return errors.New("transaction has no operation results")
	}
	for opidx, op := range transaction.Envelope.Operations() {
		var trades []xdr.ClaimAtom

		switch op.Body.Type {
		case xdr.OperationTypePathPaymentStrictReceive:
			trades = opResults[opidx].MustTr().MustPathPaymentStrictReceiveResult().
				MustSuccess().
				Offers

		case xdr.OperationTypePathPaymentStrictSend:
			trades = opResults[opidx].MustTr().
				MustPathPaymentStrictSendResult().
				MustSuccess().
				Offers

		case xdr.OperationTypeManageBuyOffer:
			manageOfferResult := opResults[opidx].MustTr().MustManageBuyOfferResult().
				MustSuccess()
			trades = manageOfferResult.OffersClaimed

		case xdr.OperationTypeManageSellOffer:
			manageOfferResult := opResults[opidx].MustTr().MustManageSellOfferResult().
				MustSuccess()
			trades = manageOfferResult.OffersClaimed

		case xdr.OperationTypeCreatePassiveSellOffer:
			result := opResults[opidx].MustTr()

			// KNOWN ISSUE:  stellar-core creates results for CreatePassiveOffer operations
			// with the wrong result arm set.
			if result.Type == xdr.OperationTypeManageSellOffer {
				manageOfferResult := result.MustManageSellOfferResult().MustSuccess()
				trades = manageOfferResult.OffersClaimed
			} else {
				passiveOfferResult := result.MustCreatePassiveSellOfferResult().MustSuccess()
				trades = passiveOfferResult.OffersClaimed
			}
		}

		for _, trade := range trades {
			// stellar-core will opportunistically garbage collect invalid offers (in the
			// event that a trader spends down their balance).  These garbage collected
			// offers get emitted in the result with the amount values set to zero.
			//
			// These zeroed ClaimOfferAtom values do not represent trades, and so we
			// skip them.
			if trade.AmountBought() == 0 && trade.AmountSold() == 0 {
				continue
			}

			sellPriceN, sellPriceD, err := p.findTradeSellPrice(transaction, opidx, trade)
			if err != nil {
				return err
			}

			var roundingSlippage null.Int
			if trade.Type == xdr.ClaimAtomTypeClaimAtomTypeLiquidityPool {
				change, err := p.liquidityPoolChange(transaction, opidx, trade)
				if err != nil {
					return err
				}
				if change != nil {
					roundingSlippage, err = p.roundingSlippage(transaction, opidx, trade, change)
					if err != nil {
						return err
					}
				}
			}
			if roundingSlippage.Valid && roundingSlippage.Int64 > int64(p.roundingSlippageFilter) {
				continue
			}

			soldAsset := trade.AssetSold().StringCanonical()
			boughtAsset := trade.AssetBought().StringCanonical()
			var baseAmount, counterAmount, priceN, priceD int64
			var key string
			if soldAsset <= boughtAsset {
				key = soldAsset + "," + boughtAsset
				counterAmount = int64(trade.AmountBought())
				baseAmount = int64(trade.AmountSold())
				priceN = sellPriceN
				priceD = sellPriceD
			} else {
				key = boughtAsset + "," + soldAsset
				counterAmount = int64(trade.AmountSold())
				baseAmount = int64(trade.AmountBought())
				priceN = sellPriceD
				priceD = sellPriceN
			}

			price := [2]int64{priceN, priceD}
			cur, ok := p.curAggregation[key]
			if !ok {
				p.curAggregation[key] = &tradeAggregation{
					count:         1,
					baseVolume:    big.NewInt(baseAmount),
					counterVolume: big.NewInt(counterAmount),
					High:          price,
					Low:           price,
					Open:          price,
					Close:         price,
				}
			} else {
				cur.count++
				cur.baseVolume.Add(cur.baseVolume, big.NewInt(baseAmount))
				cur.counterVolume.Add(cur.counterVolume, big.NewInt(counterAmount))
				cur.Close = price
				if CmpFrac(price, cur.High) > 0 {
					cur.High = price
				} else if CmpFrac(price, cur.Low) < 0 {
					cur.Low = price
				}
			}
		}
	}

	return nil
}
