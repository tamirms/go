package main

import (
	"math/big"
	"sort"
	"sync"

	"github.com/stellar/go/support/errors"
	"github.com/stellar/go/xdr"
)

var (
	errOfferNotPresent             = errors.New("offer is not present in the order book graph")
	errEmptyOffers                 = errors.New("offers is empty")
	errAssetAmountIsZero           = errors.New("current asset amount is 0")
	errOfferPriceDenominatorIsZero = errors.New("denominator of offer price is 0")
	errBatchAlreadyApplied         = errors.New("cannot apply batched updates more than once")
)

const (
	_                        = iota
	addOfferOperationType    = iota
	removeOfferOperationType = iota
)

// edgeSet maps an asset to all offers which buy that asset
// note that each key in the map is obtained by calling offer.Buying.String()
// also, the offers are sorted by price (in terms of the buying asset)
// from cheapest to expensive
type edgeSet map[string][]xdr.OfferEntry

// add will insert the given offer into the edge set
func (e edgeSet) add(offer xdr.OfferEntry) {
	buyingAsset := offer.Buying.String()
	// offers is sorted by cheapest to most expensive price to convert buyingAsset to sellingAsset
	offers := e[buyingAsset]
	if len(offers) == 0 {
		e[buyingAsset] = []xdr.OfferEntry{offer}
		return
	}

	// find the smallest i such that Price of offers[i] >  Price of offer
	insertIndex := sort.Search(len(offers), func(i int) bool {
		return big.NewRat(int64(offers[i].Price.N), int64(offers[i].Price.D)).
			Cmp(big.NewRat(int64(offer.Price.N), int64(offer.Price.D))) > 0
	})

	offers = append(offers, offer)
	last := len(offers) - 1
	for insertIndex < last {
		offers[insertIndex], offers[last] = offers[last], offers[insertIndex]
		insertIndex++
	}
	e[buyingAsset] = offers
}

// remove will delete the given offer from the edge set
// buyingAsset is obtained by calling offer.Buying.String()
func (e edgeSet) remove(offerID xdr.Int64, buyingAsset string) bool {
	edges := e[buyingAsset]
	if len(edges) == 0 {
		return false
	}
	contains := false

	for i := 0; i < len(edges); i++ {
		if edges[i].OfferId == offerID {
			contains = true
			for j := i + 1; j < len(edges); j++ {
				edges[i] = edges[j]
				i++
			}
			edges = edges[0 : len(edges)-1]
			break
		}
	}
	if contains {
		if len(edges) == 0 {
			delete(e, buyingAsset)
		} else {
			e[buyingAsset] = edges
		}
	}

	return contains
}

// BatchedUpdates is an interface for applying multiple
// operations on an order book
type BatchedUpdates interface {
	// AddOffer will queue an operation to add the given offer to the order book
	AddOffer(offer xdr.OfferEntry) BatchedUpdates
	// AddOffer will queue an operation to remove the given offer from the order book
	RemoveOffer(offerID xdr.Int64) BatchedUpdates
	// Apply will attempt to apply all the updates in the batch to the order book
	Apply() error
}

type orderBookOperation struct {
	operationType int
	offerID       xdr.Int64
	offer         *xdr.OfferEntry
}

type orderBookBatchedUpdates struct {
	operations []orderBookOperation
	orderbook  *OrderBookGraph
	committed  bool
}

// AddOffer will queue an operation to add the given offer to the order book
func (tx *orderBookBatchedUpdates) AddOffer(offer xdr.OfferEntry) BatchedUpdates {
	tx.operations = append(tx.operations, orderBookOperation{
		operationType: addOfferOperationType,
		offerID:       offer.OfferId,
		offer:         &offer,
	})

	return tx
}

// AddOffer will queue an operation to remove the given offer from the order book
func (tx *orderBookBatchedUpdates) RemoveOffer(offerID xdr.Int64) BatchedUpdates {
	tx.operations = append(tx.operations, orderBookOperation{
		operationType: removeOfferOperationType,
		offerID:       offerID,
	})

	return tx
}

// Apply will attempt to apply all the updates in the batch to the order book
func (tx *orderBookBatchedUpdates) Apply() error {
	tx.orderbook.lock.Lock()
	defer tx.orderbook.lock.Unlock()
	if tx.committed {
		return errBatchAlreadyApplied
	}
	tx.committed = true

	for _, operation := range tx.operations {
		switch operation.operationType {
		case addOfferOperationType:
			if err := tx.orderbook.add(*operation.offer); err != nil {
				return errors.Wrap(err, "could not apply update in batch")
			}
		case removeOfferOperationType:
			if err := tx.orderbook.remove(operation.offerID); err != nil {
				return errors.Wrap(err, "could not apply update in batch")
			}
		}
	}

	return nil
}

// trading pair represents two assets that can be exchanged if an order is fulfilled
type tradingPair struct {
	// buyingAsset is obtained by calling offer.Buying.String() where offer is an xdr.OfferEntry
	buyingAsset string
	// sellingAsset is obtained by calling offer.Selling.String() where offer is an xdr.OfferEntry
	sellingAsset string
}

// OrderBookGraph is an in memory graph representation of all the offers in the stellar ledger
type OrderBookGraph struct {
	// edgesForSellingAsset maps an asset to all offers which sell that asset
	// note that each key in the map is obtained by calling offer.Selling.String()
	// where offer is an xdr.OfferEntry
	edgesForSellingAsset map[string]edgeSet
	// tradingPairForOffer maps an offer id to the assets which are being exchanged
	// in the given offer
	tradingPairForOffer map[xdr.Int64]tradingPair
	lock                sync.RWMutex
}

// NewOrderBookGraph constructs a new OrderBookGraph
func NewOrderBookGraph() *OrderBookGraph {
	return &OrderBookGraph{
		edgesForSellingAsset: map[string]edgeSet{},
		tradingPairForOffer:  map[xdr.Int64]tradingPair{},
	}
}

// Batch creates a new batch of order book updates which can be applied
// on this graph
func (graph *OrderBookGraph) Batch() BatchedUpdates {
	return &orderBookBatchedUpdates{
		operations: []orderBookOperation{},
		committed:  false,
		orderbook:  graph,
	}
}

// add inserts a given offer into the order book graph
func (graph *OrderBookGraph) add(offer xdr.OfferEntry) error {
	buyingAsset := offer.Buying.String()
	sellingAsset := offer.Selling.String()

	if _, contains := graph.tradingPairForOffer[offer.OfferId]; contains {
		if err := graph.remove(offer.OfferId); err != nil {
			return errors.Wrap(err, "could not update offer in order book graph")
		}
	}

	graph.tradingPairForOffer[offer.OfferId] = tradingPair{
		buyingAsset:  buyingAsset,
		sellingAsset: sellingAsset,
	}
	if set, ok := graph.edgesForSellingAsset[sellingAsset]; !ok {
		graph.edgesForSellingAsset[sellingAsset] = edgeSet{}
		graph.edgesForSellingAsset[sellingAsset].add(offer)
	} else {
		set.add(offer)
	}

	return nil
}

// remove deletes a given offer from the order book graph
func (graph *OrderBookGraph) remove(offerID xdr.Int64) error {
	pair, ok := graph.tradingPairForOffer[offerID]
	if !ok {
		return errOfferNotPresent
	}

	delete(graph.tradingPairForOffer, offerID)

	if set, ok := graph.edgesForSellingAsset[pair.sellingAsset]; !ok {
		return errOfferNotPresent
	} else if !set.remove(offerID, pair.buyingAsset) {
		return errOfferNotPresent
	} else if len(set) == 0 {
		delete(graph.edgesForSellingAsset, pair.sellingAsset)
	}

	return nil
}

// Path represents a payment path from a source asset to some destination asset
type Path struct {
	SourceAmount xdr.Int64
	SourceAsset  xdr.Asset
	// sourceAssetString is included as an optimization to improve the performance
	// of sorting paths by avoiding serializing assets to strings repeatedly
	sourceAssetString string
	InteriorNodes     []xdr.Asset
	DestinationAsset  xdr.Asset
	DestinationAmount xdr.Int64
}

// findPaths performs a DFS of maxPathLength depth
// the DFS maintains the following invariants:
// no node is repeated
// no offers are consumed from the `ignoreOffersFrom` account
// each payment path must originate with an asset in `targetAssets`
// also, the required source asset amount cannot exceed the balance in `targetAssets`
func (graph *OrderBookGraph) findPaths(
	maxPathLength int,
	visited map[string]bool,
	visitedList []xdr.Asset,
	currentAssetString string,
	currentAsset xdr.Asset,
	currentAssetAmount xdr.Int64,
	ignoreOffersFrom xdr.AccountId,
	targetAssets map[string]xdr.Int64,
	paths []Path,
) ([]Path, error) {
	if currentAssetAmount <= 0 {
		return paths, nil
	}
	if visited[currentAssetString] {
		return paths, nil
	}
	if len(visitedList) > maxPathLength {
		return paths, nil
	}
	visited[currentAssetString] = true
	defer func() {
		visited[currentAssetString] = false
	}()

	updatedVisitedList := append(visitedList, currentAsset)
	if targetAssetBalance, ok := targetAssets[currentAssetString]; ok && targetAssetBalance >= currentAssetAmount {
		var interiorNodes []xdr.Asset
		if len(updatedVisitedList) > 2 {
			// reverse updatedVisitedList and skip the first and last elements
			interiorNodes = make([]xdr.Asset, len(updatedVisitedList)-2)
			position := 0
			for i := len(updatedVisitedList) - 2; i >= 1; i-- {
				interiorNodes[position] = updatedVisitedList[i]
				position++
			}
		} else {
			interiorNodes = []xdr.Asset{}
		}

		paths = append(paths, Path{
			sourceAssetString: currentAssetString,
			SourceAmount:      currentAssetAmount,
			SourceAsset:       currentAsset,
			InteriorNodes:     interiorNodes,
		})
	}

	edges, ok := graph.edgesForSellingAsset[currentAssetString]
	if !ok {
		return paths, nil
	}

	for nextAssetString, offers := range edges {
		if len(offers) == 0 {
			continue
		}
		nextAssetAmount, err := consumeOffers(offers, ignoreOffersFrom, currentAssetAmount)
		if err != nil {
			return nil, err
		}
		if nextAssetAmount <= 0 {
			continue
		}

		nextAsset := offers[0].Buying
		paths, err = graph.findPaths(
			maxPathLength,
			visited,
			updatedVisitedList,
			nextAssetString,
			nextAsset,
			nextAssetAmount,
			ignoreOffersFrom,
			targetAssets,
			paths,
		)
		if err != nil {
			return nil, err
		}
	}

	return paths, nil
}

// FindPaths returns a list of payment paths originating from a source account
// and ending with a given destinaton asset and amount.
func (graph *OrderBookGraph) FindPaths(
	maxPathLength int,
	destinationAsset xdr.Asset,
	destinationAmount xdr.Int64,
	sourceAccountID xdr.AccountId,
	sourceAssets []xdr.Asset,
	sourceAssetBalances []xdr.Int64,
	maxAssetsPerPath int,
) ([]Path, error) {
	graph.lock.RLock()
	defer graph.lock.RUnlock()

	destinationAssetString := destinationAsset.String()
	sourceAssetsMap := map[string]xdr.Int64{}
	for i, sourceAsset := range sourceAssets {
		sourceAssetString := sourceAsset.String()
		sourceAssetsMap[sourceAssetString] = sourceAssetBalances[i]
	}
	allPaths, err := graph.findPaths(
		maxPathLength,
		map[string]bool{},
		[]xdr.Asset{},
		destinationAssetString,
		destinationAsset,
		destinationAmount,
		sourceAccountID,
		sourceAssetsMap,
		[]Path{},
	)
	if err != nil {
		return nil, errors.Wrap(err, "could not determine paths")
	}

	return sortAndFilterPaths(
		allPaths,
		maxAssetsPerPath,
		destinationAsset,
		destinationAmount,
	), nil
}

func divideCeil(numerator, denominator xdr.Int64) xdr.Int64 {
	if numerator%denominator == 0 {
		return numerator / denominator
	}
	return (numerator / denominator) + 1
}

func consumeOffers(
	offers []xdr.OfferEntry,
	ignoreOffersFrom xdr.AccountId,
	currentAssetAmount xdr.Int64,
) (xdr.Int64, error) {
	totalConsumed := xdr.Int64(0)

	if len(offers) == 0 {
		return totalConsumed, errEmptyOffers
	}

	if currentAssetAmount == 0 {
		return totalConsumed, errAssetAmountIsZero
	}

	for _, offer := range offers {
		if offer.SellerId.Equals(ignoreOffersFrom) {
			continue
		}
		if offer.Price.D == 0 {
			return -1, errOfferPriceDenominatorIsZero
		}
		if offer.Amount >= currentAssetAmount {
			totalConsumed += divideCeil(currentAssetAmount*xdr.Int64(offer.Price.N), xdr.Int64(offer.Price.D))
			currentAssetAmount = 0
			break
		}
		totalConsumed += divideCeil(offer.Amount*xdr.Int64(offer.Price.N), xdr.Int64(offer.Price.D))
		currentAssetAmount -= offer.Amount
	}

	if currentAssetAmount <= 0 {
		return totalConsumed, nil
	}
	return -1, nil
}

// sortAndFilterPaths sorts the given list of paths by
// source asset, source asset amount, and path length
// also, we limit the number of paths with the same source asset to maxPathsPerAsset and
// we make sure the destination asset and destination amount is included in all paths
func sortAndFilterPaths(
	allPaths []Path,
	maxPathsPerAsset int,
	destinationAsset xdr.Asset,
	destinationAmount xdr.Int64,
) []Path {
	sort.Slice(allPaths, func(i, j int) bool {
		if allPaths[i].SourceAsset.Equals(allPaths[j].SourceAsset) {
			if allPaths[i].SourceAmount == allPaths[j].SourceAmount {
				return len(allPaths[i].InteriorNodes) < len(allPaths[j].InteriorNodes)
			}
			return allPaths[i].SourceAmount < allPaths[j].SourceAmount
		}
		return allPaths[i].sourceAssetString < allPaths[j].sourceAssetString
	})

	filtered := []Path{}
	countForAsset := 0
	for _, entry := range allPaths {
		if len(filtered) == 0 || !filtered[len(filtered)-1].SourceAsset.Equals(entry.SourceAsset) {
			countForAsset = 1
			entry.DestinationAsset = destinationAsset
			entry.DestinationAmount = destinationAmount
			filtered = append(filtered, entry)
		} else if countForAsset < maxPathsPerAsset {
			countForAsset++
			entry.DestinationAsset = destinationAsset
			entry.DestinationAmount = destinationAmount
			filtered = append(filtered, entry)
		}
	}

	return filtered
}
