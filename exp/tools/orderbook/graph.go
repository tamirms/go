package main

import (
	"encoding/base64"
	"sort"

	"github.com/stellar/go/support/errors"
	"github.com/stellar/go/xdr"
)

var (
	errOfferNotPresent               = errors.New("offer is not present in the order book graph")
	errorInvalidAccountID            = errors.New("account id is not Ed25519")
	errorEmptyOffers                 = errors.New("offers is empty")
	errorAssetAmountIsZero           = errors.New("current asset amount is 0")
	errorOfferPriceDeonminatorIsZero = errors.New("denominator of offer price is 0")
)

func assetAsString(asset xdr.Asset) (string, error) {
	serialized, err := asset.MarshalBinary()
	if err != nil {
		return "", errors.Wrap(
			err,
			"could not marshal offer to graph key",
		)
	}
	return base64.StdEncoding.EncodeToString(serialized), nil
}

// edgeSet maps an asset to all offers which buy that asset
// note that each key in the map is a base 64 encoding of a serialized xdr.Asset
// also, the offers are sorted by price (in terms of the buying asset)
// from cheapest to expensive
type edgeSet map[string][]xdr.OfferEntry

// add will insert the given offer into the edge set
// buyingAsset is the base 64 encoding of the serialized offer.Buying struct
func (e edgeSet) add(offer xdr.OfferEntry, buyingAsset string) {
	// offers is sorted by cheapest to most expensive price to convert buyingAsset to sellingAsset
	offers := e[buyingAsset]
	if len(offers) == 0 {
		e[buyingAsset] = []xdr.OfferEntry{offer}
		return
	}

	// find the smallest i such that Price of offers[i] >  Price of offer
	insertIndex := sort.Search(len(offers), func(i int) bool {
		// Price of offers[i] >  Price of offer
		//  <==>
		// (offers[i].Price.N / offers[i].Price.D) > (offer.Price.N / offer.Price.D)
		//  <==>
		// (offers[i].Price.N / offers[i].Price.D) * (offers[i].Price.D * offer.Price.D) > (offer.Price.N / offer.Price.D) * (offers[i].Price.D * offer.Price.D)
		//  <==>
		// offers[i].Price.N  * offer.Price.D > offer.Price.N * offers[i].Price.D
		return uint64(offers[i].Price.N)*uint64(offer.Price.D) > uint64(offer.Price.N)*uint64(offers[i].Price.D)
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
// buyingAsset is the base 64 encoding of the serialized offer.Buying struct
func (e edgeSet) remove(offerID xdr.Int64, buyingAsset string) bool {
	edges := e[buyingAsset]
	if len(edges) > 0 {
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
	return false
}

// trading pair represents two assets that can be exchanged if an order is fulfilled
type tradingPair struct {
	// buyingAsset is a base 64 encoding of a serialized xdr.Asset struct
	buyingAsset string
	// sellingAsset is a base 64 encoding of a serialized xdr.Asset struct
	sellingAsset string
}

// OrderBookGraph is an in memory graph representation of all the offers in the stellar ledger
type OrderBookGraph struct {
	// edgesForSellingAsset maps an asset to all offers which sell that asset
	// note that each key in the map is a base 64 encoding of a serialized xdr.Asset
	edgesForSellingAsset map[string]edgeSet
	// tradingPairForOffer maps an offer id to the base 64 encoded assets which are
	// being exchanged in the given offer
	tradingPairForOffer map[xdr.Int64]tradingPair
}

// NewOrderBookGraph constructs a new OrderBookGraph
func NewOrderBookGraph() OrderBookGraph {
	return OrderBookGraph{
		edgesForSellingAsset: map[string]edgeSet{},
		tradingPairForOffer:  map[xdr.Int64]tradingPair{},
	}
}

// Add inserts a given offer into the order book graph
func (graph OrderBookGraph) Add(offer xdr.OfferEntry) error {
	buyingAsset, err := assetAsString(offer.Buying)
	if err != nil {
		return errors.Wrap(err, "could not insert offer to order book graph")
	}

	sellingAsset, err := assetAsString(offer.Selling)
	if err != nil {
		return errors.Wrap(err, "could not insert offer to order book graph")
	}

	if _, contains := graph.tradingPairForOffer[offer.OfferId]; contains {
		if err := graph.Remove(offer.OfferId); err != nil {
			return errors.Wrap(err, "could not update offer in order book graph")
		}
	}

	graph.tradingPairForOffer[offer.OfferId] = tradingPair{
		buyingAsset:  buyingAsset,
		sellingAsset: sellingAsset,
	}
	if set, ok := graph.edgesForSellingAsset[sellingAsset]; !ok {
		graph.edgesForSellingAsset[sellingAsset] = edgeSet{}
		graph.edgesForSellingAsset[sellingAsset].add(offer, buyingAsset)
	} else {
		set.add(offer, buyingAsset)
	}

	return nil
}

// Remove deletes a given offer from the order book graph
func (graph OrderBookGraph) Remove(offerID xdr.Int64) error {
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
	SourceAmount      xdr.Int64
	SourceAssetString string
	SourceAsset       xdr.Asset
	InteriorNodes     []xdr.Asset
}

func (graph OrderBookGraph) findPaths(
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
			SourceAssetString: currentAssetString,
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

func (graph OrderBookGraph) FindPaths(
	maxPathLength int,
	destinationAsset xdr.Asset,
	destinationAmount xdr.Int64,
	sourceAccountID xdr.AccountId,
	sourceAssets []xdr.Asset,
	sourceAssetBalances []xdr.Int64,
	maxAssetsPerPath int,
) ([]Path, error) {
	destinationAssetString, err := assetAsString(destinationAsset)
	if err != nil {
		return nil, errors.Wrap(err, "could not serialize destination asset")
	}
	sourceAssetsMap := map[string]xdr.Int64{}
	for i, sourceAsset := range sourceAssets {
		sourceAssetString, err := assetAsString(sourceAsset)
		if err != nil {
			return nil, errors.Wrap(err, "could not serialize source asset")
		}
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

	return sortAndFilterPaths(allPaths, maxAssetsPerPath), nil
}

func accountIDEquals(a, b xdr.AccountId) (bool, error) {
	result, ok := a.GetEd25519()
	if !ok {
		return false, errorInvalidAccountID
	}
	other, ok := b.GetEd25519()
	if !ok {
		return false, errorInvalidAccountID
	}
	return result == other, nil
}

func divideCeil(numerator, denominator xdr.Int64) xdr.Int64 {
	if numerator%denominator == 0 {
		return numerator / denominator
	} else {
		return (numerator / denominator) + 1
	}
}

func consumeOffers(
	offers []xdr.OfferEntry,
	ignoreOffersFrom xdr.AccountId,
	currentAssetAmount xdr.Int64,
) (xdr.Int64, error) {
	totalConsumed := xdr.Int64(0)

	if len(offers) == 0 {
		return totalConsumed, errorEmptyOffers
	}

	if currentAssetAmount == 0 {
		return totalConsumed, errorAssetAmountIsZero
	}

	for _, offer := range offers {
		equal, err := accountIDEquals(offer.SellerId, ignoreOffersFrom)
		if err != nil {
			return -1, err
		}
		if equal {
			continue
		}
		if offer.Price.D == 0 {
			return -1, errorOfferPriceDeonminatorIsZero
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

func sortAndFilterPaths(
	allPaths []Path,
	maxPathsPerAsset int,
) []Path {
	sort.Slice(allPaths, func(i, j int) bool {
		if allPaths[i].SourceAssetString == allPaths[j].SourceAssetString {
			if allPaths[i].SourceAmount == allPaths[j].SourceAmount {
				return len(allPaths[i].InteriorNodes) < len(allPaths[j].InteriorNodes)
			}
			return allPaths[i].SourceAmount < allPaths[j].SourceAmount
		}
		return allPaths[i].SourceAssetString < allPaths[j].SourceAssetString
	})

	filtered := []Path{}
	countForAsset := 0
	for _, entry := range allPaths {
		if len(filtered) == 0 || filtered[len(filtered)-1].SourceAssetString != entry.SourceAssetString {
			countForAsset = 1
			filtered = append(filtered, entry)
		} else if countForAsset < maxPathsPerAsset {
			countForAsset++
			filtered = append(filtered, entry)
		}
	}

	return filtered
}
