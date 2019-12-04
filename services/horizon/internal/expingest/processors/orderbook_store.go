package processors

import (
	"github.com/stellar/go/exp/orderbook"
	"github.com/stellar/go/xdr"
)

type OrderbookStore struct {
	orderbookGraph *orderbook.OrderBookGraph
}

func NewOrderbookStore(orderbookGraph *orderbook.OrderBookGraph) OrderbookStore {
	return OrderbookStore{orderbookGraph}
}

func (s OrderbookStore) Add(entryChange xdr.LedgerEntryChange) error {
	if entryChange.EntryType() != xdr.LedgerEntryTypeOffer {
		return nil
	}

	offer := entryChange.MustState().Data.MustOffer()
	s.orderbookGraph.AddOffer(offer)

	return nil
}

func (s OrderbookStore) Flush() error {
	return nil
}
