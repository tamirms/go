package main

import (
	"io"

	ingest "github.com/stellar/go/exp/ingest/io"
	"github.com/stellar/go/support/errors"
	"github.com/stellar/go/support/log"
	"github.com/stellar/go/xdr"
)

func updateOrderbookWithStateStream(graph OrderBookGraph, reader ingest.StateReadCloser) error {
	i := 0
	for {
		i++
		entry, err := reader.Read()
		if err != nil {
			if err == io.EOF {
				break
			} else {
				return errors.Wrap(err, "Error reading from StateReadCloser")
			}
		}

		if i%1000 == 0 {
			log.WithField("numEntries", i).Info("processed")
		}

		state, ok := entry.GetState()
		if !ok {
			continue
		}
		offer, ok := state.Data.GetOffer()
		if !ok {
			continue
		}
		log.WithField("offer", offer).Info("adding offer to graph")
		if offer.Price.N == 0 {
			log.WithField("offer", offer).Warn("offer has 0 price")
		} else if err := graph.Add(offer); err != nil {
			return errors.Wrap(err, "could not add offer from StateReadCloser")
		}

	}

	return nil
}

func updateOrderbookWithLedgerEntryChanges(graph OrderBookGraph, changes xdr.LedgerEntryChanges) error {
	for _, change := range changes {
		var offer xdr.OfferEntry
		var ok bool
		switch change.Type {
		case xdr.LedgerEntryChangeTypeLedgerEntryCreated:
			offer, ok = change.Created.Data.GetOffer()
		case xdr.LedgerEntryChangeTypeLedgerEntryRemoved:
			offerKey, ok := change.Removed.GetOffer()
			if !ok {
				continue
			}
			if err := graph.Remove(offerKey.OfferId); err != nil {
				return errors.Wrap(err, "could not remove offer from ledger entry changes")
			}
			continue
		case xdr.LedgerEntryChangeTypeLedgerEntryState:
			offer, ok = change.State.Data.GetOffer()
		case xdr.LedgerEntryChangeTypeLedgerEntryUpdated:
			offer, ok = change.Updated.Data.GetOffer()
		}
		if !ok {
			continue
		}

		if err := graph.Add(offer); err != nil {
			return errors.Wrap(err, "could not add offer from ledger entry changes")
		}
	}
	return nil
}

func updateOrderbookWithLedgerStream(graph OrderBookGraph, reader ingest.LedgerReadCloser) error {
	for {
		entry, err := reader.Read()
		if err != nil {
			if err == io.EOF {
				break
			} else {
				return errors.Wrap(err, "Error reading from LedgerReadCloser")
			}
		}

		if entry.Result.Result.Result.Code != xdr.TransactionResultCodeTxSuccess {
			continue
		}

		if err := updateOrderbookWithLedgerEntryChanges(graph, entry.FeeChanges); err != nil {
			return errors.Wrap(err, "could not parse fee changes")
		}

		if v1Meta, ok := entry.Meta.GetV1(); ok {
			if err := updateOrderbookWithLedgerEntryChanges(graph, v1Meta.TxChanges); err != nil {
				return errors.Wrap(err, "could not parse tx changes")
			}
			for _, operation := range v1Meta.Operations {
				if err := updateOrderbookWithLedgerEntryChanges(graph, operation.Changes); err != nil {
					return errors.Wrap(err, "could not parse operations meta")
				}
			}
		}
	}

	return nil
}
