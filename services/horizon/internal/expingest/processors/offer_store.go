package processors

import (
	"github.com/stellar/go/services/horizon/internal/db2/history"
	"github.com/stellar/go/support/errors"
	"github.com/stellar/go/xdr"
)

type OfferStore struct {
	batch history.OffersBatchInsertBuilder
}

func NewOfferStore(dataQ history.QOffers) OfferStore {
	return OfferStore{dataQ.NewOffersBatchInsertBuilder(maxBatchSize)}
}

func (s OfferStore) Add(entryChange xdr.LedgerEntryChange) error {
	if entryChange.EntryType() != xdr.LedgerEntryTypeOffer {
		return nil
	}

	offer := entryChange.MustState().Data.MustOffer()
	err := s.batch.Add(
		offer,
		entryChange.MustState().LastModifiedLedgerSeq,
	)
	if err != nil {
		return errors.Wrap(err, "Error adding row to offersBatch")
	}

	return nil
}

func (s OfferStore) Flush() error {
	return s.batch.Exec()
}
