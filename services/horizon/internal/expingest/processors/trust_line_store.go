package processors

import (
	"github.com/stellar/go/services/horizon/internal/db2/history"
	"github.com/stellar/go/support/errors"
	"github.com/stellar/go/xdr"
)

type TrustLineStore struct {
	batch history.TrustLinesBatchInsertBuilder
}

func NewTrustLineStore(
	dataQ history.QTrustLines,
) TrustLineStore {
	return TrustLineStore{
		batch: dataQ.NewTrustLinesBatchInsertBuilder(maxBatchSize),
	}
}

func (s TrustLineStore) Add(entryChange xdr.LedgerEntryChange) error {
	if entryChange.EntryType() != xdr.LedgerEntryTypeTrustline {
		return nil
	}

	trustline := entryChange.MustState().Data.MustTrustLine()
	err := s.batch.Add(
		trustline,
		entryChange.MustState().LastModifiedLedgerSeq,
	)
	if err != nil {
		return errors.Wrap(err, "Error adding row to trustLinesBatch")
	}

	return nil
}

func (s TrustLineStore) Flush() error {
	return s.batch.Exec()
}
