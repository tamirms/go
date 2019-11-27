package processors

import (
	"github.com/stellar/go/services/horizon/internal/db2/history"
	"github.com/stellar/go/support/errors"
	"github.com/stellar/go/xdr"
)

type AccountDataStore struct {
	batch history.AccountDataBatchInsertBuilder
}

func NewAccountDataStore(dataQ history.QData) AccountDataStore {
	return AccountDataStore{dataQ.NewAccountDataBatchInsertBuilder(maxBatchSize)}
}

func (s AccountDataStore) Add(entryChange xdr.LedgerEntryChange) error {
	if entryChange.EntryType() != xdr.LedgerEntryTypeData {
		return nil
	}

	err := s.batch.Add(
		entryChange.MustState().Data.MustData(),
		entryChange.MustState().LastModifiedLedgerSeq,
	)
	if err != nil {
		return errors.Wrap(err, "Error adding row to accountSignerBatch")
	}

	return nil
}

func (s AccountDataStore) Flush() error {
	return s.batch.Exec()
}
