package processors

import (
	"github.com/stellar/go/services/horizon/internal/db2/history"
	"github.com/stellar/go/support/errors"
	"github.com/stellar/go/xdr"
)

type AccountStore struct {
	batch history.AccountsBatchInsertBuilder
}

func NewAccountStore(accountsQ history.QAccounts) AccountStore {
	return AccountStore{accountsQ.NewAccountsBatchInsertBuilder(maxBatchSize)}
}

func (s AccountStore) Add(entryChange xdr.LedgerEntryChange) error {
	if entryChange.EntryType() != xdr.LedgerEntryTypeAccount {
		return nil
	}

	err := s.batch.Add(
		entryChange.MustState().Data.MustAccount(),
		entryChange.MustState().LastModifiedLedgerSeq,
	)
	if err != nil {
		return errors.Wrap(err, "Error adding row to accountSignerBatch")
	}

	return nil
}

func (s AccountStore) Flush() error {
	return s.batch.Exec()
}
