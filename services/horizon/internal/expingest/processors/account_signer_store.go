package processors

import (
	"github.com/stellar/go/services/horizon/internal/db2/history"
	"github.com/stellar/go/support/errors"
	"github.com/stellar/go/xdr"
)

type AccountSignerStore struct {
	batch history.AccountSignersBatchInsertBuilder
}

func NewAccountSignerStore(dataQ history.QSigners) AccountSignerStore {
	return AccountSignerStore{dataQ.NewAccountSignersBatchInsertBuilder(maxBatchSize)}
}

func (s AccountSignerStore) Add(entryChange xdr.LedgerEntryChange) error {
	if entryChange.EntryType() != xdr.LedgerEntryTypeAccount {
		return nil
	}

	accountEntry := entryChange.MustState().Data.MustAccount()
	account := accountEntry.AccountId.Address()
	for signer, weight := range accountEntry.SignerSummary() {
		err := s.batch.Add(history.AccountSigner{
			Account: account,
			Signer:  signer,
			Weight:  weight,
		})
		if err != nil {
			return errors.Wrap(err, "Error adding row to accountSignerBatch")
		}
	}
	return nil
}

func (s AccountSignerStore) Flush() error {
	return s.batch.Exec()
}
