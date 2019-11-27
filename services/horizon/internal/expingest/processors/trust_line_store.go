package processors

import (
	"github.com/stellar/go/services/horizon/internal/db2/history"
	"github.com/stellar/go/support/errors"
	"github.com/stellar/go/xdr"
)

type TrustLineStore struct {
	batch       history.TrustLinesBatchInsertBuilder
	assetStats  AssetStatSet
	assetStatsQ history.QAssetStats
}

func NewTrustLineStore(
	dataQ history.QTrustLines,
	assetStatsQ history.QAssetStats,
) TrustLineStore {
	return TrustLineStore{
		batch:       dataQ.NewTrustLinesBatchInsertBuilder(maxBatchSize),
		assetStats:  AssetStatSet{},
		assetStatsQ: assetStatsQ,
	}
}

func (s TrustLineStore) Add(entryChange xdr.LedgerEntryChange) error {
	if entryChange.EntryType() != xdr.LedgerEntryTypeTrustline {
		return nil
	}

	trustline := entryChange.MustState().Data.MustTrustLine()
	err := s.assetStats.Add(trustline)
	if err != nil {
		return errors.Wrap(err, "Error adding trustline to asset stats set")
	}

	err = s.batch.Add(
		trustline,
		entryChange.MustState().LastModifiedLedgerSeq,
	)
	if err != nil {
		return errors.Wrap(err, "Error adding row to trustLinesBatch")
	}

	return nil
}

func (s TrustLineStore) Flush() error {
	err := s.batch.Exec()
	if err == nil {
		err = s.assetStatsQ.InsertAssetStats(s.assetStats.All(), maxBatchSize)
	}

	return err
}
