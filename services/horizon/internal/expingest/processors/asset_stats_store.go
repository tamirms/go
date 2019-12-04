package processors

import (
	"github.com/stellar/go/services/horizon/internal/db2/history"
	"github.com/stellar/go/support/errors"
	"github.com/stellar/go/xdr"
)

type AssetStatsStore struct {
	assetStats  AssetStatSet
	assetStatsQ history.QAssetStats
}

func NewAssetStatsStore(assetStatsQ history.QAssetStats) AssetStatsStore {
	return AssetStatsStore{
		assetStats:  AssetStatSet{},
		assetStatsQ: assetStatsQ,
	}
}

func (s AssetStatsStore) Add(entryChange xdr.LedgerEntryChange) error {
	if entryChange.EntryType() != xdr.LedgerEntryTypeTrustline {
		return nil
	}

	trustline := entryChange.MustState().Data.MustTrustLine()
	err := s.assetStats.Add(trustline)
	if err != nil {
		return errors.Wrap(err, "Error adding trustline to asset stats set")
	}

	return nil
}

func (s AssetStatsStore) Flush() error {
	return s.assetStatsQ.InsertAssetStats(s.assetStats.All(), maxBatchSize)
}
