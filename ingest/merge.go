package ingest

import (
	"fmt"

	"github.com/stellar/go/xdr"
)

func mergeLCMV0(merged, ledger xdr.LedgerCloseMeta) xdr.LedgerCloseMeta {
	if ledger.V0.LedgerHeader.Header.LedgerSeq > merged.V0.LedgerHeader.Header.LedgerSeq {
		merged.V0.LedgerHeader = ledger.V0.LedgerHeader
	}
	merged.V0.ScpInfo = append(merged.V0.ScpInfo, ledger.V0.ScpInfo...)
	merged.V0.UpgradesProcessing = append(merged.V0.UpgradesProcessing, ledger.V0.UpgradesProcessing...)
	merged.V0.TxProcessing = append(merged.V0.TxProcessing, ledger.V0.TxProcessing...)
	merged.V0.TxSet.Txs = append(merged.V0.TxSet.Txs, ledger.V0.TxSet.Txs...)
	return merged
}

func mergeLCMV1(merged, ledger xdr.LedgerCloseMeta) xdr.LedgerCloseMeta {
	if ledger.V1.LedgerHeader.Header.LedgerSeq > merged.V1.LedgerHeader.Header.LedgerSeq {
		merged.V1.LedgerHeader = ledger.V1.LedgerHeader
	}
	merged.V1.EvictedTemporaryLedgerKeys = append(merged.V1.EvictedTemporaryLedgerKeys, ledger.V1.EvictedTemporaryLedgerKeys...)
	merged.V1.EvictedPersistentLedgerEntries = append(merged.V1.EvictedPersistentLedgerEntries, ledger.V1.EvictedPersistentLedgerEntries...)
	merged.V1.TxProcessing = append(merged.V1.TxProcessing, ledger.V1.TxProcessing...)
	merged.V1.ScpInfo = append(merged.V1.ScpInfo, ledger.V1.ScpInfo...)
	merged.V1.TxSet.V1TxSet.Phases = append(merged.V1.TxSet.V1TxSet.Phases, ledger.V1.TxSet.V1TxSet.Phases...)
	merged.V1.UpgradesProcessing = append(merged.V1.UpgradesProcessing, ledger.V1.UpgradesProcessing...)
	return merged
}

// MergeLedgers merges all the given ledgers into one and returns the result
func MergeLedgers(ledgers ...xdr.LedgerCloseMeta) (xdr.LedgerCloseMeta, error) {
	var merged xdr.LedgerCloseMeta
	if len(ledgers) == 0 {
		return merged, fmt.Errorf("input list is empty")
	}
	serialized, err := ledgers[0].MarshalBinary()
	if err != nil {
		return merged, fmt.Errorf("failed to marshall ledger: %q", err)
	}
	if err = merged.UnmarshalBinary(serialized); err != nil {
		return merged, fmt.Errorf("failed to unmarshal ledger: %q", err)
	}
	for _, ledger := range ledgers[1:] {
		if ledger.V != merged.V {
			return ledger, fmt.Errorf("mismatch in ledger versions")
		}
		switch ledger.V {
		case 0:
			merged = mergeLCMV0(merged, ledger)
		case 1:
			merged = mergeLCMV1(merged, ledger)
		default:
			return ledger, fmt.Errorf("unsupported ledger version: %d", ledger.V)
		}
	}

	serialized, err = merged.MarshalBinary()
	if err != nil {
		return merged, fmt.Errorf("failed to marshall ledger: %q", err)
	}
	if err = merged.UnmarshalBinary(serialized); err != nil {
		return merged, fmt.Errorf("failed to unmarshal ledger: %q", err)
	}

	return merged, nil
}
