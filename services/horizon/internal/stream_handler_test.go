package horizon

import (
	"testing"

	"github.com/stellar/go/services/horizon/internal/ledger"
)

func Test_HistoryDBLedgerSourceCurrentLedger(t *testing.T) {
	state := ledger.State{
		ExpHistoryLatest: 3,
	}

	ledgerSource := HistoryDBLedgerSource{
		SSEUpdateFrequency: 0,
		CurrentState: func() ledger.State {
			return state
		},
	}

	currentLedger := ledgerSource.CurrentLedger()

	if currentLedger != 3 {
		t.Errorf("CurrentLedger = %d, want 3", currentLedger)
	}
}

func Test_HistoryDBLedgerSourceNextLedger(t *testing.T) {
	state := ledger.State{
		ExpHistoryLatest: 3,
	}

	ledgerSource := HistoryDBLedgerSource{
		SSEUpdateFrequency: 0,
		CurrentState: func() ledger.State {
			return state
		},
	}

	ledgerChan := ledgerSource.NextLedger(0)

	nextLedger := <-ledgerChan

	if nextLedger != 3 {
		t.Errorf("NextLedger = %d, want 3", nextLedger)
	}
}
