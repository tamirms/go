package actions

import (
	"github.com/stellar/go/ingest"
	"github.com/stellar/go/ingest/ledgerbackend"
	horizoningest "github.com/stellar/go/services/horizon/internal/ingest"

	"github.com/stellar/go/services/horizon/internal/ingest/processors"
	"net/http"

	"github.com/stellar/go/protocols/horizon"
	"github.com/stellar/go/services/horizon/internal/context"
	"github.com/stellar/go/services/horizon/internal/db2/history"
	"github.com/stellar/go/services/horizon/internal/ledger"
	"github.com/stellar/go/services/horizon/internal/resourceadapter"
	"github.com/stellar/go/support/render/hal"
)

type GetLedgersHandler struct {
	LedgerState *ledger.State
}

func (handler GetLedgersHandler) GetResourcePage(w HeaderWriter, r *http.Request) ([]hal.Pageable, error) {
	pq, err := GetPageQuery(handler.LedgerState, r)
	if err != nil {
		return nil, err
	}

	err = validateCursorWithinHistory(handler.LedgerState, pq)
	if err != nil {
		return nil, err
	}

	historyQ, err := context.HistoryQFromRequest(r)
	if err != nil {
		return nil, err
	}

	var records []history.Ledger
	if err = historyQ.Ledgers().Page(pq).Select(r.Context(), &records); err != nil {
		return nil, err
	}

	var result []hal.Pageable
	for _, record := range records {
		var ledger horizon.Ledger
		resourceadapter.PopulateLedger(r.Context(), &ledger, record)
		if err != nil {
			return nil, err
		}
		result = append(result, ledger)
	}

	return result, nil
}

// LedgerByIDQuery query struct for the ledger/{id} endpoint
type LedgerByIDQuery struct {
	LedgerID uint32 `schema:"ledger_id" valid:"-"`
}

type GetLedgerByIDHandler struct {
	Backend ledgerbackend.LedgerBackend
	NetworkPassphrase string
}

func (handler GetLedgerByIDHandler) GetResource(w HeaderWriter, r *http.Request) (interface{}, error) {
	qp := LedgerByIDQuery{}
	err := getParams(&qp, r)
	if err != nil {
		return nil, err
	}

	historyQ, err := context.HistoryQFromRequest(r)
	if err != nil {
		return nil, err
	}
	lcm, err := handler.Backend.GetLedger(r.Context(), qp.LedgerID)
	if err != nil {
		return nil, err
	}

	transactionReader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(handler.NetworkPassphrase, lcm)
	if err != nil {
		return nil, err
	}

	processor := processors.NewLedgerProcessor(historyQ, lcm.MustV0().LedgerHeader, horizoningest.CurrentVersion)
	if err := processors.StreamLedgerTransactions(r.Context(), processor, transactionReader); err != nil {
		return nil, err
	}
	ledger, err := processor.LedgerRow()
	if err != nil {
		return nil, err
	}
	var result horizon.Ledger
	resourceadapter.PopulateLedger(r.Context(), &result, ledger)
	return result, nil
}
