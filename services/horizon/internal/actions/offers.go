package actions

import (
	"context"
	"net/http"

	"github.com/stellar/go/protocols/horizon"
	"github.com/stellar/go/services/horizon/internal/db2/history"
	"github.com/stellar/go/services/horizon/internal/resourceadapter"
	"github.com/stellar/go/support/errors"
	"github.com/stellar/go/support/render/hal"
	"github.com/stellar/go/xdr"
)

// GetOffersHandler is the http handler for the /offers endpoint
type GetOffersHandler struct {
	HistoryQ *history.Q
}

// Streamable signals if this action supports streaming or not.
func (handler GetOffersHandler) Streamable() bool {
	return false
}

// GetObject returns an offer page.
func (handler GetOffersHandler) GetObject(r *http.Request) ([]hal.Pageable, error) {
	ctx := r.Context()
	pq, err := GetPageQuery(r)

	if err != nil {
		return []hal.Pageable{}, err
	}

	seller, err := GetString(r, "seller")

	if err != nil {
		return []hal.Pageable{}, err
	}

	var selling *xdr.Asset
	sellingAsset, found := MaybeGetAsset(r, "selling_")

	if found {
		selling = &sellingAsset
	}

	var buying *xdr.Asset
	buyingAsset, found := MaybeGetAsset(r, "buying_")

	if found {
		buying = &buyingAsset
	}

	query := history.OffersQuery{
		PageQuery: pq,
		SellerID:  seller,
		Selling:   selling,
		Buying:    buying,
	}

	offers, err := loadOffersQuery(ctx, handler.HistoryQ, query)

	if err != nil {
		return []hal.Pageable{}, err
	}

	return offers, nil
}

// GetAccountOffersHandler is the http handler for the
// `/accounts/{account_id}/offers` endpoint when using experimental ingestion.
type GetAccountOffersHandler struct {
	HistoryQ *history.Q
}

// Streamable signals if this action supports streaming or not.
func (handler GetAccountOffersHandler) Streamable() bool {
	return true
}

func (handler GetAccountOffersHandler) parseOffersQuery(r *http.Request) (history.OffersQuery, error) {
	pq, err := GetPageQuery(r)
	if err != nil {
		return history.OffersQuery{}, err
	}

	seller, err := GetString(r, "account_id")
	if err != nil {
		return history.OffersQuery{}, err
	}

	query := history.OffersQuery{
		PageQuery: pq,
		SellerID:  seller,
	}

	return query, nil
}

// GetObject gets objects for requests.
func (handler GetAccountOffersHandler) GetObject(r *http.Request) ([]hal.Pageable, error) {
	ctx := r.Context()
	query, err := handler.parseOffersQuery(r)

	if err != nil {
		return []hal.Pageable{}, err
	}

	offers, err := loadOffersQuery(ctx, handler.HistoryQ, query)

	if err != nil {
		return []hal.Pageable{}, err
	}

	return offers, nil
}

func loadOffersQuery(ctx context.Context, historyQ *history.Q, query history.OffersQuery) ([]hal.Pageable, error) {
	records, err := historyQ.GetOffers(query)

	if err != nil {
		return []hal.Pageable{}, err
	}

	offers, err := buildOffersResponse(ctx, historyQ, records)

	return offers, err
}

func buildOffersResponse(ctx context.Context, historyQ *history.Q, records []history.Offer) ([]hal.Pageable, error) {
	ledgerCache := history.LedgerCache{}
	for _, record := range records {
		ledgerCache.Queue(int32(record.LastModifiedLedger))
	}

	err := ledgerCache.Load(historyQ)

	if err != nil {
		return nil, errors.Wrap(err, "failed to load ledger batch")
	}

	var offers []hal.Pageable
	for _, record := range records {
		var offerResponse horizon.Offer

		ledger, found := ledgerCache.Records[int32(record.LastModifiedLedger)]
		ledgerPtr := &ledger
		if !found {
			ledgerPtr = nil
		}

		resourceadapter.PopulateHistoryOffer(ctx, &offerResponse, record, ledgerPtr)
		offers = append(offers, offerResponse)
	}

	return offers, nil
}
