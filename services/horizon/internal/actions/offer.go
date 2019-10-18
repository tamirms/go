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

type getOfferByID struct {
	historyQ *history.Q
}

// NewGetOfferByID returns an ObjectHandler for the /offers/{id} endpoint
func NewGetOfferByID(historyQ *history.Q) ObjectHandler {
	return repeatableReadObjectHandler{
		historyQ: historyQ,
		withQ: func(q *history.Q) ObjectHandler {
			return getOfferByID{q}
		},
	}
}

// GetResource returns an offer by id.
func (handler getOfferByID) GetResource(
	w HeaderWriter,
	r *http.Request,
) (hal.Pageable, error) {
	ctx := r.Context()
	offerID, err := GetInt64(r, "id")
	if err != nil {
		return nil, err
	}

	record, err := handler.historyQ.GetOfferByID(offerID)
	if err != nil {
		return nil, err
	}

	ledger := new(history.Ledger)
	err = handler.historyQ.LedgerBySequence(
		ledger,
		int32(record.LastModifiedLedger),
	)
	if handler.historyQ.NoRows(err) {
		ledger = nil
	} else if err != nil {
		return nil, err
	}

	var offerResponse horizon.Offer
	resourceadapter.PopulateHistoryOffer(ctx, &offerResponse, record, ledger)
	return offerResponse, nil
}

type getOffersHandler struct {
	historyQ *history.Q
}

// NewGetOffers returns a PageHandler for the /offers endpoint
func NewGetOffers(historyQ *history.Q) PageHandler {
	return repeatableReadPageHandler{
		historyQ: historyQ,
		withQ: func(q *history.Q) PageHandler {
			return getOffersHandler{q}
		},
	}
}

// GetResourcePage returns a page of offers.
func (handler getOffersHandler) GetResourcePage(
	w HeaderWriter,
	r *http.Request,
) ([]hal.Pageable, error) {
	ctx := r.Context()
	pq, err := GetPageQuery(r)
	if err != nil {
		return nil, err
	}

	seller, err := GetString(r, "seller")
	if err != nil {
		return nil, err
	}

	var selling *xdr.Asset
	if sellingAsset, found := MaybeGetAsset(r, "selling_"); found {
		selling = &sellingAsset
	}

	var buying *xdr.Asset
	if buyingAsset, found := MaybeGetAsset(r, "buying_"); found {
		buying = &buyingAsset
	}

	query := history.OffersQuery{
		PageQuery: pq,
		SellerID:  seller,
		Selling:   selling,
		Buying:    buying,
	}

	offers, err := getOffersPage(ctx, handler.historyQ, query)
	if err != nil {
		return nil, err
	}

	return offers, nil
}

type getAccountOffersHandler struct {
	historyQ *history.Q
}

// NewGetAccountOffers returns a PageHandler for the `/accounts/{account_id}/offers` endpoint
func NewGetAccountOffers(historyQ *history.Q) PageHandler {
	return repeatableReadPageHandler{
		historyQ: historyQ,
		withQ: func(q *history.Q) PageHandler {
			return getAccountOffersHandler{q}
		},
	}
}

func (handler getAccountOffersHandler) parseOffersQuery(r *http.Request) (history.OffersQuery, error) {
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

// GetResourcePage returns a page of offers for a given account.
func (handler getAccountOffersHandler) GetResourcePage(
	w HeaderWriter,
	r *http.Request,
) ([]hal.Pageable, error) {
	ctx := r.Context()
	query, err := handler.parseOffersQuery(r)
	if err != nil {
		return nil, err
	}

	offers, err := getOffersPage(ctx, handler.historyQ, query)
	if err != nil {
		return nil, err
	}

	return offers, nil
}

func getOffersPage(ctx context.Context, historyQ *history.Q, query history.OffersQuery) ([]hal.Pageable, error) {
	records, err := historyQ.GetOffers(query)
	if err != nil {
		return nil, err
	}

	ledgerCache := history.LedgerCache{}
	for _, record := range records {
		ledgerCache.Queue(int32(record.LastModifiedLedger))
	}

	if err := ledgerCache.Load(historyQ); err != nil {
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
