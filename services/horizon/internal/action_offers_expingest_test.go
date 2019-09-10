package horizon

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/go-chi/chi"

	"github.com/stellar/go/clients/horizon"
	"github.com/stellar/go/services/horizon/internal/actions"
	"github.com/stellar/go/services/horizon/internal/db2/history"
	"github.com/stellar/go/services/horizon/internal/render/problem"
	"github.com/stellar/go/services/horizon/internal/test"
	"github.com/stellar/go/xdr"
)

var (
	issuer = xdr.MustAddress("GBRPYHIL2CI3FNQ4BXLFMNDLFJUNPU2HY3ZMFSHONUCEOASW7QC7OX2H")
	seller = xdr.MustAddress("GA5WBPYA5Y4WAEHXWR2UKO2UO4BUGHUQ74EUPKON2QHV4WRHOIRNKKH2")

	nativeAsset = xdr.MustNewNativeAsset()
	usdAsset    = xdr.MustNewCreditAsset("USD", issuer.Address())
	eurAsset    = xdr.MustNewCreditAsset("EUR", issuer.Address())

	eurOffer = xdr.OfferEntry{
		SellerId: issuer,
		OfferId:  xdr.Int64(4),
		Buying:   eurAsset,
		Selling:  nativeAsset,
		Price: xdr.Price{
			N: 1,
			D: 1,
		},
		Flags:  1,
		Amount: xdr.Int64(500),
	}
	twoEurOffer = xdr.OfferEntry{
		SellerId: seller,
		OfferId:  xdr.Int64(5),
		Buying:   eurAsset,
		Selling:  nativeAsset,
		Price: xdr.Price{
			N: 2,
			D: 1,
		},
		Flags:  2,
		Amount: xdr.Int64(500),
	}
	usdOffer = xdr.OfferEntry{
		SellerId: issuer,
		OfferId:  xdr.Int64(6),
		Buying:   usdAsset,
		Selling:  eurAsset,
		Price: xdr.Price{
			N: 1,
			D: 1,
		},
		Flags:  1,
		Amount: xdr.Int64(500),
	}
)

func TestOfferActions_Show(t *testing.T) {
	ht := StartHTTPTest(t, "base")
	ht.App.config.EnableExperimentalIngestion = true
	defer ht.Finish()
	q := &history.Q{ht.HorizonSession()}

	ht.Assert.NoError(q.UpdateLastLedgerExpIngest(3))
	ht.Assert.NoError(q.UpsertOffer(eurOffer, 3))
	ht.Assert.NoError(q.UpsertOffer(twoEurOffer, 20))

	w := ht.Get(fmt.Sprintf("/offers/%v", eurOffer.OfferId))

	if ht.Assert.Equal(200, w.Code) {
		var result horizon.Offer
		err := json.Unmarshal(w.Body.Bytes(), &result)
		ht.Require.NoError(err)
		ht.Assert.Equal(int64(eurOffer.OfferId), result.ID)
		ht.Assert.Equal("native", result.Selling.Type)
		ht.Assert.Equal("credit_alphanum4", result.Buying.Type)
		ht.Assert.Equal(issuer.Address(), result.Seller)
		ht.Assert.Equal(issuer.Address(), result.Buying.Issuer)
		ht.Assert.Equal(int32(3), result.LastModifiedLedger)

		ledger := new(history.Ledger)
		err = q.LedgerBySequence(ledger, 3)

		ht.Assert.NoError(err)
		ht.Assert.True(ledger.ClosedAt.Equal(*result.LastModifiedTime))
	}

	w = ht.Get(fmt.Sprintf("/offers/%v", twoEurOffer.OfferId))

	if ht.Assert.Equal(200, w.Code) {
		var result horizon.Offer
		err := json.Unmarshal(w.Body.Bytes(), &result)
		ht.Require.NoError(err)
		ht.Assert.Equal(int32(20), result.LastModifiedLedger)
		ht.Assert.Nil(result.LastModifiedTime)
	}
}

func TestOfferActions_OfferDoesNotExist(t *testing.T) {
	ht := StartHTTPTest(t, "base")
	ht.App.config.EnableExperimentalIngestion = true
	defer ht.Finish()
	q := &history.Q{ht.HorizonSession()}
	ht.Assert.NoError(q.UpdateLastLedgerExpIngest(3))

	w := ht.Get("/offers/123456")

	ht.Assert.Equal(404, w.Code)
}

func TestOfferActionsStillIngesting_Show(t *testing.T) {
	ht := StartHTTPTest(t, "base")
	ht.App.config.EnableExperimentalIngestion = true
	defer ht.Finish()
	q := &history.Q{ht.HorizonSession()}
	ht.Assert.NoError(q.UpdateLastLedgerExpIngest(0))

	w := ht.Get("/offers/123456")
	ht.Assert.Equal(problem.StillIngesting.Status, w.Code)
}

func TestOfferActions_Index(t *testing.T) {
	ht := StartHTTPTest(t, "base")
	ht.App.config.EnableExperimentalIngestion = true
	defer ht.Finish()
	q := &history.Q{ht.HorizonSession()}

	ht.Assert.NoError(q.UpdateLastLedgerExpIngest(3))
	ht.Assert.NoError(q.UpsertOffer(eurOffer, 3))
	ht.Assert.NoError(q.UpsertOffer(twoEurOffer, 3))
	ht.Assert.NoError(q.UpsertOffer(usdOffer, 3))

	t.Run("No filter", func(t *testing.T) {
		w := ht.Get("/offers")

		if ht.Assert.Equal(http.StatusOK, w.Code) {
			ht.Assert.PageOf(3, w.Body)

			var records []horizon.Offer
			ht.UnmarshalPage(w.Body, &records)

			ht.Assert.Equal(int64(eurOffer.OfferId), records[0].ID)
			ht.Assert.Equal("native", records[0].Selling.Type)
			ht.Assert.Equal("credit_alphanum4", records[0].Buying.Type)
			ht.Assert.Equal(issuer.Address(), records[0].Seller)
			ht.Assert.Equal(issuer.Address(), records[0].Buying.Issuer)
			ht.Assert.Equal(int32(3), records[0].LastModifiedLedger)

			ledger := new(history.Ledger)
			err := q.LedgerBySequence(ledger, 3)

			ht.Assert.NoError(err)
			ht.Assert.True(ledger.ClosedAt.Equal(*records[0].LastModifiedTime))
		}
	})

	t.Run("Filter by seller", func(t *testing.T) {
		w := ht.Get(fmt.Sprintf("/offers?seller=%s", issuer.Address()))

		if ht.Assert.Equal(http.StatusOK, w.Code) {
			ht.Assert.PageOf(2, w.Body)
			var records []horizon.Offer
			ht.UnmarshalPage(w.Body, &records)

			for _, record := range records {
				ht.Assert.Equal(issuer.Address(), record.Seller)
			}
		}
	})

	t.Run("Filter by selling asset", func(t *testing.T) {
		asset := horizon.Asset{}
		nativeAsset.Extract(&asset.Type, &asset.Code, &asset.Issuer)
		w := ht.Get(fmt.Sprintf("/offers?selling_asset_type=%s", asset.Type))

		if ht.Assert.Equal(http.StatusOK, w.Code) {
			ht.Assert.PageOf(2, w.Body)
			var records []horizon.Offer
			ht.UnmarshalPage(w.Body, &records)

			for _, record := range records {
				ht.Assert.Equal(asset, record.Selling)
			}
		}

		asset = horizon.Asset{}
		eurAsset.Extract(&asset.Type, &asset.Code, &asset.Issuer)

		url := fmt.Sprintf(
			"/offers?selling_asset_type=%s&selling_asset_code=%s&selling_asset_issuer=%s",
			asset.Type,
			asset.Code,
			asset.Issuer,
		)
		w = ht.Get(url)

		if ht.Assert.Equal(http.StatusOK, w.Code) {
			ht.Assert.PageOf(1, w.Body)
			var records []horizon.Offer
			ht.UnmarshalPage(w.Body, &records)

			for _, record := range records {
				ht.Assert.Equal(asset, record.Selling)
			}
		}
	})

	t.Run("Filter by buying asset", func(t *testing.T) {
		asset := horizon.Asset{}
		eurAsset.Extract(&asset.Type, &asset.Code, &asset.Issuer)

		url := fmt.Sprintf(
			"/offers?buying_asset_type=%s&buying_asset_code=%s&buying_asset_issuer=%s",
			asset.Type,
			asset.Code,
			asset.Issuer,
		)

		w := ht.Get(url)

		if ht.Assert.Equal(http.StatusOK, w.Code) {
			ht.Assert.PageOf(2, w.Body)
			var records []horizon.Offer
			ht.UnmarshalPage(w.Body, &records)

			for _, record := range records {
				ht.Assert.Equal(asset, record.Buying)
			}
		}

		asset = horizon.Asset{}
		usdAsset.Extract(&asset.Type, &asset.Code, &asset.Issuer)

		url = fmt.Sprintf(
			"/offers?buying_asset_type=%s&buying_asset_code=%s&buying_asset_issuer=%s",
			asset.Type,
			asset.Code,
			asset.Issuer,
		)

		w = ht.Get(url)

		if ht.Assert.Equal(http.StatusOK, w.Code) {
			ht.Assert.PageOf(1, w.Body)
			var records []horizon.Offer
			ht.UnmarshalPage(w.Body, &records)

			for _, record := range records {
				ht.Assert.Equal(asset, record.Buying)
			}
		}
	})
}

func TestOfferActionsStillIngesting_Index(t *testing.T) {
	ht := StartHTTPTest(t, "base")
	ht.App.config.EnableExperimentalIngestion = true
	defer ht.Finish()
	q := &history.Q{ht.HorizonSession()}
	ht.Assert.NoError(q.UpdateLastLedgerExpIngest(0))

	w := ht.Get("/offers")
	ht.Assert.Equal(problem.StillIngesting.Status, w.Code)
}
func TestOfferActions_AccountIndexExperimentalIngestion(t *testing.T) {
	tt := test.Start(t)
	defer tt.Finish()

	test.ResetHorizonDB(t, tt.HorizonDB)
	q := &history.Q{tt.HorizonSession()}

	tt.Assert.NoError(q.UpdateLastLedgerExpIngest(3))
	tt.Assert.NoError(q.UpsertOffer(eurOffer, 3))
	tt.Assert.NoError(q.UpsertOffer(twoEurOffer, 3))
	tt.Assert.NoError(q.UpsertOffer(usdOffer, 3))

	handler := actions.GetAccountOffersHandler{HistoryQ: q}

	streamHandler := actions.StreamHandler{}
	client := accountOffersClient(tt, handler, streamHandler)

	w := client.Get(fmt.Sprintf("/accounts/%s/offers", issuer.Address()))

	if tt.Assert.Equal(http.StatusOK, w.Code) {
		var records []horizon.Offer
		tt.UnmarshalPage(w.Body, &records)

		tt.Assert.Len(records, 2)
		for _, record := range records {
			tt.Assert.Equal(issuer.Address(), record.Seller)
		}
	}
}

func accountOffersClient(tt *test.T, handler actions.GetAccountOffersHandler, streamHandler actions.StreamHandler) test.RequestHelper {
	router := chi.NewRouter()

	installAccountOfferRoute(handler, streamHandler, true, router)
	return test.NewRequestHelper(router)
}

// TODO: commenting this for now since new streaming version is not finished yet.
// func TestOfferActions_AccountSSEExperimentalIngestion(t *testing.T) {
// 	tt := test.Start(t)
// 	defer tt.Finish()
// 	test.ResetHorizonDB(t, tt.HorizonDB)

// 	q := &history.Q{tt.HorizonSession()}

// 	ledgerSource := actions.NewTestingLedgerSource(3)

// 	handler := actions.GetAccountOffersHandler{
// 		HistoryQ: q,
// 		StreamHandler: actions.StreamHandler{
// 			RateLimiter:  maybeInitWebRateLimiter(NewTestConfig().RateQuota),
// 			LedgerSource: ledgerSource,
// 		},
// 	}
// 	client := accountOffersClient(tt, handler)

// 	tt.Assert.NoError(q.UpdateLastLedgerExpIngest(3))
// 	tt.Assert.NoError(q.UpsertOffer(eurOffer, 3))
// 	tt.Assert.NoError(q.UpsertOffer(twoEurOffer, 3))
// 	tt.Assert.NoError(q.UpsertOffer(usdOffer, 3))

// 	ignoredUSDOffer := usdOffer
// 	ignoredUSDOffer.OfferId = 3

// 	includedEUROffer := eurOffer
// 	includedEUROffer.OfferId = 11

// 	otherIncludedEUROffer := eurOffer
// 	otherIncludedEUROffer.OfferId = 12

// 	st := actions.NewStreamTest(
// 		client,
// 		fmt.Sprintf("/accounts/%s/offers", issuer.Address()),
// 		ledgerSource,
// 	)
// 	st.Run(func(w *httptest.ResponseRecorder) {
// 		var offers []horizon.Offer
// 		for _, line := range strings.Split(w.Body.String(), "\n") {
// 			if strings.HasPrefix(line, "data: {") {
// 				jsonString := line[len("data: "):]
// 				var offer horizon.Offer
// 				err := json.Unmarshal([]byte(jsonString), &offer)
// 				tt.Assert.NoError(err)
// 				offers = append(offers, offer)
// 			}
// 		}

// 		expectedOfferIds := []int64{
// 			int64(eurOffer.OfferId),
// 			int64(usdOffer.OfferId),
// 			int64(includedEUROffer.OfferId),
// 			int64(otherIncludedEUROffer.OfferId),
// 		}

// 		tt.Assert.Len(offers, len(expectedOfferIds))
// 		for i, offer := range offers {
// 			tt.Assert.Equal(issuer.Address(), offer.Seller)
// 			tt.Assert.Equal(expectedOfferIds[i], offer.ID)
// 		}
// 	})

// 	ledgerSource.AddLedger(4)

// 	tt.Assert.NoError(q.UpsertOffer(ignoredUSDOffer, 1))
// 	tt.Assert.NoError(q.UpsertOffer(includedEUROffer, 4))
// 	tt.Assert.NoError(q.UpsertOffer(otherIncludedEUROffer, 5))

// 	ledgerSource.AddLedger(6)
// 	st.Wait()

// 	st = actions.NewStreamTest(
// 		client,
// 		fmt.Sprintf("/accounts/%s/offers?cursor=now", issuer.Address()),
// 		ledgerSource,
// 	)

// 	st.Run(func(w *httptest.ResponseRecorder) {
// 		var offers []horizon.Offer
// 		for _, line := range strings.Split(w.Body.String(), "\n") {
// 			if strings.HasPrefix(line, "data: {") {
// 				jsonString := line[len("data: "):]
// 				var offer horizon.Offer
// 				err := json.Unmarshal([]byte(jsonString), &offer)
// 				tt.Assert.NoError(err)
// 				offers = append(offers, offer)
// 			}
// 		}

// 		tt.Assert.Len(offers, 0)
// 	})

// 	st.Wait()
// }
