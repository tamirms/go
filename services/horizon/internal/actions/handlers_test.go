package actions

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stellar/go/services/horizon/internal/db2/history"
	"github.com/stellar/go/services/horizon/internal/test"
	"github.com/stellar/go/support/errors"
	"github.com/stellar/go/support/render/hal"
)

type testPageHandler struct {
	historyQ *history.Q
}

func (handler testPageHandler) GetResourcePage(
	w HeaderWriter,
	r *http.Request,
) ([]hal.Pageable, error) {
	if handler.historyQ.GetTx() == nil {
		return nil, errors.New("expected transaction to be in session")
	}

	return nil, nil
}

type testObjectHandler struct {
	historyQ *history.Q
}

func (handler testObjectHandler) GetResource(
	w HeaderWriter,
	r *http.Request,
) (hal.Pageable, error) {
	if handler.historyQ.GetTx() == nil {
		return nil, errors.New("expected transaction to be in session")
	}

	return nil, nil
}

func TestRepeatableReadHandlers(t *testing.T) {
	tt := test.Start(t)
	defer tt.Finish()
	test.ResetHorizonDB(t, tt.HorizonDB)

	q := &history.Q{tt.HorizonSession()}
	tt.Assert.NoError(q.UpdateLastLedgerExpIngest(2))

	pageHandler := repeatableReadPageHandler{
		historyQ: q,
		withQ: func(q *history.Q) PageHandler {
			return testPageHandler{q}
		},
	}

	objectHandler := repeatableReadObjectHandler{
		historyQ: q,
		withQ: func(q *history.Q) ObjectHandler {
			return testObjectHandler{q}
		},
	}

	request, err := http.NewRequest("GET", "http://localhost", nil)
	tt.Assert.NoError(err)

	w := httptest.NewRecorder()
	_, err = pageHandler.GetResourcePage(w, request)
	tt.Assert.NoError(err)
	tt.Assert.Equal("2", w.Header().Get(LastLedgerHeaderName))
	tt.Assert.Nil(q.GetTx())

	w = httptest.NewRecorder()
	_, err = objectHandler.GetResource(w, request)
	tt.Assert.NoError(err)
	tt.Assert.Equal("2", w.Header().Get(LastLedgerHeaderName))
	tt.Assert.Nil(q.GetTx())
}
