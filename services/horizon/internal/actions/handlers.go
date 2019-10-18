package actions

import (
	"database/sql"
	"net/http"
	"strconv"

	"github.com/stellar/go/services/horizon/internal/db2/history"
	"github.com/stellar/go/support/errors"
	"github.com/stellar/go/support/render/hal"
)

// LastLedgerHeaderName is the header which is set on all experimental ingestion endpoints
const LastLedgerHeaderName = "Latest-Ledger"

// HeaderWriter is an interface for setting HTTP response headers
type HeaderWriter interface {
	Header() http.Header
}

// StreamableObjectResponse is an interface for objects returned by streamable object endpoints
// A streamable object endpoint is an SSE endpoint which returns a single JSON object response
// instead of a page of items.
type StreamableObjectResponse interface {
	Equals(other StreamableObjectResponse) bool
}

// PageHandler represents a handler for a horizon endpoint which produces a response body
// consisting of a list of `hal.Pageable` items
type PageHandler interface {
	GetResourcePage(w HeaderWriter, r *http.Request) ([]hal.Pageable, error)
}

// StreamableObjectHandler represents a handler for a horizon endpoint which produces a
// response body consisting of a single `StreamableObjectResponse` instance
type StreamableObjectHandler interface {
	GetResource(
		w HeaderWriter,
		r *http.Request,
	) (StreamableObjectResponse, error)
}

// ObjectHandler represents a handler for a horizon endpoint which produces a
// response body consisting of a single `hal.Pageable` instance
type ObjectHandler interface {
	GetResource(
		w HeaderWriter,
		r *http.Request,
	) (hal.Pageable, error)
}

func repeatableReadSession(source *history.Q, r *http.Request) (*history.Q, error) {
	repeatableReadSession := source.Clone()
	repeatableReadSession.Ctx = r.Context()
	err := repeatableReadSession.BeginTx(&sql.TxOptions{
		Isolation: sql.LevelRepeatableRead,
		ReadOnly:  true,
	})
	if err != nil {
		return nil, errors.Wrap(err, "could not begin repeatable read transaction")
	}
	return &history.Q{repeatableReadSession}, nil
}

// SetLastLedgerHeader sets the Latest-Ledger header
func SetLastLedgerHeader(w HeaderWriter, lastIngestedLedger uint32) {
	w.Header().Set(LastLedgerHeaderName, strconv.FormatUint(uint64(lastIngestedLedger), 10))
}

func fetchAndSetLastLedgerHeader(w HeaderWriter, historyQ *history.Q) error {
	lastIngestedLedger, err := historyQ.GetLastLedgerExpIngestNonBlocking()
	if err != nil {
		return errors.Wrap(err, "could not determine last ingested ledger")
	}

	SetLastLedgerHeader(w, lastIngestedLedger)
	return nil
}

type repeatableReadPageHandler struct {
	historyQ *history.Q
	withQ    func(*history.Q) PageHandler
}

func (handler repeatableReadPageHandler) GetResourcePage(
	w HeaderWriter,
	r *http.Request,
) ([]hal.Pageable, error) {
	historyQ, err := repeatableReadSession(handler.historyQ, r)
	if err != nil {
		return nil, err
	}
	defer historyQ.Rollback()

	if err = fetchAndSetLastLedgerHeader(w, historyQ); err != nil {
		return nil, err
	}

	return handler.withQ(historyQ).GetResourcePage(w, r)
}

type repeatableReadObjectHandler struct {
	historyQ *history.Q
	withQ    func(*history.Q) ObjectHandler
}

func (handler repeatableReadObjectHandler) GetResource(
	w HeaderWriter,
	r *http.Request,
) (hal.Pageable, error) {
	historyQ, err := repeatableReadSession(handler.historyQ, r)
	if err != nil {
		return nil, err
	}
	defer historyQ.Rollback()

	if err = fetchAndSetLastLedgerHeader(w, historyQ); err != nil {
		return nil, err
	}

	return handler.withQ(historyQ).GetResource(w, r)
}
