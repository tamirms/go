package horizon

import (
	"net/http"
	"time"

	"github.com/stellar/go/services/horizon/internal/ledger"
	"github.com/stellar/go/services/horizon/internal/render/sse"
	"github.com/stellar/go/support/errors"
	"github.com/stellar/throttled"
)

type StreamHandler struct {
	RateLimiter  *throttled.HTTPRateLimiter
	LedgerSource LedgerSource
}

type LedgerSource interface {
	CurrentLedger() uint32
	NextLedger(currentSequence uint32) chan uint32
}

type CurrentStateFunc func() ledger.State

type HistoryDBLedgerSource struct {
	SSEUpdateFrequency time.Duration
	CurrentState       CurrentStateFunc
}

func (source HistoryDBLedgerSource) CurrentLedger() uint32 {
	return source.CurrentState().ExpHistoryLatest
}

func (source HistoryDBLedgerSource) NextLedger(currentSequence uint32) chan uint32 {
	// Make sure this is buffered channel of size 1. Otherwise, the go routine below
	// will never return if `newLedgers` channel is not read. From Effective Go:
	// > If the channel is unbuffered, the sender blocks until the receiver has received the value.
	newLedgers := make(chan uint32, 1)
	go func() {
		for {
			if source.SSEUpdateFrequency > 0 {
				time.Sleep(source.SSEUpdateFrequency)
			}

			currentLedgerState := source.CurrentState()
			if currentLedgerState.ExpHistoryLatest > currentSequence {
				newLedgers <- currentLedgerState.ExpHistoryLatest
				return
			}
		}
	}()

	return newLedgers
}

type GenerateEventsFunc func() ([]sse.Event, error)

func (handler StreamHandler) ServeStream(
	w http.ResponseWriter,
	r *http.Request,
	limit int,
	generateEvents GenerateEventsFunc,
) {
	ctx := r.Context()
	stream := sse.NewStream(ctx, w)
	stream.SetLimit(limit)

	currentLedgerSequence := handler.LedgerSource.CurrentLedger()
	for {
		// Rate limit the request if it's a call to stream since it queries the DB every second. See
		// https://github.com/stellar/go/issues/715 for more details.
		rateLimiter := handler.RateLimiter
		if rateLimiter != nil {
			limited, _, err := rateLimiter.RateLimiter.RateLimit(rateLimiter.VaryBy.Key(r), 1)
			if err != nil {
				stream.Err(errors.Wrap(err, "RateLimiter error"))
				return
			}
			if limited {
				stream.Err(sse.ErrRateLimited)
				return
			}
		}

		events, err := generateEvents()
		if err != nil {
			stream.Err(err)
			return
		}
		for _, event := range events {
			stream.Send(event)
		}

		// Manually send the preamble in case there are no data events in SSE to trigger a stream.Send call.
		// This method is called every iteration of the loop, but is protected by a sync.Once variable so it's
		// only executed once.
		stream.Init()

		if stream.IsDone() {
			return
		}

		select {
		case currentLedgerSequence = <-handler.LedgerSource.NextLedger(currentLedgerSequence):
			continue
		case <-ctx.Done():
		}

		stream.Done()
		return
	}
}

type TestingLedgerSource struct {
	currentLedger uint32
	newLedgers    chan uint32
}

func NewTestingLedgerSource(currentLedger uint32) *TestingLedgerSource {
	return &TestingLedgerSource{
		currentLedger: currentLedger,
		newLedgers:    make(chan uint32),
	}
}

func (source *TestingLedgerSource) CurrentLedger() uint32 {
	return source.currentLedger
}

func (source *TestingLedgerSource) AddLedger(nextSequence uint32) {
	source.newLedgers <- nextSequence
}

func (source *TestingLedgerSource) NextLedger(currentSequence uint32) chan uint32 {
	return source.newLedgers
}
