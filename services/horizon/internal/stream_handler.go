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
	CurrentLedger() int32
	NextLedger(currentSequence int32) chan int32
}

type HistoryDBLedgerSource struct {
	SSEUpdateFrequency time.Duration
}

func (source HistoryDBLedgerSource) CurrentLedger() int32 {
	return ledger.CurrentState().HistoryLatest
}

func (source HistoryDBLedgerSource) NextLedger(currentSequence int32) chan int32 {
	// Make sure this is buffered channel of size 1. Otherwise, the go routine below
	// will never return if `newLedgers` channel is not read. From Effective Go:
	// > If the channel is unbuffered, the sender blocks until the receiver has received the value.
	newLedgers := make(chan int32, 1)
	go func() {
		for {
			time.Sleep(source.SSEUpdateFrequency)
			currentLedgerState := ledger.CurrentState()
			if currentLedgerState.HistoryLatest > currentSequence {
				newLedgers <- currentLedgerState.HistoryLatest
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
	currentLedger int32
	newLedgers    chan int32
}

func NewTestingLedgerSource(currentLedger int32) *TestingLedgerSource {
	return &TestingLedgerSource{
		currentLedger: currentLedger,
		newLedgers:    make(chan int32),
	}
}

func (source *TestingLedgerSource) CurrentLedger() int32 {
	return source.currentLedger
}

func (source *TestingLedgerSource) AddLedger(nextSequence int32) {
	source.newLedgers <- nextSequence
}

func (source *TestingLedgerSource) NextLedger(currentSequence int32) chan int32 {
	return source.newLedgers
}
