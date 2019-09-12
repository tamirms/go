package sse

import (
	"net/http"
	"time"

	"github.com/stellar/go/services/horizon/internal/ledger"
	"github.com/stellar/go/support/errors"
	"github.com/stellar/throttled"
)

// StreamHandler represents a stream handling action
type StreamHandler struct {
	RateLimiter  *throttled.HTTPRateLimiter
	LedgerSource LedgerSource
}

// LedgerSource exposes two helpers methods to help you find out the current
// ledger and yield every time there is a new ledger.
type LedgerSource interface {
	CurrentLedger() uint32
	NextLedger(currentSequence uint32) chan uint32
}

// CurrentStateFunc wraps a functions which returns the current ledger.State
type CurrentStateFunc func() ledger.State

// HistoryDBLedgerSource utility struct to pass the SSE update frequency and a
// function to get the current ledger state.
type HistoryDBLedgerSource struct {
	SSEUpdateFrequency time.Duration
	CurrentState       CurrentStateFunc
}

// CurrentLedger returns the current ledger.
func (source HistoryDBLedgerSource) CurrentLedger() uint32 {
	return source.CurrentState().ExpHistoryLatest
}

// NextLedger returns a channel which yields every time there is a new ledger.
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

// GenerateEventsFunc generates a slice of sse.Event which are sent via
// streaming.
type GenerateEventsFunc func() ([]Event, error)

// ServeStream handles a SSE requests, sending data every time there is a new
// ledger.
func (handler StreamHandler) ServeStream(
	w http.ResponseWriter,
	r *http.Request,
	limit int,
	generateEvents GenerateEventsFunc,
) {
	ctx := r.Context()
	stream := NewStream(ctx, w)
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
				stream.Err(ErrRateLimited)
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

// TestingLedgerSource is helper struct which implements the LedgerSource
// interface.
type TestingLedgerSource struct {
	currentLedger uint32
	newLedgers    chan uint32
}

// NewTestingLedgerSource returns a TestingLedgerSource.
func NewTestingLedgerSource(currentLedger uint32) *TestingLedgerSource {
	return &TestingLedgerSource{
		currentLedger: currentLedger,
		newLedgers:    make(chan uint32),
	}
}

// CurrentLedger returns the current ledger.
func (source *TestingLedgerSource) CurrentLedger() uint32 {
	return source.currentLedger
}

// AddLedger adds a new sequence to the newLedgers channel, yielding a new value.
func (source *TestingLedgerSource) AddLedger(nextSequence uint32) {
	source.newLedgers <- nextSequence
}

// TryAddLedger sends a new message to the newLedgers channel, forcing the
// execution of the stream loop.
func (source *TestingLedgerSource) TryAddLedger(nextSequence uint32, timeout time.Duration) bool {
	select {
	case source.newLedgers <- nextSequence:
		return true
	case <-time.After(timeout):
		return false
	}
}

// NextLedger returns a channel which yields every time there is a new ledger.
func (source *TestingLedgerSource) NextLedger(currentSequence uint32) chan uint32 {
	return source.newLedgers
}
