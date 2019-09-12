package ledger

import "time"

// Source exposes two helpers methods to help you find out the current
// ledger and yield every time there is a new ledger.
type Source interface {
	CurrentLedger() uint32
	NextLedger(currentSequence uint32) chan uint32
}

type currentStateFunc func() State

// HistoryDBSource utility struct to pass the SSE update frequency and a
// function to get the current ledger state.
type HistoryDBSource struct {
	updateFrequency time.Duration
	currentState    currentStateFunc
}

// NewHistoryDBSource constructs a new instance of HistoryDBSource
func NewHistoryDBSource(updateFrequency time.Duration) HistoryDBSource {
	return HistoryDBSource{
		updateFrequency: updateFrequency,
		currentState:    CurrentState,
	}
}

// CurrentLedger returns the current ledger.
func (source HistoryDBSource) CurrentLedger() uint32 {
	return source.currentState().ExpHistoryLatest
}

// NextLedger returns a channel which yields every time there is a new ledger.
func (source HistoryDBSource) NextLedger(currentSequence uint32) chan uint32 {
	// Make sure this is buffered channel of size 1. Otherwise, the go routine below
	// will never return if `newLedgers` channel is not read. From Effective Go:
	// > If the channel is unbuffered, the sender blocks until the receiver has received the value.
	newLedgers := make(chan uint32, 1)
	go func() {
		for {
			if source.updateFrequency > 0 {
				time.Sleep(source.updateFrequency)
			}

			currentLedgerState := source.currentState()
			if currentLedgerState.ExpHistoryLatest > currentSequence {
				newLedgers <- currentLedgerState.ExpHistoryLatest
				return
			}
		}
	}()

	return newLedgers
}

// TestingSource is helper struct which implements the LedgerSource
// interface.
type TestingSource struct {
	currentLedger uint32
	newLedgers    chan uint32
}

// NewTestingSource returns a TestingSource.
func NewTestingSource(currentLedger uint32) *TestingSource {
	return &TestingSource{
		currentLedger: currentLedger,
		newLedgers:    make(chan uint32),
	}
}

// CurrentLedger returns the current ledger.
func (source *TestingSource) CurrentLedger() uint32 {
	return source.currentLedger
}

// AddLedger adds a new sequence to the newLedgers channel, yielding a new value.
func (source *TestingSource) AddLedger(nextSequence uint32) {
	source.newLedgers <- nextSequence
}

// TryAddLedger sends a new message to the newLedgers channel, forcing the
// execution of the stream loop.
func (source *TestingSource) TryAddLedger(nextSequence uint32, timeout time.Duration) bool {
	select {
	case source.newLedgers <- nextSequence:
		return true
	case <-time.After(timeout):
		return false
	}
}

// NextLedger returns a channel which yields every time there is a new ledger.
func (source *TestingSource) NextLedger(currentSequence uint32) chan uint32 {
	return source.newLedgers
}
