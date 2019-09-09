package test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"time"

	"github.com/stellar/go/services/horizon/internal/actions"
)

// StreamTest utility struct to wrap SSE related tests.
type StreamTest struct {
	client       RequestHelper
	uri          string
	ledgerSource *actions.TestingLedgerSource
	cancel       context.CancelFunc
	done         chan bool
}

// NewStreamTest returns a StreamTest struct
func NewStreamTest(
	client RequestHelper,
	uri string,
	ledgerSource *actions.TestingLedgerSource,
) *StreamTest {
	return &StreamTest{
		client:       client,
		uri:          uri,
		ledgerSource: ledgerSource,
	}
}

func (s *StreamTest) Run(checkResponse func(w *httptest.ResponseRecorder)) {
	var ctx context.Context
	ctx, s.cancel = context.WithCancel(context.Background())
	s.done = make(chan bool)

	go func() {
		w := s.client.Get(
			s.uri,
			RequestHelperStreaming,
			func(r *http.Request) {
				*r = *r.WithContext(ctx)
			},
		)

		checkResponse(w)
		s.done <- true
	}()
}

func (s *StreamTest) Wait() {
	// first send a ledger to the stream handler so we can ensure that at least one
	// iteration of the stream loop has been executed
	s.ledgerSource.TryAddLedger(0, 2*time.Second)
	s.cancel()
	<-s.done
}
