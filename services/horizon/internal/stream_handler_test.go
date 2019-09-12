package horizon

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-chi/chi"
	"github.com/stellar/go/services/horizon/internal/ledger"
	"github.com/stellar/go/services/horizon/internal/render/sse"
	"github.com/stellar/go/support/render/hal"
)

// StreamTest utility struct to wrap SSE related tests.
type StreamTest struct {
	handler      http.Handler
	ledgerSource *ledger.TestingSource
	cancel       context.CancelFunc
	done         chan bool
}

// NewStreamTest returns a StreamTest struct
func NewStreamTest(
	handler http.Handler,
	ledgerSource *ledger.TestingSource,
) *StreamTest {
	return &StreamTest{
		handler:      handler,
		ledgerSource: ledgerSource,
	}
}

// Run executes an SSE related test, letting you simulate ledger closings via
// AddLedger.
func (s *StreamTest) Run(request *http.Request, checkResponse func(w *httptest.ResponseRecorder)) {
	var ctx context.Context
	ctx, s.cancel = context.WithCancel(request.Context())
	s.done = make(chan bool)

	go func() {
		w := httptest.NewRecorder()
		s.handler.ServeHTTP(w, request.WithContext(ctx))

		checkResponse(w)
		s.done <- true
	}()
}

// Wait blocks testing until the stream test has finished running.
func (s *StreamTest) Wait(expectLimitReached bool) {
	if !expectLimitReached {
		// first send a ledger to the stream handler so we can ensure that at least one
		// iteration of the stream loop has been executed
		s.ledgerSource.TryAddLedger(0, 2*time.Second)
		s.cancel()
	}
	<-s.done
}

type testPage struct {
	Value       string `json:"value"`
	pagingToken int
}

func (p testPage) PagingToken() string {
	return fmt.Sprintf("%v", p.pagingToken)
}

type testPageAction struct {
	objects []string
	lock    sync.Mutex
}

func (action *testPageAction) appendObjects(objects ...string) {
	action.lock.Lock()
	defer action.lock.Unlock()
	action.objects = append(action.objects, objects...)
}

func (action *testPageAction) GetResourcePage(r *http.Request) ([]hal.Pageable, error) {
	action.lock.Lock()
	defer action.lock.Unlock()

	cursor := r.Header.Get("Last-Event-ID")
	if cursor == "" {
		cursor = r.URL.Query().Get("cursor")
	}
	if cursor == "" {
		cursor = "0"
	}
	parsedCursor, err := strconv.Atoi(cursor)
	if err != nil {
		return nil, err
	}

	limit := len(action.objects)
	if limitParam := r.URL.Query().Get("limit"); limitParam != "" {
		limit, err = strconv.Atoi(limitParam)
		if err != nil {
			return nil, err
		}
	}

	if parsedCursor < 0 {
		return nil, fmt.Errorf("cursor cannot be negative")
	}

	if parsedCursor >= len(action.objects) {
		return []hal.Pageable{}, nil
	}
	response := []hal.Pageable{}
	for i, object := range action.objects[parsedCursor:] {
		if len(response) >= limit {
			break
		}

		response = append(response, testPage{Value: object, pagingToken: parsedCursor + i + 1})
	}

	return response, nil
}

func streamRequest(t *testing.T, queryParams string) *http.Request {
	request, err := http.NewRequest("GET", "http://localhost?"+queryParams, nil)
	if err != nil {
		t.Fatalf("could not construct request: %v", err)
	}
	ctx := context.WithValue(context.Background(), chi.RouteCtxKey, chi.NewRouteContext())
	request = request.WithContext(ctx)

	return request
}

func expectResponse(t *testing.T, expectedResponse []string) func(*httptest.ResponseRecorder) {
	return func(w *httptest.ResponseRecorder) {
		var response []string
		for _, line := range strings.Split(w.Body.String(), "\n") {
			if strings.HasPrefix(line, "data: {") {
				jsonString := line[len("data: "):]
				var page testPage
				err := json.Unmarshal([]byte(jsonString), &page)
				if err != nil {
					t.Fatalf("could not parse json %v", err)
				}
				response = append(response, page.Value)
			}
		}

		if len(expectedResponse) != len(response) {
			t.Fatalf("expected %v but got %v", expectedResponse, response)
		}

		for i, entry := range expectedResponse {
			if entry != response[i] {
				t.Fatalf("expected %v but got %v", expectedResponse, response)
			}
		}
	}
}

func TestRenderStream(t *testing.T) {
	ledgerSource := ledger.NewTestingSource(3)
	action := &testPageAction{
		objects: []string{"a", "b", "c"},
	}
	streamHandler := sse.StreamHandler{
		LedgerSource: ledgerSource,
	}
	st := NewStreamTest(
		http.HandlerFunc(streamablePageHandler(action, streamHandler).renderStream),
		ledgerSource,
	)

	t.Run("without offset", func(t *testing.T) {
		request := streamRequest(t, "")
		st.Run(request, expectResponse(t, []string{"a", "b", "c", "d", "e", "f"}))

		ledgerSource.AddLedger(4)
		action.appendObjects("d", "e")

		ledgerSource.AddLedger(6)
		action.appendObjects("f")

		st.Wait(false)
	})

	action.objects = []string{"a", "b", "c"}
	t.Run("with offset", func(t *testing.T) {
		request := streamRequest(t, "cursor=1")
		st.Run(request, expectResponse(t, []string{"b", "c", "d", "e", "f"}))

		ledgerSource.AddLedger(4)
		action.appendObjects("d", "e")

		ledgerSource.AddLedger(6)
		action.appendObjects("f")

		st.Wait(false)
	})

	action.objects = []string{"a", "b", "c"}
	t.Run("with limit", func(t *testing.T) {
		request := streamRequest(t, "limit=2")
		st.Run(request, expectResponse(t, []string{"a", "b"}))

		st.Wait(true)
	})

	action.objects = []string{"a", "b", "c", "d", "e"}
	t.Run("with limit and offset", func(t *testing.T) {
		request := streamRequest(t, "limit=2&cursor=1")
		st.Run(request, expectResponse(t, []string{"b", "c"}))

		st.Wait(true)
	})
}
