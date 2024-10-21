package server

import (
	"io"
	"math/rand"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/stellar/go/exp/services/trade-aggregations/metrics"
	"github.com/stellar/go/support/log"
)

const requestsPerSecond = 20
const workers = 10 * requestsPerSecond
const queueSize = 10 * workers
const endpointURL = "http://localhost:8080"

func TestLoad(t *testing.T) {
	log.SetLevel(log.InfoLevel)
	file, err := os.ReadFile("testdata/trade-agg-requests.csv")
	require.NoError(t, err)
	lines := strings.Split(string(file), "\n")[1:]
	queries := make([]string, 0, len(lines))
	for _, line := range lines {
		if len(line) == 0 {
			continue
		}
		parts := strings.Split(line, "\",\"")
		queries = append(queries, strings.Trim(parts[1], "\""))
	}
	t.Log(len(queries))
	t.Log(queries[0], queries[len(queries)-1])
	rand.Shuffle(len(queries), func(i, j int) { queries[i], queries[j] = queries[j], queries[i] })
	serverErrors := 0

	requestLatency := metrics.NewSample("request", 100000, requestsPerSecond, 10*time.Second)
	queue := make(chan string, queueSize)
	for i := 0; i < workers; i++ {
		go func() {
			for query := range queue {
				var resp *http.Response
				requestLatency.Measure(func() {
					var err error
					resp, err = http.Get(endpointURL + query)
					require.NoError(t, err)
					if resp.StatusCode != http.StatusOK {
						log.Warnf("query %v returned status %v", query, resp.StatusCode)
						if resp.StatusCode > 500 {
							serverErrors++
							t.Log("server errors", serverErrors)
						}
					}
				})
				_, err := io.ReadAll(resp.Body)
				require.NoError(t, err)
				require.NoError(t, resp.Body.Close())
			}
		}()
	}

	ticker := time.NewTicker(time.Second)
	all := queries
	totalRequests := 0
	for range ticker.C {
		for i := 0; i < requestsPerSecond; i++ {
			if len(queries) == 0 {
				queries = all
			}
			query := queries[0]
			queries = queries[1:]
			queue <- query
		}
		totalRequests += requestsPerSecond
		if totalRequests >= 10000+requestsPerSecond*2 {
			t.Log("server errors", serverErrors)
			break
		}
	}
}
