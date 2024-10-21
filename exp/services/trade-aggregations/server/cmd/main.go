package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"cloud.google.com/go/bigtable"

	"github.com/stellar/go/exp/services/trade-aggregations/ingest"
	"github.com/stellar/go/exp/services/trade-aggregations/server"
	"github.com/stellar/go/exp/services/trade-aggregations/store"
	"github.com/stellar/go/support/log"
)

const minute = 60
const hour = 60 * minute
const week = 7 * 24 * hour

func main() {
	project := flag.String("project", "", "GCP project")
	instance := flag.String("instance", "", "bigtable instance name")
	port := flag.Int("port", 8080, "port to listen on")
	flag.Parse()

	log.SetLevel(log.InfoLevel)
	ctx, cancel := context.WithCancel(context.Background())

	intervalConfig := ingest.IntervalConfig{
		AggregationInterval: minute,
		BucketInterval:      hour,
		RowKeyInterval:      week,
	}
	schema := store.BigTableSchema{
		Project:      *project,
		Instance:     *instance,
		Table:        "weekly-trade-aggregations",
		ColumnFamily: "hours",
		RowKeyer: func(assetPair string, timestamp int64) string {
			return strings.Join(
				[]string{
					"trade-aggregations",
					"hours",
					assetPair,
					fmt.Sprintf("%013d", ingest.RoundDown(timestamp, intervalConfig.RowKeyInterval)),
				},
				"#",
			)
		},
		ColKeyer: func(timestamp int64) string {
			return fmt.Sprintf("%013d", timestamp)
		},
		ProgressColumnFamily: "progress",
		ProgressRowKey: strings.Join(
			[]string{
				"trade-aggregations",
				"progress",
				"hours",
			},
			"#",
		),
		ProgressColumn: "",
	}

	client, err := bigtable.NewClient(ctx, schema.Project, schema.Instance)
	if err != nil {
		log.Fatalf("Failed to create BigTable client: %v", err)
	}

	httpServer := &http.Server{
		Addr:                         fmt.Sprintf(":%d", *port),
		Handler:                      server.NewHandler(intervalConfig, store.NewBigTableReader(client, schema)),
		DisableGeneralOptionsHandler: false,
		TLSConfig:                    nil,
		ReadTimeout:                  5 * time.Second,
	}

	// Handle OS signals to gracefully terminate the service
	sigCh := make(chan os.Signal, 1)
	defer close(sigCh)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig, ok := <-sigCh
		if ok {
			log.Infof("Received termination signal: %v", sig)
			cancel()
			httpServer.Shutdown(context.Background())
		}
	}()

	if err := httpServer.ListenAndServe(); err != nil {
		log.Fatalf("Failed to run HTTP server: %v", err)
	}
}
