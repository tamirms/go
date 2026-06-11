package xdr_test

import (
	"os"
	"testing"

	"github.com/stellar/go-stellar-sdk/xdr"
)

// DBEvent mirrors the data stored per event row in stellar-rpc's DB.
type DBEvent struct {
	ContractID []byte    // 32 bytes or nil
	EventType  int32     // ContractEventType enum
	EventXDR   []byte    // full DiagnosticEvent XDR (for event_data column)
	Topics     [4][]byte // topics 1-4 as individual ScVal XDR (for filtering)
	TxHash     [32]byte
	TxSuccess  bool
}

// BenchmarkExtractAllEvents extracts all contract events from a ledger,
// mirroring the stellar-rpc ingestion pattern.
func BenchmarkExtractAllEvents(b *testing.B) {
	data, err := os.ReadFile("testdata/ledger_58752000.bin")
	if err != nil {
		b.Fatalf("testdata not found: %v", err)
	}

	var lcm xdr.LedgerCloseMeta
	if err := xdr.SafeUnmarshal(data, &lcm); err != nil {
		b.Fatal(err)
	}
	numTx := lcm.CountTransactions()

	// Count events for logging
	totalEvents := 0
	v1 := lcm.MustV1()
	for i := 0; i < numTx; i++ {
		meta := v1.TxProcessing[i].TxApplyProcessing
		if meta.V == 3 {
			v3 := meta.MustV3()
			if v3.SorobanMeta != nil {
				totalEvents += len(v3.SorobanMeta.Events)
			}
		}
	}
	b.Logf("Ledger: v%d, %d txs, %d events, %d bytes", lcm.V, numTx, totalEvents, len(data))

	b.Run("full_decode", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			events, err := extractAllEventsFullDecode(data)
			if err != nil {
				b.Fatal(err)
			}
			_ = events
		}
	})

	b.Run("views", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			events, err := extractAllEventsView(data)
			if err != nil {
				b.Fatal(err)
			}
			_ = events
		}
	})
}

// BenchmarkExtractEventsByTxHash finds a transaction by hash, then extracts
// all its events.
func BenchmarkExtractEventsByTxHash(b *testing.B) {
	data, err := os.ReadFile("testdata/ledger_58752000.bin")
	if err != nil {
		b.Fatalf("testdata not found: %v", err)
	}

	var lcm xdr.LedgerCloseMeta
	if err := xdr.SafeUnmarshal(data, &lcm); err != nil {
		b.Fatal(err)
	}

	// Find a transaction with events
	v1 := lcm.MustV1()
	var targetHash xdr.Hash
	targetEvents := 0
	for i := range v1.TxProcessing {
		meta := v1.TxProcessing[i].TxApplyProcessing
		if meta.V == 3 && meta.MustV3().SorobanMeta != nil && len(meta.MustV3().SorobanMeta.Events) > 0 {
			targetHash = v1.TxProcessing[i].Result.TransactionHash
			targetEvents = len(meta.MustV3().SorobanMeta.Events)
			break
		}
	}
	if targetEvents == 0 {
		b.Skip("no transactions with events in test data")
	}
	b.Logf("Target tx hash=%x..., %d events", targetHash[:4], targetEvents)

	b.Run("full_decode", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			events, err := extractEventsByHashFullDecode(data, targetHash)
			if err != nil {
				b.Fatal(err)
			}
			_ = events
		}
	})

	b.Run("views", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			events, err := extractEventsByHashView(data, targetHash)
			if err != nil {
				b.Fatal(err)
			}
			_ = events
		}
	})
}

// --- Full decode implementations ---

func extractAllEventsFullDecode(data []byte) ([]DBEvent, error) {
	var lcm xdr.LedgerCloseMeta
	if err := xdr.SafeUnmarshal(data, &lcm); err != nil {
		return nil, err
	}

	v1 := lcm.MustV1()
	var results []DBEvent

	for _, tx := range v1.TxProcessing {
		meta := tx.TxApplyProcessing
		if meta.V != 3 {
			continue
		}
		v3 := meta.MustV3()
		if v3.SorobanMeta == nil {
			continue
		}

		txHash := tx.Result.TransactionHash
		// Determine success from result code
		txSuccess := tx.Result.Result.Result.Code == xdr.TransactionResultCodeTxSuccess

		for _, event := range v3.SorobanMeta.Events {
			dbEvent := DBEvent{
				EventType: int32(event.Type),
				TxHash:    txHash,
				TxSuccess: txSuccess,
			}

			if event.ContractId != nil {
				dbEvent.ContractID = event.ContractId[:]
			}

			// Full DiagnosticEvent XDR
			diagEvent := xdr.DiagnosticEvent{
				InSuccessfulContractCall: txSuccess,
				Event:                    event,
			}
			eventXDR, err := diagEvent.MarshalBinary()
			if err != nil {
				return nil, err
			}
			dbEvent.EventXDR = eventXDR

			// Topics 1-4
			if event.Body.V == 0 && event.Body.V0 != nil {
				for j := 0; j < len(event.Body.V0.Topics) && j < 4; j++ {
					topicXDR, err := event.Body.V0.Topics[j].MarshalBinary()
					if err != nil {
						return nil, err
					}
					dbEvent.Topics[j] = topicXDR
				}
			}

			results = append(results, dbEvent)
		}
	}

	return results, nil
}

func extractEventsByHashFullDecode(data []byte, targetHash xdr.Hash) ([]DBEvent, error) {
	var lcm xdr.LedgerCloseMeta
	if err := xdr.SafeUnmarshal(data, &lcm); err != nil {
		return nil, err
	}

	v1 := lcm.MustV1()
	for _, tx := range v1.TxProcessing {
		if tx.Result.TransactionHash != targetHash {
			continue
		}

		meta := tx.TxApplyProcessing
		if meta.V != 3 {
			return nil, nil
		}
		v3 := meta.MustV3()
		if v3.SorobanMeta == nil {
			return nil, nil
		}

		txSuccess := tx.Result.Result.Result.Code == xdr.TransactionResultCodeTxSuccess
		var results []DBEvent

		for _, event := range v3.SorobanMeta.Events {
			dbEvent := DBEvent{
				EventType: int32(event.Type),
				TxHash:    targetHash,
				TxSuccess: txSuccess,
			}

			if event.ContractId != nil {
				dbEvent.ContractID = event.ContractId[:]
			}

			diagEvent := xdr.DiagnosticEvent{
				InSuccessfulContractCall: txSuccess,
				Event:                    event,
			}
			eventXDR, err := diagEvent.MarshalBinary()
			if err != nil {
				return nil, err
			}
			dbEvent.EventXDR = eventXDR

			if event.Body.V == 0 && event.Body.V0 != nil {
				for j := 0; j < len(event.Body.V0.Topics) && j < 4; j++ {
					topicXDR, err := event.Body.V0.Topics[j].MarshalBinary()
					if err != nil {
						return nil, err
					}
					dbEvent.Topics[j] = topicXDR
				}
			}

			results = append(results, dbEvent)
		}
		return results, nil
	}

	return nil, nil // tx not found
}
