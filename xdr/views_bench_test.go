package xdr_test

// views_bench_test.go holds the views-based variants of the five extraction
// benchmarks plus the correctness checks that gate them. Each function extracts
// data through the views API: Scan() cursors that walk each array element exactly
// once, Elem()/Fields() bundles whose fields are already trimmed to their exact
// wire extent (so []byte(field) is the raw bytes with no extra sizing walk),
// Bytes() for the whole-element extent, and decoded discriminants. The TestAB*
// tests assert byte/struct equality of the views output against the full-decode
// output on the real ledger so a faster-but-wrong view cannot pass; the
// full-decode path is the correctness oracle, matching the conformance harness.

import (
	"fmt"
	"os"
	"reflect"
	"testing"

	"github.com/stellar/go-stellar-sdk/xdr"
)

// --- view implementations ---

// extractAllEventsView mirrors extractAllEventsFullDecode. It uses the Scan()
// cursor over the TxProcessing array for the sticky-error + count-validated iteration,
// then navigates each element to the deep events leaf with LAZY single-field
// accessors (see extractEventsFromTxView) rather than Fields() bundles.
func extractAllEventsView(data []byte) ([]DBEvent, error) {
	v1, err := xdr.LedgerCloseMetaView(data).V1()
	if err != nil {
		return nil, err
	}
	txArr, err := v1.TxProcessing()
	if err != nil {
		return nil, err
	}

	var results []DBEvent
	c := txArr.Scan()
	for c.Next() {
		events, err := extractEventsFromTxView(c.Elem())
		if err != nil {
			return nil, err
		}
		results = append(results, events...)
	}
	if err := c.Err(); err != nil {
		return nil, err
	}
	return results, nil
}

// extractEventsByHashView mirrors extractEventsByHashFullDecode: Scan() the tx
// array, reading just the hash leaf lazily for the comparison, break early on the
// matching hash, then extract that tx's events (also via lazy navigation).
func extractEventsByHashView(data []byte, targetHash xdr.Hash) ([]DBEvent, error) {
	v1, err := xdr.LedgerCloseMetaView(data).V1()
	if err != nil {
		return nil, err
	}
	txArr, err := v1.TxProcessing()
	if err != nil {
		return nil, err
	}

	c := txArr.Scan()
	for c.Next() {
		elem := c.Elem()
		// Lazy: read just the hash leaf for the comparison during the scan.
		hashView, err := elem.Result.TransactionHash()
		if err != nil {
			return nil, err
		}
		// Value() returns the typed Hash array.
		hashBytes, err := hashView.Value()
		if err != nil {
			return nil, err
		}
		if hashBytes != targetHash {
			continue
		}
		return extractEventsFromTxView(elem)
	}
	if err := c.Err(); err != nil {
		return nil, err
	}
	return nil, nil // not found
}

// extractEventsFromTxView extracts all contract events from a single located
// TransactionResultMeta, matching the full-decode path field-for-field.
//
// Navigation idiom: LAZY single-field accessors for the deep walk down to the
// events array. The Scan() cursor already located the element's Result and
// TxApplyProcessing fields; from there we read ONE leaf per node (the hash, the
// result code, then SorobanMeta, then Events), so Fields()/bundle would be
// wasted work — it would trim and store sibling fields (V3's Operations and two
// LedgerEntryChanges, SorobanMeta's ReturnValue/DiagnosticEvents) we never read.
// The inner per-event loop, by contrast, consumes SEVERAL fields of each event
// (Type, ContractId, Body, whole-event View) and so uses the Elem() bundle.
func extractEventsFromTxView(elem xdr.TransactionResultMetaFields) ([]DBEvent, error) {
	txHash, txSuccess, err := readTxHashAndSuccess(elem)
	if err != nil {
		return nil, err
	}

	// TxApplyProcessing is a union; V() now returns the decoded discriminant
	// directly.
	metaVVal, err := elem.TxApplyProcessing.V()
	if err != nil {
		return nil, err
	}
	if metaVVal != 3 {
		return nil, nil
	}
	v3, err := elem.TxApplyProcessing.V3()
	if err != nil {
		return nil, err
	}

	// Lazy: jump straight to SorobanMeta (one field of V3), then Events (one
	// field of SorobanMeta) — no Fields() bundle at either level.
	sorobanOpt, err := v3.SorobanMeta()
	if err != nil {
		return nil, err
	}
	sorobanMeta, present, err := sorobanOpt.Unwrap()
	if err != nil {
		return nil, err
	}
	if !present {
		return nil, nil
	}
	eventsArr, err := sorobanMeta.Events()
	if err != nil {
		return nil, err
	}

	var results []DBEvent
	ec := eventsArr.Scan()
	for ec.Next() {
		dbEvent, err := extractDBEvent(ec.Elem(), txHash, txSuccess)
		if err != nil {
			return nil, err
		}
		results = append(results, dbEvent)
	}
	if err := ec.Err(); err != nil {
		return nil, err
	}

	return results, nil
}

// readTxHashAndSuccess reads the tx hash and success flag from the (trimmed)
// Result pair, lazily one field at a time.
func readTxHashAndSuccess(elem xdr.TransactionResultMetaFields) (txHash [32]byte, txSuccess bool, err error) {
	hashView, err := elem.Result.TransactionHash()
	if err != nil {
		return txHash, false, err
	}
	hashBytes, err := hashView.Value()
	if err != nil {
		return txHash, false, err
	}
	txHash = [32]byte(hashBytes)

	txResult, err := elem.Result.Result()
	if err != nil {
		return txHash, false, err
	}
	resultResult, err := txResult.Result()
	if err != nil {
		return txHash, false, err
	}
	// Code() is the union discriminant; it returns the decoded enum value
	// directly.
	code, err := resultResult.Code()
	if err != nil {
		return txHash, false, err
	}
	return txHash, code == xdr.TransactionResultCodeTxSuccess, nil
}

// extractDBEvent materializes one DBEvent from a located Soroban event element,
// mirroring the full-decode ingestion path: event type, optional contract id, the
// full event XDR (bool discriminant prefixed), and topics 1-4 as XDR blobs.
func extractDBEvent(event xdr.ContractEventFields, txHash [32]byte, txSuccess bool) (DBEvent, error) {
	dbEvent := DBEvent{TxHash: txHash, TxSuccess: txSuccess}

	// Event type.
	evTypeVal, err := event.Type.Value()
	if err != nil {
		return DBEvent{}, err
	}
	dbEvent.EventType = int32(evTypeVal)

	// Contract ID (optional).
	cidView, present, err := event.ContractId.Unwrap()
	if err != nil {
		return DBEvent{}, err
	}
	if present {
		// Value() returns a typed Hash array; slice it for the
		// []byte column. The array is a copy, so the slice is safe to retain past
		// the source buffer.
		cid, err := cidView.Value()
		if err != nil {
			return DBEvent{}, err
		}
		dbEvent.ContractID = cid[:]
	}

	// Full event XDR — prepend the bool discriminant to the ContractEvent bytes,
	// exactly as the full-decode path does. event.View is the trimmed whole-event
	// extent (zero-cost from the Scan capture).
	eventRaw := []byte(event.View)
	diagXDR := make([]byte, 4+len(eventRaw))
	if txSuccess {
		diagXDR[3] = 1
	}
	copy(diagXDR[4:], eventRaw)
	dbEvent.EventXDR = diagXDR

	// Topics 1-4 as individual XDR blobs. Body.V() returns the decoded
	// discriminant directly.
	bodyVVal, err := event.Body.V()
	if err != nil {
		return DBEvent{}, err
	}
	if bodyVVal == 0 {
		if err := extractEventTopics(event, &dbEvent); err != nil {
			return DBEvent{}, err
		}
	}
	return dbEvent, nil
}

// extractEventTopics reads up to four V0 topic blobs into dbEvent.Topics.
func extractEventTopics(event xdr.ContractEventFields, dbEvent *DBEvent) error {
	v0, err := event.Body.V0()
	if err != nil {
		return err
	}
	// Lazy: only Topics is needed (Data is unused), so jump straight to it rather
	// than locating the whole V0 bundle.
	topicsArr, err := v0.Topics()
	if err != nil {
		return err
	}
	tc := topicsArr.Scan()
	for j := 0; tc.Next() && j < 4; j++ {
		dbEvent.Topics[j] = tc.Bytes()
	}
	return tc.Err()
}

// findByHashView mirrors findByHashFullDecode using the Scan() cursor with early break.
//
// Navigation idiom: MIX. During the scan we read only the hash LEAF lazily for
// the comparison (cheap — no Fields() bundle on the non-matching elements). On
// the match we materialize three fields (Result, FeeProcessing, TxApplyProcessing)
// straight from the cursor's element bundle — the cursor already located and
// trimmed them in Next(), so []byte(field) is the raw element bytes with no extra
// walk. The bundle captured in Next() is already in hand, so matching needs no
// re-walk of the element to extract these fields.
func findByHashView(data []byte, targetHash xdr.Hash) (TransactionResponse, error) {
	v1, err := xdr.LedgerCloseMetaView(data).V1()
	if err != nil {
		return TransactionResponse{}, err
	}

	// Ledger version (read once before scanning).
	hdr, err := v1.LedgerHeader()
	if err != nil {
		return TransactionResponse{}, err
	}
	header, err := hdr.Header()
	if err != nil {
		return TransactionResponse{}, err
	}
	ledgerVersion, err := header.LedgerVersion()
	if err != nil {
		return TransactionResponse{}, err
	}
	ledgerVersionVal, err := ledgerVersion.Value()
	if err != nil {
		return TransactionResponse{}, err
	}

	txArr, err := v1.TxProcessing()
	if err != nil {
		return TransactionResponse{}, err
	}

	c := txArr.Scan()
	for c.Next() {
		elem := c.Elem()
		// Lazy: read just the hash leaf for the comparison during the scan.
		hashView, err := elem.Result.TransactionHash()
		if err != nil {
			return TransactionResponse{}, err
		}
		hashBytes, err := hashView.Value()
		if err != nil {
			return TransactionResponse{}, err
		}
		if hashBytes != targetHash {
			continue
		}

		// Found it — materialize from the cursor's element bundle. The located
		// fields are already trimmed to their wire extent, so []byte(field) is
		// the raw element bytes (no extra walk).
		return TransactionResponse{
			// c.Index() is the cursor's current 0-based position: >= 0 and bounded
			// by the array's wire-validated element count (a ledger's tx count),
			// so +1 is well within uint32.
			//nolint:gosec // G115: bounded by validated wire element count, never overflows.
			Index:         uint32(c.Index() + 1),
			Result:        []byte(elem.Result),
			FeeChanges:    []byte(elem.FeeProcessing),
			UnsafeMeta:    []byte(elem.TxApplyProcessing),
			LedgerVersion: ledgerVersionVal,
			Hash:          hashBytes,
		}, nil
	}
	if err := c.Err(); err != nil {
		return TransactionResponse{}, err
	}
	return TransactionResponse{}, fmt.Errorf("transaction not found")
}

// extractAllTxView mirrors extractAllTxFullDecode using the Scan() cursor.
//
// Navigation idiom: Scan() + Elem() bundle — this is the multi-field
// materialization case. Each element yields THREE whole-field raw blobs (Result,
// FeeProcessing, TxApplyProcessing) which the cursor already located and trimmed
// in one Next() walk, so []byte(field) is zero extra work. The tx hash is a
// single leaf, so it is read with the lazy accessor rather than re-locating the
// Result bundle.
func extractAllTxView(data []byte) ([]TxSummary, error) {
	v1, err := xdr.LedgerCloseMetaView(data).V1()
	if err != nil {
		return nil, err
	}
	txArr, err := v1.TxProcessing()
	if err != nil {
		return nil, err
	}

	c := txArr.Scan()
	results := make([]TxSummary, 0, c.Count())
	for c.Next() {
		elem := c.Elem()
		hashView, err := elem.Result.TransactionHash()
		if err != nil {
			return nil, err
		}
		hashBytes, err := hashView.Value()
		if err != nil {
			return nil, err
		}

		results = append(results, TxSummary{
			Hash:      [32]byte(hashBytes),
			ResultRaw: []byte(elem.Result),
			FeeRaw:    []byte(elem.FeeProcessing),
			MetaRaw:   []byte(elem.TxApplyProcessing),
		})
	}
	if err := c.Err(); err != nil {
		return nil, err
	}
	return results, nil
}

// extractAllHashesView mirrors extractAllHashesFullDecode. The Scan() cursor drives the
// count-validated iteration; each element needs only ONE leaf (the tx hash), so
// it is reached with the LAZY single-field accessor rather than a Fields() bundle
// that would also locate the sibling Result.
func extractAllHashesView(data []byte) ([][32]byte, error) {
	v1, err := xdr.LedgerCloseMetaView(data).V1()
	if err != nil {
		return nil, err
	}
	txArr, err := v1.TxProcessing()
	if err != nil {
		return nil, err
	}

	c := txArr.Scan()
	hashes := make([][32]byte, 0, c.Count())
	for c.Next() {
		hashView, err := c.Elem().Result.TransactionHash()
		if err != nil {
			return nil, err
		}
		hashBytes, err := hashView.Value()
		if err != nil {
			return nil, err
		}

		hashes = append(hashes, [32]byte(hashBytes))
	}
	if err := c.Err(); err != nil {
		return nil, err
	}
	return hashes, nil
}

// --- correctness checks: views output must equal full_decode output on the real ledger ---

func loadABLedger(tb testing.TB) []byte {
	tb.Helper()
	data, err := os.ReadFile("testdata/ledger_58752000.bin")
	if err != nil {
		tb.Skipf("testdata not available: %v", err)
	}
	return data
}

func TestABExtractAllEvents(t *testing.T) {
	data := loadABLedger(t)
	want, err := extractAllEventsFullDecode(data)
	if err != nil {
		t.Fatalf("full_decode: %v", err)
	}
	got, err := extractAllEventsView(data)
	if err != nil {
		t.Fatalf("views: %v", err)
	}
	if len(want) == 0 {
		t.Fatal("no events extracted (degenerate)")
	}
	if !reflect.DeepEqual(want, got) {
		t.Fatalf("views events differ from full_decode: full_decode=%d events, views=%d events", len(want), len(got))
	}
	t.Logf("verified %d DBEvents identical (full_decode vs views)", len(want))
}

func TestABExtractEventsByTxHash(t *testing.T) {
	data := loadABLedger(t)
	var lcm xdr.LedgerCloseMeta
	if err := xdr.SafeUnmarshal(data, &lcm); err != nil {
		t.Fatal(err)
	}
	// Pick a tx with events, same selection as the benchmark.
	var targetHash xdr.Hash
	found := false
	v1 := lcm.MustV1()
	for i := range v1.TxProcessing {
		meta := v1.TxProcessing[i].TxApplyProcessing
		if meta.V == 3 && meta.MustV3().SorobanMeta != nil && len(meta.MustV3().SorobanMeta.Events) > 0 {
			targetHash = v1.TxProcessing[i].Result.TransactionHash
			found = true
			break
		}
	}
	if !found {
		t.Skip("no transactions with events in test data")
	}

	want, err := extractEventsByHashFullDecode(data, targetHash)
	if err != nil {
		t.Fatalf("full_decode: %v", err)
	}
	got, err := extractEventsByHashView(data, targetHash)
	if err != nil {
		t.Fatalf("views: %v", err)
	}
	if len(want) == 0 {
		t.Fatal("no events extracted for target (degenerate)")
	}
	if !reflect.DeepEqual(want, got) {
		t.Fatalf("views events-by-hash differ from full_decode: full_decode=%d, views=%d", len(want), len(got))
	}
	t.Logf("verified %d DBEvents identical for target tx (full_decode vs views)", len(want))
}

func TestABFindTransactionByHash(t *testing.T) {
	data := loadABLedger(t)
	var lcm xdr.LedgerCloseMeta
	if err := xdr.SafeUnmarshal(data, &lcm); err != nil {
		t.Fatal(err)
	}
	numTx := lcm.CountTransactions()
	if numTx == 0 {
		t.Fatal("no transactions in test data")
	}
	v1 := lcm.MustV1()
	// Cover early/mid/late positions like the benchmark.
	for _, idx := range []int{min(numTx-1, 10), numTx / 2, numTx - 10} {
		if idx < 0 || idx >= numTx {
			continue
		}
		targetHash := v1.TxProcessing[idx].Result.TransactionHash
		want, err := findByHashFullDecode(data, targetHash)
		if err != nil {
			t.Fatalf("full_decode idx=%d: %v", idx, err)
		}
		got, err := findByHashView(data, targetHash)
		if err != nil {
			t.Fatalf("views idx=%d: %v", idx, err)
		}
		if !reflect.DeepEqual(want, got) {
			t.Fatalf("views TransactionResponse differs from full_decode at idx=%d:\n full_decode=%+v\n views=%+v", idx, want, got)
		}
	}
	t.Log("verified TransactionResponse identical at early/mid/late (full_decode vs views)")
}

func TestABExtractAllTransactions(t *testing.T) {
	data := loadABLedger(t)
	want, err := extractAllTxFullDecode(data)
	if err != nil {
		t.Fatalf("full_decode: %v", err)
	}
	got, err := extractAllTxView(data)
	if err != nil {
		t.Fatalf("views: %v", err)
	}
	if len(want) == 0 {
		t.Fatal("no transactions extracted (degenerate)")
	}
	if !reflect.DeepEqual(want, got) {
		t.Fatalf("views TxSummary slice differs from full_decode: full_decode=%d, views=%d", len(want), len(got))
	}
	t.Logf("verified %d TxSummary identical (full_decode vs views)", len(want))
}

func TestABExtractAllHashes(t *testing.T) {
	data := loadABLedger(t)
	want, err := extractAllHashesFullDecode(data)
	if err != nil {
		t.Fatalf("full_decode: %v", err)
	}
	got, err := extractAllHashesView(data)
	if err != nil {
		t.Fatalf("views: %v", err)
	}
	if len(want) == 0 {
		t.Fatal("no hashes extracted (degenerate)")
	}
	if !reflect.DeepEqual(want, got) {
		t.Fatalf("views hashes differ from full_decode: full_decode=%d, views=%d", len(want), len(got))
	}
	t.Logf("verified %d hashes identical (full_decode vs views)", len(want))
}
