package xdr_test

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/stellar/go-stellar-sdk/xdr"
)

// chunkReader reads zstd-compressed LedgerCloseMeta blobs from the stellar
// chunk file format (index + data file pair). Reimplements
// events-analysis/reader.go using pure-Go zstd to avoid CGO.
type chunkReader struct {
	dataFile *os.File
	offsets  []uint64
	decoder  *zstd.Decoder
	compBuf  []byte
}

func openChunk(indexPath, dataPath string) (*chunkReader, error) {
	indexData, err := os.ReadFile(indexPath)
	if err != nil {
		return nil, fmt.Errorf("read index: %w", err)
	}
	if len(indexData) < 8 {
		return nil, fmt.Errorf("index too small")
	}
	if indexData[0] != 1 {
		return nil, fmt.Errorf("unsupported index version %d", indexData[0])
	}
	offsetSize := int(indexData[1])
	if offsetSize != 4 && offsetSize != 8 {
		return nil, fmt.Errorf("invalid offset size %d", offsetSize)
	}
	numOffsets := (len(indexData) - 8) / offsetSize
	offsets := make([]uint64, numOffsets)
	for i := range numOffsets {
		base := 8 + i*offsetSize
		if offsetSize == 4 {
			offsets[i] = uint64(binary.LittleEndian.Uint32(indexData[base:]))
		} else {
			offsets[i] = binary.LittleEndian.Uint64(indexData[base:])
		}
	}
	df, err := os.Open(dataPath)
	if err != nil {
		return nil, fmt.Errorf("open data: %w", err)
	}
	dec, err := zstd.NewReader(nil)
	if err != nil {
		df.Close()
		return nil, err
	}
	return &chunkReader{dataFile: df, offsets: offsets, decoder: dec}, nil
}

func (r *chunkReader) numLedgers() int { return len(r.offsets) - 1 }

func (r *chunkReader) readLedger(idx int) ([]byte, error) {
	if idx < 0 || idx >= r.numLedgers() {
		return nil, fmt.Errorf("index %d out of range [0, %d)", idx, r.numLedgers())
	}
	start, end := r.offsets[idx], r.offsets[idx+1]
	size := int(end - start)
	if size > cap(r.compBuf) {
		r.compBuf = make([]byte, size)
	} else {
		r.compBuf = r.compBuf[:size]
	}
	if _, err := r.dataFile.ReadAt(r.compBuf, int64(start)); err != nil {
		return nil, fmt.Errorf("read ledger %d: %w", idx, err)
	}
	return r.decoder.DecodeAll(r.compBuf, nil)
}

func (r *chunkReader) close() {
	r.decoder.Close()
	r.dataFile.Close()
}

// sampleLedgers loads a deterministic random sample of decompressed LedgerCloseMeta blobs.
// Defaults: 1000 ledgers from /Users/tamir/events-analysis.
// Override with VIEW_BENCH_SAMPLE (count) and VIEW_BENCH_DIR (path).
func sampleLedgers(tb testing.TB) [][]byte {
	tb.Helper()
	dir := os.Getenv("VIEW_BENCH_DIR")
	if dir == "" {
		dir = "/Users/tamir/events-analysis"
	}
	indexPath := filepath.Join(dir, "006016.index")
	dataPath := filepath.Join(dir, "006016.data")
	if _, err := os.Stat(indexPath); os.IsNotExist(err) {
		tb.Skipf("chunk files not found at %s (set VIEW_BENCH_DIR)", dir)
	}
	reader, err := openChunk(indexPath, dataPath)
	if err != nil {
		tb.Fatal(err)
	}
	defer reader.close()

	total := reader.numLedgers()
	n := 5000
	if s := os.Getenv("VIEW_BENCH_SAMPLE"); s != "" {
		if v, err := strconv.Atoi(s); err == nil && v > 0 {
			n = v
		}
	}
	if n > total {
		n = total
	}

	rng := rand.New(rand.NewSource(42))
	perm := rng.Perm(total)[:n]
	sort.Ints(perm)

	tb.Logf("Loading %d/%d ledgers from %s...", n, total, dir)
	var ledgers [][]byte
	totalBytes := 0
	versions := map[uint32]int{}
	for _, idx := range perm {
		data, err := reader.readLedger(idx)
		if err != nil {
			tb.Fatalf("ledger %d: %v", idx, err)
		}
		if len(data) < 4 {
			continue
		}
		disc := binary.BigEndian.Uint32(data[:4])
		versions[disc]++
		// Only keep V1 and V2 ledgers (the ones we can benchmark).
		if disc != 1 && disc != 2 {
			continue
		}
		ledgers = append(ledgers, append([]byte(nil), data...))
		totalBytes += len(data)
	}
	tb.Logf("Loaded: %d ledgers, %.1f MB total, %.0f KB avg (versions: %v)",
		len(ledgers), float64(totalBytes)/(1<<20), float64(totalBytes)/float64(max(1, len(ledgers)))/(1<<10), versions)
	if len(ledgers) == 0 {
		tb.Skip("no usable ledgers found")
	}
	return ledgers
}

func lcmVersion(data []byte) uint32 {
	return binary.BigEndian.Uint32(data[:4])
}

// --- Version-dispatching operations ---

func sampleExtractAllHashes(data []byte) ([][32]byte, error) {
	switch lcmVersion(data) {
	case 1:
		return extractAllHashesView(data)
	case 2:
		return extractAllHashesViewV2(data)
	default:
		return nil, fmt.Errorf("unsupported version %d", lcmVersion(data))
	}
}

func sampleExtractAllHashesFullDecode(data []byte) ([][32]byte, error) {
	switch lcmVersion(data) {
	case 1:
		return extractAllHashesFullDecode(data)
	case 2:
		return extractAllHashesFullDecodeV2(data)
	default:
		return nil, fmt.Errorf("unsupported version %d", lcmVersion(data))
	}
}

func sampleFindHash(data []byte, target [32]byte) ([]byte, error) {
	switch lcmVersion(data) {
	case 1:
		return findHashView(data, target)
	case 2:
		return findHashViewV2(data, target)
	default:
		return nil, fmt.Errorf("unsupported version %d", lcmVersion(data))
	}
}

func sampleFindHashFullDecode(data []byte, target [32]byte) ([]byte, error) {
	switch lcmVersion(data) {
	case 1:
		return findHashFullDecodeV1(data, target)
	case 2:
		return findHashFullDecodeV2(data, target)
	default:
		return nil, fmt.Errorf("unsupported version %d", lcmVersion(data))
	}
}

func sampleExtractEventsByHash(data []byte, target [32]byte) (int, error) {
	switch lcmVersion(data) {
	case 1:
		events, err := extractEventsByHashView(data, xdr.Hash(target))
		return len(events), err
	case 2:
		return extractEventsByHashViewV2(data, target)
	default:
		return 0, fmt.Errorf("unsupported version %d", lcmVersion(data))
	}
}

func sampleExtractEventsByHashFullDecode(data []byte, target [32]byte) (int, error) {
	switch lcmVersion(data) {
	case 1:
		events, err := extractEventsByHashFullDecode(data, xdr.Hash(target))
		return len(events), err
	case 2:
		return extractEventsByHashFullDecodeV2(data, target)
	default:
		return 0, fmt.Errorf("unsupported version %d", lcmVersion(data))
	}
}

func sampleExtractAllTx(data []byte) (int, error) {
	switch lcmVersion(data) {
	case 1:
		txs, err := extractAllTxView(data)
		return len(txs), err
	case 2:
		return extractAllTxViewV2(data)
	default:
		return 0, fmt.Errorf("unsupported version %d", lcmVersion(data))
	}
}

func sampleExtractAllTxFullDecode(data []byte) (int, error) {
	switch lcmVersion(data) {
	case 1:
		txs, err := extractAllTxFullDecode(data)
		return len(txs), err
	case 2:
		return extractAllTxFullDecodeV2(data)
	default:
		return 0, fmt.Errorf("unsupported version %d", lcmVersion(data))
	}
}

func sampleExtractAllEvents(data []byte) (int, error) {
	switch lcmVersion(data) {
	case 1:
		events, err := extractAllEventsView(data)
		return len(events), err
	case 2:
		return extractAllEventsViewV2(data)
	default:
		return 0, fmt.Errorf("unsupported version %d", lcmVersion(data))
	}
}

func sampleExtractAllEventsFullDecode(data []byte) (int, error) {
	switch lcmVersion(data) {
	case 1:
		events, err := extractAllEventsFullDecode(data)
		return len(events), err
	case 2:
		return extractAllEventsFullDecodeV2(data)
	default:
		return 0, fmt.Errorf("unsupported version %d", lcmVersion(data))
	}
}

func sampleTxCount(data []byte) (int, error) {
	view := xdr.LedgerCloseMetaView(data)
	switch lcmVersion(data) {
	case 1:
		v1, err := view.V1()
		if err != nil {
			return 0, err
		}
		txArr, err := v1.TxProcessing()
		if err != nil {
			return 0, err
		}
		return txArr.Count()
	case 2:
		v2, err := view.V2()
		if err != nil {
			return 0, err
		}
		txArr, err := v2.TxProcessing()
		if err != nil {
			return 0, err
		}
		return txArr.Count()
	default:
		return 0, fmt.Errorf("unsupported version %d", lcmVersion(data))
	}
}

// --- V2 view implementations ---

func extractAllHashesViewV2(data []byte) ([][32]byte, error) {
	view := xdr.LedgerCloseMetaView(data)
	v2, err := view.V2()
	if err != nil {
		return nil, err
	}
	txArr, err := v2.TxProcessing()
	if err != nil {
		return nil, err
	}
	txCount, err := txArr.Count()
	if err != nil {
		return nil, err
	}
	hashes := make([][32]byte, 0, txCount)
	for tx, iterErr := range txArr.Iter() {
		if iterErr != nil {
			return nil, iterErr
		}
		resultView, err := tx.Result()
		if err != nil {
			return nil, err
		}
		hashView, err := resultView.TransactionHash()
		if err != nil {
			return nil, err
		}
		hashBytes, err := hashView.Value()
		if err != nil {
			return nil, err
		}
		var hash [32]byte
		copy(hash[:], hashBytes)
		hashes = append(hashes, hash)
	}
	return hashes, nil
}

func findHashView(data []byte, target [32]byte) ([]byte, error) {
	view := xdr.LedgerCloseMetaView(data)
	v1, err := view.V1()
	if err != nil {
		return nil, err
	}
	txArr, err := v1.TxProcessing()
	if err != nil {
		return nil, err
	}
	for tx, iterErr := range txArr.Iter() {
		if iterErr != nil {
			return nil, iterErr
		}
		resultView, err := tx.Result()
		if err != nil {
			return nil, err
		}
		hashView, err := resultView.TransactionHash()
		if err != nil {
			return nil, err
		}
		hashBytes, err := hashView.Value()
		if err != nil {
			return nil, err
		}
		if bytes.Equal(hashBytes, target[:]) {
			return resultView.Raw()
		}
	}
	return nil, nil
}

func findHashViewV2(data []byte, target [32]byte) ([]byte, error) {
	view := xdr.LedgerCloseMetaView(data)
	v2, err := view.V2()
	if err != nil {
		return nil, err
	}
	txArr, err := v2.TxProcessing()
	if err != nil {
		return nil, err
	}
	for tx, iterErr := range txArr.Iter() {
		if iterErr != nil {
			return nil, iterErr
		}
		resultView, err := tx.Result()
		if err != nil {
			return nil, err
		}
		hashView, err := resultView.TransactionHash()
		if err != nil {
			return nil, err
		}
		hashBytes, err := hashView.Value()
		if err != nil {
			return nil, err
		}
		if bytes.Equal(hashBytes, target[:]) {
			return resultView.Raw()
		}
	}
	return nil, nil
}

func extractAllTxViewV2(data []byte) (int, error) {
	view := xdr.LedgerCloseMetaView(data)
	v2, err := view.V2()
	if err != nil {
		return 0, err
	}
	txArr, err := v2.TxProcessing()
	if err != nil {
		return 0, err
	}
	count := 0
	for tx, iterErr := range txArr.Iter() {
		if iterErr != nil {
			return 0, iterErr
		}
		resultView, err := tx.Result()
		if err != nil {
			return 0, err
		}
		if _, err := resultView.Raw(); err != nil {
			return 0, err
		}
		feeView, err := tx.FeeProcessing()
		if err != nil {
			return 0, err
		}
		if _, err := feeView.Raw(); err != nil {
			return 0, err
		}
		metaView, err := tx.TxApplyProcessing()
		if err != nil {
			return 0, err
		}
		if _, err := metaView.Raw(); err != nil {
			return 0, err
		}
		count++
	}
	return count, nil
}

// --- V2 full-decode implementations ---

func extractAllHashesFullDecodeV2(data []byte) ([][32]byte, error) {
	var lcm xdr.LedgerCloseMeta
	if err := xdr.SafeUnmarshal(data, &lcm); err != nil {
		return nil, err
	}
	v2 := lcm.MustV2()
	hashes := make([][32]byte, len(v2.TxProcessing))
	for i, tx := range v2.TxProcessing {
		hashes[i] = tx.Result.TransactionHash
	}
	return hashes, nil
}

func findHashFullDecodeV1(data []byte, target [32]byte) ([]byte, error) {
	var lcm xdr.LedgerCloseMeta
	if err := xdr.SafeUnmarshal(data, &lcm); err != nil {
		return nil, err
	}
	for _, tx := range lcm.MustV1().TxProcessing {
		if tx.Result.TransactionHash == xdr.Hash(target) {
			return tx.Result.MarshalBinary()
		}
	}
	return nil, nil
}

func findHashFullDecodeV2(data []byte, target [32]byte) ([]byte, error) {
	var lcm xdr.LedgerCloseMeta
	if err := xdr.SafeUnmarshal(data, &lcm); err != nil {
		return nil, err
	}
	for _, tx := range lcm.MustV2().TxProcessing {
		if tx.Result.TransactionHash == xdr.Hash(target) {
			return tx.Result.MarshalBinary()
		}
	}
	return nil, nil
}

func extractAllTxFullDecodeV2(data []byte) (int, error) {
	var lcm xdr.LedgerCloseMeta
	if err := xdr.SafeUnmarshal(data, &lcm); err != nil {
		return 0, err
	}
	v2 := lcm.MustV2()
	for _, tx := range v2.TxProcessing {
		if _, err := tx.Result.MarshalBinary(); err != nil {
			return 0, err
		}
		if _, err := tx.FeeProcessing.MarshalBinary(); err != nil {
			return 0, err
		}
		if _, err := tx.TxApplyProcessing.MarshalBinary(); err != nil {
			return 0, err
		}
	}
	return len(v2.TxProcessing), nil
}

// --- V2 event extraction ---

func extractAllEventsViewV2(data []byte) (int, error) {
	view := xdr.LedgerCloseMetaView(data)
	v2, err := view.V2()
	if err != nil {
		return 0, err
	}
	txArr, err := v2.TxProcessing()
	if err != nil {
		return 0, err
	}
	count := 0
	for tx, iterErr := range txArr.Iter() {
		if iterErr != nil {
			return 0, iterErr
		}
		metaView, err := tx.TxApplyProcessing()
		if err != nil {
			return 0, err
		}
		metaV, err := metaView.V()
		if err != nil {
			return 0, err
		}
		metaVVal, err := metaV.Value()
		if err != nil {
			return 0, err
		}
		if metaVVal != 3 {
			continue
		}
		v3, err := metaView.V3()
		if err != nil {
			return 0, err
		}
		sorobanOpt, err := v3.SorobanMeta()
		if err != nil {
			return 0, err
		}
		sorobanMeta, present, err := sorobanOpt.Unwrap()
		if err != nil {
			return 0, err
		}
		if !present {
			continue
		}
		eventsArr, err := sorobanMeta.Events()
		if err != nil {
			return 0, err
		}
		for event, eventErr := range eventsArr.Iter() {
			if eventErr != nil {
				return 0, eventErr
			}
			if _, err := event.Raw(); err != nil {
				return 0, err
			}
			count++
		}
	}
	return count, nil
}

func extractAllEventsFullDecodeV2(data []byte) (int, error) {
	var lcm xdr.LedgerCloseMeta
	if err := xdr.SafeUnmarshal(data, &lcm); err != nil {
		return 0, err
	}
	count := 0
	for _, tx := range lcm.MustV2().TxProcessing {
		meta := tx.TxApplyProcessing
		if meta.V != 3 {
			continue
		}
		v3 := meta.MustV3()
		if v3.SorobanMeta == nil {
			continue
		}
		for _, event := range v3.SorobanMeta.Events {
			if _, err := event.MarshalBinary(); err != nil {
				return 0, err
			}
			count++
		}
	}
	return count, nil
}

func extractEventsByHashViewV2(data []byte, target [32]byte) (int, error) {
	view := xdr.LedgerCloseMetaView(data)
	v2, err := view.V2()
	if err != nil {
		return 0, err
	}
	txArr, err := v2.TxProcessing()
	if err != nil {
		return 0, err
	}
	for tx, iterErr := range txArr.Iter() {
		if iterErr != nil {
			return 0, iterErr
		}
		resultView, err := tx.Result()
		if err != nil {
			return 0, err
		}
		hashView, err := resultView.TransactionHash()
		if err != nil {
			return 0, err
		}
		hashBytes, err := hashView.Value()
		if err != nil {
			return 0, err
		}
		if !bytes.Equal(hashBytes, target[:]) {
			continue
		}
		// Found — extract events from this tx
		metaView, err := tx.TxApplyProcessing()
		if err != nil {
			return 0, err
		}
		metaV, err := metaView.V()
		if err != nil {
			return 0, err
		}
		metaVVal, err := metaV.Value()
		if err != nil {
			return 0, err
		}
		if metaVVal != 3 {
			return 0, nil
		}
		v3, err := metaView.V3()
		if err != nil {
			return 0, err
		}
		sorobanOpt, err := v3.SorobanMeta()
		if err != nil {
			return 0, err
		}
		sorobanMeta, present, err := sorobanOpt.Unwrap()
		if err != nil {
			return 0, err
		}
		if !present {
			return 0, nil
		}
		eventsArr, err := sorobanMeta.Events()
		if err != nil {
			return 0, err
		}
		count := 0
		for event, eventErr := range eventsArr.Iter() {
			if eventErr != nil {
				return 0, eventErr
			}
			if _, err := event.Raw(); err != nil {
				return 0, err
			}
			count++
		}
		return count, nil
	}
	return 0, nil
}

func extractEventsByHashFullDecodeV2(data []byte, target [32]byte) (int, error) {
	var lcm xdr.LedgerCloseMeta
	if err := xdr.SafeUnmarshal(data, &lcm); err != nil {
		return 0, err
	}
	for _, tx := range lcm.MustV2().TxProcessing {
		if tx.Result.TransactionHash != xdr.Hash(target) {
			continue
		}
		meta := tx.TxApplyProcessing
		if meta.V != 3 {
			return 0, nil
		}
		v3 := meta.MustV3()
		if v3.SorobanMeta == nil {
			return 0, nil
		}
		count := 0
		for _, event := range v3.SorobanMeta.Events {
			if _, err := event.MarshalBinary(); err != nil {
				return 0, err
			}
			count++
		}
		return count, nil
	}
	return 0, nil
}

// findEventBearingTxHash finds the hash of the first transaction with Soroban events.
// Uses full decode for simplicity — only called during setup, not benchmarked.
func findEventBearingTxHash(data []byte) [32]byte {
	var lcm xdr.LedgerCloseMeta
	if err := xdr.SafeUnmarshal(data, &lcm); err != nil {
		return [32]byte{}
	}
	switch lcm.V {
	case 1:
		for _, tx := range lcm.MustV1().TxProcessing {
			if tx.TxApplyProcessing.V == 3 {
				v3 := tx.TxApplyProcessing.MustV3()
				if v3.SorobanMeta != nil && len(v3.SorobanMeta.Events) > 0 {
					return [32]byte(tx.Result.TransactionHash)
				}
			}
		}
	case 2:
		for _, tx := range lcm.MustV2().TxProcessing {
			if tx.TxApplyProcessing.V == 3 {
				v3 := tx.TxApplyProcessing.MustV3()
				if v3.SorobanMeta != nil && len(v3.SorobanMeta.Events) > 0 {
					return [32]byte(tx.Result.TransactionHash)
				}
			}
		}
	}
	return [32]byte{}
}

// --- Helpers ---

func pctInt(sorted []int, p int) int {
	idx := len(sorted) * p / 100
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

func pctDur(sorted []time.Duration, p int) time.Duration {
	idx := len(sorted) * p / 100
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

// TestViewSampleStats reports size distributions and per-operation timing
// percentiles across a representative sample of real ledgers.
//
// Run: go test -run TestViewSampleStats -v -timeout 10m ./xdr/
func TestViewSampleStats(t *testing.T) {
	ledgers := sampleLedgers(t)

	// Size distribution
	sizes := make([]int, len(ledgers))
	for i, l := range ledgers {
		sizes[i] = len(l)
	}
	sort.Ints(sizes)
	t.Logf("Size (bytes): min=%d p25=%d p50=%d p75=%d p99=%d max=%d",
		sizes[0], pctInt(sizes, 25), pctInt(sizes, 50),
		pctInt(sizes, 75), pctInt(sizes, 99), sizes[len(sizes)-1])

	// Tx count distribution (via views, zero-alloc)
	txCounts := make([]int, len(ledgers))
	for i, l := range ledgers {
		count, err := sampleTxCount(l)
		if err != nil {
			t.Fatalf("ledger %d: tx count: %v", i, err)
		}
		txCounts[i] = count
	}
	sort.Ints(txCounts)
	t.Logf("TxCount: min=%d p25=%d p50=%d p75=%d p99=%d max=%d",
		txCounts[0], pctInt(txCounts, 25), pctInt(txCounts, 50),
		pctInt(txCounts, 75), pctInt(txCounts, 99), txCounts[len(txCounts)-1])

	// Precompute early/mid/late hashes for find_tx_by_hash timing.
	type hashPositions struct {
		early, mid, late [32]byte
	}
	posHashes := make([]hashPositions, len(ledgers))
	for i, l := range ledgers {
		hashes, err := sampleExtractAllHashes(l)
		if err != nil {
			t.Fatalf("precompute hashes for ledger %d: %v", i, err)
		}
		if len(hashes) > 0 {
			posHashes[i].early = hashes[min(10, len(hashes)-1)]
			posHashes[i].mid = hashes[len(hashes)/2]
			posHashes[i].late = hashes[max(0, len(hashes)-10)]
		}
	}

	// Precompute hashes of txs with events for extract_events_by_hash timing.
	eventTxHashes := make([][32]byte, len(ledgers))
	for i, l := range ledgers {
		eventTxHashes[i] = findEventBearingTxHash(l)
	}

	// Per-operation timing
	type op struct {
		name string
		fn   func(int, []byte) error
	}
	ops := []op{
		{"full_decode", func(_ int, data []byte) error {
			var lcm xdr.LedgerCloseMeta
			return xdr.SafeUnmarshal(data, &lcm)
		}},
		{"view/valid", func(_ int, data []byte) error {
			return xdr.LedgerCloseMetaView(data).ValidateFull()
		}},
		{"view/find_tx_by_hash_early", func(i int, data []byte) error {
			_, err := sampleFindHash(data, posHashes[i].early)
			return err
		}},
		{"view/find_tx_by_hash_mid", func(i int, data []byte) error {
			_, err := sampleFindHash(data, posHashes[i].mid)
			return err
		}},
		{"view/find_tx_by_hash_late", func(i int, data []byte) error {
			_, err := sampleFindHash(data, posHashes[i].late)
			return err
		}},
		{"full_decode/find_tx_by_hash_mid", func(i int, data []byte) error {
			_, err := sampleFindHashFullDecode(data, posHashes[i].mid)
			return err
		}},
		{"view/extract_all_hashes", func(_ int, data []byte) error {
			_, err := sampleExtractAllHashes(data)
			return err
		}},
		{"view/extract_all_txs", func(_ int, data []byte) error {
			_, err := sampleExtractAllTx(data)
			return err
		}},
		{"view/extract_all_events", func(_ int, data []byte) error {
			_, err := sampleExtractAllEvents(data)
			return err
		}},
		{"full_decode/extract_all_events", func(_ int, data []byte) error {
			_, err := sampleExtractAllEventsFullDecode(data)
			return err
		}},
		{"view/extract_events_by_hash", func(i int, data []byte) error {
			_, err := sampleExtractEventsByHash(data, eventTxHashes[i])
			return err
		}},
		{"full_decode/extract_events_by_hash", func(i int, data []byte) error {
			_, err := sampleExtractEventsByHashFullDecode(data, eventTxHashes[i])
			return err
		}},
	}

	t.Logf("")
	t.Logf("%-35s %10s %10s %10s %10s", "operation", "p50", "p90", "p99", "max")
	for _, op := range ops {
		durations := make([]time.Duration, len(ledgers))
		for i, l := range ledgers {
			start := time.Now()
			if err := op.fn(i, l); err != nil {
				t.Fatalf("%s on ledger %d: %v", op.name, i, err)
			}
			durations[i] = time.Since(start)
		}
		sort.Slice(durations, func(a, b int) bool { return durations[a] < durations[b] })
		t.Logf("%-35s %10s %10s %10s %10s",
			op.name,
			pctDur(durations, 50),
			pctDur(durations, 90),
			pctDur(durations, 99),
			durations[len(durations)-1])
	}
}

// BenchmarkViewSample runs view operations across a representative sample of
// real ledgers, cycling round-robin. Each iteration processes one ledger.
//
// Run: go test -run '^$' -bench BenchmarkViewSample -benchmem -timeout 10m ./xdr/
func BenchmarkViewSample(b *testing.B) {
	ledgers := sampleLedgers(b)
	totalBytes := 0
	for _, l := range ledgers {
		totalBytes += len(l)
	}
	avgBytes := int64(totalBytes / len(ledgers))

	// Precompute early/mid/late hashes for find_tx_by_hash
	type benchHashPositions struct {
		early, mid, late [32]byte
	}
	benchHashes := make([]benchHashPositions, len(ledgers))
	for i, l := range ledgers {
		hashes, err := sampleExtractAllHashes(l)
		if err != nil {
			b.Fatalf("precompute hashes for ledger %d: %v", i, err)
		}
		if len(hashes) > 0 {
			benchHashes[i].early = hashes[min(10, len(hashes)-1)]
			benchHashes[i].mid = hashes[len(hashes)/2]
			benchHashes[i].late = hashes[max(0, len(hashes)-10)]
		}
	}

	b.Run("full_decode", func(b *testing.B) {
		b.SetBytes(avgBytes)
		for i := range b.N {
			var lcm xdr.LedgerCloseMeta
			if err := xdr.SafeUnmarshal(ledgers[i%len(ledgers)], &lcm); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("view/valid", func(b *testing.B) {
		b.SetBytes(avgBytes)
		for i := range b.N {
			if err := xdr.LedgerCloseMetaView(ledgers[i%len(ledgers)]).ValidateFull(); err != nil {
				b.Fatal(err)
			}
		}
	})

	for _, pos := range []struct {
		name string
		hash func(int) [32]byte
	}{
		{"early", func(i int) [32]byte { return benchHashes[i].early }},
		{"mid", func(i int) [32]byte { return benchHashes[i].mid }},
		{"late", func(i int) [32]byte { return benchHashes[i].late }},
	} {
		b.Run("find_tx_by_hash/full_decode/"+pos.name, func(b *testing.B) {
			b.SetBytes(avgBytes)
			for i := range b.N {
				idx := i % len(ledgers)
				_, _ = sampleFindHashFullDecode(ledgers[idx], pos.hash(idx))
			}
		})

		b.Run("find_tx_by_hash/view/"+pos.name, func(b *testing.B) {
			b.SetBytes(avgBytes)
			for i := range b.N {
				idx := i % len(ledgers)
				_, _ = sampleFindHash(ledgers[idx], pos.hash(idx))
			}
		})
	}

	b.Run("extract_all_hashes/full_decode", func(b *testing.B) {
		b.SetBytes(avgBytes)
		for i := range b.N {
			if _, err := sampleExtractAllHashesFullDecode(ledgers[i%len(ledgers)]); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("extract_all_hashes/view", func(b *testing.B) {
		b.SetBytes(avgBytes)
		for i := range b.N {
			if _, err := sampleExtractAllHashes(ledgers[i%len(ledgers)]); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("extract_all_txs/full_decode", func(b *testing.B) {
		b.SetBytes(avgBytes)
		for i := range b.N {
			if _, err := sampleExtractAllTxFullDecode(ledgers[i%len(ledgers)]); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("extract_all_txs/view", func(b *testing.B) {
		b.SetBytes(avgBytes)
		for i := range b.N {
			if _, err := sampleExtractAllTx(ledgers[i%len(ledgers)]); err != nil {
				b.Fatal(err)
			}
		}
	})

	// Precompute event-bearing tx hashes for events-by-hash benchmarks.
	benchEventHashes := make([][32]byte, len(ledgers))
	for i, l := range ledgers {
		benchEventHashes[i] = findEventBearingTxHash(l)
	}

	b.Run("extract_events_by_hash/full_decode", func(b *testing.B) {
		b.SetBytes(avgBytes)
		for i := range b.N {
			idx := i % len(ledgers)
			_, _ = sampleExtractEventsByHashFullDecode(ledgers[idx], benchEventHashes[idx])
		}
	})

	b.Run("extract_events_by_hash/view", func(b *testing.B) {
		b.SetBytes(avgBytes)
		for i := range b.N {
			idx := i % len(ledgers)
			_, _ = sampleExtractEventsByHash(ledgers[idx], benchEventHashes[idx])
		}
	})

	b.Run("extract_all_events/full_decode", func(b *testing.B) {
		b.SetBytes(avgBytes)
		for i := range b.N {
			if _, err := sampleExtractAllEventsFullDecode(ledgers[i%len(ledgers)]); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("extract_all_events/view", func(b *testing.B) {
		b.SetBytes(avgBytes)
		for i := range b.N {
			if _, err := sampleExtractAllEvents(ledgers[i%len(ledgers)]); err != nil {
				b.Fatal(err)
			}
		}
	})
}
