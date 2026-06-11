package xdr_test

// views_gen_test.go exercises the generated Scan() cursor, Fields() bundle, and
// the cursor byproducts (Count/Index/Bytes/Elem) directly on a real
// LedgerCloseMetaV1 ledger, checking them against the struct decoder (the
// canonical oracle) and against each other, plus the cursor misuse contract.
// The reference is the struct decoder's MarshalBinary, the same oracle the
// conformance harness uses.

import (
	"bytes"
	"os"
	"testing"

	"github.com/stellar/go-stellar-sdk/xdr"
)

const genLedgerPath = "testdata/ledger_58752000.bin"

func loadGenLedger(tb testing.TB) []byte {
	tb.Helper()
	data, err := os.ReadFile(genLedgerPath)
	if err != nil {
		tb.Skipf("testdata not available: %v", err)
	}
	return data
}

// decodeResultMetaExtents is the oracle: it fully decodes the ledger and
// re-marshals each TxProcessing element and its Result/TxApplyProcessing fields
// to their canonical wire bytes.
func decodeResultMetaExtents(tb testing.TB, data []byte) (elem, result, meta [][]byte) {
	tb.Helper()
	var lcm xdr.LedgerCloseMeta
	if err := xdr.SafeUnmarshal(data, &lcm); err != nil {
		tb.Fatalf("decode: %v", err)
	}
	v1 := lcm.MustV1()
	for i := range v1.TxProcessing {
		tx := v1.TxProcessing[i]
		eb, err := tx.MarshalBinary()
		if err != nil {
			tb.Fatalf("tx[%d].MarshalBinary: %v", i, err)
		}
		rb, err := tx.Result.MarshalBinary()
		if err != nil {
			tb.Fatalf("tx[%d].Result.MarshalBinary: %v", i, err)
		}
		mb, err := tx.TxApplyProcessing.MarshalBinary()
		if err != nil {
			tb.Fatalf("tx[%d].TxApplyProcessing.MarshalBinary: %v", i, err)
		}
		elem = append(elem, eb)
		result = append(result, rb)
		meta = append(meta, mb)
	}
	return elem, result, meta
}

// extractResultMetaGen uses the generated Scan() cursor. Elem() returns the
// located TransactionResultMetaFields bundle; the Result and TxApplyProcessing
// fields are already trimmed to their exact wire extent, so []byte(...) is the
// raw element bytes with no further sizing walk.
func extractResultMetaGen(data []byte) (res, meta [][]byte, err error) {
	v1, err := xdr.LedgerCloseMetaView(data).V1()
	if err != nil {
		return nil, nil, err
	}
	arr, err := v1.TxProcessing()
	if err != nil {
		return nil, nil, err
	}
	c := arr.Scan()
	for c.Next() {
		elem := c.Elem()
		res = append(res, []byte(elem.Result))
		meta = append(meta, []byte(elem.TxApplyProcessing))
	}
	return res, meta, c.Err()
}

// TestGenMatchesDecoder asserts the generated Scan()/Fields() path produces
// byte-identical Result and TxApplyProcessing extents to the struct decoder for
// every transaction in the real ledger.
func TestGenMatchesDecoder(t *testing.T) {
	data := loadGenLedger(t)

	_, wantResult, wantMeta := decodeResultMetaExtents(t, data)
	r2, m2, err := extractResultMetaGen(data)
	if err != nil {
		t.Fatalf("generated: %v", err)
	}

	if len(wantResult) != len(r2) || len(wantMeta) != len(m2) {
		t.Fatalf("count mismatch: oracle res=%d meta=%d, views res=%d meta=%d", len(wantResult), len(wantMeta), len(r2), len(m2))
	}
	if len(wantResult) == 0 {
		t.Fatal("no transactions extracted")
	}
	for i := range wantResult {
		if !bytes.Equal(wantResult[i], r2[i]) {
			t.Fatalf("result bytes differ at tx %d (oracle %d bytes, views %d bytes)", i, len(wantResult[i]), len(r2[i]))
		}
		if !bytes.Equal(wantMeta[i], m2[i]) {
			t.Fatalf("meta bytes differ at tx %d (oracle %d bytes, views %d bytes)", i, len(wantMeta[i]), len(m2[i]))
		}
	}
	t.Logf("verified %d transactions, identical result+meta extents (generated Scan/Fields vs decoder)", len(wantResult))
}

// TestGenFieldsAndBytes exercises the generated Fields() method directly and
// the cursor's Bytes()/Index()/Count()/Elem() byproducts, checking them against
// the struct decoder and against each other, plus the cursor misuse contract.
// checkTxElement verifies one tx-processing cursor element against the decoder's
// expected extents: Bytes() and the Elem() bundle's View/Result/TxApplyProcessing
// must match, and calling Fields() directly on the (fat) element view must locate
// the same trimmed extents in one walk.
func checkTxElement(
	t *testing.T, c *xdr.LedgerCloseMetaV1TxProcessingViewCursor, i int, wantElem, wantResult, wantMeta []byte,
) {
	t.Helper()
	// Bytes() must equal the element's whole trimmed extent.
	if !bytes.Equal(c.Bytes(), wantElem) {
		t.Fatalf("Bytes() differ at tx %d", i)
	}
	// Elem() bundle's View must equal Bytes(); fields must match the decoder.
	elem := c.Elem()
	if !bytes.Equal([]byte(elem.View), wantElem) {
		t.Fatalf("Elem().View differs from Bytes() at tx %d", i)
	}
	if !bytes.Equal([]byte(elem.Result), wantResult) {
		t.Fatalf("Elem().Result differs at tx %d", i)
	}
	if !bytes.Equal([]byte(elem.TxApplyProcessing), wantMeta) {
		t.Fatalf("Elem().TxApplyProcessing differs at tx %d", i)
	}

	// Calling Fields() directly on the (fat) element view must locate the same
	// trimmed extents in one walk.
	fields, err := elem.View.Fields()
	if err != nil {
		t.Fatalf("Fields() at tx %d: %v", i, err)
	}
	if !bytes.Equal([]byte(fields.Result), wantResult) {
		t.Fatalf("Fields().Result differs at tx %d", i)
	}
	if !bytes.Equal([]byte(fields.TxApplyProcessing), wantMeta) {
		t.Fatalf("Fields().TxApplyProcessing differs at tx %d", i)
	}
	if !bytes.Equal([]byte(fields.View), wantElem) {
		t.Fatalf("Fields().View differs at tx %d", i)
	}
}

func TestGenFieldsAndBytes(t *testing.T) {
	data := loadGenLedger(t)

	wantElem, wantResult, wantMeta := decodeResultMetaExtents(t, data)
	if len(wantElem) == 0 {
		t.Fatal("no transactions extracted")
	}

	v1, err := xdr.LedgerCloseMetaView(data).V1()
	if err != nil {
		t.Fatalf("V1: %v", err)
	}
	arr, err := v1.TxProcessing()
	if err != nil {
		t.Fatalf("TxProcessing: %v", err)
	}

	c := arr.Scan()
	if got := c.Count(); got != len(wantElem) {
		t.Fatalf("Count()=%d, want %d", got, len(wantElem))
	}
	if got := c.Index(); got != -1 {
		t.Fatalf("Index() before first Next()=%d, want -1", got)
	}
	i := 0
	for c.Next() {
		if got := c.Index(); got != i {
			t.Fatalf("Index()=%d at iteration %d", got, i)
		}
		checkTxElement(t, &c, i, wantElem[i], wantResult[i], wantMeta[i])
		i++
	}
	if err := c.Err(); err != nil {
		t.Fatalf("cursor Err(): %v", err)
	}
	if i != len(wantElem) {
		t.Fatalf("cursor yielded %d elements, want %d", i, len(wantElem))
	}

	// Misuse contract: Elem() after exhaustion arms a sticky error.
	_ = c.Elem()
	if c.Err() == nil {
		t.Fatal("expected sticky misuse error after Elem() past exhaustion")
	}
}
