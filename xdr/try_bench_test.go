package xdr_test

import (
	"os"
	"testing"

	"github.com/stellar/go-stellar-sdk/xdr"
)

func loadLedgerData(b *testing.B) []byte {
	b.Helper()
	data, err := os.ReadFile("testdata/ledger_58752000.bin")
	if err != nil {
		b.Skip(err)
	}
	return data
}

// BenchmarkFindTxHash_Direct uses the standard (T, error) accessor pattern.
func BenchmarkFindTxHash_Direct(b *testing.B) {
	data := loadLedgerData(b)
	view := xdr.LedgerCloseMetaView(data)
	v1, _ := view.V1()
	txArr, _ := v1.TxProcessing()
	count, _ := txArr.Count()
	midTx, _ := txArr.At(count / 2)
	midResult, _ := midTx.Result()
	midHash, _ := midResult.TransactionHash()
	targetHash, _ := midHash.Value()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for tx, err := range txArr.Iter() {
			if err != nil {
				b.Fatal(err)
			}
			result, err := tx.Result()
			if err != nil {
				b.Fatal(err)
			}
			hashView, err := result.TransactionHash()
			if err != nil {
				b.Fatal(err)
			}
			hash, err := hashView.Value()
			if err != nil {
				b.Fatal(err)
			}
			if string(hash) == string(targetHash) {
				break
			}
		}
	}
}

// BenchmarkFindTxHash_Must uses Must methods directly (panics on error, no Try wrapper).
func BenchmarkFindTxHash_Must(b *testing.B) {
	data := loadLedgerData(b)
	view := xdr.LedgerCloseMetaView(data)
	v1, _ := view.V1()
	txArr, _ := v1.TxProcessing()
	count, _ := txArr.Count()
	midTx, _ := txArr.At(count / 2)
	midResult, _ := midTx.Result()
	midHash, _ := midResult.TransactionHash()
	targetHash, _ := midHash.Value()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for tx := range txArr.MustIter() {
			hash := tx.MustResult().MustTransactionHash().MustValue()
			if string(hash) == string(targetHash) {
				break
			}
		}
	}
}

// BenchmarkFindTxHash_Try uses Must methods inside a Try wrapper.
func BenchmarkFindTxHash_Try(b *testing.B) {
	data := loadLedgerData(b)
	view := xdr.LedgerCloseMetaView(data)
	v1, _ := view.V1()
	txArr, _ := v1.TxProcessing()
	count, _ := txArr.Count()
	midTx, _ := txArr.At(count / 2)
	midResult, _ := midTx.Result()
	midHash, _ := midResult.TransactionHash()
	targetHash, _ := midHash.Value()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := xdr.TryVoid(func() {
			for tx := range txArr.MustIter() {
				hash := tx.MustResult().MustTransactionHash().MustValue()
				if string(hash) == string(targetHash) {
					break
				}
			}
		})
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkFindTxHash_FullDirect navigates from raw bytes to hash using (T, error).
func BenchmarkFindTxHash_FullDirect(b *testing.B) {
	data := loadLedgerData(b)
	view := xdr.LedgerCloseMetaView(data)
	v1, _ := view.V1()
	txArr, _ := v1.TxProcessing()
	count, _ := txArr.Count()
	midTx, _ := txArr.At(count / 2)
	midResult, _ := midTx.Result()
	midHash, _ := midResult.TransactionHash()
	targetHash, _ := midHash.Value()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		view := xdr.LedgerCloseMetaView(data)
		v1, err := view.V1()
		if err != nil { b.Fatal(err) }
		txArr, err := v1.TxProcessing()
		if err != nil { b.Fatal(err) }
		for tx, err := range txArr.Iter() {
			if err != nil { b.Fatal(err) }
			result, err := tx.Result()
			if err != nil { b.Fatal(err) }
			hashView, err := result.TransactionHash()
			if err != nil { b.Fatal(err) }
			hash, err := hashView.Value()
			if err != nil { b.Fatal(err) }
			if string(hash) == string(targetHash) {
				break
			}
		}
	}
}

// BenchmarkFindTxHash_FullTry navigates from raw bytes to hash using Must+Try.
func BenchmarkFindTxHash_FullTry(b *testing.B) {
	data := loadLedgerData(b)
	view := xdr.LedgerCloseMetaView(data)
	v1, _ := view.V1()
	txArr, _ := v1.TxProcessing()
	count, _ := txArr.Count()
	midTx, _ := txArr.At(count / 2)
	midResult, _ := midTx.Result()
	midHash, _ := midResult.TransactionHash()
	targetHash, _ := midHash.Value()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := xdr.TryVoid(func() {
			view := xdr.LedgerCloseMetaView(data)
			txArr := view.MustV1().MustTxProcessing()
			for tx := range txArr.MustIter() {
				hash := tx.MustResult().MustTransactionHash().MustValue()
				if string(hash) == string(targetHash) {
					break
				}
			}
		})
		if err != nil { b.Fatal(err) }
	}
}

// BenchmarkExtractAllHashes_Direct uses the standard (T, error) pattern.
func BenchmarkExtractAllHashes_Direct(b *testing.B) {
	data := loadLedgerData(b)
	view := xdr.LedgerCloseMetaView(data)
	v1, _ := view.V1()
	txArr, _ := v1.TxProcessing()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		count, _ := txArr.Count()
		hashes := make([][32]byte, 0, count)
		for tx, err := range txArr.Iter() {
			if err != nil {
				b.Fatal(err)
			}
			result, err := tx.Result()
			if err != nil {
				b.Fatal(err)
			}
			hashView, err := result.TransactionHash()
			if err != nil {
				b.Fatal(err)
			}
			hash, err := hashView.Value()
			if err != nil {
				b.Fatal(err)
			}
			var h [32]byte
			copy(h[:], hash)
			hashes = append(hashes, h)
		}
		_ = hashes
	}
}

// BenchmarkExtractAllHashes_Try uses Must+Try for the same work.
func BenchmarkExtractAllHashes_Try(b *testing.B) {
	data := loadLedgerData(b)
	view := xdr.LedgerCloseMetaView(data)
	v1, _ := view.V1()
	txArr, _ := v1.TxProcessing()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := xdr.TryVoid(func() {
			hashes := make([][32]byte, 0, txArr.MustCount())
			for tx := range txArr.MustIter() {
				hash := tx.MustResult().MustTransactionHash().MustValue()
				var h [32]byte
				copy(h[:], hash)
				hashes = append(hashes, h)
			}
			_ = hashes
		})
		if err != nil {
			b.Fatal(err)
		}
	}
}
