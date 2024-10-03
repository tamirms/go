package ingest

import (
	"bytes"
	"context"
	"encoding"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/stellar/go/network"
	"github.com/stellar/go/support/compressxdr"
	"github.com/stellar/go/support/datastore"
	"github.com/stellar/go/xdr"
)

func loadLedger(t *testing.T, ledgerSeq uint32) xdr.LedgerCloseMeta {
	schema := datastore.DataStoreSchema{
		LedgersPerFile:    1,
		FilesPerPartition: 64000,
	}
	var lcmBatch xdr.LedgerCloseMetaBatch
	contents, err := os.ReadFile(filepath.Join("testdata", "ledgers", schema.GetObjectKeyFromSequenceNumber(ledgerSeq)))
	require.NoError(t, err)
	decoder := compressxdr.NewXDRDecoder(compressxdr.DefaultCompressor, &lcmBatch)
	_, err = decoder.ReadFrom(bytes.NewReader(contents))
	require.NoError(t, err)
	return lcmBatch.LedgerCloseMetas[0]
}

func assertXDREquals(t *testing.T, a, b encoding.BinaryMarshaler) {
	serialized, err := a.MarshalBinary()
	require.NoError(t, err)
	otherSerialized, err := b.MarshalBinary()
	require.NoError(t, err)
	require.Equal(t, serialized, otherSerialized)
}

func TestMergeErrors(t *testing.T) {
	_, err := MergeLedgers()
	require.ErrorContains(t, err, "input list is empty")
	_, err = MergeLedgers(loadLedger(t, 2), loadLedger(t, 53561670))
	require.ErrorContains(t, err, "mismatch in ledger versions")
}

func ledgerRange(start, end uint32) []uint32 {
	var output []uint32
	for ; start <= end; start++ {
		output = append(output, start)
	}
	return output
}

func TestMerge(t *testing.T) {
	for _, ledgerSequences := range [][]uint32{
		ledgerRange(33561670, 33561670+1000-1),
		//ledgerRange(53561670, 53562669),
	} {
		var ledgers []xdr.LedgerCloseMeta
		var changes []Change
		var transactions []LedgerTransaction
		for _, ledgerSequence := range ledgerSequences {
			ledger := loadLedger(t, ledgerSequence)
			ledgers = append(ledgers, ledger)

			reader, err := NewLedgerChangeReaderFromLedgerCloseMeta(network.PublicNetworkPassphrase, ledger)
			require.NoError(t, err)
			for {
				change, err := reader.Read()
				if err == io.EOF {
					break
				}
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				changes = append(changes, change)
			}

			txReader, err := NewLedgerTransactionReaderFromLedgerCloseMeta(network.PublicNetworkPassphrase, ledger)
			require.NoError(t, err)
			for {
				tx, err := txReader.Read()
				if err == io.EOF {
					break
				}
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				transactions = append(transactions, tx)
			}
		}

		merged, err := MergeLedgers(ledgers...)
		require.NoError(t, err)

		//reader, err := NewLedgerChangeReaderFromLedgerCloseMeta(network.PublicNetworkPassphrase, merged)
		//require.NoError(t, err)
		//i := 0
		//for {
		//	change, err := reader.Read()
		//	if err == io.EOF {
		//		require.Empty(t, changes)
		//		break
		//	}
		//	if err != nil {
		//		t.Fatalf("unexpected error: %v", err)
		//	}
		//
		//	expected := changes[0]
		//	requireChangesAreEqual(t, expected, change)
		//
		//	changes = changes[1:]
		//	i++
		//}

		txReader, err := NewLedgerTransactionReaderFromLedgerCloseMeta(network.PublicNetworkPassphrase, merged)
		require.NoError(t, err)
		for {
			tx, err := txReader.Read()
			if err == io.EOF {
				require.Empty(t, transactions)
				break
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			expected := transactions[0]
			require.Equal(t, expected.LedgerVersion, tx.LedgerVersion)
			assertXDREquals(t, expected.Envelope, tx.Envelope)
			assertXDREquals(t, expected.Result, tx.Result)
			assertXDREquals(t, expected.FeeChanges, tx.FeeChanges)
			assertXDREquals(t, expected.UnsafeMeta, tx.UnsafeMeta)

			transactions = transactions[1:]
		}
	}
}

func downloadLedgers(t *testing.T, store datastore.DataStore, schema datastore.DataStoreSchema, ledger uint32) {
	for i := uint32(0); i < 1000; i++ {
		filename := schema.GetObjectKeyFromSequenceNumber(ledger + i)
		reader, err := store.GetFile(context.Background(), filename)
		require.NoError(t, err)

		fp := filepath.Join("testdata", "ledgers", filename)
		err = os.MkdirAll(filepath.Dir(fp), 0770)
		require.NoError(t, err)
		file, err := os.Create(fp)
		require.NoError(t, err)

		_, err = io.Copy(file, reader)
		require.NoError(t, err)
		require.NoError(t, reader.Close())
		require.NoError(t, file.Close())
	}
}
func TestJonx(t *testing.T) {
	schema := datastore.DataStoreSchema{
		LedgersPerFile:    1,
		FilesPerPartition: 64000,
	}
	store, err := datastore.NewGCSDataStore(context.Background(),
		"sdf-ledger-close-meta/ledgers/pubnet/",
		schema,
	)
	require.NoError(t, err)
	downloadLedgers(t, store, schema, 161670)
	//downloadLedgers(t, store, schema, 33561670)
	//downloadLedgers(t, store, schema, 53561670)
}
