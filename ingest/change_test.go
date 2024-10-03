package ingest

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/stellar/go/xdr"
)

func requireChangesAreEqual(t *testing.T, a, b Change) {
	require.Equal(t, a.Type, b.Type)
	if a.Pre == nil {
		require.Nil(t, b.Pre)
	} else {
		aBytes, err := a.Pre.MarshalBinary()
		require.NoError(t, err)
		bBytes, err := b.Pre.MarshalBinary()
		require.NoError(t, err)
		if !bytes.Equal(aBytes, bBytes) {
			t.Log("jonx")
		}
		require.Equal(t, aBytes, bBytes)
	}
	if a.Post == nil {
		require.Nil(t, b.Post)
	} else {
		aBytes, err := a.Post.MarshalBinary()
		require.NoError(t, err)
		bBytes, err := b.Post.MarshalBinary()
		require.NoError(t, err)
		require.Equal(t, aBytes, bBytes)
	}
}

func TestSortChanges(t *testing.T) {
	for _, testCase := range []struct {
		input    []Change
		expected []Change
	}{
		{[]Change{}, []Change{}},
		{
			[]Change{
				{
					Type: xdr.LedgerEntryTypeAccount,
					Pre:  nil,
					Post: &xdr.LedgerEntry{
						LastModifiedLedgerSeq: 11,
						Data: xdr.LedgerEntryData{
							Type: xdr.LedgerEntryTypeAccount,
							Account: &xdr.AccountEntry{
								AccountId: xdr.MustAddress("GC3C4AKRBQLHOJ45U4XG35ESVWRDECWO5XLDGYADO6DPR3L7KIDVUMML"),
							},
						},
					},
				},
			},
			[]Change{
				{
					Type: xdr.LedgerEntryTypeAccount,
					Pre:  nil,
					Post: &xdr.LedgerEntry{
						LastModifiedLedgerSeq: 11,
						Data: xdr.LedgerEntryData{
							Type: xdr.LedgerEntryTypeAccount,
							Account: &xdr.AccountEntry{
								AccountId: xdr.MustAddress("GC3C4AKRBQLHOJ45U4XG35ESVWRDECWO5XLDGYADO6DPR3L7KIDVUMML"),
							},
						},
					},
				},
			},
		},
		{
			[]Change{
				{
					Type: xdr.LedgerEntryTypeAccount,
					Pre:  nil,
					Post: &xdr.LedgerEntry{
						LastModifiedLedgerSeq: 11,
						Data: xdr.LedgerEntryData{
							Type: xdr.LedgerEntryTypeAccount,
							Account: &xdr.AccountEntry{
								AccountId: xdr.MustAddress("GC3C4AKRBQLHOJ45U4XG35ESVWRDECWO5XLDGYADO6DPR3L7KIDVUMML"),
								Balance:   25,
							},
						},
					},
				},
				{
					Type: xdr.LedgerEntryTypeAccount,
					Pre: &xdr.LedgerEntry{
						LastModifiedLedgerSeq: 11,
						Data: xdr.LedgerEntryData{
							Type: xdr.LedgerEntryTypeAccount,
							Account: &xdr.AccountEntry{
								AccountId: xdr.MustAddress("GCMNSW2UZMSH3ZFRLWP6TW2TG4UX4HLSYO5HNIKUSFMLN2KFSF26JKWF"),
								Balance:   20,
							},
						},
					},
					Post: nil,
				},
				{
					Type: xdr.LedgerEntryTypeTtl,
					Pre: &xdr.LedgerEntry{
						LastModifiedLedgerSeq: 11,
						Data: xdr.LedgerEntryData{
							Type: xdr.LedgerEntryTypeTtl,
							Ttl: &xdr.TtlEntry{
								KeyHash:            xdr.Hash{1},
								LiveUntilLedgerSeq: 50,
							},
						},
					},
					Post: &xdr.LedgerEntry{
						LastModifiedLedgerSeq: 11,
						Data: xdr.LedgerEntryData{
							Type: xdr.LedgerEntryTypeTtl,
							Ttl: &xdr.TtlEntry{
								KeyHash:            xdr.Hash{1},
								LiveUntilLedgerSeq: 100,
							},
						},
					},
				},
				{
					Type: xdr.LedgerEntryTypeAccount,
					Pre: &xdr.LedgerEntry{
						LastModifiedLedgerSeq: 11,
						Data: xdr.LedgerEntryData{
							Type: xdr.LedgerEntryTypeAccount,
							Account: &xdr.AccountEntry{
								AccountId: xdr.MustAddress("GC3C4AKRBQLHOJ45U4XG35ESVWRDECWO5XLDGYADO6DPR3L7KIDVUMML"),
								Balance:   25,
							},
						},
					},
					Post: nil,
				},
			},
			[]Change{
				{
					Type: xdr.LedgerEntryTypeAccount,
					Pre: &xdr.LedgerEntry{
						LastModifiedLedgerSeq: 11,
						Data: xdr.LedgerEntryData{
							Type: xdr.LedgerEntryTypeAccount,
							Account: &xdr.AccountEntry{
								AccountId: xdr.MustAddress("GCMNSW2UZMSH3ZFRLWP6TW2TG4UX4HLSYO5HNIKUSFMLN2KFSF26JKWF"),
								Balance:   20,
							},
						},
					},
					Post: nil,
				},
				{
					Type: xdr.LedgerEntryTypeAccount,
					Pre:  nil,
					Post: &xdr.LedgerEntry{
						LastModifiedLedgerSeq: 11,
						Data: xdr.LedgerEntryData{
							Type: xdr.LedgerEntryTypeAccount,
							Account: &xdr.AccountEntry{
								AccountId: xdr.MustAddress("GC3C4AKRBQLHOJ45U4XG35ESVWRDECWO5XLDGYADO6DPR3L7KIDVUMML"),
								Balance:   25,
							},
						},
					},
				},
				{
					Type: xdr.LedgerEntryTypeAccount,
					Pre: &xdr.LedgerEntry{
						LastModifiedLedgerSeq: 11,
						Data: xdr.LedgerEntryData{
							Type: xdr.LedgerEntryTypeAccount,
							Account: &xdr.AccountEntry{
								AccountId: xdr.MustAddress("GC3C4AKRBQLHOJ45U4XG35ESVWRDECWO5XLDGYADO6DPR3L7KIDVUMML"),
								Balance:   25,
							},
						},
					},
					Post: nil,
				},

				{
					Type: xdr.LedgerEntryTypeTtl,
					Pre: &xdr.LedgerEntry{
						LastModifiedLedgerSeq: 11,
						Data: xdr.LedgerEntryData{
							Type: xdr.LedgerEntryTypeTtl,
							Ttl: &xdr.TtlEntry{
								KeyHash:            xdr.Hash{1},
								LiveUntilLedgerSeq: 50,
							},
						},
					},
					Post: &xdr.LedgerEntry{
						LastModifiedLedgerSeq: 11,
						Data: xdr.LedgerEntryData{
							Type: xdr.LedgerEntryTypeTtl,
							Ttl: &xdr.TtlEntry{
								KeyHash:            xdr.Hash{1},
								LiveUntilLedgerSeq: 100,
							},
						},
					},
				},
			},
		},
	} {
		sortChanges(testCase.input)
		require.Equal(t, len(testCase.input), len(testCase.expected))
		for i := range testCase.input {
			requireChangesAreEqual(t, testCase.input[i], testCase.expected[i])
		}
	}
}
