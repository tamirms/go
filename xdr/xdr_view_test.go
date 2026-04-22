package xdr

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestView_LedgerCloseMeta_RoundTrip(t *testing.T) {
	lcm := LedgerCloseMeta{
		V: int32(0),
		V0: &LedgerCloseMetaV0{
			LedgerHeader: LedgerHeaderHistoryEntry{
				Header: LedgerHeader{
					LedgerSeq: 12345,
				},
			},
		},
	}

	data, err := lcm.MarshalBinary()
	require.NoError(t, err)

	view := LedgerCloseMetaView(data)

	raw, err := view.Raw()
	require.NoError(t, err)
	assert.Equal(t, len(data), len(raw))

	ver, err := view.V()
	require.NoError(t, err)
	verVal, err := ver.Value()
	require.NoError(t, err)
	assert.Equal(t, int32(0), verVal)

	v0, err := view.V0()
	require.NoError(t, err)

	hdr, err := v0.LedgerHeader()
	require.NoError(t, err)

	header, err := hdr.Header()
	require.NoError(t, err)

	seq, err := header.LedgerSeq()
	require.NoError(t, err)
	seqVal, err := seq.Value()
	require.NoError(t, err)
	assert.Equal(t, uint32(12345), seqVal)

	raw2, err := view.Raw()
	require.NoError(t, err)
	assert.Equal(t, data, raw2)

	require.NoError(t, view.ValidateFull())
}

func TestView_LedgerCloseMeta_ConvenienceMethods(t *testing.T) {
	lcm := LedgerCloseMeta{
		V: int32(0),
		V0: &LedgerCloseMetaV0{
			LedgerHeader: LedgerHeaderHistoryEntry{
				Hash: Hash{0xAB},
				Header: LedgerHeader{
					LedgerSeq: 99999,
				},
			},
		},
	}

	data, err := lcm.MarshalBinary()
	require.NoError(t, err)

	view := LedgerCloseMetaView(data)

	seq, err := view.LedgerSequence()
	require.NoError(t, err)
	assert.Equal(t, uint32(99999), seq)

	hash, err := view.LedgerHash()
	require.NoError(t, err)
	assert.Equal(t, byte(0xAB), hash[0])
}
