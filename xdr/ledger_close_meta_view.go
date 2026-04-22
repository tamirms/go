package xdr

import "fmt"

// NewLedgerCloseMetaBatchView creates a view over raw LedgerCloseMetaBatch bytes.
func NewLedgerCloseMetaBatchView(data []byte) (LedgerCloseMetaBatchView, error) {
	// LedgerCloseMetaBatch = startSequence(uint32) + endSequence(uint32) + ledgerCloseMetas(var array)
	if len(data) < 12 {
		return nil, fmt.Errorf("batch too small: %d bytes", len(data))
	}
	return LedgerCloseMetaBatchView(data), nil
}

func (v LedgerCloseMetaView) LedgerSequence() (uint32, error) {
	hdr, err := v.ledgerHeaderView()
	if err != nil { return 0, err }
	header, err := hdr.Header()
	if err != nil { return 0, err }
	seq, err := header.LedgerSeq()
	if err != nil { return 0, err }
	val, err := seq.Value()
	if err != nil { return 0, err }
	return val, nil
}

func (v LedgerCloseMetaView) LedgerHash() ([]byte, error) {
	hdr, err := v.ledgerHeaderView()
	if err != nil { return nil, err }
	hash, err := hdr.Hash()
	if err != nil { return nil, err }
	return hash.Value()
}

func (v LedgerCloseMetaView) ledgerHeaderView() (LedgerHeaderHistoryEntryView, error) {
	ver, err := v.V()
	if err != nil { return nil, err }
	verVal, err := ver.Value()
	if err != nil { return nil, err }
	switch verVal {
	case 0:
		v0, err := v.V0()
		if err != nil { return nil, err }
		return v0.LedgerHeader()
	case 1:
		v1, err := v.V1()
		if err != nil { return nil, err }
		return v1.LedgerHeader()
	case 2:
		v2, err := v.V2()
		if err != nil { return nil, err }
		return v2.LedgerHeader()
	default:
		return nil, viewErrUnknownDiscriminant(0, verVal)
	}
}
