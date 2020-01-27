package io

import (
	"io"

	"github.com/stellar/go/exp/ingest/ledgerbackend"
	"github.com/stellar/go/xdr"
)

// ChangeReader provides convenient, streaming access to changes within a ledger.
type ChangeReader interface {
	GetSequence() uint32
	GetHeader() xdr.LedgerHeaderHistoryEntry
	// Read should return the next `Change` in the leader. If there are no more
	// changes left it should return an `io.EOF` error.
	Read() (Change, error)
}

type changeReader struct {
	dbReader               DBLedgerReader
	streamedFeeChanges     bool
	streamedMetaChanges    bool
	streamedUpgradeChanges bool
	pending                []Change
	pendingIndex           int
}

// Ensure changeReader implements ChangeReader
var _ ChangeReader = (*changeReader)(nil)

// NewChangeReader constructs a new ChangeReader instance bound to the given eldger
func NewChangeReader(sequence uint32, backend ledgerbackend.LedgerBackend) (ChangeReader, error) {
	reader, err := NewDBLedgerReader(sequence, backend)
	if err != nil {
		return nil, err
	}

	return &changeReader{dbReader: *reader}, nil
}

// GetSequence returns the ledger sequence for the reader
func (r *changeReader) GetSequence() uint32 {
	return r.dbReader.GetSequence()
}

// GetSequence returns the ledger header for the reader
func (r *changeReader) GetHeader() xdr.LedgerHeaderHistoryEntry {
	return r.dbReader.GetHeader()
}

func (r *changeReader) getNextFeeChange() (Change, error) {
	if r.streamedFeeChanges {
		return Change{}, io.EOF
	}

	// Remember that it's possible that transaction can remove a preauth
	// tx signer even when it's a failed transaction so we need to check
	// failed transactions too.
	for {
		transaction, err := r.dbReader.Read()
		if err != nil {
			if err == io.EOF {
				r.dbReader.rewind()
				r.streamedFeeChanges = true
				return Change{}, io.EOF
			} else {
				return Change{}, err
			}
		}

		changes := transaction.GetFeeChanges()
		if len(changes) >= 1 {
			r.pending = append(r.pending, changes[1:]...)
			return changes[0], nil
		}
	}
}

func (r *changeReader) getNextMetaChange() (Change, error) {
	if r.streamedMetaChanges {
		return Change{}, io.EOF
	}

	for {
		transaction, err := r.dbReader.Read()
		if err != nil {
			if err == io.EOF {
				r.streamedMetaChanges = true
				return Change{}, io.EOF
			} else {
				return Change{}, err
			}
		}

		changes, err := transaction.GetChanges()
		if err != nil {
			return Change{}, err
		}
		if len(changes) >= 1 {
			r.pending = append(r.pending, changes[1:]...)
			return changes[0], nil
		}
	}
}

func (r *changeReader) getNextUpgradeChange() (Change, error) {
	if r.streamedUpgradeChanges {
		return Change{}, io.EOF
	}

	change, err := r.dbReader.readUpgradeChange()
	if err != nil {
		if err == io.EOF {
			r.streamedUpgradeChanges = true
			return Change{}, io.EOF
		} else {
			return Change{}, err
		}
	}

	return change, nil
}

func (r *changeReader) Read() (Change, error) {
	if r.pendingIndex < len(r.pending) {
		next := r.pending[r.pendingIndex]
		r.pendingIndex++
		if r.pendingIndex == len(r.pending) {
			r.pendingIndex = 0
			r.pending = r.pending[:0]
		}
		return next, nil
	}

	change, err := r.getNextFeeChange()
	if err == nil || err != io.EOF {
		return change, err
	}

	change, err = r.getNextMetaChange()
	if err == nil || err != io.EOF {
		return change, err
	}

	return r.getNextUpgradeChange()
}
