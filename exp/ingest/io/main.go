package io

import (
	"github.com/stellar/go/xdr"
)

// StateReadCloser interface placeholder
type StateReadCloser interface {
	GetSequence() uint32
	// Read should return next ledger entry. If there are no more
	// entries it should return `EOF` error.
	Read() (xdr.LedgerEntryChange, error)
	// Close should be called when reading is finished. This is especially
	// helpful when there are still some entries available so reader can stop
	// streaming them.
	Close() error
}

// StateWriteCloser interface placeholder
type StateWriteCloser interface {
	// Write is used to pass ledger entry change to the next processor. It can return
	// `ErrClosedPipe` when the pipe between processors has been closed meaning
	// that next processor does not need more data. In such situation the current
	// processor can terminate as sending more entries to a `StateWriteCloser`
	// does not make sense (will not be read).
	Write(xdr.LedgerEntryChange) error
	// Close should be called when there are no more entries
	// to write.
	Close() error
}
