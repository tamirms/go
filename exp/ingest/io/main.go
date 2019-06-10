package io

import (
	"io"

	"github.com/stellar/go/xdr"
)

var EOF = io.EOF
var ErrClosedPipe = io.ErrClosedPipe

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

type LedgerTransaction struct {
	Envelope xdr.TransactionEnvelope
	Result   xdr.TransactionResult
	Meta     xdr.TransactionMeta
}

type LedgerReadCloser interface {
	GetSequence() uint32
	GetHeader() xdr.LedgerHeader
	// Read returns io.EOF error when there is no more data.
	Read() (LedgerTransaction, error)
	// Close should be called when reading is finished. This is especially
	// helpful when there are still some transactions available so reader can stop
	// streaming them.
	Close() error
}

type LedgerWriteCloser interface {
	// Write is used to pass a transaction to the next processor. It can return
	// `io.ErrClosedPipe` when the pipe between processors has been closed meaning
	// that next processor does not need more data. In such situation the current
	// processor can terminate as sending more transactions to a `LedgerWriteCloser`
	// does not make sense (will not be read).
	Write(LedgerTransaction) error
	// Close should be called when there are no more entries
	// to write.
	Close() error
}
