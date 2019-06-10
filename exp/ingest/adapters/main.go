package adapters

import (
	"github.com/stellar/go/exp/ingest/io"
)

type LedgerBackendAdapter interface {
	GetLatestLedgerSequence() (uint32, error)
	GetLedger(sequence uint32) (io.LedgerReadCloser, error)
}
