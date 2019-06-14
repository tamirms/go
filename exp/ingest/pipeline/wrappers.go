package pipeline

import (
	"context"

	supportPipeline "github.com/stellar/go/exp/support/pipeline"
	"github.com/stellar/go/support/errors"
	"github.com/stellar/go/xdr"
)

func (w *stateProcessorWrapper) Process(ctx context.Context, store *supportPipeline.Store, readCloser supportPipeline.ReadCloser, writeCloser supportPipeline.WriteCloser) error {
	return w.StateProcessor.ProcessState(
		ctx,
		store,
		&readCloserWrapper{readCloser},
		&writeCloserWrapper{writeCloser},
	)
}

func (w *stateReadCloserWrapper) Read() (interface{}, error) {
	return w.StateReadCloser.Read()
}

func (w *readCloserWrapper) GetSequence() uint32 {
	// TODO we should probably keep ledger sequence in context and this
	// method will be just a wrapper that fetches the data.
	return 0
}

func (w *readCloserWrapper) Read() (xdr.LedgerEntryChange, error) {
	object, err := w.ReadCloser.Read()
	if err != nil {
		return xdr.LedgerEntryChange{}, err
	}

	entry, ok := object.(xdr.LedgerEntryChange)
	if !ok {
		return xdr.LedgerEntryChange{}, errors.New("Read object is not xdr.LedgerEntryChange")
	}

	return entry, nil
}

func (w *writeCloserWrapper) Write(entry xdr.LedgerEntryChange) error {
	return w.WriteCloser.Write(entry)
}
