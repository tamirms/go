package ingest

import (
	"github.com/stellar/go/exp/ingest/io"
	"github.com/stellar/go/xdr"
)

type ChangeProcessor interface {
	Init(sequence uint32) error
	ProcessChange(change io.Change) error
	Commit() error
}

type LedgerTransactionProcessor interface {
	Init(header xdr.LedgerHeader) error
	ProcessTransaction(transaction io.LedgerTransaction) error
	Commit() error
}

type cpGroup []ChangeProcessor

func (g cpGroup) Init(sequence uint32) error {
	for _, p := range g {
		if err := p.Init(sequence); err != nil {
			return err
		}
	}
	return nil
}

func (g cpGroup) ProcessChange(change io.Change) error {
	for _, p := range g {
		if err := p.ProcessChange(change); err != nil {
			return err
		}
	}
	return nil
}

func (g cpGroup) Commit() error {
	for _, p := range g {
		if err := p.Commit(); err != nil {
			return err
		}
	}
	return nil
}

func GroupChangeProcessors(
	processors ...ChangeProcessor,
) ChangeProcessor {
	return cpGroup(processors)
}

type ltGroup []LedgerTransactionProcessor

func (g ltGroup) Init(header xdr.LedgerHeader) error {
	for _, p := range g {
		if err := p.Init(header); err != nil {
			return err
		}
	}
	return nil
}

func (g ltGroup) ProcessTransaction(transaction io.LedgerTransaction) error {
	for _, p := range g {
		if err := p.ProcessTransaction(transaction); err != nil {
			return err
		}
	}
	return nil
}

func (g ltGroup) Commit() error {
	for _, p := range g {
		if err := p.Commit(); err != nil {
			return err
		}
	}
	return nil
}

func GroupLedgerTransactionProcessors(
	processors ...LedgerTransactionProcessor,
) LedgerTransactionProcessor {
	return ltGroup(processors)
}
