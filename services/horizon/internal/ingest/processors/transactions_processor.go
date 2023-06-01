package processors

import (
	"context"
	"github.com/stellar/go/support/db"
	"github.com/stellar/go/xdr"

	"github.com/stellar/go/ingest"
	"github.com/stellar/go/services/horizon/internal/db2/history"
)

type TransactionProcessor struct {
	batch history.TransactionBatchInsertBuilder
}

func NewTransactionFilteredTmpProcessor(transactionsQ history.QTransactions, sequence uint32) *TransactionProcessor {
	return &TransactionProcessor{
		batch: transactionsQ.NewTransactionFilteredTmpBatchInsertBuilder(),
	}
}

func NewTransactionProcessor(batch history.TransactionBatchInsertBuilder) *TransactionProcessor {
	return &TransactionProcessor{
		batch: batch,
	}
}

func (p *TransactionProcessor) ProcessTransaction(lcm xdr.LedgerCloseMeta, transaction ingest.LedgerTransaction) error {
	return p.batch.Add(transaction, lcm.LedgerSequence())
}

func (p *TransactionProcessor) Commit(ctx context.Context, session db.SessionInterface) error {
	return p.batch.Exec(ctx, session)
}
