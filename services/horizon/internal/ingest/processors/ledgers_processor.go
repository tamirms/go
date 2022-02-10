package processors

import (
	"context"
	"encoding/hex"
	"github.com/guregu/null"
	"github.com/stellar/go/services/horizon/internal/toid"
	"time"

	"github.com/stellar/go/ingest"
	"github.com/stellar/go/services/horizon/internal/db2/history"
	"github.com/stellar/go/support/errors"
	"github.com/stellar/go/xdr"
)

type LedgersProcessor struct {
	ledgersQ       history.QLedgers
	ledger         xdr.LedgerHeaderHistoryEntry
	ingestVersion  int
	successTxCount int
	failedTxCount  int
	opCount        int
	txSetOpCount   int
}

func NewLedgerProcessor(
	ledgerQ history.QLedgers,
	ledger xdr.LedgerHeaderHistoryEntry,
	ingestVersion int,
) *LedgersProcessor {
	return &LedgersProcessor{
		ledger:        ledger,
		ledgersQ:      ledgerQ,
		ingestVersion: ingestVersion,
	}
}

func (p *LedgersProcessor) ProcessTransaction(ctx context.Context, transaction ingest.LedgerTransaction) (err error) {
	opCount := len(transaction.Envelope.Operations())
	p.txSetOpCount += opCount
	if transaction.Result.Successful() {
		p.successTxCount++
		p.opCount += opCount
	} else {
		p.failedTxCount++
	}

	return nil
}

func (p *LedgersProcessor) Commit(ctx context.Context) error {
	rowsAffected, err := p.ledgersQ.InsertLedger(ctx,
		p.ledger,
		p.successTxCount,
		p.failedTxCount,
		p.opCount,
		p.txSetOpCount,
		p.ingestVersion,
	)

	if err != nil {
		return errors.Wrap(err, "Could not insert ledger")
	}

	sequence := uint32(p.ledger.Header.LedgerSeq)

	if rowsAffected != 1 {
		log.WithField("rowsAffected", rowsAffected).
			WithField("sequence", sequence).
			Error("Invalid number of rows affected when ingesting new ledger")
		return errors.Errorf(
			"0 rows affected when ingesting new ledger: %v",
			sequence,
		)
	}

	return nil
}

func (p *LedgersProcessor) LedgerRow() (history.Ledger, error) {
	ledgerHeaderBase64, err := xdr.MarshalBase64(p.ledger.Header)
	if err != nil {
		return history.Ledger{}, err
	}
	closeTime := time.Unix(int64(p.ledger.Header.ScpValue.CloseTime), 0).UTC()

	l := history.Ledger{
		Sequence:                   int32(p.ledger.Header.LedgerSeq),
		ImporterVersion:            int32(p.ingestVersion),
		LedgerHash:                 hex.EncodeToString(p.ledger.Hash[:]),
		PreviousLedgerHash:         null.NewString(hex.EncodeToString(p.ledger.Header.PreviousLedgerHash[:]), p.ledger.Header.LedgerSeq > 1),
		TransactionCount:           int32(p.successTxCount),
		SuccessfulTransactionCount: new(int32),
		FailedTransactionCount:     new(int32),
		OperationCount:             int32(p.opCount),
		TxSetOperationCount:        new(int32),
		ClosedAt:                   closeTime,
		CreatedAt:                  closeTime,
		UpdatedAt:                  closeTime,
		TotalCoins:                 int64(p.ledger.Header.TotalCoins),
		FeePool:                    int64(p.ledger.Header.FeePool),
		BaseFee:                    int32(p.ledger.Header.BaseFee),
		BaseReserve:                int32(p.ledger.Header.BaseReserve),
		MaxTxSetSize:               int32(p.ledger.Header.MaxTxSetSize),
		ProtocolVersion:            int32(p.ledger.Header.LedgerVersion),
		LedgerHeaderXDR:            null.NewString(ledgerHeaderBase64, true),
	}
	*l.SuccessfulTransactionCount = int32(p.successTxCount)
	*l.FailedTransactionCount = int32(p.failedTxCount)
	*l.TxSetOperationCount = int32(p.txSetOpCount)
	l.TotalOrderID.ID = toid.New(int32(p.ledger.Header.LedgerSeq), 0, 0).ToInt64()
	return l, nil
}