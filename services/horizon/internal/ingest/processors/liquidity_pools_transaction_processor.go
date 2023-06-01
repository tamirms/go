package processors

import (
	"context"
	"github.com/stellar/go/support/db"

	"github.com/stellar/go/ingest"
	"github.com/stellar/go/services/horizon/internal/db2/history"
	set "github.com/stellar/go/support/collections/set"
	"github.com/stellar/go/support/errors"
	"github.com/stellar/go/toid"
	"github.com/stellar/go/xdr"
)

type LiquidityPoolsTransactionProcessor struct {
	lpLoader *history.Loader
	txBatch  history.TransactionLiquidityPoolBatchInsertBuilder
	opBatch  history.OperationLiquidityPoolBatchInsertBuilder
}

func NewLiquidityPoolsTransactionProcessor(
	lpLoader *history.Loader,
	txBatch history.TransactionLiquidityPoolBatchInsertBuilder,
	opBatch history.OperationLiquidityPoolBatchInsertBuilder,
) *LiquidityPoolsTransactionProcessor {
	return &LiquidityPoolsTransactionProcessor{
		lpLoader: lpLoader,
		txBatch:  txBatch,
		opBatch:  opBatch,
	}
}

func (p *LiquidityPoolsTransactionProcessor) ProcessTransaction(lcm xdr.LedgerCloseMeta, transaction ingest.LedgerTransaction) error {
	err := p.addTransactionLiquidityPools(lcm.LedgerSequence(), transaction)
	if err != nil {
		return err
	}

	err = p.addOperationLiquidityPools(lcm.LedgerSequence(), transaction)
	if err != nil {
		return err
	}

	return nil
}

func (p *LiquidityPoolsTransactionProcessor) addTransactionLiquidityPools(sequence uint32, transaction ingest.LedgerTransaction) error {
	transactionID := toid.New(int32(sequence), int32(transaction.Index), 0).ToInt64()
	transactionLiquidityPools, err := liquidityPoolsForTransaction(transaction)
	if err != nil {
		return errors.Wrap(err, "Could not determine liquidity pools for transaction")
	}

	for _, lp := range transactionLiquidityPools {
		if err = p.txBatch.Add(transactionID, p.lpLoader.GetFuture(lp)); err != nil {
			return err
		}
	}

	return nil
}

func liquidityPoolsForTransaction(transaction ingest.LedgerTransaction) ([]string, error) {
	changes, err := transaction.GetChanges()
	if err != nil {
		return nil, err
	}
	return liquidityPoolsForChanges(changes)
}

func dedupeLiquidityPools(in []string) (out []string, err error) {
	set := set.Set[string]{}
	for _, id := range in {
		set.Add(id)
	}

	for id := range set {
		out = append(out, id)
	}
	return
}

func liquidityPoolsForChanges(
	changes []ingest.Change,
) ([]string, error) {
	var lps []string
	set := map[xdr.PoolId]bool{}

	for _, c := range changes {
		if c.Type != xdr.LedgerEntryTypeLiquidityPool {
			continue
		}

		if c.Pre == nil && c.Post == nil {
			return nil, errors.New("Invalid io.Change: change.Pre == nil && change.Post == nil")
		}

		if c.Pre != nil {
			poolID := c.Pre.Data.MustLiquidityPool().LiquidityPoolId
			if !set[poolID] {
				lps = append(lps, PoolIDToString(poolID))
				set[poolID] = true
			}
		}
		if c.Post != nil {
			poolID := c.Post.Data.MustLiquidityPool().LiquidityPoolId
			if !set[poolID] {
				lps = append(lps, PoolIDToString(poolID))
				set[poolID] = true
			}
		}
	}

	return lps, nil
}

func (p *LiquidityPoolsTransactionProcessor) addOperationLiquidityPools(sequence uint32, transaction ingest.LedgerTransaction) error {
	liquidityPools, err := liquidityPoolsForOperations(transaction, sequence)
	if err != nil {
		return errors.Wrap(err, "could not determine operation liquidity pools")
	}

	for operationID, lps := range liquidityPools {
		for _, lp := range lps {
			if err := p.opBatch.Add(operationID, p.lpLoader.GetFuture(lp)); err != nil {
				return err
			}
		}
	}

	return nil
}

func liquidityPoolsForOperations(transaction ingest.LedgerTransaction, sequence uint32) (map[int64][]string, error) {
	lps := map[int64][]string{}

	for opi, op := range transaction.Envelope.Operations() {
		operation := transactionOperationWrapper{
			index:          uint32(opi),
			transaction:    transaction,
			operation:      op,
			ledgerSequence: sequence,
		}

		changes, err := transaction.GetOperationChanges(uint32(opi))
		if err != nil {
			return lps, err
		}
		c, err := liquidityPoolsForChanges(changes)
		if err != nil {
			return lps, errors.Wrapf(err, "reading operation %v liquidity pools", operation.ID())
		}
		lps[operation.ID()] = c
	}

	return lps, nil
}

func (p *LiquidityPoolsTransactionProcessor) Commit(ctx context.Context, session db.SessionInterface) error {
	err := p.txBatch.Exec(ctx, session)
	if err != nil {
		return err
	}
	err = p.opBatch.Exec(ctx, session)
	if err != nil {
		return err
	}
	return err
}
