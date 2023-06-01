package history

import (
	"context"

	"github.com/stellar/go/support/db"
)

// OperationParticipantBatchInsertBuilder is used to insert a transaction's operations into the
// history_operations table
type OperationParticipantBatchInsertBuilder interface {
	Add(operationID int64, account FutureID) error
	Exec(ctx context.Context, session db.SessionInterface) error
}

// operationParticipantBatchInsertBuilder is a simple wrapper around db.BatchInsertBuilder
type operationParticipantBatchInsertBuilder struct {
	table   string
	builder db.FastBatchInsertBuilder
}

// NewOperationParticipantBatchInsertBuilder constructs a new TransactionBatchInsertBuilder instance
func (q *Q) NewOperationParticipantBatchInsertBuilder() OperationParticipantBatchInsertBuilder {
	return &operationParticipantBatchInsertBuilder{
		table:   "history_operation_participants",
		builder: db.FastBatchInsertBuilder{},
	}
}

// Add adds an operation participant to the batch
func (i *operationParticipantBatchInsertBuilder) Add(
	operationID int64,
	account FutureID,
) error {
	return i.builder.Row(map[string]interface{}{
		"history_operation_id": operationID,
		"history_account_id":   account,
	})
}

func (i *operationParticipantBatchInsertBuilder) Exec(ctx context.Context, session db.SessionInterface) error {
	return i.builder.Exec(ctx, session, i.table)
}
