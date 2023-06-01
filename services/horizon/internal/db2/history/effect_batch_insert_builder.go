package history

import (
	"context"

	"github.com/guregu/null"
	"github.com/stellar/go/support/db"
)

// EffectBatchInsertBuilder is used to insert effects into the
// history_effects table
type EffectBatchInsertBuilder interface {
	Add(
		account FutureID,
		muxedAccount null.String,
		operationID int64,
		order uint32,
		effectType EffectType,
		details []byte,
	) error
	Exec(ctx context.Context, session db.SessionInterface) error
}

// effectBatchInsertBuilder is a simple wrapper around db.BatchInsertBuilder
type effectBatchInsertBuilder struct {
	table   string
	builder db.FastBatchInsertBuilder
}

// NewEffectBatchInsertBuilder constructs a new EffectBatchInsertBuilder instance
func (q *Q) NewEffectBatchInsertBuilder() EffectBatchInsertBuilder {
	return &effectBatchInsertBuilder{
		table:   "history_effects",
		builder: db.FastBatchInsertBuilder{},
	}
}

// Add adds a effect to the batch
func (i *effectBatchInsertBuilder) Add(
	account FutureID,
	muxedAccount null.String,
	operationID int64,
	order uint32,
	effectType EffectType,
	details []byte,
) error {
	return i.builder.Row(map[string]interface{}{
		"history_account_id":   account,
		"address_muxed":        muxedAccount,
		"history_operation_id": operationID,
		"order":                order,
		"type":                 effectType,
		"details":              details,
	})
}

func (i *effectBatchInsertBuilder) Exec(ctx context.Context, session db.SessionInterface) error {
	return i.builder.Exec(ctx, session, i.table)
}
