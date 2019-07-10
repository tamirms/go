package history

import (
	"encoding/json"

	sq "github.com/Masterminds/squirrel"
	"github.com/go-errors/errors"
	"github.com/jmoiron/sqlx"
	"github.com/stellar/go/services/horizon/internal/db2"
	"github.com/stellar/go/services/horizon/internal/toid"
	"github.com/stellar/go/xdr"
)

func (t *Operation) IsTransactionSuccessful() bool {
	if t.TransactionSuccessful == nil {
		return true
	}

	return *t.TransactionSuccessful
}

// LedgerSequence return the ledger in which the effect occurred.
func (r *Operation) LedgerSequence() int32 {
	id := toid.Parse(r.ID)
	return id.LedgerSequence
}

// UnmarshalDetails unmarshals the details of this operation into `dest`
func (r *Operation) UnmarshalDetails(dest interface{}) error {
	if !r.DetailsString.Valid {
		return nil
	}

	err := json.Unmarshal([]byte(r.DetailsString.String), &dest)
	if err != nil {
		err = errors.Wrap(err, 1)
	}

	return err
}

// OperationFeeStats returns operation fee stats for the last 5 ledgers.
// Currently, we hard code the query to return the last 5 ledgers worth of transactions.
// TODO: make the number of ledgers configurable.
func (q *Q) OperationFeeStats(currentSeq int32, dest *FeeStats) error {
	return q.GetRaw(dest, `
		SELECT
			ceil(min(max_fee/operation_count))::bigint AS "min",
			ceil(mode() within group (order by max_fee/operation_count))::bigint AS "mode",
			ceil(percentile_cont(0.10) WITHIN GROUP (ORDER BY max_fee/operation_count))::bigint AS "p10",
			ceil(percentile_cont(0.20) WITHIN GROUP (ORDER BY max_fee/operation_count))::bigint AS "p20",
			ceil(percentile_cont(0.30) WITHIN GROUP (ORDER BY max_fee/operation_count))::bigint AS "p30",
			ceil(percentile_cont(0.40) WITHIN GROUP (ORDER BY max_fee/operation_count))::bigint AS "p40",
			ceil(percentile_cont(0.50) WITHIN GROUP (ORDER BY max_fee/operation_count))::bigint AS "p50",
			ceil(percentile_cont(0.60) WITHIN GROUP (ORDER BY max_fee/operation_count))::bigint AS "p60",
			ceil(percentile_cont(0.70) WITHIN GROUP (ORDER BY max_fee/operation_count))::bigint AS "p70",
			ceil(percentile_cont(0.80) WITHIN GROUP (ORDER BY max_fee/operation_count))::bigint AS "p80",
			ceil(percentile_cont(0.90) WITHIN GROUP (ORDER BY max_fee/operation_count))::bigint AS "p90",
			ceil(percentile_cont(0.95) WITHIN GROUP (ORDER BY max_fee/operation_count))::bigint AS "p95",
			ceil(percentile_cont(0.99) WITHIN GROUP (ORDER BY max_fee/operation_count))::bigint AS "p99"
		FROM history_transactions
		WHERE ledger_sequence > $1 AND ledger_sequence <= $2
	`, currentSeq-5, currentSeq)
}

// Operations provides a helper to filter the operations table with pre-defined
// filters.  See `OperationsQ` for the available filters.
func (q *Q) Operations(includeTransactions bool) *OperationsQ {
	query := &OperationsQ{
		parent:              q,
		opIdCol:             "hop.id",
		includeFailed:       false,
		includeTransactions: includeTransactions,
	}

	if includeTransactions {
		query.sql = selectOperationAndTransactions
	} else {
		query.sql = selectOperation
	}

	return query
}

// OperationByID loads a single operation with `id` into `dest`
func (q *Q) OperationByID(includeTransactions bool, id int64) (Operation, *Transaction, error) {
	sql := selectOperation
	if includeTransactions {
		sql = selectOperationAndTransactions
	}

	sql = sql.
		Limit(1).
		Where("hop.id = ?", id)

	if includeTransactions {
		rows, err := q.Query(sql)
		if err != nil {
			return Operation{}, nil, err
		}
		defer rows.Close()
		operations, transactions, err := parseOperationAndTransactionRecords(rows)
		if err != nil {
			return Operation{}, nil, err
		}
		if len(operations) != len(transactions) || len(operations) != 1 {
			return Operation{}, nil, errors.New("unexpected number of operations and transactions")
		}

		return operations[0], &transactions[0], nil
	} else {
		var operation Operation
		err := q.Get(&operation, sql)
		return operation, nil, err
	}
}

// ForAccount filters the operations collection to a specific account
func (q *OperationsQ) ForAccount(aid string) *OperationsQ {
	var account Account
	q.Err = q.parent.AccountByAddress(&account, aid)
	if q.Err != nil {
		return q
	}

	q.sql = q.sql.Join(
		"history_operation_participants hopp ON "+
			"hopp.history_operation_id = hop.id",
	).Where("hopp.history_account_id = ?", account.ID)

	// in order to use history_operation_participants.hist_op_p_id index
	q.opIdCol = "hopp.history_operation_id"

	return q
}

// ForLedger filters the query to a only operations in a specific ledger,
// specified by its sequence.
func (q *OperationsQ) ForLedger(seq int32) *OperationsQ {
	var ledger Ledger
	q.Err = q.parent.LedgerBySequence(&ledger, seq)
	if q.Err != nil {
		return q
	}

	start := toid.ID{LedgerSequence: seq}
	end := toid.ID{LedgerSequence: seq + 1}
	q.sql = q.sql.Where(
		"hop.id >= ? AND hop.id < ?",
		start.ToInt64(),
		end.ToInt64(),
	)

	return q
}

// ForTransaction filters the query to a only operations in a specific
// transaction, specified by the transactions's hex-encoded hash.
func (q *OperationsQ) ForTransaction(hash string) *OperationsQ {
	var tx Transaction
	q.Err = q.parent.TransactionByHash(&tx, hash)
	if q.Err != nil {
		return q
	}

	start := toid.Parse(tx.ID)
	end := start
	end.TransactionOrder++
	q.sql = q.sql.Where(
		"hop.id >= ? AND hop.id < ?",
		start.ToInt64(),
		end.ToInt64(),
	)

	return q
}

// OnlyPayments filters the query being built to only include operations that
// are in the "payment" class of operations:  CreateAccountOps, Payments, and
// PathPayments.
func (q *OperationsQ) OnlyPayments() *OperationsQ {
	q.sql = q.sql.Where(sq.Eq{"hop.type": []xdr.OperationType{
		xdr.OperationTypeCreateAccount,
		xdr.OperationTypePayment,
		xdr.OperationTypePathPayment,
		xdr.OperationTypeAccountMerge,
	}})
	return q
}

// IncludeFailed changes the query to include failed transactions.
func (q *OperationsQ) IncludeFailed() *OperationsQ {
	q.includeFailed = true
	return q
}

// Page specifies the paging constraints for the query being built by `q`.
func (q *OperationsQ) Page(page db2.PageQuery) *OperationsQ {
	if q.Err != nil {
		return q
	}

	q.sql, q.Err = page.ApplyTo(q.sql, q.opIdCol)
	return q
}

func parseOperationAndTransactionRecords(rows *sqlx.Rows) ([]Operation, []Transaction, error) {
	var operations []Operation
	var transactions []Transaction

	for rows.Next() {
		var operation Operation
		var transaction Transaction
		err := rows.Scan(
			&operation.ID,
			&operation.TransactionID,
			&operation.ApplicationOrder,
			&operation.Type,
			&operation.DetailsString,
			&operation.SourceAccount,
			&operation.TransactionHash,
			&operation.TxResult,
			&operation.TransactionSuccessful,
			&transaction.LedgerSequence,
			&transaction.ApplicationOrder,
			&transaction.Account,
			&transaction.AccountSequence,
			&transaction.MaxFee,
			&transaction.FeeCharged,
			&transaction.OperationCount,
			&transaction.TxEnvelope,
			&transaction.TxMeta,
			&transaction.TxFeeMeta,
			&transaction.CreatedAt,
			&transaction.UpdatedAt,
			&transaction.SignatureString,
			&transaction.MemoType,
			&transaction.Memo,
			&transaction.ValidAfter,
			&transaction.ValidBefore,
			&transaction.LedgerCloseTime,
		)
		if err != nil {
			return nil, nil, err
		}
		transaction.TxResult = operation.TxResult
		transaction.Successful = operation.TransactionSuccessful
		transaction.ID = operation.TransactionID
		transaction.TransactionHash = operation.TransactionHash

		operations = append(operations, operation)
		transactions = append(transactions, transaction)
	}

	return operations, transactions, nil
}

// Fetch returns results of the query specified by `q`
func (q *OperationsQ) Fetch() ([]Operation, []Transaction, error) {
	if q.Err != nil {
		return nil, nil, q.Err
	}

	if !q.includeFailed {
		q.sql = q.sql.
			Where("(ht.successful = true OR ht.successful IS NULL)")
	}

	var operations []Operation
	var transactions []Transaction

	if q.includeTransactions {
		var rows *sqlx.Rows
		rows, q.Err = q.parent.Query(q.sql)
		if q.Err != nil {
			return nil, nil, q.Err
		}
		defer rows.Close()
		operations, transactions, q.Err = parseOperationAndTransactionRecords(rows)
		if q.Err != nil {
			return nil, nil, q.Err
		}
	} else {
		q.Err = q.parent.Select(&operations, q.sql)
		if q.Err != nil {
			return nil, nil, q.Err
		}
	}

	for _, o := range operations {
		var resultXDR xdr.TransactionResult
		err := xdr.SafeUnmarshalBase64(o.TxResult, &resultXDR)
		if err != nil {
			return nil, nil, err
		}

		if !q.includeFailed {
			if !o.IsTransactionSuccessful() {
				return nil, nil, errors.Errorf("Corrupted data! `include_failed=false` but returned transaction is failed: %s", o.TransactionHash)
			}

			if resultXDR.Result.Code != xdr.TransactionResultCodeTxSuccess {
				return nil, nil, errors.Errorf("Corrupted data! `include_failed=false` but returned transaction is failed: %s %s", o.TransactionHash, o.TxResult)
			}
		}

		// Check if `successful` equals resultXDR
		if o.IsTransactionSuccessful() && resultXDR.Result.Code != xdr.TransactionResultCodeTxSuccess {
			return nil, nil, errors.Errorf("Corrupted data! `successful=true` but returned transaction is not success: %s %s", o.TransactionHash, o.TxResult)
		}

		if !o.IsTransactionSuccessful() && resultXDR.Result.Code == xdr.TransactionResultCodeTxSuccess {
			return nil, nil, errors.Errorf("Corrupted data! `successful=false` but returned transaction is success: %s %s", o.TransactionHash, o.TxResult)
		}
	}

	return operations, transactions, nil
}

var selectOperation = sq.Select(
	"hop.id, " +
		"hop.transaction_id, " +
		"hop.application_order, " +
		"hop.type, " +
		"hop.details, " +
		"hop.source_account, " +
		"ht.transaction_hash, " +
		"ht.tx_result, " +
		"ht.successful as transaction_successful").
	From("history_operations hop").
	LeftJoin("history_transactions ht ON ht.id = hop.transaction_id")

var selectOperationAndTransactions = sq.Select(
	"hop.id, " +
		"hop.transaction_id, " +
		"hop.application_order, " +
		"hop.type, " +
		"hop.details, " +
		"hop.source_account, " +
		"ht.transaction_hash, " +
		"ht.tx_result, " +
		"ht.successful as transaction_successful, " +
		"ht.ledger_sequence, " +
		"ht.application_order, " +
		"ht.account, " +
		"ht.account_sequence, " +
		"ht.max_fee, " +
		// `fee_charged` is NULL by default, DB needs to be reingested
		// to populate the value. If value is not present display `max_fee`.
		"COALESCE(ht.fee_charged, ht.max_fee) as fee_charged, " +
		"ht.operation_count, " +
		"ht.tx_envelope, " +
		"ht.tx_meta, " +
		"ht.tx_fee_meta, " +
		"ht.created_at, " +
		"ht.updated_at, " +
		"array_to_string(ht.signatures, ',') AS signatures, " +
		"ht.memo_type, " +
		"ht.memo, " +
		"lower(ht.time_bounds) AS valid_after, " +
		"upper(ht.time_bounds) AS valid_before, " +
		"hl.closed_at AS ledger_close_time").
	From("history_operations hop").
	LeftJoin("history_transactions ht ON ht.id = hop.transaction_id").
	LeftJoin("history_ledgers hl ON ht.ledger_sequence = hl.sequence")
