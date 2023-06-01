package db

import (
	"context"
	"github.com/jackc/pgx/v5"
	"reflect"
	"sort"

	"github.com/stellar/go/support/errors"
)

// FastBatchInsertBuilder works like sq.InsertBuilder but has a better support for batching
// large number of rows.
// It is NOT safe for concurrent use.
type FastBatchInsertBuilder struct {
	columns       []string
	rows          [][]interface{}
	rowStructType reflect.Type
}

// Row adds a new row to the batch. All rows must have exactly the same columns
// (map keys). Otherwise, error will be returned. Please note that rows are not
// added one by one but in batches when `Exec` is called (or `MaxBatchSize` is
// reached).
func (b *FastBatchInsertBuilder) Row(row map[string]interface{}) error {
	if b.columns == nil {
		b.columns = make([]string, 0, len(row))
		b.rows = make([][]interface{}, 0)

		for column := range row {
			b.columns = append(b.columns, column)
		}

		sort.Strings(b.columns)
	}

	if len(b.columns) != len(row) {
		return errors.Errorf("invalid number of columns (expected=%d, actual=%d)", len(b.columns), len(row))
	}

	rowSlice := make([]interface{}, 0, len(b.columns))
	for _, column := range b.columns {
		val, ok := row[column]
		if !ok {
			return errors.Errorf(`column "%s" does not exist`, column)
		}
		rowSlice = append(rowSlice, val)
	}
	b.rows = append(b.rows, rowSlice)

	return nil
}

func (b *FastBatchInsertBuilder) RowStruct(row interface{}) error {
	if b.columns == nil {
		b.columns = ColumnsForStruct(row)
		b.rows = make([][]interface{}, 0)
	}

	rowType := reflect.TypeOf(row)
	if b.rowStructType == nil {
		b.rowStructType = rowType
	} else if b.rowStructType != rowType {
		return errors.Errorf(`expected value of type "%s" but got "%s" value`, b.rowStructType.String(), rowType.String())
	}

	rrow := reflect.ValueOf(row)
	rvals := mapper.FieldsByName(rrow, b.columns)

	// convert fields values to interface{}
	columnValues := make([]interface{}, len(b.columns))
	for i, rval := range rvals {
		columnValues[i] = rval.Interface()
	}

	b.rows = append(b.rows, columnValues)

	return nil
}

func (b *FastBatchInsertBuilder) Len() int {
	return len(b.rows)
}

// Exec inserts rows in batches. In case of errors it's possible that some batches
// were added so this should be run in a DB transaction for easy rollbacks.
func (b *FastBatchInsertBuilder) Exec(ctx context.Context, session SessionInterface, tableName string) error {
	if len(b.rows) == 0 {
		return nil
	}

	_, err := session.CopyFrom(ctx, pgx.Identifier{tableName}, b.columns, pgx.CopyFromRows(b.rows))
	if err != nil {
		return err
	}
	b.rows = [][]interface{}{}
	return nil
}
