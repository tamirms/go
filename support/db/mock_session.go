package db

import (
	"context"
	"database/sql"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"time"

	"github.com/Masterminds/squirrel"
	sq "github.com/Masterminds/squirrel"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/mock"
)

var _ SessionInterface = (*MockSession)(nil)

type MockSession struct {
	mock.Mock
}

func (m *MockSession) Begin() error {
	args := m.Called()
	return args.Error(0)
}

func (m *MockSession) BeginTx(opts *sql.TxOptions) error {
	args := m.Called(opts)
	return args.Error(0)
}

func (m *MockSession) Rollback() error {
	args := m.Called()
	return args.Error(0)
}

func (m *MockSession) CopyFrom(ctx context.Context, tableName pgx.Identifier, columnNames []string, rowSrc pgx.CopyFromSource) (int64, error) {
	args := m.Called(ctx, tableName, columnNames, rowSrc)
	return args.Get(0).(int64), args.Error(1)
}

func (m *MockSession) BeginPgxTx(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}

// CommitPgxTx commits the current transaction
func (m *MockSession) CommitPgxTx(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}

// RollbackPgxTx rolls back the current transaction
func (m *MockSession) RollbackPgxTx(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}

// ExecRawPgx rolls back the current transaction
func (m *MockSession) ExecRawPgx(ctx context.Context, query string, vals ...any) (pgconn.CommandTag, error) {
	args := m.Called(ctx, query, vals)
	return args.Get(0).(pgconn.CommandTag), args.Error(1)
}

// QueryPgx rolls back the current transaction
func (m *MockSession) QueryPgx(ctx context.Context, query string, vals ...any) (pgx.Rows, error) {
	args := m.Called(ctx, query, vals)
	return args.Get(0).(pgx.Rows), args.Error(1)
}

func (m *MockSession) Commit() error {
	args := m.Called()
	return args.Error(0)
}

func (m *MockSession) GetTx() *sqlx.Tx {
	args := m.Called()
	return args.Get(0).(*sqlx.Tx)
}

func (m *MockSession) GetTxOptions() *sql.TxOptions {
	args := m.Called()
	return args.Get(0).(*sql.TxOptions)
}

func (m *MockSession) TruncateTables(ctx context.Context, tables []string) error {
	args := m.Called(ctx, tables)
	return args.Error(0)
}

func (m *MockSession) Clone() SessionInterface {
	args := m.Called()
	return args.Get(0).(SessionInterface)
}

func (m *MockSession) Close() error {
	args := m.Called()
	return args.Error(0)
}

func (m *MockSession) Get(ctx context.Context, dest interface{}, query sq.Sqlizer) error {
	args := m.Called(ctx, dest, query)
	return args.Error(0)
}

func (m *MockSession) GetRaw(ctx context.Context, dest interface{}, query string, args ...interface{}) error {
	argss := m.Called(ctx, dest, query, args)
	return argss.Error(0)
}

func (m *MockSession) Query(ctx context.Context, query squirrel.Sqlizer) (*sqlx.Rows, error) {
	args := m.Called(ctx, query)
	return args.Get(0).(*sqlx.Rows), args.Error(1)
}

func (m *MockSession) QueryRaw(ctx context.Context, query string, args ...interface{}) (*sqlx.Rows, error) {
	argss := m.Called(ctx, query, args)
	return argss.Get(0).(*sqlx.Rows), argss.Error(1)
}

func (m *MockSession) Select(ctx context.Context, dest interface{}, query squirrel.Sqlizer) error {
	argss := m.Called(ctx, dest, query)
	return argss.Error(0)
}

func (m *MockSession) SelectRaw(ctx context.Context,
	dest interface{},
	query string,
	args ...interface{},
) error {
	argss := m.Called(ctx, dest, query, args)
	return argss.Error(0)
}

func (m *MockSession) GetTable(name string) *Table {
	args := m.Called(name)
	return args.Get(0).(*Table)
}

func (m *MockSession) Exec(ctx context.Context, query squirrel.Sqlizer) (sql.Result, error) {
	args := m.Called(ctx, query)
	return args.Get(0).(sql.Result), args.Error(1)
}

func (m *MockSession) ExecRaw(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	argss := m.Called(ctx, query, args)
	return argss.Get(0).(sql.Result), argss.Error(1)
}

func (m *MockSession) NoRows(err error) bool {
	args := m.Called(err)
	return args.Get(0).(bool)
}

func (m *MockSession) Ping(ctx context.Context, timeout time.Duration) error {
	return m.Called(ctx, timeout).Error(0)
}

func (m *MockSession) DeleteRange(
	ctx context.Context,
	start, end int64,
	table string,
	idCol string,
) (err error) {
	return m.Called(ctx, start, end, table, idCol).Error(0)
}
