package history

import (
	"context"
	"database/sql/driver"
	"fmt"
	sq "github.com/Masterminds/squirrel"
	"github.com/jackc/pgx/v5"
	"github.com/stellar/go/support/db"
	"github.com/stellar/go/xdr"
	"sort"
	"strings"
)

type FutureID struct {
	key    string
	loader *Loader
}

// Value implements the database/sql/driver Valuer interface.
func (a FutureID) Value() (driver.Value, error) {
	return a.loader.GetNow(a.key)
}

type Loader struct {
	table          string
	conflictFields []string
	schema         func([]interface{}) []upsertField
	queryRows      func(context.Context, db.SessionInterface, []interface{}) (map[string]int64, error)

	set map[string]interface{}
	ids map[string]int64
}

func NewAccountLoader() *Loader {
	return &Loader{
		table:          "history_accounts",
		conflictFields: []string{"address"},
		schema: func(keys []interface{}) []upsertField {
			return []upsertField{
				{
					name:    "address",
					dbType:  "character varying(64)",
					objects: keys,
				},
			}
		},
		queryRows: func(ctx context.Context, session db.SessionInterface, keys []interface{}) (map[string]int64, error) {
			sql := selectAccount.Where(map[string]interface{}{
				"ha.address": keys, // ha.address IN (...)
			})
			query, args, err := sql.ToSql()
			if err != nil {
				return nil, err
			}
			rows, err := session.QueryPgx(ctx, query, args...)
			if err != nil {
				return nil, err
			}
			accountRows, err := pgx.CollectRows(rows, pgx.RowToStructByName[Account])
			if err != nil {
				return nil, err
			}
			result := map[string]int64{}
			for _, row := range accountRows {
				result[row.Address] = row.ID
			}
			return result, nil
		},

		set: map[string]interface{}{},
		ids: map[string]int64{},
	}
}

func NewClaimableBalanceLoader() *Loader {
	return &Loader{
		table:          "history_claimable_balances",
		conflictFields: []string{"claimable_balance_id"},
		schema: func(keys []interface{}) []upsertField {
			return []upsertField{
				{
					name:    "claimable_balance_id",
					dbType:  "text",
					objects: keys,
				},
			}
		},
		queryRows: func(ctx context.Context, session db.SessionInterface, keys []interface{}) (map[string]int64, error) {
			sql := selectHistoryClaimableBalance.Where(map[string]interface{}{
				"hcb.claimable_balance_id": keys, // hcb.claimable_balance_id IN (...)
			})
			query, args, err := sql.ToSql()
			if err != nil {
				return nil, err
			}
			rows, err := session.QueryPgx(ctx, query, args...)
			if err != nil {
				return nil, err
			}
			cbRows, err := pgx.CollectRows(rows, pgx.RowToStructByName[HistoryClaimableBalance])
			if err != nil {
				return nil, err
			}
			result := map[string]int64{}
			for _, row := range cbRows {
				result[row.BalanceID] = row.InternalID
			}
			return result, nil
		},

		set: map[string]interface{}{},
		ids: map[string]int64{},
	}
}

func NewLiquidityPoolLoader() *Loader {
	return &Loader{
		table:          "history_liquidity_pools",
		conflictFields: []string{"liquidity_pool_id"},
		schema: func(keys []interface{}) []upsertField {
			return []upsertField{
				{
					name:    "liquidity_pool_id",
					dbType:  "text",
					objects: keys,
				},
			}
		},
		queryRows: func(ctx context.Context, session db.SessionInterface, keys []interface{}) (map[string]int64, error) {
			sql := selectHistoryLiquidityPool.Where(map[string]interface{}{
				"hlp.liquidity_pool_id": keys, // hlp.liquidity_pool_id IN (...)
			})
			query, args, err := sql.ToSql()
			if err != nil {
				return nil, err
			}
			rows, err := session.QueryPgx(ctx, query, args...)
			if err != nil {
				return nil, err
			}
			lpRows, err := pgx.CollectRows(rows, pgx.RowToStructByName[HistoryLiquidityPool])
			if err != nil {
				return nil, err
			}
			result := map[string]int64{}
			for _, row := range lpRows {
				result[row.PoolID] = row.InternalID
			}
			return result, nil
		},

		set: map[string]interface{}{},
		ids: map[string]int64{},
	}
}

func NewAssetLoader() *Loader {
	return &Loader{
		table:          "history_assets",
		conflictFields: []string{"asset_code", "asset_type", "asset_issuer"},
		schema: func(keys []interface{}) []upsertField {
			codes := make([]interface{}, len(keys))
			issuers := make([]interface{}, len(keys))
			types := make([]interface{}, len(keys))
			for i, k := range keys {
				key := k.(string)
				parts := strings.Split(key, "/")
				if len(parts) == 1 {
					types[i] = parts[0]
					codes[i] = ""
					issuers[i] = ""
				} else {
					types[i] = parts[0]
					codes[i] = parts[1]
					issuers[i] = parts[2]
				}
			}

			return []upsertField{
				{
					name:    "asset_code",
					dbType:  "character varying(12)",
					objects: codes,
				},
				{
					name:    "asset_issuer",
					dbType:  "character varying(56)",
					objects: issuers,
				},
				{
					name:    "asset_type",
					dbType:  "character varying(64)",
					objects: types,
				},
			}
		},
		queryRows: func(ctx context.Context, session db.SessionInterface, keys []interface{}) (map[string]int64, error) {
			for i, key := range keys {
				strKey := key.(string)
				if strKey == xdr.AssetTypeToString[xdr.AssetTypeAssetTypeNative] {
					keys[i] = strKey + "//"
				}
			}
			sql := sq.Select("*").From("history_assets").Where(sq.Eq{
				"concat(asset_type, '/', asset_code, '/', asset_issuer)": keys,
			})
			query, args, err := sql.ToSql()
			if err != nil {
				return nil, err
			}
			rows, err := session.QueryPgx(ctx, query, args...)
			if err != nil {
				return nil, err
			}
			assetRows, err := pgx.CollectRows(rows, pgx.RowToStructByName[Asset])
			if err != nil {
				return nil, err
			}
			result := map[string]int64{}
			for _, row := range assetRows {
				if row.Type == xdr.AssetTypeToString[xdr.AssetTypeAssetTypeNative] {
					result[row.Type] = row.ID
				} else {
					result[row.Type+"/"+row.Code+"/"+row.Issuer] = row.ID
				}
			}
			return result, nil
		},

		set: map[string]interface{}{},
		ids: map[string]int64{},
	}
}

func (a *Loader) GetFuture(key string) FutureID {
	a.set[key] = nil
	return FutureID{
		key:    key,
		loader: a,
	}
}

func (a *Loader) GetNow(key string) (int64, error) {
	if id, ok := a.ids[key]; !ok {
		return 0, fmt.Errorf("key %v not present", key)
	} else {
		return id, nil
	}
}

func (a *Loader) lookupKeys(ctx context.Context, session db.SessionInterface, keys []interface{}) error {
	const batchSize = 50000
	for i := 0; i < len(keys); i += batchSize {
		end := i + batchSize
		if end > len(keys) {
			end = len(keys)
		}
		result, err := a.queryRows(ctx, session, keys[i:end])
		if err != nil {
			return err
		}
		for key, val := range result {
			a.ids[key] = val
		}
	}
	return nil
}

func (a *Loader) Exec(ctx context.Context, session db.SessionInterface) error {
	if len(a.set) == 0 {
		return nil
	}
	keys := make([]interface{}, 0, len(a.set))
	for key := range a.set {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		return keys[i].(string) < keys[j].(string)
	})

	if err := a.lookupKeys(ctx, session, keys); err != nil {
		return err
	}
	insert := 0
	for i, key := range keys {
		if _, ok := a.ids[key.(string)]; ok {
			continue
		}
		if i != insert {
			keys[insert] = keys[i]
		}
		insert++
	}
	if insert == 0 {
		return nil
	}
	keys = keys[:insert]

	err := bulkInsert(
		ctx,
		session,
		a.table,
		a.conflictFields,
		a.schema(keys),
		[]string{},
	)
	if err != nil {
		return err
	}

	return a.lookupKeys(ctx, session, keys)
}

func (a *Loader) Reset() {
	a.set = map[string]interface{}{}
	a.ids = map[string]int64{}
}
