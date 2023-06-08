#!/bin/bash


psql $DATABASE_URL?sslmode=disable -c "\COPY history_accounts TO history_accounts.dump"
psql $DATABASE_URL?sslmode=disable -c "\COPY history_assets TO history_assets.dump;"
psql $DATABASE_URL?sslmode=disable -c "\COPY history_claimable_balances TO history_claimable_balances.dump;"
psql $DATABASE_URL?sslmode=disable -c "\COPY  history_effects TO history_effects.dump;"
psql $DATABASE_URL?sslmode=disable -c "\COPY  history_ledgers TO history_ledgers.dump;"
psql $DATABASE_URL?sslmode=disable -c "\COPY  history_liquidity_pools TO history_liquidity_pools.dump;"
psql $DATABASE_URL?sslmode=disable -c "\COPY  history_operation_claimable_balances TO history_operation_claimable_balances.dump;"
psql $DATABASE_URL?sslmode=disable -c "\COPY  history_operation_liquidity_pools TO history_operation_liquidity_pools.dump;"
psql $DATABASE_URL?sslmode=disable -c "\COPY  history_operation_participants TO history_operation_participants.dump;"
psql $DATABASE_URL?sslmode=disable -c "\COPY  history_operations TO history_operations.dump;"
psql $DATABASE_URL?sslmode=disable -c "\COPY  history_trades TO history_trades.dump;"
psql $DATABASE_URL?sslmode=disable -c "\COPY  history_trades_60000 TO history_trades_60000.dump;"
psql $DATABASE_URL?sslmode=disable -c "\COPY  history_transaction_claimable_balances TO history_transaction_claimable_balances.dump;"
psql $DATABASE_URL?sslmode=disable -c "\COPY  history_transaction_liquidity_pools TO history_transaction_liquidity_pools.dump;"
psql $DATABASE_URL?sslmode=disable -c "\COPY  history_transaction_participants TO history_transaction_participants.dump;"
psql $DATABASE_URL?sslmode=disable -c "\COPY  history_transactions TO history_transactions.dump;"