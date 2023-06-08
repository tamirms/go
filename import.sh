#!/bin/bash


psql "$DATABASE_URL" -c "\COPY history_accounts FROM history_accounts.dump"
psql "$DATABASE_URL" -c "\COPY history_assets FROM history_assets.dump;"
psql "$DATABASE_URL" -c "\COPY history_claimable_balances from history_claimable_balances.dump;"
psql "$DATABASE_URL" -c "\COPY  history_effects from history_effects.dump;"
psql "$DATABASE_URL" -c "\COPY  history_ledgers from history_ledgers.dump;"
psql "$DATABASE_URL" -c "\COPY  history_liquidity_pools from history_liquidity_pools.dump;"
psql "$DATABASE_URL" -c "\COPY  history_operation_claimable_balances from history_operation_claimable_balances.dump;"
psql "$DATABASE_URL" -c "\COPY  history_operation_liquidity_pools from history_operation_liquidity_pools.dump;"
psql "$DATABASE_URL" -c "\COPY  history_operation_participants from history_operation_participants.dump;"
psql "$DATABASE_URL" -c "\COPY  history_operations from history_operations.dump;"
psql "$DATABASE_URL" -c "\COPY  history_trades from history_trades.dump;"
psql "$DATABASE_URL" -c "\COPY  history_trades_60000 from history_trades_60000.dump;"
psql "$DATABASE_URL" -c "\COPY  history_transaction_claimable_balances from history_transaction_claimable_balances.dump;"
psql "$DATABASE_URL" -c "\COPY  history_transaction_liquidity_pools from history_transaction_liquidity_pools.dump;"
psql "$DATABASE_URL" -c "\COPY  history_transaction_participants from history_transaction_participants.dump;"
psql "$DATABASE_URL" -c "\COPY  history_transactions from history_transactions.dump;"