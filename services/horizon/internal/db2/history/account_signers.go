package history

import (
	sq "github.com/Masterminds/squirrel"
	"github.com/stellar/go/services/horizon/internal/db2"
	"github.com/stellar/go/support/errors"
)

// AccountsForSigner returns a list of `AccountSigner` rows for a given signer
func (q *Q) AccountsForSigner(signer string, page db2.PageQuery) ([]AccountSigner, error) {
	sql, err := page.ApplyTo(selectAccountSigners.Where("accounts_signers.signer = ?", signer), "accounts_signers.account")
	if err != nil {
		return nil, errors.Wrap(err, "could not apply query to page")
	}

	var results []AccountSigner
	if err := q.Select(&results, sql); err != nil {
		return nil, errors.Wrap(err, "could not run select query")
	}

	return results, nil
}

// CreateAccountSigner creates a row in the accounts_signers table
func (q *Q) CreateAccountSigner(account, signer string, weight int32) error {
	sql := sq.Insert("accounts_signers").
		Columns("account", "signer", "weight").
		Values(account, signer, weight).
		Suffix("ON CONFLICT (signer, account) DO UPDATE SET weight=EXCLUDED.weight")

	_, err := q.Exec(sql)
	return err
}

var selectAccountSigners = sq.Select("accounts_signers.*").From("accounts_signers")
