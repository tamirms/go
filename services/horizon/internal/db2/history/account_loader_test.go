package history

import (
	"context"
	"github.com/stellar/go/services/horizon/internal/test"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestAccountLoader(t *testing.T) {
	tt := test.Start(t)
	defer tt.Finish()
	test.ResetHorizonDB(t, tt.HorizonDB)
	session := tt.HorizonSession()
	defer session.Close()

	loader := NewAccountLoader()
	acct := loader.GetFuture("GBRPYHIL2CI3FNQ4BXLFMNDLFJUNPU2HY3ZMFSHONUCEOASW7QC7OX2H")
	loader.GetFuture("GCYVFGI3SEQJGBNQQG7YCMFWEYOHK3XPVOVPA6C566PXWN4SN7LILZSM")
	loader.GetFuture("GBYSBDAJZMHL5AMD7QXQ3JEP3Q4GLKADWIJURAAHQALNAWD6Z5XF2RAC")
	loader.GetFuture("GAOQJGUAB7NI7K7I62ORBXMN3J4SSWQUQ7FOEPSDJ322W2HMCNWPHXFB")
	assert.NoError(t, loader.Exec(context.Background(), session))

	val, err := acct.Value()
	assert.NoError(t, err)
	t.Log(val)
}
