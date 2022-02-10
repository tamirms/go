package ledgerbackend

import (
	"bytes"
	"cloud.google.com/go/storage"
	"context"
	"fmt"
	"github.com/stellar/go/xdr"
	"io"
	"strconv"
	"time"
)

type GCSBackend struct {
	Bucket *storage.BucketHandle
}

func (b GCSBackend) GetLatestLedgerSequence(ctx context.Context) (uint32, error) {
	r, err := b.Bucket.Object("latest").NewReader(context.Background())
	if err != nil {
		return 0, err
	}
	defer r.Close()

	var buf bytes.Buffer
	if _, err := io.Copy(&buf, r); err != nil {
		return 0, err
	}
	if parsed, err := strconv.ParseUint(buf.String(), 10, 32); err != nil {
		return 0, err
	} else {
		return uint32(parsed), nil
	}
}

func (b GCSBackend) GetLedger(ctx context.Context, sequence uint32) (xdr.LedgerCloseMeta, error) {
	var ledger xdr.LedgerCloseMeta
	startTime := time.Now()
	r, err := b.Bucket.Object("ledgers/" + strconv.FormatUint(uint64(sequence), 10)).NewReader(context.Background())
	if err != nil {
		return ledger, err
	}
	defer r.Close()

	var buf bytes.Buffer
	if _, err := io.Copy(&buf, r); err != nil {
		return ledger, err
	}
	if err = ledger.UnmarshalBinary(buf.Bytes()); err != nil {
		return ledger, err
	}

	duration := time.Now().Sub(startTime)
	fmt.Printf("duration is %v\n", duration)
	return ledger, nil
}

func (b GCSBackend) PrepareRange(ctx context.Context, ledgerRange Range) error {
	return nil
}

func (b GCSBackend) IsPrepared(ctx context.Context, ledgerRange Range) (bool, error) {
	return true, nil
}

func (b GCSBackend) Close() error {
	return b.Close()
}
