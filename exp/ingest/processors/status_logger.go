package processors

import (
	"context"
	"fmt"

	"github.com/stellar/go/exp/ingest/io"
	"github.com/stellar/go/exp/ingest/pipeline"
)

func (p *StatusLogger) ProcessState(ctx context.Context, store *pipeline.Store, r io.StateReadCloser, w io.StateWriteCloser) error {
	defer r.Close()
	defer w.Close()

	n := 0
	for {
		entry, err := r.Read()
		if err != nil {
			if err == io.EOF {
				break
			} else {
				return err
			}
		}

		n++

		if n%p.N == 0 {
			fmt.Printf("Read %d entries...\n", n)
		}

		err = w.Write(entry)
		if err != nil {
			if err == io.ErrClosedPipe {
				// Reader does not need more data
				return nil
			}
			return err
		}

		select {
		case <-ctx.Done():
			return nil
		default:
			continue
		}
	}

	return nil
}

func (n *StatusLogger) IsConcurrent() bool {
	return false
}

func (p *StatusLogger) Name() string {
	return fmt.Sprintf("StatusLogger (N=%d)", p.N)
}
