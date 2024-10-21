package ingest

import (
	"context"
	"sync"
	"time"

	"github.com/stellar/go/exp/services/trade-aggregations/metrics"
	"github.com/stellar/go/exp/services/trade-aggregations/store"
	"github.com/stellar/go/support/collections/heap"
)

type progressMarker struct {
	lock                sync.Mutex
	completed           map[int64]bool
	inProgress          *heap.Heap[int64]
	maxCompleted        int64
	progressWriteActive bool
	wg                  sync.WaitGroup
	writeDuration       *metrics.Sample
	writer              store.BigTableWriter
	done                bool
}

func newProgressMarker(writer store.BigTableWriter) *progressMarker {
	return &progressMarker{
		completed:     map[int64]bool{},
		writer:        writer,
		writeDuration: metrics.NewSample("writeProgress", 100, 1, time.Second*5),
		inProgress: heap.New(
			func(a, b int64) bool {
				return a < b
			},
			0,
		),
	}
}

func (p *progressMarker) markInProgress(id int64) {
	p.lock.Lock()
	defer p.lock.Unlock()
	p.inProgress.Push(id)
}

func (p *progressMarker) markCompleted(ctx context.Context, onError func(error), id int64) {
	p.lock.Lock()
	defer p.lock.Unlock()
	if p.done {
		return
	}
	p.completed[id] = true
	var updateMarker bool

	for p.inProgress.Len() > 0 && p.completed[p.inProgress.Peek()] {
		p.maxCompleted = p.inProgress.Pop()
		delete(p.completed, p.maxCompleted)
		updateMarker = true
	}

	if updateMarker && !p.progressWriteActive {
		p.progressWriteActive = true
		p.wg.Add(1)
		go func() {
			defer p.wg.Done()
			var finished bool
			for !finished {
				p.lock.Lock()
				maxCompleted := p.maxCompleted
				done := p.done
				p.lock.Unlock()
				if done {
					return
				}

				var err error
				p.writeDuration.Measure(func() {
					err = p.writer.WriteProgress(ctx, maxCompleted)
				})
				if err != nil {
					onError(err)
					return
				}

				p.lock.Lock()
				if p.maxCompleted > maxCompleted {
					maxCompleted = p.maxCompleted
				} else {
					p.progressWriteActive = false
					finished = true
				}
				p.lock.Unlock()
			}
		}()
	}
}

func (p *progressMarker) wait() {
	p.lock.Lock()
	p.done = true
	p.lock.Unlock()
	p.wg.Wait()
}
