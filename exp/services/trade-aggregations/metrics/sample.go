package metrics

import (
	"sync"
	"time"

	"github.com/stellar/go/support/log"
	"github.com/stellar/go/support/ordered"
)

type Sample struct {
	name             string
	sum              time.Duration
	count            int64
	min              time.Duration
	max              time.Duration
	last             time.Duration
	reportFreq       int
	size             int
	lock             sync.RWMutex
	timeoutThreshold time.Duration
	timeoutCount     int64
}

func NewSample(name string, size, reportFreq int, timeoutThreshold time.Duration) *Sample {
	return &Sample{
		name:             name,
		size:             size,
		reportFreq:       reportFreq,
		timeoutThreshold: timeoutThreshold,
	}
}

func (s *Sample) Measure(f func()) time.Duration {
	start := time.Now()
	f()
	elapsed := time.Since(start)

	s.lock.Lock()
	defer s.lock.Unlock()

	s.sum += elapsed
	if s.count == 0 {
		s.min = elapsed
		s.max = elapsed
	} else {
		s.min = ordered.Min(s.min, elapsed)
		s.max = ordered.Max(s.max, elapsed)
	}
	s.count++
	s.last = elapsed
	if elapsed > s.timeoutThreshold {
		s.timeoutCount++
	}
	if s.count%int64(s.reportFreq) == 0 || s.count >= int64(s.size) {
		s.report()
		if s.count >= int64(s.size) {
			s.reset()
		}
	}
	return elapsed
}

func (s *Sample) Report() {
	s.lock.RLock()
	defer s.lock.RUnlock()
	s.report()
}

func (s *Sample) report() {
	if s.count == 0 {
		return
	}
	log.WithFields(log.F{
		"min":      s.min,
		"avg":      s.sum / time.Duration(s.count),
		"max":      s.max,
		"last":     s.last,
		"count":    s.count,
		"timeouts": s.timeoutCount,
	}).Infof("%s sample report", s.name)
}

func (s *Sample) reset() {
	s.sum = 0
	s.max = 0
	s.min = 0
	s.count = 0
	s.last = 0
	s.timeoutCount = 0
}
