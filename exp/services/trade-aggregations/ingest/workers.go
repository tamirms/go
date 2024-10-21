package ingest

type workerGroup struct {
	sem        chan struct{}
	maxWorkers int
}

func newWorkerGroup(maxWorkers int) workerGroup {
	sem := make(chan struct{}, maxWorkers)
	for i := 0; i < maxWorkers; i++ {
		sem <- struct{}{}
	}
	return workerGroup{
		sem:        sem,
		maxWorkers: maxWorkers,
	}
}

func (w workerGroup) run(task func()) {
	_, ok := <-w.sem
	if !ok {
		return
	}
	go func() {
		defer func() {
			w.sem <- struct {
			}{}
		}()
		task()
	}()
}

func (w workerGroup) wait() {
	for i := 0; i < w.maxWorkers; i++ {
		<-w.sem
	}
	close(w.sem)
}
