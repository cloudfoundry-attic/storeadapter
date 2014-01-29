package workerpool

import (
	"sync"
	"time"
)

type WorkerPool struct {
	workerChannels []chan func ()
	indexProvider  chan chan int
	lock           *sync.RWMutex
	stopped        bool

	timeSpentWorking     time.Duration
	usageSampleStartTime time.Time
}

func NewWorkerPool(poolSize int) (pool *WorkerPool) {
	pool = &WorkerPool{
		workerChannels: make([]chan func (), poolSize),
		indexProvider:  make(chan chan int, 0),
		lock:           &sync.RWMutex{},
	}

	pool.resetUsageTracking()
	go pool.mux()

	for i := range pool.workerChannels {
		pool.workerChannels[i] = make(chan func (), 0)
		go pool.startWorker(pool.workerChannels[i])
	}

	return
}

func (pool *WorkerPool) mux() {
	index := 0
	for {
		select {
		case c := <-pool.indexProvider:
			go func(index int) {
				c <- index
			}(index)
			index = (index + 1)%len(pool.workerChannels)
		}
	}
}

func (pool *WorkerPool) getNextIndex() int {
	c := make(chan int, 1)
	pool.indexProvider <- c
	return <-c
}

func (pool *WorkerPool) ScheduleWork(work func ()) {
	pool.lock.RLock()
	defer pool.lock.RUnlock()
	if pool.stopped {
		return
	}

	go func() {
		for {
			pool.lock.RLock()
			index := pool.getNextIndex()
			if !pool.stopped {
				select {
				case pool.workerChannels[index] <- work:
					pool.lock.RUnlock()
					return
				default:
					pool.lock.RUnlock()
					continue
				}
			}
		}
	}()
}

func (pool *WorkerPool) StopWorkers() {
	pool.lock.Lock()
	defer pool.lock.Unlock()
	if pool.stopped {
		return
	}
	pool.stopped = true
	for _, workerChannel := range pool.workerChannels {
		close(workerChannel)
	}
}

func (pool *WorkerPool) startWorker(workerChannel chan func ()) {
	for {
		f, ok := <-workerChannel
		if ok {
			tWork := time.Now()
			f()
			dtWork := time.Since(tWork)

			pool.lock.Lock()
			pool.timeSpentWorking += dtWork
			pool.lock.Unlock()
		} else {
			return
		}
	}
}

func (pool *WorkerPool) StartTrackingUsage() {
	pool.resetUsageTracking()
}

func (pool *WorkerPool) MeasureUsage() (usage float64, measurementDuration time.Duration) {
	pool.lock.Lock()
	timeSpentWorking := pool.timeSpentWorking
	measurementDuration = time.Since(pool.usageSampleStartTime)
	pool.lock.Unlock()

	usage = timeSpentWorking.Seconds()/(measurementDuration.Seconds()*float64(len(pool.workerChannels)))

	pool.resetUsageTracking()
	return usage, measurementDuration
}

func (pool *WorkerPool) resetUsageTracking() {
	pool.lock.Lock()
	pool.usageSampleStartTime = time.Now()
	pool.timeSpentWorking = 0
	pool.lock.Unlock()
}
