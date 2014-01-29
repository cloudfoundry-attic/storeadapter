package workerpool

import (
	"sync"
	"time"
)

type WorkerPool struct {
	workQueue chan func ()
	workerCloseChannels []chan bool

	lock           *sync.RWMutex
	stopped        bool

	timeSpentWorking     time.Duration
	usageSampleStartTime time.Time
}

func NewWorkerPool(poolSize int) (pool *WorkerPool) {
	pool = &WorkerPool{
		workQueue: make(chan func (), 0),
		workerCloseChannels: make([]chan bool, poolSize),
		lock:           &sync.RWMutex{},
	}

	pool.resetUsageTracking()

	for i := range pool.workerCloseChannels {
		pool.workerCloseChannels[i] = make(chan bool, 0)
		go pool.startWorker(pool.workQueue, pool.workerCloseChannels[i])
	}

	return
}

func (pool *WorkerPool) ScheduleWork(work func ()) {
	pool.lock.RLock()
	defer pool.lock.RUnlock()
	if pool.stopped {
		return
	}

	go func() {
		pool.workQueue <- work
	}()
}

func (pool *WorkerPool) StopWorkers() {
	pool.lock.Lock()
	defer pool.lock.Unlock()
	if pool.stopped {
		return
	}
	pool.stopped = true

	for _, closeChan := range pool.workerCloseChannels {
		closeChan <- true
	}
}

func (pool *WorkerPool) startWorker(workChannel chan func (), closeChannel chan bool) {
	for {
		select {
		case f := <-workChannel:
			tWork := time.Now()
			f()
			dtWork := time.Since(tWork)

			pool.lock.Lock()
			pool.timeSpentWorking += dtWork
			pool.lock.Unlock()
		case <-closeChannel:
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

	usage = timeSpentWorking.Seconds()/(measurementDuration.Seconds()*float64(len(pool.workerCloseChannels)))

	pool.resetUsageTracking()
	return usage, measurementDuration
}

func (pool *WorkerPool) resetUsageTracking() {
	pool.lock.Lock()
	pool.usageSampleStartTime = time.Now()
	pool.timeSpentWorking = 0
	pool.lock.Unlock()
}
