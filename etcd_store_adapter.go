package storeadapter

import (
	"github.com/cloudfoundry/storeadapter/workerpool"
	"github.com/coreos/go-etcd/etcd"
	"github.com/nu7hatch/gouuid"
	"path"
	"time"
)

type ETCDStoreAdapter struct {
	urls       []string
	client     *etcd.Client
	workerPool *workerpool.WorkerPool
}

func NewETCDStoreAdapter(urls []string, workerPool *workerpool.WorkerPool) *ETCDStoreAdapter {
	return &ETCDStoreAdapter{
		urls:       urls,
		workerPool: workerPool,
	}
}

func (adapter *ETCDStoreAdapter) Connect() error {
	adapter.client = etcd.NewClient(adapter.urls)

	return nil
}

func (adapter *ETCDStoreAdapter) Disconnect() error {
	adapter.workerPool.StopWorkers()

	return nil
}

func (adapter *ETCDStoreAdapter) isEventIndexClearedError(err error) bool {
	return adapter.etcdErrorCode(err) == 401
}

func (adapter *ETCDStoreAdapter) etcdErrorCode(err error) int {
	if err != nil {
		switch err.(type) {
		case etcd.EtcdError:
			return err.(etcd.EtcdError).ErrorCode
		case *etcd.EtcdError:
			return err.(*etcd.EtcdError).ErrorCode
		}
	}
	return 0
}

func (adapter *ETCDStoreAdapter) convertError(err error) error {
	switch adapter.etcdErrorCode(err) {
	case 501:
		return ErrorTimeout
	case 100:
		return ErrorKeyNotFound
	case 102:
		return ErrorNodeIsDirectory
	case 105:
		return ErrorKeyExists
	}

	return err
}

func (adapter *ETCDStoreAdapter) SetMulti(nodes []StoreNode) error {
	results := make(chan error, len(nodes))

	for _, node := range nodes {
		node := node
		adapter.workerPool.ScheduleWork(func() {
			_, err := adapter.client.Set(node.Key, string(node.Value), node.TTL)
			results <- err
		})
	}

	var err error
	numReceived := 0
	for numReceived < len(nodes) {
		result := <-results
		numReceived++
		if err == nil {
			err = result
		}
	}

	return adapter.convertError(err)
}

func (adapter *ETCDStoreAdapter) Get(key string) (StoreNode, error) {
	done := make(chan bool, 1)
	var response *etcd.Response
	var err error

	//we route through the worker pool to enable usage tracking
	adapter.workerPool.ScheduleWork(func() {
		response, err = adapter.client.Get(key, false, false)
		done <- true
	})

	<-done

	if err != nil {
		return StoreNode{}, adapter.convertError(err)
	}

	if response.Node.Dir {
		return StoreNode{}, ErrorNodeIsDirectory
	}

	return StoreNode{
		Key:   response.Node.Key,
		Value: []byte(response.Node.Value),
		Dir:   response.Node.Dir,
		TTL:   uint64(response.Node.TTL),
	}, nil
}

func (adapter *ETCDStoreAdapter) ListRecursively(key string) (StoreNode, error) {
	done := make(chan bool, 1)
	var response *etcd.Response
	var err error

	//we route through the worker pool to enable usage tracking
	adapter.workerPool.ScheduleWork(func() {
		response, err = adapter.client.Get(key, false, true)
		done <- true
	})

	<-done

	if err != nil {
		return StoreNode{}, adapter.convertError(err)
	}

	if !response.Node.Dir {
		return StoreNode{}, ErrorNodeIsNotDirectory
	}

	if len(response.Node.Nodes) == 0 {
		return StoreNode{Key: key, Dir: true, Value: []byte{}, ChildNodes: []StoreNode{}}, nil
	}

	return adapter.makeStoreNode(*response.Node), nil
}

func (adapter *ETCDStoreAdapter) Watch(key string) (<-chan WatchEvent, chan<- bool, <-chan error) {
	events := make(chan WatchEvent)
	errors := make(chan error, 1)
	stop := make(chan bool, 1)

	go adapter.dispatchWatchEvents(key, events, stop, errors)

	return events, stop, errors
}

func (adapter *ETCDStoreAdapter) Create(node StoreNode) error {
	results := make(chan error, 1)

	adapter.workerPool.ScheduleWork(func() {
		_, err := adapter.client.Create(node.Key, string(node.Value), node.TTL)
		results <- err
	})

	return adapter.convertError(<-results)
}

func (adapter *ETCDStoreAdapter) Delete(keys ...string) error {
	results := make(chan error, len(keys))

	for _, key := range keys {
		key := key
		adapter.workerPool.ScheduleWork(func() {
			_, err := adapter.client.Delete(key, true)
			results <- err
		})
	}

	var err error
	numReceived := 0
	for numReceived < len(keys) {
		result := <-results
		numReceived++
		if err == nil {
			err = result
		}
	}

	return adapter.convertError(err)
}

func (adapter *ETCDStoreAdapter) dispatchWatchEvents(key string, events chan<- WatchEvent, stop <-chan bool, errors chan<- error) {
	var index uint64

	for {
		response, err := adapter.client.Watch(key, index, true, nil, nil)
		if err != nil {
			if adapter.isEventIndexClearedError(err) {
				index++
				continue
			} else {
				errors <- adapter.convertError(err)
				return
			}
		}

		select {
		case events <- adapter.makeWatchEvent(response):
		case <-stop:
			close(events)
			return
		}

		index = response.Node.ModifiedIndex + 1
	}
}

func (adapter *ETCDStoreAdapter) makeStoreNode(etcdNode etcd.Node) StoreNode {
	if etcdNode.Dir {
		node := StoreNode{
			Key:        etcdNode.Key,
			Dir:        true,
			Value:      []byte{},
			ChildNodes: []StoreNode{},
		}

		for _, child := range etcdNode.Nodes {
			node.ChildNodes = append(node.ChildNodes, adapter.makeStoreNode(child))
		}

		return node
	} else {
		return StoreNode{
			Key:   etcdNode.Key,
			Value: []byte(etcdNode.Value),
			TTL:   uint64(etcdNode.TTL),
		}
	}
}

func (adapter *ETCDStoreAdapter) makeWatchEvent(event *etcd.Response) WatchEvent {
	var eventType EventType
	var node *etcd.Node

	if event.Action == "delete" {
		eventType = DeleteEvent
		node = event.PrevNode
	}

	if event.Action == "create" {
		eventType = CreateEvent
		node = event.Node
	}

	if event.Action == "set" {
		eventType = UpdateEvent
		node = event.Node
	}

	if event.Action == "expire" {
		eventType = ExpireEvent
		node = event.PrevNode
	}

	return WatchEvent{
		Type: eventType,
		Node: adapter.makeStoreNode(*node),
	}
}

func (adapter *ETCDStoreAdapter) lockKey(lockName string) string {
	return path.Join("/hm/locks", lockName)
}

func (adapter *ETCDStoreAdapter) GetAndMaintainLock(lockName string, lockTTL uint64) (lostLock <-chan bool, releaseLock chan<- bool, err error) {
	if lockTTL == 0 {
		return nil, nil, ErrorInvalidTTL
	}

	guid, err := uuid.NewV4()
	if err != nil {
		return nil, nil, err
	}
	currentLockValue := guid.String()

	lockKey := adapter.lockKey(lockName)

	releaseLockChannel := make(chan bool, 0)
	lostLockChannel := make(chan bool, 0)

	for {
		_, err := adapter.client.Create(lockKey, currentLockValue, lockTTL)
		convertedError := adapter.convertError(err)
		if convertedError == ErrorTimeout {
			return nil, nil, ErrorTimeout
		}

		if err == nil {
			break
		}

		time.Sleep(1 * time.Second)
	}

	go adapter.maintainLock(lockKey, currentLockValue, lockTTL, lostLockChannel, releaseLockChannel)

	return lostLockChannel, releaseLockChannel, nil
}

func (adapter *ETCDStoreAdapter) maintainLock(lockKey string, currentLockValue string, lockTTL uint64, lostLockChannel chan bool, releaseLockChannel chan bool) {
	maintenanceInterval := time.Duration(lockTTL) * time.Second / time.Duration(2)
	ticker := time.NewTicker(maintenanceInterval)
	for {
		select {
		case <-ticker.C:
			_, err := adapter.client.CompareAndSwap(lockKey, currentLockValue, lockTTL, currentLockValue, 0)
			if err != nil {
				lostLockChannel <- true
			}
		case <-releaseLockChannel:
			adapter.client.CompareAndSwap(lockKey, currentLockValue, 1, currentLockValue, 0)
			return
		}
	}
}
