package common

import (
	"encoding/json"
	"github.com/portworx/kvdb"
	"sync"
)

var (
	path = "/var/cores/"
)

// ToBytes converts to value to a byte slice.
func ToBytes(val interface{}) ([]byte, error) {
	switch val.(type) {
	case string:
		return []byte(val.(string)), nil
	case []byte:
		b := make([]byte, len(val.([]byte)))
		copy(b, val.([]byte))
		return b, nil
	default:
		return json.Marshal(val)
	}
}

// BaseKvdb provides common functionality across kvdb types
type BaseKvdb struct {
	// FatalCb invoked for fatal errors
	FatalCb kvdb.FatalErrorCB
}

// watchUpdate refers to an update to this kvdb
type watchUpdate struct {
	// key is the key that was updated
	key string
	// kvp is the key-value that was updated
	kvp *kvdb.KVPair
	// err is any error on update
	err error
}

// WatchUpdateQueue is a producer consumer queue.
type WatchUpdateQueue interface {
	// Enqueue will enqueue an update. It is non-blocking.
	Enqueue(key string, kvp *kvdb.KVPair, err error)
	// Dequeue will either return an element from front of the queue or
	// will block until element becomes available
	Dequeue() (string, *kvdb.KVPair, error)
}

// watchQueue implements WatchUpdateQueue interface for watchUpdates
type watchQueue struct {
	// updates is the list of updates
	updates []*watchUpdate
	// m is the mutex to protect updates
	m *sync.Mutex
	// cv is used to coordinate the producer-consumer threads
	cv *sync.Cond
}

// NewWatchUpdateQueue returns WatchUpdateQueue
func NewWatchUpdateQueue() WatchUpdateQueue {
	mtx := &sync.Mutex{}
	return &watchQueue{
		m:       mtx,
		cv:      sync.NewCond(mtx),
		updates: make([]*watchUpdate, 0)}
}

func (w *watchQueue) Dequeue() (string, *kvdb.KVPair, error) {
	w.m.Lock()
	for {
		if len(w.updates) > 0 {
			update := w.updates[0]
			w.updates = w.updates[1:]
			w.m.Unlock()
			return update.key, update.kvp, update.err
		}
		w.cv.Wait()
	}
}

// Enqueue enqueues and never blocks
func (w *watchQueue) Enqueue(key string, kvp *kvdb.KVPair, err error) {
	w.m.Lock()
	w.updates = append(w.updates, &watchUpdate{key: key, kvp: kvp, err: err})
	w.cv.Signal()
	w.m.Unlock()
}
