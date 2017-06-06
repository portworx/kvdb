package mem

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/Sirupsen/logrus"
	"github.com/portworx/kvdb"
	"github.com/portworx/kvdb/common"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	// Name is the name of this kvdb implementation.
	Name = "kv-mem"
	// KvSnap is an option passed to designate this kvdb as a snap.
	KvSnap       = "KvSnap"
	bootstrapKey = "bootstrap"
)

var (
	// ErrSnap is returned if an operation is not supported on a snap.
	ErrSnap = errors.New("operation not supported on snap")
)

func init() {
	if err := kvdb.Register(Name, New, Version); err != nil {
		panic(err.Error())
	}
}

type memKV struct {
	common.BaseKvdb
	// m is the key value database
	m map[string]*kvdb.KVPair
	// w is a set of watches currently active on keys
	w map[string]*watchData
	// wt is a set of watches currently active on trees
	wt map[string]*watchData
	// mutex protects m, w, wt
	mutex sync.Mutex
	// updateWKeys is true if wKeys was updated
	updateWatchKeys bool
	// watchUpdateQueue is the queue of updates for which watch should be fired
	watchUpdateQueue WatchUpdateQueue
	index            uint64
	domain           string
	kvdb.KvdbController
}

type snapMem struct {
	*memKV
}

// watchUpdate refers to an update to this kvdb
type watchUpdate struct {
	key string
	kvp kvdb.KVPair
	err error
}

// WatchUpdateQueue is a producer consumer queue.
type WatchUpdateQueue interface {
	// Enqueue will enqueue an update. It is non-blocking.
	Enqueue(update *watchUpdate)
	// Dequeue will either return an element from front of the queue or
	// will block until element becomes available
	Dequeue() *watchUpdate
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

func NewWatchUpdateQueue() WatchUpdateQueue {
	mtx := &sync.Mutex{}
	return &watchQueue{
		m:       mtx,
		cv:      sync.NewCond(mtx),
		updates: make([]*watchUpdate, 0)}
}

func (w *watchQueue) Dequeue() *watchUpdate {
	w.m.Lock()
	for {
		if len(w.updates) > 0 {
			update := w.updates[0]
			w.updates = w.updates[1:]
			w.m.Unlock()
			return update
		}
		w.cv.Wait()
	}
}

// Enqueue enqueues and never blocks
func (w *watchQueue) Enqueue(update *watchUpdate) {
	w.m.Lock()
	w.updates = append(w.updates, update)
	w.cv.Signal()
	w.m.Unlock()
}

type watchData struct {
	cb        kvdb.WatchCB
	opaque    interface{}
	waitIndex uint64
}

// New constructs a new kvdb.Kvdb.
func New(
	domain string,
	machines []string,
	options map[string]string,
	fatalErrorCb kvdb.FatalErrorCB,
) (kvdb.Kvdb, error) {
	if domain != "" && !strings.HasSuffix(domain, "/") {
		domain = domain + "/"
	}

	mem := &memKV{
		BaseKvdb:         common.BaseKvdb{FatalCb: fatalErrorCb},
		m:                make(map[string]*kvdb.KVPair),
		w:                make(map[string]*watchData),
		wt:               make(map[string]*watchData),
		watchUpdateQueue: NewWatchUpdateQueue(),
		updateWatchKeys:  true,
		domain:           domain,
		KvdbController:   kvdb.KvdbControllerNotSupported,
	}

	if _, ok := options[KvSnap]; ok {
		return &snapMem{memKV: mem}, nil
	}
	go mem.watchUpdates()
	return mem, nil
}

// Version returns the supported version of the mem implementation
func Version(url string, kvdbOptions map[string]string) (string, error) {
	return kvdb.MemVersion1, nil
}

func (kv *memKV) String() string {
	return Name
}

func (kv *memKV) Capabilities() int {
	return kvdb.KVCapabilityOrderedUpdates
}

func (kv *memKV) get(key string) (*kvdb.KVPair, error) {
	key = kv.domain + key
	v, ok := kv.m[key]
	if !ok {
		return nil, kvdb.ErrNotFound
	}
	return v, nil
}

func (kv *memKV) Get(key string) (*kvdb.KVPair, error) {
	kv.mutex.Lock()
	defer kv.mutex.Unlock()
	return kv.get(key)
}

func (kv *memKV) Snapshot(prefix string) (kvdb.Kvdb, uint64, error) {
	kv.mutex.Lock()
	defer kv.mutex.Unlock()
	_, err := kv.put(bootstrapKey, time.Now().UnixNano(), 0)
	if err != nil {
		return nil, 0, fmt.Errorf("Failed to create snap bootstrap key: %v", err)
	}
	data := make(map[string]*kvdb.KVPair)
	for key, value := range kv.m {
		if !strings.HasPrefix(key, prefix) && strings.Contains(key, "/_") {
			continue
		}
		snap := &kvdb.KVPair{}
		*snap = *value
		snap.Value = make([]byte, len(value.Value))
		copy(snap.Value, value.Value)
		data[key] = snap
	}
	highestKvPair, _ := kv.delete(bootstrapKey)
	// Snapshot only data, watches are not copied.
	return &memKV{
		m:      data,
		w:      make(map[string]*watchData),
		wt:     make(map[string]*watchData),
		domain: kv.domain,
	}, highestKvPair.ModifiedIndex, nil
}

func (kv *memKV) put(
	key string,
	value interface{},
	ttl uint64,
) (*kvdb.KVPair, error) {

	var kvp *kvdb.KVPair

	suffix := key
	key = kv.domain + suffix
	index := atomic.AddUint64(&kv.index, 1)
	if ttl != 0 {
		time.AfterFunc(time.Second*time.Duration(ttl), func() {
			// TODO: handle error
			_, _ = kv.delete(suffix)
		})
	}
	b, err := common.ToBytes(value)
	if err != nil {
		return nil, err
	}
	if old, ok := kv.m[key]; ok {
		old.Value = b
		old.Action = kvdb.KVSet
		old.ModifiedIndex = index
		old.KVDBIndex = index
		kvp = old

	} else {
		kvp = &kvdb.KVPair{
			Key:           key,
			Value:         b,
			TTL:           int64(ttl),
			KVDBIndex:     index,
			ModifiedIndex: index,
			CreatedIndex:  index,
			Action:        kvdb.KVCreate,
		}
		kv.m[key] = kvp
	}

	kv.normalize(kvp)
	kv.watchUpdateQueue.Enqueue(&watchUpdate{key, *kvp, nil})
	return kvp, nil
}

func (kv *memKV) Put(
	key string,
	value interface{},
	ttl uint64,
) (*kvdb.KVPair, error) {

	kv.mutex.Lock()
	defer kv.mutex.Unlock()
	return kv.put(key, value, ttl)
}

func (kv *memKV) GetVal(key string, v interface{}) (*kvdb.KVPair, error) {
	kvp, err := kv.Get(key)
	if err != nil {
		return nil, err
	}

	err = json.Unmarshal(kvp.Value, v)
	return kvp, err
}

func (kv *memKV) Create(
	key string,
	value interface{},
	ttl uint64,
) (*kvdb.KVPair, error) {
	kv.mutex.Lock()
	defer kv.mutex.Unlock()

	result, err := kv.get(key)
	if err != nil {
		return kv.put(key, value, ttl)
	}
	return result, kvdb.ErrExist
}

func (kv *memKV) Update(
	key string,
	value interface{},
	ttl uint64,
) (*kvdb.KVPair, error) {
	kv.mutex.Lock()
	defer kv.mutex.Unlock()

	if _, err := kv.get(key); err != nil {
		return nil, kvdb.ErrNotFound
	}
	return kv.put(key, value, ttl)
}

func (kv *memKV) Enumerate(prefix string) (kvdb.KVPairs, error) {
	var kvp = make(kvdb.KVPairs, 0, 100)
	prefix = kv.domain + prefix

	for k, v := range kv.m {
		if strings.HasPrefix(k, prefix) && !strings.Contains(k, "/_") {
			kvpLocal := *v
			kv.normalize(&kvpLocal)
			kvp = append(kvp, &kvpLocal)
		}
	}

	return kvp, nil
}

func (kv *memKV) delete(key string) (*kvdb.KVPair, error) {
	kvp, err := kv.get(key)
	if err != nil {
		return nil, err
	}
	kvp.KVDBIndex = atomic.AddUint64(&kv.index, 1)
	kvp.ModifiedIndex = kvp.KVDBIndex
	kvp.Action = kvdb.KVDelete
	delete(kv.m, kv.domain+key)
	kv.watchUpdateQueue.Enqueue(&watchUpdate{kv.domain + key, *kvp, nil})
	return kvp, nil
}

func (kv *memKV) Delete(key string) (*kvdb.KVPair, error) {
	kv.mutex.Lock()
	defer kv.mutex.Unlock()

	return kv.delete(key)
}

func (kv *memKV) DeleteTree(prefix string) error {
	kv.mutex.Lock()
	defer kv.mutex.Unlock()

	kvp, err := kv.Enumerate(prefix)
	if err != nil {
		return err
	}
	for _, v := range kvp {
		// TODO: multiple errors
		if _, iErr := kv.delete(v.Key); iErr != nil {
			err = iErr
		}
	}
	return err
}

func (kv *memKV) Keys(prefix, sep string) ([]string, error) {
	if "" == sep {
		sep = "/"
	}
	prefix = kv.domain + prefix
	lenPrefix := len(prefix)
	lenSep := len(sep)
	if prefix[lenPrefix-lenSep:] != sep {
		prefix += sep
		lenPrefix += lenSep
	}

	seen := make(map[string]bool)
	for k := range kv.m {
		if strings.HasPrefix(k, prefix) && !strings.Contains(k, "/_") {
			key := k[lenPrefix:]
			if idx := strings.Index(key, sep); idx > 0 {
				key = key[:idx]
			}
			seen[key] = true
		}
	}
	retList := make([]string, len(seen))
	i := 0
	for k := range seen {
		retList[i] = k
		i++
	}

	return retList, nil
}

func (kv *memKV) CompareAndSet(
	kvp *kvdb.KVPair,
	flags kvdb.KVFlags,
	prevValue []byte,
) (*kvdb.KVPair, error) {

	kv.mutex.Lock()
	defer kv.mutex.Unlock()

	result, err := kv.get(kvp.Key)
	if err != nil {
		return nil, err
	}
	if prevValue != nil {
		if !bytes.Equal(result.Value, prevValue) {
			return nil, kvdb.ErrValueMismatch
		}
	}
	if flags == kvdb.KVModifiedIndex {
		if kvp.ModifiedIndex != result.ModifiedIndex {
			return nil, kvdb.ErrValueMismatch
		}
	}
	return kv.put(kvp.Key, kvp.Value, 0)
}

func (kv *memKV) CompareAndDelete(
	kvp *kvdb.KVPair,
	flags kvdb.KVFlags,
) (*kvdb.KVPair, error) {
	kv.mutex.Lock()
	defer kv.mutex.Unlock()

	if flags != kvdb.KVFlags(0) {
		return nil, kvdb.ErrNotSupported
	}
	if result, err := kv.get(kvp.Key); err != nil {
		return nil, err
	} else if !bytes.Equal(result.Value, kvp.Value) {
		return nil, kvdb.ErrNotFound
	}
	return kv.delete(kvp.Key)
}

func (kv *memKV) WatchKey(
	key string,
	waitIndex uint64,
	opaque interface{},
	cb kvdb.WatchCB,
) error {
	kv.mutex.Lock()
	defer kv.mutex.Unlock()
	key = kv.domain + key
	if _, ok := kv.w[key]; ok {
		return kvdb.ErrExist
	}
	kv.w[key] = &watchData{cb: cb, waitIndex: waitIndex, opaque: opaque}
	kv.updateWatchKeys = true
	return nil
}

func (kv *memKV) WatchTree(
	prefix string,
	waitIndex uint64,
	opaque interface{},
	cb kvdb.WatchCB,
) error {

	kv.mutex.Lock()
	defer kv.mutex.Unlock()
	prefix = kv.domain + prefix
	if _, ok := kv.wt[prefix]; ok {
		return kvdb.ErrExist
	}
	kv.wt[prefix] = &watchData{cb: cb, waitIndex: waitIndex, opaque: opaque}
	kv.updateWatchKeys = true
	return nil
}

func (kv *memKV) Lock(key string) (*kvdb.KVPair, error) {
	return kv.LockWithID(key, "locked")
}

func (kv *memKV) LockWithID(
	key string,
	lockerID string,
) (*kvdb.KVPair, error) {
	key = kv.domain + key
	duration := time.Second

	result, err := kv.Create(key, lockerID, uint64(duration*3))
	count := 0
	for err != nil {
		time.Sleep(duration)
		result, err = kv.Create(key, lockerID, uint64(duration*3))
		if err != nil && count > 0 && count%15 == 0 {
			var currLockerID string
			if _, errGet := kv.GetVal(key, currLockerID); errGet == nil {
				logrus.Infof("Lock %v locked for %v seconds, tag: %v",
					key, count, currLockerID)
			}
		}
	}

	if err != nil {
		return nil, err
	}
	return result, err
}

func (kv *memKV) Unlock(kvp *kvdb.KVPair) error {
	_, err := kv.CompareAndDelete(kvp, kvdb.KVFlags(0))
	return err
}

func (kv *memKV) TxNew() (kvdb.Tx, error) {
	return nil, kvdb.ErrNotSupported
}

func (kv *memKV) normalize(kvp *kvdb.KVPair) {
	kvp.Key = strings.TrimPrefix(kvp.Key, kv.domain)
}

func copyWatchKeys(w map[string]*watchData) []string {
	keys := make([]string, len(w))
	i := 0
	for key := range w {
		keys[i] = key
		i++
	}
	return keys
}

func (kv *memKV) watchUpdates() {
	var watchKeys, watchTreeKeys []string
	for {
		update := kv.watchUpdateQueue.Dequeue()
		// Make a copy of the keys so that we can iterate over the map
		// without the lock.
		kv.mutex.Lock()
		if kv.updateWatchKeys {
			watchKeys = copyWatchKeys(kv.w)
			watchTreeKeys = copyWatchKeys(kv.wt)
			kv.updateWatchKeys = false
		}
		kv.mutex.Unlock()

		for _, k := range watchKeys {
			kv.mutex.Lock()
			v, ok := kv.w[k]
			kv.mutex.Unlock()
			if !ok {
				continue
			}
			if k == update.key && (v.waitIndex == 0 ||
				v.waitIndex < update.kvp.ModifiedIndex) {
				err := v.cb(update.key, v.opaque, &update.kvp, update.err)
				if err != nil {
					_ = v.cb("", v.opaque, nil, kvdb.ErrWatchStopped)
					kv.mutex.Lock()
					delete(kv.w, update.key)
					kv.updateWatchKeys = true
					kv.mutex.Unlock()
				}
			}
		}
		for _, k := range watchTreeKeys {
			kv.mutex.Lock()
			v, ok := kv.wt[k]
			kv.mutex.Unlock()
			if !ok {
				continue
			}
			if strings.HasPrefix(update.key, k) &&
				(v.waitIndex == 0 || v.waitIndex < update.kvp.ModifiedIndex) {
				err := v.cb(update.key, v.opaque, &update.kvp, update.err)
				if err != nil {
					_ = v.cb("", v.opaque, nil, kvdb.ErrWatchStopped)
					kv.mutex.Lock()
					delete(kv.wt, update.key)
					kv.updateWatchKeys = true
					kv.mutex.Unlock()
				}
			}
		}
	}
}

func (kv *memKV) SnapPut(snapKvp *kvdb.KVPair) (*kvdb.KVPair, error) {
	return nil, kvdb.ErrNotSupported
}

func (kv *snapMem) SnapPut(snapKvp *kvdb.KVPair) (*kvdb.KVPair, error) {
	var kvp *kvdb.KVPair

	key := kv.domain + snapKvp.Key
	kv.mutex.Lock()
	defer kv.mutex.Unlock()

	if old, ok := kv.m[key]; ok {
		old.Value = snapKvp.Value
		old.Action = kvdb.KVSet
		old.ModifiedIndex = snapKvp.ModifiedIndex
		old.KVDBIndex = snapKvp.KVDBIndex
		kvp = old

	} else {
		kvp = &kvdb.KVPair{
			Key:           key,
			Value:         snapKvp.Value,
			TTL:           0,
			KVDBIndex:     snapKvp.KVDBIndex,
			ModifiedIndex: snapKvp.ModifiedIndex,
			CreatedIndex:  snapKvp.CreatedIndex,
			Action:        kvdb.KVCreate,
		}
		kv.m[key] = kvp
	}

	kv.normalize(kvp)
	return kvp, nil
}

func (kv *snapMem) Put(
	key string,
	value interface{},
	ttl uint64,
) (*kvdb.KVPair, error) {
	return nil, ErrSnap
}

func (kv *snapMem) Create(
	key string,
	value interface{},
	ttl uint64,
) (*kvdb.KVPair, error) {
	return nil, ErrSnap
}

func (kv *snapMem) Update(
	key string,
	value interface{},
	ttl uint64,
) (*kvdb.KVPair, error) {
	return nil, ErrSnap
}

func (kv *snapMem) Delete(key string) (*kvdb.KVPair, error) {
	return nil, ErrSnap
}

func (kv *snapMem) DeleteTree(prefix string) error {
	return ErrSnap
}

func (kv *snapMem) CompareAndSet(
	kvp *kvdb.KVPair,
	flags kvdb.KVFlags,
	prevValue []byte,
) (*kvdb.KVPair, error) {
	return nil, ErrSnap
}

func (kv *snapMem) CompareAndDelete(
	kvp *kvdb.KVPair,
	flags kvdb.KVFlags,
) (*kvdb.KVPair, error) {
	return nil, ErrSnap
}

func (kv *snapMem) WatchKey(
	key string,
	waitIndex uint64,
	opaque interface{},
	watchCB kvdb.WatchCB,
) error {
	return ErrSnap
}

func (kv *snapMem) WatchTree(
	prefix string,
	waitIndex uint64,
	opaque interface{},
	watchCB kvdb.WatchCB,
) error {
	return ErrSnap
}

func (kv *memKV) AddUser(username string, password string) error {
	return kvdb.ErrNotSupported
}

func (kv *memKV) RemoveUser(username string) error {
	return kvdb.ErrNotSupported
}

func (kv *memKV) GrantUserAccess(
	username string,
	permType kvdb.PermissionType,
	subtree string,
) error {
	return kvdb.ErrNotSupported
}

func (kv *memKV) RevokeUsersAccess(
	username string,
	permType kvdb.PermissionType,
	subtree string,
) error {
	return kvdb.ErrNotSupported
}
