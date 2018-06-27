package bolt

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/boltdb/bolt"
	"github.com/hashicorp/memberlist"
	"github.com/portworx/kvdb"
	"github.com/portworx/kvdb/common"
)

const (
	// Name is the name of this kvdb implementation.
	Name = "bolt-kv"
	// KvSnap is an option passed to designate this kvdb as a snap.
	KvSnap = "KvSnap"
	// KvUseInterface is an option passed that configures the mem to store
	// the values as interfaces instead of bytes. It will not create a
	// copy of the interface that is passed in. USE WITH CAUTION
	KvUseInterface = "KvUseInterface"
	// bootstrapkey is the name of the KV bootstrap key space.
	bootstrapKey = "bootstrap"
	// dbName is the name of the bolt database file.
	dbPath = "px.db"
)

var (
	// ErrSnap is returned if an operation is not supported on a snap.
	ErrSnap = errors.New("operation not supported on snap")
	// ErrSnapWithInterfaceNotSupported is returned when a snap kv-mem is
	// created with KvUseInterface flag on
	ErrSnapWithInterfaceNotSupported = errors.New("snap kvdb not supported with interfaces")
	// ErrIllegalSelect is returned when an incorrect select function
	// implementation is detected.
	ErrIllegalSelect = errors.New("Illegal Select implementation")
	// pxBucket is the name of the default PX keyspace in the internal KVDB.
	pxBucket = []byte("px")
)

func init() {
	logrus.Infof("Registering internal KVDB provider")
	if err := kvdb.Register(Name, New, Version); err != nil {
		panic(err.Error())
	}
}

type inKV struct {
	common.BaseKvdb

	// db is the handle to the bolt DB.
	db *bolt.DB
	// locks is the map of currently held locks
	locks map[string]chan int

	// updates is the list of latest few updates
	dist WatchDistributor

	// mutex protects m, w, wt
	mutex sync.Mutex

	// index current kvdb index
	index  uint64
	domain string

	kvdb.Controller
}

// watchUpdate refers to an update to this kvdb
type watchUpdate struct {
	// key is the key that was updated
	key string
	// kvp is the key-value that was updated
	kvp kvdb.KVPair
	// err is any error on update
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

// WatchDistributor distributes updates to the watchers
type WatchDistributor interface {
	// Add creates a new watch queue to send updates
	Add() WatchUpdateQueue
	// Remove removes an existing watch queue
	Remove(WatchUpdateQueue)
	// NewUpdate is invoked to distribute a new update
	NewUpdate(w *watchUpdate)
}

// distributor implements WatchDistributor interface
type distributor struct {
	sync.Mutex
	// updates is the list of latest few updates
	updates []*watchUpdate
	// watchers watch for updates
	watchers []WatchUpdateQueue
}

// NewWatchDistributor returns a new instance of
// the WatchDistrubtor interface
func NewWatchDistributor() WatchDistributor {
	return &distributor{}
}

func (d *distributor) Add() WatchUpdateQueue {
	d.Lock()
	defer d.Unlock()
	q := NewWatchUpdateQueue()
	for _, u := range d.updates {
		q.Enqueue(u)
	}
	d.watchers = append(d.watchers, q)
	return q
}

func (d *distributor) Remove(r WatchUpdateQueue) {
	d.Lock()
	defer d.Unlock()
	for i, q := range d.watchers {
		if q == r {
			copy(d.watchers[i:], d.watchers[i+1:])
			d.watchers[len(d.watchers)-1] = nil
			d.watchers = d.watchers[:len(d.watchers)-1]
		}
	}
}

func (d *distributor) NewUpdate(u *watchUpdate) {
	d.Lock()
	defer d.Unlock()
	// collect update
	d.updates = append(d.updates, u)
	if len(d.updates) > 100 {
		d.updates = d.updates[100:]
	}
	// send update to watchers
	for _, q := range d.watchers {
		q.Enqueue(u)
	}
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

// NewWatchUpdateQueue returns an instance of WatchUpdateQueue
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

	logrus.Infof("Initializing a new internal KVDB client with domain %v and pairs %v",
		domain,
		machines,
	)

	handle, err := bolt.Open(
		dbPath,
		0777,
		nil,
		// &bolt.Options{Timeout: 1 * time.Second},
	)
	if err != nil {
		logrus.Fatalf("Could not open internal KVDB: %v", err)
		return nil, err
	}

	tx, err := handle.Begin(true)
	if err != nil {
		logrus.Fatalf("Could not open KVDB transaction: %v", err)
		return nil, err
	}
	defer tx.Rollback()

	if _, err = tx.CreateBucketIfNotExists(pxBucket); err != nil {
		logrus.Fatalf("Could not create default KVDB bucket: %v", err)
		return nil, err
	}

	if err = tx.Commit(); err != nil {
		logrus.Fatalf("Could not commit default KVDB bucket: %v", err)
		return nil, err
	}

	inkv := &inKV{
		BaseKvdb:   common.BaseKvdb{FatalCb: fatalErrorCb},
		db:         handle,
		dist:       NewWatchDistributor(),
		domain:     domain,
		Controller: kvdb.ControllerNotSupported,
		locks:      make(map[string]chan int),
	}

	return inkv, nil
}

// Version returns the supported version of the mem implementation
func Version(url string, kvdbOptions map[string]string) (string, error) {
	return kvdb.BoltVersion1, nil
}

func (kv *inKV) String() string {
	return Name
}

func (kv *inKV) Capabilities() int {
	return kvdb.KVCapabilityOrderedUpdates
}

func (kv *inKV) get(key string) (*kvdb.KVPair, error) {
	if kv.db == nil {
		return nil, kvdb.ErrNotFound
	}
	key = kv.domain + key

	tx, err := kv.db.Begin(false)
	if err != nil {
		logrus.Fatalf("Could not open KVDB transaction in GET: %v", err)
		return nil, err
	}
	defer tx.Rollback()

	bucket := tx.Bucket(pxBucket)
	val := bucket.Get([]byte(key))
	if val == nil {
		return nil, kvdb.ErrNotFound
	}

	var kvp *kvdb.KVPair
	err = json.Unmarshal(val, &kvp)
	if err != nil {
		logrus.Warnf("Requested key could not be parsed from KVDB: %v, %v (%v)",
			key,
			val,
			err,
		)
		return nil, err
	}
	return kvp, nil
}

func (kv *inKV) put(
	key string,
	value interface{},
	ttl uint64,
) (*kvdb.KVPair, error) {
	var (
		kvp *kvdb.KVPair
		b   []byte
		err error
	)
	suffix := key
	key = kv.domain + key

	tx, err := kv.db.Begin(true)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	bucket := tx.Bucket(pxBucket)
	if bucket == nil {
		logrus.Warnf("Requested bucket not found in internal KVDB: %v (%v)",
			pxBucket,
			err,
		)
		return nil, kvdb.ErrNotFound
	}

	index := atomic.AddUint64(&kv.index, 1)

	b, err = common.ToBytes(value)
	if err != nil {
		return nil, err
	}

	kvp = &kvdb.KVPair{
		Key:           key,
		Value:         b,
		TTL:           int64(ttl),
		KVDBIndex:     index,
		ModifiedIndex: index,
		CreatedIndex:  index,
		Action:        kvdb.KVCreate,
	}

	kv.normalize(kvp)

	enc, err := json.Marshal(kvp)
	if err != nil {
		logrus.Warnf("Requested KVP cannot be marshalled into internal KVDB: %v (%v)",
			kvp,
			err,
		)
		return nil, err
	}

	if err = bucket.Put([]byte(key), enc); err != nil {
		logrus.Warnf("Requested KVP could not be inserted into internal KVDB: %v (%v)",
			kvp,
			err,
		)
		return nil, err
	}

	if err = tx.Commit(); err != nil {
		logrus.Fatalf("Could not commit put transaction in KVDB bucket for %v (%v): %v",
			key,
			enc,
			err,
		)
		return nil, err
	}

	kv.dist.NewUpdate(&watchUpdate{key, *kvp, nil})

	if ttl != 0 {
		time.AfterFunc(time.Second*time.Duration(ttl), func() {
			// TODO: handle error
			kv.mutex.Lock()
			defer kv.mutex.Unlock()
			_, _ = kv.delete(suffix)
		})
	}

	/*
		logrus.Warnf("PUT OP 0 %v %v %v", key, reflect.TypeOf(value), value)
		logrus.Warnf("PUT OP 1 on %v %v %v", key, string(enc), ttl)
		logrus.Warnf("PUT OP 2 RAW BYTES OK on %v %v", key, b)
		logrus.Warnf("PUT OP 3 RAW BYTES OK on %v %v", key, kvp)
	*/

	return kvp, nil
}

// enumerate returns a list of values and creates a copy if specified
func (kv *inKV) enumerate(prefix string) (kvdb.KVPairs, error) {
	var kvps = make(kvdb.KVPairs, 0, 100)
	prefix = kv.domain + prefix

	kv.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(pxBucket)
		if bucket == nil {
			logrus.Warnf("Requested bucket not found in internal KVDB: %v",
				pxBucket,
			)
			return kvdb.ErrNotFound
		}

		c := bucket.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			if strings.HasPrefix(string(k), prefix) && !strings.Contains(string(k), "/_") {
				var kvp *kvdb.KVPair
				if err := json.Unmarshal(v, &kvp); err != nil {
					logrus.Warnf("Enumerated prefix could not be parsed from KVDB: %v, %v (%v)",
						prefix,
						v,
						err,
					)
					logrus.Fatalf("Could not enumerate internal KVDB: %v: %v",
						v,
						err,
					)
					return err
				}
				kv.normalize(kvp)
				kvps = append(kvps, kvp)
			}
		}
		return nil
	})

	return kvps, nil
}

func (kv *inKV) delete(key string) (*kvdb.KVPair, error) {
	kvp, err := kv.get(key)
	if err != nil {
		return nil, err
	}
	kvp.KVDBIndex = atomic.AddUint64(&kv.index, 1)
	kvp.ModifiedIndex = kvp.KVDBIndex
	kvp.Action = kvdb.KVDelete

	tx, err := kv.db.Begin(true)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	bucket := tx.Bucket(pxBucket)
	if bucket == nil {
		logrus.Warnf("Requested bucket for delete not found in internal KVDB: %v (%v)",
			pxBucket,
			err,
		)
		return nil, kvdb.ErrNotFound
	}

	if err = bucket.Delete([]byte(kv.domain + key)); err != nil {
		logrus.Warnf("Requested KVP for delete could not be deleted from internal KVDB: %v (%v)",
			kvp,
			err,
		)
		return nil, err
	}

	if err = tx.Commit(); err != nil {
		logrus.Fatalf("Could not commit delete transaction in KVDB bucket for %v: %v",
			key,
			err,
		)
		return nil, err
	}

	kv.dist.NewUpdate(&watchUpdate{kv.domain + key, *kvp, nil})
	return kvp, nil
}

func (kv *inKV) exists(key string) (*kvdb.KVPair, error) {
	return kv.get(key)
}

func (kv *inKV) Get(key string) (*kvdb.KVPair, error) {
	kv.mutex.Lock()
	defer kv.mutex.Unlock()
	v, err := kv.get(key)
	if err != nil {
		return nil, err
	}
	return v, nil
}

func (kv *inKV) Snapshot(prefix string) (kvdb.Kvdb, uint64, error) {
	// XXX FIXME - see etcdv2 implementation
	return nil, 0, kvdb.ErrNotSupported
}

func (kv *inKV) Put(
	key string,
	value interface{},
	ttl uint64,
) (*kvdb.KVPair, error) {
	kv.mutex.Lock()
	defer kv.mutex.Unlock()
	return kv.put(key, value, ttl)
}

func (kv *inKV) GetVal(key string, v interface{}) (*kvdb.KVPair, error) {
	kv.mutex.Lock()
	defer kv.mutex.Unlock()
	kvp, err := kv.get(key)
	if err != nil {
		return nil, err
	}

	err = json.Unmarshal(kvp.Value, v)
	return kvp, err
}

func (kv *inKV) Create(
	key string,
	value interface{},
	ttl uint64,
) (*kvdb.KVPair, error) {
	kv.mutex.Lock()
	defer kv.mutex.Unlock()

	result, err := kv.exists(key)
	if err != nil {
		return kv.put(key, value, ttl)
	}
	return result, kvdb.ErrExist
}

func (kv *inKV) Update(
	key string,
	value interface{},
	ttl uint64,
) (*kvdb.KVPair, error) {
	kv.mutex.Lock()
	defer kv.mutex.Unlock()

	if _, err := kv.exists(key); err != nil {
		return nil, kvdb.ErrNotFound
	}
	return kv.put(key, value, ttl)
}

func (kv *inKV) Enumerate(prefix string) (kvdb.KVPairs, error) {
	kv.mutex.Lock()
	defer kv.mutex.Unlock()
	return kv.enumerate(prefix)
}

func (kv *inKV) Delete(key string) (*kvdb.KVPair, error) {
	kv.mutex.Lock()
	defer kv.mutex.Unlock()

	return kv.delete(key)
}

func (kv *inKV) DeleteTree(prefix string) error {
	kv.mutex.Lock()
	defer kv.mutex.Unlock()

	if len(prefix) > 0 && !strings.HasSuffix(prefix, kvdb.DefaultSeparator) {
		prefix += kvdb.DefaultSeparator
	}

	kvp, err := kv.enumerate(prefix)
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

func (kv *inKV) Keys(prefix, sep string) ([]string, error) {
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
	kv.mutex.Lock()
	defer kv.mutex.Unlock()

	// XXX
	/*
		for k := range kv.m {
			if strings.HasPrefix(k, prefix) && !strings.Contains(k, "/_") {
				key := k[lenPrefix:]
				if idx := strings.Index(key, sep); idx > 0 {
					key = key[:idx]
				}
				seen[key] = true
			}
		}
	*/
	retList := make([]string, len(seen))
	i := 0
	for k := range seen {
		retList[i] = k
		i++
	}

	return retList, nil
}

func (kv *inKV) CompareAndSet(
	kvp *kvdb.KVPair,
	flags kvdb.KVFlags,
	prevValue []byte,
) (*kvdb.KVPair, error) {
	kv.mutex.Lock()
	defer kv.mutex.Unlock()

	// XXX FIXME some bug above this cases the prefix to be pre-loaded.
	kvp.Key = strings.TrimPrefix(kvp.Key, kv.domain)

	result, err := kv.exists(kvp.Key)
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

func (kv *inKV) CompareAndDelete(
	kvp *kvdb.KVPair,
	flags kvdb.KVFlags,
) (*kvdb.KVPair, error) {
	kv.mutex.Lock()
	defer kv.mutex.Unlock()

	// XXX FIXME some bug above this cases the prefix to be pre-loaded.
	kvp.Key = strings.TrimPrefix(kvp.Key, kv.domain)

	if flags != kvdb.KVFlags(0) {
		return nil, kvdb.ErrNotSupported
	}
	result, err := kv.exists(kvp.Key)
	if err != nil {
		return nil, err
	}
	if !bytes.Equal(result.Value, kvp.Value) {
		return nil, kvdb.ErrNotFound
	}
	return kv.delete(kvp.Key)
}

func (kv *inKV) WatchKey(
	key string,
	waitIndex uint64,
	opaque interface{},
	cb kvdb.WatchCB,
) error {
	kv.mutex.Lock()
	defer kv.mutex.Unlock()
	key = kv.domain + key
	go kv.watchCb(kv.dist.Add(), key,
		&watchData{cb: cb, waitIndex: waitIndex, opaque: opaque},
		false)
	return nil
}

func (kv *inKV) WatchTree(
	prefix string,
	waitIndex uint64,
	opaque interface{},
	cb kvdb.WatchCB,
) error {
	kv.mutex.Lock()
	defer kv.mutex.Unlock()
	prefix = kv.domain + prefix
	go kv.watchCb(kv.dist.Add(), prefix,
		&watchData{cb: cb, waitIndex: waitIndex, opaque: opaque},
		true)
	return nil
}

func (kv *inKV) Lock(key string) (*kvdb.KVPair, error) {
	return kv.LockWithID(key, "locked")
}

func (kv *inKV) LockWithID(
	key string,
	lockerID string,
) (*kvdb.KVPair, error) {
	return kv.LockWithTimeout(key, lockerID, kvdb.DefaultLockTryDuration, kv.GetLockTimeout())
}

func (kv *inKV) LockWithTimeout(
	key string,
	lockerID string,
	lockTryDuration time.Duration,
	lockHoldDuration time.Duration,
) (*kvdb.KVPair, error) {
	key = kv.domain + key
	duration := time.Second

	result, err := kv.Create(key, lockerID, uint64(duration*3))
	startTime := time.Now()
	for count := 0; err != nil; count++ {
		time.Sleep(duration)
		result, err = kv.Create(key, lockerID, uint64(duration*3))
		if err != nil && count > 0 && count%15 == 0 {
			var currLockerID string
			if _, errGet := kv.GetVal(key, currLockerID); errGet == nil {
				logrus.Infof("Lock %v locked for %v seconds, tag: %v",
					key, count, currLockerID)
			}
		}
		if err != nil && time.Since(startTime) > lockTryDuration {
			return nil, err
		}
	}

	if err != nil {
		return nil, err
	}

	lockChan := make(chan int)
	kv.mutex.Lock()
	kv.locks[key] = lockChan
	kv.mutex.Unlock()
	if lockHoldDuration > 0 {
		go func() {
			timeout := time.After(lockHoldDuration)
			for {
				select {
				case <-timeout:
					kv.LockTimedout(key)
				case <-lockChan:
					return
				}
			}
		}()
	}

	return result, err
}

func (kv *inKV) Unlock(kvp *kvdb.KVPair) error {
	kv.mutex.Lock()
	lockChan, ok := kv.locks[kvp.Key]
	if ok {
		delete(kv.locks, kvp.Key)
	}
	kv.mutex.Unlock()
	if lockChan != nil {
		close(lockChan)
	}
	_, err := kv.CompareAndDelete(kvp, kvdb.KVFlags(0))
	return err
}

func (kv *inKV) EnumerateWithSelect(
	prefix string,
	enumerateSelect kvdb.EnumerateSelect,
	copySelect kvdb.CopySelect,
) ([]interface{}, error) {
	return nil, kvdb.ErrNotSupported
}

func (kv *inKV) GetWithCopy(
	key string,
	copySelect kvdb.CopySelect,
) (interface{}, error) {
	return nil, kvdb.ErrNotSupported
}

func (kv *inKV) TxNew() (kvdb.Tx, error) {
	return nil, kvdb.ErrNotSupported
}

func (kv *inKV) normalize(kvp *kvdb.KVPair) {
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

func (kv *inKV) watchCb(
	q WatchUpdateQueue,
	prefix string,
	v *watchData,
	treeWatch bool,
) {
	for {
		update := q.Dequeue()
		if ((treeWatch && strings.HasPrefix(update.key, prefix)) ||
			(!treeWatch && update.key == prefix)) &&
			(v.waitIndex == 0 || v.waitIndex < update.kvp.ModifiedIndex) {
			err := v.cb(update.key, v.opaque, &update.kvp, update.err)
			if err != nil {
				_ = v.cb("", v.opaque, nil, kvdb.ErrWatchStopped)
				kv.dist.Remove(q)
				return
			}
		}
	}
}

func (kv *inKV) SnapPut(snapKvp *kvdb.KVPair) (*kvdb.KVPair, error) {
	return nil, kvdb.ErrNotSupported
}

func (kv *inKV) AddUser(username string, password string) error {
	return kvdb.ErrNotSupported
}

func (kv *inKV) RemoveUser(username string) error {
	return kvdb.ErrNotSupported
}

func (kv *inKV) GrantUserAccess(
	username string,
	permType kvdb.PermissionType,
	subtree string,
) error {
	return kvdb.ErrNotSupported
}

func (kv *inKV) RevokeUsersAccess(
	username string,
	permType kvdb.PermissionType,
	subtree string,
) error {
	return kvdb.ErrNotSupported
}

func (kv *inKV) Serialize() ([]byte, error) {

	kvps, err := kv.Enumerate("")
	if err != nil {
		return nil, err
	}
	return kv.SerializeAll(kvps)
}

func (kv *inKV) Deserialize(b []byte) (kvdb.KVPairs, error) {
	return kv.DeserializeAll(b)
}

// MemberList based Bolt implementation
var (
	mtx        sync.RWMutex
	items      = map[string]string{}
	broadcasts *memberlist.TransmitLimitedQueue
)

type broadcast struct {
	msg    []byte
	notify chan<- struct{}
}

type delegate struct{}

type update struct {
	Action string // add, del
	Data   map[string]string
}

func (b *broadcast) Invalidates(other memberlist.Broadcast) bool {
	return false
}

func (b *broadcast) Message() []byte {
	return b.msg
}

func (b *broadcast) Finished() {
	if b.notify != nil {
		close(b.notify)
	}
}

func (d *delegate) NodeMeta(limit int) []byte {
	return []byte{}
}

func (d *delegate) NotifyMsg(b []byte) {
	if len(b) == 0 {
		return
	}

	switch b[0] {
	case 'd': // data
		var updates []*update
		if err := json.Unmarshal(b[1:], &updates); err != nil {
			return
		}
		mtx.Lock()
		for _, u := range updates {
			for k, v := range u.Data {
				switch u.Action {
				case "add":
					items[k] = v
				case "del":
					delete(items, k)
				}
			}
		}
		mtx.Unlock()
	}
}

func (d *delegate) GetBroadcasts(overhead, limit int) [][]byte {
	return broadcasts.GetBroadcasts(overhead, limit)
}

func (d *delegate) LocalState(join bool) []byte {
	mtx.RLock()
	m := items
	mtx.RUnlock()
	b, _ := json.Marshal(m)
	return b
}

func (d *delegate) MergeRemoteState(buf []byte, join bool) {
	if len(buf) == 0 {
		return
	}
	if !join {
		return
	}
	var m map[string]string
	if err := json.Unmarshal(buf, &m); err != nil {
		return
	}
	mtx.Lock()
	for k, v := range m {
		items[k] = v
	}
	mtx.Unlock()
}

func mlPut(key string, val string) error {
	mtx.Lock()
	defer mtx.Unlock()

	b, err := json.Marshal([]*update{
		{
			Action: "add",
			Data: map[string]string{
				key: val,
			},
		},
	})

	if err != nil {
		return err
	}

	items[key] = val

	broadcasts.QueueBroadcast(&broadcast{
		msg:    append([]byte("d"), b...),
		notify: nil,
	})

	return nil
}

func mlDel(key string) error {
	mtx.Lock()
	defer mtx.Unlock()

	b, err := json.Marshal([]*update{
		{
			Action: "del",
			Data: map[string]string{
				key: "",
			},
		},
	})

	if err != nil {
		return err
	}

	delete(items, key)

	broadcasts.QueueBroadcast(&broadcast{
		msg:    append([]byte("d"), b...),
		notify: nil,
	})

	return nil
}

func mlGet(key string) (error, []byte) {
	mtx.Lock()
	defer mtx.Unlock()

	val := items[key]
	return nil, []byte(val)
}

func mlStart() error {
	hostname, _ := os.Hostname()
	c := memberlist.DefaultLocalConfig()
	c.Delegate = &delegate{}
	c.BindPort = 0
	c.Name = hostname + "-" + "UUIDXXX"
	m, err := memberlist.Create(c)
	if err != nil {
		return err
	}

	// XXX TODO
	members := []string{"127.0.0.1"}

	if len(members) > 0 {
		if members, err := m.Join(members); err != nil {
			return err
		} else {
			logrus.Infof("Internal KVDB joining members: %v", members)
		}
	}

	broadcasts = &memberlist.TransmitLimitedQueue{
		NumNodes: func() int {
			return m.NumMembers()
		},
		RetransmitMult: 3,
	}
	node := m.LocalNode()
	fmt.Printf("Local member %s:%d\n", node.Addr, node.Port)
	return nil
}
