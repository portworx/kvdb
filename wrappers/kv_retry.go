package mem

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/portworx/kvdb"
	"github.com/portworx/kvdb/common"
	"github.com/sirupsen/logrus"
)

type kvRetry struct {
	// kv is the kvdb which is wrapped
	kv kvdb.Kvdb
	// hasQuorum is true if kv has quorum
	hasQuorum bool
	// mutex protects hasQuorum
	mutex sync.Mutex
}

// New constructs a new kvdb.Kvdb.
func New(
	kv kvdb.Kvdb,
	domain string,
	machines []string,
	options map[string]string,
	fatalErrorCb kvdb.FatalErrorCB,
) (kvdb.Kvdb, error) {
	return &kvRetry{
		kv: kv,
	}
}

// Version returns the supported version of the mem implementation
func Version(url string, kvdbOptions map[string]string) (string, error) {
	return kvdb.MemVersion1, nil
}

func (k *kvRetry) String() string {
	return k.kv.String()
}

func (k *kvRetry) Capabilities() int {
	return k.kv.Capabilities()
}

func (k *kvRetry) Get(key string) (*kvdb.KVPair, error) {
	return k.kv.Get(key)
}

func (k *kvRetry) Snapshot(prefixes []string, consistent bool) (kvdb.Kvdb, uint64, error) {
	return k.kv.Snapshot(prefixes, consistent)
}

func (k *kvRetry) Put(
	key string,
	value interface{},
	ttl uint64,
) (*kvdb.KVPair, error) {
	return k.kv.Put(key, value, ttl)
}

func (k *kvRetry) GetVal(key string, v interface{}) (*kvdb.KVPair, error) {
	return k.kv.GetVal(key, v)
}

func (k *kvRetry) Create(
	key string,
	value interface{},
	ttl uint64,
) (*kvdb.KVPair, error) {
	return k.kv.Create(key, value, ttl)
}

func (k *kvRetry) Update(
	key string,
	value interface{},
	ttl uint64,
) (*kvdb.KVPair, error) {
	return k.kv.Update(key, value, ttl)
}

func (k *kvRetry) Enumerate(prefix string) (kvdb.KVPairs, error) {
	return k.kv.Enumerate(prefix)
}

func (k *kvRetry) Delete(key string) (*kvdb.KVPair, error) {
	return k.kv.Delete(key)
}

func (k *kvRetry) DeleteTree(prefix string) error {
	return k.kv.DeleteTree(prefix)
}

func (k *kvRetry) Keys(prefix, sep string) ([]string, error) {
	return k.kv.Keys(prefix, sep)
}

func (k *kvRetry) CompareAndSet(
	kvp *kvdb.KVPair,
	flags kvdb.KVFlags,
	prevValue []byte,
) (*kvdb.KVPair, error) {
	return k.kv.CompareAndSet(kvp, flags, prevValue)
}

func (k *kvRetry) CompareAndDelete(
	kvp *kvdb.KVPair,
	flags kvdb.KVFlags,
) (*kvdb.KVPair, error) {
	return k.kv.CompareAndDelete(kvp, flags)
}

func (k *kvRetry) WatchKey(
	key string,
	waitIndex uint64,
	opaque interface{},
	cb kvdb.WatchCB,
) error {
	return k.kv.WatchKey(key, waitIndex, opaque, cb)
}

func (k *kvRetry) WatchTree(
	prefix string,
	waitIndex uint64,
	opaque interface{},
	cb kvdb.WatchCB,
) error {
	return k.kv.WatchTree(prefix, waitIndex, opaque, cb)
}

func (k *kvRetry) Lock(key string) (*kvdb.KVPair, error) {
	return k.kv.Lock(key)
}

func (k *kvRetry) LockWithID(
	key string,
	lockerID string,
) (*kvdb.KVPair, error) {
	return k.kv.LockWithID(key, lockerID)
}

func (k *kvRetry) LockWithTimeout(
	key string,
	lockerID string,
	lockTryDuration time.Duration,
	lockHoldDuration time.Duration,
) (*kvdb.KVPair, error) {
	return k.kv.LockWithTimeout(key, lockerID, lockTryDuration, lockHoldDuration)
}

func (k *kvRetry) Unlock(kvp *kvdb.KVPair) error {
	return k.kv.Unlock(kvp)
}

func (k *kvRetry) EnumerateWithSelect(
	prefix string,
	enumerateSelect kvdb.EnumerateSelect,
	copySelect kvdb.CopySelect,
) ([]interface{}, error) {
	return k.kv.EnumerateWithSelect(prefix, enumerateSelect, copySelect)
}

func (k *kvRetry) GetWithCopy(
	key string,
	copySelect kvdb.CopySelect,
) (interface{}, error) {
	return k.kv.GetWithCopy(key, copySelect)
}

func (k *kvRetry) TxNew() (kvdb.Tx, error) {
	return k.kv.TxNew()
}

func (k *kvRetry) SnapPut(snapKvp *kvdb.KVPair) (*kvdb.KVPair, error) {
	return k.kv.SnapPut(snapKvp)
}

func (k *kvRetry) AddUser(username string, password string) error {
	return k.kv.AddUser(username, password)
}

func (k *kvRetry) RemoveUser(username string) error {
	return k.kv.RemoveUser(username)
}

func (k *kvRetry) GrantUserAccess(
	username string,
	permType kvdb.PermissionType,
	subtree string,
) error {
	return k.kv.GrantUserAccess(username, permType, subtree)
}

func (k *kvRetry) RevokeUsersAccess(
	username string,
	permType kvdb.PermissionType,
	subtree string,
) error {
	return k.kv.RevokeUsersAccess(username, permType, subtree)
}

func (k *kvRetry) Serialize() ([]byte, error) {
	return k.kv.Serialize()
}

func (k *kvRetry) Deserialize(b []byte) (kvdb.KVPairs, error) {
	return k.kv.DeserializeAll(b)
}
