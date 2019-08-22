package wrappers

import (
	"sync/atomic"
	"time"

	"github.com/portworx/kvdb"
	"github.com/sirupsen/logrus"
)

type kvRetry struct {
	// kv is the kvdb which is wrapped
	kv kvdb.Kvdb
	// quorumState is kvdb quorum state
	quorumState uint32
}

// New constructs a new kvdb.Kvdb.
func New(
	kv kvdb.Kvdb,
	domain string,
	machines []string,
	options map[string]string,
	fatalErrorCb kvdb.FatalErrorCB,
) (kvdb.Kvdb, error) {
	logrus.Infof("Registering with retry wrapper")
	return &kvRetry{
		kv:          kv,
		quorumState: uint32(kvdb.KvdbInQuorum),
	}, nil
}

func (k *kvRetry) SetQuorumState(state kvdb.KvdbQuorumState) {
	atomic.StoreUint32(&k.quorumState, uint32(state))
}

func (k *kvRetry) QuorumState() kvdb.KvdbQuorumState {
	return kvdb.KvdbQuorumState(atomic.LoadUint32(&k.quorumState))
}

func (k *kvRetry) inQuorum() bool {
	return k.QuorumState() == kvdb.KvdbInQuorum
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
	if k.inQuorum() {
		return k.kv.Get(key)
	} else {
		return nil, kvdb.ErrNoQuorum
	}
}

func (k *kvRetry) Snapshot(prefixes []string, consistent bool) (kvdb.Kvdb, uint64, error) {
	if k.inQuorum() {
		return k.kv.Snapshot(prefixes, consistent)
	} else {
		return nil, 0, kvdb.ErrNoQuorum
	}
}

func (k *kvRetry) Put(
	key string,
	value interface{},
	ttl uint64,
) (*kvdb.KVPair, error) {
	if k.inQuorum() {
		return k.kv.Put(key, value, ttl)
	} else {
		return nil, kvdb.ErrNoQuorum
	}
}

func (k *kvRetry) GetVal(key string, v interface{}) (*kvdb.KVPair, error) {
	if k.inQuorum() {
		return k.kv.GetVal(key, v)
	} else {
		return nil, kvdb.ErrNoQuorum
	}
}

func (k *kvRetry) Create(
	key string,
	value interface{},
	ttl uint64,
) (*kvdb.KVPair, error) {
	if k.inQuorum() {
		return k.kv.Create(key, value, ttl)
	} else {
		return nil, kvdb.ErrNoQuorum
	}
}

func (k *kvRetry) Update(
	key string,
	value interface{},
	ttl uint64,
) (*kvdb.KVPair, error) {
	if k.inQuorum() {
		return k.kv.Update(key, value, ttl)
	} else {
		return nil, kvdb.ErrNoQuorum
	}
}

func (k *kvRetry) Enumerate(prefix string) (kvdb.KVPairs, error) {
	if k.inQuorum() {
		return k.kv.Enumerate(prefix)
	} else {
		return nil, kvdb.ErrNoQuorum
	}
}

func (k *kvRetry) Delete(key string) (*kvdb.KVPair, error) {
	if k.inQuorum() {
		return k.kv.Delete(key)
	} else {
		return nil, kvdb.ErrNoQuorum
	}
}

func (k *kvRetry) DeleteTree(prefix string) error {
	if k.inQuorum() {
		return k.kv.DeleteTree(prefix)
	} else {
		return kvdb.ErrNoQuorum
	}
}

func (k *kvRetry) Keys(prefix, sep string) ([]string, error) {
	if k.inQuorum() {
		return k.kv.Keys(prefix, sep)
	} else {
		return nil, kvdb.ErrNoQuorum
	}
}

func (k *kvRetry) CompareAndSet(
	kvp *kvdb.KVPair,
	flags kvdb.KVFlags,
	prevValue []byte,
) (*kvdb.KVPair, error) {
	if k.inQuorum() {
		return k.kv.CompareAndSet(kvp, flags, prevValue)
	} else {
		return nil, kvdb.ErrNoQuorum
	}
}

func (k *kvRetry) CompareAndDelete(
	kvp *kvdb.KVPair,
	flags kvdb.KVFlags,
) (*kvdb.KVPair, error) {
	if k.inQuorum() {
		return k.kv.CompareAndDelete(kvp, flags)
	} else {
		return nil, kvdb.ErrNoQuorum
	}
}

func (k *kvRetry) WatchKey(
	key string,
	waitIndex uint64,
	opaque interface{},
	cb kvdb.WatchCB,
) error {
	if k.inQuorum() {
		return k.kv.WatchKey(key, waitIndex, opaque, cb)
	} else {
		return kvdb.ErrNoQuorum
	}
}

func (k *kvRetry) WatchTree(
	prefix string,
	waitIndex uint64,
	opaque interface{},
	cb kvdb.WatchCB,
) error {
	if k.inQuorum() {
		return k.kv.WatchTree(prefix, waitIndex, opaque, cb)
	} else {
		return kvdb.ErrNoQuorum
	}
}

func (k *kvRetry) Lock(key string) (*kvdb.KVPair, error) {
	if k.inQuorum() {
		return k.kv.Lock(key)
	} else {
		return nil, kvdb.ErrNoQuorum
	}
}

func (k *kvRetry) LockWithID(
	key string,
	lockerID string,
) (*kvdb.KVPair, error) {
	if k.inQuorum() {
		return k.kv.LockWithID(key, lockerID)
	} else {
		return nil, kvdb.ErrNoQuorum
	}
}

func (k *kvRetry) LockWithTimeout(
	key string,
	lockerID string,
	lockTryDuration time.Duration,
	lockHoldDuration time.Duration,
) (*kvdb.KVPair, error) {
	if k.inQuorum() {
		return k.kv.LockWithTimeout(key, lockerID, lockTryDuration, lockHoldDuration)
	} else {
		return nil, kvdb.ErrNoQuorum
	}
}

func (k *kvRetry) Unlock(kvp *kvdb.KVPair) error {
	if k.inQuorum() {
		return k.kv.Unlock(kvp)
	} else {
		return kvdb.ErrNoQuorum
	}
}

func (k *kvRetry) EnumerateWithSelect(
	prefix string,
	enumerateSelect kvdb.EnumerateSelect,
	copySelect kvdb.CopySelect,
) ([]interface{}, error) {
	if k.inQuorum() {
		return k.kv.EnumerateWithSelect(prefix, enumerateSelect, copySelect)
	} else {
		return nil, kvdb.ErrNoQuorum
	}
}

func (k *kvRetry) GetWithCopy(
	key string,
	copySelect kvdb.CopySelect,
) (interface{}, error) {
	if k.inQuorum() {
		return k.kv.GetWithCopy(key, copySelect)
	} else {
		return nil, kvdb.ErrNoQuorum
	}
}

func (k *kvRetry) TxNew() (kvdb.Tx, error) {
	if k.inQuorum() {
		return k.kv.TxNew()
	} else {
		return nil, kvdb.ErrNoQuorum
	}
}

func (k *kvRetry) SnapPut(snapKvp *kvdb.KVPair) (*kvdb.KVPair, error) {
	if k.inQuorum() {
		return k.kv.SnapPut(snapKvp)
	} else {
		return nil, kvdb.ErrNoQuorum
	}
}

func (k *kvRetry) AddUser(username string, password string) error {
	if k.inQuorum() {
		return k.kv.AddUser(username, password)
	} else {
		return kvdb.ErrNoQuorum
	}
}

func (k *kvRetry) RemoveUser(username string) error {
	if k.inQuorum() {
		return k.kv.RemoveUser(username)
	} else {
		return kvdb.ErrNoQuorum
	}
}

func (k *kvRetry) GrantUserAccess(
	username string,
	permType kvdb.PermissionType,
	subtree string,
) error {
	if k.inQuorum() {
		return k.kv.GrantUserAccess(username, permType, subtree)
	} else {
		return kvdb.ErrNoQuorum
	}
}

func (k *kvRetry) RevokeUsersAccess(
	username string,
	permType kvdb.PermissionType,
	subtree string,
) error {
	if k.inQuorum() {
		return k.kv.RevokeUsersAccess(username, permType, subtree)
	} else {
		return kvdb.ErrNoQuorum
	}
}

func (k *kvRetry) SetFatalCb(f kvdb.FatalErrorCB) {
	k.kv.SetFatalCb(f)
}

func (k *kvRetry) SetLockTimeout(timeout time.Duration) {
	k.kv.SetLockTimeout(timeout)
}

func (k *kvRetry) GetLockTimeout() time.Duration {
	return k.kv.GetLockTimeout()
}

func (k *kvRetry) Serialize() ([]byte, error) {
	if k.inQuorum() {
		return k.kv.Serialize()
	} else {
		return nil, kvdb.ErrNoQuorum
	}
}

func (k *kvRetry) Deserialize(b []byte) (kvdb.KVPairs, error) {
	if k.inQuorum() {
		return k.kv.Deserialize(b)
	} else {
		return nil, kvdb.ErrNoQuorum
	}
}

func (k *kvRetry) AddMember(nodeIP, nodePeerPort, nodeName string) (map[string][]string, error) {
	if k.inQuorum() {
		return k.kv.AddMember(nodeIP, nodePeerPort, nodeName)
	} else {
		return nil, kvdb.ErrNoQuorum
	}
}

func (k *kvRetry) RemoveMember(nodeName, nodeIP string) error {
	if k.inQuorum() {
		return k.kv.RemoveMember(nodeName, nodeIP)
	} else {
		return kvdb.ErrNoQuorum
	}
}

func (k *kvRetry) UpdateMember(nodeIP, nodePeerPort, nodeName string) (map[string][]string, error) {
	return k.kv.UpdateMember(nodeIP, nodePeerPort, nodeName)
}

func (k *kvRetry) ListMembers() (map[string]*kvdb.MemberInfo, error) {
	return k.kv.ListMembers()
}

func (k *kvRetry) SetEndpoints(endpoints []string) error {
	if k.inQuorum() {
		return k.kv.SetEndpoints(endpoints)
	} else {
		return kvdb.ErrNoQuorum
	}
}

func (k *kvRetry) GetEndpoints() []string {
	return k.kv.GetEndpoints()
}

func (k *kvRetry) Defragment(endpoint string, timeout int) error {
	if k.inQuorum() {
		return k.kv.Defragment(endpoint, timeout)
	} else {
		return kvdb.ErrNoQuorum
	}
}

func init() {
	if err := kvdb.RegisterWrapper(kvdb.UseRetryWrapper, New); err != nil {
		panic(err.Error())
	}
}
