package wrappers

import (
	"sync/atomic"
	"time"

	"github.com/portworx/kvdb"
	"github.com/sirupsen/logrus"
)

type kvQuorumCheckWrapper struct {
	// kv is the kvdb which is wrapped
	kv kvdb.Kvdb
	// quorumState is kvdb quorum state
	quorumState uint32
}

// NewKvQuorumCheckWrapper constructs a new kvdb.Kvdb.
func NewKvQuorumCheckWrapper(
	kv kvdb.Kvdb,
	domain string,
	machines []string,
	options map[string]string,
	fatalErrorCb kvdb.FatalErrorCB,
) (kvdb.Kvdb, error) {
	logrus.Infof("Registering with retry wrapper")
	return &kvQuorumCheckWrapper{
		kv:          kv,
		quorumState: uint32(kvdb.KvdbInQuorum),
	}, nil
}

func (k *kvQuorumCheckWrapper) SetQuorumState(state kvdb.KvdbQuorumState) {
	atomic.StoreUint32(&k.quorumState, uint32(state))
}

func (k *kvQuorumCheckWrapper) QuorumState() kvdb.KvdbQuorumState {
	return kvdb.KvdbQuorumState(atomic.LoadUint32(&k.quorumState))
}

func (k *kvQuorumCheckWrapper) inQuorum() bool {
	return k.QuorumState() == kvdb.KvdbInQuorum
}

// Version returns the supported version of the mem implementation
func Version(url string, kvdbOptions map[string]string) (string, error) {
	return kvdb.MemVersion1, nil
}

func (k *kvQuorumCheckWrapper) String() string {
	return k.kv.String()
}

func (k *kvQuorumCheckWrapper) Capabilities() int {
	return k.kv.Capabilities()
}

func (k *kvQuorumCheckWrapper) Get(key string) (*kvdb.KVPair, error) {
	if k.inQuorum() {
		return k.kv.Get(key)
	} else {
		return nil, kvdb.ErrNoQuorum
	}
}

func (k *kvQuorumCheckWrapper) Snapshot(prefixes []string, consistent bool) (kvdb.Kvdb, uint64, error) {
	if k.inQuorum() {
		return k.kv.Snapshot(prefixes, consistent)
	} else {
		return nil, 0, kvdb.ErrNoQuorum
	}
}

func (k *kvQuorumCheckWrapper) Put(
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

func (k *kvQuorumCheckWrapper) GetVal(key string, v interface{}) (*kvdb.KVPair, error) {
	if k.inQuorum() {
		return k.kv.GetVal(key, v)
	} else {
		return nil, kvdb.ErrNoQuorum
	}
}

func (k *kvQuorumCheckWrapper) Create(
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

func (k *kvQuorumCheckWrapper) Update(
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

func (k *kvQuorumCheckWrapper) Enumerate(prefix string) (kvdb.KVPairs, error) {
	if k.inQuorum() {
		return k.kv.Enumerate(prefix)
	} else {
		return nil, kvdb.ErrNoQuorum
	}
}

func (k *kvQuorumCheckWrapper) Delete(key string) (*kvdb.KVPair, error) {
	if k.inQuorum() {
		return k.kv.Delete(key)
	} else {
		return nil, kvdb.ErrNoQuorum
	}
}

func (k *kvQuorumCheckWrapper) DeleteTree(prefix string) error {
	if k.inQuorum() {
		return k.kv.DeleteTree(prefix)
	} else {
		return kvdb.ErrNoQuorum
	}
}

func (k *kvQuorumCheckWrapper) Keys(prefix, sep string) ([]string, error) {
	if k.inQuorum() {
		return k.kv.Keys(prefix, sep)
	} else {
		return nil, kvdb.ErrNoQuorum
	}
}

func (k *kvQuorumCheckWrapper) CompareAndSet(
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

func (k *kvQuorumCheckWrapper) CompareAndDelete(
	kvp *kvdb.KVPair,
	flags kvdb.KVFlags,
) (*kvdb.KVPair, error) {
	if k.inQuorum() {
		return k.kv.CompareAndDelete(kvp, flags)
	} else {
		return nil, kvdb.ErrNoQuorum
	}
}

func (k *kvQuorumCheckWrapper) WatchKey(
	key string,
	waitIndex uint64,
	opaque interface{},
	cb kvdb.WatchCB,
) error {
	return k.kv.WatchKey(key, waitIndex, opaque, cb)
}

func (k *kvQuorumCheckWrapper) WatchTree(
	prefix string,
	waitIndex uint64,
	opaque interface{},
	cb kvdb.WatchCB,
) error {
	return k.kv.WatchTree(prefix, waitIndex, opaque, cb)
}

func (k *kvQuorumCheckWrapper) Lock(key string) (*kvdb.KVPair, error) {
	if k.inQuorum() {
		return k.kv.Lock(key)
	} else {
		return nil, kvdb.ErrNoQuorum
	}
}

func (k *kvQuorumCheckWrapper) LockWithID(
	key string,
	lockerID string,
) (*kvdb.KVPair, error) {
	if k.inQuorum() {
		return k.kv.LockWithID(key, lockerID)
	} else {
		return nil, kvdb.ErrNoQuorum
	}
}

func (k *kvQuorumCheckWrapper) LockWithTimeout(
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

func (k *kvQuorumCheckWrapper) Unlock(kvp *kvdb.KVPair) error {
	if k.inQuorum() {
		return k.kv.Unlock(kvp)
	} else {
		return kvdb.ErrNoQuorum
	}
}

func (k *kvQuorumCheckWrapper) EnumerateWithSelect(
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

func (k *kvQuorumCheckWrapper) GetWithCopy(
	key string,
	copySelect kvdb.CopySelect,
) (interface{}, error) {
	if k.inQuorum() {
		return k.kv.GetWithCopy(key, copySelect)
	} else {
		return nil, kvdb.ErrNoQuorum
	}
}

func (k *kvQuorumCheckWrapper) TxNew() (kvdb.Tx, error) {
	if k.inQuorum() {
		return k.kv.TxNew()
	} else {
		return nil, kvdb.ErrNoQuorum
	}
}

func (k *kvQuorumCheckWrapper) SnapPut(snapKvp *kvdb.KVPair) (*kvdb.KVPair, error) {
	if k.inQuorum() {
		return k.kv.SnapPut(snapKvp)
	} else {
		return nil, kvdb.ErrNoQuorum
	}
}

func (k *kvQuorumCheckWrapper) AddUser(username string, password string) error {
	if k.inQuorum() {
		return k.kv.AddUser(username, password)
	} else {
		return kvdb.ErrNoQuorum
	}
}

func (k *kvQuorumCheckWrapper) RemoveUser(username string) error {
	if k.inQuorum() {
		return k.kv.RemoveUser(username)
	} else {
		return kvdb.ErrNoQuorum
	}
}

func (k *kvQuorumCheckWrapper) GrantUserAccess(
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

func (k *kvQuorumCheckWrapper) RevokeUsersAccess(
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

func (k *kvQuorumCheckWrapper) SetFatalCb(f kvdb.FatalErrorCB) {
	k.kv.SetFatalCb(f)
}

func (k *kvQuorumCheckWrapper) SetLockTimeout(timeout time.Duration) {
	k.kv.SetLockTimeout(timeout)
}

func (k *kvQuorumCheckWrapper) GetLockTimeout() time.Duration {
	return k.kv.GetLockTimeout()
}

func (k *kvQuorumCheckWrapper) Serialize() ([]byte, error) {
	if k.inQuorum() {
		return k.kv.Serialize()
	} else {
		return nil, kvdb.ErrNoQuorum
	}
}

func (k *kvQuorumCheckWrapper) Deserialize(b []byte) (kvdb.KVPairs, error) {
	if k.inQuorum() {
		return k.kv.Deserialize(b)
	} else {
		return nil, kvdb.ErrNoQuorum
	}
}

func (k *kvQuorumCheckWrapper) AddMember(nodeIP, nodePeerPort, nodeName string) (map[string][]string, error) {
	if k.inQuorum() {
		return k.kv.AddMember(nodeIP, nodePeerPort, nodeName)
	} else {
		return nil, kvdb.ErrNoQuorum
	}
}

func (k *kvQuorumCheckWrapper) RemoveMember(nodeName, nodeIP string) error {
	if k.inQuorum() {
		return k.kv.RemoveMember(nodeName, nodeIP)
	} else {
		return kvdb.ErrNoQuorum
	}
}

func (k *kvQuorumCheckWrapper) UpdateMember(nodeIP, nodePeerPort, nodeName string) (map[string][]string, error) {
	return k.kv.UpdateMember(nodeIP, nodePeerPort, nodeName)
}

func (k *kvQuorumCheckWrapper) ListMembers() (map[string]*kvdb.MemberInfo, error) {
	return k.kv.ListMembers()
}

func (k *kvQuorumCheckWrapper) SetEndpoints(endpoints []string) error {
	if k.inQuorum() {
		return k.kv.SetEndpoints(endpoints)
	} else {
		return kvdb.ErrNoQuorum
	}
}

func (k *kvQuorumCheckWrapper) GetEndpoints() []string {
	return k.kv.GetEndpoints()
}

func (k *kvQuorumCheckWrapper) Defragment(endpoint string, timeout int) error {
	if k.inQuorum() {
		return k.kv.Defragment(endpoint, timeout)
	} else {
		return kvdb.ErrNoQuorum
	}
}

func init() {
	if err := kvdb.RegisterWrapper(kvdb.UseRetryWrapperOption, New); err != nil {
		panic(err.Error())
	}
	if err := kvdb.RegisterWrapper(kvdb.UseLogWrapperOption, NewLogWrapper); err != nil {
		panic(err.Error())
	}
}
