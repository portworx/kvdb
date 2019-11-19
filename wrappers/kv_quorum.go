package wrappers

import (
	"sync/atomic"
	"time"

	"github.com/portworx/kvdb"
	"github.com/sirupsen/logrus"
)

type kvQuorumCheckFilter struct {
	kvBaseWrapper
	// quorumState is kvdb quorum state
	quorumState uint32
}

// NewKvQuorumCheckFilter constructs a new kvdb.Kvdb.
func NewKvQuorumCheckFilter(
	kv kvdb.Kvdb,
	options map[string]string,
) (kvdb.Kvdb, error) {
	logrus.Infof("creating quorum check wrapper")
	return &kvQuorumCheckFilter{
		kvBaseWrapper: kvBaseWrapper{
			name:        kvdb.WrapperQuorumFilter,
			wrappedKvdb: kv,
		},
		quorumState: uint32(kvdb.KvdbInQuorum),
	}, nil
}

func (k *kvQuorumCheckFilter) SetQuorumState(state kvdb.KvdbQuorumState) {
	atomic.StoreUint32(&k.quorumState, uint32(state))
}

func (k *kvQuorumCheckFilter) QuorumState() kvdb.KvdbQuorumState {
	return kvdb.KvdbQuorumState(atomic.LoadUint32(&k.quorumState))
}

func (k *kvQuorumCheckFilter) inQuorum() bool {
	return k.QuorumState() == kvdb.KvdbInQuorum
}

// Version returns the supported version of the mem implementation
func Version(url string, kvdbOptions map[string]string) (string, error) {
	return kvdb.MemVersion1, nil
}

func (k *kvQuorumCheckFilter) String() string {
	return k.wrappedKvdb.String()
}

func (k *kvQuorumCheckFilter) Capabilities() int {
	return k.wrappedKvdb.Capabilities()
}

func (k *kvQuorumCheckFilter) Get(key string) (*kvdb.KVPair, error) {
	if k.inQuorum() {
		return k.wrappedKvdb.Get(key)
	} else {
		return nil, kvdb.ErrNoQuorum
	}
}

func (k *kvQuorumCheckFilter) Snapshot(prefixes []string, consistent bool) (kvdb.Kvdb, uint64, error) {
	if k.inQuorum() {
		return k.wrappedKvdb.Snapshot(prefixes, consistent)
	} else {
		return nil, 0, kvdb.ErrNoQuorum
	}
}

func (k *kvQuorumCheckFilter) Put(
	key string,
	value interface{},
	ttl uint64,
) (*kvdb.KVPair, error) {
	if k.inQuorum() {
		return k.wrappedKvdb.Put(key, value, ttl)
	} else {
		return nil, kvdb.ErrNoQuorum
	}
}

func (k *kvQuorumCheckFilter) GetVal(key string, v interface{}) (*kvdb.KVPair, error) {
	if k.inQuorum() {
		return k.wrappedKvdb.GetVal(key, v)
	} else {
		return nil, kvdb.ErrNoQuorum
	}
}

func (k *kvQuorumCheckFilter) Create(
	key string,
	value interface{},
	ttl uint64,
) (*kvdb.KVPair, error) {
	if k.inQuorum() {
		return k.wrappedKvdb.Create(key, value, ttl)
	} else {
		return nil, kvdb.ErrNoQuorum
	}
}

func (k *kvQuorumCheckFilter) Update(
	key string,
	value interface{},
	ttl uint64,
) (*kvdb.KVPair, error) {
	if k.inQuorum() {
		return k.wrappedKvdb.Update(key, value, ttl)
	} else {
		return nil, kvdb.ErrNoQuorum
	}
}

func (k *kvQuorumCheckFilter) Enumerate(prefix string) (kvdb.KVPairs, error) {
	if k.inQuorum() {
		return k.wrappedKvdb.Enumerate(prefix)
	} else {
		return nil, kvdb.ErrNoQuorum
	}
}

func (k *kvQuorumCheckFilter) Delete(key string) (*kvdb.KVPair, error) {
	if k.inQuorum() {
		return k.wrappedKvdb.Delete(key)
	} else {
		return nil, kvdb.ErrNoQuorum
	}
}

func (k *kvQuorumCheckFilter) DeleteTree(prefix string) error {
	if k.inQuorum() {
		return k.wrappedKvdb.DeleteTree(prefix)
	} else {
		return kvdb.ErrNoQuorum
	}
}

func (k *kvQuorumCheckFilter) Keys(prefix, sep string) ([]string, error) {
	if k.inQuorum() {
		return k.wrappedKvdb.Keys(prefix, sep)
	} else {
		return nil, kvdb.ErrNoQuorum
	}
}

func (k *kvQuorumCheckFilter) CompareAndSet(
	kvp *kvdb.KVPair,
	flags kvdb.KVFlags,
	prevValue []byte,
) (*kvdb.KVPair, error) {
	if k.inQuorum() {
		return k.wrappedKvdb.CompareAndSet(kvp, flags, prevValue)
	} else {
		return nil, kvdb.ErrNoQuorum
	}
}

func (k *kvQuorumCheckFilter) CompareAndDelete(
	kvp *kvdb.KVPair,
	flags kvdb.KVFlags,
) (*kvdb.KVPair, error) {
	if k.inQuorum() {
		return k.wrappedKvdb.CompareAndDelete(kvp, flags)
	} else {
		return nil, kvdb.ErrNoQuorum
	}
}

func (k *kvQuorumCheckFilter) WatchKey(
	key string,
	waitIndex uint64,
	opaque interface{},
	cb kvdb.WatchCB,
) error {
	return k.wrappedKvdb.WatchKey(key, waitIndex, opaque, cb)
}

func (k *kvQuorumCheckFilter) WatchTree(
	prefix string,
	waitIndex uint64,
	opaque interface{},
	cb kvdb.WatchCB,
) error {
	return k.wrappedKvdb.WatchTree(prefix, waitIndex, opaque, cb)
}

func (k *kvQuorumCheckFilter) Lock(key string) (*kvdb.KVPair, error) {
	if k.inQuorum() {
		return k.wrappedKvdb.Lock(key)
	} else {
		return nil, kvdb.ErrNoQuorum
	}
}

func (k *kvQuorumCheckFilter) LockWithID(
	key string,
	lockerID string,
) (*kvdb.KVPair, error) {
	if k.inQuorum() {
		return k.wrappedKvdb.LockWithID(key, lockerID)
	} else {
		return nil, kvdb.ErrNoQuorum
	}
}

func (k *kvQuorumCheckFilter) LockWithTimeout(
	key string,
	lockerID string,
	lockTryDuration time.Duration,
	lockHoldDuration time.Duration,
) (*kvdb.KVPair, error) {
	if k.inQuorum() {
		return k.wrappedKvdb.LockWithTimeout(key, lockerID, lockTryDuration, lockHoldDuration)
	} else {
		return nil, kvdb.ErrNoQuorum
	}
}

func (k *kvQuorumCheckFilter) Unlock(kvp *kvdb.KVPair) error {
	return k.wrappedKvdb.Unlock(kvp)
}

func (k *kvQuorumCheckFilter) EnumerateWithSelect(
	prefix string,
	enumerateSelect kvdb.EnumerateSelect,
	copySelect kvdb.CopySelect,
) ([]interface{}, error) {
	if k.inQuorum() {
		return k.wrappedKvdb.EnumerateWithSelect(prefix, enumerateSelect, copySelect)
	} else {
		return nil, kvdb.ErrNoQuorum
	}
}

func (k *kvQuorumCheckFilter) GetWithCopy(
	key string,
	copySelect kvdb.CopySelect,
) (interface{}, error) {
	if k.inQuorum() {
		return k.wrappedKvdb.GetWithCopy(key, copySelect)
	} else {
		return nil, kvdb.ErrNoQuorum
	}
}

func (k *kvQuorumCheckFilter) TxNew() (kvdb.Tx, error) {
	if k.inQuorum() {
		return k.wrappedKvdb.TxNew()
	} else {
		return nil, kvdb.ErrNoQuorum
	}
}

func (k *kvQuorumCheckFilter) SnapPut(snapKvp *kvdb.KVPair) (*kvdb.KVPair, error) {
	if k.inQuorum() {
		return k.wrappedKvdb.SnapPut(snapKvp)
	} else {
		return nil, kvdb.ErrNoQuorum
	}
}

func (k *kvQuorumCheckFilter) AddUser(username string, password string) error {
	if k.inQuorum() {
		return k.wrappedKvdb.AddUser(username, password)
	} else {
		return kvdb.ErrNoQuorum
	}
}

func (k *kvQuorumCheckFilter) RemoveUser(username string) error {
	if k.inQuorum() {
		return k.wrappedKvdb.RemoveUser(username)
	} else {
		return kvdb.ErrNoQuorum
	}
}

func (k *kvQuorumCheckFilter) GrantUserAccess(
	username string,
	permType kvdb.PermissionType,
	subtree string,
) error {
	if k.inQuorum() {
		return k.wrappedKvdb.GrantUserAccess(username, permType, subtree)
	} else {
		return kvdb.ErrNoQuorum
	}
}

func (k *kvQuorumCheckFilter) RevokeUsersAccess(
	username string,
	permType kvdb.PermissionType,
	subtree string,
) error {
	if k.inQuorum() {
		return k.wrappedKvdb.RevokeUsersAccess(username, permType, subtree)
	} else {
		return kvdb.ErrNoQuorum
	}
}

func (k *kvQuorumCheckFilter) SetFatalCb(f kvdb.FatalErrorCB) {
	k.wrappedKvdb.SetFatalCb(f)
}

func (k *kvQuorumCheckFilter) SetLockTimeout(timeout time.Duration) {
	k.wrappedKvdb.SetLockTimeout(timeout)
}

func (k *kvQuorumCheckFilter) GetLockTimeout() time.Duration {
	return k.wrappedKvdb.GetLockTimeout()
}

func (k *kvQuorumCheckFilter) Serialize() ([]byte, error) {
	if k.inQuorum() {
		return k.wrappedKvdb.Serialize()
	} else {
		return nil, kvdb.ErrNoQuorum
	}
}

func (k *kvQuorumCheckFilter) Deserialize(b []byte) (kvdb.KVPairs, error) {
	if k.inQuorum() {
		return k.wrappedKvdb.Deserialize(b)
	} else {
		return nil, kvdb.ErrNoQuorum
	}
}

func (k *kvQuorumCheckFilter) AddMember(nodeIP, nodePeerPort, nodeName string) (map[string][]string, error) {
	if k.inQuorum() {
		return k.wrappedKvdb.AddMember(nodeIP, nodePeerPort, nodeName)
	} else {
		return nil, kvdb.ErrNoQuorum
	}
}

func (k *kvQuorumCheckFilter) RemoveMember(nodeName, nodeIP string) error {
	if k.inQuorum() {
		return k.wrappedKvdb.RemoveMember(nodeName, nodeIP)
	} else {
		return kvdb.ErrNoQuorum
	}
}

func (k *kvQuorumCheckFilter) UpdateMember(nodeIP, nodePeerPort, nodeName string) (map[string][]string, error) {
	return k.wrappedKvdb.UpdateMember(nodeIP, nodePeerPort, nodeName)
}

func (k *kvQuorumCheckFilter) ListMembers() (map[string]*kvdb.MemberInfo, error) {
	return k.wrappedKvdb.ListMembers()
}

func (k *kvQuorumCheckFilter) SetEndpoints(endpoints []string) error {
	if k.inQuorum() {
		return k.wrappedKvdb.SetEndpoints(endpoints)
	} else {
		return kvdb.ErrNoQuorum
	}
}

func (k *kvQuorumCheckFilter) GetEndpoints() []string {
	return k.wrappedKvdb.GetEndpoints()
}

func (k *kvQuorumCheckFilter) Defragment(endpoint string, timeout int) error {
	if k.inQuorum() {
		return k.wrappedKvdb.Defragment(endpoint, timeout)
	} else {
		return kvdb.ErrNoQuorum
	}
}

func init() {
	if err := kvdb.RegisterWrapper(kvdb.WrapperQuorumFilter, NewKvQuorumCheckFilter); err != nil {
		panic(err.Error())
	}
	if err := kvdb.RegisterWrapper(kvdb.WrapperLog, NewLogWrapper); err != nil {
		panic(err.Error())
	}
}
