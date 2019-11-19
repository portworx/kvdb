package wrappers

import (
	"time"

	"github.com/portworx/kvdb"
	"github.com/sirupsen/logrus"
)

type kvBaseWrapper struct {
	kvdb.KvdbWrapperInfo
}

func (b *kvBaseWrapper) WrappedKvdbInfo() *kvdb.KvdbWrapperInfo {
	return &b.KvdbWrapperInfo
}

type kvLogger struct {
	kvBaseWrapper
}

const (
	opType    = "operation"
	errString = "error"
	output    = "output"
)

// New constructs a new kvdb.Kvdb.
func NewLogWrapper(
	kv kvdb.Kvdb,
	options map[string]string,
) (kvdb.Kvdb, error) {
	logrus.Infof("creating kvdb logging wrapper, options: %v", options)
	return &kvLogger{
		kvBaseWrapper{
			kvdb.KvdbWrapperInfo{
				Name:        kvdb.WrapperLog,
				WrappedKvdb: kv,
			}},
	}, nil
}

func (k *kvLogger) inQuorum() bool {
	return k.inQuorum()
}

func (k *kvLogger) SetQuorumState(state kvdb.KvdbQuorumState) {
	k.WrappedKvdb.SetQuorumState(state)
}

func (k *kvLogger) QuorumState() kvdb.KvdbQuorumState {
	return k.WrappedKvdb.QuorumState()
}

func (k *kvLogger) String() string {
	return k.WrappedKvdb.String()
}

func (k *kvLogger) Capabilities() int {
	return k.WrappedKvdb.Capabilities()
}

func (k *kvLogger) Get(key string) (*kvdb.KVPair, error) {
	pair, err := k.WrappedKvdb.Get(key)
	logrus.WithFields(logrus.Fields{
		opType:    "Get",
		output:    pair,
		errString: err,
	}).Info()
	return pair, err
}

func (k *kvLogger) Snapshot(prefixes []string, consistent bool) (kvdb.Kvdb, uint64, error) {
	kv, version, err := k.WrappedKvdb.Snapshot(prefixes, consistent)
	logrus.WithFields(logrus.Fields{
		opType:    "Snapshot",
		errString: err,
	}).Info()
	return kv, version, err
}

func (k *kvLogger) Put(
	key string,
	value interface{},
	ttl uint64,
) (*kvdb.KVPair, error) {
	pair, err := k.WrappedKvdb.Put(key, value, ttl)
	logrus.WithFields(logrus.Fields{
		opType:    "Put",
		output:    pair,
		errString: err,
	}).Info()
	return pair, err
}

func (k *kvLogger) GetVal(key string, v interface{}) (*kvdb.KVPair, error) {
	pair, err := k.WrappedKvdb.GetVal(key, v)
	logrus.WithFields(logrus.Fields{
		opType:    "GetValue",
		output:    pair,
		errString: err,
	}).Info()
	return pair, err
}

func (k *kvLogger) Create(
	key string,
	value interface{},
	ttl uint64,
) (*kvdb.KVPair, error) {
	pair, err := k.WrappedKvdb.Create(key, value, ttl)
	logrus.WithFields(logrus.Fields{
		opType:    "Create",
		output:    pair,
		errString: err,
	}).Info()
	return pair, err
}

func (k *kvLogger) Update(
	key string,
	value interface{},
	ttl uint64,
) (*kvdb.KVPair, error) {
	pair, err := k.WrappedKvdb.Update(key, value, ttl)
	logrus.WithFields(logrus.Fields{
		opType:    "Update",
		output:    pair,
		errString: err,
	}).Info()
	return pair, err
}

func (k *kvLogger) Enumerate(prefix string) (kvdb.KVPairs, error) {
	pairs, err := k.WrappedKvdb.Enumerate(prefix)
	logrus.WithFields(logrus.Fields{
		opType:    "Enumerate",
		"length":  len(pairs),
		errString: err,
	}).Info()
	return pairs, err
}

func (k *kvLogger) Delete(key string) (*kvdb.KVPair, error) {
	pair, err := k.WrappedKvdb.Delete(key)
	logrus.WithFields(logrus.Fields{
		opType:    "Delete",
		output:    pair,
		errString: err,
	}).Info()
	return pair, err
}

func (k *kvLogger) DeleteTree(prefix string) error {
	err := k.WrappedKvdb.DeleteTree(prefix)
	logrus.WithFields(logrus.Fields{
		opType:    "DeleteTree",
		errString: err,
	}).Info()
	return err
}

func (k *kvLogger) Keys(prefix, sep string) ([]string, error) {
	keys, err := k.WrappedKvdb.Keys(prefix, sep)
	logrus.WithFields(logrus.Fields{
		opType:    "Keys",
		"length":  len(keys),
		errString: err,
	}).Info()
	return keys, err
}

func (k *kvLogger) CompareAndSet(
	kvp *kvdb.KVPair,
	flags kvdb.KVFlags,
	prevValue []byte,
) (*kvdb.KVPair, error) {
	pair, err := k.WrappedKvdb.CompareAndSet(kvp, flags, prevValue)
	logrus.WithFields(logrus.Fields{
		opType:    "CompareAndSet",
		output:    pair,
		errString: err,
	}).Info()
	return pair, err
}

func (k *kvLogger) CompareAndDelete(
	kvp *kvdb.KVPair,
	flags kvdb.KVFlags,
) (*kvdb.KVPair, error) {
	pair, err := k.WrappedKvdb.CompareAndDelete(kvp, flags)
	logrus.WithFields(logrus.Fields{
		opType:    "CompareAndDelete",
		output:    pair,
		errString: err,
	}).Info()
	return pair, err
}

func (k *kvLogger) WatchKey(
	key string,
	waitIndex uint64,
	opaque interface{},
	cb kvdb.WatchCB,
) error {
	err := k.WrappedKvdb.WatchKey(key, waitIndex, opaque, cb)
	logrus.WithFields(logrus.Fields{
		opType:    "WatchKey",
		errString: err,
	}).Info()
	return err
}

func (k *kvLogger) WatchTree(
	prefix string,
	waitIndex uint64,
	opaque interface{},
	cb kvdb.WatchCB,
) error {
	err := k.WrappedKvdb.WatchTree(prefix, waitIndex, opaque, cb)
	logrus.WithFields(logrus.Fields{
		opType:    "WatchTree",
		errString: err,
	}).Info()
	return err
}

func (k *kvLogger) Lock(key string) (*kvdb.KVPair, error) {
	pair, err := k.WrappedKvdb.Lock(key)
	logrus.WithFields(logrus.Fields{
		opType:    "Lock",
		output:    pair,
		errString: err,
	}).Info()
	return pair, err
}

func (k *kvLogger) LockWithID(
	key string,
	lockerID string,
) (*kvdb.KVPair, error) {
	pair, err := k.WrappedKvdb.LockWithID(key, lockerID)
	logrus.WithFields(logrus.Fields{
		opType:    "LockWithID",
		output:    pair,
		errString: err,
	}).Info()
	return pair, err
}

func (k *kvLogger) LockWithTimeout(
	key string,
	lockerID string,
	lockTryDuration time.Duration,
	lockHoldDuration time.Duration,
) (*kvdb.KVPair, error) {
	pair, err := k.WrappedKvdb.LockWithTimeout(key, lockerID, lockTryDuration, lockHoldDuration)
	logrus.WithFields(logrus.Fields{
		opType:    "LockWithTimeout",
		output:    pair,
		errString: err,
	}).Info()
	return pair, err
}

func (k *kvLogger) Unlock(kvp *kvdb.KVPair) error {
	err := k.WrappedKvdb.Unlock(kvp)
	logrus.WithFields(logrus.Fields{
		opType:    "Unlock",
		errString: err,
	}).Info()
	return err
}

func (k *kvLogger) EnumerateWithSelect(
	prefix string,
	enumerateSelect kvdb.EnumerateSelect,
	copySelect kvdb.CopySelect,
) ([]interface{}, error) {
	vals, err := k.WrappedKvdb.EnumerateWithSelect(prefix, enumerateSelect, copySelect)
	logrus.WithFields(logrus.Fields{
		opType:    "EnumerateWithSelect",
		"length":  len(vals),
		errString: err,
	}).Info()
	return vals, err
}

func (k *kvLogger) GetWithCopy(
	key string,
	copySelect kvdb.CopySelect,
) (interface{}, error) {
	pair, err := k.WrappedKvdb.GetWithCopy(key, copySelect)
	logrus.WithFields(logrus.Fields{
		opType:    "GetWithCopy",
		output:    pair,
		errString: err,
	}).Info()
	return pair, err
}

func (k *kvLogger) TxNew() (kvdb.Tx, error) {
	tx, err := k.WrappedKvdb.TxNew()
	logrus.WithFields(logrus.Fields{
		opType:    "Snapshot",
		errString: err,
	}).Info()
	return tx, err
}

func (k *kvLogger) SnapPut(snapKvp *kvdb.KVPair) (*kvdb.KVPair, error) {
	pair, err := k.WrappedKvdb.SnapPut(snapKvp)
	logrus.WithFields(logrus.Fields{
		opType:    "SnapPut",
		output:    pair,
		errString: err,
	}).Info()
	return pair, err
}

func (k *kvLogger) AddUser(username string, password string) error {
	err := k.WrappedKvdb.AddUser(username, password)
	logrus.WithFields(logrus.Fields{
		opType:    "AddUser",
		errString: err,
	}).Info()
	return err
}

func (k *kvLogger) RemoveUser(username string) error {
	err := k.WrappedKvdb.RemoveUser(username)
	logrus.WithFields(logrus.Fields{
		opType:    "RemoveUser",
		errString: err,
	}).Info()
	return err
}

func (k *kvLogger) GrantUserAccess(
	username string,
	permType kvdb.PermissionType,
	subtree string,
) error {
	err := k.WrappedKvdb.GrantUserAccess(username, permType, subtree)
	logrus.WithFields(logrus.Fields{
		opType:    "GrantUserAccess",
		errString: err,
	}).Info()
	return err
}

func (k *kvLogger) RevokeUsersAccess(
	username string,
	permType kvdb.PermissionType,
	subtree string,
) error {
	err := k.WrappedKvdb.RevokeUsersAccess(username, permType, subtree)
	logrus.WithFields(logrus.Fields{
		opType:    "RevokeUsersAccess",
		errString: err,
	}).Info()
	return err
}

func (k *kvLogger) SetFatalCb(f kvdb.FatalErrorCB) {
	k.WrappedKvdb.SetFatalCb(f)
}

func (k *kvLogger) SetLockTimeout(timeout time.Duration) {
	k.WrappedKvdb.SetLockTimeout(timeout)
}

func (k *kvLogger) GetLockTimeout() time.Duration {
	return k.WrappedKvdb.GetLockTimeout()
}

func (k *kvLogger) Serialize() ([]byte, error) {
	return k.WrappedKvdb.Serialize()
}

func (k *kvLogger) Deserialize(b []byte) (kvdb.KVPairs, error) {
	return k.WrappedKvdb.Deserialize(b)
}

func (k *kvLogger) AddMember(nodeIP, nodePeerPort, nodeName string) (map[string][]string, error) {
	members, err := k.WrappedKvdb.AddMember(nodeIP, nodePeerPort, nodeName)
	logrus.WithFields(logrus.Fields{
		opType:    "AddMember",
		output:    members,
		errString: err,
	}).Info()
	return members, err
}

func (k *kvLogger) RemoveMember(nodeName, nodeIP string) error {
	err := k.WrappedKvdb.RemoveMember(nodeName, nodeIP)
	logrus.WithFields(logrus.Fields{
		opType:    "RemoveMember",
		errString: err,
	}).Info()
	return err
}

func (k *kvLogger) UpdateMember(nodeIP, nodePeerPort, nodeName string) (map[string][]string, error) {
	members, err := k.WrappedKvdb.UpdateMember(nodeIP, nodePeerPort, nodeName)
	logrus.WithFields(logrus.Fields{
		opType:    "UpdateMember",
		output:    members,
		errString: err,
	}).Info()
	return members, err
}

func (k *kvLogger) ListMembers() (map[string]*kvdb.MemberInfo, error) {
	members, err := k.WrappedKvdb.ListMembers()
	logrus.WithFields(logrus.Fields{
		opType:    "ListMembers",
		output:    members,
		errString: err,
	}).Info()
	return members, err
}

func (k *kvLogger) SetEndpoints(endpoints []string) error {
	err := k.WrappedKvdb.SetEndpoints(endpoints)
	logrus.WithFields(logrus.Fields{
		opType:    "SetEndpoints",
		errString: err,
	}).Info()
	return err
}

func (k *kvLogger) GetEndpoints() []string {
	endpoints := k.WrappedKvdb.GetEndpoints()
	logrus.WithFields(logrus.Fields{
		opType: "GetEndpoints",
		output: endpoints,
	}).Info()
	return endpoints
}

func (k *kvLogger) Defragment(endpoint string, timeout int) error {
	return k.WrappedKvdb.Defragment(endpoint, timeout)
}
