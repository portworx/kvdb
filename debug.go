package kvdb

import (
	"fmt"
	"os"
	"runtime"
	"strings"
	"time"

	"github.com/sirupsen/logrus"
)

const (
	defaultLogLocation = "/var/lib/osd/kvdb_audit.log"
)

// NewKvdbDebugFilter returns a wrapper over kvdb APIs
// It logs every API request made to the kvdb request
func NewKvdbDebugFilter(
	kv Kvdb,
	logFileName string,
) (Kvdb, error) {
	if len(logFileName) == 0 {
		logFileName = defaultLogLocation
	}
	logIoWriter, err := OpenLog(logFileName)
	if err != nil {
		return nil, err
	}
	logger := logrus.New()
	logger.Out = logIoWriter

	return &debugKvdb{
		kv:     kv,
		logger: logger,
	}, nil
}

type debugKvdb struct {
	kv     Kvdb
	logger *logrus.Logger
}

func (d *debugKvdb) log(
	key string,
) {
	fpcs := make([]uintptr, 2)

	// Skip 2 levels to get the caller of log
	n := runtime.Callers(2, fpcs)
	if n == 0 {
		return
	}

	frames := runtime.CallersFrames(fpcs)
	// Get the caller of this log function

	logCallerFrame, more := frames.Next()
	logFn := logCallerFrame.Function
	caller := ""
	if more {
		kvdbCallerFrame, _ := frames.Next()
		caller = kvdbCallerFrame.Function

	}
	d.logger.WithFields(
		logrus.Fields{
			"type":   d.kv.String(),
			"method": logFn,
			"caller": caller,
			"key":    key,
		},
	).Infof("")
}
func (d *debugKvdb) String() string {
	d.log("")
	return d.kv.String()
}

func (d *debugKvdb) Capabilities() int {
	d.log("")
	return d.kv.Capabilities()
}

func (d *debugKvdb) Get(key string) (*KVPair, error) {
	d.log(key)
	return d.kv.Get(key)
}

func (d *debugKvdb) GetVal(
	key string,
	value interface{},
) (*KVPair, error) {
	d.log(key)
	return d.kv.GetVal(key, value)
}

func (d *debugKvdb) Put(
	key string,
	value interface{},
	ttl uint64,
) (*KVPair, error) {
	d.log(key)
	return d.kv.Put(key, value, ttl)
}

func (d *debugKvdb) Create(
	key string,
	value interface{},
	ttl uint64,
) (*KVPair, error) {
	d.log(key)
	return d.kv.Create(key, value, ttl)
}

func (d *debugKvdb) Update(
	key string,
	value interface{},
	ttl uint64,
) (*KVPair, error) {
	d.log(key)
	return d.kv.Update(key, value, ttl)
}

func (d *debugKvdb) SetFatalCb(f FatalErrorCB) {
	d.log("")
	d.kv.SetFatalCb(f)
}

func (d *debugKvdb) SetLockTimeout(timeout time.Duration) {
	d.log(timeout.String())
	d.kv.SetLockTimeout(timeout)
}

func (d *debugKvdb) GetLockTimeout() time.Duration {
	d.log("")
	return d.kv.GetLockTimeout()
}

func (d *debugKvdb) Enumerate(prefix string) (KVPairs, error) {
	d.log(prefix)
	return d.kv.Enumerate(prefix)
}

func (d *debugKvdb) Delete(key string) (*KVPair, error) {
	d.log(key)
	return d.kv.Delete(key)
}

func (d *debugKvdb) DeleteTree(prefix string) error {
	d.log(prefix)
	return d.kv.DeleteTree(prefix)
}

func (d *debugKvdb) Keys(prefix, key string) ([]string, error) {
	d.log(prefix)
	return d.kv.Keys(prefix, key)
}

func (d *debugKvdb) CompareAndSet(
	kvp *KVPair,
	flags KVFlags,
	prevValue []byte,
) (*KVPair, error) {
	d.log(kvp.Key)
	return d.kv.CompareAndSet(kvp, flags, prevValue)
}

func (d *debugKvdb) CompareAndDelete(
	kvp *KVPair,
	flags KVFlags,
) (*KVPair, error) {
	d.log(kvp.Key)
	return d.kv.CompareAndDelete(kvp, flags)
}

func (d *debugKvdb) WatchKey(
	key string,
	waitIndex uint64,
	opaque interface{},
	watchCB WatchCB,
) error {
	d.log(key)
	return d.kv.WatchKey(key, waitIndex, opaque, watchCB)
}

func (d *debugKvdb) WatchTree(
	prefix string,
	waitIndex uint64,
	opaque interface{},
	watchCB WatchCB,
) error {
	d.log(prefix)
	return d.kv.WatchTree(prefix, waitIndex, opaque, watchCB)
}

func (d *debugKvdb) EnumerateWithSelect(
	prefix string,
	enumerateSelect EnumerateSelect,
	copySelect CopySelect,
) ([]interface{}, error) {
	d.log(prefix)
	return d.kv.EnumerateWithSelect(prefix, enumerateSelect, copySelect)
}

func (d *debugKvdb) GetWithCopy(
	key string,
	copySelect CopySelect,
) (interface{}, error) {
	d.log(key)
	return d.kv.GetWithCopy(key, copySelect)
}

func (d *debugKvdb) Serialize() ([]byte, error) {
	d.log("")
	return d.kv.Serialize()
}

func (d *debugKvdb) Deserialize(b []byte) (KVPairs, error) {
	d.log("")
	return d.kv.Deserialize(b)
}

func (d *debugKvdb) Snapshot(prefix []string, consistent bool) (Kvdb, uint64, error) {
	d.log(strings.Join(prefix, ","))
	return d.kv.Snapshot(prefix, consistent)
}

func (d *debugKvdb) SnapPut(kvp *KVPair) (*KVPair, error) {
	d.log(kvp.Key)
	return d.kv.SnapPut(kvp)
}

func (d *debugKvdb) LockWithID(
	key string,
	lockerID string,
) (*KVPair, error) {
	d.log(key)
	return d.kv.LockWithID(key, lockerID)
}

func (d *debugKvdb) LockWithTimeout(
	key string,
	lockerID string,
	lockTryDuration time.Duration,
	lockHoldDuration time.Duration,
) (*KVPair, error) {
	d.log(key)
	return d.kv.LockWithTimeout(key, lockerID, lockTryDuration, lockHoldDuration)
}

func (d *debugKvdb) Lock(key string) (*KVPair, error) {
	d.log(key)
	return d.kv.Lock(key)
}

func (d *debugKvdb) Unlock(kvp *KVPair) error {
	d.log(kvp.Key)
	return d.kv.Unlock(kvp)
}

func (d *debugKvdb) TxNew() (Tx, error) {
	d.log("")
	return d.kv.TxNew()
}

func (d *debugKvdb) AddUser(username string, password string) error {
	d.log("")
	return d.kv.AddUser(username, password)
}

func (d *debugKvdb) RemoveUser(username string) error {
	d.log("")
	return d.kv.RemoveUser(username)
}

func (d *debugKvdb) GrantUserAccess(
	username string,
	permType PermissionType,
	subtree string,
) error {
	d.log("")
	return d.kv.GrantUserAccess(username, permType, subtree)
}

func (d *debugKvdb) RevokeUsersAccess(
	username string,
	permType PermissionType,
	subtree string,
) error {
	d.log("")
	return d.kv.RevokeUsersAccess(username, permType, subtree)
}

func (d *debugKvdb) AddMember(
	nodeIP string,
	nodePeerPort string,
	nodeName string,
) (map[string][]string, error) {
	d.log(nodeIP)
	return d.kv.AddMember(nodeIP, nodePeerPort, nodeName)
}

func (d *debugKvdb) UpdateMember(
	nodeIP string,
	nodePeerPort string,
	nodeName string,
) (map[string][]string, error) {
	d.log(nodeIP)
	return d.kv.UpdateMember(nodeIP, nodePeerPort, nodeName)
}

func (d *debugKvdb) RemoveMember(
	nodeName string,
	nodeIP string,
) error {
	d.log(nodeIP)
	return d.kv.RemoveMember(nodeName, nodeIP)
}

func (d *debugKvdb) ListMembers() (map[string]*MemberInfo, error) {
	d.log("")
	return d.kv.ListMembers()
}

func (d *debugKvdb) SetEndpoints(endpoints []string) error {
	d.log(strings.Join(endpoints, ","))
	return d.kv.SetEndpoints(endpoints)
}

func (d *debugKvdb) GetEndpoints() []string {
	d.log("")
	return d.kv.GetEndpoints()
}

func (d *debugKvdb) Defragment(endpoint string, timeout int) error {
	d.log(endpoint)
	return d.kv.Defragment(endpoint, timeout)
}

func OpenLog(logfile string) (*os.File, error) {
	file, err := os.OpenFile(logfile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		return nil, fmt.Errorf("Unable to open logfile %s: %v", logfile, err)
	}
	return file, nil
}
