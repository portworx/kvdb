package kvdb

import (
	"errors"
	"time"

	_ "github.com/golang/mock/mockgen/model"
	"github.com/sirupsen/logrus"
)

const (
	// KVSet signifies the KV was modified.
	KVSet KVAction = 1 << iota
	// KVCreate set if the KV pair was created.
	KVCreate
	// KVGet set when the key is fetched from the KV store
	KVGet
	// KVDelete set when the key is deleted from the KV store
	KVDelete
	// KVExpire set when the key expires
	KVExpire
	// KVUknown operation on KV pair
	KVUknown
)

const (
	// KVCapabilityOrderedUpdates support requires watch to send an watch update
	// for every put - instead of coalescing multiple puts in one update.
	KVCapabilityOrderedUpdates = 1 << iota
)

const (
	// KVPrevExists flag to check key already exists
	KVPrevExists KVFlags = 1 << iota
	// KVCreatedIndex flag compares with passed in index (possibly in KVPair)
	KVCreatedIndex
	// KVModifiedIndex flag compares with passed in index (possibly in KVPair)
	KVModifiedIndex
	// KVTTL uses TTL val from KVPair.
	KVTTL
)

const (
	// ReadPermission for read only access
	ReadPermission = iota
	// WritePermission for write only access
	WritePermission
	// ReadWritePermission for read-write access
	ReadWritePermission
)
const (
	// UsernameKey for an authenticated kvdb endpoint
	UsernameKey = "Username"
	// PasswordKey for an authenticated kvdb endpoint
	PasswordKey = "Password"
	// CAFileKey is the CA file path for an authenticated kvdb endpoint
	CAFileKey = "CAFile"
	// CertFileKey is the certificate file path for an authenticated kvdb endpoint
	CertFileKey = "CertFile"
	// CertKeyFileKey is the key to the certificate
	CertKeyFileKey = "CertKeyFile"
	// TrustedCAFileKey is the key for the trusted CA.
	TrustedCAFileKey = "TrustedCAFile"
	// ClientCertAuthKey is the boolean value indicating client authenticated certificate.
	ClientCertAuthKey = "ClientCertAuth"
	// RetryCountKey is the integer value indicating the retry count of etcd operations
	RetryCountKey = "RetryCount"
	// ACLTokenKey is the token value for ACL based KV stores
	ACLTokenKey = "ACLToken"
	// CAAuthAddress is the address of CA signing authority (required in consul TLS config)
	CAAuthAddress = "CAAuthAddress"
	// InsecureSkipVerify has a value true or false (required in consul TLS config)
	InsecureSkipVerify = "InsecureSkipVerify"
	// TransportScheme points to http transport being either http or https.
	TransportScheme = "TransportScheme"
	// LogPathOption is the name of the option which specified the log path location
	LogPathOption = "LogPathOption"
)

// List of kvdb endpoints supported versions
const (
	// ConsulVersion1 key
	ConsulVersion1 = "consulv1"
	// EtcdBaseVersion key
	EtcdBaseVersion = "etcd"
	// EtcdVersion3 key
	EtcdVersion3 = "etcdv3"
	// MemVersion1 key
	MemVersion1 = "memv1"
	// BoltVersion1 key
	BoltVersion1 = "boltv1"
	// ZookeeperVersion1 key
	ZookeeperVersion1 = "zookeeperv1"
)

const (
	// DefaultLockTryDuration is the maximum time spent trying to acquire lock
	DefaultLockTryDuration = 300 * time.Second
	// DefaultSeparator separate key components
	DefaultSeparator = "/"
)

type WrapperName string

const (
	Wrapper_None     = WrapperName("Wrapper_None")
	Wrapper_Log      = WrapperName("Wrapper_Log")
	Wrapper_NoQuorum = WrapperName("Wrapper_NoQuorum")
)

type KvdbWrapper interface {
	// WrapperName is the name of this wrapper
	WrapperName() WrapperName
	// WrappedKvdb is the Kvdb wrapped by this wrapper
	WrappedKvdb() Kvdb
	// Removed is called when wrapper is removed
	Removed()
	// WrappedKvdb is the Kvdb wrapped by this wrapper
	SetWrappedKvdb(kvdb Kvdb) error
}

var (
	// ErrNotSupported implemenation of a specific function is not supported.
	ErrNotSupported = errors.New("implementation not supported")
	// ErrWatchStopped is raised when user stops watch.
	ErrWatchStopped = errors.New("Watch Stopped")
	// ErrNotFound raised if Key is not found
	ErrNotFound = errors.New("Key not found")
	// ErrExist raised if key already exists
	ErrExist = errors.New("Key already exists")
	// ErrUnmarshal raised if Get fails to unmarshal value.
	ErrUnmarshal = errors.New("Failed to unmarshal value")
	// ErrIllegal raised if object is not valid.
	ErrIllegal = errors.New("Illegal operation")
	// ErrValueMismatch raised if existing KVDB value mismatches with user provided value
	ErrValueMismatch = errors.New("Value mismatch")
	// ErrEmptyValue raised if the value is empty
	ErrEmptyValue = errors.New("Value cannot be empty")
	// ErrModified raised during an atomic operation if the index does not match the one in the store
	ErrModified = errors.New("Key Index mismatch")
	// ErrSetTTLFailed raised if unable to set ttl value for a key create/put/update action
	ErrSetTTLFailed = errors.New("Unable to set ttl value")
	// ErrTTLNotSupported if kvdb implementation doesn't support TTL
	ErrTTLNotSupported = errors.New("TTL value not supported")
	// ErrInvalidLock Lock and unlock operations don't match.
	ErrInvalidLock = errors.New("Invalid lock/unlock operation")
	// ErrNoPassword provided
	ErrNoPassword = errors.New("Username provided without any password")
	// ErrAuthNotSupported authentication not supported for this kvdb implementation
	ErrAuthNotSupported = errors.New("Kvdb authentication not supported")
	// ErrNoCertificate no certificate provided for authentication
	ErrNoCertificate = errors.New("Certificate File Path not provided")
	// ErrUnknownPermission raised if unknown permission type
	ErrUnknownPermission = errors.New("Unknown Permission Type")
	// ErrMemberDoesNotExist returned when an operation fails for a member
	// which does not exist
	ErrMemberDoesNotExist = errors.New("Kvdb member does not exist")
	// ErrWatchRevisionCompacted requested watch version has been compacted
	ErrWatchRevisionCompacted = errors.New("Kvdb watch revision compacted")
	// ErrLockRefreshFailed could not refresh lock key so exclusive access to lock may be lost
	ErrLockRefreshFailed = errors.New("Failed to refresh lock")
	// ErrLockHoldTimeoutTriggered triggers if lock is held beyond configured timeout
	ErrLockHoldTimeoutTriggered = errors.New("Lock held beyond configured timeout")
	// ErrNoConnection no connection to server
	ErrNoConnection = errors.New("No server connection")
	// ErrNoQuorum kvdb has lost quorum
	ErrNoQuorum = errors.New("KVDB connection failed, either node has " +
		"networking issues or KVDB members are down or KVDB cluster is unhealthy. " +
		"All operations (get/update/delete) are unavailable.")
)

// KVAction specifies the action on a KV pair. This is useful to make decisions
// from the results of  a Watch.
type KVAction int

// KVFlags options for operations on KVDB
type KVFlags uint64

// PermissionType for user access
type PermissionType int

// WatchCB is called when a watched key or tree is modified. If the callback
// returns an error, then watch stops and the cb is called one last time
// with ErrWatchStopped.
type WatchCB func(prefix string, opaque interface{}, kvp *KVPair, err error) error

// FatalErrorCB callback is invoked incase of fatal errors
type FatalErrorCB func(err error, format string, args ...interface{})

// DatastoreInit is called to activate a backend KV store.
type DatastoreInit func(domain string, machines []string, options map[string]string,
	cb FatalErrorCB) (Kvdb, error)

// DatastoreVersion is called to get the version of a backend KV store
type DatastoreVersion func(url string, kvdbOptions map[string]string) (string, error)

// WrapperInit is called to activate a backend KV store.
type WrapperInit func(kv Kvdb, options map[string]string) (Kvdb, error)

// EnumerateSelect function is a callback function provided to EnumerateWithSelect API
// This fn is executed over all the keys and only those values are returned by Enumerate for which
// this function return true.
type EnumerateSelect func(val interface{}) bool

// CopySelect function is a callback function provided to EnumerateWithSelect API
// This fn should perform a deep copy of the input interface and return the copy
type CopySelect func(val interface{}) interface{}

// EnumerateKVPSelect function is a callback function provided to EnumerateKVPWithSelect API
// This fn is executed over all the keys and only those values are returned by Enumerate for which
// this function return true.
type EnumerateKVPSelect func(kvp *KVPair, val interface{}) bool

// CopyKVPSelect function is a callback function provided to EnumerateKVPWithSelect API
// This fn should perform a deep copy of the input KVPair and return the copy
type CopyKVPSelect func(kvp *KVPair, val interface{}) *KVPair

// KVPair represents the results of an operation on KVDB.
type KVPair struct {
	// Key for this kv pair.
	Key string
	// Value for this kv pair
	Value []byte
	// Action the last action on this KVPair.
	Action KVAction
	// TTL value after which this key will expire from KVDB
	TTL int64
	// KVDBIndex A Monotonically index updated at each modification operation.
	KVDBIndex uint64
	// CreatedIndex for this kv pair
	CreatedIndex uint64
	// ModifiedIndex for this kv pair
	ModifiedIndex uint64
	// Lock is a generic interface to represent a lock held on a key.
	Lock interface{}
}

// KVPairs list of KVPairs
type KVPairs []*KVPair

// Tx Interface to transactionally apply updates to a set of keys.
type Tx interface {
	// Put specified key value pair in TX.
	Put(key string, value interface{}, ttl uint64) (*KVPair, error)
	// Get returns KVPair in this TXs view. If not found, returns value from
	// backing KVDB.
	Get(key string) (*KVPair, error)
	// Get same as get except that value has the unmarshalled value.
	GetVal(key string, value interface{}) (*KVPair, error)
	// Prepare returns an error it transaction cannot be logged.
	Prepare() error
	// Commit propagates updates to the KVDB. No operations on this Tx are
	// allowed after commit.
	Commit() error
	// Abort aborts this transaction.  No operations on this Tx are allowed
	// afer commit.
	Abort() error
}

// Kvdb interface implemented by backing datastores.
//
//go:generate mockgen -package=mock -destination=mock/mock_kvdb.go . Kvdb
type Kvdb interface {
	Controller
	// String representation of backend datastore.
	String() string
	// Capbilities - see KVCapabilityXXX
	Capabilities() int
	// Get returns KVPair that maps to specified key or ErrNotFound.
	Get(key string) (*KVPair, error)
	// Get returns KVPair that maps to specified key or ErrNotFound. If found
	// value contains the unmarshalled result or error is ErrUnmarshal
	GetVal(key string, value interface{}) (*KVPair, error)
	// GetWithCopy returns a copy of the value as an interface for the specified key
	GetWithCopy(key string, copySelect CopySelect) (interface{}, error)
	// Put inserts value at key in kvdb. If value is a runtime.Object, it is
	// marshalled. If Value is []byte it is set directly. If Value is a string,
	// its byte representation is stored.
	Put(key string, value interface{}, ttl uint64) (*KVPair, error)
	// Create is the same as Put except that ErrExist is returned if the key exists.
	Create(key string, value interface{}, ttl uint64) (*KVPair, error)
	// Update is the same as Put except that ErrNotFound is returned if the key
	// does not exist.
	Update(key string, value interface{}, ttl uint64) (*KVPair, error)
	// Enumerate returns a list of KVPair for all keys that share the specified prefix.
	Enumerate(prefix string) (KVPairs, error)
	// EnumerateWithSelect returns a copy of all values under the prefix that satisfy the select
	// function in the provided output array of interfaces
	EnumerateWithSelect(prefix string, enumerateSelect EnumerateSelect, copySelect CopySelect) ([]interface{}, error)
	// EnumerateKVPWithSelect returns a copy of all the KVPairs under the prefix that satisfy the select
	// function in the provided output array of key-value pairs
	EnumerateKVPWithSelect(prefix string, enumerateSelect EnumerateKVPSelect, copySelect CopyKVPSelect) (KVPairs, error)
	// Delete deletes the KVPair specified by the key. ErrNotFound is returned
	// if the key is not found. The old KVPair is returned if successful.
	Delete(key string) (*KVPair, error)
	// DeleteTree same as Delete execpt that all keys sharing the prefix are
	// deleted.
	DeleteTree(prefix string) error
	// Keys returns an array of keys that share specified prefix (ie. "1st level directory").
	// sep parameter defines a key-separator, and if not provided the "/" is assumed.
	Keys(prefix, sep string) ([]string, error)
	// CompareAndSet updates value at kvp.Key if the previous resident
	// satisfies conditions set in flags and optional prevValue.
	CompareAndSet(kvp *KVPair, flags KVFlags, prevValue []byte) (*KVPair, error)
	// CompareAndDelete deletes value at kvp.Key if the previous resident matches
	// satisfies conditions set in flags.
	CompareAndDelete(kvp *KVPair, flags KVFlags) (*KVPair, error)
	// WatchKey calls watchCB everytime a value at key is updated. waitIndex
	// is the oldest ModifiedIndex of a KVPair for which updates are requestd.
	WatchKey(key string, waitIndex uint64, opaque interface{}, watchCB WatchCB) error
	// WatchTree is the same as WatchKey except that watchCB is triggered
	// for updates on all keys that share the prefix.
	WatchTree(prefix string, waitIndex uint64, opaque interface{}, watchCB WatchCB) error
	// Snapshot returns a kvdb snapshot of the provided list of prefixes and the last updated index.
	// If no prefixes are provided, then the whole kvdb tree is snapshotted and could be potentially an expensive operation
	// If consistent is true, then snapshot is going to return all the updates happening during the snapshot operation and the last
	// updated index from the snapshot
	Snapshot(prefixes []string, consistent bool) (Kvdb, uint64, error)
	// SnapPut records the key value pair including the index.
	SnapPut(kvp *KVPair) (*KVPair, error)
	// LockWithID locks the specified key and associates a lockerID with it, probably to identify
	// who acquired the lock. The KVPair returned should be used to unlock.
	LockWithID(key string, lockerID string) (*KVPair, error)
	// Lock locks the specified key. The KVPair returned should be used to unlock.
	Lock(key string) (*KVPair, error)
	// LockWithTimeout locks with specified key and associates a lockerID with it.
	// lockTryDuration is the maximum time that can be spent trying to acquire
	// lock, else return error.
	// lockHoldDuration is the maximum time the lock can be held, after which
	// FatalCb is invoked.
	// The KVPair returned should be used to unlock if successful.
	LockWithTimeout(key string, lockerID string, lockTryDuration time.Duration,
		lockHoldDuration time.Duration) (*KVPair, error)
	// IsKeyLocked returns a boolean if the lock is held or not. If held, returns the owner.
	IsKeyLocked(key string) (bool, string, error)
	// Unlock kvp previously acquired through a call to lock.
	Unlock(kvp *KVPair) error
	// TxNew returns a new Tx coordinator object or ErrNotSupported
	TxNew() (Tx, error)
	// AddUser adds a new user to kvdb
	AddUser(username string, password string) error
	// RemoveUser removes a user from kvdb
	RemoveUser(username string) error
	// GrantUserAccess grants user access to a subtree/prefix based on the permission
	GrantUserAccess(username string, permType PermissionType, subtree string) error
	// RevokeUsersAccess revokes user's access to a subtree/prefix based on the permission
	RevokeUsersAccess(username string, permType PermissionType, subtree string) error
	// SetFatalCb sets the function to be called in case of fatal errors
	SetFatalCb(f FatalErrorCB)
	// SetLockHoldDuration sets maximum time a lock may be held
	SetLockHoldDuration(timeout time.Duration)
	// GetLockTryDuration gets the maximum time to attempt to get a lock.
	GetLockTryDuration() time.Duration
	// GetLockHoldDuration gets the currently set lock hold timeout
	GetLockHoldDuration() time.Duration
	// Serialize serializes all the keys under the domain and returns a byte array
	Serialize() ([]byte, error)
	// Deserialize deserializes the given byte array into a list of kv pairs
	Deserialize([]byte) (KVPairs, error)
	// Compact removes the history before the specified index/revision to reduce the space and memory usage
	Compact(index uint64) error
	KvdbWrapper
}

// ReplayCb provides info required for replay
type ReplayCb struct {
	// Prefix is the watch key/tree prefix
	Prefix string
	// WaitIndex is the index after which updates must be returned
	WaitIndex uint64
	// Opaque is a hint returned by the caller
	Opaque interface{}
	// WatchCB is the watch callback
	WatchCB WatchCB
}

// UpdatesCollector collects updates from kvdb.
type UpdatesCollector interface {
	// Stop collecting updates
	Stop()
	// ReplayUpdates replays the collected updates.
	// Returns the version until the replay's were done
	// and any errors it encountered.
	ReplayUpdates(updateCb []ReplayCb) (uint64, error)
}

// NewUpdatesCollector creates new Kvdb collector that collects updates
// starting at startIndex + 1 index.
func NewUpdatesCollector(
	db Kvdb,
	prefix string,
	startIndex uint64,
) (UpdatesCollector, error) {
	collector := &updatesCollectorImpl{updates: make([]*kvdbUpdate, 0),
		startIndex: startIndex}
	logrus.Infof("Starting collector watch on %v at %v", prefix, startIndex)
	if err := db.WatchTree(prefix, startIndex, nil, collector.watchCb); err != nil {
		return nil, err
	}
	return collector, nil
}

// List of kvdb controller ports
const (
	// PeerPort is the port on which peers identify themselves
	PeerPort = "2380"
	// ClientPort is the port on which clients send requests to kvdb.
	ClientPort = "2379"
)

// MemberInfo represents a member of the kvdb cluster
type MemberInfo struct {
	// PeerUrls is this member's URL on which it talks to its peers
	PeerUrls []string
	// ClientUrls is this member's URL on which clients can reach this member.
	ClientUrls []string
	// Leader indicates if this member is the leader of this cluster.
	Leader bool
	// DbSize is the current DB size as seen by this member.
	DbSize int64
	// IsHealthy indicates the health of the member.
	IsHealthy bool
	// ID is the string representation of member's ID
	ID string
	// Name of the member. A member which has not started has an empty Name.
	Name string
	// HasStarted indicates if this member has successfully started kvdb.
	HasStarted bool
	// IsLearner indicates if this member is a learner (i.e. not yet promoted to a full voting member).
	IsLearner bool
}

// Controller interface provides APIs to manage Kvdb Cluster and Kvdb Clients.
type Controller interface {
	// AddMember adds a new member to an existing kvdb cluster. Add API should be
	// invoked on an existing kvdb node where kvdb is already running. It should be
	// followed by a Setup call on the node which is being added.
	// Returns: map of nodeID to peerUrls of all members in the initial cluster or error.
	AddMember(nodeIP, nodePeerPort, nodeName string) (map[string][]string, error)

	// AddLearner is same as AddMember except that the new member is added as a learner.
	// It is caller's responsibility to promote it to a full voting member.
	AddLearner(nodeIP, nodePeerPort, nodeName string) (map[string][]string, error)

	// RemoveMember removes a member based on its Name from an existing kvdb cluster.
	// Returns: error if it fails to remove a member
	RemoveMember(nodeName, nodeIP string) error

	// RemoveMemberByID removes a member based on its ID from an existing kvdb cluster.
	// Returns: error if it fails to remove a member
	RemoveMemberByID(memberID uint64) error

	// UpdateMember updates the IP for the given node in an existing kvdb cluster
	// Returns: map of nodeID to peerUrls of all members from the existing cluster
	UpdateMember(nodeIP, nodePeerPort, nodeName string) (map[string][]string, error)

	// ListMembers enumerates the members of the kvdb cluster. It includes both the
	// started and unstarted members.
	// Returns: the member's ID  to MemberInfo mappings for all the members
	ListMembers() (map[uint64]*MemberInfo, error)

	// SetEndpoints set the kvdb endpoints for the client
	SetEndpoints(endpoints []string) error

	// GetEndpoints returns the kvdb endpoints for the client
	GetEndpoints() []string

	// Defragment defrags the underlying database for the given endpoint
	// with a timeout specified in seconds
	Defragment(endpoint string, timeout int) error
}

func LogFatalErrorCB(err error, format string, args ...interface{}) {
	logrus.Errorf("encountered fatal error: %v", err)
	logrus.Panicf(format, args...)
}
