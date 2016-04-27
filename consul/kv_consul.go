// Package consul implements the KVDB interface based on consul.
// Code from docker/libkv was leveraged to build parts of this module.
package consul

import (
	"encoding/json"
	"net/http"
	"strings"
	"time"

	api "github.com/hashicorp/consul/api"

	"github.com/portworx/kvdb"
)

const (
	// Name is the name of this kvdb implementation.
	Name    = "consul-kv"
	defHost = "127.0.0.1:8500"
)

func init() {
	kvdb.Register(Name, New)
}

type consulKV struct {
	client *api.Client
	config *api.Config
	domain string
}

type consulLock struct {
	lock    *api.Lock
	renewCh chan struct{}
}

// New constructs a new kvdb.Kvdb.
func New(
	domain string,
	machines []string,
	options map[string]string,
) (kvdb.Kvdb, error) {

	if len(machines) == 0 {
		machines = []string{defHost}
	} else {
		if strings.HasPrefix(machines[0], "http://") {
			machines[0] = strings.TrimPrefix(machines[0], "http://")
		} else if strings.HasPrefix(machines[0], "https://") {
			machines[0] = strings.TrimPrefix(machines[0], "https://")
		}
	}

	if domain != "" && !strings.HasSuffix(domain, "/") {
		domain = domain + "/"
	}

	// Create Consul client
	config := api.DefaultConfig()
	config.HttpClient = http.DefaultClient
	config.Address = machines[0]
	config.Scheme = "http"

	// Creates a new client
	client, err := api.NewClient(config)
	if err != nil {
		return nil, err
	}
	return &consulKV{
		client,
		config,
		domain,
	}, nil
}

func (kv *consulKV) String() string {
	return Name
}

func (kv *consulKV) Get(key string) (*kvdb.KVPair, error) {
	options := &api.QueryOptions{
		AllowStale:        false,
		RequireConsistent: true,
	}

	key = kv.domain + key
	pair, meta, err := kv.client.KV().Get(key, options)
	if err != nil {
		return nil, err
	}

	if pair == nil {
		return nil, kvdb.ErrNotFound
	}

	return kv.pairToKv("get", pair, meta), nil
}

func (kv *consulKV) GetVal(key string, val interface{}) (*kvdb.KVPair, error) {
	kvp, err := kv.Get(key)
	if err != nil {
		return nil, err
	}
	err = json.Unmarshal(kvp.Value, val)
	return kvp, err
}

func (kv *consulKV) Put(key string, val interface{}, ttl uint64) (*kvdb.KVPair, error) {
	key = kv.domain + key

	b, err := kv.toBytes(val)
	if err != nil {
		return nil, err
	}

	pair := &api.KVPair{
		Key:   key,
		Value: b,
	}

	_, err = kv.client.KV().Put(pair, nil)
	if err != nil {
		return nil, err
	}

	return kv.pairToKv("put", pair, nil), nil
}

func (kv *consulKV) Create(key string, val interface{}, ttl uint64) (*kvdb.KVPair, error) {
	_, err := kv.Get(key)
	if err == nil {
		return nil, kvdb.ErrExist
	}

	key = kv.domain + key

	b, err := kv.toBytes(val)
	if err != nil {
		return nil, err
	}

	pair := &api.KVPair{
		Key:   key,
		Value: b,
	}

	_, err = kv.client.KV().Put(pair, nil)
	if err != nil {
		return nil, err
	}

	return kv.pairToKv("create", pair, nil), nil
}

func (kv *consulKV) Update(key string, val interface{}, ttl uint64) (*kvdb.KVPair, error) {
	_, err := kv.Get(key)
	if err != nil {
		return nil, err
	}

	key = kv.domain + key

	b, err := kv.toBytes(val)
	if err != nil {
		return nil, err
	}

	pair := &api.KVPair{
		Key:   key,
		Value: b,
	}

	_, err = kv.client.KV().Put(pair, nil)
	if err != nil {
		return nil, err
	}

	return kv.pairToKv("update", pair, nil), nil
}

func (kv *consulKV) Enumerate(prefix string) (kvdb.KVPairs, error) {
	prefix = kv.domain + prefix

	pairs, _, err := kv.client.KV().List(prefix, nil)
	if err != nil {
		return nil, err
	}

	return kv.pairToKvs("enumerate", pairs, nil), nil
}

func (kv *consulKV) Delete(key string) (*kvdb.KVPair, error) {
	pair, err := kv.Get(key)
	if err != nil {
		return nil, err
	}

	key = kv.domain + key

	_, err = kv.client.KV().Delete(key, nil)
	if err != nil {
		return nil, err
	}

	return pair, nil
}

func (kv *consulKV) DeleteTree(key string) error {
	key = kv.domain + key

	_, err := kv.client.KV().DeleteTree(key, nil)
	if err != nil {
		return err
	}

	return nil
}

func (kv *consulKV) Keys(prefix, key string) ([]string, error) {
	return nil, kvdb.ErrNotSupported
}

func (kv *consulKV) CompareAndSet(kvp *kvdb.KVPair, flags kvdb.KVFlags, prevValue []byte) (*kvdb.KVPair, error) {
	return nil, kvdb.ErrNotSupported
}

func (kv *consulKV) CompareAndDelete(kvp *kvdb.KVPair, flags kvdb.KVFlags) (*kvdb.KVPair, error) {
	return nil, kvdb.ErrNotSupported
}

func (kv *consulKV) WatchKey(key string, waitIndex uint64, opaque interface{}, cb kvdb.WatchCB) error {
	return kvdb.ErrNotSupported
}

func (kv *consulKV) WatchTree(prefix string, waitIndex uint64, opaque interface{}, cb kvdb.WatchCB) error {
	return kvdb.ErrNotSupported
}

func (kv *consulKV) Lock(key string, ttl uint64) (*kvdb.KVPair, error) {
	l, err := kv.getLock(key, ttl)
	if err != nil {
		return nil, err
	}

	_, err = l.lock.Lock(nil)
	if err != nil {
		return nil, err
	}

	pair := &kvdb.KVPair{
		Key:  key,
		Lock: l,
	}

	return pair, nil
}

func (kv *consulKV) Unlock(kvp *kvdb.KVPair) error {
	l := kvp.Lock.(*consulLock)

	return l.lock.Unlock()
}

func (kv *consulKV) TxNew() (kvdb.Tx, error) {
	return nil, kvdb.ErrNotSupported
}

func (kv *consulKV) createKv(pair *api.KVPair) *kvdb.KVPair {
	kvp := &kvdb.KVPair{
		Key:   pair.Key,
		Value: []byte(pair.Value),
	}

	kvp.Key = strings.TrimPrefix(kvp.Key, kv.domain)
	return kvp
}

func (kv *consulKV) pairToKv(action string, pair *api.KVPair, meta *api.QueryMeta) *kvdb.KVPair {
	kvp := kv.createKv(pair)
	switch action {
	case "create":
		kvp.Action = kvdb.KVCreate
	case "set", "update", "put":
		kvp.Action = kvdb.KVSet
	case "delete":
		kvp.Action = kvdb.KVDelete
	case "get":
		kvp.Action = kvdb.KVGet
	default:
		kvp.Action = kvdb.KVUknown
	}

	if meta != nil {
		kvp.KVDBIndex = meta.LastIndex
	}

	return kvp
}

func (kv *consulKV) pairToKvs(action string, pair []*api.KVPair, meta *api.QueryMeta) kvdb.KVPairs {
	kvs := make([]*kvdb.KVPair, len(pair))
	for i := range pair {
		kvs[i] = kv.pairToKv(action, pair[i], meta)
		if meta != nil {
			kvs[i].KVDBIndex = meta.LastIndex
		}
	}
	return kvs
}

func (kv *consulKV) toBytes(val interface{}) ([]byte, error) {
	var (
		b   []byte
		err error
	)

	switch val.(type) {
	case string:
		b = []byte(val.(string))
	case []byte:
		b = val.([]byte)
	default:
		b, err = json.Marshal(val)
		if err != nil {
			return nil, err
		}
	}

	return b, nil
}

func (kv *consulKV) getLock(key string, ttl uint64) (*consulLock, error) {
	key = kv.domain + key

	lockOpts := &api.LockOptions{
		Key: key,
	}

	lock := &consulLock{}

	if ttl != 0 {
		TTL := time.Duration(0)
		entry := &api.SessionEntry{
			Behavior:  api.SessionBehaviorRelease, // Release the lock when the session expires
			TTL:       (TTL / 2).String(),         // Consul multiplies the TTL by 2x
			LockDelay: 1 * time.Millisecond,       // Virtually disable lock delay
		}

		// Create the key session
		session, _, err := kv.client.Session().Create(entry, nil)
		if err != nil {
			return nil, err
		}

		// Place the session on lock
		lockOpts.Session = session

		// Renew the session ttl lock periodically
		go kv.client.Session().RenewPeriodic(entry.TTL, session, nil, nil)
	}

	l, err := kv.client.LockOpts(lockOpts)
	if err != nil {
		return nil, err
	}

	lock.lock = l
	return lock, nil
}
