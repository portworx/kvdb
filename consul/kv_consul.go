package consul

import (
	"encoding/json"
	"net/http"
	"strings"

	api "github.com/hashicorp/consul/api"

	"github.com/portworx/kvdb"
)

const (
	Name    = "consul-kv"
	defHost = "127.0.0.1:8500"
)

type ConsulKV struct {
	client *api.Client
	config *api.Config
	domain string
}

func ConsulInit(domain string,
	machines []string,
	options map[string]string) (kvdb.Kvdb, error) {

	if len(machines) == 0 {
		machines = []string{defHost}
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

	kv := &ConsulKV{
		config: config,
		client: client,
		domain: domain,
	}

	return kv, nil
}

func (kv *ConsulKV) String() string {
	return Name
}

func (kv *ConsulKV) createKv(pair *api.KVPair) *kvdb.KVPair {
	kvp := &kvdb.KVPair{
		Value: []byte(pair.Value),
		// TTL:           node.TTL,
		// ModifiedIndex: node.ModifiedIndex,
		// CreatedIndex:  node.CreatedIndex,
	}

	// Strip out the leading '/'
	if len(pair.Key) != 0 {
		kvp.Key = pair.Key[1:]
	} else {
		kvp.Key = pair.Key
	}
	kvp.Key = strings.TrimPrefix(kvp.Key, kv.domain)
	return kvp
}

func (kv *ConsulKV) pairToKv(action string, pair *api.KVPair, meta *api.QueryMeta) *kvdb.KVPair {
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

func (kv *ConsulKV) pairToKvs(action string, pair []*api.KVPair, meta *api.QueryMeta) kvdb.KVPairs {
	kvs := make([]*kvdb.KVPair, len(pair))
	for i := range pair {
		kvs[i] = kv.pairToKv(action, pair[i], meta)
		if meta != nil {
			kvs[i].KVDBIndex = meta.LastIndex
		}
	}
	return kvs
}

func (kv *ConsulKV) toBytes(val interface{}) ([]byte, error) {
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

func (kv *ConsulKV) Get(key string) (*kvdb.KVPair, error) {
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

func (kv *ConsulKV) GetVal(key string, val interface{}) (*kvdb.KVPair, error) {
	kvp, err := kv.Get(key)
	if err != nil {
		return nil, err
	}
	err = json.Unmarshal(kvp.Value, val)
	return kvp, err
}

func (kv *ConsulKV) Put(key string, val interface{}, ttl uint64) (*kvdb.KVPair, error) {
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

func (kv *ConsulKV) Create(key string, val interface{}, ttl uint64) (*kvdb.KVPair, error) {
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

func (kv *ConsulKV) Update(key string, val interface{}, ttl uint64) (*kvdb.KVPair, error) {
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

func (kv *ConsulKV) Enumerate(prefix string) (kvdb.KVPairs, error) {
	prefix = kv.domain + prefix

	pairs, _, err := kv.client.KV().List(prefix, nil)
	if err != nil {
		return nil, err
	}

	return kv.pairToKvs("enumerate", pairs, nil), nil
}

func (kv *ConsulKV) Delete(key string) (*kvdb.KVPair, error) {
	key = kv.domain + key

	pair, err := kv.Get(key)
	if err != nil {
		return nil, err
	}

	_, err = kv.client.KV().Delete(key, nil)
	if err != nil {
		return nil, err
	}

	return pair, nil
}

func (kv *ConsulKV) DeleteTree(key string) error {
	key = kv.domain + key

	if _, err := kv.Get(key); err != nil {
		return err
	}

	_, err := kv.client.KV().DeleteTree(key, nil)
	if err != nil {
		return err
	}

	return nil
}

func (kv *ConsulKV) Keys(prefix, key string) ([]string, error) {
	return nil, kvdb.ErrNotSupported
}

func (kv *ConsulKV) CompareAndSet(kvp *kvdb.KVPair, flags kvdb.KVFlags, prevValue []byte) (*kvdb.KVPair, error) {
	return nil, kvdb.ErrNotSupported
}

func (kv *ConsulKV) CompareAndDelete(kvp *kvdb.KVPair, flags kvdb.KVFlags) (*kvdb.KVPair, error) {
	return nil, kvdb.ErrNotSupported
}

func (kv *ConsulKV) WatchKey(key string, waitIndex uint64, opaque interface{}, cb kvdb.WatchCB) error {
	return kvdb.ErrNotSupported

	/*
		key = kv.domain + key
		go kv.watchStart(key, false, waitIndex, opaque, cb)
		return nil
	*/
}

func (kv *ConsulKV) WatchTree(prefix string, waitIndex uint64, opaque interface{}, cb kvdb.WatchCB) error {
	return kvdb.ErrNotSupported
	/*
		prefix = kv.domain + prefix
		go kv.watchStart(prefix, true, waitIndex, opaque, cb)
		return nil
	*/
}

func (kv *ConsulKV) Lock(key string, ttl uint64) (*kvdb.KVPair, error) {
	return nil, kvdb.ErrNotSupported

	/*
		key = kv.domain + key

		duration := time.Duration(math.Min(float64(time.Second),
			float64((time.Duration(ttl)*time.Second)/10)))

		result, err := kv.client.Create(key, "locked", ttl)
		for err != nil {
			time.Sleep(duration)
			result, err = kv.client.Create(key, "locked", ttl)
		}

		if err != nil {
			return nil, err
		}
		return kv.pairToKv(result), err
	*/
}

func (kv *ConsulKV) Unlock(kvp *kvdb.KVPair) error {
	return kvdb.ErrNotSupported
	/*
		// Don't modify kvp here, CompareAndDelete does that.
		_, err := kv.CompareAndDelete(kvp, kvdb.KVFlags(0))
		return err
	*/
}

/*
func (kv *ConsulKV) watchReceive(
	key string,
	opaque interface{},
	c chan *e.Response,
	stop chan bool,
	cb kvdb.WatchCB) {

	var err error
	for r, more := <-c; err == nil && more; {
		if more {
			err = cb(key, opaque, kv.pairToKv(r), nil)
			if err == nil {
				r, more = <-c
			}
		}
	}
	stop <- true
	close(stop)
}

func (kv *ConsulKV) watchStart(key string,
	recursive bool,
	waitIndex uint64,
	opaque interface{},
	cb kvdb.WatchCB) {

	ch := make(chan *e.Response, 10)
	stop := make(chan bool, 1)

	go kv.watchReceive(key, opaque, ch, stop, cb)

	_, err := kv.client.Watch(key, waitIndex, recursive, ch, stop)
	if err != e.ErrWatchStoppedByUser {
		e, ok := err.(e.EtcdError)
		if ok {
			fmt.Printf("Etcd error code %d, message %s cause %s Index %ju\n",
				e.ErrorCode, e.Message, e.Cause, e.Index)
		}
		cb(key, opaque, nil, err)
		fmt.Errorf("Watch returned unexpected error %s\n", err.Error())
	} else {
		cb(key, opaque, nil, kvdb.ErrWatchStopped)
	}
}
*/

func (kv *ConsulKV) TxNew() (kvdb.Tx, error) {
	return nil, kvdb.ErrNotSupported
}

func init() {
	kvdb.Register(Name, ConsulInit)
}
