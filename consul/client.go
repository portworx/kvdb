package consul

import (
	"bytes"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/hashicorp/consul/api"
	"github.com/portworx/kvdb"
)

// clientConsuler defines methods that a px based consul client should satisfy.
type clientConsuler interface {
	// KV returns pointer to underlying consul KV object.
	KV() *api.KV
	// Session returns pointer to underlying Session object.
	Session() *api.Session
	// Refresh is PX specific op. It refreshes consul client on failover.
	Refresh() error
	// kvConsuler includes methods from that interface.
	kvConsuler
	// sessionConsuler includes methods methods from that interface.
	sessionConsuler
	// metaConsuler includes methods from that interface.
	metaConsuler
	// lockOptsConsuler includes methods from that interface.
	lockOptsConsuler
}

type kvConsuler interface {
	// Get exposes underlying KV().Get but with refresh on failover.
	Get(key string, q *api.QueryOptions) (*api.KVPair, *api.QueryMeta, error)
	// Put exposes underlying KV().Put but with refresh on failover.
	Put(p *api.KVPair, q *api.WriteOptions) (*api.WriteMeta, error)
	// Acquire exposes underlying KV().Acquire but with refresh on failover.
	Acquire(p *api.KVPair, q *api.WriteOptions) (*api.WriteMeta, error)
	// Delete exposes underlying KV().Delete but with refresh on failover.
	Delete(key string, w *api.WriteOptions) (*api.WriteMeta, error)
	// DeleteTree exposes underlying KV().DeleteTree but with refresh on failover.
	DeleteTree(prefix string, w *api.WriteOptions) (*api.WriteMeta, error)
	// Keys exposes underlying KV().Keys but with refresh on failover.
	Keys(prefix, separator string, q *api.QueryOptions) ([]string, *api.QueryMeta, error)
	// List exposes underlying KV().List but with refresh on failover.
	List(prefix string, q *api.QueryOptions) (api.KVPairs, *api.QueryMeta, error)
}

type sessionConsuler interface {
	// Create exposes underlying Session().Create but with refresh on failover.
	Create(se *api.SessionEntry, q *api.WriteOptions) (string, *api.WriteMeta, error)
	// Destroy exposes underlying Session().Destroy but with refresh on failover.
	Destroy(id string, q *api.WriteOptions) (*api.WriteMeta, error)
	// Renew exposes underlying Session().Renew but with refresh on failover.
	Renew(id string, q *api.WriteOptions) (*api.SessionEntry, *api.WriteMeta, error)
	// RenewPeriodic exposes underlying Session().RenewPeriodic but with refresh on failover.
	RenewPeriodic(initialTTL string, id string, q *api.WriteOptions, doneCh <-chan struct{}) error
}

type metaConsuler interface {
	// CreateMeta is a meta writer wrapping KV().Acquire and Session().Destroy but with refresh on failover.
	CreateMeta(id string, p *api.KVPair, q *api.WriteOptions) (*api.WriteMeta, bool, error)
	// CompareAndSet is a meta func wrapping KV().CAS and KV().Get but with refresh on failover.
	CompareAndSet(id string, value []byte, p *api.KVPair, q *api.WriteOptions) (bool, *api.WriteMeta, error)
	// CompareAndDelete is a meta func wrapping KV().DeleteCAS and KV().Get but with refresh on failover.
	CompareAndDelete(id string, value []byte, p *api.KVPair, q *api.WriteOptions) (bool, *api.WriteMeta, error)
}

type lockOptsConsuler interface {
	// LockOpts returns pointer to underlying Lock object and an error.
	LockOpts(opts *api.LockOptions) (*api.Lock, error)
}

// consulClient wraps config information and consul client along with sync functionality to refresh it once.
// consulClient also satisfies interface defined above.
type consulClient struct {
	// config is the configuration used to create consulClient
	config *api.Config
	// client provides access to consul api
	client *api.Client
	// once is used to refresh consulClient only once among concurrently running threads
	once *sync.Once
	// myParams holds all params required to obtain new api client
	myParams param
	// refreshDelay is the time duration to wait between machines
	refreshDelay time.Duration
	// refreshCount is the number of times refresh should be tried.
	refreshCount int
}

// newConsulClient provides an instance of clientConsuler interface.
func newConsulClienter(config *api.Config,
	client *api.Client,
	refreshDelay time.Duration, p param) clientConsuler {
	c := new(consulClient)
	c.config = config
	c.client = client
	c.once = new(sync.Once)
	c.myParams = p
	c.refreshDelay = refreshDelay
	if len(c.myParams.machines) < 3 {
		c.refreshCount = 3
	} else {
		c.refreshCount = len(c.myParams.machines)
	}
	return c
}

// KV returns pointer to underlying consul KV object.
func (c *consulClient) KV() *api.KV {
	return c.client.KV()
}

// Session returns pointer to underlying Session object.
func (c *consulClient) Session() *api.Session {
	return c.client.Session()
}

// LockOpts returns pointer to underlying Lock object and an error.
func (c *consulClient) LockOpts(opts *api.LockOptions) (*api.Lock, error) {
	return c.client.LockOpts(opts)
}

// Refresh is PX specific op. It refreshes consul client on failover.
func (c *consulClient) Refresh() error {
	var err error
	var executed bool

	// once.Do executes func() only once across concurrently executing threads
	c.once.Do(func() {
		executed = true
		var config *api.Config
		var client *api.Client

		for _, machine := range c.myParams.machines {
			machine := machine

			logrus.Infof("%s: %s\n", "trying to refresh client with machine", machine)

			if strings.HasPrefix(machine, "http://") {
				machine = strings.TrimPrefix(machine, "http://")
			} else if strings.HasPrefix(machine, "https://") {
				machine = strings.TrimPrefix(machine, "https://")
			}

			// sleep for requested delay before testing new connection
			time.Sleep(c.refreshDelay)
			if config, client, err = newKvClient(machine, c.myParams); err == nil {
				c.client = client
				c.config = config
				logrus.Infof("%s: %s\n", "successfully connected to", machine)
				return
			}
		}
	})

	// update once only if the func above was executed
	if executed {
		c.once = new(sync.Once)
	}

	return err
}

// newKvClient constructs new kvdb.Kvdb given a single end-point to connect to.
func newKvClient(machine string, p param) (*api.Config, *api.Client, error) {
	config := api.DefaultConfig()
	config.HttpClient = http.DefaultClient
	config.Address = machine
	config.Scheme = "http"
	// Get the ACL token if provided
	config.Token = p.options[kvdb.ACLTokenKey]

	client, err := api.NewClient(config)
	if err != nil {
		return nil, nil, err
	}

	// check health to ensure communication with consul are working
	if _, _, err := client.Health().State(api.HealthAny, nil); err != nil {
		return nil, nil, err
	}

	return config, client, nil
}

type writeFunc func() (*api.WriteMeta, error)

func (c *consulClient) writeRetryFunc(f writeFunc) (*api.WriteMeta, error) {
	var err error
	var meta *api.WriteMeta
	retry := false
	for i := 0; i < c.refreshCount; i++ {
		meta, err = f()
		if retry, err = c.checkAndRefresh(err); !retry {
			break
		}

	}
	return meta, err
}

/// checkAndRefresh returns (retry, error), retry is true is client refreshed
func (c *consulClient) checkAndRefresh(err error) (bool, error) {
	if err == nil {
		return false, nil
	} else if isConsulErrNeedingRetry(err) {
		if clientErr := c.Refresh(); clientErr != nil {
			return false, clientErr
		} else {
			return true, nil
		}
	} else {
		return false, err
	}
}

func (c *consulClient) Get(key string, q *api.QueryOptions) (*api.KVPair, *api.QueryMeta, error) {
	var pair *api.KVPair
	var meta *api.QueryMeta
	var err error
	retry := false

	for i := 0; i < c.refreshCount; i++ {
		pair, meta, err = c.client.KV().Get(key, q)
		if retry, err = c.checkAndRefresh(err); !retry {
			break
		}
	}

	return pair, meta, err
}

func (c *consulClient) Put(p *api.KVPair, q *api.WriteOptions) (*api.WriteMeta, error) {
	return c.writeRetryFunc(func() (*api.WriteMeta, error) { return c.client.KV().Put(p, nil) })
}

func (c *consulClient) Delete(key string, w *api.WriteOptions) (*api.WriteMeta, error) {
	return c.writeRetryFunc(func() (*api.WriteMeta, error) { return c.client.KV().Delete(key, nil) })
}

func (c *consulClient) DeleteTree(prefix string, w *api.WriteOptions) (*api.WriteMeta, error) {
	return c.writeRetryFunc(func() (*api.WriteMeta, error) { return c.client.KV().DeleteTree(prefix, nil) })
}

func (c *consulClient) Keys(prefix, separator string, q *api.QueryOptions) ([]string, *api.QueryMeta, error) {
	var list []string
	var meta *api.QueryMeta
	var err error
	retry := false

	for i := 0; i < c.refreshCount; i++ {
		list, meta, err = c.client.KV().Keys(prefix, separator, nil)
		if retry, err = c.checkAndRefresh(err); !retry {
			break
		}
	}

	return list, meta, err
}

func (c *consulClient) List(prefix string, q *api.QueryOptions) (api.KVPairs, *api.QueryMeta, error) {
	var pairs api.KVPairs
	var meta *api.QueryMeta
	var err error
	retry := false

	for i := 0; i < c.refreshCount; i++ {
		pairs, meta, err = c.client.KV().List(prefix, nil)
		if retry, err = c.checkAndRefresh(err); !retry {
			break
		}
	}

	return pairs, meta, err
}

func (c *consulClient) Acquire(p *api.KVPair, q *api.WriteOptions) (*api.WriteMeta, error) {
	var err error
	var meta *api.WriteMeta
	var ok bool

	retry := false
	for i := 0; i < c.refreshCount; i++ {
		ok, meta, err = c.client.KV().Acquire(p, nil)
		if retry, err = c.checkAndRefresh(err); !retry {
			break
		}
	}

	// *** this error is created in loop above
	if err != nil {
		return nil, err
	}

	if !ok {
		return nil, fmt.Errorf("Acquire failed")
	}

	return meta, err
}

func (c *consulClient) Create(se *api.SessionEntry, q *api.WriteOptions) (string, *api.WriteMeta, error) {
	var session string
	var meta *api.WriteMeta
	var err error
	retry := false

	for i := 0; i < c.refreshCount; i++ {
		session, meta, err = c.client.Session().Create(se, q)
		if retry, err = c.checkAndRefresh(err); !retry {
			break
		}
	}

	return session, meta, err
}

func (c *consulClient) Destroy(id string, q *api.WriteOptions) (*api.WriteMeta, error) {
	return c.writeRetryFunc(func() (*api.WriteMeta, error) { return c.client.Session().Destroy(id, q) })
}

func (c *consulClient) Renew(id string, q *api.WriteOptions) (*api.SessionEntry, *api.WriteMeta, error) {
	var entry *api.SessionEntry
	var meta *api.WriteMeta
	var err error
	retry := false

	for i := 0; i < c.refreshCount; i++ {
		entry, meta, err = c.client.Session().Renew(id, q)
		if retry, err = c.checkAndRefresh(err); !retry {
			break
		}
	}

	return entry, meta, err
}

func (c *consulClient) RenewPeriodic(initialTTL string, id string, q *api.WriteOptions, doneCh <-chan struct{}) error {
	var err error
	retry := false

	for i := 0; i < c.refreshCount; i++ {
		err = c.client.Session().RenewPeriodic(initialTTL, id, nil, doneCh)
		if retry, err = c.checkAndRefresh(err); !retry {
			break
		}
	}

	return err
}

func (c *consulClient) CreateMeta(id string, p *api.KVPair, q *api.WriteOptions) (*api.WriteMeta, bool, error) {
	var ok bool
	var meta *api.WriteMeta
	var err error
	for i := 0; i < c.refreshCount; i++ {
		ok, meta, err = c.client.KV().Acquire(p, q)
		if ok && err == nil {
			return nil, ok, err
		}
		if _, err := c.client.Session().Destroy(p.Session, nil); err != nil {
			logrus.Error(err)
		}
		if _, err := c.Delete(id, nil); err != nil {
			logrus.Error(err)
		}
		if err != nil {
			if isConsulErrNeedingRetry(err) {
				if clientErr := c.Refresh(); clientErr != nil {
					return nil, ok, clientErr
				} else {
					continue
				}
			} else {
				return nil, ok, err
			}
		} else {
			break
		}
	}

	if !ok {
		return nil, ok, fmt.Errorf("Failed to set ttl")
	}

	return meta, ok, err
}

func (c *consulClient) CompareAndSet(id string, value []byte, p *api.KVPair, q *api.WriteOptions) (bool, *api.WriteMeta, error) {
	var ok bool
	var meta *api.WriteMeta
	var err error
	retried := false

	for i := 0; i < c.refreshCount; i++ {
		ok, meta, err = c.client.KV().CAS(p, q)
		if err != nil && isConsulErrNeedingRetry(err) {
			retried = true
			if clientErr := c.Refresh(); clientErr != nil {
				return false, nil, clientErr
			} else {
				continue
			}

		} else if err != nil && isKeyIndexMismatchErr(err) && retried {
			kvPair, _, getErr := c.client.KV().Get(id, nil)
			if getErr != nil {
				// failed to get value from kvdb
				return false, nil, err
			}

			// Prev Value not equal to current value in consul
			if bytes.Compare(kvPair.Value, value) != 0 {
				return false, nil, err
			} else {
				// kvdb has the new value that we are trying to set
				err = nil
				break
			}
		} else {
			break
		}
	}

	return ok, meta, err
}

func (c *consulClient) CompareAndDelete(id string, value []byte, p *api.KVPair, q *api.WriteOptions) (bool, *api.WriteMeta, error) {
	var ok bool
	var meta *api.WriteMeta
	var err error
	retried := false

	for i := 0; i < c.refreshCount; i++ {
		ok, meta, err = c.client.KV().DeleteCAS(p, q)
		if err != nil && isConsulErrNeedingRetry(err) {
			retried = true
			if clientErr := c.Refresh(); clientErr != nil {
				return false, nil, clientErr
			} else {
				continue
			}

		} else if err != nil && isKeyIndexMismatchErr(err) && retried {
			kvPair, _, getErr := c.client.KV().Get(id, nil)
			if getErr != nil {
				// failed to get value from kvdb
				return false, nil, err
			}

			// Prev Value not equal to current value in consul
			if bytes.Compare(kvPair.Value, value) != 0 {
				return false, nil, err
			} else {
				// kvdb has the new value that we are trying to set
				err = nil
				break
			}
		} else {
			break
		}
	}

	return ok, meta, err
}
