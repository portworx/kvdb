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
	RenewPeriodic(initialTTL string, id string, q *api.WriteOptions, doneCh chan struct{}) error
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

// consulConnection stores current consul connection state
type consulConnection struct {
	// config is the configuration used to create consulClient
	config *api.Config
	// client provides access to consul api
	client *api.Client
	// once is used to refresh consulClient only once among concurrently running threads
	once *sync.Once
}

// consulClient wraps config information and consul client along with sync functionality to refresh it once.
// consulClient also satisfies interface defined above.
type consulClient struct {
	// conn current consul connection state
	conn *consulConnection
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
	c := &consulClient{
		conn: &consulConnection{
			config: config,
			client: client,
			once:   new(sync.Once)},
		myParams:     p,
		refreshDelay: refreshDelay,
	}
	if len(c.myParams.machines) < 3 {
		c.refreshCount = 3
	} else {
		c.refreshCount = len(c.myParams.machines)
	}
	return c
}

// LockOpts returns pointer to underlying Lock object and an error.
func (c *consulClient) LockOpts(opts *api.LockOptions) (*api.Lock, error) {
	return c.conn.client.LockOpts(opts)
}

// Refresh is PX specific op. It refreshes consul client on failover.
func (c *consulClient) Refresh(conn *consulConnection) error {
	var err error

	// once.Do executes func() only once across concurrently executing threads
	conn.once.Do(func() {
		var config *api.Config
		var client *api.Client

		for _, machine := range c.myParams.machines {

			logrus.Infof("%s: %s\n", "trying to refresh client with machine", machine)

			if strings.HasPrefix(machine, "http://") {
				machine = strings.TrimPrefix(machine, "http://")
			} else if strings.HasPrefix(machine, "https://") {
				machine = strings.TrimPrefix(machine, "https://")
			}

			// sleep for requested delay before testing new connection
			time.Sleep(c.refreshDelay)
			if config, client, err = newKvClient(machine, c.myParams); err == nil {
				c.conn = &consulConnection{
					client: client,
					config: config,
					once:   new(sync.Once),
				}
				logrus.Infof("%s: %s\n", "successfully connected to", machine)
				break
			} else {
				logrus.Errorf("failed to refresh client on: %s", machine)
			}
		}
	})

	if err != nil {
		logrus.Infof("Failed to refresh client: %v", err)
	}
	return err
}

// newKvClient constructs new kvdb.Kvdb given a single end-point to connect to.
func newKvClient(machine string, p param) (*api.Config, *api.Client, error) {
	logrus.Infof("consul: connecting to %v", machine)
	config := api.DefaultConfig()
	config.HttpClient = http.DefaultClient
	config.Address = machine
	config.Scheme = "http"
	// Get the ACL token if provided
	config.Token = p.options[kvdb.ACLTokenKey]

	client, err := api.NewClient(config)
	if err != nil {
		logrus.Info("consul: failed to get new api client: %v", err)
		return nil, nil, err
	}

	// check health to ensure communication with consul are working
	if _, _, err := client.Health().State(api.HealthAny, nil); err != nil {
		logrus.Errorf("consul: health check failed for %v : %v", machine, err)
		return nil, nil, err
	}

	return config, client, nil
}

type writeFunc func(conn *consulConnection) (*api.WriteMeta, error)

func (c *consulClient) writeRetryFunc(f writeFunc) (*api.WriteMeta, error) {
	var err error
	var meta *api.WriteMeta
	retry := false
	c.runWithRetry(func() bool {
		conn := c.conn
		meta, err = f(conn)
		retry, err = c.checkAndRefresh(conn, err)
		return retry
	})
	return meta, err
}

/// checkAndRefresh returns (retry, error), retry is true is client refreshed
func (c *consulClient) checkAndRefresh(conn *consulConnection, err error) (bool, error) {
	if err == nil {
		return false, nil
	} else if isConsulErrNeedingRetry(err) {
		logrus.Errorf("consul error: %v, trying to refresh..", err)
		if clientErr := c.Refresh(conn); clientErr != nil {
			return false, clientErr
		} else {
			return true, nil
		}
	} else {
		return false, err
	}
}

/// consulFunc runs a consulFunc operation and returns true if needs to be retried
type consulFunc func() bool

func (c *consulClient) runWithRetry(f consulFunc) {
	for i := 0; i < c.refreshCount; i++ {
		if !f() {
			break
		}
	}
}

func (c *consulClient) Get(key string, q *api.QueryOptions) (*api.KVPair, *api.QueryMeta, error) {
	var pair *api.KVPair
	var meta *api.QueryMeta
	var err error
	retry := false

	c.runWithRetry(func() bool {
		conn := c.conn
		pair, meta, err = conn.client.KV().Get(key, q)
		retry, err = c.checkAndRefresh(conn, err)
		return retry
	})

	return pair, meta, err
}

func (c *consulClient) Put(p *api.KVPair, q *api.WriteOptions) (*api.WriteMeta, error) {
	return c.writeRetryFunc(func(conn *consulConnection) (*api.WriteMeta, error) {
		return conn.client.KV().Put(p, nil)
	})
}

func (c *consulClient) Delete(key string, w *api.WriteOptions) (*api.WriteMeta, error) {
	return c.writeRetryFunc(func(conn *consulConnection) (*api.WriteMeta, error) {
		return conn.client.KV().Delete(key, nil)
	})
}

func (c *consulClient) DeleteTree(prefix string, w *api.WriteOptions) (*api.WriteMeta, error) {
	return c.writeRetryFunc(func(conn *consulConnection) (*api.WriteMeta, error) {
		return conn.client.KV().DeleteTree(prefix, nil)
	})
}

func (c *consulClient) Keys(prefix, separator string, q *api.QueryOptions) ([]string, *api.QueryMeta, error) {
	var list []string
	var meta *api.QueryMeta
	var err error
	retry := false

	c.runWithRetry(func() bool {
		conn := c.conn
		list, meta, err = conn.client.KV().Keys(prefix, separator, nil)
		retry, err = c.checkAndRefresh(conn, err)
		return retry
	})

	return list, meta, err
}

func (c *consulClient) List(prefix string, q *api.QueryOptions) (api.KVPairs, *api.QueryMeta, error) {
	var pairs api.KVPairs
	var meta *api.QueryMeta
	var err error
	retry := false

	c.runWithRetry(func() bool {
		conn := c.conn
		pairs, meta, err = conn.client.KV().List(prefix, nil)
		retry, err = c.checkAndRefresh(conn, err)
		return retry
	})

	return pairs, meta, err
}

func (c *consulClient) Acquire(p *api.KVPair, q *api.WriteOptions) (*api.WriteMeta, error) {
	var err error
	var meta *api.WriteMeta
	var ok bool

	retry := false
	c.runWithRetry(func() bool {
		conn := c.conn
		ok, meta, err = conn.client.KV().Acquire(p, nil)
		retry, err = c.checkAndRefresh(conn, err)
		return retry
	})

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

	c.runWithRetry(func() bool {
		conn := c.conn
		session, meta, err = conn.client.Session().Create(se, q)
		retry, err = c.checkAndRefresh(conn, err)
		return retry
	})

	return session, meta, err
}

func (c *consulClient) Destroy(id string, q *api.WriteOptions) (*api.WriteMeta, error) {
	return c.writeRetryFunc(func(conn *consulConnection) (*api.WriteMeta, error) {
		return conn.client.Session().Destroy(id, q)
	})
}

func (c *consulClient) Renew(id string, q *api.WriteOptions) (*api.SessionEntry, *api.WriteMeta, error) {
	var entry *api.SessionEntry
	var meta *api.WriteMeta
	var err error
	retry := false

	c.runWithRetry(func() bool {
		conn := c.conn
		entry, meta, err = conn.client.Session().Renew(id, q)
		retry, err = c.checkAndRefresh(conn, err)
		return retry
	})

	return entry, meta, err
}

func (c *consulClient) RenewPeriodic(
	initialTTL string,
	id string,
	q *api.WriteOptions,
	doneCh chan struct{},
) error {
	var err error
	retry := false

	c.runWithRetry(func() bool {
		conn := c.conn
		err = conn.client.Session().RenewPeriodic(initialTTL, id, nil, doneCh)
		retry, err = c.checkAndRefresh(conn, err)
		return retry
	})

	return err
}

func (c *consulClient) CreateMeta(id string, p *api.KVPair, q *api.WriteOptions) (*api.WriteMeta, bool, error) {
	var ok bool
	var meta *api.WriteMeta
	var err error
	for i := 0; i < c.refreshCount; i++ {
		conn := c.conn
		ok, meta, err = conn.client.KV().Acquire(p, q)
		if ok && err == nil {
			return nil, ok, err
		}
		if _, err := conn.client.Session().Destroy(p.Session, nil); err != nil {
			logrus.Error(err)
		}
		if _, err := c.Delete(id, nil); err != nil {
			logrus.Error(err)
		}
		if err != nil {
			if isConsulErrNeedingRetry(err) {
				if clientErr := c.Refresh(c.conn); clientErr != nil {
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
		conn := c.conn
		ok, meta, err = conn.client.KV().CAS(p, q)
		if err != nil && isConsulErrNeedingRetry(err) {
			retried = true
			if clientErr := c.Refresh(c.conn); clientErr != nil {
				return false, nil, clientErr
			} else {
				continue
			}

		} else if err != nil && isKeyIndexMismatchErr(err) && retried {
			kvPair, _, getErr := conn.client.KV().Get(id, nil)
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
		conn := c.conn
		ok, meta, err = conn.client.KV().DeleteCAS(p, q)
		if err != nil && isConsulErrNeedingRetry(err) {
			retried = true
			if clientErr := c.Refresh(c.conn); clientErr != nil {
				return false, nil, clientErr
			} else {
				continue
			}

		} else if err != nil && isKeyIndexMismatchErr(err) && retried {
			kvPair, _, getErr := conn.client.KV().Get(id, nil)
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
