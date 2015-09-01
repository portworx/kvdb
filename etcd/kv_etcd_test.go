package etcd

import (
	"testing"

	"github.com/portworx/kvdb"
	"github.com/portworx/kvdb/test"
)

func TestAll(t *testing.T) {
	kv, err := kvdb.New(Name, "pwx/test", nil, nil)
	if err != nil {
		t.Fatalf("Failed to initialize KVDB: %v", err)
	}

	err = kvdb.SetInstance(kv)
	if err != nil {
		t.Fatalf("Failed to set instance: %v", err)
	}

	test.Run(t)
}
