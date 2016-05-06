package etcd

import (
	"testing"

	"github.com/portworx/kvdb/test"
)

func TestAll(t *testing.T) {
	test.Run(New, t)
}
