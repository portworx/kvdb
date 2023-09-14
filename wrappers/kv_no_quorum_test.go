package wrappers

import (
	"testing"
	"time"

	"github.com/portworx/kvdb"
	"github.com/stretchr/testify/require"
)

// TestKVNoQuorumLock tests all functions related to kvp and locks
func TestKVNoQuorumLock(t *testing.T) {
	wrapper := noKvdbQuorumWrapper{}

	kv, err := wrapper.LockWithID("key", "id")
	require.Error(t, err)
	require.ErrorIs(t, err, kvdb.ErrNoQuorum)
	require.Nil(t, kv)

	kv, err = wrapper.Lock("key")
	require.Error(t, err)
	require.ErrorIs(t, err, kvdb.ErrNoQuorum)
	require.Nil(t, kv)

	kv, err = wrapper.LockWithTimeout("key", "id", time.Hour, time.Hour)
	require.Error(t, err)
	require.ErrorIs(t, err, kvdb.ErrNoQuorum)
	require.Nil(t, kv)

	var kvp *kvdb.KVPair
	err = wrapper.Unlock(kvp)
	require.Error(t, err)
	require.ErrorIs(t, err, kvdb.ErrEmptyValue)
}
