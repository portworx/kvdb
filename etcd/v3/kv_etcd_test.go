package etcdv3

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"go.etcd.io/etcd/etcdserver"
	"go.etcd.io/etcd/etcdserver/api/v3rpc/rpctypes"
	"github.com/portworx/kvdb"
	"github.com/portworx/kvdb/etcd/common"
	"github.com/portworx/kvdb/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestAll(t *testing.T) {
	test.Run(New, t, common.TestStart, common.TestStop)
	// Run the basic tests with an authenticated etcd
	// Uncomment if you have an auth enabled etcd setup. Checkout the test/kv.go for options
	//test.RunAuth(New, t)
	test.RunControllerTests(New, t)
}

func TestIsRetryNeeded(t *testing.T) {
	fn := "TestIsRetryNeeded"
	key := "test"
	retryCount := 1

	// context.DeadlineExceeded
	retry, err := isRetryNeeded(context.DeadlineExceeded, fn, key, retryCount)
	assert.EqualError(t, context.DeadlineExceeded, err.Error(), "Unexpcted error")
	assert.True(t, retry, "Expected a retry")

	// etcdserver.ErrTimeout
	retry, err = isRetryNeeded(etcdserver.ErrTimeout, fn, key, retryCount)
	assert.EqualError(t, etcdserver.ErrTimeout, err.Error(), "Unexpcted error")
	assert.True(t, retry, "Expected a retry")

	// etcdserver.ErrUnhealthy
	retry, err = isRetryNeeded(etcdserver.ErrUnhealthy, fn, key, retryCount)
	assert.EqualError(t, etcdserver.ErrUnhealthy, err.Error(), "Unexpcted error")
	assert.True(t, retry, "Expected a retry")

	// etcd is sending the error ErrTimeout over grpc instead of the actual ErrGRPCTimeout
	// hence isRetryNeeded cannot do an actual error comparison
	retry, err = isRetryNeeded(fmt.Errorf("etcdserver: request timed out"), fn, key, retryCount)
	assert.True(t, retry, "Expected a retry")

	// rpctypes.ErrGRPCTimeout
	retry, err = isRetryNeeded(rpctypes.ErrGRPCTimeout, fn, key, retryCount)
	assert.EqualError(t, rpctypes.ErrGRPCTimeout, err.Error(), "Unexpcted error")
	assert.True(t, retry, "Expected a retry")

	// rpctypes.ErrGRPCNoLeader
	retry, err = isRetryNeeded(rpctypes.ErrGRPCNoLeader, fn, key, retryCount)
	assert.EqualError(t, rpctypes.ErrGRPCNoLeader, err.Error(), "Unexpcted error")
	assert.True(t, retry, "Expected a retry")

	// rpctypes.ErrGRPCEmptyKey
	retry, err = isRetryNeeded(rpctypes.ErrGRPCEmptyKey, fn, key, retryCount)
	assert.EqualError(t, kvdb.ErrNotFound, err.Error(), "Unexpcted error")
	assert.False(t, retry, "Expected no retry")

	// etcd v3.2.x uses following grpc error format
	grpcErr := grpc.Errorf(codes.Unavailable, "desc = some grpc error")
	retry, err = isRetryNeeded(grpcErr, fn, key, retryCount)
	assert.EqualError(t, grpcErr, err.Error(), "Unexpcted error")
	assert.True(t, retry, "Expected a retry")

	// etcd v3.3.x uses the following grpc error format
	grpcErr = status.New(codes.Unavailable, "desc = some grpc error").Err()
	retry, err = isRetryNeeded(grpcErr, fn, key, retryCount)
	assert.EqualError(t, grpcErr, err.Error(), "Unexpcted error")
	assert.True(t, retry, "Expected a retry")

	// grpc error of ContextDeadlineExceeded
	gErr := status.New(codes.DeadlineExceeded, "context deadline exceeded").Err()
	retry, err = isRetryNeeded(gErr, fn, key, retryCount)
	assert.EqualError(t, gErr, err.Error(), "Unexpcted error")
	assert.True(t, retry, "Expected a retry")

	retry, err = isRetryNeeded(kvdb.ErrNotFound, fn, key, retryCount)
	assert.EqualError(t, kvdb.ErrNotFound, err.Error(), "Unexpcted error")
	assert.False(t, retry, "Expected no retry")

	retry, err = isRetryNeeded(kvdb.ErrValueMismatch, fn, key, retryCount)
	assert.EqualError(t, kvdb.ErrValueMismatch, err.Error(), "Unexpcted error")
	assert.False(t, retry, "Expected no retry")

}

func TestCasWithRestarts(t *testing.T) {
	fmt.Println("casWithRestarts")
	testFn := func(lockChan chan int, kv kvdb.Kvdb, kvPair *kvdb.KVPair, val string) {
		_, err := kv.CompareAndSet(kvPair, kvdb.KVFlags(0), []byte(val))
		assert.NoError(t, err, "CompareAndSet should succeed on an correct value")
		lockChan <- 1
	}
	testWithRestarts(t, testFn)

}

func TestCadWithRestarts(t *testing.T) {
	fmt.Println("cadWithRestarts")
	testFn := func(lockChan chan int, kv kvdb.Kvdb, kvPair *kvdb.KVPair, val string) {
		_, err := kv.CompareAndDelete(kvPair, kvdb.KVFlags(0))
		assert.NoError(t, err, "CompareAndDelete should succeed on an correct value")
		lockChan <- 1
	}
	testWithRestarts(t, testFn)
}

func testWithRestarts(t *testing.T, testFn func(chan int, kvdb.Kvdb, *kvdb.KVPair, string)) {
	key := "foo/cadCasWithRestarts"
	val := "great"
	err := common.TestStart(true)
	assert.NoError(t, err, "Unable to start kvdb")
	kv := newKv(t, 0)

	defer func() {
		kv.DeleteTree(key)
	}()

	kvPair, err := kv.Put(key, []byte(val), 0)
	assert.NoError(t, err, "Unxpected error in Put")

	kvPair, err = kv.Get(key)
	assert.NoError(t, err, "Failed in Get")
	fmt.Println("stopping kvdb")
	err = common.TestStop()
	assert.NoError(t, err, "Unable to stop kvdb")

	lockChan := make(chan int)
	go func() {
		testFn(lockChan, kv, kvPair, val)
	}()

	fmt.Println("starting kvdb")
	err = common.TestStart(false)
	assert.NoError(t, err, "Unable to start kvdb")
	select {
	case <-time.After(10 * time.Second):
		assert.Fail(t, "Unable to take a lock whose session is expired")
	case <-lockChan:
	}

}

func TestKeys(t *testing.T) {
	err := common.TestStart(true)
	require.NoError(t, err, "Unable to start kvdb")
	kv := newKv(t, 0)
	require.NotNil(t, kv)

	defer func() {
		t.Log("Stopping kvdb")
		common.TestStop()
	}()

	data := []struct {
		key, val string
	}{
		{"foo/keys/key1", "val1"},
		{"foo/keys/key2", "val2"},
		{"foo/keys/key3d/key3", "val3"},
		{"foo/keys/key3d/key3b", "val3b"},
	}

	// load the data
	for i, td := range data {
		_, err := kv.Put(td.key, []byte(td.val), 0)
		assert.NoError(t, err, "Unxpected error in Put on test# %d : %v", i+1, td)
	}

	// check the Keys()
	results := []struct {
		input    string
		expected []string
	}{
		{"", []string{"foo"}},
		{"non-existent", []string{}},
		{"foo", []string{"keys"}},
		{"foo/keys", []string{"key1", "key2", "key3d"}},
		{"foo/keys/non-existent", []string{}},
		{"foo/keys/key3", []string{}},
		{"foo/keys/key3d", []string{"key3", "key3b"}},
	}
	for i, td := range results {
		res, err := kv.Keys(td.input, "/")
		assert.NoError(t, err, "Unexpected error on test# %d : %v", i+1, td)
		assert.Equal(t, td.expected, res, "Unexpected result on test# %d : %v", i+1, td)
	}
	for i, td := range results {
		res, err := kv.Keys(td.input, "")
		assert.NoError(t, err, "Unexpected error on test# %d : %v", i+1, td)
		assert.Equal(t, td.expected, res, "Unexpected result on test# %d : %v", i+1, td)
	}

	results = results[1:] // the first test will not work on 3rd pass -- let's skip it
	for i, td := range results {
		res, err := kv.Keys(td.input+"/", "/")
		assert.NoError(t, err, "Unexpected error on test# %d : %v", i+1, td)
		assert.Equal(t, td.expected, res, "Unexpected result on test# %d : %v", i+1, td)
	}

	// using different separator ('e')
	results = []struct {
		input    string
		expected []string
	}{
		{"", []string{"foo/k"}},
		{"foo/keys/non-existent", []string{}},
		{"foo/ke", []string{"ys/k"}},
		{"foo/k", []string{"ys/k"}},
		{"foo/keys/ke", []string{"y1", "y2", "y3d/k"}},
		{"foo/keys/k", []string{"y1", "y2", "y3d/k"}},
	}
	for i, td := range results {
		res, err := kv.Keys(td.input, "e")
		assert.NoError(t, err, "Unexpected error on test# %d : %v", i+1, td)
		assert.Equal(t, td.expected, res, "Unexpected result on test# %d : %v", i+1, td)
	}
}

func TestUnlock(t *testing.T) {
	err := common.TestStart(true)
	require.NoError(t, err, "Unable to start kvdb")
	t.Log("Started kvdb")

	kv := newKv(t, 2*time.Second)
	require.NotNil(t, kv)

	defer func() {
		t.Log("Stopping kvdb")
		common.TestStop()
	}()

	data := []struct {
		key, val string
	}{
		{"foo/keys/key1", "val1"},
		{"foo/keys/key2", "val2"},
		{"foo/keys/key3", "val3"},
	}

	// load the data
	for i, td := range data {
		_, err := kv.Put(td.key, []byte(td.val), 0)
		require.NoError(t, err, "Unxpected error in Put on test# %d : %v", i+1, td)
	}
	t.Log("loaded data")

	var wg sync.WaitGroup
	for i := 0; i < 30; i++ {
		key := fmt.Sprintf("foo/keys/key%d", i%3+1)
		wg.Add(1)
		go func() {
			defer wg.Done()
			kvp, err := kv.Lock(key)
			require.NoError(t, err, "Failed to lock key %s", key)
			t.Log("locked key", key)

			dur := time.Duration(2000+rand.Intn(2000)) * time.Millisecond
			time.Sleep(dur)

			err = kv.Unlock(kvp)
			require.NoError(t, err, "Failed to unlock key %s", key)
			t.Log("unlocked key", key)

			dur = time.Duration(rand.Intn(500)) * time.Millisecond
			time.Sleep(dur)

			// unlock again (double unlock is not recommended but can happen)
			err = kv.Unlock(kvp)
			require.NoError(t, err, "Failed to double unlock key %s", key)
			t.Log("unlocked key again", key)
		}()
	}
	wg.Wait()
	// give some time for refreshLocks to finish
	time.Sleep(time.Second)
}

func newKv(t *testing.T, lockRefreshDuration time.Duration) kvdb.Kvdb {
	kv, err := New("pwx/test", nil, nil, func(err error, format string, args ...interface{}) {
		t.Fatalf(format, args...)
	})
	if err != nil {
		t.Fatalf(err.Error())
	}
	if lockRefreshDuration != 0 {
		et := kv.(*etcdKV)
		et.lockRefreshDuration = lockRefreshDuration
	}
	return kv
}
