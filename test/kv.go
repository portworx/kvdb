package test

import (
	"errors"
	"fmt"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/portworx/kvdb"
	"github.com/stretchr/testify/assert"
)

type watchData struct {
	t            *testing.T
	key          string
	otherKey     string
	stop         string
	localIndex   uint64
	updateIndex  uint64
	kvp          *kvdb.KVPair
	watchStopped bool
	iterations   int
	action       kvdb.KVAction
	writer       int32
	reader       int32
	whichKey     int32
}

// Run runs the test suite.
func Run(datastoreInit kvdb.DatastoreInit, t *testing.T) {
	kv, err := datastoreInit("pwx/test", nil, nil)
	if err != nil {
		t.Fatalf(err.Error())
	}
	snapshot(kv, t)
	get(kv, t)
	getInterface(kv, t)
	create(kv, t)
	createWithTTL(kv, t)
	update(kv, t)
	deleteKey(kv, t)
	deleteTree(kv, t)
	enumerate(kv, t)
	lock(kv, t)
	watchKey(kv, t)
	watchTree(kv, t)
	cas(kv, t)
}

// RunBasic runs the basic test suite.
func RunBasic(datastoreInit kvdb.DatastoreInit, t *testing.T) {
	kv, err := datastoreInit("pwx/test", nil, nil)
	if err != nil {
		t.Fatalf(err.Error())
	}
	get(kv, t)
	getInterface(kv, t)
	create(kv, t)
	update(kv, t)
	deleteKey(kv, t)
	deleteTree(kv, t)
	enumerate(kv, t)
	lockBasic(kv, t)
	snapshot(kv, t)
	// watchKey(kv, t)
	// watchTree(kv, t)
	// cas(kv, t)
}

func get(kv kvdb.Kvdb, t *testing.T) {
	fmt.Println("get")

	kvPair, err := kv.Get("DEADCAFE")
	assert.Error(t, err, "Expecting error value for non-existent value")

	key := "foo/docker"
	val := "great"
	defer func() {
		kv.Delete(key)
	}()

	kvPair, err = kv.Put(key, []byte(val), 0)
	assert.NoError(t, err, "Unexpected error in Put")

	kvPair, err = kv.Get(key)
	assert.NoError(t, err, "Failed in Get")

	assert.Equal(t, key, kvPair.Key, "Key mismatch in Get")
	assert.Equal(t, string(kvPair.Value), val, "value mismatch in Get")
}

func getInterface(kv kvdb.Kvdb, t *testing.T) {

	fmt.Println("getInterface")
	expected := struct {
		N int
		S string
	}{
		N: 10,
		S: "Ten",
	}

	actual := expected
	actual.N = 0
	actual.S = "zero"

	key := "DEADBEEF"
	_, err := kv.Delete(key)
	_, err = kv.Put(key, &expected, 0)
	assert.NoError(t, err, "Failed in Put")

	_, err = kv.GetVal(key, &actual)
	assert.NoError(t, err, "Failed in Get")

	assert.Equal(t, expected, actual, "Expected %#v but got %#v",
		expected, actual)
}

func create(kv kvdb.Kvdb, t *testing.T) {
	fmt.Println("create")

	key := "create/foo"
	kv.Delete(key)

	kvp, err := kv.Create(key, []byte("bar"), 0)
	assert.NoError(t, err, "Error on create")

	defer func() {
		kv.Delete(key)
	}()
	assert.Equal(t, kvp.Action, kvdb.KVCreate,
		"Expected action KVCreate, actual %v", kvp.Action)

	_, err = kv.Create(key, []byte("bar"), 0)
	assert.Error(t, err, "Create on existing key should have errored.")
}

func createWithTTL(kv kvdb.Kvdb, t *testing.T) {
	fmt.Println("create with ttl")
	key := "create/foo"
	kv.Delete(key)
	assert.NotNil(t, kv, "Default KVDB is not set")
	_, err := kv.Create(key, []byte("bar"), 1)
	if err != nil {
		// Consul does not support ttl less than 10
		assert.EqualError(t, err, kvdb.ErrTTLNotSupported.Error(), "ttl not supported")
		_, err := kv.Create(key, []byte("bar"), 20)
		assert.NoError(t, err, "Error on create")
		// Consul doubles the ttl value
		time.Sleep(time.Second * 20)
		_, err = kv.Get(key)
		assert.Error(t, err, "Expecting error value for expired value")
		
	} else {
		assert.NoError(t, err, "Error on create")
		time.Sleep(time.Second * 2)
		_, err = kv.Get(key)
		assert.Error(t, err, "Expecting error value for expired value")
	}
}

func update(kv kvdb.Kvdb, t *testing.T) {
	fmt.Println("update")

	key := "update/foo"
	kv.Delete(key)

	kvp, err := kv.Update(key, []byte("bar"), 0)
	assert.Error(t, err, "Update should error on non-existent key")

	defer func() {
		kv.Delete(key)
	}()

	kvp, err = kv.Create(key, []byte("bar"), 0)
	assert.NoError(t, err, "Unexpected error on create")

	kvp, err = kv.Update(key, []byte("bar"), 0)
	assert.NoError(t, err, "Unexpected error on update")

	assert.Equal(t, kvp.Action, kvdb.KVSet,
		"Expected action KVSet, actual %v", kvp.Action)
}

func deleteKey(kv kvdb.Kvdb, t *testing.T) {
	fmt.Println("deleteKey")

	key := "delete_key"
	_, err := kv.Delete(key)

	_, err = kv.Put(key, []byte("delete_me"), 0)
	assert.NoError(t, err, "Unexpected error on Put")

	_, err = kv.Get(key)
	assert.NoError(t, err, "Unexpected error on Get")

	_, err = kv.Delete(key)
	assert.NoError(t, err, "Unexpected error on Delete")

	_, err = kv.Get(key)
	assert.Error(t, err, "Get should fail on deleted key")

	_, err = kv.Delete(key)
	assert.Error(t, err, "Delete should fail on non existent key")
}

func deleteTree(kv kvdb.Kvdb, t *testing.T) {
	fmt.Println("deleteTree")

	prefix := "tree"
	keys := map[string]string{
		prefix + "/1cbc9a98-072a-4793-8608-01ab43db96c8": "bar",
		prefix + "/foo":                                  "baz",
	}

	for key, val := range keys {
		_, err := kv.Put(key, []byte(val), 0)
		assert.NoError(t, err, "Unexpected error on Put")
	}

	for key := range keys {
		_, err := kv.Get(key)
		assert.NoError(t, err, "Unexpected error on Get")
	}
	err := kv.DeleteTree(prefix)
	assert.NoError(t, err, "Unexpected error on DeleteTree")

	for key := range keys {
		_, err := kv.Get(key)
		assert.Error(t, err, "Get should fail on all keys after DeleteTree")
	}
}

func enumerate(kv kvdb.Kvdb, t *testing.T) {

	fmt.Println("enumerate")

	prefix := "enumerate"
	keys := map[string]string{
		prefix + "/1cbc9a98-072a-4793-8608-01ab43db96c8": "bar",
		prefix + "/foo":                                  "baz",
	}

	kv.DeleteTree(prefix)
	defer func() {
		kv.DeleteTree(prefix)
	}()

	for key, val := range keys {
		_, err := kv.Put(key, []byte(val), 0)
		assert.NoError(t, err, "Unexpected error on Put")
	}
	kvPairs, err := kv.Enumerate(prefix)
	assert.NoError(t, err, "Unexpected error on Enumerate")

	assert.Equal(t, len(kvPairs), len(keys),
		"Expecting %d keys under %s got: %d",
		len(keys), prefix, len(kvPairs))

	for i := range kvPairs {
		v, ok := keys[kvPairs[i].Key]
		assert.True(t, ok, "unexpected kvpair (%s)->(%s)",
			kvPairs[i].Key, kvPairs[i].Value)
		assert.Equal(t, v, string(kvPairs[i].Value),
			"Invalid kvpair (%s)->(%s) expect value %s",
			kvPairs[i].Key, kvPairs[i].Value, v)
	}
}

func snapshot(kv kvdb.Kvdb, t *testing.T) {

	fmt.Println("snapshot")

	prefix := "snapshot/"

	kv.DeleteTree(prefix)
	defer func() {
		kv.DeleteTree(prefix)
	}()

	inputData := make(map[string]string)
	inputDataVersion := make(map[string]uint64)
	key := "key"
	value := "bar"
	count := 100

	doneUpdate := make(chan bool, 2)
	updateFn := func(count int, v string) {
		for i := 0; i < count; i++ {
			suffix := strconv.Itoa(i)
			inputKey := prefix + key + suffix
			inputValue := v
			kv, err := kv.Put(inputKey, []byte(inputValue), 0)
			assert.NoError(t, err, "Unexpected error on Put")
			inputData[inputKey] = inputValue
			inputDataVersion[inputKey] = kv.KVDBIndex
		}
		doneUpdate <- true
	}

	updateFn(count, value)
	<-doneUpdate
	newValue := "bar2"
	go updateFn(50, newValue)

	snap, snapVersion, err := kv.Snapshot(prefix)
	assert.NoError(t, err, "Unexpected error on Snapshot")
	<-doneUpdate

	kvPairs, err := snap.Enumerate(prefix)
	assert.NoError(t, err, "Unexpected error on Enumerate")

	assert.Equal(t, len(kvPairs), count,
		"Expecting %d keys under %s got: %d, kv: %v",
		count, prefix, len(kvPairs), kvPairs)

	for i := range kvPairs {
		currValue, ok1 := inputData[kvPairs[i].Key]
		mapVersion, ok2 := inputDataVersion[kvPairs[i].Key]
		assert.True(t, ok1 && ok2, "unexpected kvpair (%s)->(%s)",
			kvPairs[i].Key, kvPairs[i].Value)
		expectedValue := value
		if mapVersion <= snapVersion {
			expectedValue = currValue
		}

		assert.Equal(t, expectedValue, string(kvPairs[i].Value),
			"Invalid kvpair %v (%s)->(%s) expect value %s"+
				" snap version: %v kvVersion: %v",
			i, kvPairs[i].Key, kvPairs[i].Value, expectedValue,
			snapVersion, mapVersion)
	}
}

func lock(kv kvdb.Kvdb, t *testing.T) {

	fmt.Println("lock")

	key := "locktest"
	kvPair, err := kv.Lock(key, 10)
	assert.NoError(t, err, "Unexpected error in lock")

	if kvPair == nil {
		return
	}

	// For consul unlock does not deal with the value part of the kvPair
	/*
		stash := *kvPair
		stash.Value = []byte("hoohah")
		fmt.Println("bad unlock")
		err = kv.Unlock(&stash)
		assert.Error(t, err, "Unlock should fail for bad KVPair")
	*/

	fmt.Println("unlock")
	err = kv.Unlock(kvPair)
	assert.NoError(t, err, "Unexpected error from Unlock")

	fmt.Println("relock")
	kvPair, err = kv.Lock(key, 3)
	assert.NoError(t, err, "Failed to lock after unlock")

	fmt.Println("reunlock")
	err = kv.Unlock(kvPair)
	assert.NoError(t, err, "Unexpected error from Unlock")

	fmt.Println("repeat lock once")
	kvPair, err = kv.Lock(key, 3)
	assert.NoError(t, err, "Failed to lock unlock")

	done := 0
	go func() {
		time.Sleep(time.Second * 10)
		done = 1
		err = kv.Unlock(kvPair)
		fmt.Println("repeat lock unlock once")
		assert.NoError(t, err, "Unexpected error from Unlock")
	}()
	fmt.Println("repeat lock lock twice")
	kvPair, err = kv.Lock(key, 3)
	assert.NoError(t, err, "Failed to lock")
	assert.Equal(t, done, 1, "Locked before unlock")
	fmt.Println("repeat lock unlock twice")
	err = kv.Unlock(kvPair)
	assert.NoError(t, err, "Unexpected error from Unlock")
}

func lockBasic(kv kvdb.Kvdb, t *testing.T) {

	fmt.Println("lock")

	key := "locktest"
	kvPair, err := kv.Lock(key, 100)
	assert.NoError(t, err, "Unexpected error in lock")

	if kvPair == nil {
		return
	}

	err = kv.Unlock(kvPair)
	assert.NoError(t, err, "Unexpected error from Unlock")

	kvPair, err = kv.Lock(key, 20)
	assert.NoError(t, err, "Failed to lock after unlock")

	err = kv.Unlock(kvPair)
	assert.NoError(t, err, "Unexpected error from Unlock")
}


func watchFn(
	prefix string,
	opaque interface{},
	kvp *kvdb.KVPair,
	err error,
) error {
	data := opaque.(*watchData)

	time.Sleep(100 * time.Millisecond)
	atomic.AddInt32(&data.reader, 1)
	if err != nil {
		assert.Equal(data.t, err, kvdb.ErrWatchStopped)
		data.watchStopped = true
		return err

	}
	fmt.Printf("+")

	// Doesn't work for ETCD because HTTP header contains Etcd-Index
	/*
			assert.True(data.t, kvp.KVDBIndex >= data.updateIndex,
				"KVDBIndex %v must be >= than updateIndex %v",
				kvp.KVDBIndex, data.updateIndex)


		assert.True(data.t, kvp.KVDBIndex > data.localIndex,
			"For Key (%v) : KVDBIndex %v must be > than localIndex %v action %v %v",
			kvp.Key, kvp.KVDBIndex, data.localIndex, kvp.Action, kvdb.KVCreate)

	*/
	assert.True(data.t, kvp.ModifiedIndex > data.localIndex,
		"For Key (%v) : ModifiedIndex %v must be > than localIndex %v",
		kvp.Key, kvp.ModifiedIndex, data.localIndex)

	data.localIndex = kvp.ModifiedIndex

	if data.whichKey == 0 {
		assert.Contains(data.t, data.otherKey, kvp.Key,
			"Bad kvpair key %s expecting %s with action %v",
			kvp.Key, data.key, kvp.Action)
	} else {
		assert.Contains(data.t, data.key, kvp.Key,
			"Bad kvpair key %s expecting %s with action %v",
			kvp.Key, data.key, kvp.Action)
	}

	assert.Equal(data.t, data.action, kvp.Action,
		"For Key (%v) : Expected action %v actual %v",
		kvp.Key, data.action, kvp.Action)

	if string(kvp.Value) == data.stop {
		return errors.New(data.stop)
	}

	return nil
}

func watchUpdate(kv kvdb.Kvdb, data *watchData) error {
	var err error
	var kvp *kvdb.KVPair

	data.reader, data.writer = 0, 0
	atomic.AddInt32(&data.writer, 1)
	// whichKey = 1 : key
	// whichKey = 0 : otherKey
	atomic.SwapInt32(&data.whichKey, 1)
	data.action = kvdb.KVCreate
	kvp, err = kv.Create(data.key, []byte("bar"), 0)
	for i := 0; i < data.iterations && err == nil; i++ {
		fmt.Printf("-")

		for data.writer != data.reader {
			time.Sleep(time.Millisecond * 100)
		}
		atomic.AddInt32(&data.writer, 1)
		data.action = kvdb.KVSet
		kvp, err = kv.Put(data.key, []byte("bar"), 0)

		data.updateIndex = kvp.KVDBIndex
		assert.NoError(data.t, err, "Unexpected error in Put")
	}

	for data.writer != data.reader {
		time.Sleep(time.Millisecond * 100)
	}
	atomic.AddInt32(&data.writer, 1)
	// Delete key
	data.action = kvdb.KVDelete
	kv.Delete(data.key)

	for data.writer != data.reader {
		time.Sleep(time.Millisecond * 100)
	}
	atomic.AddInt32(&data.writer, 1)

	atomic.SwapInt32(&data.whichKey, 0)
	data.action = kvdb.KVDelete
	// Delete otherKey
	kv.Delete(data.otherKey)

	for data.writer != data.reader {
		time.Sleep(time.Millisecond * 100)
	}
	atomic.AddInt32(&data.writer, 1)

	atomic.SwapInt32(&data.whichKey, 1)
	data.action = kvdb.KVCreate
	_, err = kv.Create(data.key, []byte(data.stop), 0)

	return err
}

func watchKey(kv kvdb.Kvdb, t *testing.T) {
	fmt.Println("watchKey")

	watchData := watchData{
		t:          t,
		key:        "tree/key1",
		otherKey:   "tree/otherKey1",
		stop:       "stop",
		iterations: 2,
	}

	kv.Delete(watchData.key)
	kv.Delete(watchData.otherKey)
	// First create a key. We should not get update for this create.
	_, err := kv.Create(watchData.otherKey, []byte("bar"), 0)

	err = kv.WatchKey(watchData.otherKey, 0, &watchData, watchFn)
	if err != nil {
		fmt.Printf("Cannot test watchKey: %v\n", err)
		return
	}

	err = kv.WatchKey(watchData.key, 0, &watchData, watchFn)
	if err != nil {
		fmt.Printf("Cannot test watchKey: %v\n", err)
		return
	}

	go watchUpdate(kv, &watchData)

	for watchData.watchStopped == false {
		time.Sleep(time.Millisecond * 100)
	}

	// Stop the second watch
	atomic.SwapInt32(&watchData.whichKey, 0)
	watchData.action = kvdb.KVCreate
	_, err = kv.Create(watchData.otherKey, []byte(watchData.stop), 0)
}

func randomUpdate(kv kvdb.Kvdb, w *watchData) {
	for w.watchStopped == false {
		kv.Put("randomKey", []byte("bar"), 0)
		time.Sleep(time.Millisecond * 80)
	}
}

func watchTree(kv kvdb.Kvdb, t *testing.T) {
	fmt.Println("watchTree")

	tree := "tree"

	watchData := watchData{
		t:          t,
		key:        tree + "/key",
		otherKey:   tree + "/otherKey",
		stop:       "stop",
		iterations: 2,
	}
	_, err := kv.Delete(watchData.key)
	_, err = kv.Delete(watchData.otherKey)

	// First create a tree to watch for. We should not get update for this create.
	_, err = kv.Create(watchData.otherKey, []byte("bar"), 0)
	err = kv.WatchTree(tree, 0, &watchData, watchFn)
	if err != nil {
		fmt.Printf("Cannot test watchKey: %v\n", err)
		return
	}

	// Sleep for sometime before calling the watchUpdate go routine.
	time.Sleep(time.Millisecond * 100)

	go randomUpdate(kv, &watchData)
	go watchUpdate(kv, &watchData)

	for watchData.watchStopped == false {
		time.Sleep(time.Millisecond * 100)
	}
}

func cas(kv kvdb.Kvdb, t *testing.T) {
	fmt.Println("cas")

	key := "foo/docker"
	val := "great"
	defer func() {
		kv.Delete(key)
	}()

	kvPair, err := kv.Put(key, []byte(val), 0)
	assert.NoError(t, err, "Unxpected error in Put")

	kvPair, err = kv.Get(key)
	assert.NoError(t, err, "Failed in Get")

	fmt.Println("cas badval")
	_, err = kv.CompareAndSet(kvPair, kvdb.KVFlags(0), []byte("badval"))
	assert.Error(t, err, "CompareAndSet should fail on an incorrect previous value")
	//assert.EqualError(t, err, kvdb.ErrValueMismatch.Error(), "CompareAndSet should return value mismatch error")

	kvPair.ModifiedIndex++
	_, err = kv.CompareAndSet(kvPair, kvdb.KVModifiedIndex, nil)
	assert.Error(t, err, "CompareAndSet should fail on an incorrect modified index")

	kvPair.ModifiedIndex--
	kvPair, err = kv.CompareAndSet(kvPair, kvdb.KVModifiedIndex, nil)
	assert.NoError(t, err, "CompareAndSet should succeed on an correct modified index")

	kvPairNew, err := kv.CompareAndSet(kvPair, kvdb.KVFlags(0), []byte(val))
	if err != nil {
		// consul does not handle this kind of compare and set
		assert.EqualError(t, err, kvdb.ErrNotSupported.Error(), "Invalid error returned : %v", err)
	} else {
		assert.NoError(t, err, "CompareAndSet should succeed on an correct value")
	}

	if kvPairNew != nil {
		kvPair = kvPairNew
	}

	kvPair, err = kv.CompareAndSet(kvPair, kvdb.KVModifiedIndex, []byte(val))
	assert.NoError(t, err, "CompareAndSet should succeed on an correct value and modified index")
}
