package test

import (
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/portworx/kvdb"
	"github.com/stretchr/testify/require"
)

const (
	urlPrefix = "http://"
	localhost = "localhost"
)

var (
	names         = []string{"infra0", "infra1", "infra2", "infra3", "infra4"}
	clientUrls    = []string{"http://127.0.0.1:20379", "http://127.0.0.2:21379", "http://127.0.0.3:22379", "http://127.0.0.4:23379", "http://127.0.0.5:24379"}
	peerPorts     = []string{"20380", "21380", "22380", "23380", "24380"}
	dataDirs      = []string{"/tmp/node0", "/tmp/node1", "/tmp/node2", "/tmp/node3", "/tmp/node4"}
	cmds          map[int]*exec.Cmd
	firstMemberID uint64
)

// RunControllerTests is a test suite for kvdb controller APIs
func RunControllerTests(datastoreInit kvdb.DatastoreInit, t *testing.T) {
	cleanup()
	// Initialize node 0
	cmds = make(map[int]*exec.Cmd)
	index := 0
	initCluster := make(map[string][]string)
	peerURL := urlPrefix + localhost + ":" + peerPorts[index]
	initCluster[names[index]] = []string{peerURL}
	cmd, err := startEtcd(index, initCluster, "new")
	if err != nil {
		t.Fatalf(err.Error())
	}
	cmds[index] = cmd
	kv, err := datastoreInit("pwx/test", clientUrls, nil, fatalErrorCb())
	if err != nil {
		cmd.Process.Kill()
		t.Fatalf(err.Error())
	}

	memberList, err := kv.ListMembers()
	require.NoError(t, err, "error on ListMembers")
	require.Equal(t, 1, len(memberList), "incorrect number of members")
	for id := range memberList {
		firstMemberID = id
		break
	}
	testAddMember(kv, t)
	testRemoveMember(kv, t)
	testReAddAndRemoveByMemberID(kv, t)
	testUpdateMember(kv, t)
	testMemberStatus(kv, t)
	testDefrag(kv, t)
	testGetSetEndpoints(kv, t)
	controllerLog("Stopping all etcd processes")
	for _, cmd := range cmds {
		cmd.Process.Kill()
	}
}

func testAddMember(kv kvdb.Kvdb, t *testing.T) {
	controllerLog("testAddMember")
	// Add node 1
	index := 1
	controllerLog("Adding node 1")
	initCluster, err := kv.AddMember(localhost, peerPorts[index], names[index])
	require.NoError(t, err, "Error on AddMember")
	require.Equal(t, 2, len(initCluster), "Init Cluster length does not match")

	// Check for unstarted members
	memberList, err := kv.ListMembers()
	require.NoError(t, err, "error on ListMembers")
	require.Equal(t, len(memberList), 2, "incorrect number of members")

	for memberID, m := range memberList {
		if memberID != firstMemberID {
			require.Equal(t, len(m.ClientUrls), 0, "Unexpected no. of client urls on unstarted member")
			require.False(t, m.IsHealthy, "Unexpected health of unstarted member")
			require.Empty(t, m.Name, "expected name to be empty")
			require.False(t, m.HasStarted, "expected member to be unstarted")
			require.Equal(t, len(m.PeerUrls), 1, "peerURLs should be set for unstarted members")
			require.Equal(t, m.DbSize, int64(0), "db size should be 0")
		} else {
			require.Equal(t, len(m.ClientUrls), 1, "clientURLs should be set for started members")
			require.True(t, m.IsHealthy, "expected member to be healthy")
			require.NotEmpty(t, m.Name, "expected name")
			require.True(t, m.HasStarted, "expected member to be started")
			require.Equal(t, len(m.PeerUrls), 1, "peerURLs should be set for started members")
			require.NotEqual(t, m.DbSize, int64(0), "db size should not be 0")
		}
	}

	cmd, err := startEtcd(index, initCluster, "existing")
	require.NoError(t, err, "Error on start etcd")
	cmds[index] = cmd

	// Check the list again after starting the second member.
	memberList, err = kv.ListMembers()
	require.NoError(t, err, "Error on ListMembers")
	require.Equal(t, 2, len(memberList), "List returned different length of cluster")

	for _, m := range memberList {
		require.True(t, m.IsHealthy, "expected member to be healthy")
		require.NotEmpty(t, m.Name, "expected name")
		require.True(t, m.HasStarted, "expected member to be started")
		require.Equal(t, len(m.PeerUrls), 1, "peerURLs should be set for started members")
		require.Equal(t, len(m.ClientUrls), 1, "clientURLs should be set for started members")
		require.NotEqual(t, m.DbSize, 0, "db size should not be 0")
	}
}

func testRemoveMember(kv kvdb.Kvdb, t *testing.T) {
	controllerLog("testRemoveMember")
	// Add node 2
	index := 2
	controllerLog("Adding node 2")
	initCluster, err := kv.AddMember(localhost, peerPorts[index], names[index])
	require.NoError(t, err, "Error on AddMember")
	require.Equal(t, 3, len(initCluster), "Init Cluster length does not match")
	cmd, err := startEtcd(index, initCluster, "existing")
	require.NoError(t, err, "Error on start etcd")
	cmds[index] = cmd
	// Check the list returned
	list, err := kv.ListMembers()
	require.NoError(t, err, "Error on ListMembers")
	require.Equal(t, 3, len(list), "List returned different length of cluster")

	// Before removing all endpoints should be set
	require.Equal(t, len(clientUrls), len(kv.GetEndpoints()), "unexpected endpoints")

	// Remove node 1
	index = 1
	controllerLog("Removing node 1")
	err = kv.RemoveMember(names[index], localhost)
	require.NoError(t, err, "Error on RemoveMember")

	// Only 2 endpoints should be set and the third one should have been removed
	require.Equal(t, 2, len(kv.GetEndpoints()), "unexpected endpoints")
	for _, actualEndpoint := range kv.GetEndpoints() {
		require.NotEqual(t, actualEndpoint, clientUrls[index], "removed member should not be present")
	}

	cmd, _ = cmds[index]
	cmd.Process.Kill()
	delete(cmds, index)
	// Check the list returned
	list, err = kv.ListMembers()
	require.NoError(t, err, "Error on ListMembers")
	require.Equal(t, 2, len(list), "List returned different length of cluster")

	// Remove an already removed node
	index = 1
	controllerLog("Removing node 1")
	err = kv.RemoveMember(names[index], localhost)
	require.NoError(t, err, "Error on RemoveMember")
}

func testReAddAndRemoveByMemberID(kv kvdb.Kvdb, t *testing.T) {
	controllerLog("testReAddAndRemoveByMemberID")

	// Add node 1 back
	node1Index := 1
	controllerLog("Re-adding node 1")
	// For re-adding we need to delete the data-dir of this member
	os.RemoveAll(dataDirs[node1Index])
	initCluster, err := kv.AddMember(localhost, peerPorts[node1Index], names[node1Index])
	require.NoError(t, err, "Error on AddMember")
	require.Equal(t, 3, len(initCluster), "Init Cluster length does not match")
	cmd, err := startEtcd(node1Index, initCluster, "existing")
	require.NoError(t, err, "Error on start etcd")
	cmds[node1Index] = cmd

	// Check the list returned
	list, err := kv.ListMembers()
	require.NoError(t, err, "Error on ListMembers")
	require.Equal(t, 3, len(list), "List returned different length of cluster")

	// Remove node 1
	var removeMemberID uint64
	for memberID, member := range list {
		if member.Name == names[node1Index] {
			removeMemberID = memberID
			break
		}
	}
	require.NotEqual(t, removeMemberID, 0, "unexpected memberID")

	// Remove on non-existent member should succeed
	err = kv.RemoveMemberByID(12345)
	require.NoError(t, err, "unexpected error on removing a non-existent member")

	err = kv.RemoveMemberByID(removeMemberID)
	require.NoError(t, err, "unexpected error on remove")

	// Only 2 endpoints should be set and the third one should have been removed
	require.Equal(t, 2, len(kv.GetEndpoints()), "unexpected endpoints")
	for _, actualEndpoint := range kv.GetEndpoints() {
		require.NotEqual(t, actualEndpoint, clientUrls[node1Index], "removed member should not be present")
	}

	// Kill the old etcd process
	cmd, _ = cmds[node1Index]
	cmd.Process.Kill()
	delete(cmds, node1Index)

	// Add node 1 back
	controllerLog("Re-adding node 1 again")
	// For re-adding we need to delete the data-dir of this member
	os.RemoveAll(dataDirs[node1Index])
	initCluster, err = kv.AddMember(localhost, peerPorts[node1Index], names[node1Index])
	require.NoError(t, err, "Error on AddMember")
	require.Equal(t, 3, len(initCluster), "Init Cluster length does not match")
	cmd, err = startEtcd(node1Index, initCluster, "existing")
	require.NoError(t, err, "Error on start etcd")
	cmds[node1Index] = cmd

}

func testUpdateMember(kv kvdb.Kvdb, t *testing.T) {
	controllerLog("testUpdateMember")

	// Stop node 1
	index := 1
	cmd, _ := cmds[index]
	cmd.Process.Kill()
	delete(cmds, index)
	// Change the port
	peerPorts[index] = "33380"

	// Update the member
	initCluster, err := kv.UpdateMember(localhost, peerPorts[index], names[index])
	require.NoError(t, err, "Error on UpdateMember")
	require.Equal(t, 3, len(initCluster), "Initial cluster length does not match")
	cmd, err = startEtcd(index, initCluster, "existing")
	require.NoError(t, err, "Error on start etcd")
	cmds[index] = cmd

	list, err := kv.ListMembers()
	require.NoError(t, err, "Error on ListMembers")
	require.Equal(t, 3, len(list), "List returned different length of cluster")

	// Update an invalid member
	_, err = kv.UpdateMember(localhost, peerPorts[index], "foobar")
	require.EqualError(t, kvdb.ErrMemberDoesNotExist, err.Error(), "Unexpected error on UpdateMember")
}

func testDefrag(kv kvdb.Kvdb, t *testing.T) {
	controllerLog("testDefrag")

	// Run defrag with 0 timeout
	index := 1
	err := kv.Defragment(clientUrls[index], 0)
	require.NoError(t, err, "Unexpected error on Defragment")

	// Run defrag with 60 timeout
	index = 4
	err = kv.Defragment(clientUrls[index], 60)
	require.NoError(t, err, "Unexpected error on Defragment")
}

func testMemberStatus(kv kvdb.Kvdb, t *testing.T) {
	controllerLog("testMemberStatus")

	index := 3
	controllerLog("Adding node 3")
	initCluster, err := kv.AddMember(localhost, peerPorts[index], names[index])
	require.NoError(t, err, "Error on AddMember")
	cmd, err := startEtcd(index, initCluster, "existing")
	require.NoError(t, err, "Error on start etcd")
	cmds[index] = cmd

	// Wait for some time for etcd to detect a node offline
	time.Sleep(5 * time.Second)

	index = 4
	controllerLog("Adding node 4")
	initCluster, err = kv.AddMember(localhost, peerPorts[index], names[index])
	require.NoError(t, err, "Error on AddMember")
	cmd, err = startEtcd(index, initCluster, "existing")
	require.NoError(t, err, "Error on start etcd")
	cmds[index] = cmd

	// Wait for some time for etcd to detect a node offline
	time.Sleep(5 * time.Second)

	// Stop node 2
	stoppedIndex := 2
	cmd, _ = cmds[stoppedIndex]
	cmd.Process.Kill()
	delete(cmds, stoppedIndex)

	// Stop node 3
	stoppedIndex2 := 3
	cmd, _ = cmds[stoppedIndex2]
	cmd.Process.Kill()
	delete(cmds, stoppedIndex2)

	// Wait for some time for etcd to detect a node offline
	time.Sleep(5 * time.Second)

	numOfGoroutines := 10
	var wg sync.WaitGroup
	wg.Add(numOfGoroutines)

	checkMembers := func(index string, wait int) {
		defer wg.Done()
		// Add a sleep so that all go routines run just around the same time
		time.Sleep(time.Duration(wait) * time.Second)
		controllerLog("Listing Members for goroutine no. " + index)
		list, err := kv.ListMembers()
		require.NoError(t, err, "%v: Error on ListMembers", index)
		require.Equal(t, 5, len(list), "%v: List returned different length of cluster", index)

		for _, m := range list {
			if m.Name == names[stoppedIndex] || m.Name == names[stoppedIndex2] {
				require.Equal(t, len(m.ClientUrls), 0, "%v: Unexpected no. of client urls on down member", index)
				require.False(t, m.IsHealthy, "%v: Unexpected health of down member", index)
			} else {
				require.True(t, m.IsHealthy, "%v: Expected member %v to be healthy", index, m.Name)
			}
		}
		fmt.Println("checkMembers done for ", index)
	}
	for i := 0; i < numOfGoroutines; i++ {
		go checkMembers(strconv.Itoa(i), numOfGoroutines-1)
	}
	c := make(chan struct{})
	go func() {
		wg.Wait()
		close(c)
	}()

	select {
	case <-c:
		return
	case <-time.After(10 * time.Minute):
		t.Fatalf("testMemberStatus timeout")
	}
}

func testGetSetEndpoints(kv kvdb.Kvdb, t *testing.T) {
	err := kv.SetEndpoints(clientUrls)
	require.NoError(t, err, "Unexpected error on SetEndpoints")
	endpoints := kv.GetEndpoints()
	require.Equal(t, len(endpoints), len(clientUrls), "Unexpected no. of endpoints")

	subsetUrls := clientUrls[1:]

	err = kv.SetEndpoints(subsetUrls)
	require.NoError(t, err, "Unexpected error on SetEndpoints")
	endpoints = kv.GetEndpoints()
	require.Equal(t, len(endpoints), len(subsetUrls), "Unexpected no. of endpoints")

}

func startEtcd(index int, initCluster map[string][]string, initState string) (*exec.Cmd, error) {
	peerURL := urlPrefix + localhost + ":" + peerPorts[index]
	clientURL := clientUrls[index]
	initialCluster := ""
	for name, ip := range initCluster {
		initialCluster = initialCluster + name + "=" + ip[0] + ","
	}
	fmt.Println("Starting etcd for node ", index, "with initial cluster: ", initialCluster)
	initialCluster = strings.TrimSuffix(initialCluster, ",")
	etcdArgs := []string{
		"--name=" +
			names[index],
		"--initial-advertise-peer-urls=" +
			peerURL,
		"--listen-peer-urls=" +
			peerURL,
		"--listen-client-urls=" +
			clientURL,
		"--advertise-client-urls=" +
			clientURL,
		"--initial-cluster=" +
			initialCluster,
		"--data-dir=" +
			dataDirs[index],
		"--initial-cluster-state=" +
			initState,
	}

	// unset env that can prevent etcd startup
	os.Unsetenv("ETCD_LISTEN_CLIENT_URLS")
	os.Unsetenv("ETCDCTL_API")

	cmd := exec.Command("/tmp/test-etcd/etcd", etcdArgs...)
	cmd.Stdout = ioutil.Discard
	cmd.Stderr = ioutil.Discard
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("Failed to run %v(%v) : %v",
			names[index], etcdArgs, err.Error())
	}
	// XXX: Replace with check for etcd is up
	time.Sleep(10 * time.Second)
	return cmd, nil
}

func cleanup() {
	for _, dir := range dataDirs {
		os.RemoveAll(dir)
		os.MkdirAll(dir, 0777)
	}
}

func controllerLog(log string) {
	fmt.Println("--------------------")
	fmt.Println(log)
	fmt.Println("--------------------")
}
