package test

import (
	"fmt"
	"strings"
	"os"
	"os/exec"
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
	names = []string{"infra0", "infra1", "infra2"}
	clientUrls = []string{"http://127.0.0.1:20379", "http://127.0.0.1:21379", "http://127.0.0.1:22379"}
	peerPorts = []string{"20380", "21380", "22380"}
	dataDirs = []string{"/tmp/node0", "/tmp/node1", "/tmp/node2"}
	cmds map[int]*exec.Cmd
)

func RunControllerTests(datastoreInit kvdb.DatastoreInit, t *testing.T) {
	cleanup()
	// Initialize node 0
	cmds = make(map[int]*exec.Cmd)
	index := 0
	initCluster := make(map[string][]string)
	peerUrl := urlPrefix + localhost + ":" + peerPorts[index]
	initCluster[names[index]] = []string{peerUrl}
	cmd, err := startEtcd(index, initCluster, "new")
	if err != nil {
		t.Fatalf(err.Error())
	}
	cmds[index] = cmd
	kv, err := datastoreInit("pwx/test", []string{clientUrls[index]}, nil, fatalErrorCb())
	if err != nil {
		cmd.Process.Kill()
		t.Fatalf(err.Error())
	}

	testAddMember(kv, t)
	testRemoveMember(kv, t)
	for _, cmd := range cmds {
		cmd.Process.Kill()
	}
}

func testAddMember(kv kvdb.Kvdb, t *testing.T) {
	fmt.Println("testAddMember")
	// Add node 1
	index := 1
	initCluster, err := kv.AddMember(localhost, peerPorts[index], names[index])
	require.NoError(t, err, "Error on AddMember")
	require.Equal(t, 2, len(initCluster), "Init Cluster length does not match")
	cmd, err := startEtcd(index, initCluster, "existing")
	require.NoError(t, err, "Error on start etcd")
	cmds[index] = cmd

	// Check the list returned
	list, err := kv.ListMembers()
	require.NoError(t, err, "Error on ListMembers")
	require.Equal(t, 2, len(list), "List returned different length of cluster")
}

func testRemoveMember(kv kvdb.Kvdb, t *testing.T) {
	fmt.Println("testRemoveMember")
	// Add node 2
	index := 2
	initCluster, err := kv.AddMember(localhost, peerPorts[index], names[index])
	require.NoError(t, err, "Error on AddMember")
	require.Equal(t, 3, len(initCluster), "Init Cluster length does not match")
	cmd, err := startEtcd(index, initCluster, "existing")
	require.NoError(t, err, "Error on start etcd")
	cmds[index] = cmd
	
	// Remove node 1
	index = 1
	err = kv.RemoveMember(names[index])
	require.NoError(t, err, "Error on RemoveMember")

	cmd, _ = cmds[index]
	cmd.Process.Kill()
	delete(cmds, index)
	// Check the list returned
	list, err := kv.ListMembers()
	require.NoError(t, err, "Error on ListMembers")
	require.Equal(t, 2, len(list), "List returned different length of cluster")
}

func startEtcd(index int, initCluster map[string][]string, initState string) (*exec.Cmd, error){
	peerUrl := urlPrefix + localhost + ":" + peerPorts[index]
	clientUrl := clientUrls[index]
	initialCluster := ""
	for name, ip := range initCluster {
		initialCluster = initialCluster + name + "=" + ip[0] + ","
	}
	fmt.Println("Starting etcd for node ", index, "with initial cluster: ", initialCluster)
	initialCluster = strings.TrimSuffix(initialCluster, ",")
	etcdArgs := []string{
		"--name="+
		names[index],
		"--initial-advertise-peer-urls="+
		peerUrl,
		"--listen-peer-urls="+
		peerUrl,
		"--listen-client-urls="+
		clientUrl,
		"--advertise-client-urls="+
		clientUrl,
		"--initial-cluster="+
		initialCluster,
		"--data-dir="+
		dataDirs[index],
		"--initial-cluster-state="+
		initState,
	}

	cmd := exec.Command("/tmp/test-etcd/etcd", etcdArgs...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
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
