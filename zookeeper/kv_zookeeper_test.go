package zookeeper

import (
	"io/ioutil"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/portworx/kvdb/test"
)

const (
	dataDir = "/data/zookeeper"
)

func TestAll(t *testing.T) {
	test.RunBasic(New, t, Start, Stop)
}

func Start(removeData bool) error {
	if removeData {
		err := os.RemoveAll(dataDir)
		if err != nil {
			return err
		}
	}
	err := os.MkdirAll(dataDir, 0644)
	if err != nil {
		return err
	}
	err = ioutil.WriteFile(dataDir+"/myid", []byte("1"), 0644)
	if err != nil {
		return err
	}
	cmd := exec.Command("/tmp/test-zookeeper/bin/zkServer.sh", "start")
	err = cmd.Start()
	time.Sleep(5 * time.Second)
	return err
}

func Stop() error {
	cmd := exec.Command("/tmp/test-zookeeper/bin/zkServer.sh", "stop")
	err := cmd.Start()
	return err
}
