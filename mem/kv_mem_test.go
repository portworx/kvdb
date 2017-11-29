package mem

import (
	"testing"

	"github.com/portworx/kvdb/test"
)

func TestAll(t *testing.T) {
	options := make(map[string]string)
	// RunBasic with values as bytes
	test.RunBasic(New, t, Start, Stop, options)
	options[KvUseInterface] = ""
	// RunBasic with values as interface
	test.RunBasic(New, t, Start, Stop, options)
}

func Start() error {
	return nil
}

func Stop() error {
	return nil
}
