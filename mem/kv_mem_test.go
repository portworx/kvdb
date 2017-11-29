package mem

import (
	"fmt"
	"testing"

	"github.com/portworx/kvdb"
	"github.com/portworx/kvdb/test"
	"github.com/stretchr/testify/assert"
)

func TestAll(t *testing.T) {
	options := make(map[string]string)
	// RunBasic with values as bytes
	test.RunBasic(New, t, Start, Stop, options)
	options[KvUseInterface] = ""
	//  RunBasic with values as interface
	test.RunBasic(New, t, Start, Stop, options)
	// Run mem specific tests
	kv, err := New("pwx/test", nil, options, nil)
	if err != nil {
		t.Fatalf(err.Error())
	}
	testNoCopy(kv, t)
}

func testNoCopy(kv kvdb.Kvdb, t *testing.T) {
	fmt.Println("testNoCopy")
	type Test struct {
		A int
		B string
	}
	val := &Test{1, "abc"}
	_, err := kv.Put("key1", &val, 0)
	assert.NoError(t, err, "Expected no error on put")
	val.A = 2
	val.B = "def"
	var newVal Test
	_, err = kv.GetVal("key1", &newVal)
	assert.NoError(t, err, "Expected no error on get")
	assert.Equal(t, newVal.A, val.A, "Expected equal values")
	assert.Equal(t, newVal.B, val.B, "Expected equal values")
}

func Start() error {
	return nil
}

func Stop() error {
	return nil
}
