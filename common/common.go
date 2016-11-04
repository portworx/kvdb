package common

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"runtime"
	"time"

	"github.com/Sirupsen/logrus"
)

var (
	path = "/var/cores/"
)

// ToBytes converts to value to a byte slice.
func ToBytes(val interface{}) ([]byte, error) {
	switch val.(type) {
	case string:
		return []byte(val.(string)), nil
	case []byte:
		b := make([]byte, len(val.([]byte)))
		copy(b, val.([]byte))
		return b, nil
	default:
		return json.Marshal(val)
	}
}

func Panicf(format string, args ...interface{}) {
	logrus.Warnf(format, args)
	trace := make([]byte, 1024*1024)
	runtime.Stack(trace, true)
	err := ioutil.WriteFile(path+time.Now().String()+".stack", trace, 0644)
	if err != nil {
		logrus.Fatal(err)
	}
	os.Exit(1)
}
