package common

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"time"

	"strconv"

	"github.com/coreos/etcd/pkg/transport"
	"github.com/coreos/etcd/version"
	"github.com/portworx/kvdb"
)

const (
	DefaultRetryCount             = 60
	DefaultIntervalBetweenRetries = time.Millisecond * 500
	Bootstrap                     = "kvdb/bootstrap"
	// the maximum amount of time a dial will wait for a connection to setup.
	// 30s is long enough for most of the network conditions.
	DefaultDialTimeout         = 30 * time.Second
	DefaultLockTTL             = 8
	DefaultLockRefreshDuration = 2 * time.Second
)

// EtcdCommon defined the common functions between v2 and v3 etcd implementations.
type EtcdCommon interface {
	// GetAuthInfoFromOptions
	GetAuthInfoFromOptions(map[string]string) (transport.TLSInfo, string, string, error)
}

type etcdCommon struct{}

// NewEtcdCommon returns the EtcdCommon interface
func NewEtcdCommon() EtcdCommon {
	return &etcdCommon{}
}

func (ec *etcdCommon) GetAuthInfoFromOptions(options map[string]string) (transport.TLSInfo, string, string, error) {
	var (
		username       string
		password       string
		caFile         string
		certFile       string
		keyFile        string
		trustedCAFile  string
		clientCertAuth bool
		err            error
	)
	// options provided. Probably auth options
	if options != nil || len(options) > 0 {
		var ok bool
		// Check if username provided
		username, ok = options[kvdb.UsernameKey]
		if ok {
			// Check if password provided
			password, ok = options[kvdb.PasswordKey]
			if !ok {
				return transport.TLSInfo{}, "", "", kvdb.ErrNoPassword
			}
			// Check if CA file provided
			caFile, ok = options[kvdb.CAFileKey]
			if !ok {
				return transport.TLSInfo{}, "", "", kvdb.ErrNoCertificate
			}
			// Check if certificate file provided
			certFile, ok = options[kvdb.CertFileKey]
			if !ok {
				certFile = ""
			}
			// Check if certificate key is provided
			keyFile, ok = options[kvdb.CertKeyFileKey]
			if !ok {
				keyFile = ""
			}
			// Check if trusted ca file is provided
			trustedCAFile, ok = options[kvdb.TrustedCAFileKey]
			if !ok {
				trustedCAFile = ""
			}
			// Check if client cert auth is provided
			clientCertAuthStr, ok := options[kvdb.ClientCertAuthKey]
			if !ok {
				clientCertAuth = false
			} else {
				clientCertAuth, err = strconv.ParseBool(clientCertAuthStr)
				if err != nil {
					clientCertAuth = false
				}
			}

		}
	}
	tls := transport.TLSInfo{
		CAFile:         caFile,
		CertFile:       certFile,
		KeyFile:        keyFile,
		TrustedCAFile:  trustedCAFile,
		ClientCertAuth: clientCertAuth,
	}
	return tls, username, password, nil
}

// Version returns the version of the provided etcd server
func Version(url string) (string, error) {
	response, err := http.Get(url + "/version")
	if err != nil {
		return "", err
	}

	defer response.Body.Close()
	contents, _ := ioutil.ReadAll(response.Body)

	var version version.Versions
	err = json.Unmarshal(contents, &version)
	if err != nil {
		return "", err
	}
	if version.Server[0] == '2' || version.Server[0] == '1' {
		return kvdb.EtcdBaseVersion, nil
	} else if version.Server[0] == '3' {
		return kvdb.EtcdVersion3, nil
	} else {
		return "", fmt.Errorf("Unsupported etcd version: %v", version.Server)
	}
}
