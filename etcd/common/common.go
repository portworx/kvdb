package common

import (
	"crypto/tls"
	"crypto/x509"
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
	// DefaultRetryCount for etcd operations
	DefaultRetryCount = 60
	// DefaultIntervalBetweenRetries for etcd failed operations
	DefaultIntervalBetweenRetries = time.Millisecond * 500
	// Bootstrap key
	Bootstrap = "kvdb/bootstrap"
	// DefaultDialTimeout in etcd http requests
	// the maximum amount of time a dial will wait for a connection to setup.
	// 30s is long enough for most of the network conditions.
	DefaultDialTimeout = 30 * time.Second
	// DefaultLockTTL is the ttl for an etcd lock
	DefaultLockTTL = 8
	// DefaultLockRefreshDuration is the time interval for refreshing an etcd lock
	DefaultLockRefreshDuration = 2 * time.Second
)

// EtcdCommon defined the common functions between v2 and v3 etcd implementations.
type EtcdCommon interface {
	// GetAuthInfoFromOptions
	GetAuthInfoFromOptions() (transport.TLSInfo, string, string, error)

	// GetRetryCount
	GetRetryCount() int
}

type etcdCommon struct{
	options map[string]string
}

// NewEtcdCommon returns the EtcdCommon interface
func NewEtcdCommon(options map[string]string) EtcdCommon {
	return &etcdCommon{
		options: options,
	}
}

func (ec *etcdCommon) GetRetryCount() int {
	retryCount, ok := ec.options[kvdb.RetryCountKey]
	if !ok {
		return DefaultRetryCount
	}
	retry, err := strconv.ParseInt(retryCount, 10, 0)
	if err != nil {
		// use default value
		return DefaultRetryCount
	}
	return int(retry)
}

func (ec *etcdCommon) GetAuthInfoFromOptions() (transport.TLSInfo, string, string, error) {
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
	if ec.options != nil || len(ec.options) > 0 {
		var ok bool
		// Check if username provided
		username, ok = ec.options[kvdb.UsernameKey]
		if ok {
			// Check if password provided
			password, ok = ec.options[kvdb.PasswordKey]
			if !ok {
				return transport.TLSInfo{}, "", "", kvdb.ErrNoPassword
			}
			// Check if CA file provided
			caFile, ok = ec.options[kvdb.CAFileKey]
			if !ok {
				return transport.TLSInfo{}, "", "", kvdb.ErrNoCertificate
			}
			// Check if certificate file provided
			certFile, ok = ec.options[kvdb.CertFileKey]
			if !ok {
				certFile = ""
			}
			// Check if certificate key is provided
			keyFile, ok = ec.options[kvdb.CertKeyFileKey]
			if !ok {
				keyFile = ""
			}
			// Check if trusted ca file is provided
			trustedCAFile, ok = ec.options[kvdb.TrustedCAFileKey]
			if !ok {
				trustedCAFile = ""
			}
			// Check if client cert auth is provided
			clientCertAuthStr, ok :=ec.options[kvdb.ClientCertAuthKey]
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
func Version(url string, options map[string]string) (string, error) {
	tlsConfig := &tls.Config{}
	// Check if CA file provided
	caFile, ok := options[kvdb.CAFileKey]
	if ok {
		// Load CA cert
		caCert, err := ioutil.ReadFile(caFile)
		if err != nil {
			return "", err
		}
		caCertPool := x509.NewCertPool()
		caCertPool.AppendCertsFromPEM(caCert)
		tlsConfig.RootCAs = caCertPool
	}
	// Check if certificate file provided
	certFile, certOk := options[kvdb.CertFileKey]
	// Check if certificate key is provided
	keyFile, keyOk := options[kvdb.CertKeyFileKey]
	if certOk && keyOk {
		// Load client cert
		cert, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			return "", err
		}
		tlsConfig.Certificates = []tls.Certificate{cert}
	}

	tlsConfig.BuildNameToCertificate()
	transport := &http.Transport{TLSClientConfig: tlsConfig}
	client := &http.Client{Transport: transport}

	// Do GET something
	resp, err := client.Get(url + "/version")
	if err != nil {
		return "", fmt.Errorf("Error in obtaining etcd version: %v", err)
	}
	defer resp.Body.Close()

	// Dump response
	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("Error in obtaining etcd version: %v", err)
	}

	var version version.Versions
	err = json.Unmarshal(data, &version)
	if err != nil {
		// Probably a version less than 2.3. Default to using v2 apis
		return kvdb.EtcdBaseVersion, nil
	}
	if version.Server[0] == '2' || version.Server[0] == '1' {
		return kvdb.EtcdBaseVersion, nil
	} else if version.Server[0] == '3' {
		return kvdb.EtcdVersion3, nil
	} else {
		return "", fmt.Errorf("Unsupported etcd version: %v", version.Server)
	}
}
