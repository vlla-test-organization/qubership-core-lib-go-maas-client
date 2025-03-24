package model

import (
	"net/http"
	"net/url"
	"sort"

	"github.com/gorilla/websocket"
	"github.com/netcracker/qubership-core-lib-go-maas-client/v3/classifier"
)

type TopicAddress struct {
	Classifier      classifier.Keys
	TopicName       string
	NumPartitions   int
	BoostrapServers map[string][]string
	Credentials     map[string]TopicUserCredentials
	CACert          string
	Configs         map[string]*string
}

func (ta TopicAddress) GetClassifier() classifier.Keys {
	return ta.Classifier
}

func (ta TopicAddress) GetBoostrapServers(protocol string) []string {
	return ta.BoostrapServers[protocol]
}

func (ta TopicAddress) GetProtocols() []string {
	var protocols []string
	for protocol := range ta.BoostrapServers {
		protocols = append(protocols, protocol)
	}
	if protocols != nil {
		sort.Strings(protocols)
	}
	return protocols
}

func (ta TopicAddress) GetCredTypes() []string {
	var credTypes []string
	for credType := range ta.Credentials {
		credTypes = append(credTypes, credType)
	}
	if credTypes != nil {
		sort.Strings(credTypes)
	}
	return credTypes
}

func (ta TopicAddress) GetCredentials(credType string) *TopicUserCredentials {
	if credentials, ok := ta.Credentials[credType]; ok {
		return &credentials
	} else {
		return nil
	}
}

func (ta TopicAddress) GetConfig(key string) *string {
	return ta.Configs[key]
}

type TopicConnectionProperties struct {
	TopicName        string
	NumPartitions    int
	Protocol         string
	BootstrapServers []string
	SaslMechanism    string
	CACert           string
	Username         string
	Password         string
	ClientKey        string
	ClientCert       string
}

type TopicUserCredentials struct {
	Username   string
	Password   string
	ClientKey  string
	ClientCert string
}

type TopicCreateOptions struct {
	Name              string
	NumPartitions     int
	MinNumPartitions  int
	ReplicationFactor string
	ReplicaAssignment map[int32][]int32
	ExternallyManaged bool
	OnTopicExists     OnTopicExists
	Configs           map[string]*string
	Template          string
}

type OnTopicExists string

const (
	OnTopicExistsFail  OnTopicExists = "fail"
	OnTopicExistsMerge OnTopicExists = "merge"
)

type Dial func(webSocketURL url.URL, dialer websocket.Dialer, requestHeaders http.Header) (*websocket.Conn, *http.Response, error)

type Dialer struct {
	DialFunc Dial
}

func (d *Dialer) Dial(webSocketURL url.URL, dialer websocket.Dialer, requestHeaders http.Header) (*websocket.Conn, *http.Response, error) {
	return d.DialFunc(webSocketURL, dialer, requestHeaders)
}
