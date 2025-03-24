package internal

import "github.com/netcracker/qubership-core-lib-go-maas-client/v3/classifier"

type TopicRequest struct {
	Name              string             `json:"name,omitempty"`
	Classifier        classifier.Keys    `json:"classifier"`
	ExternallyManaged bool               `json:"externallyManaged,omitempty"`
	Instance          string             `json:"instance,omitempty"` // usage of 'instance' field in runtime is deprecated, clients have to use Instance designators, see maas aggregator's docs
	NumPartitions     interface{}        `json:"numPartitions,omitempty"`
	ReplicationFactor interface{}        `json:"replicationFactor,omitempty"`
	ReplicaAssignment map[int32][]int32  `json:"replicaAssignment,omitempty"`
	Configs           map[string]*string `json:"configs,omitempty"`
	Template          string             `json:"template,omitempty"`
}

type TopicSearchRequest struct {
	Classifier           classifier.Keys `json:"classifier"`
	Topic                string          `json:"topic"`
	Namespace            string          `json:"namespace"`
	Instance             string          `json:"instance"`
	Template             int             `json:"template"`
	LeaveRealTopicIntact bool            `json:"leaveRealTopicIntact"`
}
