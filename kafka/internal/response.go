package internal

type TopicResponse struct {
	Addresses         map[string][]string         `json:"addresses"`
	Name              string                      `json:"name"`
	Classifier        map[string]string           `json:"classifier"`
	Namespace         string                      `json:"namespace"`
	ExternallyManaged bool                        `json:"externallyManaged"`
	Instance          string                      `json:"instance"`
	CACert            string                      `json:"caCert,omitempty"`
	Credentials       map[string][]map[string]any `json:"credential,omitempty"`
	RequestedSettings *TopicSettings              `json:"requestedSettings"`
	ActualSettings    *TopicSettings              `json:"actualSettings"`
	Template          string                      `json:"template,omitempty"`
}

type KafkaInstance struct {
	Id           string                         `json:"id"`
	Addresses    map[string][]string            `json:"addresses"`
	Default      bool                           `json:"default"`
	MaaSProtocol string                         `json:"maasProtocol"`
	CACert       string                         `json:"caCert,omitempty"`
	Credentials  map[string][]map[string]string `json:"credentials,omitempty"`
}

type TopicSettings struct {
	NumPartitions     int                `json:"numPartitions,omitempty"`
	ReplicationFactor *int               `json:"replicationFactor,omitempty"`
	ReplicaAssignment map[int32][]int32  `json:"replicaAssignment,omitempty"`
	Configs           map[string]*string `json:"configs,omitempty"`
}
