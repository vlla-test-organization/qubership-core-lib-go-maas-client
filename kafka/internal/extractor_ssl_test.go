package internal

import (
	"testing"

	"github.com/netcracker/qubership-core-lib-go-maas-client/v3/classifier"
	"github.com/netcracker/qubership-core-lib-go-maas-client/v3/kafka/model"
	"github.com/stretchr/testify/require"
)

func Test_SslExtractor_sslCert_Extract(t *testing.T) {
	assertions := require.New(t)
	extractor := &SslExtractor{}
	connectionProperties := extractor.Extract(model.TopicAddress{
		Classifier:    classifier.New("test.topic").WithNamespace("test-namespace"),
		TopicName:     "maas.test-namespace.test.topic",
		NumPartitions: 1,
		BoostrapServers: map[string][]string{
			"PLAINTEXT": {"localkafka.kafka-cluster:9092"},
			"SSL":       {"localkafka.kafka-cluster:9094"},
		},
		CACert: "test-caCert",
	})
	assertions.NotNil(connectionProperties)
	assertions.Equal("", connectionProperties.ClientCert)
	assertions.Equal("", connectionProperties.ClientKey)
	assertions.Equal(wrapCert("test-caCert"), connectionProperties.CACert)
	assertions.Equal("", connectionProperties.SaslMechanism)
	assertions.Equal("localkafka.kafka-cluster:9094", connectionProperties.BootstrapServers[0])
}
