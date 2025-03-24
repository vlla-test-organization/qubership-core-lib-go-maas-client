package internal

import (
	"testing"

	"github.com/netcracker/qubership-core-lib-go-maas-client/v3/classifier"
	"github.com/netcracker/qubership-core-lib-go-maas-client/v3/kafka/model"
	"github.com/stretchr/testify/require"
)

func Test_PlaintextExtractor_NoCreds_Extract(t *testing.T) {
	assertions := require.New(t)
	extractor := &PlaintextExtractor{}
	connectionProperties := extractor.Extract(model.TopicAddress{
		Classifier:    classifier.New("test.topic").WithNamespace("test-namespace"),
		TopicName:     "maas.test-namespace.test.topic",
		NumPartitions: 1,
		BoostrapServers: map[string][]string{
			"PLAINTEXT": {"localkafka.kafka-cluster:9092"},
		},
	})
	assertions.NotNil(connectionProperties)
	assertions.Equal("", connectionProperties.SaslMechanism)
	assertions.Equal("", connectionProperties.Username)
	assertions.Equal("", connectionProperties.Password)
	assertions.Equal("PLAINTEXT", connectionProperties.Protocol)
	assertions.Equal("localkafka.kafka-cluster:9092", connectionProperties.BootstrapServers[0])
}
