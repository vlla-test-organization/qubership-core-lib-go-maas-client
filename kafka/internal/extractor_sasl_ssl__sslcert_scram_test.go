package internal

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/vlla-test-organization/qubership-core-lib-go-maas-client/v3/classifier"
	"github.com/vlla-test-organization/qubership-core-lib-go-maas-client/v3/kafka/model"
)

func Test_SaslSslExtractor_sslCert_SCRAM_Extract(t *testing.T) {
	assertions := require.New(t)
	extractor := &SaslSslExtractor{}
	connectionProperties := extractor.Extract(model.TopicAddress{
		Classifier:    classifier.New("test.topic").WithNamespace("test-namespace"),
		TopicName:     "maas.test-namespace.test.topic",
		NumPartitions: 1,
		BoostrapServers: map[string][]string{
			"PLAINTEXT": {"localkafka.kafka-cluster:9092"},
			"SASL_SSL":  {"localkafka.kafka-cluster:9094"},
		},
		Credentials: map[string]model.TopicUserCredentials{
			"sslCert+SCRAM": {
				Username:   "alice",
				Password:   "plain:alice-secret",
				ClientKey:  "test-clientKey",
				ClientCert: "test-clientCert",
			},
		},
		CACert: "test-caCert",
	})
	assertions.NotNil(connectionProperties)
	assertions.Equal(wrapCert("test-clientCert"), connectionProperties.ClientCert)
	assertions.Equal(wrapClientKey("test-clientKey"), connectionProperties.ClientKey)
	assertions.Equal(wrapCert("test-caCert"), connectionProperties.CACert)
	assertions.Equal("SCRAM-SHA-512", connectionProperties.SaslMechanism)
	assertions.Equal("localkafka.kafka-cluster:9094", connectionProperties.BootstrapServers[0])
}

func Test_SaslSslExtractor_sslCert_plain_Extract(t *testing.T) {
	assertions := require.New(t)
	extractor := &SaslSslExtractor{}
	connectionProperties := extractor.Extract(model.TopicAddress{
		Classifier:    classifier.New("test.topic").WithNamespace("test-namespace"),
		TopicName:     "maas.test-namespace.test.topic",
		NumPartitions: 1,
		BoostrapServers: map[string][]string{
			"PLAINTEXT": {"localkafka.kafka-cluster:9092"},
			"SASL_SSL":  {"localkafka.kafka-cluster:9094"},
		},
		Credentials: map[string]model.TopicUserCredentials{
			"sslCert+plain": {
				Username:   "alice",
				Password:   "plain:alice-secret",
				ClientKey:  "test-clientKey",
				ClientCert: "test-clientCert",
			},
		},
		CACert: "test-caCert",
	})
	assertions.NotNil(connectionProperties)
	assertions.Equal(wrapCert("test-clientCert"), connectionProperties.ClientCert)
	assertions.Equal(wrapClientKey("test-clientKey"), connectionProperties.ClientKey)
	assertions.Equal(wrapCert("test-caCert"), connectionProperties.CACert)
	assertions.Equal("PLAIN", connectionProperties.SaslMechanism)
	assertions.Equal("localkafka.kafka-cluster:9094", connectionProperties.BootstrapServers[0])
}

func Test_SaslSslExtractor_SCRAM_Extract(t *testing.T) {
	assertions := require.New(t)
	extractor := &SaslSslExtractor{}
	connectionProperties := extractor.Extract(model.TopicAddress{
		Classifier:    classifier.New("test.topic").WithNamespace("test-namespace"),
		TopicName:     "maas.test-namespace.test.topic",
		NumPartitions: 1,
		BoostrapServers: map[string][]string{
			"PLAINTEXT": {"localkafka.kafka-cluster:9092"},
			"SASL_SSL":  {"localkafka.kafka-cluster:9094"},
		},
		Credentials: map[string]model.TopicUserCredentials{
			"SCRAM": {
				Username: "alice",
				Password: "plain:alice-secret",
			},
		},
		CACert: "test-caCert",
	})
	assertions.NotNil(connectionProperties)
	assertions.Equal("", connectionProperties.ClientCert)
	assertions.Equal("", connectionProperties.ClientKey)
	assertions.Equal(wrapCert("test-caCert"), connectionProperties.CACert)
	assertions.Equal("SCRAM-SHA-512", connectionProperties.SaslMechanism)
	assertions.Equal("localkafka.kafka-cluster:9094", connectionProperties.BootstrapServers[0])
}
