package internal

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/vlla-test-organization/qubership-core-lib-go-maas-client/v3/classifier"
	"github.com/vlla-test-organization/qubership-core-lib-go-maas-client/v3/kafka/model"
)

func Test_SslExtractor_sslCertMTLS_Extract(t *testing.T) {
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
		Credentials: map[string]model.TopicUserCredentials{
			"sslCert": {
				ClientCert: "test-clientCert",
				ClientKey:  "test-clientKey",
			},
		},
	})
	assertions.NotNil(connectionProperties)
	assertions.Equal(wrapCert("test-clientCert"), connectionProperties.ClientCert)
	assertions.Equal(wrapClientKey("test-clientKey"), connectionProperties.ClientKey)
	assertions.Equal(wrapCert("test-caCert"), connectionProperties.CACert)
	assertions.Equal("", connectionProperties.SaslMechanism)
	assertions.Equal("localkafka.kafka-cluster:9094", connectionProperties.BootstrapServers[0])
}
