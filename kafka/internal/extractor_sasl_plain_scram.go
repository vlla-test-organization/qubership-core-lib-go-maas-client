package internal

import "github.com/netcracker/qubership-core-lib-go-maas-client/v3/kafka/model"

type SaslPlaintextScramExtractor struct {
}

func (e *SaslPlaintextScramExtractor) Extract(topic model.TopicAddress) *model.TopicConnectionProperties {
	protocol := "SASL_PLAINTEXT"
	servers := topic.GetBoostrapServers(protocol)
	if servers != nil {
		credentials := topic.GetCredentials("SCRAM")
		if credentials != nil {
			return &model.TopicConnectionProperties{
				TopicName:        topic.TopicName,
				NumPartitions:    topic.NumPartitions,
				Protocol:         protocol,
				BootstrapServers: servers,
				SaslMechanism:    "SCRAM-SHA-512",
				Username:         credentials.Username,
				Password:         credentials.Password,
			}
		}
	}
	return nil
}

func (e *SaslPlaintextScramExtractor) Priority() int {
	return 10
}
