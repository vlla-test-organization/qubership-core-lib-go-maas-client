package internal

import "github.com/netcracker/qubership-core-lib-go-maas-client/v3/kafka/model"

type PlaintextScramExtractor struct {
}

func (e *PlaintextScramExtractor) Extract(topic model.TopicAddress) *model.TopicConnectionProperties {
	protocol := "PLAINTEXT"
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

func (e *PlaintextScramExtractor) Priority() int {
	return 15
}
