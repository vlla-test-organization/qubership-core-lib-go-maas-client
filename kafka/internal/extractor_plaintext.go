package internal

import "github.com/netcracker/qubership-core-lib-go-maas-client/v3/kafka/model"

type PlaintextExtractor struct {
}

func (e *PlaintextExtractor) Extract(topic model.TopicAddress) *model.TopicConnectionProperties {
	protocol := "PLAINTEXT"
	servers := topic.GetBoostrapServers(protocol)
	if servers != nil {
		credentials := topic.GetCredentials("plain")
		if credentials == nil && len(topic.Credentials) == 0 {
			return &model.TopicConnectionProperties{
				TopicName:        topic.TopicName,
				NumPartitions:    topic.NumPartitions,
				Protocol:         protocol,
				BootstrapServers: servers,
			}
		} else if credentials != nil {
			return &model.TopicConnectionProperties{
				TopicName:        topic.TopicName,
				NumPartitions:    topic.NumPartitions,
				Protocol:         protocol,
				BootstrapServers: servers,
				SaslMechanism:    "PLAIN",
				Username:         credentials.Username,
				Password:         credentials.Password,
			}
		}
	}
	return nil
}

func (e *PlaintextExtractor) Priority() int {
	return 0
}
