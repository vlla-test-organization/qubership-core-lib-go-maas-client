package internal

import "github.com/netcracker/qubership-core-lib-go-maas-client/v3/kafka/model"

type SaslSslExtractor struct {
}

const (
	beginRsaPrivateKey = "-----BEGIN RSA PRIVATE KEY-----\n"
	endRsaPrivateKey   = "\n-----END RSA PRIVATE KEY-----"
)

func (e *SaslSslExtractor) Extract(topic model.TopicAddress) *model.TopicConnectionProperties {
	protocol := "SASL_SSL"
	servers := topic.GetBoostrapServers(protocol)
	if servers != nil {
		caCert := topic.CACert
		if caCert != "" {
			topicConfig := &model.TopicConnectionProperties{
				TopicName:        topic.TopicName,
				NumPartitions:    topic.NumPartitions,
				Protocol:         protocol,
				BootstrapServers: servers,
				CACert:           wrapCert(caCert),
			}
			credentials := topic.GetCredentials("sslCert+SCRAM")
			if credentials != nil {
				topicConfig.Username = credentials.Username
				topicConfig.Password = credentials.Password
				topicConfig.ClientCert = wrapCert(credentials.ClientCert)
				topicConfig.ClientKey = wrapClientKey(credentials.ClientKey)
				topicConfig.SaslMechanism = "SCRAM-SHA-512"
				return topicConfig
			}
			credentials = topic.GetCredentials("sslCert+plain")
			if credentials != nil {
				topicConfig.Username = credentials.Username
				topicConfig.Password = credentials.Password
				topicConfig.ClientCert = wrapCert(credentials.ClientCert)
				topicConfig.ClientKey = wrapClientKey(credentials.ClientKey)
				topicConfig.SaslMechanism = "PLAIN"
				return topicConfig
			}
			credentials = topic.GetCredentials("SCRAM")
			if credentials != nil {
				topicConfig.Username = credentials.Username
				topicConfig.Password = credentials.Password
				topicConfig.SaslMechanism = "SCRAM-SHA-512"
				return topicConfig
			}
		}
	}
	return nil
}

func (e *SaslSslExtractor) Priority() int {
	return 30
}

func wrapClientKey(key string) string {
	return beginRsaPrivateKey + key + endRsaPrivateKey
}
