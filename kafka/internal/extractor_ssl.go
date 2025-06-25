package internal

import (
	"github.com/netcracker/qubership-core-lib-go-maas-client/v3/kafka/model"
)

type SslExtractor struct {
}

const (
	beginCertificate = "-----BEGIN CERTIFICATE-----\n"
	endCertificate   = "\n-----END CERTIFICATE-----"
)

func (e *SslExtractor) Extract(topic model.TopicAddress) *model.TopicConnectionProperties {
	protocol := "SSL"
	servers := topic.GetBoostrapServers(protocol)
	if servers != nil {
		caCert := topic.CACert
		if caCert != "" {
			topicConfig := &model.TopicConnectionProperties{
				TopicName:        topic.TopicName,
				NumPartitions:    topic.NumPartitions,
				Protocol:         protocol,
				BootstrapServers: servers,
				SaslMechanism:    "",
				CACert:           wrapCert(caCert),
			}

			credentials := topic.GetCredentials("sslCert")
			if credentials != nil {
				topicConfig.ClientCert = wrapCert(credentials.ClientCert)
				topicConfig.ClientKey = wrapClientKey(credentials.ClientKey)
			}
			return topicConfig
		}

	}
	return nil
}

func (e *SslExtractor) Priority() int {
	return 20
}

func wrapCert(cert string) string {
	return beginCertificate + cert + endCertificate
}
