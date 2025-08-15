package rabbit

import (
	"github.com/go-resty/resty/v2"
	"github.com/vlla-test-organization/qubership-core-lib-go-maas-client/v3/rabbit/internal"
)

func NewClient(namespace string, maasAgentUrl string, httpClient *resty.Client) MaasClient {
	client := &internal.CrudClient{
		MaasAgentUrl: maasAgentUrl,
		Namespace:    namespace,
		HttpClient:   httpClient,
	}

	return internal.NewRabbitClient(namespace, client)
}
