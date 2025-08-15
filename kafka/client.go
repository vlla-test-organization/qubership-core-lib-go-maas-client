package kafka

import (
	"context"

	"github.com/go-resty/resty/v2"
	"github.com/gorilla/websocket"
	"github.com/vlla-test-organization/qubership-core-lib-go-maas-client/v3/classifier"
	"github.com/vlla-test-organization/qubership-core-lib-go-maas-client/v3/kafka/internal"
	watchInternal "github.com/vlla-test-organization/qubership-core-lib-go-maas-client/v3/kafka/internal/watch"
	"github.com/vlla-test-organization/qubership-core-lib-go-maas-client/v3/kafka/model"
	"github.com/vlla-test-organization/qubership-core-lib-go-maas-client/v3/util"
	"github.com/vlla-test-organization/qubership-core-lib-go-maas-client/v3/watch"
)

func NewClient(namespace string, maasAgentUrl string, tenantManagerUrl string, httpClient *resty.Client,
	dialer *websocket.Dialer, authSupplier func(ctx context.Context) (string, error)) MaasClient {
	crudClient := &internal.CrudClient{
		MaasAgentUrl:  maasAgentUrl,
		Namespace:     namespace,
		HttpClient:    httpClient,
		Auth:          authSupplier,
		RetryInterval: util.DefaultRetryInterval,
		RetryAttempts: util.DefaultRetryAttempts,
	}
	watchClient := watchInternal.NewClient[model.TopicAddress](maasAgentUrl,
		"/api/v2/kafka/topic/watch-create?timeout=60s", httpClient, internal.ResponseToTopicAddress)
	getResources := func(ctx context.Context, keys classifier.Keys, tenants []watch.Tenant) ([]model.TopicAddress, error) {
		var topics []model.TopicAddress
		for _, tenant := range tenants {
			keysWithTenant := keys.WithTenantId(tenant.ExternalId)
			topic, err := crudClient.GetTopic(ctx, keysWithTenant)
			if err != nil {
				return nil, err
			} else if topic != nil {
				topics = append(topics, *topic)
			}
		}
		return topics, nil
	}
	tenantWatchClient := watchInternal.NewTenantWatchClient[model.TopicAddress](tenantManagerUrl, getResources, dialer, authSupplier)
	return internal.NewKafkaClient(namespace, crudClient, watchClient, tenantWatchClient)
}
