package kafka

import (
	"context"

	"github.com/vlla-test-organization/qubership-core-lib-go-maas-client/v3/classifier"
	"github.com/vlla-test-organization/qubership-core-lib-go-maas-client/v3/kafka/model"
)

type MaasClient interface {
	// GetOrCreateTopic allows to get or create (in case the topic does not exist) topic by classifier
	// options - optional TopicCreateOptions struct to customize topic creation
	GetOrCreateTopic(ctx context.Context, classifier classifier.Keys, options ...model.TopicCreateOptions) (*model.TopicAddress, error)

	// GetTopic allows to get topic by classifier, returns non-empty topic only if it is present in MaaS
	// or nil in case topic with provided classifier does not exist
	// classifier - must be equal to the classifier of the target topic to be retrieved
	GetTopic(ctx context.Context, classifier classifier.Keys) (*model.TopicAddress, error)

	// DeleteTopic allows to delete topic by classifier
	// classifier - must be equal to the classifier of the target topic to be deleted
	DeleteTopic(ctx context.Context, classifier classifier.Keys) error

	// WatchTopicCreate allows to subscribe for notifications about topics
	// (tenant topics are ignored by this watch function. For tenant topics watching use WatchTenantTopics method)
	// ctx - use context.WithCancel() in case you want to cancel watcher created by this function
	// classifier - classifier must include name and namespace parameters
	// callback - function which will be called when topic for provided classifier was created in MaaS
	WatchTopicCreate(ctx context.Context, classifier classifier.Keys, callback func(model.TopicAddress)) error

	// Deprecated: use WatchTenantKafkaTopics instead
	WatchTenantTopics(ctx context.Context, classifier classifier.Keys, callback func(topics []model.TopicAddress)) error

	// WatchTenantKafkaTopics allows to subscribe for notifications about tenant topics
	// ctx - use context.WithCancel() in case you want to cancel watcher created by this function
	// classifier - classifier must include name and namespace parameters and must not include tenantId parameter
	// callback - function which will be called each time when active tenant list is changed or nil (see why it can be nil below).
	// topics slice contains tenant topics which were resolved by received active tenant list
	// returns error in case initial attempt of the internal watcher to connect to tenant-manager failed or timeout occurred
	//
	// Note! callback function's err parameter allows clients to detect if internal tenant-manager watcher is alive
	// and maas-agent searches topics without errors.
	// In case there is connection problem with TM, watcher terminates after some retries and
	// clients can know about this from passed 'err' param.
	// If err != nil - it means watcher was terminated or maas-agent returned error and clients need to re-connect or terminate exceptionally.
	// In case err != nil the 'topics' parameter will be nil
	WatchTenantKafkaTopics(ctx context.Context, classifier classifier.Keys, callback func(topics []model.TopicAddress, err error)) error
}
