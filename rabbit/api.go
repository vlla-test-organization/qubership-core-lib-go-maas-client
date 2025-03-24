package rabbit

import (
	"context"

	"github.com/netcracker/qubership-core-lib-go-maas-client/v3/classifier"
	"github.com/netcracker/qubership-core-lib-go-maas-client/v3/rabbit/model"
	amqp "github.com/rabbitmq/amqp091-go"
)

type MaasClient interface {
	// GetOrCreateVhost allows to get or create (in case the vhost does not exist) vhost by classifier
	GetOrCreateVhost(ctx context.Context, classifier classifier.Keys) (*model.Vhost, error)

	// GetVhost allows to get Vhost by classifier, returns non-empty vhost only if it is present in MaaS
	// or nil in case vhost with provided classifier does not exist
	// classifier - must be equal to the classifier of the target vhost to be retrieved
	GetVhost(ctx context.Context, classifier classifier.Keys) (*model.VhostConfig, error)

	// BuildHeaders should be used to propagate context to rabbit messages
	BuildHeaders(ctxData map[string]string) amqp.Table
}
