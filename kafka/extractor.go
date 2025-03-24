package kafka

import (
	"fmt"
	"sort"

	"github.com/netcracker/qubership-core-lib-go-maas-client/v3/kafka/internal"
	"github.com/netcracker/qubership-core-lib-go-maas-client/v3/kafka/model"
)

type ConnectionPropertiesExtractor interface {
	Extract(topic model.TopicAddress) *model.TopicConnectionProperties
	Priority() int
}

type TopicConfigResolver interface {
	Resolve(topic model.TopicAddress, extractors ...ConnectionPropertiesExtractor)
}

var defaultExtractors = []ConnectionPropertiesExtractor{
	&internal.PlaintextExtractor{},
	&internal.SaslPlaintextScramExtractor{},
	&internal.PlaintextScramExtractor{},
	&internal.SaslSslExtractor{},
	&internal.SslExtractor{},
}

func RegisterExtractor(extractor ConnectionPropertiesExtractor) {
	defaultExtractors = append(defaultExtractors, extractor)
}

func Extract(topicAddress model.TopicAddress) (*model.TopicConnectionProperties, error) {
	priorities, extractorsMap := getSortedExtractors(defaultExtractors)
	for _, priority := range priorities {
		extractor := extractorsMap[priority]
		primaryTopicConfig := extractor.Extract(topicAddress)
		if primaryTopicConfig != nil {
			return primaryTopicConfig, nil
		}
	}
	return nil, fmt.Errorf("failed to extract TopicConnectionProperties. All %d registered extractors returned nil", len(extractorsMap))
}

func getSortedExtractors(extractors []ConnectionPropertiesExtractor) ([]int, map[int]ConnectionPropertiesExtractor) {
	extractorsMap := make(map[int]ConnectionPropertiesExtractor, len(extractors))
	var priorities []int
	for _, extractor := range extractors {
		priority := extractor.Priority()
		if extractorsMap[priority] != nil {
			panic(fmt.Sprintf("second extractor with priority %d found", priority))
		}
		extractorsMap[priority] = extractor
		priorities = append(priorities, priority)
	}
	sort.Sort(sort.Reverse(sort.IntSlice(priorities)))
	return priorities, extractorsMap
}
