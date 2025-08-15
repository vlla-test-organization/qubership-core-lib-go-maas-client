package internal

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/go-resty/resty/v2"
	"github.com/stretchr/testify/require"
	"github.com/vlla-test-organization/qubership-core-lib-go-maas-client/v3/classifier"
	"github.com/vlla-test-organization/qubership-core-lib-go-maas-client/v3/kafka/model"
)

var (
	testNamespace        = " test-namespace"
	waitTimeout          = 3 * time.Second
	testToken            = "test-token"
	defaultRetryAttempts = 5
	defaultRetryInterval = 10 * time.Millisecond
)

func Test_GetOrCreateTopic(t *testing.T) {
	configRetentionBytes := "50000" // this variable only to be able to get pointer to string value in map

	assertions := require.New(t)
	ctx := context.Background()

	wg := &sync.WaitGroup{}
	wg.Add(2)
	ts := createTestServer(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == resty.MethodPost && r.URL.Path == "/api/v1/kafka/topic" {
			assertions.Equal(fmt.Sprintf("Bearer %s", testToken), r.Header.Get("Authorization"))
			onTopicExistsQueryParam := r.URL.Query().Get(OnTopicExistsQueryParam)
			assertions.NotEmpty(onTopicExistsQueryParam)
			w.WriteHeader(http.StatusOK)
			topicResp := TopicResponse{
				Addresses:         map[string][]string{"PLAINTEXT": {"server1:9092", "server2:9092"}},
				Name:              "maas.test-namespace.test.topic",
				Classifier:        map[string]string{"name": "test.topic", "namespace": testNamespace},
				Namespace:         testNamespace,
				ExternallyManaged: false,
				Instance:          "test-kafka",
				CACert:            "",
				Credentials:       map[string][]map[string]any{"client": {map[string]any{"type": "PLAIN", "username": "client", "password": "plain:client"}}},
				RequestedSettings: &TopicSettings{
					NumPartitions:     1,
					ReplicationFactor: func(val int) *int { return &val }(1),
				},
				ActualSettings: &TopicSettings{
					NumPartitions:     1,
					ReplicationFactor: func(val int) *int { return &val }(1),
					ReplicaAssignment: map[int32][]int32{0: {3}},
					Configs:           map[string]*string{"retention.bytes": &configRetentionBytes},
				},
			}
			bytes, _ := json.Marshal(topicResp)
			_, _ = w.Write(bytes)
			wg.Done()
		}
	})
	defer ts.Close()
	kafkaClient := newKafkaClient(ts.URL)
	options := model.TopicCreateOptions{
		Name:          "test-topic",
		NumPartitions: 2,
		OnTopicExists: model.OnTopicExistsMerge,
	}
	topic, err := kafkaClient.GetOrCreateTopic(ctx, classifier.New("test"), options)
	assertions.NoError(err)
	assertions.NotNil(topic)
	expectedTopic := model.TopicAddress{
		Classifier:      classifier.New("test.topic").WithNamespace(testNamespace),
		TopicName:       "maas.test-namespace.test.topic",
		NumPartitions:   1,
		BoostrapServers: map[string][]string{"PLAINTEXT": {"server1:9092", "server2:9092"}},
		Credentials:     map[string]model.TopicUserCredentials{"PLAIN": {Username: "client", Password: "client"}},
		CACert:          "",
		Configs:         map[string]*string{"retention.bytes": &configRetentionBytes},
	}
	assertions.Equal(expectedTopic, *topic)
	topic, err = kafkaClient.GetOrCreateTopic(ctx, classifier.New("test"), options)
	assertions.NoError(err)
	assertions.Equal(expectedTopic, *topic)
	assertions.True(waitWG(waitTimeout, wg))
}

func Test_GetOrCreateTopicWithRetry(t *testing.T) {
	assertions := require.New(t)
	ctx := context.Background()
	attempts := defaultRetryAttempts - 1
	ts := createTestServer(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == resty.MethodPost && r.URL.Path == "/api/v1/kafka/topic" {
			assertions.Equal(fmt.Sprintf("Bearer %s", testToken), r.Header.Get("Authorization"))
			onTopicExistsQueryParam := r.URL.Query().Get(OnTopicExistsQueryParam)
			assertions.NotEmpty(onTopicExistsQueryParam)
			if attempts > 0 {
				attempts--
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			w.WriteHeader(http.StatusOK)
			topicResp := TopicResponse{
				Addresses:         map[string][]string{"PLAINTEXT": {"server1:9092", "server2:9092"}},
				Name:              "maas.test-namespace.test.topic",
				Classifier:        map[string]string{"name": "test.topic", "namespace": testNamespace},
				Namespace:         testNamespace,
				ExternallyManaged: false,
				Instance:          "test-kafka",
				CACert:            "",
				Credentials:       map[string][]map[string]any{"client": {map[string]any{"type": "PLAIN", "username": "client", "password": "plain:client"}}},
				RequestedSettings: &TopicSettings{
					NumPartitions:     1,
					ReplicationFactor: func(val int) *int { return &val }(1),
				},
				ActualSettings: &TopicSettings{
					NumPartitions:     1,
					ReplicationFactor: func(val int) *int { return &val }(1),
					ReplicaAssignment: map[int32][]int32{0: {3}},
				},
			}
			bytes, _ := json.Marshal(topicResp)
			_, _ = w.Write(bytes)
		}
	})
	defer ts.Close()
	kafkaClient := newKafkaClient(ts.URL)
	options := model.TopicCreateOptions{
		Name:          "test-topic",
		NumPartitions: 2,
		OnTopicExists: model.OnTopicExistsMerge,
	}
	topic, err := kafkaClient.GetOrCreateTopic(ctx, classifier.New("test"), options)
	assertions.NoError(err)
	assertions.NotNil(topic)
	expectedTopic := model.TopicAddress{
		Classifier:      classifier.New("test.topic").WithNamespace(testNamespace),
		TopicName:       "maas.test-namespace.test.topic",
		NumPartitions:   1,
		BoostrapServers: map[string][]string{"PLAINTEXT": {"server1:9092", "server2:9092"}},
		Credentials:     map[string]model.TopicUserCredentials{"PLAIN": {Username: "client", Password: "client"}},
		CACert:          "",
	}
	assertions.Equal(expectedTopic, *topic)
}

func Test_GetTopic(t *testing.T) {
	assertions := require.New(t)
	ctx := context.Background()
	wg := &sync.WaitGroup{}
	wg.Add(2)
	ts := createTestServer(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == resty.MethodPost && r.URL.Path == "/api/v1/kafka/topic/get-by-classifier" {
			assertions.Equal(fmt.Sprintf("Bearer %s", testToken), r.Header.Get("Authorization"))
			w.WriteHeader(http.StatusOK)
			topicResp := TopicResponse{
				Addresses:         map[string][]string{"PLAINTEXT": {"server1:9092", "server2:9092"}},
				Name:              "maas.test-namespace.test.topic",
				Classifier:        map[string]string{"name": "test.topic", "namespace": testNamespace},
				Namespace:         testNamespace,
				ExternallyManaged: false,
				Instance:          "test-kafka",
				CACert:            "",
				Credentials:       map[string][]map[string]any{"client": {map[string]any{"type": "PLAIN", "username": "client", "password": "plain:client"}}},
				RequestedSettings: &TopicSettings{
					NumPartitions:     1,
					ReplicationFactor: func(val int) *int { return &val }(1),
				},
				ActualSettings: &TopicSettings{
					NumPartitions:     1,
					ReplicationFactor: func(val int) *int { return &val }(1),
					ReplicaAssignment: map[int32][]int32{0: {3}},
				},
			}
			bytes, _ := json.Marshal(topicResp)
			_, _ = w.Write(bytes)
			wg.Done()
		}
	})
	defer ts.Close()
	kafkaClient := newKafkaClient(ts.URL)
	topic, err := kafkaClient.GetTopic(ctx, classifier.New("test"))
	assertions.NoError(err)
	assertions.NotNil(topic)
	expectedTopic := model.TopicAddress{
		Classifier:      classifier.New("test.topic").WithNamespace(testNamespace),
		TopicName:       "maas.test-namespace.test.topic",
		NumPartitions:   1,
		BoostrapServers: map[string][]string{"PLAINTEXT": {"server1:9092", "server2:9092"}},
		Credentials:     map[string]model.TopicUserCredentials{"PLAIN": {Username: "client", Password: "client"}},
		CACert:          "",
	}
	assertions.Equal(expectedTopic, *topic)
	topic, err = kafkaClient.GetTopic(ctx, classifier.New("test"))
	assertions.NoError(err)
	assertions.Equal(expectedTopic, *topic)
	assertions.True(waitWG(waitTimeout, wg))
}

func Test_GetTopicWithRetry(t *testing.T) {
	assertions := require.New(t)
	ctx := context.Background()
	attempts := defaultRetryAttempts - 1
	ts := createTestServer(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == resty.MethodPost && r.URL.Path == "/api/v1/kafka/topic/get-by-classifier" {
			assertions.Equal(fmt.Sprintf("Bearer %s", testToken), r.Header.Get("Authorization"))
			if attempts > 0 {
				attempts--
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			w.WriteHeader(http.StatusOK)
			topicResp := TopicResponse{
				Addresses:         map[string][]string{"PLAINTEXT": {"server1:9092", "server2:9092"}},
				Name:              "maas.test-namespace.test.topic",
				Classifier:        map[string]string{"name": "test.topic", "namespace": testNamespace},
				Namespace:         testNamespace,
				ExternallyManaged: false,
				Instance:          "test-kafka",
				CACert:            "",
				Credentials:       map[string][]map[string]any{"client": {map[string]any{"type": "PLAIN", "username": "client", "password": "plain:client"}}},
				RequestedSettings: &TopicSettings{
					NumPartitions:     1,
					ReplicationFactor: func(val int) *int { return &val }(1),
				},
				ActualSettings: &TopicSettings{
					NumPartitions:     1,
					ReplicationFactor: func(val int) *int { return &val }(1),
					ReplicaAssignment: map[int32][]int32{0: {3}},
				},
			}
			bytes, _ := json.Marshal(topicResp)
			_, _ = w.Write(bytes)
		}
	})
	defer ts.Close()
	kafkaClient := newKafkaClient(ts.URL)
	topic, err := kafkaClient.GetTopic(ctx, classifier.New("test"))
	assertions.NoError(err)
	assertions.NotNil(topic)
	expectedTopic := model.TopicAddress{
		Classifier:      classifier.New("test.topic").WithNamespace(testNamespace),
		TopicName:       "maas.test-namespace.test.topic",
		NumPartitions:   1,
		BoostrapServers: map[string][]string{"PLAINTEXT": {"server1:9092", "server2:9092"}},
		Credentials:     map[string]model.TopicUserCredentials{"PLAIN": {Username: "client", Password: "client"}},
		CACert:          "",
	}
	assertions.Equal(expectedTopic, *topic)
}

func Test_DeleteTopic(t *testing.T) {
	assertions := require.New(t)
	ctx := context.Background()
	wg := &sync.WaitGroup{}
	wg.Add(2)
	ts := createTestServer(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == resty.MethodDelete && r.URL.Path == "/api/v1/kafka/topic" {
			assertions.Equal(fmt.Sprintf("Bearer %s", testToken), r.Header.Get("Authorization"))
			w.WriteHeader(http.StatusOK)
			wg.Done()
		}
	})
	defer ts.Close()
	kafkaClient := newKafkaClient(ts.URL)
	err := kafkaClient.DeleteTopic(ctx, classifier.New("test"))
	assertions.NoError(err)
	err = kafkaClient.DeleteTopic(ctx, classifier.New("test"))
	assertions.NoError(err)
	assertions.True(waitWG(waitTimeout, wg))
}

func Test_DeleteTopicWithRetry(t *testing.T) {
	assertions := require.New(t)
	ctx := context.Background()
	attempts := defaultRetryAttempts - 1
	ts := createTestServer(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == resty.MethodDelete && r.URL.Path == "/api/v1/kafka/topic" {
			assertions.Equal(fmt.Sprintf("Bearer %s", testToken), r.Header.Get("Authorization"))
			if attempts > 0 {
				attempts--
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			w.WriteHeader(http.StatusOK)
		}
	})
	defer ts.Close()
	kafkaClient := newKafkaClient(ts.URL)
	err := kafkaClient.DeleteTopic(ctx, classifier.New("test"))
	assertions.NoError(err)
}

func newKafkaClient(agentUrl string) *MaasClient {
	crudClient := &CrudClient{MaasAgentUrl: agentUrl, Namespace: testNamespace, HttpClient: resty.New(), Auth: func(ctx context.Context) (string, error) {
		return testToken, nil
	}, RetryAttempts: defaultRetryAttempts, RetryInterval: defaultRetryInterval}
	return &MaasClient{namespace: testNamespace, crudClient: crudClient}
}

func createTestServer(fn func(w http.ResponseWriter, r *http.Request)) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(fn))
}

func waitWG(timeout time.Duration, groups ...*sync.WaitGroup) bool {
	finalWg := &sync.WaitGroup{}
	finalWg.Add(len(groups))
	c := make(chan struct{})
	for _, wg := range groups {
		go func(wg *sync.WaitGroup) {
			wg.Wait()
			finalWg.Done()
		}(wg)
	}
	go func() {
		finalWg.Wait()
		c <- struct{}{}
	}()

	timer := time.NewTimer(timeout)
	select {
	case <-c:
		timer.Stop()
		return true
	case <-timer.C:
		return false
	}
}
