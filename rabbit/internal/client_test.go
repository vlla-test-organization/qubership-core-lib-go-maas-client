package internal

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/go-resty/resty/v2"
	"github.com/stretchr/testify/require"
	"github.com/vlla-test-organization/qubership-core-lib-go-maas-client/v3/classifier"
)

const testNamespace = "test-namespace"

func Test_GetOrCreateVhost(t *testing.T) {
	assertions := require.New(t)
	ctx := context.Background()
	ts := createTestServer(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == resty.MethodPost {
			if strings.HasPrefix(r.URL.Path, "/api/v1/rabbit/vhost") {
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte(
					`{
                          "cnn": "amqp://127.0.0.1:5672/namespace.test12",
                          "username": "user",
                          "password": "plain:password"
                      }`,
				))
			}
		}
	})
	defer ts.Close()

	client := &CrudClient{
		MaasAgentUrl: ts.URL,
		Namespace:    testNamespace,
		HttpClient:   resty.New(),
	}

	rabbitClient := NewRabbitClient(testNamespace, client)

	vhost, err := rabbitClient.GetOrCreateVhost(ctx, classifier.New("test"))
	assertions.Nil(err)
	assertions.NotNil(vhost)
	assertions.Equal("amqp://127.0.0.1:5672/namespace.test12", vhost.GetConnectionUri())
	assertions.Equal("user", vhost.GetUsername())

	pass, err := vhost.GetPassword()
	assertions.Nil(err)
	assertions.Equal("password", pass)
}

func Test_GetVhost(t *testing.T) {
	assertions := require.New(t)
	ctx := context.Background()
	ts := createTestServer(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == resty.MethodPost {
			if strings.HasPrefix(r.URL.Path, "/api/v1/rabbit/vhost/get-by-classifier") {
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte(
					`{
                          "vhost": {
							"cnn": "amqp://127.0.0.1:5672/namespace.test12",
                            "username": "user",
                            "password": "plain:password"
	                      }
                      }`,
				))
			}
		}
	})
	defer ts.Close()

	client := &CrudClient{
		MaasAgentUrl: ts.URL,
		Namespace:    testNamespace,
		HttpClient:   resty.New(),
	}

	rabbitClient := NewRabbitClient(testNamespace, client)

	vhost, err := rabbitClient.GetVhost(ctx, classifier.New("test"))
	assertions.Nil(err)
	assertions.NotNil(vhost)
	assertions.Equal("amqp://127.0.0.1:5672/namespace.test12", vhost.Vhost.GetConnectionUri())
	assertions.Equal("user", vhost.Vhost.GetUsername())

	pass, err := vhost.Vhost.GetPassword()
	assertions.Nil(err)
	assertions.Equal("password", pass)
}

func Test_GetVhost_NotFound(t *testing.T) {
	assertions := require.New(t)
	ctx := context.Background()
	ts := createTestServer(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == resty.MethodPost {
			if strings.HasPrefix(r.URL.Path, "/api/v1/rabbit/vhost/get-by-classifier") {
				w.WriteHeader(http.StatusNotFound)
			}
		}
	})
	defer ts.Close()

	client := &CrudClient{
		MaasAgentUrl: ts.URL,
		Namespace:    testNamespace,
		HttpClient:   resty.New(),
	}

	rabbitClient := NewRabbitClient(testNamespace, client)

	vhost, err := rabbitClient.GetVhost(ctx, classifier.New("test"))
	assertions.Nil(err)
	assertions.Nil(vhost)
}

func createTestServer(fn func(w http.ResponseWriter, r *http.Request)) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(fn))
}
