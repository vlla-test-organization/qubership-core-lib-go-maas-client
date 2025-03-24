package rabbit

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestNewClient(t *testing.T) {
	client := NewClient("core-dev", "http://maas-agent:8080", nil)
	assert.NotNil(t, client)

	table := client.BuildHeaders(map[string]string{"x-version": "v1"})
	assert.Equal(t, table["x-version"], "v1")
}
