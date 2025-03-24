package model

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

// let's assume that configs section didn't received from maas-server
func TestTopicAddress_GetConfig(t *testing.T) {
	retentionValue := "50000"
	ta := TopicAddress{Configs: map[string]*string{"retention.ms": &retentionValue}}
	assert.Equal(t, &retentionValue, ta.GetConfig("retention.ms"))
}

// let's assume that configs section didn't received from maas-server
func TestTopicAddress_GetConfig_Empty(t *testing.T) {
	ta := TopicAddress{}
	assert.Nil(t, ta.GetConfig("absent"))
}
