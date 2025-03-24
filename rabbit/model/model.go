package model

import (
	"fmt"
	"strings"
)

type VhostConfig struct {
	Vhost  Vhost
	Config map[string]interface{}
}

type Vhost struct {
	Cnn             string `json:"cnn" example:"amqp://127.0.0.1:5672/namespace.test12"`
	Username        string `json:"username" example:"9757343a78a04057a07ee4215f1b1355"`
	EncodedPassword string `json:"password" example:"plain:504a449065924332b062c4a84e830cbe"`
}

func (vh Vhost) GetConnectionUri() string {
	return vh.Cnn
}

func (vh Vhost) GetUsername() string {
	return vh.Username
}

func (vh Vhost) GetPassword() (string, error) {
	if strings.HasPrefix(vh.EncodedPassword, "plain:") {
		return vh.EncodedPassword[len("plain:"):], nil
	} else {
		return "", fmt.Errorf("Unsupported password encoding format")
	}
}

func (vh Vhost) String() string {
	return fmt.Sprintf("{Cnn:%v Username:%v ****}", vh.Cnn, vh.Username)
}
