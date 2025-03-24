package classifier

import (
	"encoding/json"
	"fmt"
)

type Keys map[string]string

const (
	Name      = "name"
	Namespace = "namespace"
	TenantId  = "tenantId"
)

func New(name string) Keys {
	return Keys{Name: name}
}

func (c Keys) WithNamespace(namespace string) Keys {
	c[Namespace] = namespace
	return c
}

func (c Keys) WithTenantId(tenantId string) Keys {
	c[TenantId] = tenantId
	return c
}

func (c Keys) WithParam(name, value string) Keys {
	c[name] = value
	return c
}

func (c Keys) AsString() string {
	marshal, err := json.Marshal(c)
	if err != nil {
		// should never happen
		panic(fmt.Errorf("failed to marshal classifier '%v' to string. Cause: %w", c, err))
	}
	return string(marshal)
}
