package watch

type Tenant struct {
	ExternalId string       `json:"externalId"`
	Status     TenantStatus `json:"status"`
	Name       string       `json:"name"`
	Namespace  string       `json:"namespace"`
}

type EventType string
type TenantStatus string

const (
	SUBSCRIBED EventType = "SUBSCRIBED"
	CREATED    EventType = "CREATED"
	MODIFIED   EventType = "MODIFIED"
	DELETED    EventType = "DELETED"

	StatusActive    TenantStatus = "ACTIVE"
	StatusCreated   TenantStatus = "CREATED"
	StatusSuspended TenantStatus = "SUSPENDED"
)

type TenantWatchEvent struct {
	Type    EventType `json:"type"`
	Tenants []Tenant  `json:"tenants"`
}
