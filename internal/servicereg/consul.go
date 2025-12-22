package servicereg

import (
    "context"
    "fmt"

    capi "github.com/hashicorp/consul/api"
)

type ConsulRegistry struct {
    c *capi.Client
}

func NewConsulRegistry(c *capi.Client) *ConsulRegistry { return &ConsulRegistry{c: c} }

func (r *ConsulRegistry) Register(ctx context.Context, reg Registration) error {
    s := &capi.AgentServiceRegistration{
        ID:      reg.ID,
        Name:    reg.Name,
        Address: reg.Address,
        Port:    reg.Port,
        Tags:    reg.Tags,
        Check: &capi.AgentServiceCheck{
            TTL:     reg.TTL,
            CheckID: reg.CheckID,
        },
    }
    return r.c.Agent().ServiceRegister(s)
}

func (r *ConsulRegistry) Deregister(ctx context.Context, id string) error {
    return r.c.Agent().ServiceDeregister(id)
}

func (r *ConsulRegistry) SetTTL(ctx context.Context, checkID string, status Status, note string) error {
    switch status {
    case StatusPassing:
        return r.c.Agent().PassTTL(checkID, note)
    case StatusWarning:
        return r.c.Agent().WarnTTL(checkID, note)
    case StatusCritical:
        return r.c.Agent().FailTTL(checkID, note)
    default:
        return fmt.Errorf("unknown status: %s", status)
    }
}
