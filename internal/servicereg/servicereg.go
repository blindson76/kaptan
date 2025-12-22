package servicereg

import "context"

type Status string

const (
    StatusPassing  Status = "passing"
    StatusWarning  Status = "warning"
    StatusCritical Status = "critical"
)

type Registration struct {
    Name    string
    ID      string
    Address string
    Port    int
    Tags    []string

    CheckID string
    TTL     string
}

type Registry interface {
    Register(ctx context.Context, r Registration) error
    Deregister(ctx context.Context, id string) error
    SetTTL(ctx context.Context, checkID string, status Status, note string) error
}
