package mongo

import (
    "context"

    "github.com/umitbozkurt/consul-replctl/internal/types"
)

// Provider abstracts "do the real work" (Nomad jobs, mongosh rs.reconfig, etc).
// Controller only decides desired spec + replacement; execution should be implemented here.
type Provider interface {
    PublishSpec(ctx context.Context, spec types.ReplicaSpec) error
    ReadHealth(ctx context.Context) ([]types.HealthStatus, error)
}
