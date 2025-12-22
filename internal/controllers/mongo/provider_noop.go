package mongo

import (
    "context"
    "log"

    "github.com/umitbozkurt/consul-replctl/internal/types"
)

type NoopProvider struct{}

func (p NoopProvider) PublishSpec(ctx context.Context, spec types.ReplicaSpec) error {
    log.Printf("[mongo] noop provider: would publish spec to runtime: %+v", spec)
    return nil
}

func (p NoopProvider) ReadHealth(ctx context.Context) ([]types.HealthStatus, error) {
    return nil, nil
}
