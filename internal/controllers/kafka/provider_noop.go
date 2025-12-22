package kafka

import (
    "context"
    "log"

    "github.com/umitbozkurt/consul-replctl/internal/types"
)

type NoopProvider struct{}

func (p NoopProvider) PublishSpec(ctx context.Context, spec types.ReplicaSpec) error {
    log.Printf("[kafka] noop provider: would publish spec to runtime: %+v", spec)
    return nil
}
