package kafka

import (
    "context"

    "github.com/umitbozkurt/consul-replctl/internal/types"
)

type Provider interface {
    PublishSpec(ctx context.Context, spec types.ReplicaSpec) error
}
