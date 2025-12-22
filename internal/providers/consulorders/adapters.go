package consulorders

import (
    "context"

    kctl "github.com/umitbozkurt/consul-replctl/internal/controllers/kafka"
    mctl "github.com/umitbozkurt/consul-replctl/internal/controllers/mongo"
    "github.com/umitbozkurt/consul-replctl/internal/types"
)

type MongoAdapter struct{ P Provider }
func (a MongoAdapter) PublishSpec(ctx context.Context, spec types.ReplicaSpec) error { return a.P.PublishMongoSpec(ctx, spec) }
func (a MongoAdapter) ReadHealth(ctx context.Context) ([]types.HealthStatus, error) { return nil, nil }
var _ mctl.Provider = MongoAdapter{}

type KafkaAdapter struct{ P Provider }
func (a KafkaAdapter) PublishSpec(ctx context.Context, spec types.ReplicaSpec) error { return a.P.PublishKafkaSpec(ctx, spec) }
var _ kctl.Provider = KafkaAdapter{}
