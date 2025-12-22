package consulorders

import (
    "context"
    "fmt"
    "time"

    "github.com/umitbozkurt/consul-replctl/internal/orders"
    "github.com/umitbozkurt/consul-replctl/internal/store"
    "github.com/umitbozkurt/consul-replctl/internal/types"
)

type Provider struct {
    KV store.KV

    MongoOrdersPrefix string
    MongoAckPrefix string
    KafkaOrdersPrefix string
    KafkaAckPrefix string

    KafkaCandidatesPrefix string

    KafkaLastAppliedKey string
    MongoLastAppliedKey string
}

func (p Provider) PublishMongoSpec(ctx context.Context, spec types.ReplicaSpec) error {
    epoch := spec.Version
    for _, id := range spec.Members {
        if spec.MongoWipeMembers != nil && spec.MongoWipeMembers[id] {
            _ = p.issueAndWait(ctx, orders.KindMongo, id, orders.ActionWipe, epoch, nil)
        }
        if err := p.issueAndWait(ctx, orders.KindMongo, id, orders.ActionStart, epoch, map[string]any{
            "replSetName": spec.MongoReplicaSetID,
        }); err != nil {
            return err
        }
    }
    if len(spec.Members) > 0 {
        _ = p.issueAndWait(ctx, orders.KindMongo, spec.Members[0], orders.ActionInit, epoch, map[string]any{
            "members": spec.Members,
            "replSetName": spec.MongoReplicaSetID,
        })
    }
    if p.MongoLastAppliedKey != "" {
        _ = p.KV.PutJSON(ctx, p.MongoLastAppliedKey, &spec)
    }
    return nil
}

func (p Provider) PublishKafkaSpec(ctx context.Context, spec types.ReplicaSpec) error {
    epoch := spec.Version
    if p.KafkaLastAppliedKey == "" {
        p.KafkaLastAppliedKey = "provider/kafka/last_applied_spec"
    }
    if p.KafkaCandidatesPrefix == "" {
        p.KafkaCandidatesPrefix = "candidates/kafka"
    }

    var old types.ReplicaSpec
    _, _ = p.KV.GetJSON(ctx, p.KafkaLastAppliedKey, &old)

    var candidates []types.CandidateReport
    _ = p.KV.ListJSON(ctx, p.KafkaCandidatesPrefix, &candidates)

    ctrlAddrByID := map[string]string{}
    nodeIDByID := map[string]string{}
    for _, c := range candidates {
        if c.ID == "" { continue }
        if c.KafkaControllerAddr != "" { ctrlAddrByID[c.ID] = c.KafkaControllerAddr }
        if c.KafkaNodeID != "" { nodeIDByID[c.ID] = c.KafkaNodeID }
    }

    added, removed := diffMembers(old.Members, spec.Members)

    coordinatorID := ""
    bootstrap := ""
    if len(spec.Members) > 0 {
        coordinatorID = spec.Members[0]
        bootstrap = ctrlAddrByID[coordinatorID]
    }
    if bootstrap == "" && len(spec.KafkaBootstrapServers) > 0 {
        bootstrap = spec.KafkaBootstrapServers[0]
    }

    // remove voters first
    for _, id := range removed {
        voterID := nodeIDByID[id]
        if voterID != "" && coordinatorID != "" && bootstrap != "" {
            _ = p.issueAndWait(ctx, orders.KindKafka, coordinatorID, orders.ActionRemoveVoter, epoch, map[string]any{
                "bootstrapServer": bootstrap,
                "voterId": voterID,
            })
        }
        _ = p.issueAndWait(ctx, orders.KindKafka, id, orders.ActionStop, epoch, nil)
    }

    // start added members
    for _, id := range added {
        _ = p.issueAndWait(ctx, orders.KindKafka, id, orders.ActionStart, epoch, map[string]any{
            "bootstrapServers": spec.KafkaBootstrapServers,
        })
    }

    // add voters
    for _, id := range added {
        voterID := nodeIDByID[id]
        endpoint := ctrlAddrByID[id]
        if voterID == "" || endpoint == "" || coordinatorID == "" || bootstrap == "" {
            continue
        }
        _ = p.issueAndWait(ctx, orders.KindKafka, coordinatorID, orders.ActionAddVoter, epoch, map[string]any{
            "bootstrapServer": bootstrap,
            "voterId": voterID,
            "voterEndpoint": endpoint,
        })
    }

    // rebalance partitions
    if (len(added) > 0 || len(removed) > 0) && coordinatorID != "" && bootstrap != "" {
        _ = p.issueAndWait(ctx, orders.KindKafka, coordinatorID, orders.ActionReassignPartitions, epoch, map[string]any{
            "bootstrapServer": bootstrap,
        })
    }

    // ensure started
    for _, id := range spec.Members {
        _ = p.issueAndWait(ctx, orders.KindKafka, id, orders.ActionStart, epoch, map[string]any{
            "bootstrapServers": spec.KafkaBootstrapServers,
        })
    }

    _ = p.KV.PutJSON(ctx, p.KafkaLastAppliedKey, &spec)
    return nil
}

func diffMembers(old, neu []string) (added []string, removed []string) {
    o := map[string]bool{}
    n := map[string]bool{}
    for _, x := range old { if x != "" { o[x]=true } }
    for _, x := range neu { if x != "" { n[x]=true } }
    for x := range n { if !o[x] { added = append(added,x) } }
    for x := range o { if !n[x] { removed = append(removed,x) } }
    return
}

func (p Provider) issueAndWait(ctx context.Context, kind orders.Kind, target string, action orders.Action, epoch int64, payload map[string]any) error {
    var op, ap string
    if kind == orders.KindMongo {
        op, ap = p.MongoOrdersPrefix, p.MongoAckPrefix
    } else {
        op, ap = p.KafkaOrdersPrefix, p.KafkaAckPrefix
    }
    ord := orders.Order{Kind: kind, TargetID: target, Action: action, Payload: payload, IssuedAt: time.Now(), Epoch: epoch}
    if err := p.KV.PutJSON(ctx, fmt.Sprintf("%s/%s", op, target), &ord); err != nil { return err }

    deadline := time.Now().Add(60*time.Second)
    for time.Now().Before(deadline) {
        var ack orders.Ack
        ok, err := p.KV.GetJSON(ctx, fmt.Sprintf("%s/%s", ap, target), &ack)
        if err == nil && ok && ack.Epoch == epoch && ack.Action == action {
            if ack.Ok { return nil }
            return fmt.Errorf("%s %s failed: %s", kind, action, ack.Message)
        }
        select {
        case <-ctx.Done():
            return ctx.Err()
        case <-time.After(1*time.Second):
        }
    }
    return nil
}
