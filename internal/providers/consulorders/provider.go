package consulorders

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"reflect"
	"strings"
	"time"

	"github.com/umitbozkurt/consul-replctl/internal/orders"
	"github.com/umitbozkurt/consul-replctl/internal/store"
	"github.com/umitbozkurt/consul-replctl/internal/types"
	"golang.org/x/sync/errgroup"
)

type Provider struct {
	KV store.KV

	MongoOrdersPrefix     string
	MongoAckPrefix        string
	MongoCandidatesPrefix string
	MongoHealthPrefix     string
	KafkaOrdersPrefix     string
	KafkaAckPrefix        string

	KafkaCandidatesPrefix string

	KafkaLastAppliedKey string
	MongoLastAppliedKey string

	OrderHistoryKeep int
}

func (p Provider) PublishMongoSpec(ctx context.Context, spec types.ReplicaSpec) error {
	epoch := spec.Version
	if p.MongoCandidatesPrefix == "" {
		p.MongoCandidatesPrefix = "candidates/mongo"
	}
	if p.MongoHealthPrefix == "" {
		p.MongoHealthPrefix = "health/mongo"
	}
	if p.MongoLastAppliedKey == "" {
		p.MongoLastAppliedKey = "provider/mongo/last_applied_spec"
	}
	var prev types.ReplicaSpec
	prevExists, _ := p.KV.GetJSON(ctx, p.MongoLastAppliedKey, &prev)

	candByID := map[string]types.CandidateReport{}
	var cands []types.CandidateReport
	if err := p.KV.ListJSON(ctx, p.MongoCandidatesPrefix, &cands); err == nil {
		for _, c := range cands {
			candByID[c.ID] = c
		}
	}
	hasReplicaConfig := false
	for _, c := range cands {
		if c.LastSeenReplicaSetID != "" || c.LastSeenReplicaSetUUID != "" {
			hasReplicaConfig = true
			break
		}
	}
	healthByID := map[string]types.HealthStatus{}
	if p.MongoHealthPrefix != "" {
		var health []types.HealthStatus
		if err := p.KV.ListJSON(ctx, p.MongoHealthPrefix, &health); err == nil {
			for _, h := range health {
				if h.ID != "" {
					healthByID[h.ID] = h
				}
			}
		}
	}
	buildMembers := func(ids []string) []any {
		out := make([]any, 0, len(ids))
		for _, id := range ids {
			host := id
			var memberID *int
			if c, ok := candByID[id]; ok {
				h := c.Addr
				if h == "" {
					h = c.Host
				}
				if h != "" {
					if strings.Contains(h, ":") {
						host = h
					} else {
						host = fmt.Sprintf("%s:%d", h, 27017)
					}
				}
				if c.MongoMemberID != nil {
					memberID = c.MongoMemberID
				}
			}
			member := map[string]any{"host": host}
			if memberID != nil {
				member["id"] = *memberID
			}
			out = append(out, member)
		}
		return out
	}
	replMembers := buildMembers(spec.Members)

	_, removed := diffMembers(prev.Members, spec.Members)

	startTasks := make([]orderTask, 0, len(spec.Members))
	for _, id := range spec.Members {
		id := id
		startTasks = append(startTasks, func(ctx context.Context) error {
			if spec.MongoWipeMembers != nil && spec.MongoWipeMembers[id] {
				_ = p.issueAndWait(ctx, orders.KindMongo, id, orders.ActionWipe, epoch, nil)
			}
			return p.issueAndWait(ctx, orders.KindMongo, id, orders.ActionStart, epoch, map[string]any{
				"replSetName": spec.MongoReplicaSetID,
			})
		})
	}
	if err := runParallel(ctx, startTasks); err != nil {
		return err
	}
	if len(spec.Members) > 0 {
		payload := map[string]any{
			"members":     replMembers,
			"replSetName": spec.MongoReplicaSetID,
		}
		hasHealthyReplica := hasHealthyReplicaMember(healthByID)
		hasHealthReplicaConfig := hasReplicaConfigFromHealth(healthByID, spec.Members)
		shouldInit := (!prevExists || len(prev.Members) == 0) && !hasReplicaConfig && !hasHealthyReplica && !hasHealthReplicaConfig
		shouldReconfig := !shouldInit
		reconfigReasons := []string{}
		if prevExists && len(prev.Members) > 0 {
			sameMembers := reflect.DeepEqual(prev.Members, spec.Members)
			sameSetID := prev.MongoReplicaSetID == spec.MongoReplicaSetID
			sameUUID := strings.EqualFold(prev.MongoReplicaSetUUID, spec.MongoReplicaSetUUID)
			shouldReconfig = !(sameMembers && sameSetID && sameUUID)
			if shouldReconfig {
				if !sameMembers {
					reconfigReasons = append(reconfigReasons, fmt.Sprintf("members changed (prev=%v new=%v)", prev.Members, spec.Members))
				}
				if !sameSetID {
					reconfigReasons = append(reconfigReasons, fmt.Sprintf("replicaSetID changed (prev=%q new=%q)", prev.MongoReplicaSetID, spec.MongoReplicaSetID))
				}
				if !sameUUID {
					reconfigReasons = append(reconfigReasons, fmt.Sprintf("replicaSetUUID changed (prev=%q new=%q)", prev.MongoReplicaSetUUID, spec.MongoReplicaSetUUID))
				}
			}
		} else if shouldReconfig {
			reconfigReasons = append(reconfigReasons, "previous spec missing/empty")
			if hasReplicaConfig {
				reconfigReasons = append(reconfigReasons, "existing replica config detected")
			}
			if hasHealthyReplica {
				reconfigReasons = append(reconfigReasons, "healthy replica member detected")
			}
			if hasHealthReplicaConfig {
				reconfigReasons = append(reconfigReasons, "health reports replica config")
			}
		}

		if shouldInit {
			target := spec.Members[0]
			_ = p.issueAndWait(ctx, orders.KindMongo, target, orders.ActionInit, epoch, payload)
		} else if shouldReconfig {
			if len(reconfigReasons) == 0 {
				reconfigReasons = append(reconfigReasons, "spec change detected")
			}
			log.Printf("[orders] mongo reconfigure reason=%s", strings.Join(reconfigReasons, ", "))
			// Try reconfig on a writable PRIMARY first, then force if it still fails.
			reconfigTargets := mergeMembers(spec.Members, prev.Members)
			primary := selectPrimaryTarget(reconfigTargets, healthByID)
			orderedTargets := moveToFront(reconfigTargets, primary)
			if err := p.issueReconfigTargets(ctx, orderedTargets, epoch, payload); err != nil {
				log.Printf("[orders] reconfigure (non-force) failed: %v; retrying with force", err)
				forceTarget := selectForceTarget(primary, orderedTargets, healthByID)
				if forceTarget != "" {
					forced := copyMap(payload)
					forced["force"] = true
					_ = p.issueAndWait(ctx, orders.KindMongo, forceTarget, orders.ActionReconfigure, epoch, forced)
				}
			}
		} else {
			log.Printf("[orders] replica set config unchanged; skipping reconfigure")
		}
	}

	// Stop members that are no longer part of the spec after config changes.
	for _, id := range removed {
		if err := p.issue(ctx, orders.KindMongo, id, orders.ActionStop, epoch, nil); err != nil {
			log.Printf("[orders] stop issue failed kind=%s target=%s action=%s epoch=%d err=%v", orders.KindMongo, id, orders.ActionStop, epoch, err)
		}
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
		if c.ID == "" {
			continue
		}
		if c.KafkaControllerAddr != "" {
			ctrlAddrByID[c.ID] = c.KafkaControllerAddr
		}
		if c.KafkaNodeID != "" {
			nodeIDByID[c.ID] = c.KafkaNodeID
		}
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
	stopTasks := make([]orderTask, 0, len(removed))
	for _, id := range removed {
		voterID := nodeIDByID[id]
		if voterID != "" && coordinatorID != "" && bootstrap != "" {
			_ = p.issueAndWait(ctx, orders.KindKafka, coordinatorID, orders.ActionRemoveVoter, epoch, map[string]any{
				"bootstrapServer": bootstrap,
				"voterId":         voterID,
			})
		}
		id := id
		stopTasks = append(stopTasks, func(ctx context.Context) error {
			return p.issueAndWait(ctx, orders.KindKafka, id, orders.ActionStop, epoch, nil)
		})
	}
	runParallelBestEffort(ctx, stopTasks)

	// start added members
	startTasks := make([]orderTask, 0, len(added))
	for _, id := range added {
		id := id
		startTasks = append(startTasks, func(ctx context.Context) error {
			return p.issueAndWait(ctx, orders.KindKafka, id, orders.ActionStart, epoch, map[string]any{
				"bootstrapServers": spec.KafkaBootstrapServers,
			})
		})
	}
	runParallelBestEffort(ctx, startTasks)

	// add voters
	for _, id := range added {
		voterID := nodeIDByID[id]
		endpoint := ctrlAddrByID[id]
		if voterID == "" || endpoint == "" || coordinatorID == "" || bootstrap == "" {
			continue
		}
		_ = p.issueAndWait(ctx, orders.KindKafka, coordinatorID, orders.ActionAddVoter, epoch, map[string]any{
			"bootstrapServer": bootstrap,
			"voterId":         voterID,
			"voterEndpoint":   endpoint,
		})
	}

	// rebalance partitions
	if (len(added) > 0 || len(removed) > 0) && coordinatorID != "" && bootstrap != "" {
		_ = p.issueAndWait(ctx, orders.KindKafka, coordinatorID, orders.ActionReassignPartitions, epoch, map[string]any{
			"bootstrapServer": bootstrap,
		})
	}

	// ensure started
	ensureTasks := make([]orderTask, 0, len(spec.Members))
	for _, id := range spec.Members {
		id := id
		ensureTasks = append(ensureTasks, func(ctx context.Context) error {
			return p.issueAndWait(ctx, orders.KindKafka, id, orders.ActionStart, epoch, map[string]any{
				"bootstrapServers": spec.KafkaBootstrapServers,
			})
		})
	}
	runParallelBestEffort(ctx, ensureTasks)

	_ = p.KV.PutJSON(ctx, p.KafkaLastAppliedKey, &spec)
	return nil
}

func diffMembers(old, neu []string) (added []string, removed []string) {
	o := map[string]bool{}
	n := map[string]bool{}
	for _, x := range old {
		if x != "" {
			o[x] = true
		}
	}
	for _, x := range neu {
		if x != "" {
			n[x] = true
		}
	}
	for x := range n {
		if !o[x] {
			added = append(added, x)
		}
	}
	for x := range o {
		if !n[x] {
			removed = append(removed, x)
		}
	}
	return
}

type orderTask func(context.Context) error

func runParallel(ctx context.Context, tasks []orderTask) error {
	if len(tasks) == 0 {
		return nil
	}
	g, ctx := errgroup.WithContext(ctx)
	for _, task := range tasks {
		task := task
		g.Go(func() error {
			return task(ctx)
		})
	}
	return g.Wait()
}

func runParallelBestEffort(ctx context.Context, tasks []orderTask) {
	if len(tasks) == 0 {
		return
	}
	var g errgroup.Group
	for _, task := range tasks {
		task := task
		g.Go(func() error {
			_ = task(ctx)
			return nil
		})
	}
	_ = g.Wait()
}

func (p Provider) issueAndWait(ctx context.Context, kind orders.Kind, target string, action orders.Action, epoch int64, payload map[string]any) error {
	var ap string
	if kind == orders.KindMongo {
		ap = p.MongoAckPrefix
	} else {
		ap = p.KafkaAckPrefix
	}
	if err := p.issue(ctx, kind, target, action, epoch, payload); err != nil {
		return err
	}

	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		var ack orders.Ack
		ok, err := p.KV.GetJSON(ctx, fmt.Sprintf("%s/%s", ap, target), &ack)
		if err == nil && ok && ack.Epoch == epoch && ack.Action == action {
			if ack.Ok {
				log.Printf("[orders] ack received kind=%s target=%s action=%s epoch=%d", kind, target, action, epoch)
				return nil
			}
			return fmt.Errorf("%s %s failed: %s", kind, action, ack.Message)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(1 * time.Second):
		}
	}
	log.Printf("[orders] ack timeout kind=%s target=%s action=%s epoch=%d", kind, target, action, epoch)
	return nil
}

func (p Provider) issue(ctx context.Context, kind orders.Kind, target string, action orders.Action, epoch int64, payload map[string]any) error {
	var op string
	if kind == orders.KindMongo {
		op = p.MongoOrdersPrefix
	} else {
		op = p.KafkaOrdersPrefix
	}
	orderKey := fmt.Sprintf("%s/%s", op, target)
	ord := orders.Order{Kind: kind, TargetID: target, Action: action, Payload: payload, IssuedAt: time.Now(), Epoch: epoch}
	if err := orders.SaveWithHistory(ctx, p.KV, orderKey, ord, p.OrderHistoryKeep); err != nil {
		return err
	}
	log.Printf("[orders] publish kind=%s target=%s action=%s epoch=%d payload=%v", kind, target, action, epoch, payload)
	return nil
}

func (p Provider) issueReconfigTargets(ctx context.Context, targets []string, epoch int64, payload map[string]any) error {
	var lastErr error
	for _, target := range targets {
		if target == "" {
			continue
		}
		err := p.issueAndWait(ctx, orders.KindMongo, target, orders.ActionReconfigure, epoch, payload)
		if err == nil {
			return nil
		}
		if isNotWritablePrimary(err) {
			lastErr = err
			log.Printf("[orders] reconfigure target=%s not writable primary; trying next", target)
			continue
		}
		return err
	}
	return lastErr
}

func mergeMembers(primary []string, extra []string) []string {
	out := make([]string, 0, len(primary)+len(extra))
	seen := map[string]bool{}
	for _, id := range primary {
		if id == "" || seen[id] {
			continue
		}
		seen[id] = true
		out = append(out, id)
	}
	for _, id := range extra {
		if id == "" || seen[id] {
			continue
		}
		seen[id] = true
		out = append(out, id)
	}
	return out
}

func moveToFront(list []string, preferred string) []string {
	if preferred == "" {
		return list
	}
	found := false
	for _, id := range list {
		if id == preferred {
			found = true
			break
		}
	}
	if !found {
		return list
	}
	out := make([]string, 0, len(list))
	out = append(out, preferred)
	for _, id := range list {
		if id == preferred {
			continue
		}
		out = append(out, id)
	}
	return out
}

func selectPrimaryTarget(targets []string, healthByID map[string]types.HealthStatus) string {
	for _, id := range targets {
		h, ok := healthByID[id]
		if !ok || !h.Healthy {
			continue
		}
		if isMongoPrimary(h.Reason) {
			return id
		}
	}
	return ""
}

func selectForceTarget(primary string, targets []string, healthByID map[string]types.HealthStatus) string {
	if primary != "" {
		return primary
	}
	for _, id := range targets {
		if h, ok := healthByID[id]; ok && h.Healthy {
			return id
		}
	}
	if len(targets) > 0 {
		return targets[0]
	}
	return ""
}

func isMongoPrimary(reason string) bool {
	state, stateStr, ok := parseMongoState(reason)
	if !ok {
		return false
	}
	if state == 1 {
		return true
	}
	return strings.EqualFold(stateStr, "PRIMARY")
}

func hasHealthyReplicaMember(healthByID map[string]types.HealthStatus) bool {
	for _, h := range healthByID {
		if !h.Healthy {
			continue
		}
		state, stateStr, ok := parseMongoState(h.Reason)
		if !ok {
			continue
		}
		if state == 1 || state == 2 || state == 7 {
			return true
		}
		if strings.EqualFold(stateStr, "PRIMARY") || strings.EqualFold(stateStr, "SECONDARY") || strings.EqualFold(stateStr, "ARBITER") {
			return true
		}
	}
	return false
}

func hasReplicaConfigFromHealth(healthByID map[string]types.HealthStatus, members []string) bool {
	if len(healthByID) == 0 || len(members) == 0 {
		return false
	}
	memberSet := map[string]bool{}
	for _, id := range members {
		if id != "" {
			memberSet[id] = true
		}
	}
	for id, h := range healthByID {
		if !memberSet[id] {
			continue
		}
		_, _, rsid, ok := parseMongoNote(h.Reason)
		if ok && rsid != "" {
			return true
		}
	}
	return false
}

func parseMongoState(reason string) (int, string, bool) {
	state, stateStr, _, ok := parseMongoNote(reason)
	return state, stateStr, ok
}

func parseMongoNote(reason string) (int, string, string, bool) {
	if reason == "" {
		return 0, "", "", false
	}
	var note struct {
		Mongo struct {
			State         int    `json:"state"`
			StateStr      string `json:"stateStr"`
			ReplicaSetID  string `json:"replicaSetId"`
			ReplicaSetID2 string `json:"replicaSetID"`
			ReplicaSetID3 string `json:"replicasetid"`
		} `json:"mongo"`
	}
	if err := json.Unmarshal([]byte(reason), &note); err == nil {
		rsid := note.Mongo.ReplicaSetID
		if rsid == "" {
			rsid = note.Mongo.ReplicaSetID2
		}
		if rsid == "" {
			rsid = note.Mongo.ReplicaSetID3
		}
		if note.Mongo.State != 0 || note.Mongo.StateStr != "" || rsid != "" {
			return note.Mongo.State, note.Mongo.StateStr, rsid, true
		}
	}
	upper := strings.ToUpper(reason)
	switch {
	case strings.Contains(upper, "PRIMARY"):
		return 1, "PRIMARY", "", true
	case strings.Contains(upper, "SECONDARY"):
		return 2, "SECONDARY", "", true
	case strings.Contains(upper, "ARBITER"):
		return 7, "ARBITER", "", true
	case strings.Contains(upper, "REMOVED"):
		return 10, "REMOVED", "", true
	}
	return 0, "", "", false
}

func isNotWritablePrimary(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "notwritableprimary") ||
		strings.Contains(msg, "not writable primary") ||
		strings.Contains(msg, "not primary") ||
		strings.Contains(msg, "not master")
}

func copyMap(in map[string]any) map[string]any {
	out := make(map[string]any, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}
