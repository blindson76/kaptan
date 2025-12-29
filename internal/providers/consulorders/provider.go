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
	healthByID := map[string]types.HealthStatus{}
	if p.MongoHealthPrefix != "" {
		healthByID = loadHealthByID(ctx, p.KV, p.MongoHealthPrefix)
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

	_, removed, _ := diffMembers(prev.Members, spec.Members)

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

	if p.MongoHealthPrefix != "" {
		healthByID = loadHealthByID(ctx, p.KV, p.MongoHealthPrefix)
	}
	if len(spec.Members) > 0 {
		payload := map[string]any{
			"members":     replMembers,
			"replSetName": spec.MongoReplicaSetID,
		}

		// Initial/bootstrapping decisions must be based on candidate reports (offline probe).
		// Health is only used to target the current PRIMARY when doing a (non-force) reconfig.
		specHasReplicaConfig := hasReplicaConfigAfterWipe(spec, candByID)
		shouldInit := !specHasReplicaConfig
		shouldReconfig := false
		reconfigReasons := []string{}

		if !shouldInit {
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
			} else {
				// Previous spec missing/empty, but desired members report an existing replica config.
				// Ensure config matches the newly published spec.
				shouldReconfig = true
				reconfigReasons = append(reconfigReasons, "previous spec missing/empty")
				reconfigReasons = append(reconfigReasons, "candidate reports replica config")
			}
		}

		if shouldInit {
			target := selectInitTarget(spec, candByID)
			if err := p.issueAndWait(ctx, orders.KindMongo, target, orders.ActionInit, epoch, payload); err != nil {
				if isAlreadyInitialized(err) {
					// Race/mis-detection: fall back to reconfig path.
					log.Printf("[orders] mongo init target=%s already initialized; switching to reconfigure", target)
					shouldInit = false
					shouldReconfig = true
				} else {
					log.Printf("[orders] mongo init failed target=%s err=%v", target, err)
				}
			}
		}

		if shouldReconfig {
			if len(reconfigReasons) == 0 {
				reconfigReasons = append(reconfigReasons, "spec change detected")
			}
			log.Printf("[orders] mongo reconfigure reason=%s", strings.Join(reconfigReasons, ", "))
			// Try reconfig on a writable PRIMARY first, then force if it still fails.
			reconfigTargets := mergeMembers(spec.Members, prev.Members)
			reconfigTargets = filterWipedTargets(reconfigTargets, spec.MongoWipeMembers)
			primary := selectPrimaryTarget(reconfigTargets, healthByID)
			orderedTargets := moveToFront(reconfigTargets, primary)
			if err := p.issueReconfigTargets(ctx, orderedTargets, epoch, payload); err != nil {
				// If replica set is not initialized, recover by initiating on the most recent member.
				if isNotYetInitialized(err) {
					target := selectInitTarget(spec, candByID)
					log.Printf("[orders] mongo reconfig failed (not initialized); initiating target=%s", target)
					if err2 := p.issueAndWait(ctx, orders.KindMongo, target, orders.ActionInit, epoch, payload); err2 != nil {
						log.Printf("[orders] mongo init (fallback) failed target=%s err=%v", target, err2)
					}
				} else {
					log.Printf("[orders] reconfigure (non-force) failed: %v; retrying with force", err)
					forceTarget := selectForceTarget(primary, orderedTargets, healthByID, candByID, spec.MongoWipeMembers)
					if forceTarget != "" {
						forced := copyMap(payload)
						forced["force"] = true
						if err2 := p.issueAndWait(ctx, orders.KindMongo, forceTarget, orders.ActionReconfigure, epoch, forced); err2 != nil {
							if isNotYetInitialized(err2) {
								target := selectInitTarget(spec, candByID)
								log.Printf("[orders] mongo force reconfig failed (not initialized); initiating target=%s", target)
								if err3 := p.issueAndWait(ctx, orders.KindMongo, target, orders.ActionInit, epoch, payload); err3 != nil {
									log.Printf("[orders] mongo init (fallback) failed target=%s err=%v", target, err3)
								}
							} else {
								log.Printf("[orders] mongo force reconfig failed target=%s err=%v", forceTarget, err2)
							}
						}
					}
				}
			}
		} else if !shouldInit {
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
	existingCluster := len(old.Members) > 0

	var candidates []types.CandidateReport
	_ = p.KV.ListJSON(ctx, p.KafkaCandidatesPrefix, &candidates)

	ctrlAddrByID := map[string]string{}
	nodeIDByID := map[string]string{}
	storageIDByID := map[string]string{}
	candPresent := map[string]bool{}
	for _, c := range candidates {
		if c.ID == "" {
			continue
		}
		candPresent[c.ID] = true
		if c.KafkaControllerAddr != "" {
			ctrlAddrByID[c.ID] = c.KafkaControllerAddr
		}
		if c.KafkaNodeID != "" {
			nodeIDByID[c.ID] = c.KafkaNodeID
		}
		if c.KafkaStorageID != "" {
			storageIDByID[c.ID] = c.KafkaStorageID
		}
	}
	dirIDFromSpec := map[string]string{}
	for k, v := range old.KafkaControllerDirectoryIDs {
		if k != "" && v != "" {
			dirIDFromSpec[k] = v
		}
	}
	memberIDFromSpec := map[string]string{}
	for k, v := range old.KafkaMemberIDs {
		if k != "" && v != "" {
			memberIDFromSpec[k] = v
		}
	}

	added, removed, surrendered := diffMembers(old.Members, spec.Members)
	log.Printf("Diff: added:%v removed:%v surrendered:%v", added, removed, surrendered)
	addedSet := map[string]bool{}
	for _, id := range added {
		if id != "" {
			addedSet[id] = true
		}
	}

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
		log.Printf("[kafka-orders] removing member %s, members:%v dirs:%v", id, memberIDFromSpec, dirIDFromSpec)
		for _, sId := range surrendered {
			if err := p.issueAndWait(ctx, orders.KindKafka, sId, orders.ActionRemoveVoter, epoch, map[string]any{
				"bootstrapServer":         ctrlAddrByID[sId],
				"controller-id":           memberIDFromSpec[id],
				"controller-directory-id": dirIDFromSpec[id],
			}); err == nil {
				log.Printf("[kafka-orders] surrendered member %s removed as voter from %s", id, sId)
				break
			}

		}
	}
	runParallelBestEffort(ctx, stopTasks)

	// start added members
	startTasks := make([]orderTask, 0, len(added))
	for i, id := range added {
		if i == 0 && !existingCluster {
			if err := p.issueAndWait(ctx, orders.KindKafka, id, orders.ActionStart, epoch, map[string]any{
				"bootstrapServers": spec.KafkaBootstrapServers,
				"mode":             "standalone",
			}); err != nil {
				log.Printf("[kafka-orders] failed to start initial controller %s: %v", id, err)
				return err
			}

		} else {

			if err := p.issueAndWait(ctx, orders.KindKafka, id, orders.ActionStart, epoch, map[string]any{
				"bootstrapServers": spec.KafkaBootstrapServers,
				"mode":             "no-initial-controllers",
			}); err != nil {
				log.Printf("[kafka-orders] failed to start initial controller %s: %v", id, err)
				return err
			}

		}
	}
	// if len(added) == 1 && existingCluster {
	// 	id := added[0]
	// 	if err := p.issueAndWait(ctx, orders.KindKafka, id, orders.ActionReassignPartitions, epoch, map[string]any{
	// 		"bootstrapServers": spec.KafkaBootstrapServers,
	// 		"mode":             "no-initial-controllers",
	// 	}); err != nil {
	// 		log.Printf("[kafka-orders] failed to start reassignpartitions %s: %v", id, err)
	// 		return err
	// 	}
	// }
	runParallelBestEffort(ctx, startTasks)

	_ = p.KV.PutJSON(ctx, p.KafkaLastAppliedKey, &spec)
	return nil
}

func diffMembers(old, neu []string) (added []string, removed []string, surrendered []string) {
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
	for x := range o {
		if n[x] {
			surrendered = append(surrendered, x)
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

func loadHealthByID(ctx context.Context, kv store.KV, prefix string) map[string]types.HealthStatus {
	healthByID := map[string]types.HealthStatus{}
	if prefix == "" || kv == nil {
		return healthByID
	}
	var health []types.HealthStatus
	if err := kv.ListJSON(ctx, prefix, &health); err == nil {
		for _, h := range health {
			if h.ID != "" {
				healthByID[h.ID] = h
			}
		}
	}
	return healthByID
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

func hasReplicaConfigAfterWipe(spec types.ReplicaSpec, candByID map[string]types.CandidateReport) bool {
	if len(spec.Members) == 0 {
		return false
	}
	for _, id := range spec.Members {
		if id == "" {
			continue
		}
		if spec.MongoWipeMembers != nil && spec.MongoWipeMembers[id] {
			continue
		}
		c, ok := candByID[id]
		if !ok {
			continue
		}
		if c.LastSeenReplicaSetID != "" || c.LastSeenReplicaSetUUID != "" {
			return true
		}
	}
	return false
}

func filterWipedTargets(targets []string, wipe map[string]bool) []string {
	if len(targets) == 0 || len(wipe) == 0 {
		return targets
	}
	out := make([]string, 0, len(targets))
	for _, id := range targets {
		if id == "" {
			continue
		}
		if wipe != nil && wipe[id] {
			continue
		}
		out = append(out, id)
	}
	return out
}

func selectInitTarget(spec types.ReplicaSpec, candByID map[string]types.CandidateReport) string {
	// Prefer a non-wiped member with the newest optime (term, oplog timestamp).
	// This ensures the initial PRIMARY is the most up-to-date member.
	best := ""
	var bestCand types.CandidateReport
	bestSet := false
	for _, id := range spec.Members {
		if id == "" {
			continue
		}
		if spec.MongoWipeMembers != nil && spec.MongoWipeMembers[id] {
			continue
		}
		c, ok := candByID[id]
		if !ok {
			if best == "" {
				best = id
			}
			continue
		}
		if !bestSet || candidateNewer(c, bestCand) {
			best = id
			bestCand = c
			bestSet = true
		}
	}
	if best != "" {
		return best
	}
	// If every desired member is marked wiped, just pick the first member.
	if len(spec.Members) > 0 {
		return spec.Members[0]
	}
	return ""
}

func selectForceTarget(primary string, targets []string, healthByID map[string]types.HealthStatus, candByID map[string]types.CandidateReport, wipe map[string]bool) string {
	if primary != "" && !(wipe != nil && wipe[primary]) {
		return primary
	}

	// Prefer a healthy (or unknown) target with the newest optime.
	best := ""
	var bestCand types.CandidateReport
	bestSet := false
	for _, id := range targets {
		if id == "" {
			continue
		}
		if wipe != nil && wipe[id] {
			continue
		}
		if h, ok := healthByID[id]; ok && !h.Healthy {
			continue
		}
		c, ok := candByID[id]
		if !ok {
			if best == "" {
				best = id
			}
			continue
		}
		if !bestSet || candidateNewer(c, bestCand) {
			best = id
			bestCand = c
			bestSet = true
		}
	}
	if best != "" {
		return best
	}

	// Fallback: first non-wiped target (if any)
	for _, id := range targets {
		if id != "" && !(wipe != nil && wipe[id]) {
			return id
		}
	}
	return ""
}

func candidateNewer(a, b types.CandidateReport) bool {
	if a.LastTerm != b.LastTerm {
		return a.LastTerm > b.LastTerm
	}
	if !a.LastOplogTs.IsZero() || !b.LastOplogTs.IsZero() {
		if a.LastOplogTs.Equal(b.LastOplogTs) {
			return a.UpdatedAt.After(b.UpdatedAt)
		}
		return a.LastOplogTs.After(b.LastOplogTs)
	}
	return a.UpdatedAt.After(b.UpdatedAt)
}

func isNotYetInitialized(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "notyetinitialized") ||
		strings.Contains(msg, "not yet initialized") ||
		strings.Contains(msg, "no replset config has been received") ||
		strings.Contains(msg, "no replica set config has been received") ||
		strings.Contains(msg, "replica set is not initialized")
}

func isAlreadyInitialized(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "alreadyinitialized") ||
		strings.Contains(msg, "already initialized") ||
		strings.Contains(msg, "already initiated") ||
		strings.Contains(msg, "alreadyinitiated")
}
