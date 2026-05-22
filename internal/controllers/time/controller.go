package timectl

import (
	"context"
	"crypto/sha1"
	"encoding/json"
	"fmt"
	"log"
	"sort"
	"strings"
	"time"

	"github.com/umitbozkurt/consul-replctl/internal/controllers/common"
	"github.com/umitbozkurt/consul-replctl/internal/orders"
	"github.com/umitbozkurt/consul-replctl/internal/store"
	"github.com/umitbozkurt/consul-replctl/internal/timeproto"
)

type Config struct {
	ControllerID string
	LockKey      string
	StateKey     string

	ReportsPrefix string
	OrdersPrefix  string
	AcksPrefix    string

	ReconcileInterval       time.Duration
	MinAgentReports         int
	DefaultMode             string
	ExternalServers         []string
	ExternalLossTimeout     time.Duration
	ExternalRecoveryTimeout time.Duration
	OrphanStratum           int
	OperatorRequestKey      string

	OrderHistoryKeep int
}

type Controller struct {
	cfg    Config
	kv     store.KV
	locker interface {
		Acquire(context.Context, string, string) (func() error, error)
	}

	reports []timeproto.AgentReport
	state   timeproto.ControllerState
}

func New(cfg Config, kv store.KV, locker interface {
	Acquire(context.Context, string, string) (func() error, error)
}) *Controller {
	if cfg.ReconcileInterval <= 0 {
		cfg.ReconcileInterval = 10 * time.Second
	}
	if cfg.MinAgentReports <= 0 {
		cfg.MinAgentReports = 3
	}
	if cfg.DefaultMode == "" {
		cfg.DefaultMode = timeproto.ModeAuto
	}
	if cfg.ExternalLossTimeout <= 0 {
		cfg.ExternalLossTimeout = 30 * time.Second
	}
	if cfg.ExternalRecoveryTimeout <= 0 {
		cfg.ExternalRecoveryTimeout = 60 * time.Second
	}
	if cfg.OrphanStratum <= 0 {
		cfg.OrphanStratum = 5
	}
	if cfg.OrderHistoryKeep < 0 {
		cfg.OrderHistoryKeep = 0
	}
	return &Controller{cfg: cfg, kv: kv, locker: locker}
}

func (c *Controller) Run(ctx context.Context) error {
	return common.RunLeaderLoop(ctx, c.locker, c.cfg.LockKey, c.cfg.ControllerID, c.runActive)
}

func (c *Controller) runActive(ctx context.Context) error {
	if err := c.loadState(ctx); err != nil {
		log.Printf("[time-controller] state load error: %v", err)
	}
	if c.state.Mode == "" {
		c.state.Mode = c.cfg.DefaultMode
	}

	reportCh := c.kv.WatchPrefixJSON(ctx, c.cfg.ReportsPrefix, func() any { return &[]timeproto.AgentReport{} })
	ticker := time.NewTicker(c.cfg.ReconcileInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case v, ok := <-reportCh:
			if !ok {
				return nil
			}
			c.reports = v.([]timeproto.AgentReport)
			c.reconcile(ctx)
		case <-ticker.C:
			c.reconcile(ctx)
		}
	}
}

func (c *Controller) reconcile(ctx context.Context) {
	if len(c.reports) < c.cfg.MinAgentReports {
		log.Printf("[time-controller] waiting reports: have=%d need=%d", len(c.reports), c.cfg.MinAgentReports)
		return
	}
	now := time.Now()
	reports := dedupeReports(c.reports)
	if len(reports) < c.cfg.MinAgentReports {
		log.Printf("[time-controller] waiting unique reports: have=%d need=%d", len(reports), c.cfg.MinAgentReports)
		return
	}

	operator := c.readOperatorRequest(ctx)
	baselineMode, baselineManual := newestOrderMode(reports, c.cfg.DefaultMode)
	targetMode := baselineMode
	targetManual := baselineManual
	switch normalizeMode(operator.Mode) {
	case timeproto.ModeManual:
		targetMode = timeproto.ModeManual
		targetManual = operator.ManualTime
	case timeproto.ModeAuto, timeproto.ModeHandover:
		targetMode = normalizeMode(operator.Mode)
		targetManual = ""
	default:
		anyExternal := anyExternalReachable(reports)
		if !anyExternal {
			if c.state.ExternalDownSince.IsZero() {
				c.state.ExternalDownSince = now
			}
			c.state.ExternalUpSince = time.Time{}
			if now.Sub(c.state.ExternalDownSince) >= c.cfg.ExternalLossTimeout {
				targetMode = timeproto.ModeHandover
				targetManual = ""
			}
		} else {
			c.state.ExternalDownSince = time.Time{}
			if c.state.ExternalUpSince.IsZero() {
				c.state.ExternalUpSince = now
			}
			if targetMode != timeproto.ModeManual && now.Sub(c.state.ExternalUpSince) >= c.cfg.ExternalRecoveryTimeout {
				targetMode = timeproto.ModeAuto
				targetManual = ""
			}
		}
	}

	peerIDs := sortedAgentIDs(reports)
	masterID := ""
	if targetMode == timeproto.ModeHandover {
		masterID = selectMaster(reports)
		if masterID == "" {
			log.Printf("[time-controller] handover requested but no master candidate found")
			return
		}
	}

	decisionHash := hashDecision(targetMode, targetManual, masterID, peerIDs, c.cfg.ExternalServers, c.cfg.OrphanStratum)
	if decisionHash == c.state.LastDecisionHash {
		c.state.Mode = targetMode
		c.state.MasterAgentID = masterID
		c.state.LastManualTime = targetManual
		c.state.UpdatedAt = now
		_ = c.saveState(ctx)
		return
	}

	nextOrderNo := c.state.LastOrderNo + 1
	for _, id := range peerIDs {
		payload := timeproto.OrderPayload{
			OrderNo:         nextOrderNo,
			Mode:            targetMode,
			MasterAgentID:   masterID,
			ManualTime:      targetManual,
			ExternalServers: append([]string{}, c.cfg.ExternalServers...),
			Peers:           append([]string{}, peerIDs...),
			OrphanStratum:   c.cfg.OrphanStratum,
			GeneratedAt:     now.Format(time.RFC3339Nano),
		}
		ord := orders.Order{
			Kind:     orders.KindTime,
			TargetID: id,
			Action:   orders.ActionSetMode,
			Epoch:    nextOrderNo,
			IssuedAt: now,
			Payload:  payloadToMap(payload),
		}
		key := strings.TrimSuffix(c.cfg.OrdersPrefix, "/") + "/" + id
		if err := orders.SaveWithHistory(ctx, c.kv, key, ord, c.cfg.OrderHistoryKeep); err != nil {
			log.Printf("[time-controller] publish order failed key=%s err=%v", key, err)
			return
		}
	}

	c.state.Mode = targetMode
	c.state.MasterAgentID = masterID
	c.state.LastOrderNo = nextOrderNo
	c.state.LastDecisionHash = decisionHash
	c.state.LastManualTime = targetManual
	c.state.UpdatedAt = now
	_ = c.saveState(ctx)
	log.Printf("[time-controller] issued orderNo=%d mode=%s master=%s peers=%d", nextOrderNo, targetMode, masterID, len(peerIDs))
}

func (c *Controller) loadState(ctx context.Context) error {
	ok, err := c.kv.GetJSON(ctx, c.cfg.StateKey, &c.state)
	if err != nil {
		return err
	}
	if !ok {
		c.state = timeproto.ControllerState{}
	}
	return nil
}

func (c *Controller) saveState(ctx context.Context) error {
	if c.cfg.StateKey == "" {
		return nil
	}
	return c.kv.PutJSON(ctx, c.cfg.StateKey, &c.state)
}

func (c *Controller) readOperatorRequest(ctx context.Context) timeproto.OperatorRequest {
	key := c.cfg.OperatorRequestKey
	if key == "" && c.cfg.StateKey != "" {
		key = strings.TrimSuffix(c.cfg.StateKey, "/") + "/operator"
	}
	if key == "" {
		return timeproto.OperatorRequest{}
	}
	var req timeproto.OperatorRequest
	ok, err := c.kv.GetJSON(ctx, key, &req)
	if err != nil || !ok {
		return timeproto.OperatorRequest{}
	}
	return req
}

func dedupeReports(in []timeproto.AgentReport) []timeproto.AgentReport {
	m := map[string]timeproto.AgentReport{}
	for _, r := range in {
		if r.AgentID == "" {
			continue
		}
		prev, ok := m[r.AgentID]
		if !ok || r.UpdatedAt.After(prev.UpdatedAt) {
			m[r.AgentID] = r
		}
	}
	out := make([]timeproto.AgentReport, 0, len(m))
	for _, r := range m {
		out = append(out, r)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].UpdatedAt.Equal(out[j].UpdatedAt) {
			return out[i].AgentID < out[j].AgentID
		}
		return out[i].UpdatedAt.After(out[j].UpdatedAt)
	})
	return out
}

func newestOrderMode(reports []timeproto.AgentReport, fallback string) (mode string, manualTime string) {
	mode = normalizeMode(fallback)
	var best timeproto.AgentReport
	found := false
	for _, r := range reports {
		if r.LastOrder.OrderNo <= 0 {
			continue
		}
		if !found || r.LastOrder.OrderNo > best.LastOrder.OrderNo ||
			(r.LastOrder.OrderNo == best.LastOrder.OrderNo && r.UpdatedAt.After(best.UpdatedAt)) {
			best = r
			found = true
		}
	}
	if !found {
		return mode, ""
	}
	m := normalizeMode(best.LastOrder.Mode)
	if m != "" {
		mode = m
	}
	return mode, best.LastOrder.ManualTime
}

func anyExternalReachable(reports []timeproto.AgentReport) bool {
	for _, r := range reports {
		if r.ExternalSourceReachable {
			return true
		}
	}
	return false
}

func selectMaster(reports []timeproto.AgentReport) string {
	var best timeproto.AgentReport
	found := false
	for _, r := range reports {
		if r.AgentID == "" {
			continue
		}
		if !found || r.MasterCandidateScore > best.MasterCandidateScore ||
			(r.MasterCandidateScore == best.MasterCandidateScore && r.UpdatedAt.After(best.UpdatedAt)) ||
			(r.MasterCandidateScore == best.MasterCandidateScore && r.UpdatedAt.Equal(best.UpdatedAt) && r.AgentID < best.AgentID) {
			best = r
			found = true
		}
	}
	if !found {
		return ""
	}
	return best.AgentID
}

func sortedAgentIDs(reports []timeproto.AgentReport) []string {
	ids := make([]string, 0, len(reports))
	for _, r := range reports {
		if r.AgentID != "" {
			ids = append(ids, r.AgentID)
		}
	}
	sort.Strings(ids)
	return ids
}

func hashDecision(mode, manual, master string, peers, externals []string, orphan int) string {
	payload := map[string]any{
		"mode":      mode,
		"manual":    manual,
		"master":    master,
		"peers":     peers,
		"externals": externals,
		"orphan":    orphan,
	}
	b, _ := json.Marshal(payload)
	sum := sha1.Sum(b)
	return fmt.Sprintf("%x", sum[:])
}

func normalizeMode(mode string) string {
	switch strings.ToLower(strings.TrimSpace(mode)) {
	case timeproto.ModeAuto:
		return timeproto.ModeAuto
	case timeproto.ModeHandover:
		return timeproto.ModeHandover
	case timeproto.ModeManual:
		return timeproto.ModeManual
	default:
		return ""
	}
}

func payloadToMap(p timeproto.OrderPayload) map[string]any {
	b, _ := json.Marshal(p)
	out := map[string]any{}
	_ = json.Unmarshal(b, &out)
	return out
}
