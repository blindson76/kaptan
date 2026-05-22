package timeagent

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/umitbozkurt/consul-replctl/internal/orders"
	"github.com/umitbozkurt/consul-replctl/internal/store"
	"github.com/umitbozkurt/consul-replctl/internal/timeproto"
)

type Config struct {
	AgentID string

	ReportKey string
	OrdersKey string
	AckKey    string

	OrderStorePath string

	NtpdBin         string
	NtpdConfigPath  string
	NtpdServiceName string

	ReportInterval time.Duration
}

type Agent struct {
	cfg Config
	kv  store.KV

	lastApplied timeproto.OrderPayload
	lastMode    string
}

func New(cfg Config, kv store.KV) *Agent {
	if cfg.ReportInterval <= 0 {
		cfg.ReportInterval = 10 * time.Second
	}
	return &Agent{cfg: cfg, kv: kv}
}

func (a *Agent) Run(ctx context.Context) error {
	if a.cfg.AgentID == "" {
		return fmt.Errorf("time agent_id is required")
	}
	if err := a.loadLastOrderFromStore(); err != nil {
		log.Printf("[time-agent] load order store failed: %v", err)
	}
	a.publishReport(ctx)

	ch := a.kv.WatchPrefixJSON(ctx, a.cfg.OrdersKey, func() any { return &[]orders.Order{} })
	ticker := time.NewTicker(a.cfg.ReportInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			a.publishReport(ctx)
		case v, ok := <-ch:
			if !ok {
				return nil
			}
			lst := v.([]orders.Order)
			ord, ok := latestTimeOrderForAgent(lst, a.cfg.AgentID)
			if !ok {
				continue
			}
			a.applyOrder(ctx, ord)
		}
	}
}

func (a *Agent) applyOrder(ctx context.Context, ord orders.Order) {
	ack := orders.Ack{
		TargetID:   a.cfg.AgentID,
		Action:     ord.Action,
		Epoch:      ord.Epoch,
		FinishedAt: time.Now(),
	}
	payload, err := decodePayload(ord.Payload)
	if err != nil {
		ack.Ok = false
		ack.Message = err.Error()
		a.publishAck(ctx, ack)
		return
	}
	if payload.OrderNo <= a.lastApplied.OrderNo {
		ack.Ok = true
		a.publishAck(ctx, ack)
		return
	}

	if err := a.applyTimeMode(payload); err != nil {
		ack.Ok = false
		ack.Message = err.Error()
		a.publishAck(ctx, ack)
		return
	}
	a.lastApplied = payload
	a.lastMode = normalizeMode(payload.Mode)
	if a.lastMode == "" {
		a.lastMode = timeproto.ModeAuto
	}
	if err := a.saveLastOrderToStore(); err != nil {
		ack.Ok = false
		ack.Message = err.Error()
		a.publishAck(ctx, ack)
		return
	}
	ack.Ok = true
	a.publishAck(ctx, ack)
	a.publishReport(ctx)
}

func (a *Agent) applyTimeMode(payload timeproto.OrderPayload) error {
	mode := normalizeMode(payload.Mode)
	if mode == "" {
		return fmt.Errorf("unsupported mode %q", payload.Mode)
	}
	if mode == timeproto.ModeManual && payload.ManualTime == "" {
		return fmt.Errorf("manual mode requires manualTime")
	}
	if a.cfg.NtpdConfigPath != "" {
		if err := a.writeNtpdConfig(payload); err != nil {
			return err
		}
	}
	return nil
}

func (a *Agent) writeNtpdConfig(payload timeproto.OrderPayload) error {
	lines := []string{
		"# managed by replctl time-agent",
		fmt.Sprintf("# order_no=%d", payload.OrderNo),
		fmt.Sprintf("# mode=%s", payload.Mode),
	}
	if payload.Mode == timeproto.ModeManual {
		lines = append(lines, fmt.Sprintf("# manual_time=%s", payload.ManualTime))
	}
	for _, s := range payload.ExternalServers {
		s = strings.TrimSpace(s)
		if s == "" {
			continue
		}
		lines = append(lines, fmt.Sprintf("server %s iburst", s))
	}
	for _, p := range payload.Peers {
		p = strings.TrimSpace(p)
		if p == "" || p == a.cfg.AgentID {
			continue
		}
		lines = append(lines, fmt.Sprintf("server %s prefer", p))
	}
	if payload.OrphanStratum > 0 {
		lines = append(lines, fmt.Sprintf("tos orphan %d", payload.OrphanStratum))
	}
	content := strings.Join(lines, "\n") + "\n"
	if err := os.MkdirAll(filepath.Dir(a.cfg.NtpdConfigPath), 0o755); err != nil {
		return err
	}
	return os.WriteFile(a.cfg.NtpdConfigPath, []byte(content), 0o644)
}

func (a *Agent) publishAck(ctx context.Context, ack orders.Ack) {
	if a.cfg.AckKey == "" {
		return
	}
	_ = a.kv.PutJSON(ctx, a.cfg.AckKey, &ack)
}

func (a *Agent) publishReport(ctx context.Context) {
	if a.cfg.ReportKey == "" {
		return
	}
	externalReachable := probeAnyExternal(a.lastApplied.ExternalServers, 1500*time.Millisecond)
	mode := a.lastMode
	if mode == "" {
		mode = timeproto.ModeAuto
	}
	rep := timeproto.AgentReport{
		AgentID: a.cfg.AgentID,
		Mode:    mode,
		LastOrder: timeproto.LastOrderSnapshot{
			OrderNo:    a.lastApplied.OrderNo,
			Mode:       normalizeMode(a.lastApplied.Mode),
			ManualTime: a.lastApplied.ManualTime,
		},
		ExternalSourceReachable: externalReachable,
		MasterCandidateScore:    a.lastApplied.OrderNo,
		UpdatedAt:               time.Now(),
	}
	_ = a.kv.PutJSON(ctx, a.cfg.ReportKey, &rep)
}

func (a *Agent) loadLastOrderFromStore() error {
	if a.cfg.OrderStorePath == "" {
		return nil
	}
	b, err := os.ReadFile(a.cfg.OrderStorePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	var p timeproto.OrderPayload
	if err := json.Unmarshal(b, &p); err != nil {
		return err
	}
	a.lastApplied = p
	a.lastMode = normalizeMode(p.Mode)
	return nil
}

func (a *Agent) saveLastOrderToStore() error {
	if a.cfg.OrderStorePath == "" {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(a.cfg.OrderStorePath), 0o755); err != nil {
		return err
	}
	b, err := json.MarshalIndent(a.lastApplied, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(a.cfg.OrderStorePath, b, 0o644)
}

func latestTimeOrderForAgent(list []orders.Order, agentID string) (orders.Order, bool) {
	items := make([]orders.Order, 0, len(list))
	for _, ord := range list {
		if ord.Kind != orders.KindTime || ord.Action != orders.ActionSetMode {
			continue
		}
		if ord.TargetID != agentID {
			continue
		}
		items = append(items, ord)
	}
	if len(items) == 0 {
		return orders.Order{}, false
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].Epoch == items[j].Epoch {
			return items[i].IssuedAt.After(items[j].IssuedAt)
		}
		return items[i].Epoch > items[j].Epoch
	})
	return items[0], true
}

func decodePayload(m map[string]any) (timeproto.OrderPayload, error) {
	b, err := json.Marshal(m)
	if err != nil {
		return timeproto.OrderPayload{}, err
	}
	var p timeproto.OrderPayload
	if err := json.Unmarshal(b, &p); err != nil {
		return timeproto.OrderPayload{}, err
	}
	return p, nil
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

func probeAnyExternal(servers []string, timeout time.Duration) bool {
	for _, s := range servers {
		s = strings.TrimSpace(s)
		if s == "" {
			continue
		}
		hostPort := s
		if !strings.Contains(hostPort, ":") {
			hostPort = net.JoinHostPort(hostPort, "123")
		}
		conn, err := net.DialTimeout("udp", hostPort, timeout)
		if err == nil {
			_ = conn.Close()
			return true
		}
	}
	return false
}
