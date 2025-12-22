package kafka

import (
	"context"
	"log"
	"sort"
	"time"

	"github.com/qmuntal/stateless"
	"github.com/umitbozkurt/consul-replctl/internal/controllers/common"
	"github.com/umitbozkurt/consul-replctl/internal/fsm"
	"github.com/umitbozkurt/consul-replctl/internal/store"
	"github.com/umitbozkurt/consul-replctl/internal/types"
)

type State = string
type Trigger = string

const (
	StBoot           State = "boot"
	StWaitCandidates State = "wait_candidates"
	StPublish        State = "publish"
	StWaitHealth     State = "wait_health"
	StMonitor        State = "monitor"
	StReconcile      State = "reconcile"
	StDegraded       State = "degraded"
)

const (
	TrStart      Trigger = "START"
	TrCandidates Trigger = "CANDIDATES"
	TrHealth     Trigger = "HEALTH"
	TrTimer      Trigger = "TIMER"
	TrPublished  Trigger = "PUBLISHED"
)

type Config struct {
	ControllerID              string
	LockKey                   string
	StateKey                  string
	CandidatesPrefix          string
	HealthPrefix              string
	SpecKey                   string
	ElectionInterval          time.Duration
	InitialSettleDuration     time.Duration
	AllowDegradedSingleMember bool
}

type Controller struct {
	cfg    Config
	kv     store.KV
	locker interface {
		Acquire(context.Context, string, string) (func() error, error)
	}
	provider Provider

	sm *stateless.StateMachine
	ps *fsm.PersistedState

	candidates []types.CandidateReport
	health     []types.HealthStatus

	spec        types.ReplicaSpec
	specVersion int64

	initialDeadline time.Time
}

func New(cfg Config, kv store.KV, locker interface {
	Acquire(context.Context, string, string) (func() error, error)
}, provider Provider) *Controller {
	if cfg.ElectionInterval == 0 {
		cfg.ElectionInterval = 5 * time.Second
	}
	if cfg.InitialSettleDuration == 0 {
		cfg.InitialSettleDuration = 15 * time.Second
	}
	return &Controller{cfg: cfg, kv: kv, locker: locker, provider: provider}
}

func (c *Controller) Run(ctx context.Context) error {
	return common.RunLeaderLoop(ctx, c.locker, c.cfg.LockKey, c.cfg.ControllerID, c.runActive)
}

func (c *Controller) runActive(ctx context.Context) error {
	c.ps = fsm.NewPersistedState(c.kv, c.cfg.StateKey, string(StBoot))
	c.sm = common.NewMachine(ctx, c.ps)
	c.configure(c.sm)
	c.sm.OnTransitioned(func(_ context.Context, t stateless.Transition) {
		log.Printf("[kafka] state transition: %s --(%s)--> %s", t.Source, t.Trigger, t.Destination)
	})

	candCh := c.kv.WatchPrefixJSON(ctx, c.cfg.CandidatesPrefix, func() any { return &[]types.CandidateReport{} })
	healthCh := c.kv.WatchPrefixJSON(ctx, c.cfg.HealthPrefix, func() any { return &[]types.HealthStatus{} })

	_ = c.loadSpec(ctx)

	_ = c.sm.FireCtx(ctx, TrStart)

	ticker := time.NewTicker(c.cfg.ElectionInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case v, ok := <-candCh:
			if !ok {
				return nil
			}
			snap := v.([]types.CandidateReport)
			c.candidates = snap
			log.Printf("[kafka] received candidate reports: total=%d eligible=%d", len(snap), countEligible(snap))
			if len(c.spec.Members) == 0 && c.hasEnoughEligible(3) {
				c.maybeStartOrExtendInitialWindow(time.Now())
			}
			_ = c.sm.FireCtx(ctx, TrCandidates)
		case v, ok := <-healthCh:
			if !ok {
				return nil
			}
			c.health = v.([]types.HealthStatus)
			_ = c.sm.FireCtx(ctx, TrHealth)
		case <-ticker.C:
			_ = c.sm.FireCtx(ctx, TrTimer)
		}
	}
}

func (c *Controller) configure(sm *stateless.StateMachine) {
	sm.Configure(StBoot).Permit(TrStart, StWaitCandidates)

	sm.Configure(StWaitCandidates).
		OnEntry(func(ctx context.Context, _ ...any) error {
			log.Printf("[kafka] wait_candidates: waiting for enough eligible candidates")
			c.initialDeadline = time.Time{}
			if c.loadSpec(ctx) == nil && len(c.spec.Members) > 0 {
				log.Printf("[kafka] existing spec found (members=%v), skipping initial publish", c.spec.Members)
				_ = c.sm.FireCtx(ctx, TrPublished)
			}
			return nil
		}).
		Ignore(TrCandidates).
		Ignore(TrTimer).
		Permit(TrTimer, StPublish, func(context.Context, ...any) bool {
			return c.hasEnoughEligible(3) && c.initialWindowElapsed(time.Now())
		}).
		Permit(TrCandidates, StDegraded, func(context.Context, ...any) bool {
			return c.cfg.AllowDegradedSingleMember && !c.hasEnoughEligible(3) && c.hasEnoughEligible(1)
		}).
		Permit(TrTimer, StDegraded, func(context.Context, ...any) bool {
			return c.cfg.AllowDegradedSingleMember && !c.hasEnoughEligible(3) && c.hasEnoughEligible(1)
		}).
		Permit(TrPublished, StWaitHealth)

	sm.Configure(StDegraded).
		OnEntry(func(ctx context.Context, _ ...any) error {
			log.Printf("[kafka] degraded: publishing single-member spec")
			c.publishSpec(ctx, 1)
			_ = c.sm.FireCtx(ctx, TrPublished)
			return nil
		}).
		Permit(TrPublished, StWaitHealth)

	sm.Configure(StPublish).
		OnEntry(func(ctx context.Context, _ ...any) error {
			log.Printf("[kafka] publish: publishing initial spec")
			c.publishSpec(ctx, 3)
			_ = c.sm.FireCtx(ctx, TrPublished)
			return nil
		}).
		Permit(TrPublished, StWaitHealth)

	sm.Configure(StWaitHealth).
		OnEntry(func(ctx context.Context, _ ...any) error {
			log.Printf("[kafka] wait_health: waiting until spec members are healthy")
			_ = c.loadSpec(ctx)
			return nil
		}).
		PermitReentry(TrHealth).
		PermitReentry(TrTimer).
		Permit(TrHealth, StMonitor, func(context.Context, ...any) bool {
			_ = c.loadSpec(context.Background())
			return c.specAllHealthy()
		}).
		Permit(TrTimer, StMonitor, func(context.Context, ...any) bool {
			_ = c.loadSpec(context.Background())
			return c.specAllHealthy()
		})

	sm.Configure(StMonitor).
		OnEntry(func(ctx context.Context, _ ...any) error {
			log.Printf("[kafka] monitor: monitoring health and failures")
			_ = c.loadSpec(ctx)
			return nil
		}).
		PermitReentry(TrHealth).
		PermitReentry(TrCandidates).
		PermitReentry(TrTimer).
		Permit(TrHealth, StReconcile, func(context.Context, ...any) bool {
			_ = c.loadSpec(context.Background())
			return c.needsReplace()
		}).
		Permit(TrTimer, StReconcile, func(context.Context, ...any) bool {
			_ = c.loadSpec(context.Background())
			return c.needsReplace()
		})

	sm.Configure(StReconcile).
		OnEntry(func(ctx context.Context, _ ...any) error {
			log.Printf("[kafka] reconcile: replacing failed member if possible")
			_ = c.loadSpec(ctx)
			changed := c.replaceOnce(ctx)
			if changed {
				_ = c.sm.FireCtx(ctx, TrPublished)
			} else {
				_ = c.ps.Save(ctx, string(StMonitor))
			}
			return nil
		}).
		Permit(TrPublished, StWaitHealth)
}

func (c *Controller) maybeStartOrExtendInitialWindow(now time.Time) {
	if c.initialDeadline.IsZero() {
		c.initialDeadline = now.Add(c.cfg.InitialSettleDuration)
		log.Printf("[kafka] initial settle window started: %s", c.initialDeadline.Format(time.RFC3339))
		return
	}
	c.initialDeadline = now.Add(c.cfg.InitialSettleDuration)
}

func (c *Controller) initialWindowElapsed(now time.Time) bool {
	if c.initialDeadline.IsZero() {
		return false
	}
	return !now.Before(c.initialDeadline)
}

func (c *Controller) eligibleCandidates() []types.CandidateReport {
	out := make([]types.CandidateReport, 0, len(c.candidates))
	for _, r := range c.candidates {
		if r.Eligible {
			out = append(out, r)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Less(out[j]) })
	return out
}

func (c *Controller) hasEnoughEligible(n int) bool { return len(c.eligibleCandidates()) >= n }

func (c *Controller) loadSpec(ctx context.Context) error {
	var s types.ReplicaSpec
	ok, err := c.kv.GetJSON(ctx, c.cfg.SpecKey, &s)
	if err != nil {
		return err
	}
	if ok {
		c.spec = s
		if s.Version > c.specVersion {
			c.specVersion = s.Version
		}
	}
	return nil
}

func (c *Controller) publishSpec(ctx context.Context, want int) {
	eligible := c.eligibleCandidates()
	if len(eligible) < want {
		want = len(eligible)
	}
	members := make([]string, 0, want)
	for i := 0; i < want; i++ {
		members = append(members, eligible[i].ID)
	}
	c.specVersion++
	bootstrap := make([]string, 0, len(members))
	// Build controller.quorum.bootstrap.servers from selected candidates controller addresses
	for _, id := range members {
		for _, cr := range eligible {
			if cr.ID == id {
				if cr.KafkaControllerAddr != "" {
					bootstrap = append(bootstrap, cr.KafkaControllerAddr)
				}
			}
		}
	}

	spec := types.ReplicaSpec{
		Kind:                  types.CandidateKafka,
		Members:               members,
		UpdatedAt:             time.Now(),
		Version:               c.specVersion,
		KafkaMode:             "combined",
		KafkaDynamicVoter:     true,
		KafkaBootstrapServers: bootstrap,
	}
	c.spec = spec
	_ = c.kv.PutJSON(ctx, c.cfg.SpecKey, &spec)
	if c.provider != nil {
		_ = c.provider.PublishSpec(ctx, spec)
	}
}

func (c *Controller) specAllHealthy() bool {
	if len(c.spec.Members) == 0 {
		return false
	}
	h := map[string]bool{}
	for _, s := range c.health {
		h[s.ID] = s.Healthy
	}
	for _, id := range c.spec.Members {
		if ok, exists := h[id]; !exists || !ok {
			return false
		}
	}
	return true
}

func (c *Controller) needsReplace() bool {
	if len(c.spec.Members) == 0 {
		return false
	}
	h := map[string]bool{}
	for _, s := range c.health {
		h[s.ID] = s.Healthy
	}
	failedIdx := -1
	for i, id := range c.spec.Members {
		if ok, exists := h[id]; exists && !ok {
			failedIdx = i
			break
		}
	}
	if failedIdx < 0 {
		return false
	}
	specSet := map[string]bool{}
	for _, id := range c.spec.Members {
		specSet[id] = true
	}
	for _, cand := range c.eligibleCandidates() {
		if !specSet[cand.ID] {
			return true
		}
	}
	return false
}

func countEligible(reports []types.CandidateReport) int {
	n := 0
	for _, r := range reports {
		if r.Eligible {
			n++
		}
	}
	return n
}

func (c *Controller) replaceOnce(ctx context.Context) bool {
	if len(c.spec.Members) == 0 {
		return false
	}
	h := map[string]bool{}
	for _, s := range c.health {
		h[s.ID] = s.Healthy
	}
	failedIdx := -1
	for i, id := range c.spec.Members {
		if ok, exists := h[id]; exists && !ok {
			failedIdx = i
			break
		}
	}
	if failedIdx < 0 {
		return false
	}

	eligible := c.eligibleCandidates()
	specSet := map[string]bool{}
	for _, id := range c.spec.Members {
		specSet[id] = true
	}
	replacement := ""
	for _, cand := range eligible {
		if !specSet[cand.ID] {
			replacement = cand.ID
			break
		}
	}
	if replacement == "" {
		log.Printf("[kafka] reconcile: no replacement available")
		return false
	}

	members := append([]string{}, c.spec.Members...)
	members[failedIdx] = replacement

	c.specVersion++
	bootstrap := make([]string, 0, len(members))
	// Build controller.quorum.bootstrap.servers from selected candidates controller addresses
	for _, id := range members {
		for _, cr := range eligible {
			if cr.ID == id {
				if cr.KafkaControllerAddr != "" {
					bootstrap = append(bootstrap, cr.KafkaControllerAddr)
				}
			}
		}
	}

	spec := types.ReplicaSpec{
		Kind:                  types.CandidateKafka,
		Members:               members,
		UpdatedAt:             time.Now(),
		Version:               c.specVersion,
		KafkaMode:             "combined",
		KafkaDynamicVoter:     true,
		KafkaBootstrapServers: bootstrap,
	}
	c.spec = spec
	_ = c.kv.PutJSON(ctx, c.cfg.SpecKey, &spec)
	if c.provider != nil {
		_ = c.provider.PublishSpec(ctx, spec)
	}
	return true
}
