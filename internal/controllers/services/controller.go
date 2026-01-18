package services

import (
	"context"
	"log"
	"sort"
	"time"

	capi "github.com/hashicorp/consul/api"
	"github.com/qmuntal/stateless"
	"github.com/umitbozkurt/consul-replctl/internal/controllers/common"
	"github.com/umitbozkurt/consul-replctl/internal/fsm"
	"github.com/umitbozkurt/consul-replctl/internal/orders"
	"github.com/umitbozkurt/consul-replctl/internal/store"
	"github.com/umitbozkurt/consul-replctl/internal/types"
)

type Config struct {
	ControllerID string
	LockKey      string
	StateKey     string

	KafkaCandidatesPrefix string
	MongoCandidatesPrefix string

	WaitFor    []string
	MinPassing int

	Services          []ServiceDef
	ReconcileInterval time.Duration

	OrderHistoryKeep int
}

type ServiceDef struct {
	Name      string
	Instances int
	Tags      []string
	TTL       string
	StartCmd  string
	StartArgs []string
	WorkDir   string
}

type Controller struct {
	cfg    Config
	kv     store.KV
	locker interface {
		Acquire(context.Context, string, string) (func() error, error)
	}
	consul *capi.Client

	candidates []string // node ids
	sm         *stateless.StateMachine
}

type serviceCounter struct {
	Count     int64     `json:"count"`
	UpdatedAt time.Time `json:"updated_at"`
}

func New(cfg Config, kv store.KV, locker interface {
	Acquire(context.Context, string, string) (func() error, error)
}, consulCli *capi.Client) *Controller {
	if cfg.ReconcileInterval == 0 {
		cfg.ReconcileInterval = 10 * time.Second
	}
	if cfg.MinPassing == 0 {
		cfg.MinPassing = 3
	}
	if cfg.OrderHistoryKeep < 0 {
		cfg.OrderHistoryKeep = 0
	}
	return &Controller{cfg: cfg, kv: kv, locker: locker, consul: consulCli}
}

func (c *Controller) Run(ctx context.Context) error {
	return common.RunLeaderLoop(ctx, c.locker, c.cfg.LockKey, c.cfg.ControllerID, c.runActive)
}

type State = string
type Trigger = string

const (
	StBoot      State = "boot"
	StWaitDeps  State = "wait_deps"
	StPlace     State = "place"
	StReconcile State = "reconcile"
)
const (
	TrStart Trigger = "START"
	TrTimer Trigger = "TIMER"
)

func (c *Controller) runActive(ctx context.Context) error {
	log.Printf("[services] runActive")
	ps := fsm.NewPersistedState(c.kv, c.cfg.StateKey, string(StBoot))
	c.sm = common.NewMachine(ctx, ps)
	c.configure(c.sm)
	c.sm.OnTransitioned(func(_ context.Context, t stateless.Transition) {
		log.Printf("[services] state transition: %s --(%s)--> %s", t.Source, t.Trigger, t.Destination)
	})

	_ = c.sm.FireCtx(ctx, TrStart)
	ticker := time.NewTicker(c.cfg.ReconcileInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			_ = c.sm.FireCtx(ctx, TrTimer)
		}
	}
}

func (c *Controller) configure(sm *stateless.StateMachine) {
	sm.Configure(StBoot).Permit(TrStart, StWaitDeps)

	sm.Configure(StWaitDeps).
		OnEntry(func(ctx context.Context, _ ...any) error {
			log.Printf("[services] waiting dependencies: %v", c.cfg.WaitFor)
			return nil
		}).
		PermitDynamic(TrTimer, func(ctx context.Context, args ...any) (any, error) {
			if c.depsReady() {
				return StPlace, nil
			}
			return StWaitDeps, nil
		})

	sm.Configure(StPlace).
		OnEntry(func(ctx context.Context, _ ...any) error {
			c.collectCandidates(ctx)
			c.placeAndIssueOrders(ctx)
			return nil
		}).
		Permit(TrTimer, StReconcile)

	sm.Configure(StReconcile).
		OnEntry(func(ctx context.Context, _ ...any) error {
			c.collectCandidates(ctx)
			c.placeAndIssueOrders(ctx)
			return nil
		}).
		PermitReentry(TrTimer)
}

func (c *Controller) depsReady() bool {
	if c.consul == nil {
		return true
	}
	for _, name := range c.cfg.WaitFor {
		ents, _, err := c.consul.Health().Service(name, "", true, nil)
		if err != nil {
			return false
		}
		if len(ents) < c.cfg.MinPassing {
			return false
		}
	}
	return true
}

func (c *Controller) collectCandidates(ctx context.Context) {
	// Use union of kafka+mongo candidate IDs that are eligible (best-effort)
	var kc []types.CandidateReport
	_ = c.kv.ListJSON(ctx, c.cfg.KafkaCandidatesPrefix, &kc)
	m := map[string]bool{}
	for _, r := range kc {
		if r.Eligible {
			m[r.ID] = true
		}
	}
	var mc []types.CandidateReport
	_ = c.kv.ListJSON(ctx, c.cfg.MongoCandidatesPrefix, &mc)
	for _, r := range mc {
		if r.Eligible {
			m[r.ID] = true
		}
	}
	ids := make([]string, 0, len(m))
	for id := range m {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	c.candidates = ids
}

func (c *Controller) placeAndIssueOrders(ctx context.Context) {
	if len(c.candidates) == 0 {
		log.Printf("[services] no candidates")
		return
	}
	n := len(c.candidates)
	firstNext := 0
	for si, svc := range c.cfg.Services {
		inst := svc.Instances
		if inst <= 0 {
			inst = 2
		}
		if inst > 2 {
			inst = 2
		}
		if inst > n {
			inst = n
		}
		activeNodes, activeCount, ok := c.activeServiceNodes(ctx, svc.Name)
		if !ok {
			continue
		}
		if activeCount >= inst {
			continue
		}
		need := inst - activeCount
		used := map[string]bool{}
		isReplacement := activeCount > 0

		if activeCount == 0 && need > 0 {
			if id, ok := pickCandidate(c.candidates, activeNodes, used, firstNext); ok {
				firstNext++
				if firstNext >= n {
					firstNext = 0
				}
				c.issueOrder(ctx, svc, id, "master")
				if isReplacement {
					c.incrementReplacementCount(ctx, svc.Name)
				}
				used[id] = true
				need--
			}
		}

		start := si % n
		for i := 0; need > 0 && i < n; i++ {
			id := c.candidates[(start+i)%n]
			if activeNodes[id] || used[id] {
				continue
			}
			c.issueOrder(ctx, svc, id, "slave")
			if isReplacement {
				c.incrementReplacementCount(ctx, svc.Name)
			}
			used[id] = true
			need--
		}
		if need > 0 {
			log.Printf("[services] not enough candidates to place service=%s need=%d active=%d total=%d", svc.Name, need, activeCount, n)
		}
	}
}

func (c *Controller) activeServiceNodes(ctx context.Context, svcName string) (map[string]bool, int, bool) {
	active := map[string]bool{}
	if c.consul == nil {
		return active, 0, true
	}
	ents, _, err := c.consul.Health().Service(svcName, "", true, nil)
	if err != nil {
		log.Printf("[services] health check error service=%s: %v", svcName, err)
		return nil, 0, false
	}
	for _, ent := range ents {
		if ent == nil || ent.Node == nil {
			continue
		}
		if ent.Node.Node != "" {
			active[ent.Node.Node] = true
		}
	}
	return active, len(ents), true
}

func pickCandidate(candidates []string, active map[string]bool, used map[string]bool, start int) (string, bool) {
	n := len(candidates)
	if n == 0 {
		return "", false
	}
	for i := 0; i < n; i++ {
		id := candidates[(start+i)%n]
		if active[id] || used[id] {
			continue
		}
		return id, true
	}
	return "", false
}

func (c *Controller) issueOrder(ctx context.Context, svc ServiceDef, id string, role string) {
	ord := orders.Order{
		Kind:     orders.KindService,
		TargetID: id,
		Action:   orders.ActionStart,
		Epoch:    time.Now().Unix(),
		IssuedAt: time.Now(),
		Payload: map[string]any{
			"service": svc.Name,
			"role":    role,
			"cmd":     svc.StartCmd,
			"args":    svc.StartArgs,
			"workDir": svc.WorkDir,
			"tags":    svc.Tags,
			"ttl":     svc.TTL,
		},
	}
	orderKey := "orders/services/" + svc.Name + "/" + id
	log.Printf("[services] order publish service=%s role=%s target=%s action=%s epoch=%d key=%s", svc.Name, role, id, ord.Action, ord.Epoch, orderKey)
	_ = orders.SaveWithHistory(ctx, c.kv, orderKey, ord, c.cfg.OrderHistoryKeep)
}

func (c *Controller) incrementReplacementCount(ctx context.Context, svcName string) {
	if svcName == "" || c.kv == nil {
		return
	}
	key := "stats/services/replacements/" + svcName
	c.incrementCounter(ctx, key)
}

func (c *Controller) incrementCounter(ctx context.Context, key string) {
	var cstat serviceCounter
	ok, err := c.kv.GetJSON(ctx, key, &cstat)
	if err != nil {
		log.Printf("[services] counter read error key=%s err=%v", key, err)
		return
	}
	if !ok {
		cstat = serviceCounter{}
	}
	cstat.Count++
	cstat.UpdatedAt = time.Now()
	if err := c.kv.PutJSON(ctx, key, &cstat); err != nil {
		log.Printf("[services] counter write error key=%s err=%v", key, err)
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
