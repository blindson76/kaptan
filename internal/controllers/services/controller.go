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
	Name              string
	Instances         int
	Tags              []string
	TTL               string
	DependsOn         []string
	DependsMinPassing int
	StartCmd          string
	StartArgs         []string
	WorkDir           string
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

type serviceInstance struct {
	NodeID string
	Role   string
}

const (
	serviceRoleMaster = "master"
	serviceRoleSlave  = "slave"
)

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
	for si, svc := range c.cfg.Services {
		if !c.depsReadyForService(svc) {
			log.Printf("[services] waiting service dependencies service=%s deps=%v", svc.Name, svc.DependsOn)
			continue
		}
		inst := svc.Instances
		if inst <= 0 {
			inst = 2
		}
		if inst > 2 {
			inst = 2
		}
		if inst > len(c.candidates) {
			inst = len(c.candidates)
		}
		activeInstances, ok := c.activeServiceInstances(ctx, svc.Name)
		if !ok {
			continue
		}
		plan := buildPlacementPlan(activeInstances, desiredRoles(inst))
		start := 0
		if len(c.candidates) > 0 {
			start = si % len(c.candidates)
		}
		targeted := map[string]bool{}
		isReplacement := len(activeInstances) > 0

		for _, role := range plan.MissingRoles {
			id, ok := pickPlacementNode(c.candidates, plan.UsedNodes, plan.ActiveNodes, targeted, start)
			if !ok {
				log.Printf("[services] not enough candidates to place service=%s role=%s instances=%d total=%d", svc.Name, role, len(activeInstances), len(c.candidates))
				continue
			}
			c.issueOrder(ctx, svc, id, role)
			if isReplacement {
				c.incrementReplacementCount(ctx, svc.Name)
			}
			plan.UsedNodes[id] = true
			targeted[id] = true
			start = nextStartIndex(c.candidates, id)
		}

		for _, id := range plan.ExtraNodes {
			if targeted[id] {
				continue
			}
			c.issueStopOrder(ctx, svc.Name, id)
		}
	}
}

func (c *Controller) depsReadyForService(svc ServiceDef) bool {
	if len(svc.DependsOn) == 0 || c.consul == nil {
		return true
	}
	minPassing := svc.DependsMinPassing
	if minPassing <= 0 {
		minPassing = 1
	}
	for _, dep := range svc.DependsOn {
		if dep == "" {
			continue
		}
		ents, _, err := c.consul.Health().Service(dep, "", true, nil)
		if err != nil {
			log.Printf("[services] dependency health error service=%s dep=%s err=%v", svc.Name, dep, err)
			return false
		}
		if len(ents) < minPassing {
			return false
		}
	}
	return true
}

func (c *Controller) activeServiceInstances(ctx context.Context, svcName string) ([]serviceInstance, bool) {
	active := []serviceInstance{}
	if c.consul == nil {
		return active, true
	}
	ents, _, err := c.consul.Health().Service(svcName, "", true, nil)
	if err != nil {
		log.Printf("[services] health check error service=%s: %v", svcName, err)
		return nil, false
	}
	for _, ent := range ents {
		if ent == nil || ent.Node == nil || ent.Service == nil {
			continue
		}
		if ent.Node.Node != "" {
			active = append(active, serviceInstance{
				NodeID: ent.Node.Node,
				Role:   serviceRoleFromTags(ent.Service.Tags),
			})
		}
	}
	// lexicographic ordering keeps placement deterministic so reconciles keep preferring the same healthy instances.
	sort.Slice(active, func(i, j int) bool {
		if active[i].NodeID != active[j].NodeID {
			return active[i].NodeID < active[j].NodeID
		}
		return active[i].Role < active[j].Role
	})
	return active, true
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

type placementPlan struct {
	ActiveNodes  map[string]bool
	UsedNodes    map[string]bool
	MissingRoles []string
	ExtraNodes   []string
}

func buildPlacementPlan(instances []serviceInstance, roles []string) placementPlan {
	plan := placementPlan{
		ActiveNodes: make(map[string]bool),
		UsedNodes:   make(map[string]bool),
	}
	if len(roles) == 0 {
		plan.ExtraNodes = uniqueNodeOrder(instances, nil)
		return plan
	}

	kept := make([]bool, len(instances))
	for _, inst := range instances {
		if inst.NodeID == "" {
			continue
		}
		plan.ActiveNodes[inst.NodeID] = true
	}

	for _, role := range roles {
		idx := findMatchingInstance(instances, kept, plan.UsedNodes, role)
		if idx < 0 {
			plan.MissingRoles = append(plan.MissingRoles, role)
			continue
		}
		kept[idx] = true
		plan.UsedNodes[instances[idx].NodeID] = true
	}

	plan.ExtraNodes = uniqueNodeOrder(instances, kept)
	return plan
}

func desiredRoles(instances int) []string {
	if instances <= 0 {
		return nil
	}
	// Services are limited to a master/slave pair, so requests above two still map to those two roles.
	roles := []string{serviceRoleMaster}
	if instances > 1 {
		roles = append(roles, serviceRoleSlave)
	}
	return roles
}

func findMatchingInstance(instances []serviceInstance, kept []bool, usedNodes map[string]bool, role string) int {
	for i, inst := range instances {
		if kept[i] || inst.NodeID == "" || usedNodes[inst.NodeID] {
			continue
		}
		if inst.Role == role {
			return i
		}
	}
	return -1
}

func uniqueNodeOrder(instances []serviceInstance, kept []bool) []string {
	nodes := []string{}
	seen := map[string]bool{}
	for i, inst := range instances {
		if inst.NodeID == "" {
			continue
		}
		if kept != nil && kept[i] {
			continue
		}
		if seen[inst.NodeID] {
			continue
		}
		seen[inst.NodeID] = true
		nodes = append(nodes, inst.NodeID)
	}
	return nodes
}

func pickPlacementNode(candidates []string, usedNodes, activeNodes, targeted map[string]bool, start int) (string, bool) {
	if id, ok := pickCandidateWithFilter(candidates, usedNodes, targeted, activeNodes, start, false); ok {
		return id, true
	}
	return pickCandidateWithFilter(candidates, usedNodes, targeted, activeNodes, start, true)
}

func pickCandidateWithFilter(candidates []string, usedNodes, targeted, activeNodes map[string]bool, start int, allowActive bool) (string, bool) {
	n := len(candidates)
	if n == 0 {
		return "", false
	}
	for i := 0; i < n; i++ {
		id := candidates[(start+i)%n]
		if usedNodes[id] || targeted[id] {
			continue
		}
		if !allowActive && activeNodes[id] {
			continue
		}
		return id, true
	}
	return "", false
}

func nextStartIndex(candidates []string, picked string) int {
	for i, id := range candidates {
		if id == picked {
			if i+1 >= len(candidates) {
				return 0
			}
			return i + 1
		}
	}
	return 0
}

func serviceRoleFromTags(tags []string) string {
	for _, tag := range tags {
		switch tag {
		case serviceRoleMaster, serviceRoleSlave:
			return tag
		}
	}
	return ""
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

func (c *Controller) issueStopOrder(ctx context.Context, svcName string, id string) {
	ord := orders.Order{
		Kind:     orders.KindService,
		TargetID: id,
		Action:   orders.ActionStop,
		Epoch:    time.Now().Unix(),
		IssuedAt: time.Now(),
		Payload: map[string]any{
			"service": svcName,
		},
	}
	orderKey := "orders/services/" + svcName + "/" + id
	log.Printf("[services] order publish service=%s target=%s action=%s epoch=%d key=%s", svcName, id, ord.Action, ord.Epoch, orderKey)
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
