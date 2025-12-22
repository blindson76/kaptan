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
    LockKey string
    StateKey string

    KafkaCandidatesPrefix string
    MongoCandidatesPrefix string

    WaitFor []string
    MinPassing int

    Services []ServiceDef
    ReconcileInterval time.Duration
}

type ServiceDef struct {
    Name string
    Instances int
    Tags []string
    TTL string
    StartCmd string
    StartArgs []string
    WorkDir string
}

type Controller struct {
    cfg Config
    kv store.KV
    locker interface {
        Acquire(context.Context,string,string) (func() error,error)
    }
    consul *capi.Client

    candidates []string // node ids
    sm *stateless.StateMachine
}

func New(cfg Config, kv store.KV, locker interface{Acquire(context.Context,string,string)(func() error,error)}, consulCli *capi.Client) *Controller {
    if cfg.ReconcileInterval == 0 { cfg.ReconcileInterval = 10*time.Second }
    if cfg.MinPassing == 0 { cfg.MinPassing = 3 }
    return &Controller{cfg: cfg, kv: kv, locker: locker, consul: consulCli}
}

func (c *Controller) Run(ctx context.Context) error {
    return common.RunLeaderLoop(ctx, c.locker, c.cfg.LockKey, c.cfg.ControllerID, c.runActive)
}

type State string
type Trigger string
const (
    StBoot State="boot"
    StWaitDeps State="wait_deps"
    StPlace State="place"
    StReconcile State="reconcile"
)
const (
    TrStart Trigger="START"
    TrTimer Trigger="TIMER"
)

func (c *Controller) runActive(ctx context.Context) error {
    ps := fsm.NewPersistedState(c.kv, c.cfg.StateKey, string(StBoot))
    c.sm = common.NewMachine(ctx, ps)
    c.configure(c.sm)

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
        PermitReentry(TrTimer).
        PermitIf(TrTimer, StPlace, func(context.Context, ...any) bool { return c.depsReady() })

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
        if r.Eligible { m[r.ID] = true }
    }
    var mc []types.CandidateReport
    _ = c.kv.ListJSON(ctx, c.cfg.MongoCandidatesPrefix, &mc)
    for _, r := range mc {
        if r.Eligible { m[r.ID] = true }
    }
    ids := make([]string,0,len(m))
    for id := range m { ids = append(ids,id) }
    sort.Strings(ids)
    c.candidates = ids
}

func (c *Controller) placeAndIssueOrders(ctx context.Context) {
    if len(c.candidates) == 0 {
        log.Printf("[services] no candidates")
        return
    }
    used := map[string]bool{}
    for _, svc := range c.cfg.Services {
        inst := svc.Instances
        if inst <= 0 { inst = 2 }
        if inst > 2 { inst = 2 }
        // choose 2 distinct nodes
        chosen := []string{}
        for _, id := range c.candidates {
            if used[id] { continue }
            chosen = append(chosen,id)
            used[id]=true
            if len(chosen)==inst { break }
        }
        if len(chosen) < inst {
            // allow reuse if not enough
            for _, id := range c.candidates {
                if contains(chosen,id) { continue }
                chosen = append(chosen,id)
                if len(chosen)==inst { break }
            }
        }
        roles := []string{"master","slave"}
        for i, id := range chosen {
            role := roles[min(i,len(roles)-1)]
            ord := orders.Order{
                Kind: orders.KindService,
                TargetID: id,
                Action: orders.ActionStart,
                Epoch: time.Now().Unix(),
                IssuedAt: time.Now(),
                Payload: map[string]any{
                    "service": svc.Name,
                    "role": role,
                    "cmd": svc.StartCmd,
                    "args": svc.StartArgs,
                    "workDir": svc.WorkDir,
                    "tags": svc.Tags,
                    "ttl": svc.TTL,
                },
            }
            _ = c.kv.PutJSON(ctx, "orders/services/"+svc.Name+"/"+id, &ord)
        }
    }
}

func contains(a []string, s string) bool {
    for _, x := range a { if x==s { return true } }
    return false
}
func min(a,b int) int { if a<b { return a }; return b }
