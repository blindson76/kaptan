package hmi

import (
	"context"
	"fmt"
	"log"
	"sort"
	"time"

	capi "github.com/hashicorp/consul/api"
	"github.com/umitbozkurt/consul-replctl/internal/controllers/common"
	"github.com/umitbozkurt/consul-replctl/internal/store"
)

type Config struct {
	ControllerID string
	LockKey      string
	StateKey     string

	AssignmentsPrefix string
	WorkersPrefix     string

	WaitFor    []string
	MinPassing int

	DefaultAssignments []Assignment
}

type Assignment struct {
	WorkerID string `json:"worker_id"`
	Role     string `json:"role"`
}

type roleState struct {
	Workers   map[string]string `json:"workers"`
	UpdatedAt time.Time         `json:"updated_at"`
}

type WorkerStatus struct {
	ID        string    `json:"id"`
	Host      string    `json:"host,omitempty"`
	StartedAt time.Time `json:"started_at"`
}

type Controller struct {
	cfg    Config
	kv     store.KV
	locker interface {
		Acquire(context.Context, string, string) (func() error, error)
	}
	consul *capi.Client

	last map[string]string

	assignments map[string]string
	workers     map[string]WorkerStatus
	lastGen     map[string]time.Time
}

func New(cfg Config, kv store.KV, locker interface {
	Acquire(context.Context, string, string) (func() error, error)
}, consulCli *capi.Client) *Controller {
	if cfg.AssignmentsPrefix == "" {
		cfg.AssignmentsPrefix = "hmi/assignments"
	}
	if cfg.WorkersPrefix == "" {
		cfg.WorkersPrefix = "hmi/workers"
	}
	return &Controller{
		cfg:         cfg,
		kv:          kv,
		locker:      locker,
		consul:      consulCli,
		last:        map[string]string{},
		assignments: map[string]string{},
		workers:     map[string]WorkerStatus{},
		lastGen:     map[string]time.Time{},
	}
}

func (c *Controller) Run(ctx context.Context) error {
	return common.RunLeaderLoop(ctx, c.locker, c.cfg.LockKey, c.cfg.ControllerID, c.runActive)
}

func (c *Controller) runActive(ctx context.Context) error {
	log.Printf("[hmi] runActive assignments_prefix=%s", c.cfg.AssignmentsPrefix)
	c.loadState(ctx)
	if err := c.waitForDeps(ctx); err != nil {
		return err
	}

	assignCh := c.kv.WatchPrefixJSON(ctx, c.cfg.AssignmentsPrefix, func() any { return &[]Assignment{} })
	workersCh := c.kv.WatchPrefixJSON(ctx, c.cfg.WorkersPrefix, func() any { return &[]WorkerStatus{} })
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case v, ok := <-assignCh:
			if !ok {
				return nil
			}
			assignments := v.([]Assignment)
			log.Printf("[hmi] new assignments: %v", assignments)
			if len(assignments) == 0 && c.publishDefaultAssignments(ctx) {
				continue
			}
			c.updateAssignments(assignments)
			c.reconcile(ctx)
		case v, ok := <-workersCh:
			if !ok {
				return nil
			}
			workers := v.([]WorkerStatus)
			c.updateWorkers(workers)
			c.reconcile(ctx)
		}
	}
}

func (c *Controller) waitForDeps(ctx context.Context) error {
	if c.consul == nil || len(c.cfg.WaitFor) == 0 {
		return nil
	}
	minPassing := c.cfg.MinPassing
	if minPassing <= 0 {
		minPassing = 1
	}
	log.Printf("[hmi] waiting dependencies: %v (minPassing=%d)", c.cfg.WaitFor, minPassing)
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()
	for {
		if c.depsReady(minPassing) {
			log.Printf("[hmi] dependencies ready")
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (c *Controller) depsReady(minPassing int) bool {
	for _, name := range c.cfg.WaitFor {
		if name == "" {
			continue
		}
		ents, _, err := c.consul.Health().Service(name, "", true, nil)
		if err != nil {
			return false
		}
		if len(ents) < minPassing {
			return false
		}
	}
	return true
}

func (c *Controller) reconcile(ctx context.Context) {
	if c.last == nil {
		c.last = map[string]string{}
	}
	if c.lastGen == nil {
		c.lastGen = map[string]time.Time{}
	}

	for _, id := range sortedKeys(c.last) {
		desiredRole, ok := c.assignments[id]
		if ok && desiredRole != "" && c.workerPresent(id) {
			continue
		}
		delete(c.last, id)
		delete(c.lastGen, id)
	}

	for _, id := range sortedKeys(c.assignments) {
		newRole := c.assignments[id]
		if newRole == "" {
			continue
		}
		if !c.workerPresent(id) {
			continue
		}
		oldRole := c.last[id]
		currentGen := c.workerGen(id)
		sameGen := !currentGen.IsZero() && currentGen.Equal(c.lastGen[id])
		if newRole == oldRole && sameGen {
			continue
		}
		c.last[id] = newRole
		if !currentGen.IsZero() {
			c.lastGen[id] = currentGen
		}
	}

	c.saveState(ctx)
}

func (c *Controller) loadState(ctx context.Context) {
	if c.cfg.StateKey == "" {
		return
	}
	var st roleState
	ok, err := c.kv.GetJSON(ctx, c.cfg.StateKey, &st)
	if err != nil {
		log.Printf("[hmi] state load error: %v", err)
		return
	}
	if ok && st.Workers != nil {
		c.last = st.Workers
	}
}

func (c *Controller) saveState(ctx context.Context) {
	if c.cfg.StateKey == "" {
		return
	}
	st := roleState{Workers: c.last, UpdatedAt: time.Now()}
	if err := c.kv.PutJSON(ctx, c.cfg.StateKey, &st); err != nil {
		log.Printf("[hmi] state save error: %v", err)
	}
}

func (c *Controller) publishDefaultAssignments(ctx context.Context) bool {
	if len(c.cfg.DefaultAssignments) == 0 {
		return false
	}
	published := false
	for _, a := range c.cfg.DefaultAssignments {
		if a.WorkerID == "" || a.Role == "" {
			continue
		}
		key := fmt.Sprintf("%s/%s", c.cfg.AssignmentsPrefix, a.WorkerID)
		if err := c.kv.PutJSON(ctx, key, &a); err != nil {
			log.Printf("[hmi] default assignment write failed key=%s err=%v", key, err)
			continue
		}
		published = true
	}
	if published {
		log.Printf("[hmi] published default assignments count=%d", len(c.cfg.DefaultAssignments))
	}
	return published
}

func (c *Controller) updateAssignments(assignments []Assignment) {
	if c.assignments == nil {
		c.assignments = map[string]string{}
	}
	next := map[string]string{}
	for _, a := range assignments {
		if a.WorkerID == "" {
			continue
		}
		next[a.WorkerID] = a.Role
	}
	c.assignments = next
}

func (c *Controller) updateWorkers(workers []WorkerStatus) {
	if c.workers == nil {
		c.workers = map[string]WorkerStatus{}
	}
	next := map[string]WorkerStatus{}
	for _, w := range workers {
		if w.ID == "" {
			continue
		}
		next[w.ID] = w
	}
	c.workers = next
}

func (c *Controller) workerPresent(id string) bool {
	if c.cfg.WorkersPrefix == "" {
		return true
	}
	_, ok := c.workers[id]
	return ok
}

func (c *Controller) workerGen(id string) time.Time {
	if c.cfg.WorkersPrefix == "" {
		return time.Time{}
	}
	w, ok := c.workers[id]
	if !ok {
		return time.Time{}
	}
	return w.StartedAt
}

func sortedKeys(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
