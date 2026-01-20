package hmi

import (
	"context"
	"fmt"
	"log"
	"sort"
	"time"

	"github.com/umitbozkurt/consul-replctl/internal/controllers/common"
	"github.com/umitbozkurt/consul-replctl/internal/orders"
	"github.com/umitbozkurt/consul-replctl/internal/store"
)

type Config struct {
	ControllerID string
	LockKey      string
	StateKey     string

	AssignmentsPrefix string
	OrdersPrefix      string
	AckPrefix         string
	ServiceName       string
	AckTimeout        time.Duration

	Roles              []RoleDef
	DefaultAssignments []Assignment

	OrderHistoryKeep int
}

type RoleDef struct {
	Name         string
	StartCmd     string
	StartArgs    []string
	StartWorkDir string
	StopCmd      string
	StopArgs     []string
	StopWorkDir  string
}

type Assignment struct {
	WorkerID string `json:"worker_id"`
	Role     string `json:"role"`
}

type roleState struct {
	Workers   map[string]string `json:"workers"`
	UpdatedAt time.Time         `json:"updated_at"`
}

type Controller struct {
	cfg    Config
	kv     store.KV
	locker interface {
		Acquire(context.Context, string, string) (func() error, error)
	}

	roles map[string]RoleDef
	last  map[string]string
}

func New(cfg Config, kv store.KV, locker interface {
	Acquire(context.Context, string, string) (func() error, error)
}) *Controller {
	if cfg.AssignmentsPrefix == "" {
		cfg.AssignmentsPrefix = "hmi/assignments"
	}
	if cfg.OrdersPrefix == "" {
		cfg.OrdersPrefix = "orders/hmi"
	}
	if cfg.AckPrefix == "" {
		cfg.AckPrefix = "acks/hmi"
	}
	if cfg.ServiceName == "" {
		cfg.ServiceName = "hmi"
	}
	if cfg.AckTimeout == 0 {
		cfg.AckTimeout = 30 * time.Second
	}
	roleMap := map[string]RoleDef{}
	for _, r := range cfg.Roles {
		if r.Name == "" {
			continue
		}
		roleMap[r.Name] = r
	}
	return &Controller{
		cfg:    cfg,
		kv:     kv,
		locker: locker,
		roles:  roleMap,
		last:   map[string]string{},
	}
}

func (c *Controller) Run(ctx context.Context) error {
	return common.RunLeaderLoop(ctx, c.locker, c.cfg.LockKey, c.cfg.ControllerID, c.runActive)
}

func (c *Controller) runActive(ctx context.Context) error {
	log.Printf("[hmi] runActive assignments_prefix=%s", c.cfg.AssignmentsPrefix)
	c.loadState(ctx)

	ch := c.kv.WatchPrefixJSON(ctx, c.cfg.AssignmentsPrefix, func() any { return &[]Assignment{} })
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case v, ok := <-ch:
			if !ok {
				return nil
			}
			assignments := v.([]Assignment)
			log.Printf("[hmi] new assignments: %v", assignments)
			if len(assignments) == 0 && c.publishDefaultAssignments(ctx) {
				continue
			}
			desired := map[string]string{}
			for _, a := range assignments {
				if a.WorkerID == "" {
					continue
				}
				desired[a.WorkerID] = a.Role
			}
			c.reconcile(ctx, desired)
		}
	}
}

func (c *Controller) reconcile(ctx context.Context, desired map[string]string) {
	if c.last == nil {
		c.last = map[string]string{}
	}

	for _, id := range sortedKeys(c.last) {
		role := c.last[id]
		desiredRole, ok := desired[id]
		if ok && desiredRole != "" {
			continue
		}
		if role != "" {
			if err := c.stopRole(ctx, id, role); err != nil {
				log.Printf("[hmi] stop role failed worker=%s role=%s err=%v", id, role, err)
				continue
			}
		}
		delete(c.last, id)
	}

	for _, id := range sortedKeys(desired) {
		newRole := desired[id]
		if newRole == "" {
			continue
		}
		oldRole := c.last[id]
		if newRole == oldRole {
			continue
		}
		def, ok := c.roles[newRole]
		if !ok {
			log.Printf("[hmi] unknown role=%s worker=%s", newRole, id)
			continue
		}
		if oldRole != "" && oldRole != newRole {
			if err := c.stopRole(ctx, id, oldRole); err != nil {
				log.Printf("[hmi] stop role failed worker=%s role=%s err=%v", id, oldRole, err)
			}
		}
		if err := c.startRole(ctx, id, newRole, def); err != nil {
			log.Printf("[hmi] start role failed worker=%s role=%s err=%v", id, newRole, err)
			continue
		}
		c.last[id] = newRole
	}

	c.saveState(ctx)
}

func (c *Controller) startRole(ctx context.Context, workerID string, role string, def RoleDef) error {
	payload := map[string]any{
		"service": c.cfg.ServiceName,
		"role":    role,
		"cmd":     def.StartCmd,
		"args":    def.StartArgs,
		"workDir": def.StartWorkDir,
	}
	epoch := time.Now().UnixNano()
	return c.issue(ctx, workerID, orders.ActionStart, epoch, payload)
}

func (c *Controller) stopRole(ctx context.Context, workerID string, role string) error {
	payload := map[string]any{
		"service": c.cfg.ServiceName,
		"role":    role,
	}
	if def, ok := c.roles[role]; ok {
		if def.StopCmd != "" {
			payload["stopCmd"] = def.StopCmd
		}
		if len(def.StopArgs) > 0 {
			payload["stopArgs"] = def.StopArgs
		}
		if def.StopWorkDir != "" {
			payload["stopWorkDir"] = def.StopWorkDir
		}
	}
	epoch := time.Now().UnixNano()
	if err := c.issue(ctx, workerID, orders.ActionStop, epoch, payload); err != nil {
		return err
	}
	return c.waitAck(ctx, workerID, orders.ActionStop, epoch)
}

func (c *Controller) issue(ctx context.Context, workerID string, action orders.Action, epoch int64, payload map[string]any) error {
	orderKey := fmt.Sprintf("%s/%s/%s", c.cfg.OrdersPrefix, c.cfg.ServiceName, workerID)
	ord := orders.Order{
		Kind:     orders.KindHMI,
		TargetID: workerID,
		Action:   action,
		Epoch:    epoch,
		IssuedAt: time.Now(),
		Payload:  payload,
	}
	log.Printf("[hmi] order publish worker=%s action=%s epoch=%d key=%s payload=%v", workerID, action, epoch, orderKey, payload)
	return orders.SaveWithHistory(ctx, c.kv, orderKey, ord, c.cfg.OrderHistoryKeep)
}

func (c *Controller) waitAck(ctx context.Context, workerID string, action orders.Action, epoch int64) error {
	if c.cfg.AckPrefix == "" {
		return nil
	}
	ackKey := fmt.Sprintf("%s/%s/%s", c.cfg.AckPrefix, c.cfg.ServiceName, workerID)
	deadline := time.Now().Add(c.cfg.AckTimeout)
	for time.Now().Before(deadline) {
		var ack orders.Ack
		ok, err := c.kv.GetJSON(ctx, ackKey, &ack)
		if err == nil && ok && ack.Epoch == epoch && ack.Action == action {
			if ack.Ok {
				return nil
			}
			return fmt.Errorf("ack failed: %s", ack.Message)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(1 * time.Second):
		}
	}
	log.Printf("[hmi] ack timeout worker=%s action=%s epoch=%d", workerID, action, epoch)
	return nil
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

func sortedKeys(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
