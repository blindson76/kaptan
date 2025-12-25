package mongo

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/umitbozkurt/consul-replctl/internal/orders"
	"github.com/umitbozkurt/consul-replctl/internal/servicereg"
	"github.com/umitbozkurt/consul-replctl/internal/store"
	"github.com/umitbozkurt/consul-replctl/internal/types"
)

type Config struct {
	AgentID   string
	OrdersKey string
	AckKey    string
	HealthKey string
	SpecKey   string

	MongodPath  string
	DBPath      string
	BindIP      string
	Port        int
	ReplSetName string
	LogPath     string

	AdminUser string
	AdminPass string

	Service servicereg.Registration
}

type Agent struct {
	cfg Config
	kv  store.KV
	reg servicereg.Registry

	baseService   servicereg.Registration
	activeService servicereg.Registration
	activeSlot    int
	regCancel     context.CancelFunc

	mongodMu           sync.Mutex
	mongodCmd          *exec.Cmd
	mongodExpectedExit bool
	mongodKeyFilePath  string
}

func New(cfg Config, kv store.KV, reg servicereg.Registry) *Agent {
	return &Agent{cfg: cfg, kv: kv, reg: reg, baseService: cfg.Service}
}

func (a *Agent) Run(ctx context.Context) error {
	log.Printf("[mongo-agent] starting agent_id=%s orders_key=%s ack_key=%s bind=%s port=%d dbpath=%s",
		a.cfg.AgentID, a.cfg.OrdersKey, a.cfg.AckKey, nz(a.cfg.BindIP, "0.0.0.0"), nzInt(a.cfg.Port, 27017), a.cfg.DBPath)
	if a.reg != nil && a.cfg.Service.ID != "" {
		log.Printf("[mongo-agent] service registration id=%s check=%s addr=%s port=%d", a.cfg.Service.ID, a.cfg.Service.CheckID, a.cfg.Service.Address, a.cfg.Service.Port)
	}

	ch := a.kv.WatchPrefixJSON(ctx, a.cfg.OrdersKey, func() any { return &[]orders.Order{} })
	specKey := a.cfg.SpecKey
	if specKey == "" {
		specKey = "spec/mongo"
	}
	specCh := a.kv.WatchPrefixJSON(ctx, specKey, func() any { return &[]types.ReplicaSpec{} })

	for {
		select {
		case <-ctx.Done():
			a.stopServiceRegistration()
			return ctx.Err()
		case v, ok := <-ch:
			if !ok {
				a.stopServiceRegistration()
				return nil
			}
			lst := v.([]orders.Order)
			if len(lst) == 0 {
				continue
			}
			ord := lst[len(lst)-1]
			log.Printf("[mongo-agent] observed order target=%s action=%s epoch=%d (watch=%s)", ord.TargetID, ord.Action, ord.Epoch, a.cfg.OrdersKey)
			if ord.TargetID != a.cfg.AgentID {
				log.Printf("[mongo-agent] ignoring order for target %s (this agent: %s)", ord.TargetID, a.cfg.AgentID)
				continue
			}
			a.execute(ctx, ord)
		case v, ok := <-specCh:
			if !ok {
				a.stopServiceRegistration()
				return nil
			}
			specs := v.([]types.ReplicaSpec)
			var spec types.ReplicaSpec
			if len(specs) > 0 {
				spec = specs[len(specs)-1]
			}
			a.updateServiceRegistration(ctx, spec)
		}
	}
}

func (a *Agent) execute(ctx context.Context, ord orders.Order) {
	ack := orders.Ack{TargetID: a.cfg.AgentID, Action: ord.Action, Epoch: ord.Epoch, FinishedAt: time.Now()}
	err := error(nil)

	switch ord.Action {
	case orders.ActionWipe:
		log.Printf("[mongo-agent] action wipe dbpath=%s epoch=%d", a.cfg.DBPath, ord.Epoch)
		err = wipeDir(a.cfg.DBPath)
	case orders.ActionStart:
		repl := a.cfg.ReplSetName
		if v, ok := ord.Payload["replSetName"].(string); ok && v != "" {
			repl = v
		}
		log.Printf("[mongo-agent] received order action=%s epoch=%d replSet=%s", ord.Action, ord.Epoch, repl)
		err = a.startMongod(ctx, repl)
	case orders.ActionStop:
		log.Printf("[mongo-agent] received order action=%s epoch=%d", ord.Action, ord.Epoch)
		err = a.shutdown(ctx)
	case orders.ActionInit:
		members, _ := ord.Payload["members"].([]any)
		repl, _ := ord.Payload["replSetName"].(string)
		log.Printf("[mongo-agent] received order action=%s epoch=%d members=%v replSet=%s", ord.Action, ord.Epoch, members, repl)
		err = a.rsInitiate(ctx, repl, members)
	case orders.ActionReconfigure:
		members, _ := ord.Payload["members"].([]any)
		repl, _ := ord.Payload["replSetName"].(string)
		force, _ := ord.Payload["force"].(bool)
		log.Printf("[mongo-agent] received order action=%s epoch=%d members=%v replSet=%s force=%v", ord.Action, ord.Epoch, members, repl, force)
		err = a.rsReconfigWithForce(ctx, repl, members, force)
	}
	if err != nil {
		ack.Ok = false
		ack.Message = err.Error()
		log.Printf("%s", redf("[mongo-agent] action %s epoch=%d failed: %v", ord.Action, ord.Epoch, err))
	} else {
		ack.Ok = true
		log.Printf("[mongo-agent] action %s epoch=%d ok", ord.Action, ord.Epoch)
	}
	err = a.kv.PutJSON(ctx, a.cfg.AckKey, &ack)
	log.Printf("[mongo-agent] ack published action=%s epoch=%d ok=%v, err=%v", ord.Action, ord.Epoch, ack.Ok, err)
}

func wipeDir(dir string) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	for _, e := range entries {
		_ = os.RemoveAll(filepath.Join(dir, e.Name()))
	}
	return nil
}

func (a *Agent) startMongod(ctx context.Context, repl string) error {
	keyFilePath, err := a.ensureKeyFile()
	if err != nil {
		return err
	}
	args := []string{
		"--dbpath", a.cfg.DBPath,
		"--bind_ip", nz(a.cfg.BindIP, "0.0.0.0"),
		"--port", fmt.Sprintf("%d", nzInt(a.cfg.Port, 27017)),
		"--replSet", repl,
		"--logpath", nz(a.cfg.LogPath, filepath.Join(a.cfg.DBPath, "mongod.log")),
		"--logappend",
		"--auth",
		"--keyFile", keyFilePath,
	}
	a.mongodMu.Lock()
	if a.mongodCmd != nil && a.mongodCmd.Process != nil && a.mongodCmd.ProcessState == nil {
		log.Printf("[mongo-agent] mongod already running pid=%d, skipping start", a.mongodCmd.Process.Pid)
		a.mongodMu.Unlock()
		return nil
	}
	a.mongodMu.Unlock()
	log.Printf("[mongo-agent] exec mongod: %s %s", a.cfg.MongodPath, strings.Join(args, " "))
	cmd := exec.CommandContext(ctx, a.cfg.MongodPath, args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		return err
	}
	a.mongodMu.Lock()
	a.mongodCmd = cmd
	a.mongodExpectedExit = false
	a.mongodMu.Unlock()
	log.Printf("[mongo-agent] started mongod pid=%d", cmd.Process.Pid)
	go a.watchMongod(ctx, cmd)
	waitCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	log.Printf("[mongo-agent] waiting for mongod to accept connections on %s:%d", a.connectHost(), nzInt(a.cfg.Port, 27017))
	if err := a.waitMongodReady(waitCtx); err != nil {
		return fmt.Errorf("mongod did not become ready: %w", err)
	}
	log.Printf("[mongo-agent] mongod is ready")
	return nil
}

func (a *Agent) mongoClient(ctx context.Context) (*mongo.Client, error) {
	host := a.connectHost()
	uri := fmt.Sprintf("mongodb://%s:%d/?directConnection=true", host, nzInt(a.cfg.Port, 27017))
	opts := options.Client().ApplyURI(uri)
	if a.cfg.AdminUser != "" {
		opts.SetAuth(options.Credential{
			Username:   a.cfg.AdminUser,
			Password:   a.cfg.AdminPass,
			AuthSource: "admin",
		})
	}
	return mongo.Connect(ctx, opts)
}

func (a *Agent) waitMongodReady(ctx context.Context) error {
	host := a.connectHost()
	uri := fmt.Sprintf("mongodb://%s:%d/?directConnection=true", host, nzInt(a.cfg.Port, 27017))
	opts := options.Client().ApplyURI(uri).SetServerSelectionTimeout(2 * time.Second)
	if a.cfg.AdminUser != "" {
		opts.SetAuth(options.Credential{
			Username:   a.cfg.AdminUser,
			Password:   a.cfg.AdminPass,
			AuthSource: "admin",
		})
	}
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		cli, err := mongo.Connect(ctx, opts)
		if err == nil {
			pingCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
			err = cli.Database("admin").RunCommand(pingCtx, bson.D{{Key: "ping", Value: 1}}).Err()
			cancel()
			_ = cli.Disconnect(context.Background())
		}
		if err == nil {
			return nil
		}
		time.Sleep(300 * time.Millisecond)
	}
}

func (a *Agent) shutdown(ctx context.Context) error {
	a.setMongodExpectedExit(true)
	cli, err := a.mongoClient(ctx)
	if err != nil {
		a.setMongodExpectedExit(false)
		return err
	}
	defer cli.Disconnect(context.Background())
	return cli.Database("admin").RunCommand(ctx, bson.D{{Key: "shutdown", Value: 1}}).Err()
}

func (a *Agent) watchMongod(ctx context.Context, cmd *exec.Cmd) {
	err := cmd.Wait()
	a.mongodMu.Lock()
	expected := a.mongodExpectedExit
	if a.mongodCmd == cmd {
		a.mongodCmd = nil
	}
	a.mongodExpectedExit = false
	a.mongodMu.Unlock()
	if ctx.Err() != nil || expected {
		log.Printf("[mongo-agent] mongod exited (expected): %v", err)
		return
	}
	reason := "mongod exited"
	if err != nil {
		reason = fmt.Sprintf("mongod exited: %v", err)
	}
	log.Printf("%s", redf("[mongo-agent] mongod exited unexpectedly: %v", err))
	a.stopServiceRegistration()
	a.publishUnhealthy(reason)
}

func (a *Agent) setMongodExpectedExit(v bool) {
	a.mongodMu.Lock()
	a.mongodExpectedExit = v
	a.mongodMu.Unlock()
}

func (a *Agent) publishUnhealthy(reason string) {
	if a.cfg.HealthKey == "" {
		return
	}
	meta := map[string]string{}
	if a.activeService.ID != "" {
		meta["serviceId"] = a.activeService.ID
	}
	if a.activeService.CheckID != "" {
		meta["checkId"] = a.activeService.CheckID
	}
	if a.activeService.Address != "" {
		meta["address"] = a.activeService.Address
	}
	if a.activeService.Port > 0 {
		meta["port"] = fmt.Sprintf("%d", a.activeService.Port)
	}
	if len(a.activeService.Tags) > 0 {
		meta["tags"] = strings.Join(a.activeService.Tags, ",")
	}
	h := types.HealthStatus{
		ID:          a.cfg.AgentID,
		Healthy:     false,
		Reason:      reason,
		Note:        reason,
		ServiceMeta: meta,
		UpdatedAt:   time.Now(),
	}
	if err := a.kv.PutJSON(context.Background(), a.cfg.HealthKey, &h); err != nil {
		log.Printf("%s", redf("[mongo-agent] health publish error: %v", err))
	}
}

func (a *Agent) rsInitiate(ctx context.Context, repl string, members []any) error {
	repl = nz(repl, a.cfg.ReplSetName)
	cfg := buildReplCfgDoc(repl, members, 1)
	cli, err := a.mongoClient(ctx)
	if err != nil {
		return err
	}
	defer cli.Disconnect(context.Background())
	cmd := bson.D{{Key: "replSetInitiate", Value: cfg}}
	log.Printf("[mongo-agent] replSetInitiate config=%v", cfg)
	return cli.Database("admin").RunCommand(ctx, cmd).Err()
}
func (a *Agent) rsReconfig(ctx context.Context, repl string, members []any) error {
	return a.rsReconfigWithForce(ctx, repl, members, true)
}

func (a *Agent) rsReconfigWithForce(ctx context.Context, repl string, members []any, force bool) error {
	repl = nz(repl, a.cfg.ReplSetName)
	cfg := buildReplCfgDoc(repl, members, time.Now().Unix())
	cli, err := a.mongoClient(ctx)
	if err != nil {
		return err
	}
	defer cli.Disconnect(context.Background())
	cmd := bson.D{{Key: "replSetReconfig", Value: cfg}, {Key: "force", Value: force}}
	log.Printf("[mongo-agent] replSetReconfig config=%v force=%v", cfg, force)
	return cli.Database("admin").RunCommand(ctx, cmd).Err()
}
func buildReplCfgDoc(repl string, members []any, version int64) bson.M {
	mems := make([]bson.M, 0, len(members))
	for i, m := range members {
		host := ""
		memberID := i
		switch v := m.(type) {
		case string:
			host = v
		case map[string]any:
			if h, ok := v["host"].(string); ok {
				host = h
			}
			if id, ok := memberIDFromAny(v["id"]); ok {
				memberID = id
			}
		}
		if host == "" {
			continue
		}
		mems = append(mems, bson.M{"_id": memberID, "host": host})
	}
	cfg := bson.M{
		"_id":     repl,
		"members": mems,
	}
	if version > 0 {
		cfg["version"] = version
	}
	return cfg
}

func memberIDFromAny(v any) (int, bool) {
	switch t := v.(type) {
	case int:
		return t, true
	case int32:
		return int(t), true
	case int64:
		return int(t), true
	case float64:
		return int(t), true
	case float32:
		return int(t), true
	default:
		return 0, false
	}
}

func (a *Agent) connectHost() string {
	ip := strings.TrimSpace(a.cfg.BindIP)
	switch ip {
	case "", "0.0.0.0", "::", "::0":
		return "127.0.0.1"
	default:
		return ip
	}
}

func (a *Agent) updateServiceRegistration(ctx context.Context, spec types.ReplicaSpec) {
	if a.reg == nil || a.baseService.Name == "" {
		return
	}
	member := false
	slot := 0
	for _, id := range spec.Members {
		slot++
		if id == a.cfg.AgentID {
			member = true
			break
		}
	}
	if member {
		newReg := a.baseService
		if newReg.Name == "" {
			newReg.Name = "mongo"
		}
		newReg.ID = fmt.Sprintf("%s-%d", newReg.Name, slot)
		newReg.CheckID = fmt.Sprintf("check:%s-%d", newReg.Name, slot)
		if a.activeService.ID == newReg.ID && a.activeSlot == slot {
			return
		}
		a.stopServiceRegistration()
		if err := a.reg.Register(ctx, newReg); err != nil {
			log.Printf("%s", redf("[mongo-agent] service register failed: %v", err))
			return
		}
		a.activeService = newReg
		a.activeSlot = slot
		ctxReg, cancel := context.WithCancel(ctx)
		a.regCancel = cancel
		_ = a.reg.SetTTL(ctxReg, newReg.CheckID, servicereg.StatusWarning, "startup")
		go a.roleHeartbeat(ctxReg)
		log.Printf("[mongo-agent] service registered (member of spec slot=%d) id=%s", slot, newReg.ID)
		return
	}
	if !member {
		a.stopServiceRegistration()
	}
}

func (a *Agent) stopServiceRegistration() {
	if a.regCancel != nil {
		a.regCancel()
		a.regCancel = nil
	}
	if a.activeService.ID != "" && a.reg != nil {
		_ = a.reg.Deregister(context.Background(), a.activeService.ID)
	}
	a.activeService = servicereg.Registration{}
	a.activeSlot = 0
}

func (a *Agent) roleHeartbeat(ctx context.Context) {
	log.Printf("[mongo-agent] starting role heartbeat for service check_id=%s", a.activeService.CheckID)
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if a.reg == nil || a.activeService.CheckID == "" {
				continue
			}
			state, stateStr, rsid, optime, term, syncSource, ok, prog := a.probeReplStatus(ctx)
			note := a.buildServiceNote(state, stateStr, rsid, optime, term, syncSource, prog)

			// Map to Consul TTL status
			switch state {
			case 1, 2, 7: // PRIMARY, SECONDARY, ARBITER
				_ = a.reg.SetTTL(ctx, a.activeService.CheckID, servicereg.StatusPassing, note)
			case 5, 3, 0: // STARTUP2, RECOVERING, STARTUP
				_ = a.reg.SetTTL(ctx, a.activeService.CheckID, servicereg.StatusWarning, note)
			case 9, 8, 6, 10: // ROLLBACK, DOWN, UNKNOWN, REMOVED
				_ = a.reg.SetTTL(ctx, a.activeService.CheckID, servicereg.StatusCritical, note)
			default:
				if !ok {
					_ = a.reg.SetTTL(ctx, a.activeService.CheckID, servicereg.StatusWarning, note)
				} else {
					_ = a.reg.SetTTL(ctx, a.activeService.CheckID, servicereg.StatusWarning, note)
				}
			}

			log.Printf("[mongo-agent] healthKey=%s role heartbeat state=%d stateStr=%s rsid=%s optime=%s term=%v syncSource=%s ok=%v",
				a.cfg.HealthKey, state, stateStr, rsid, optime, term, syncSource, ok)

			// Publish health to KV for controller consumption.
			if a.cfg.HealthKey != "" {
				healthy := ok && (state == 1 || state == 2 || state == 7) // PRIMARY/SECONDARY/ARBITER considered healthy
				meta := map[string]string{}
				if a.activeService.ID != "" {
					meta["serviceId"] = a.activeService.ID
				}
				if a.activeService.CheckID != "" {
					meta["checkId"] = a.activeService.CheckID
				}
				if a.activeService.Address != "" {
					meta["address"] = a.activeService.Address
				}
				if a.activeService.Port > 0 {
					meta["port"] = fmt.Sprintf("%d", a.activeService.Port)
				}
				if len(a.activeService.Tags) > 0 {
					meta["tags"] = strings.Join(a.activeService.Tags, ",")
				}
				h := types.HealthStatus{
					ID:          a.cfg.AgentID,
					Healthy:     healthy,
					Reason:      note,
					Note:        note,
					ServiceMeta: meta,
					UpdatedAt:   time.Now(),
				}
				log.Printf("[mongo-agent] publishing health status healthy=%v reason=%s", h.Healthy, h.Reason)
				if err := a.kv.PutJSON(ctx, a.cfg.HealthKey, &h); err != nil {
					log.Printf("[mongo-agent] health publish error: %v", err)
				}
			}
		}
	}
}

func (a *Agent) detectRole(ctx context.Context) (string, bool) {
	cli, err := a.mongoClient(ctx)
	if err != nil {
		return "startup", false
	}
	defer cli.Disconnect(context.Background())
	var res bson.M
	if err := cli.Database("admin").RunCommand(ctx, bson.D{{Key: "hello", Value: 1}}).Decode(&res); err != nil {
		return "startup", false
	}
	if v, ok := res["isWritablePrimary"].(bool); ok && v {
		return "primary", true
	}
	if v, ok := res["secondary"].(bool); ok && v {
		return "secondary", true
	}
	return "startup", true
}

func nz(s, d string) string {
	if s == "" {
		return d
	}
	return s
}
func nzInt(v, d int) int {
	if v == 0 {
		return d
	}
	return v
}
func redf(format string, args ...any) string {
	return fmt.Sprintf("\x1b[31m"+format+"\x1b[0m", args...)
}

func mongoStateStr(state int) string {
	switch state {
	case 0:
		return "STARTUP"
	case 1:
		return "PRIMARY"
	case 2:
		return "SECONDARY"
	case 3:
		return "RECOVERING"
	case 5:
		return "STARTUP2"
	case 6:
		return "UNKNOWN"
	case 7:
		return "ARBITER"
	case 8:
		return "DOWN"
	case 9:
		return "ROLLBACK"
	case 10:
		return "REMOVED"
	default:
		return "UNKNOWN"
	}
}

func (a *Agent) ensureKeyFile() (string, error) {
	a.mongodMu.Lock()
	defer a.mongodMu.Unlock()
	if a.mongodKeyFilePath != "" {
		return a.mongodKeyFilePath, nil
	}
	f, err := os.CreateTemp("", "mongod-keyfile-*")
	if err != nil {
		return "", err
	}
	if err := os.WriteFile(f.Name(), []byte("MONGOSECRET"), 0o600); err != nil {
		_ = os.Remove(f.Name())
		return "", err
	}
	a.mongodKeyFilePath = f.Name()
	log.Printf("[mongo-agent] created keyFile at %s", a.mongodKeyFilePath)
	return a.mongodKeyFilePath, nil
}

func (a *Agent) buildServiceNote(state int, stateStr string, rsid string, optime string, term any, syncSource string, progress map[string]any) string {
	m := map[string]any{
		"mongo": map[string]any{
			"state":    state,
			"stateStr": stateStr,
		},
	}
	mm := m["mongo"].(map[string]any)
	if rsid != "" {
		mm["replicaSetId"] = rsid
	}
	if optime != "" {
		mm["optimeTs"] = optime
	}
	if term != nil {
		mm["term"] = term
	}
	if syncSource != "" {
		mm["syncSource"] = syncSource
	}
	if progress != nil {
		mm["progress"] = progress
	}
	b, err := json.Marshal(m)
	if err != nil {
		return stateStr
	}
	return string(b)
}

func (a *Agent) probeReplStatus(ctx context.Context) (state int, stateStr string, rsid string, optime string, term any, syncSource string, ok bool, progress map[string]any) {
	cli, err := a.mongoClient(ctx)
	if err != nil {
		return 0, "STARTUP", "", "", nil, "", false, nil
	}
	defer cli.Disconnect(context.Background())

	// replSetGetStatus
	var st bson.M
	if err := cli.Database("admin").RunCommand(ctx, bson.D{{Key: "replSetGetStatus", Value: 1}}).Decode(&st); err != nil {
		// fallback to hello
		var hello bson.M
		if err2 := cli.Database("admin").RunCommand(ctx, bson.D{{Key: "hello", Value: 1}}).Decode(&hello); err2 == nil {
			if v, _ := hello["isWritablePrimary"].(bool); v {
				return 1, "PRIMARY", "", "", nil, "", true, nil
			}
			if v, _ := hello["secondary"].(bool); v {
				return 2, "SECONDARY", "", "", nil, "", true, nil
			}
		}
		return 0, "STARTUP", "", "", nil, "", false, nil
	}

	// state
	if ms, ok2 := st["myState"].(int32); ok2 {
		state = int(ms)
	} else if ms, ok2 := st["myState"].(int64); ok2 {
		state = int(ms)
	} else if ms, ok2 := st["myState"].(float64); ok2 {
		state = int(ms)
	} else {
		state = 0
	}
	stateStr = mongoStateStr(state)

	// members: find self
	if mems, ok2 := st["members"].(bson.A); ok2 {
		for _, it := range mems {
			m, _ := it.(bson.M)
			if m == nil {
				continue
			}
			if self, ok3 := m["self"].(bool); ok3 && self {
				if name, ok4 := m["name"].(string); ok4 {
					// name is host:port, not rs id
					_ = name
				}
				if ss, ok4 := m["syncSourceHost"].(string); ok4 {
					syncSource = ss
				}
				if t, ok4 := m["term"]; ok4 {
					term = t
				}
				if od, ok4 := m["optimeDate"].(primitive.DateTime); ok4 {
					optime = od.Time().UTC().Format(time.RFC3339)
				}
				break
			}
		}
	}

	// rsid: from config doc or set name if present
	if set, ok2 := st["set"].(string); ok2 {
		rsid = set
	}

	// initial sync progress: only when STARTUP2
	if state == 5 {
		progress = map[string]any{
			"op":        "initial_sync",
			"phase":     "running",
			"done":      false,
			"updatedAt": time.Now().UTC().Format(time.RFC3339),
		}
		// try to extract extra info if present
		if iss, ok2 := st["initialSyncStatus"].(bson.M); ok2 {
			// keep small
			if s, ok3 := iss["status"].(string); ok3 && s != "" {
				progress["phase"] = strings.ToLower(s)
			}
		}
	}

	return state, stateStr, rsid, optime, term, syncSource, true, progress
}
