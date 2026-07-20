package service

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/umitbozkurt/consul-replctl/internal/orders"
	"github.com/umitbozkurt/consul-replctl/internal/servicereg"
	"github.com/umitbozkurt/consul-replctl/internal/store"
)

type Config struct {
	AgentID        string
	OrdersPrefix   string
	AckPrefix      string
	ServiceAddress string
	// LogDir is the directory where service stdout/stderr log files are written.
	// When empty, service output is not captured to disk.
	LogDir string
	// LogAddr is the HTTP listen address for the remote log viewer (e.g. ":8888").
	// Requires LogDir to be set.
	LogAddr string
}

type Agent struct {
	cfg Config
	kv  store.KV
	reg servicereg.Registry
	// running processes by service name
	mu     sync.Mutex
	procs  map[string]*procHandle
	lastMu sync.Mutex
	last   map[string]lastOrder
}

type procHandle struct {
	cmd       *exec.Cmd
	logFile   *os.File
	serviceID string
	checkID   string
	stopCh    chan struct{}
	stopOnce  sync.Once
}

type serviceCounter struct {
	Count     int64     `json:"count"`
	UpdatedAt time.Time `json:"updated_at"`
}

type lastOrder struct {
	Action orders.Action
	Role   string
	Epoch  int64
}

func New(cfg Config, kv store.KV, reg servicereg.Registry) *Agent {
	return &Agent{
		cfg:   cfg,
		kv:    kv,
		reg:   reg,
		procs: map[string]*procHandle{},
		last:  map[string]lastOrder{},
	}
}

func (a *Agent) Run(ctx context.Context) error {
	if a.cfg.LogAddr != "" && a.cfg.LogDir != "" {
		srv := newLogServer(a.cfg.LogDir)
		srv.start(ctx, a.cfg.LogAddr)
		log.Printf("[service-agent] log server listening on %s", a.cfg.LogAddr)
	}
	// Watch all orders under prefix, filter by TargetID
	ch := a.kv.WatchPrefixJSON(ctx, a.cfg.OrdersPrefix, func() any { return &[]orders.Order{} })
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case v, ok := <-ch:
			if !ok {
				return nil
			}
			lst := v.([]orders.Order)
			if len(lst) == 0 {
				continue
			}
			latestByService := map[string]orders.Order{}
			for _, ord := range lst {
				if ord.TargetID != a.cfg.AgentID || ord.Kind != orders.KindService {
					continue
				}
				svcName, _ := ord.Payload["service"].(string)
				if svcName == "" {
					continue
				}
				prev, ok := latestByService[svcName]
				if !ok || ord.Epoch > prev.Epoch {
					latestByService[svcName] = ord
				}
			}
			if len(latestByService) == 0 {
				continue
			}
			names := make([]string, 0, len(latestByService))
			for name := range latestByService {
				names = append(names, name)
			}
			sort.Strings(names)
			for _, name := range names {
				a.execute(ctx, latestByService[name])
			}
		}
	}
}

func (a *Agent) execute(ctx context.Context, ord orders.Order) {
	ack := orders.Ack{TargetID: a.cfg.AgentID, Action: ord.Action, Epoch: ord.Epoch, FinishedAt: time.Now()}
	err := error(nil)

	svcName, _ := ord.Payload["service"].(string)
	role, _ := ord.Payload["role"].(string)
	cmdStr, _ := ord.Payload["cmd"].(string)
	workDir, _ := ord.Payload["workDir"].(string)
	if svcName != "" && a.isDuplicateOrder(svcName, ord.Action, role, ord.Epoch) {
		ack.Ok = true
		_ = a.kv.PutJSON(ctx, fmt.Sprintf("%s/%s/%s", a.cfg.AckPrefix, svcName, a.cfg.AgentID), &ack)
		return
	}

	// args is []any
	argsAny, _ := ord.Payload["args"].([]any)
	args := make([]string, 0, len(argsAny))
	for _, x := range argsAny {
		if s, ok := x.(string); ok {
			args = append(args, s)
		}
	}
	// Expand simple placeholders ${ROLE}
	for i := range args {
		args[i] = strings.ReplaceAll(args[i], "${ROLE}", role)
	}

	switch ord.Action {
	case orders.ActionStart:
		err = a.startService(ctx, svcName, role, cmdStr, args, workDir, ord.Payload)
	case orders.ActionStop:
		err = a.stopService(svcName)
	default:
		err = nil
	}

	if err != nil {
		ack.Ok = false
		ack.Message = err.Error()
	} else {
		ack.Ok = true
	}
	_ = a.kv.PutJSON(ctx, fmt.Sprintf("%s/%s/%s", a.cfg.AckPrefix, svcName, a.cfg.AgentID), &ack)
}

func (a *Agent) isDuplicateOrder(svcName string, action orders.Action, role string, epoch int64) bool {
	a.lastMu.Lock()
	defer a.lastMu.Unlock()
	prev, ok := a.last[svcName]
	if ok && prev.Action == action && prev.Role == role && prev.Epoch == epoch {
		return true
	}
	a.last[svcName] = lastOrder{Action: action, Role: role, Epoch: epoch}
	return false
}

func (a *Agent) startService(ctx context.Context, name, role, cmdStr string, args []string, workDir string, payload map[string]any) error {
	if name == "" || cmdStr == "" {
		return fmt.Errorf("service start missing name/cmd")
	}
	// stop existing
	_ = a.stopService(name)

	args = append([]string{fmt.Sprintf("-DDEFAULT_REDUNDANCY_MODE=%s", strings.ToUpper(role))}, args...)
	log.Printf("[service-agent] starting proc name:%v, role:%v, cmd:%v, args:%v", name, role, cmdStr, args)
	cmd := exec.CommandContext(ctx, cmdStr, args...)
	if workDir != "" {
		cmd.Dir = workDir
	}
	var logFile *os.File
	if a.cfg.LogDir != "" {
		_, lf, err := openServiceLogFile(a.cfg.LogDir, name, role, a.cfg.AgentID)
		if err != nil {
			log.Printf("[service-agent] log file open error service=%s: %v", name, err)
		} else {
			logFile = lf
			cmd.Stdout = logFile
			cmd.Stderr = logFile
		}
	}
	if err := cmd.Start(); err != nil {
		if logFile != nil {
			_ = logFile.Close()
		}
		return err
	}
	pid := 0
	if cmd.Process != nil {
		pid = cmd.Process.Pid
	}
	instanceToken := strconv.Itoa(pid)
	if pid <= 0 {
		instanceToken = fmt.Sprintf("startup-%d", time.Now().UnixNano())
	}
	serviceID := serviceInstanceID(name, role, a.cfg.AgentID, instanceToken)
	checkID := fmt.Sprintf("check:%s", serviceID)
	handle := &procHandle{
		cmd:       cmd,
		logFile:   logFile,
		serviceID: serviceID,
		checkID:   checkID,
		stopCh:    make(chan struct{}),
	}
	a.mu.Lock()
	a.procs[name] = handle
	a.mu.Unlock()
	log.Printf("[service-agent] started service=%s role=%s pid=%d", name, role, pid)

	// Register service with TTL note that includes role + pid
	if a.reg != nil {
		ttl := "15s"
		if v, ok := payload["ttl"].(string); ok && v != "" {
			ttl = v
		}
		heartbeatEvery := 5 * time.Second
		if d, err := time.ParseDuration(ttl); err == nil && d > 0 {
			heartbeatEvery = d / 3
			if heartbeatEvery < time.Second {
				heartbeatEvery = time.Second
			}
			if heartbeatEvery >= d {
				heartbeatEvery = d / 2
				if heartbeatEvery < time.Second {
					heartbeatEvery = time.Second
				}
			}
		}
		addr := a.cfg.ServiceAddress
		if addr == "" {
			h, _ := os.Hostname()
			addr = h
		}
		tags := []string{}
		if tAny, ok := payload["tags"].([]any); ok {
			for _, x := range tAny {
				if s, ok := x.(string); ok {
					tags = append(tags, s)
				}
			}
		}
		if role != "" {
			tags = append(tags, role)
		}
		_ = a.reg.Register(ctx, servicereg.Registration{
			Name:    name,
			ID:      serviceID,
			Address: addr,
			Port:    0,
			Tags:    tags,
			CheckID: checkID,
			TTL:     ttl,
		})
		note := map[string]any{"service": map[string]any{"name": name, "role": role, "pid": pid}}
		b, _ := json.Marshal(note)
		_ = a.reg.SetTTL(ctx, checkID, servicereg.StatusPassing, string(b))
		// Heartbeat loop
		go func(handle *procHandle) {
			t := time.NewTicker(heartbeatEvery)
			defer t.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-handle.stopCh:
					return
				case <-t.C:
					if a.reg == nil {
						return
					}
					if cmd.ProcessState != nil && cmd.ProcessState.Exited() {
						log.Printf("[service-agent] heartbeat stopping service=%s reason=process-exited", name)
						return
					}
					_ = a.reg.SetTTL(ctx, checkID, servicereg.StatusPassing, string(b))
				}
			}
		}(handle)
	}

	go a.waitForExit(name, handle)
	return nil
}

func (a *Agent) stopService(name string) error {
	a.mu.Lock()
	handle := a.procs[name]
	delete(a.procs, name)
	a.mu.Unlock()
	if handle == nil {
		return nil
	}
	handle.stop()
	if handle.cmd != nil && handle.cmd.Process != nil {
		_ = handle.cmd.Process.Kill()
	}
	a.deregister(handle.serviceID)
	return nil
}

func openServiceLogFile(workDir, name, role, agentID string) (string, *os.File, error) {
	baseDir := workDir
	if baseDir == "" {
		baseDir = "."
	}
	subDir := agentID
	if subDir == "" {
		subDir = "node"
	}
	baseDir = filepath.Join(baseDir, subDir)
	if err := os.MkdirAll(baseDir, 0o755); err != nil {
		return "", nil, err
	}
	suffix := role
	if suffix == "" {
		suffix = "unknown"
	}
	logPath := filepath.Join(baseDir, name+"-"+suffix+".log")
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return "", nil, err
	}
	return logPath, logFile, nil
}

func (a *Agent) waitForExit(name string, handle *procHandle) {
	err := handle.cmd.Wait()
	a.safeClose(handle.logFile)
	if handle.stopped() {
		a.cleanupProc(name, handle.cmd)
		return
	}
	if err != nil {
		log.Printf("[service-agent] process exited service=%s err=%v", name, err)
	} else {
		log.Printf("[service-agent] process exited service=%s", name)
	}
	handle.stop()
	a.deregister(handle.serviceID)
	a.incrementRestartCount(context.Background(), name)
	a.cleanupProc(name, handle.cmd)
}

func (a *Agent) cleanupProc(name string, cmd *exec.Cmd) {
	a.mu.Lock()
	handle := a.procs[name]
	if handle != nil && handle.cmd == cmd {
		delete(a.procs, name)
	}
	a.mu.Unlock()
}

func (a *Agent) safeClose(f *os.File) {
	if f == nil {
		return
	}
	_ = f.Close()
}

func (a *Agent) deregister(serviceID string) {
	if a.reg == nil || serviceID == "" {
		return
	}
	_ = a.reg.Deregister(context.Background(), serviceID)
}

func (h *procHandle) stop() {
	if h == nil {
		return
	}
	h.stopOnce.Do(func() {
		close(h.stopCh)
	})
}

func (h *procHandle) stopped() bool {
	if h == nil {
		return false
	}
	select {
	case <-h.stopCh:
		return true
	default:
		return false
	}
}

func serviceInstanceID(name, role, agentID string, instanceToken string) string {
	nodeID := agentID
	if nodeID == "" {
		nodeID = "node"
	}
	if role == "" {
		role = "unknown"
	}
	if instanceToken == "" {
		instanceToken = fmt.Sprintf("startup-%d", time.Now().UnixNano())
	}
	return fmt.Sprintf("%s-%s-%s-%s", name, role, nodeID, instanceToken)
}

func (a *Agent) incrementRestartCount(ctx context.Context, svcName string) {
	if svcName == "" || a.kv == nil {
		return
	}
	agentID := a.cfg.AgentID
	if agentID == "" {
		agentID = "node"
	}
	key := fmt.Sprintf("stats/services/restarts/%s/%s", svcName, agentID)
	a.incrementCounter(ctx, key)
}

func (a *Agent) incrementCounter(ctx context.Context, key string) {
	var c serviceCounter
	ok, err := a.kv.GetJSON(ctx, key, &c)
	if err != nil {
		log.Printf("[service-agent] counter read error key=%s err=%v", key, err)
		return
	}
	if !ok {
		c = serviceCounter{}
	}
	c.Count++
	c.UpdatedAt = time.Now()
	if err := a.kv.PutJSON(ctx, key, &c); err != nil {
		log.Printf("[service-agent] counter write error key=%s err=%v", key, err)
	}
}
