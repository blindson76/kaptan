package hmi

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/umitbozkurt/consul-replctl/internal/orders"
	"github.com/umitbozkurt/consul-replctl/internal/store"
)

type Config struct {
	AgentID      string
	OrdersPrefix string
	AckPrefix    string
}

type Agent struct {
	cfg Config
	kv  store.KV

	mu      sync.Mutex
	procs   map[string]*procHandle
	stopped map[string]time.Time
	lastMu  sync.Mutex
	last    map[string]lastOrder
}

type procHandle struct {
	cmd     *exec.Cmd
	logFile *os.File
}

type lastOrder struct {
	Action orders.Action
	Role   string
	Epoch  int64
}

func New(cfg Config, kv store.KV) *Agent {
	return &Agent{
		cfg:     cfg,
		kv:      kv,
		procs:   map[string]*procHandle{},
		stopped: map[string]time.Time{},
		last:    map[string]lastOrder{},
	}
}

func (a *Agent) Run(ctx context.Context) error {
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
				if ord.TargetID != a.cfg.AgentID || ord.Kind != orders.KindHMI {
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

	args := parseArgs(ord.Payload["args"], role)

	switch ord.Action {
	case orders.ActionStart:
		err = a.startService(ctx, svcName, role, cmdStr, args, workDir)
	case orders.ActionStop:
		stopCmd, _ := ord.Payload["stopCmd"].(string)
		stopWorkDir, _ := ord.Payload["stopWorkDir"].(string)
		stopArgs := parseArgs(ord.Payload["stopArgs"], role)
		a.markStopped(svcName)
		if stopCmd != "" {
			err = a.runStopCommand(ctx, svcName, role, stopCmd, stopArgs, stopWorkDir)
		}
		if err == nil {
			err = a.stopService(svcName)
		}
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

func (a *Agent) startService(ctx context.Context, name, role, cmdStr string, args []string, workDir string) error {
	if name == "" || cmdStr == "" {
		return fmt.Errorf("hmi start missing name/cmd")
	}
	_ = a.stopService(name)
	a.clearStopped(name)

	log.Printf("[hmi-agent] starting proc name=%s role=%s cmd=%s args=%v", name, role, cmdStr, args)
	cmd := exec.CommandContext(ctx, cmdStr, args...)
	if workDir != "" {
		cmd.Dir = workDir
	}
	logPath, logFile, err := openHmiLogFile(workDir, name, role, a.cfg.AgentID)
	if err != nil {
		return err
	}
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	if err := cmd.Start(); err != nil {
		_ = logFile.Close()
		return err
	}
	a.mu.Lock()
	a.procs[name] = &procHandle{cmd: cmd, logFile: logFile}
	a.mu.Unlock()
	log.Printf("[hmi-agent] started service=%s role=%s pid=%d log=%s", name, role, cmd.Process.Pid, logPath)

	go a.waitForExit(name, cmd, logFile)
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
	if handle.cmd != nil && handle.cmd.Process != nil {
		_ = handle.cmd.Process.Kill()
	}
	if handle.logFile != nil {
		_ = handle.logFile.Close()
	}
	delete(a.procs, name)
	return nil
}

func (a *Agent) runStopCommand(ctx context.Context, name, role, cmdStr string, args []string, workDir string) error {
	if cmdStr == "" {
		return nil
	}
	log.Printf("[hmi-agent] stopping proc name=%s role=%s cmd=%s args=%v", name, role, cmdStr, args)
	cmd := exec.CommandContext(ctx, cmdStr, args...)
	if workDir != "" {
		cmd.Dir = workDir
	}
	logPath, logFile, err := openHmiLogFile(workDir, name, role, a.cfg.AgentID)
	if err != nil {
		return err
	}
	defer a.safeClose(logFile)
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("stop command failed (log=%s): %w", logPath, err)
	}
	return nil
}

func parseArgs(v any, role string) []string {
	argsAny, _ := v.([]any)
	args := make([]string, 0, len(argsAny))
	for _, x := range argsAny {
		if s, ok := x.(string); ok {
			args = append(args, s)
		}
	}
	for i := range args {
		args[i] = strings.ReplaceAll(args[i], "${ROLE}", role)
	}
	return args
}

func openHmiLogFile(workDir, name, role, agentID string) (string, *os.File, error) {
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

func (a *Agent) waitForExit(name string, cmd *exec.Cmd, logFile *os.File) {
	err := cmd.Wait()
	a.safeClose(logFile)
	if a.wasStopped(name) {
		a.cleanupProc(name, cmd)
		return
	}
	if err != nil {
		log.Printf("[hmi-agent] process exited service=%s err=%v", name, err)
	} else {
		log.Printf("[hmi-agent] process exited service=%s", name)
	}
	a.cleanupProc(name, cmd)
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

func (a *Agent) markStopped(name string) {
	if name == "" {
		return
	}
	a.mu.Lock()
	a.stopped[name] = time.Now()
	a.mu.Unlock()
}

func (a *Agent) clearStopped(name string) {
	if name == "" {
		return
	}
	a.mu.Lock()
	delete(a.stopped, name)
	a.mu.Unlock()
}

func (a *Agent) wasStopped(name string) bool {
	if name == "" {
		return false
	}
	a.mu.Lock()
	_, ok := a.stopped[name]
	if ok {
		delete(a.stopped, name)
	}
	a.mu.Unlock()
	return ok
}
