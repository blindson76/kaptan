package hmi

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/umitbozkurt/consul-replctl/internal/store"
)

// RoleDef describes the start and stop commands for a role.
type RoleDef struct {
	StartCmd     string
	StartArgs    []string
	StartWorkDir string
	StopCmd      string
	StopArgs     []string
	StopWorkDir  string
	// StopTimeout is how long to wait for graceful shutdown before killing.
	// Falls back to Config.DefaultStopTimeout when zero.
	StopTimeout time.Duration
}

// Config holds the agent configuration.
type Config struct {
	AgentID            string
	AssignmentsPrefix  string
	WorkersPrefix      string
	Roles              map[string]RoleDef
	DefaultStopTimeout time.Duration
}

// Agent monitors this node's assigned role and runs the corresponding process.
type Agent struct {
	cfg  Config
	kv   store.KV
	mu   sync.Mutex
	role string // currently active role
}

// WorkerStatus is published to the workers prefix so the controller knows this node is alive.
type WorkerStatus struct {
	ID        string    `json:"id"`
	Host      string    `json:"host,omitempty"`
	StartedAt time.Time `json:"started_at"`
}

// nodeAssignment mirrors the Assignment struct stored by the controller.
type nodeAssignment struct {
	WorkerID string `json:"worker_id"`
	Role     string `json:"role"`
}

const (
	// defaultStopTimeout is the final fallback when no positive stop timeout is configured.
	defaultStopTimeout = 30 * time.Second
)

func New(cfg Config, kv store.KV) *Agent {
	return &Agent{cfg: cfg, kv: kv}
}

// Run publishes this node's presence, then continuously watches the assignments
// prefix for its own role assignment. When the role changes it gracefully stops
// the current role and starts the new one.
func (a *Agent) Run(ctx context.Context) error {
	a.publishPresence(ctx)

	if a.cfg.AssignmentsPrefix == "" {
		log.Printf("[hmi-agent] assignments_prefix not configured, agent idle")
		<-ctx.Done()
		return ctx.Err()
	}

	ch := a.kv.WatchPrefixJSON(ctx, a.cfg.AssignmentsPrefix, func() any { return &[]nodeAssignment{} })
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case v, ok := <-ch:
			if !ok {
				return nil
			}
			assignments := v.([]nodeAssignment)
			newRole := ""
			for _, assign := range assignments {
				if assign.WorkerID == a.cfg.AgentID {
					newRole = assign.Role
					break
				}
			}
			a.applyRole(ctx, newRole)
		}
	}
}

func (a *Agent) publishPresence(ctx context.Context) {
	if a.kv == nil || a.cfg.WorkersPrefix == "" || a.cfg.AgentID == "" {
		return
	}
	host, _ := os.Hostname()
	status := WorkerStatus{ID: a.cfg.AgentID, Host: host, StartedAt: time.Now()}
	key := fmt.Sprintf("%s/%s", a.cfg.WorkersPrefix, a.cfg.AgentID)
	if err := a.kv.PutJSONEphemeral(ctx, key, a.cfg.AgentID, &status); err != nil {
		log.Printf("[hmi-agent] publish presence failed key=%s err=%v", key, err)
	}
}

// applyRole transitions from the currently running role to newRole.
// It is a no-op when newRole equals the currently running role.
func (a *Agent) applyRole(ctx context.Context, newRole string) {
	a.mu.Lock()
	currentRole := a.role
	a.mu.Unlock()

	if newRole == currentRole {
		return
	}

	log.Printf("[hmi-agent] role change agent=%s %q -> %q", a.cfg.AgentID, currentRole, newRole)

	// Gracefully stop the previous role.
	if currentRole != "" {
		if err := a.gracefulStop(ctx, currentRole); err != nil {
			log.Printf("[hmi-agent] graceful stop error role=%s err=%v", currentRole, err)
		}
	}

	// Start the new role.
	if newRole != "" {
		if err := a.startRole(ctx, newRole); err != nil {
			log.Printf("[hmi-agent] start role error role=%s err=%v", newRole, err)
			return
		}
	}

	a.mu.Lock()
	a.role = newRole
	a.mu.Unlock()
}

// startRole runs the start command for role and requires it to exit successfully.
func (a *Agent) startRole(ctx context.Context, role string) error {
	def, ok := a.cfg.Roles[role]
	if !ok {
		return fmt.Errorf("unknown role: %s", role)
	}
	if def.StartCmd == "" {
		return fmt.Errorf("role %s has no start command", role)
	}

	args := parseStringArgs(def.StartArgs, role)
	log.Printf("[hmi-agent] starting role=%s cmd=%s args=%v", role, def.StartCmd, args)

	cmd := exec.CommandContext(ctx, def.StartCmd, args...)
	if def.StartWorkDir != "" {
		cmd.Dir = def.StartWorkDir
	}

	logPath, logFile, err := openHmiLogFile(def.StartWorkDir, role, a.cfg.AgentID)
	if err != nil {
		return err
	}
	defer func() { _ = logFile.Close() }()
	cmd.Stdout = logFile
	cmd.Stderr = logFile

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("start command failed (log=%s): %w", logPath, err)
	}

	log.Printf("[hmi-agent] start command completed role=%s log=%s", role, logPath)
	return nil
}

// gracefulStop runs the stop command for role and waits up to the configured timeout.
func (a *Agent) gracefulStop(ctx context.Context, role string) error {
	def, hasDef := a.cfg.Roles[role]
	timeout := a.cfg.DefaultStopTimeout
	if hasDef && def.StopTimeout > 0 {
		timeout = def.StopTimeout
	}
	if timeout <= 0 {
		timeout = defaultStopTimeout
	}

	if !hasDef || def.StopCmd == "" {
		return nil
	}

	stopArgs := parseStringArgs(def.StopArgs, role)
	log.Printf("[hmi-agent] running stop command role=%s cmd=%s", role, def.StopCmd)
	stopCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	return runStopCommand(stopCtx, role, def.StopCmd, stopArgs, def.StopWorkDir, a.cfg.AgentID)
}

// runStopCommand executes the stop script for a role and logs its output.
func runStopCommand(ctx context.Context, role, cmdStr string, args []string, workDir, agentID string) error {
	if cmdStr == "" {
		return nil
	}
	cmd := exec.CommandContext(ctx, cmdStr, args...)
	if workDir != "" {
		cmd.Dir = workDir
	}
	logPath, logFile, err := openHmiLogFile(workDir, role+"-stop", agentID)
	if err != nil {
		return err
	}
	defer func() { _ = logFile.Close() }()
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("stop command failed (log=%s): %w", logPath, err)
	}
	return nil
}

func parseStringArgs(args []string, role string) []string {
	result := make([]string, len(args))
	for i, s := range args {
		result[i] = strings.ReplaceAll(s, "${ROLE}", role)
	}
	return result
}

func openHmiLogFile(workDir, logName, agentID string) (string, *os.File, error) {
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
	logPath := filepath.Join(baseDir, logName+".log")
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return "", nil, err
	}
	return logPath, logFile, nil
}
