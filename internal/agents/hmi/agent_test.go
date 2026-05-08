package hmi

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestApplyRoleKeepsRoleActiveAfterStartCommandExits(t *testing.T) {
	t.Parallel()

	workDir := t.TempDir()
	agent := New(Config{
		AgentID: "node-1",
		Roles: map[string]RoleDef{
			"demo": {
				StartCmd:     "sh",
				StartArgs:    []string{"-c", "printf started > start.txt"},
				StartWorkDir: workDir,
				StopCmd:      "sh",
				StopArgs:     []string{"-c", "printf stopped > stop.txt"},
				StopWorkDir:  workDir,
				StopTimeout:  time.Second,
			},
		},
		DefaultStopTimeout: time.Second,
	}, nil)

	ctx := context.Background()

	agent.applyRole(ctx, "demo")

	if got := agent.role; got != "demo" {
		t.Fatalf("expected role to remain active after successful start command, got %q", got)
	}
	if _, err := os.Stat(filepath.Join(workDir, "start.txt")); err != nil {
		t.Fatalf("expected start command side effect: %v", err)
	}

	agent.applyRole(ctx, "")

	if got := agent.role; got != "" {
		t.Fatalf("expected role to clear after stop, got %q", got)
	}
	if _, err := os.Stat(filepath.Join(workDir, "stop.txt")); err != nil {
		t.Fatalf("expected stop command side effect: %v", err)
	}
}

func TestApplyRoleLeavesRoleInactiveWhenStartCommandFails(t *testing.T) {
	t.Parallel()

	workDir := t.TempDir()
	agent := New(Config{
		AgentID: "node-1",
		Roles: map[string]RoleDef{
			"broken": {
				StartCmd:     "sh",
				StartArgs:    []string{"-c", "exit 2"},
				StartWorkDir: workDir,
			},
		},
	}, nil)

	agent.applyRole(context.Background(), "broken")

	if got := agent.role; got != "" {
		t.Fatalf("expected role to remain inactive after failed start command, got %q", got)
	}
}
