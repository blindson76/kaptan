package service

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/umitbozkurt/consul-replctl/internal/servicereg"
)

type fakeRegistry struct {
	mu          sync.Mutex
	registers   []servicereg.Registration
	deregisters []string
}

func (f *fakeRegistry) Register(_ context.Context, r servicereg.Registration) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.registers = append(f.registers, r)
	return nil
}

func (f *fakeRegistry) Deregister(_ context.Context, id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deregisters = append(f.deregisters, id)
	return nil
}

func (f *fakeRegistry) SetTTL(context.Context, string, servicereg.Status, string) error {
	return nil
}

func (f *fakeRegistry) deregisterCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.deregisters)
}

func TestStopServiceDeregistersImmediately(t *testing.T) {
	script := writeExecutable(t, "#!/bin/sh\nsleep 30\n")
	reg := &fakeRegistry{}
	agent := New(Config{AgentID: "node-1", ServiceAddress: "127.0.0.1"}, nil, reg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := agent.startService(ctx, "svc", "master", script, nil, filepath.Dir(script), map[string]any{"ttl": "3s"}); err != nil {
		t.Fatalf("startService failed: %v", err)
	}
	if err := agent.stopService("svc"); err != nil {
		t.Fatalf("stopService failed: %v", err)
	}

	waitFor(t, 2*time.Second, func() bool {
		return reg.deregisterCount() == 1
	})
	time.Sleep(200 * time.Millisecond)
	if got := reg.deregisterCount(); got != 1 {
		t.Fatalf("expected single deregister, got %d", got)
	}
}

func TestUnexpectedExitDeregistersService(t *testing.T) {
	script := writeExecutable(t, "#!/bin/sh\nexit 0\n")
	reg := &fakeRegistry{}
	agent := New(Config{AgentID: "node-1", ServiceAddress: "127.0.0.1"}, nil, reg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := agent.startService(ctx, "svc", "slave", script, nil, filepath.Dir(script), map[string]any{"ttl": "3s"}); err != nil {
		t.Fatalf("startService failed: %v", err)
	}

	waitFor(t, 2*time.Second, func() bool {
		return reg.deregisterCount() == 1
	})
}

func writeExecutable(t *testing.T, content string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "service.sh")
	if err := os.WriteFile(path, []byte(content), 0o755); err != nil {
		t.Fatalf("write script: %v", err)
	}
	return path
}

func waitFor(t *testing.T, timeout time.Duration, fn func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatal("condition not met before timeout")
}
