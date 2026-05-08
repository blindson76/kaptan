package kafka

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/umitbozkurt/consul-replctl/internal/types"
)

type testKV struct {
	report types.CandidateReport
	health types.HealthStatus
}

func (k *testKV) PutJSON(_ context.Context, _ string, v any) error {
	if h, ok := v.(*types.HealthStatus); ok {
		k.health = *h
	}
	return nil
}

func (k *testKV) PutJSONEphemeral(_ context.Context, _ string, _ string, v any) error {
	if r, ok := v.(*types.CandidateReport); ok {
		k.report = *r
	}
	return nil
}

func (k *testKV) GetJSON(context.Context, string, any) (bool, error) { return false, nil }
func (k *testKV) Delete(context.Context, string) error               { return nil }
func (k *testKV) ListJSON(context.Context, string, any) error        { return nil }
func (k *testKV) WatchPrefixJSON(context.Context, string, func() any) <-chan any {
	return nil
}

func TestRunProbeReadsMetaPropertiesAndKeepsDataDirs(t *testing.T) {
	t.Parallel()

	tmp := t.TempDir()
	metaDir := filepath.Join(tmp, "meta")
	logDir := filepath.Join(tmp, "log")
	if err := os.MkdirAll(metaDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(logDir, 0o755); err != nil {
		t.Fatal(err)
	}
	metaPath := filepath.Join(metaDir, "meta.properties")
	if err := os.WriteFile(metaPath, []byte("cluster.id=cluster-a\nnode.id=2\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	sentinel := filepath.Join(logDir, "sentinel.txt")
	if err := os.WriteFile(sentinel, []byte("keep"), 0o644); err != nil {
		t.Fatal(err)
	}

	kv := &testKV{}
	w := New(Config{
		WorkerID:   "node-2",
		ReportKey:  "candidates/kafka/node-2",
		HealthKey:  "health/kafka/node-2",
		NodeID:     "2",
		MetaDirs:   []string{metaDir},
		LogDir:     logDir,
		BrokerAddr: "127.0.0.1:9092",
	}, kv)

	if err := w.runProbe(context.Background()); err != nil {
		t.Fatalf("runProbe error: %v", err)
	}
	if kv.report.KafkaClusterID != "cluster-a" {
		t.Fatalf("expected cluster id cluster-a, got %q", kv.report.KafkaClusterID)
	}
	if !kv.report.Eligible {
		t.Fatalf("expected eligible=true, reason=%q", kv.report.Reason)
	}
	if _, err := os.Stat(sentinel); err != nil {
		t.Fatalf("expected existing log dir contents to remain, stat error: %v", err)
	}
}

func TestRunProbeMarksNodeIDMismatchIneligible(t *testing.T) {
	t.Parallel()

	tmp := t.TempDir()
	metaDir := filepath.Join(tmp, "meta")
	if err := os.MkdirAll(metaDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(metaDir, "meta.properties"), []byte("cluster.id=cluster-a\nnode.id=9\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	kv := &testKV{}
	w := New(Config{
		WorkerID:  "node-2",
		ReportKey: "candidates/kafka/node-2",
		NodeID:    "2",
		MetaDirs:  []string{metaDir},
	}, kv)

	if err := w.runProbe(context.Background()); err != nil {
		t.Fatalf("runProbe error: %v", err)
	}
	if kv.report.Eligible {
		t.Fatalf("expected eligible=false for node mismatch")
	}
	if !strings.Contains(kv.report.Reason, "node.id mismatch") {
		t.Fatalf("expected mismatch reason, got %q", kv.report.Reason)
	}
}
