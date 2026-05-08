package kafka

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/umitbozkurt/consul-replctl/internal/store"
	"github.com/umitbozkurt/consul-replctl/internal/types"
)

type Config struct {
	WorkerID  string
	ReportKey string
	MetaDirs  []string
	HealthKey string

	NodeID     string
	LogDir     string
	MetaLogDir string

	StorageID string

	Host string
	// For combined mode we keep both addresses (advertised listeners).
	BrokerAddr     string
	ControllerAddr string
}

type Worker struct {
	cfg Config
	kv  store.KV
}

func New(cfg Config, kv store.KV) *Worker { return &Worker{cfg: cfg, kv: kv} }

func (w *Worker) RunOnce(ctx context.Context) error {
	attempt := 0
	for {
		attempt++
		err := w.runProbe(ctx)
		if err == nil {
			return nil
		}
		log.Printf("[kafka-worker] offline status probe attempt=%d failed: %v", attempt, err)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(5 * time.Second):
		}
	}
}

func (w *Worker) runProbe(ctx context.Context) error {
	log.Printf("[kafka-worker] offline status probe starting")
	clusterID := ""
	reportNodeID := w.cfg.NodeID
	eligible := true
	reason := "meta.properties not found (uninitialized member)"
	clusterID, diskNodeID, foundMeta, err := w.readLocalMetaProperties()
	if err != nil {
		eligible = false
		reason = fmt.Sprintf("meta.properties probe failed: %v", err)
		log.Printf("[kafka-worker] offline status probe failed: %v", err)
	} else if foundMeta {
		reason = ""
		if reportNodeID == "" {
			reportNodeID = diskNodeID
		}
		if w.cfg.NodeID != "" && diskNodeID != "" && w.cfg.NodeID != diskNodeID {
			eligible = false
			reason = fmt.Sprintf("node.id mismatch in meta.properties: expected=%s actual=%s", w.cfg.NodeID, diskNodeID)
		}
	}

	rep := types.CandidateReport{
		ID:                  w.cfg.WorkerID,
		Kind:                types.CandidateKafka,
		Host:                w.cfg.Host,
		KafkaClusterID:      clusterID,
		KafkaNodeID:         reportNodeID,
		KafkaBrokerAddr:     w.cfg.BrokerAddr,
		KafkaControllerAddr: w.cfg.ControllerAddr,
		KafkaStorageID:      w.cfg.StorageID,
		Eligible:            eligible,
		Reason:              reason,
		UpdatedAt:           time.Now(),
	}
	log.Printf("[kafka-worker] offline status report: %+v", rep)
	if err := w.kv.PutJSONEphemeral(ctx, w.cfg.ReportKey, w.cfg.WorkerID, &rep); err != nil {
		return err
	}
	if w.cfg.HealthKey != "" {
		h := types.HealthStatus{
			ID:        w.cfg.WorkerID,
			Healthy:   eligible,
			Reason:    reason,
			Note:      reason,
			UpdatedAt: time.Now(),
		}
		if err := w.kv.PutJSON(ctx, w.cfg.HealthKey, &h); err != nil {
			return err
		}
	}
	return nil
}

func readMetaProperties(path string) (clusterID, nodeID string, ok bool, err error) {
	f, err := os.Open(path)
	if err != nil {
		return "", "", false, err
	}
	defer f.Close()

	s := bufio.NewScanner(f)
	for s.Scan() {
		line := strings.TrimSpace(s.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if i := strings.Index(line, "="); i > 0 {
			k := strings.TrimSpace(line[:i])
			v := strings.TrimSpace(line[i+1:])
			switch k {
			case "cluster.id":
				clusterID = v
			case "node.id":
				nodeID = v
			}
		}
	}
	if err := s.Err(); err != nil {
		return "", "", false, err
	}
	if clusterID == "" && nodeID == "" {
		return "", "", false, nil
	}
	return clusterID, nodeID, true, nil
}

func (w *Worker) readLocalMetaProperties() (clusterID, nodeID string, found bool, err error) {
	for _, p := range w.metaPropertiesPaths() {
		clusterID, nodeID, found, err := readMetaProperties(p)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			return "", "", false, fmt.Errorf("read %s: %w", p, err)
		}
		if found {
			return clusterID, nodeID, true, nil
		}
	}
	return "", "", false, nil
}

func (w *Worker) metaPropertiesPaths() []string {
	seen := map[string]struct{}{}
	out := make([]string, 0, len(w.cfg.MetaDirs)+2)
	paths := make([]string, 0, len(w.cfg.MetaDirs)+2)
	paths = append(paths, w.cfg.MetaDirs...)
	paths = append(paths, w.cfg.MetaLogDir, w.cfg.LogDir)
	for _, p := range paths {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		if !strings.EqualFold(filepath.Base(p), "meta.properties") {
			p = filepath.Join(p, "meta.properties")
		}
		if _, ok := seen[p]; ok {
			continue
		}
		seen[p] = struct{}{}
		out = append(out, p)
	}
	return out
}
