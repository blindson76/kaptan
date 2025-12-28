package kafka

import (
	"bufio"
	"context"
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
	log.Printf("[kafka-worker] offline status probe starting")

	w.cleanupDataDirs()

	clusterID := ""
	eligible := false
	reason := ""

	// Scan meta.properties under each meta dir. Prefer the newest one.
	newest := time.Time{}
	foundMeta := false
	for _, d := range w.cfg.MetaDirs {
		p := filepath.Join(d, "meta.properties")
		st, err := os.Stat(p)
		if err != nil {
			continue
		}
		foundMeta = true
		if st.ModTime().After(newest) {
			newest = st.ModTime()
			c, _, ok := readMetaProperties(p)
			if ok {
				clusterID = c
				eligible = true
				reason = ""
			} else {
				eligible = false
				reason = "meta.properties parse failed"
			}
		}
	}
	if !foundMeta {
		eligible = true
		reason = "meta.properties not found (uninitialized member)"
	}

	rep := types.CandidateReport{
		ID:                  w.cfg.WorkerID,
		Kind:                types.CandidateKafka,
		Host:                w.cfg.Host,
		KafkaClusterID:      clusterID,
		KafkaNodeID:         w.cfg.NodeID,
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

func readMetaProperties(path string) (clusterID, nodeID string, ok bool) {
	f, err := os.Open(path)
	if err != nil {
		return "", "", false
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
	if clusterID == "" && nodeID == "" {
		return "", "", false
	}
	return clusterID, nodeID, true
}

func (w *Worker) cleanupDataDirs() {
	paths := []string{w.cfg.LogDir, w.cfg.MetaLogDir}
	for _, d := range paths {
		if d == "" {
			continue
		}
		log.Printf("[kafka-worker] cleanup dir %s", d)
		if err := os.RemoveAll(d); err != nil {
			log.Printf("[kafka-worker] failed to remove dir %s: %v", d, err)
			continue
		}
		if err := os.MkdirAll(d, 0o755); err != nil {
			log.Printf("[kafka-worker] failed to recreate dir %s: %v", d, err)
		}
	}
}
