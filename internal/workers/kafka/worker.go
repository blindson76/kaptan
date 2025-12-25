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

	clusterID := ""
	nodeID := ""
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
			c, n, ok := readMetaProperties(p)
			if ok {
				clusterID = c
				nodeID = n
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
		KafkaNodeID:         nodeID,
		KafkaBrokerAddr:     w.cfg.BrokerAddr,
		KafkaControllerAddr: w.cfg.ControllerAddr,
		Eligible:            eligible,
		Reason:              reason,
		UpdatedAt:           time.Now(),
	}
	return w.kv.PutJSONEphemeral(ctx, w.cfg.ReportKey, w.cfg.WorkerID, &rep)
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
