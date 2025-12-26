package mongo

import (
	"context"
	"log"
	"time"

	"github.com/umitbozkurt/consul-replctl/internal/store"
	"github.com/umitbozkurt/consul-replctl/internal/types"
)

type Config struct {
	WorkerID  string
	ReportKey string
	MemberID  *int

	MongodPath string
	DBPath     string
	BindAddr   string
	TempPort   int
	AdminUser  string
	AdminPass  string
	Host       string
	Addr       string
}

type Worker struct {
	cfg Config
	kv  store.KV
}

func New(cfg Config, kv store.KV) *Worker {
	return &Worker{cfg: cfg, kv: kv}
}

func (w *Worker) RunOnce(ctx context.Context) error {
	log.Printf("[mongo-worker] offline status probe starting")
	rsid, rsuuid, term, ts, err := Probe(ctx, OfflineProbeConfig{
		MongodPath: w.cfg.MongodPath,
		DBPath:     w.cfg.DBPath,
		Bind:       w.cfg.BindAddr,
		Port:       w.cfg.TempPort,
		AdminUser:  w.cfg.AdminUser,
		AdminPass:  w.cfg.AdminPass,
	})
	if err != nil {
		log.Printf("[mongo-worker] probe error: %v", err)
	}

	rep := types.CandidateReport{
		ID:   w.cfg.WorkerID,
		Kind: types.CandidateMongo,
		Host: w.cfg.Host,
		Addr: w.cfg.Addr,
		Meta: map[string]string{
			"dbpath": w.cfg.DBPath,
		},
		MongoMemberID:          w.cfg.MemberID,
		LastSeenReplicaSetID:   rsid,
		LastSeenReplicaSetUUID: rsuuid,
		LastTerm:               term,
		LastOplogTs:            ts,
		Eligible:               err == nil,
		Reason: func() string {
			if err == nil {
				return ""
			}
			return err.Error()
		}(),
		UpdatedAt: time.Now(),
	}
	return w.kv.PutJSONEphemeral(ctx, w.cfg.ReportKey, w.cfg.WorkerID, w.cfg.Host, &rep)
}
