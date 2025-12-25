package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	capi "github.com/hashicorp/consul/api"
	kafkaagent "github.com/umitbozkurt/consul-replctl/internal/agents/kafka"
	mongoagent "github.com/umitbozkurt/consul-replctl/internal/agents/mongo"
	serviceagent "github.com/umitbozkurt/consul-replctl/internal/agents/service"
	"github.com/umitbozkurt/consul-replctl/internal/config"
	"github.com/umitbozkurt/consul-replctl/internal/controllers/kafka"
	mctl "github.com/umitbozkurt/consul-replctl/internal/controllers/mongo"
	sctl "github.com/umitbozkurt/consul-replctl/internal/controllers/services"
	"github.com/umitbozkurt/consul-replctl/internal/providers/consulorders"
	"github.com/umitbozkurt/consul-replctl/internal/runtime"
	"github.com/umitbozkurt/consul-replctl/internal/servicereg"
	"github.com/umitbozkurt/consul-replctl/internal/store/consul"
	kw "github.com/umitbozkurt/consul-replctl/internal/workers/kafka"
	mw "github.com/umitbozkurt/consul-replctl/internal/workers/mongo"
)

const logPrefix = "[main]"

func main() {
	var cfgPath string
	flag.StringVar(&cfgPath, "config", "config.yaml", "path to config yaml")
	flag.Parse()

	cfg, err := config.Load(cfgPath)
	if err != nil {
		log.Fatalf("config load error: %v", err)
	}

	ctx, cancel := runtime.WithSignals(context.Background())
	defer cancel()

	st, err := consul.New(cfg.Consul.Address, cfg.Consul.Datacenter, cfg.Consul.Token, cfg.Consul.Prefix)
	if err != nil {
		log.Fatalf("consul init error: %v", err)
	}

	rawCfg := capi.DefaultConfig()
	rawCfg.Address = cfg.Consul.Address
	if cfg.Consul.Datacenter != "" {
		rawCfg.Datacenter = cfg.Consul.Datacenter
	}
	if cfg.Consul.Token != "" {
		rawCfg.Token = cfg.Consul.Token
	}
	rawCli, _ := capi.NewClient(rawCfg)
	log.Printf("%s waiting for consul to be ready...", logPrefix)
	if err := runtime.WaitConsulReady(ctx, rawCli, 2*time.Minute); err != nil {
		log.Fatalf("%s consul not ready: %v", logPrefix, err)
	}
	log.Printf("%s consul is ready", logPrefix)
	locker := st.Locker()

	reg := servicereg.NewConsulRegistry(rawCli)

	orderProv := consulorders.Provider{
		KV:               st,
		OrderHistoryKeep: cfg.Consul.OrderHistoryKeep,

		MongoOrdersPrefix:     "orders/mongo",
		MongoAckPrefix:        "acks/mongo",
		MongoCandidatesPrefix: "candidates/mongo",
		MongoLastAppliedKey:   "provider/mongo/last_applied_spec",

		KafkaOrdersPrefix:     "orders/kafka",
		KafkaAckPrefix:        "acks/kafka",
		KafkaCandidatesPrefix: "candidates/kafka",
		KafkaLastAppliedKey:   "provider/kafka/last_applied_spec",
	}

	// derive host info
	nodeName := cfg.NodeName
	if nodeName == "" {
		nodeName, _ = os.Hostname()
	}

	// derive controller IDs (unique per instance)
	mongoControllerID := cfg.Tasks.MongoController.ControllerID
	if mongoControllerID == "" && cfg.Tasks.MongoController.Enabled {
		mongoControllerID = fmt.Sprintf("%s#%d", nodeName, cfg.Tasks.MongoController.InstanceNumber)
	}
	kafkaControllerID := cfg.Tasks.KafkaController.ControllerID
	if kafkaControllerID == "" && cfg.Tasks.KafkaController.Enabled {
		kafkaControllerID = fmt.Sprintf("%s#%d", nodeName, cfg.Tasks.KafkaController.InstanceNumber)
	}

	// Controllers are long-running but leader-elected.
	if cfg.Tasks.MongoController.Enabled {
		ctl := mctl.New(mctl.Config{
			ControllerID:              mongoControllerID,
			LockKey:                   cfg.Tasks.MongoController.LockKey,
			StateKey:                  cfg.Tasks.MongoController.StateKey,
			CandidatesPrefix:          cfg.Tasks.MongoController.CandidatesPrefix,
			HealthPrefix:              cfg.Tasks.MongoController.HealthPrefix,
			SpecKey:                   cfg.Tasks.MongoController.SpecKey,
			ReplicaSetID:              cfg.Tasks.MongoController.ReplicaSetID,
			ElectionInterval:          cfg.Tasks.MongoController.ElectionInterval,
			InitialSettleDuration:     cfg.Tasks.MongoController.InitialSettleDuration,
			AllowDegradedSingleMember: cfg.Tasks.MongoController.AllowDegradedSingleMember,
		}, st, locker, consulorders.MongoAdapter{P: orderProv})
		go func() {
			log.Printf("%s mongo controller started", logPrefix)
			if err := ctl.Run(ctx); err != nil {
				log.Printf("%s mongo controller stopped: %v", logPrefix, err)
			}
		}()
	}

	// Agents execute orders locally and register services with status/role.
	if cfg.Tasks.MongoAgent.Enabled {
		if cfg.Tasks.MongoAgent.AgentID == "" {
			cfg.Tasks.MongoAgent.AgentID = nodeName
		}
		if cfg.Tasks.MongoAgent.SpecKey == "" {
			cfg.Tasks.MongoAgent.SpecKey = cfg.Tasks.MongoController.SpecKey
		}
		if cfg.Tasks.MongoAgent.SpecKey == "" {
			cfg.Tasks.MongoAgent.SpecKey = "spec/mongo"
		}
		if cfg.Tasks.MongoAgent.WorkerID == "" {
			cfg.Tasks.MongoAgent.WorkerID = cfg.Tasks.MongoAgent.AgentID
		}
		if cfg.Tasks.MongoAgent.HealthKey == "" {
			cfg.Tasks.MongoAgent.HealthKey = fmt.Sprintf("health/mongo/%s", cfg.Tasks.MongoAgent.AgentID)
		}
		if cfg.Tasks.MongoAgent.BindAddr == "" {
			cfg.Tasks.MongoAgent.BindAddr = cfg.Tasks.MongoAgent.BindIP
		}
		if cfg.Tasks.MongoAgent.TempPort == 0 {
			cfg.Tasks.MongoAgent.TempPort = 27028
		}
		log.Printf("%s starting mongo_agent...", logPrefix)
		svc := servicereg.Registration{}
		if cfg.Tasks.MongoAgent.Service.Enabled {
			ttl := cfg.Tasks.MongoAgent.Service.TTL
			if ttl == "" {
				ttl = "15s"
			}
			addr := cfg.Tasks.MongoAgent.Service.Address
			if addr == "" {
				addr = nodeName
			}
			svc = servicereg.Registration{
				Name:    "mongo",
				Address: addr,
				Port:    cfg.Tasks.MongoAgent.Port,
				Tags:    cfg.Tasks.MongoAgent.Service.Tags,
				TTL:     ttl,
			}
		}
		if cfg.Tasks.MongoAgent.ReportKey != "" {
			w := mw.New(mw.Config{
				WorkerID:   cfg.Tasks.MongoAgent.WorkerID,
				MemberID:   cfg.Tasks.MongoAgent.MemberID,
				ReportKey:  cfg.Tasks.MongoAgent.ReportKey,
				MongodPath: cfg.Tasks.MongoAgent.MongodPath,
				DBPath:     cfg.Tasks.MongoAgent.DBPath,
				BindAddr:   cfg.Tasks.MongoAgent.BindAddr,
				TempPort:   cfg.Tasks.MongoAgent.TempPort,
				AdminUser:  cfg.Tasks.MongoAgent.AdminUser,
				AdminPass:  cfg.Tasks.MongoAgent.AdminPass,
				Host:       nodeName,
				Addr:       cfg.Tasks.MongoAgent.BindAddr,
			}, st)
			runOnce(ctx, "mongo-worker", func() error { return w.RunOnce(ctx) })
		}
		ag := mongoagent.New(mongoagent.Config{
			AgentID:     cfg.Tasks.MongoAgent.AgentID,
			OrdersKey:   cfg.Tasks.MongoAgent.OrdersKey,
			AckKey:      cfg.Tasks.MongoAgent.AckKey,
			MongodPath:  cfg.Tasks.MongoAgent.MongodPath,
			DBPath:      cfg.Tasks.MongoAgent.DBPath,
			BindIP:      cfg.Tasks.MongoAgent.BindIP,
			Port:        cfg.Tasks.MongoAgent.Port,
			ReplSetName: cfg.Tasks.MongoAgent.ReplSetName,
			LogPath:     cfg.Tasks.MongoAgent.LogPath,
			AdminUser:   cfg.Tasks.MongoAgent.AdminUser,
			AdminPass:   cfg.Tasks.MongoAgent.AdminPass,
			HealthKey:   cfg.Tasks.MongoAgent.HealthKey,
			SpecKey:     cfg.Tasks.MongoAgent.SpecKey,
			Service:     svc,
		}, st, reg)
		go func() {
			log.Printf("%s mongo_agent started", logPrefix)
			if err := ag.Run(ctx); err != nil {
				log.Printf("%s mongo_agent stopped: %v", logPrefix, err)
			}
		}()
	}
	if cfg.Tasks.KafkaAgent.Enabled {
		if cfg.Tasks.KafkaAgent.AgentID == "" {
			cfg.Tasks.KafkaAgent.AgentID = nodeName
		}
		if cfg.Tasks.KafkaAgent.WorkerID == "" {
			cfg.Tasks.KafkaAgent.WorkerID = cfg.Tasks.KafkaAgent.AgentID
		}
		if cfg.Tasks.KafkaAgent.ReportKey != "" {
			w := kw.New(kw.Config{
				WorkerID:       cfg.Tasks.KafkaAgent.WorkerID,
				ReportKey:      cfg.Tasks.KafkaAgent.ReportKey,
				MetaDirs:       cfg.Tasks.KafkaAgent.MetaDirs,
				Host:           nodeName,
				BrokerAddr:     cfg.Tasks.KafkaAgent.BrokerAddr,
				ControllerAddr: cfg.Tasks.KafkaAgent.ControllerAddr,
			}, st)
			runOnce(ctx, "kafka-worker", func() error { return w.RunOnce(ctx) })
		}
		svc := servicereg.Registration{}
		if cfg.Tasks.KafkaAgent.Service.Enabled {
			ttl := cfg.Tasks.KafkaAgent.Service.TTL
			if ttl == "" {
				ttl = "15s"
			}
			addr := cfg.Tasks.KafkaAgent.Service.Address
			if addr == "" {
				addr = nodeName
			}
			// parse port from broker addr
			port := 0
			if i := strings.LastIndex(cfg.Tasks.KafkaAgent.BrokerAddr, ":"); i >= 0 {
				p, _ := strconv.Atoi(cfg.Tasks.KafkaAgent.BrokerAddr[i+1:])
				port = p
			}
			svc = servicereg.Registration{
				Name:    "kafka",
				ID:      "kafka-" + cfg.Tasks.KafkaAgent.AgentID,
				Address: addr,
				Port:    port,
				Tags:    cfg.Tasks.KafkaAgent.Service.Tags,
				CheckID: "check:kafka-" + cfg.Tasks.KafkaAgent.AgentID,
				TTL:     ttl,
			}
		}
		ag := kafkaagent.New(kafkaagent.Config{
			AgentID:        cfg.Tasks.KafkaAgent.AgentID,
			OrdersKey:      cfg.Tasks.KafkaAgent.OrdersKey,
			AckKey:         cfg.Tasks.KafkaAgent.AckKey,
			KafkaBinDir:    cfg.Tasks.KafkaAgent.KafkaBinDir,
			WorkDir:        cfg.Tasks.KafkaAgent.WorkDir,
			LogDir:         cfg.Tasks.KafkaAgent.LogDir,
			MetaLogDir:     cfg.Tasks.KafkaAgent.MetaLogDir,
			BrokerAddr:     cfg.Tasks.KafkaAgent.BrokerAddr,
			ControllerAddr: cfg.Tasks.KafkaAgent.ControllerAddr,
			ClusterID:      cfg.Tasks.KafkaAgent.ClusterID,
			NodeID:         cfg.Tasks.KafkaAgent.NodeID,
			Service:        svc,
		}, st, reg)
		go func() {
			log.Printf("%s kafka_agent started", logPrefix)
			if err := ag.Run(ctx); err != nil {
				log.Printf("%s kafka_agent stopped: %v", logPrefix, err)
			}
		}()
	}

	if cfg.Tasks.KafkaController.Enabled {
		ctl := kafka.New(kafka.Config{
			ControllerID:              kafkaControllerID,
			LockKey:                   cfg.Tasks.KafkaController.LockKey,
			StateKey:                  cfg.Tasks.KafkaController.StateKey,
			CandidatesPrefix:          cfg.Tasks.KafkaController.CandidatesPrefix,
			HealthPrefix:              cfg.Tasks.KafkaController.HealthPrefix,
			SpecKey:                   cfg.Tasks.KafkaController.SpecKey,
			ElectionInterval:          cfg.Tasks.KafkaController.ElectionInterval,
			InitialSettleDuration:     cfg.Tasks.KafkaController.InitialSettleDuration,
			AllowDegradedSingleMember: cfg.Tasks.KafkaController.AllowDegradedSingleMember,
		}, st, locker, consulorders.KafkaAdapter{P: orderProv})
		go func() {
			log.Printf("%s kafka controller started", logPrefix)
			if err := ctl.Run(ctx); err != nil {
				log.Printf("%s kafka controller stopped: %v", logPrefix, err)
			}
		}()
	}

	// Generic services agent (runs user-defined services based on Consul orders)
	if cfg.Tasks.ServicesAgent.Enabled {
		svcAddr := cfg.Tasks.ServicesAgent.ServiceAddress
		if svcAddr == "" {
			svcAddr = nodeName
		}
		ag := serviceagent.New(serviceagent.Config{
			AgentID:        cfg.Tasks.ServicesAgent.AgentID,
			OrdersPrefix:   cfg.Tasks.ServicesAgent.OrdersPrefix,
			AckPrefix:      cfg.Tasks.ServicesAgent.AckPrefix,
			ServiceAddress: svcAddr,
		}, st, reg)
		go func() {
			log.Printf("%s services_agent started", logPrefix)
			if err := ag.Run(ctx); err != nil {
				log.Printf("%s services_agent stopped: %v", logPrefix, err)
			}
		}()
	}

	// Services controller: spreads services across nodes and ensures 2 instances (master/slave).
	if cfg.Tasks.ServicesController.Enabled {
		id := cfg.Tasks.ServicesController.ControllerID
		if id == "" {
			id = fmt.Sprintf("%s#%d", nodeName, cfg.Tasks.ServicesController.InstanceNumber)
		}
		// Map config services
		svcDefs := make([]sctl.ServiceDef, 0, len(cfg.Tasks.ServicesController.Services))
		for _, s := range cfg.Tasks.ServicesController.Services {
			svcDefs = append(svcDefs, sctl.ServiceDef{
				Name:      s.Name,
				Instances: s.Instances,
				Tags:      s.Tags,
				TTL:       s.TTL,
				StartCmd:  s.Start.Cmd,
				StartArgs: s.Start.Args,
				WorkDir:   s.Start.WorkDir,
			})
		}
		ctl := sctl.New(sctl.Config{
			ControllerID:          id,
			LockKey:               cfg.Tasks.ServicesController.LockKey,
			StateKey:              cfg.Tasks.ServicesController.StateKey,
			KafkaCandidatesPrefix: cfg.Tasks.ServicesController.KafkaCandidatesPrefix,
			MongoCandidatesPrefix: cfg.Tasks.ServicesController.MongoCandidatesPrefix,
			WaitFor:               cfg.Tasks.ServicesController.WaitFor,
			MinPassing:            cfg.Tasks.ServicesController.MinPassing,
			Services:              svcDefs,
			ReconcileInterval:     cfg.Tasks.ServicesController.ReconcileInterval,
			OrderHistoryKeep:      cfg.Consul.OrderHistoryKeep,
		}, st, locker, rawCli)
		go func() {
			log.Printf("%s services_controller started", logPrefix)
			if err := ctl.Run(ctx); err != nil {
				log.Printf("%s services_controller stopped: %v", logPrefix, err)
			}
		}()
	}

	<-ctx.Done()
	time.Sleep(200 * time.Millisecond)
}

func runOnce(ctx context.Context, name string, fn func() error) {
	if err := fn(); err != nil {
		log.Printf("%s %s runOnce error: %v", logPrefix, name, err)
	} else {
		log.Printf("%s %s runOnce ok", logPrefix, name)
	}
}
