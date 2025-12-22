package main

import (
    "context"
    "flag"
    "fmt"
    "log"
    "os"
    "time"
    "strconv"
    "strings"

    "github.com/umitbozkurt/consul-replctl/internal/config"
    "github.com/umitbozkurt/consul-replctl/internal/controllers/kafka"
    sctl "github.com/umitbozkurt/consul-replctl/internal/controllers/services"
    mctl "github.com/umitbozkurt/consul-replctl/internal/controllers/mongo"
    "github.com/umitbozkurt/consul-replctl/internal/runtime"
    capi "github.com/hashicorp/consul/api"
    "github.com/umitbozkurt/consul-replctl/internal/store/consul"
    "github.com/umitbozkurt/consul-replctl/internal/providers/consulorders"
    mongoagent "github.com/umitbozkurt/consul-replctl/internal/agents/mongo"
    kafkaagent "github.com/umitbozkurt/consul-replctl/internal/agents/kafka"
    serviceagent "github.com/umitbozkurt/consul-replctl/internal/agents/service"
    "github.com/umitbozkurt/consul-replctl/internal/servicereg"
    kw "github.com/umitbozkurt/consul-replctl/internal/workers/kafka"
    mw "github.com/umitbozkurt/consul-replctl/internal/workers/mongo"
)

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
    if cfg.Consul.Datacenter != "" { rawCfg.Datacenter = cfg.Consul.Datacenter }
    if cfg.Consul.Token != "" { rawCfg.Token = cfg.Consul.Token }
    rawCli, _ := capi.NewClient(rawCfg)
    log.Printf("waiting for consul to be ready...")
    if err := runtime.WaitConsulReady(ctx, rawCli, 2*time.Minute); err != nil {
        log.Fatalf("consul not ready: %v", err)
    }
    log.Printf("consul is ready")
    locker := st.Locker()

    reg := servicereg.NewConsulRegistry(rawCli)

    orderProv := consulorders.Provider{
        KV: st,
        MongoOrdersPrefix: "orders/mongo",
        MongoAckPrefix: "acks/mongo",
        MongoLastAppliedKey: "provider/mongo/last_applied_spec",
        KafkaOrdersPrefix: "orders/kafka",
        KafkaAckPrefix: "acks/kafka",
        KafkaCandidatesPrefix: "candidates/kafka",
        KafkaLastAppliedKey: "provider/kafka/last_applied_spec",
    }

    // derive host info
    hostname, _ := os.Hostname()


// derive controller IDs (unique per instance)
mongoControllerID := cfg.Tasks.MongoController.ControllerID
if mongoControllerID == "" && cfg.Tasks.MongoController.Enabled {
    mongoControllerID = fmt.Sprintf("%s#%d", hostname, cfg.Tasks.MongoController.InstanceNumber)
}
kafkaControllerID := cfg.Tasks.KafkaController.ControllerID
if kafkaControllerID == "" && cfg.Tasks.KafkaController.Enabled {
    kafkaControllerID = fmt.Sprintf("%s#%d", hostname, cfg.Tasks.KafkaController.InstanceNumber)
}

    // Workers run ONCE at startup, as required.
    if cfg.Tasks.MongoWorker.Enabled {
        w := mw.New(mw.Config{
            WorkerID: cfg.Tasks.MongoWorker.WorkerID,
            ReportKey: cfg.Tasks.MongoWorker.ReportKey,
            MongodPath: cfg.Tasks.MongoWorker.MongodPath,
            DBPath: cfg.Tasks.MongoWorker.DBPath,
            TempPort: cfg.Tasks.MongoWorker.TempPort,
            AdminUser: cfg.Tasks.MongoWorker.AdminUser,
            AdminPass: cfg.Tasks.MongoWorker.AdminPass,
            Host: hostname,
        }, st)
        runOnce(ctx, "mongo-worker", func() error { return w.RunOnce(ctx) })
    }
    if cfg.Tasks.KafkaWorker.Enabled {
        w := kw.New(kw.Config{
            WorkerID: cfg.Tasks.KafkaWorker.WorkerID,
            ReportKey: cfg.Tasks.KafkaWorker.ReportKey,
            MetaDirs: cfg.Tasks.KafkaWorker.MetaDirs,
            Host: hostname,
            BrokerAddr: cfg.Tasks.KafkaWorker.BrokerAddr,
            ControllerAddr: cfg.Tasks.KafkaWorker.ControllerAddr,
        }, st)
        runOnce(ctx, "kafka-worker", func() error { return w.RunOnce(ctx) })
    }

    // Controllers are long-running but leader-elected.
    if cfg.Tasks.MongoController.Enabled {
        ctl := mctl.New(mctl.Config{
            ControllerID: mongoControllerID,
            LockKey: cfg.Tasks.MongoController.LockKey,
            StateKey: cfg.Tasks.MongoController.StateKey,
            CandidatesPrefix: cfg.Tasks.MongoController.CandidatesPrefix,
            HealthPrefix: cfg.Tasks.MongoController.HealthPrefix,
            SpecKey: cfg.Tasks.MongoController.SpecKey,
            ReplicaSetID: cfg.Tasks.MongoController.ReplicaSetID,
            ElectionInterval: cfg.Tasks.MongoController.ElectionInterval,
            InitialSettleDuration: cfg.Tasks.MongoController.InitialSettleDuration,
            AllowDegradedSingleMember: cfg.Tasks.MongoController.AllowDegradedSingleMember,
        }, st, locker, consulorders.MongoAdapter{P: orderProv})
        go func() {
            log.Printf("mongo controller started")
            if err := ctl.Run(ctx); err != nil {
                log.Printf("mongo controller stopped: %v", err)
            }
        }()
    }

// Agents execute orders locally and register services with status/role.
if cfg.Tasks.MongoAgent.Enabled {
    svc := servicereg.Registration{}
    if cfg.Tasks.MongoAgent.Service.Enabled {
        ttl := cfg.Tasks.MongoAgent.Service.TTL
        if ttl == "" { ttl = "15s" }
        addr := cfg.Tasks.MongoAgent.Service.Address
        if addr == "" { addr, _ = os.Hostname() }
        svc = servicereg.Registration{
            Name: "mongo",
            ID: "mongo-"+cfg.Tasks.MongoAgent.AgentID,
            Address: addr,
            Port: cfg.Tasks.MongoAgent.Port,
            Tags: cfg.Tasks.MongoAgent.Service.Tags,
            CheckID: "check:mongo-"+cfg.Tasks.MongoAgent.AgentID,
            TTL: ttl,
        }
    }
    ag := mongoagent.New(mongoagent.Config{
        AgentID: cfg.Tasks.MongoAgent.AgentID,
        OrdersKey: cfg.Tasks.MongoAgent.OrdersKey,
        AckKey: cfg.Tasks.MongoAgent.AckKey,
        MongodPath: cfg.Tasks.MongoAgent.MongodPath,
        MongoshPath: cfg.Tasks.MongoAgent.MongoshPath,
        DBPath: cfg.Tasks.MongoAgent.DBPath,
        BindIP: cfg.Tasks.MongoAgent.BindIP,
        Port: cfg.Tasks.MongoAgent.Port,
        ReplSetName: cfg.Tasks.MongoAgent.ReplSetName,
        LogPath: cfg.Tasks.MongoAgent.LogPath,
        Service: svc,
    }, st, reg)
    go func() {
        log.Printf("mongo_agent started")
        if err := ag.Run(ctx); err != nil {
            log.Printf("mongo_agent stopped: %v", err)
        }
    }()
}
if cfg.Tasks.KafkaAgent.Enabled {
    svc := servicereg.Registration{}
    if cfg.Tasks.KafkaAgent.Service.Enabled {
        ttl := cfg.Tasks.KafkaAgent.Service.TTL
        if ttl == "" { ttl = "15s" }
        addr := cfg.Tasks.KafkaAgent.Service.Address
        if addr == "" { addr, _ = os.Hostname() }
        // parse port from broker addr
        port := 0
        if i := strings.LastIndex(cfg.Tasks.KafkaAgent.BrokerAddr, ":"); i >= 0 {
            p, _ := strconv.Atoi(cfg.Tasks.KafkaAgent.BrokerAddr[i+1:])
            port = p
        }
        svc = servicereg.Registration{
            Name: "kafka",
            ID: "kafka-"+cfg.Tasks.KafkaAgent.AgentID,
            Address: addr,
            Port: port,
            Tags: cfg.Tasks.KafkaAgent.Service.Tags,
            CheckID: "check:kafka-"+cfg.Tasks.KafkaAgent.AgentID,
            TTL: ttl,
        }
    }
    ag := kafkaagent.New(kafkaagent.Config{
        AgentID: cfg.Tasks.KafkaAgent.AgentID,
        OrdersKey: cfg.Tasks.KafkaAgent.OrdersKey,
        AckKey: cfg.Tasks.KafkaAgent.AckKey,
        KafkaBinDir: cfg.Tasks.KafkaAgent.KafkaBinDir,
        WorkDir: cfg.Tasks.KafkaAgent.WorkDir,
        LogDir: cfg.Tasks.KafkaAgent.LogDir,
        MetaLogDir: cfg.Tasks.KafkaAgent.MetaLogDir,
        BrokerAddr: cfg.Tasks.KafkaAgent.BrokerAddr,
        ControllerAddr: cfg.Tasks.KafkaAgent.ControllerAddr,
        ClusterID: cfg.Tasks.KafkaAgent.ClusterID,
        NodeID: cfg.Tasks.KafkaAgent.NodeID,
        Service: svc,
    }, st, reg)
    go func() {
        log.Printf("kafka_agent started")
        if err := ag.Run(ctx); err != nil {
            log.Printf("kafka_agent stopped: %v", err)
        }
    }()
}

    if cfg.Tasks.KafkaController.Enabled {
        ctl := kafka.New(kafka.Config{
            ControllerID: kafkaControllerID,
            LockKey: cfg.Tasks.KafkaController.LockKey,
            StateKey: cfg.Tasks.KafkaController.StateKey,
            CandidatesPrefix: cfg.Tasks.KafkaController.CandidatesPrefix,
            HealthPrefix: cfg.Tasks.KafkaController.HealthPrefix,
            SpecKey: cfg.Tasks.KafkaController.SpecKey,
            ElectionInterval: cfg.Tasks.KafkaController.ElectionInterval,
            InitialSettleDuration: cfg.Tasks.KafkaController.InitialSettleDuration,
            AllowDegradedSingleMember: cfg.Tasks.KafkaController.AllowDegradedSingleMember,
        }, st, locker, consulorders.KafkaAdapter{P: orderProv})
        go func() {
            log.Printf("kafka controller started")
            if err := ctl.Run(ctx); err != nil {
                log.Printf("kafka controller stopped: %v", err)
            }
        }()
    }


// Generic services agent (runs user-defined services based on Consul orders)
if cfg.Tasks.ServicesAgent.Enabled {
    ag := serviceagent.New(serviceagent.Config{
        AgentID: cfg.Tasks.ServicesAgent.AgentID,
        OrdersPrefix: cfg.Tasks.ServicesAgent.OrdersPrefix,
        AckPrefix: cfg.Tasks.ServicesAgent.AckPrefix,
        ServiceAddress: cfg.Tasks.ServicesAgent.ServiceAddress,
    }, st, reg)
    go func() {
        log.Printf("services_agent started")
        if err := ag.Run(ctx); err != nil {
            log.Printf("services_agent stopped: %v", err)
        }
    }()
}

// Services controller: spreads services across nodes and ensures 2 instances (master/slave).
if cfg.Tasks.ServicesController.Enabled {
    id := cfg.Tasks.ServicesController.ControllerID
    if id == "" {
        id = fmt.Sprintf("%s#%d", hostname, cfg.Tasks.ServicesController.InstanceNumber)
    }
    // Map config services
    svcDefs := make([]sctl.ServiceDef, 0, len(cfg.Tasks.ServicesController.Services))
    for _, s := range cfg.Tasks.ServicesController.Services {
        svcDefs = append(svcDefs, sctl.ServiceDef{
            Name: s.Name,
            Instances: s.Instances,
            Tags: s.Tags,
            TTL: s.TTL,
            StartCmd: s.Start.Cmd,
            StartArgs: s.Start.Args,
            WorkDir: s.Start.WorkDir,
        })
    }
    ctl := sctl.New(sctl.Config{
        ControllerID: id,
        LockKey: cfg.Tasks.ServicesController.LockKey,
        StateKey: cfg.Tasks.ServicesController.StateKey,
        KafkaCandidatesPrefix: cfg.Tasks.ServicesController.KafkaCandidatesPrefix,
        MongoCandidatesPrefix: cfg.Tasks.ServicesController.MongoCandidatesPrefix,
        WaitFor: cfg.Tasks.ServicesController.WaitFor,
        MinPassing: cfg.Tasks.ServicesController.MinPassing,
        Services: svcDefs,
        ReconcileInterval: cfg.Tasks.ServicesController.ReconcileInterval,
    }, st, locker, rawCli)
    go func() {
        log.Printf("services_controller started")
        if err := ctl.Run(ctx); err != nil {
            log.Printf("services_controller stopped: %v", err)
        }
    }()
}

    <-ctx.Done()
    time.Sleep(200 * time.Millisecond)
}

func runOnce(ctx context.Context, name string, fn func() error) {
    if err := fn(); err != nil {
        log.Printf("%s runOnce error: %v", name, err)
    } else {
        log.Printf("%s runOnce ok", name)
    }
}
