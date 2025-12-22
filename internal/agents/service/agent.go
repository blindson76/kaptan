package service

import (
    "context"
    "encoding/json"
    "fmt"
    "log"
    "os"
    "os/exec"
    "path/filepath"
    "strings"
    "time"

    "github.com/umitbozkurt/consul-replctl/internal/orders"
    "github.com/umitbozkurt/consul-replctl/internal/servicereg"
    "github.com/umitbozkurt/consul-replctl/internal/store"
)

type Config struct {
    AgentID string
    OrdersPrefix string
    AckPrefix string
    ServiceAddress string
}

type Agent struct {
    cfg Config
    kv store.KV
    reg servicereg.Registry
    // running processes by service name
    procs map[string]*exec.Cmd
}

func New(cfg Config, kv store.KV, reg servicereg.Registry) *Agent {
    return &Agent{cfg: cfg, kv: kv, reg: reg, procs: map[string]*exec.Cmd{}}
}

func (a *Agent) Run(ctx context.Context) error {
    // Watch all orders under prefix, filter by TargetID
    ch := a.kv.WatchPrefixJSON(ctx, a.cfg.OrdersPrefix, func() any { return &[]orders.Order{} })
    for {
        select {
        case <-ctx.Done():
            return ctx.Err()
        case v, ok := <-ch:
            if !ok { return nil }
            lst := v.([]orders.Order)
            if len(lst)==0 { continue }
            // process newest first
            for i := len(lst)-1; i>=0; i-- {
                ord := lst[i]
                if ord.TargetID != a.cfg.AgentID || ord.Kind != orders.KindService {
                    continue
                }
                a.execute(ctx, ord)
                break
            }
        }
    }
}

func (a *Agent) execute(ctx context.Context, ord orders.Order) {
    ack := orders.Ack{TargetID: a.cfg.AgentID, Action: ord.Action, Epoch: ord.Epoch, FinishedAt: time.Now()}
    err := error(nil)

    svcName, _ := ord.Payload["service"].(string)
    role, _ := ord.Payload["role"].(string)
    cmdStr, _ := ord.Payload["cmd"].(string)
    workDir, _ := ord.Payload["workDir"].(string)

    // args is []any
    argsAny, _ := ord.Payload["args"].([]any)
    args := make([]string, 0, len(argsAny))
    for _, x := range argsAny {
        if s, ok := x.(string); ok {
            args = append(args, s)
        }
    }
    // Expand simple placeholders ${ROLE}
    for i := range args {
        args[i] = strings.ReplaceAll(args[i], "${ROLE}", role)
    }

    switch ord.Action {
    case orders.ActionStart:
        err = a.startService(ctx, svcName, role, cmdStr, args, workDir, ord.Payload)
    case orders.ActionStop:
        err = a.stopService(svcName)
    default:
        err = nil
    }

    if err != nil {
        ack.Ok = false
        ack.Message = err.Error()
    } else {
        ack.Ok = true
    }
    _ = a.kv.PutJSON(ctx, fmt.Sprintf("%s/%s/%s", a.cfg.AckPrefix, svcName, a.cfg.AgentID), &ack)
}

func (a *Agent) startService(ctx context.Context, name, role, cmdStr string, args []string, workDir string, payload map[string]any) error {
    if name == "" || cmdStr == "" {
        return fmt.Errorf("service start missing name/cmd")
    }
    // stop existing
    _ = a.stopService(name)

    cmd := exec.CommandContext(ctx, cmdStr, args...)
    if workDir != "" {
        cmd.Dir = workDir
    }
    cmd.Stdout = os.Stdout
    cmd.Stderr = os.Stderr
    if err := cmd.Start(); err != nil {
        return err
    }
    a.procs[name] = cmd
    log.Printf("[service-agent] started service=%s role=%s pid=%d", name, role, cmd.Process.Pid)

    // Register service with TTL note that includes role + pid
    if a.reg != nil {
        ttl := "15s"
        if v, ok := payload["ttl"].(string); ok && v != "" { ttl = v }
        addr := a.cfg.ServiceAddress
        if addr == "" {
            h, _ := os.Hostname()
            addr = h
        }
        svcID := fmt.Sprintf("%s-%s-%s", name, role, a.cfg.AgentID)
        checkID := fmt.Sprintf("check:%s", svcID)
        tags := []string{}
        if tAny, ok := payload["tags"].([]any); ok {
            for _, x := range tAny {
                if s, ok := x.(string); ok {
                    tags = append(tags, s)
                }
            }
        }
        _ = a.reg.Register(ctx, servicereg.Registration{
            Name: name,
            ID: svcID,
            Address: addr,
            Port: 0,
            Tags: tags,
            CheckID: checkID,
            TTL: ttl,
        })
        note := map[string]any{"service": map[string]any{"name": name, "role": role, "pid": cmd.Process.Pid}}
        b, _ := json.Marshal(note)
        _ = a.reg.SetTTL(ctx, checkID, servicereg.StatusPassing, string(b))
        // Heartbeat loop
        go func() {
            t := time.NewTicker(5*time.Second)
            defer t.Stop()
            for {
                select {
                case <-ctx.Done():
                    return
                case <-t.C:
                    if a.reg == nil { return }
                    // if process ended, mark critical
                    if cmd.ProcessState != nil && cmd.ProcessState.Exited() {
                        _ = a.reg.SetTTL(ctx, checkID, servicereg.StatusCritical, "{"service":{"state":"exited"}}")
                        return
                    }
                    _ = a.reg.SetTTL(ctx, checkID, servicereg.StatusPassing, string(b))
                }
            }
        }()
    }

    return nil
}

func (a *Agent) stopService(name string) error {
    cmd := a.procs[name]
    if cmd == nil {
        return nil
    }
    if cmd.Process != nil {
        _ = cmd.Process.Kill()
    }
    delete(a.procs, name)
    return nil
}

