package mongo

import (
    "encoding/json"
    "context"
    "fmt"
    "log"
    "os"
    "os/exec"
    "path/filepath"
    "time"
    "strings"

    "go.mongodb.org/mongo-driver/bson"
    "go.mongodb.org/mongo-driver/bson/primitive"
    "go.mongodb.org/mongo-driver/mongo"
    "go.mongodb.org/mongo-driver/mongo/options"

    "github.com/umitbozkurt/consul-replctl/internal/orders"
    "github.com/umitbozkurt/consul-replctl/internal/servicereg"
    "github.com/umitbozkurt/consul-replctl/internal/store"
)

type Config struct {
    AgentID string
    OrdersKey string
    AckKey string

    MongodPath string
    MongoshPath string
    DBPath string
    BindIP string
    Port int
    ReplSetName string
    LogPath string

    Service servicereg.Registration
}

type Agent struct {
    cfg Config
    kv store.KV
    reg servicereg.Registry
}

func New(cfg Config, kv store.KV, reg servicereg.Registry) *Agent {
    return &Agent{cfg: cfg, kv: kv, reg: reg}
}

func (a *Agent) Run(ctx context.Context) error {
    if a.reg != nil && a.cfg.Service.ID != "" {
        _ = a.reg.Register(ctx, a.cfg.Service)
        defer a.reg.Deregister(context.Background(), a.cfg.Service.ID)
        _ = a.reg.SetTTL(ctx, a.cfg.Service.CheckID, servicereg.StatusWarning, "startup")
        go a.roleHeartbeat(ctx)
    }

    ch := a.kv.WatchPrefixJSON(ctx, a.cfg.OrdersKey, func() any { return &[]orders.Order{} })
    for {
        select {
        case <-ctx.Done():
            return ctx.Err()
        case v, ok := <-ch:
            if !ok { return nil }
            lst := v.([]orders.Order)
            if len(lst)==0 { continue }
            ord := lst[len(lst)-1]
            if ord.TargetID != a.cfg.AgentID { continue }
            a.execute(ctx, ord)
        }
    }
}

func (a *Agent) execute(ctx context.Context, ord orders.Order) {
    ack := orders.Ack{TargetID: a.cfg.AgentID, Action: ord.Action, Epoch: ord.Epoch, FinishedAt: time.Now()}
    err := error(nil)

    switch ord.Action {
    case orders.ActionWipe:
        err = wipeDir(a.cfg.DBPath)
    case orders.ActionStart:
        repl := a.cfg.ReplSetName
        if v, ok := ord.Payload["replSetName"].(string); ok && v != "" { repl = v }
        err = a.startMongod(ctx, repl)
    case orders.ActionStop:
        err = a.shutdown(ctx)
    case orders.ActionInit:
        members, _ := ord.Payload["members"].([]any)
        repl, _ := ord.Payload["replSetName"].(string)
        err = a.rsInitiate(ctx, repl, members)
    case orders.ActionReconfigure:
        members, _ := ord.Payload["members"].([]any)
        repl, _ := ord.Payload["replSetName"].(string)
        err = a.rsReconfig(ctx, repl, members)
    }
    if err != nil {
        ack.Ok = false
        ack.Message = err.Error()
    } else {
        ack.Ok = true
    }
    _ = a.kv.PutJSON(ctx, a.cfg.AckKey, &ack)
}

func wipeDir(dir string) error {
    entries, err := os.ReadDir(dir)
    if err != nil { return err }
    for _, e := range entries {
        _ = os.RemoveAll(filepath.Join(dir, e.Name()))
    }
    return nil
}

func (a *Agent) startMongod(ctx context.Context, repl string) error {
    args := []string{
        "--dbpath", a.cfg.DBPath,
        "--bind_ip", nz(a.cfg.BindIP, "0.0.0.0"),
        "--port", fmt.Sprintf("%d", nzInt(a.cfg.Port, 27017)),
        "--replSet", repl,
        "--logpath", nz(a.cfg.LogPath, filepath.Join(a.cfg.DBPath, "mongod.log")),
        "--logappend",
    }
    cmd := exec.CommandContext(ctx, a.cfg.MongodPath, args...)
    cmd.Stdout = os.Stdout
    cmd.Stderr = os.Stderr
    if err := cmd.Start(); err != nil { return err }
    log.Printf("[mongo-agent] started mongod pid=%d", cmd.Process.Pid)
    return nil
}

func (a *Agent) mongoClient(ctx context.Context) (*mongo.Client, error) {
    uri := fmt.Sprintf("mongodb://127.0.0.1:%d/?directConnection=true", nzInt(a.cfg.Port, 27017))
    return mongo.Connect(ctx, options.Client().ApplyURI(uri))
}

func (a *Agent) shutdown(ctx context.Context) error {
    cli, err := a.mongoClient(ctx)
    if err != nil { return err }
    defer cli.Disconnect(context.Background())
    return cli.Database("admin").RunCommand(ctx, bson.D{{Key:"shutdown", Value:1}}).Err()
}

func (a *Agent) rsInitiate(ctx context.Context, repl string, members []any) error {
    cfg := buildReplCfg(nz(repl, a.cfg.ReplSetName), members)
    js := fmt.Sprintf("rs.initiate(%s)", cfg)
    return a.runMongosh(ctx, js)
}
func (a *Agent) rsReconfig(ctx context.Context, repl string, members []any) error {
    cfg := buildReplCfg(nz(repl, a.cfg.ReplSetName), members)
    js := fmt.Sprintf("rs.reconfig(%s,{force:true})", cfg)
    return a.runMongosh(ctx, js)
}
func buildReplCfg(repl string, members []any) string {
    arr := ""
    for i, m := range members {
        id, _ := m.(string)
        if i>0 { arr += "," }
        arr += fmt.Sprintf("{ _id:%d, host:%q }", i, id)
    }
    return fmt.Sprintf("{ _id:%q, members:[%s] }", repl, arr)
}
func (a *Agent) runMongosh(ctx context.Context, js string) error {
    uri := fmt.Sprintf("mongodb://127.0.0.1:%d/?directConnection=true", nzInt(a.cfg.Port, 27017))
    cmd := exec.CommandContext(ctx, a.cfg.MongoshPath, uri, "--quiet", "--eval", js)
    cmd.Stdout = os.Stdout
    cmd.Stderr = os.Stderr
    return cmd.Run()
}

func (a *Agent) roleHeartbeat(ctx context.Context) {
    ticker := time.NewTicker(5 * time.Second)
    defer ticker.Stop()
    for {
        select {
        case <-ctx.Done():
            return
        case <-ticker.C:
            if a.reg == nil || a.cfg.Service.CheckID == "" {
                continue
            }
            state, stateStr, rsid, optime, term, syncSource, ok, prog := a.probeReplStatus(ctx)
            note := a.buildServiceNote(state, stateStr, rsid, optime, term, syncSource, prog)

            // Map to Consul TTL status
            switch state {
            case 1, 2, 7: // PRIMARY, SECONDARY, ARBITER
                _ = a.reg.SetTTL(ctx, a.cfg.Service.CheckID, servicereg.StatusPassing, note)
            case 5, 3, 0: // STARTUP2, RECOVERING, STARTUP
                _ = a.reg.SetTTL(ctx, a.cfg.Service.CheckID, servicereg.StatusWarning, note)
            case 9, 8, 6, 10: // ROLLBACK, DOWN, UNKNOWN, REMOVED
                _ = a.reg.SetTTL(ctx, a.cfg.Service.CheckID, servicereg.StatusCritical, note)
            default:
                if !ok {
                    _ = a.reg.SetTTL(ctx, a.cfg.Service.CheckID, servicereg.StatusWarning, note)
                } else {
                    _ = a.reg.SetTTL(ctx, a.cfg.Service.CheckID, servicereg.StatusWarning, note)
                }
            }
        }
    }
}

func (a *Agent) detectRole(ctx context.Context) (string, bool) {
    cli, err := a.mongoClient(ctx)
    if err != nil { return "startup", false }
    defer cli.Disconnect(context.Background())
    var res bson.M
    if err := cli.Database("admin").RunCommand(ctx, bson.D{{Key:"hello", Value:1}}).Decode(&res); err != nil {
        return "startup", false
    }
    if v, ok := res["isWritablePrimary"].(bool); ok && v { return "primary", true }
    if v, ok := res["secondary"].(bool); ok && v { return "secondary", true }
    return "startup", true
}

func nz(s, d string) string { if s=="" { return d }; return s }
func nzInt(v, d int) int { if v==0 { return d }; return v }


func mongoStateStr(state int) string {
    switch state {
    case 0:
        return "STARTUP"
    case 1:
        return "PRIMARY"
    case 2:
        return "SECONDARY"
    case 3:
        return "RECOVERING"
    case 5:
        return "STARTUP2"
    case 6:
        return "UNKNOWN"
    case 7:
        return "ARBITER"
    case 8:
        return "DOWN"
    case 9:
        return "ROLLBACK"
    case 10:
        return "REMOVED"
    default:
        return "UNKNOWN"
    }
}


func (a *Agent) buildServiceNote(state int, stateStr string, rsid string, optime string, term any, syncSource string, progress map[string]any) string {
    m := map[string]any{
        "mongo": map[string]any{
            "state": state,
            "stateStr": stateStr,
        },
    }
    mm := m["mongo"].(map[string]any)
    if rsid != "" { mm["replicaSetId"] = rsid }
    if optime != "" { mm["optimeTs"] = optime }
    if term != nil { mm["term"] = term }
    if syncSource != "" { mm["syncSource"] = syncSource }
    if progress != nil { mm["progress"] = progress }
    b, err := json.Marshal(m)
    if err != nil {
        return stateStr
    }
    return string(b)
}


func (a *Agent) probeReplStatus(ctx context.Context) (state int, stateStr string, rsid string, optime string, term any, syncSource string, ok bool, progress map[string]any) {
    cli, err := a.mongoClient(ctx)
    if err != nil {
        return 0, "STARTUP", "", "", nil, "", false, nil
    }
    defer cli.Disconnect(context.Background())

    // replSetGetStatus
    var st bson.M
    if err := cli.Database("admin").RunCommand(ctx, bson.D{{Key: "replSetGetStatus", Value: 1}}).Decode(&st); err != nil {
        // fallback to hello
        var hello bson.M
        if err2 := cli.Database("admin").RunCommand(ctx, bson.D{{Key: "hello", Value: 1}}).Decode(&hello); err2 == nil {
            if v, _ := hello["isWritablePrimary"].(bool); v {
                return 1, "PRIMARY", "", "", nil, "", true, nil
            }
            if v, _ := hello["secondary"].(bool); v {
                return 2, "SECONDARY", "", "", nil, "", true, nil
            }
        }
        return 0, "STARTUP", "", "", nil, "", false, nil
    }

    // state
    if ms, ok2 := st["myState"].(int32); ok2 {
        state = int(ms)
    } else if ms, ok2 := st["myState"].(int64); ok2 {
        state = int(ms)
    } else if ms, ok2 := st["myState"].(float64); ok2 {
        state = int(ms)
    } else {
        state = 0
    }
    stateStr = mongoStateStr(state)

    // members: find self
    if mems, ok2 := st["members"].(bson.A); ok2 {
        for _, it := range mems {
            m, _ := it.(bson.M)
            if m == nil {
                continue
            }
            if self, ok3 := m["self"].(bool); ok3 && self {
                if name, ok4 := m["name"].(string); ok4 {
                    // name is host:port, not rs id
                    _ = name
                }
                if ss, ok4 := m["syncSourceHost"].(string); ok4 {
                    syncSource = ss
                }
                if t, ok4 := m["term"]; ok4 {
                    term = t
                }
                if od, ok4 := m["optimeDate"].(primitive.DateTime); ok4 {
                    optime = od.Time().UTC().Format(time.RFC3339)
                }
                break
            }
        }
    }

    // rsid: from config doc or set name if present
    if set, ok2 := st["set"].(string); ok2 {
        rsid = set
    }

    // initial sync progress: only when STARTUP2
    if state == 5 {
        progress = map[string]any{
            "op": "initial_sync",
            "phase": "running",
            "done": false,
            "updatedAt": time.Now().UTC().Format(time.RFC3339),
        }
        // try to extract extra info if present
        if iss, ok2 := st["initialSyncStatus"].(bson.M); ok2 {
            // keep small
            if s, ok3 := iss["status"].(string); ok3 && s != "" {
                progress["phase"] = strings.ToLower(s)
            }
        }
    }

    return state, stateStr, rsid, optime, term, syncSource, true, progress
}
