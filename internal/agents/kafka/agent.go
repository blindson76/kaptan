package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/umitbozkurt/consul-replctl/internal/orders"
	"github.com/umitbozkurt/consul-replctl/internal/servicereg"
	"github.com/umitbozkurt/consul-replctl/internal/store"
)

type Config struct {
	AgentID   string
	OrdersKey string
	AckKey    string

	KafkaBinDir string
	WorkDir     string
	LogDir      string
	MetaLogDir  string

	BrokerAddr     string
	ControllerAddr string

	ClusterID string
	NodeID    string

	Service servicereg.Registration
}

type Agent struct {
	cfg  Config
	kv   store.KV
	reg  servicereg.Registry
	proc *os.Process

	opMu sync.Mutex
	op   map[string]any
}

func New(cfg Config, kv store.KV, reg servicereg.Registry) *Agent {
	return &Agent{cfg: cfg, kv: kv, reg: reg}
}

func (a *Agent) Run(ctx context.Context) error {
	log.Printf("[kafka-agent] starting agent_id=%s orders_key=%s ack_key=%s bin_dir=%s work_dir=%s broker_addr=%s controller_addr=%s",
		a.cfg.AgentID, a.cfg.OrdersKey, a.cfg.AckKey, a.cfg.KafkaBinDir, a.cfg.WorkDir, a.cfg.BrokerAddr, a.cfg.ControllerAddr)

	if a.reg != nil && a.cfg.Service.ID != "" {
		_ = a.reg.Register(ctx, a.cfg.Service)
		defer a.reg.Deregister(context.Background(), a.cfg.Service.ID)
		_ = a.reg.SetTTL(ctx, a.cfg.Service.CheckID, servicereg.StatusWarning, a.buildServiceNote("startup"))
		go a.kafkaHeartbeat(ctx)
	}

	ch := a.kv.WatchPrefixJSON(ctx, a.cfg.OrdersKey, func() any { return &[]orders.Order{} })
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case v, ok := <-ch:
			if !ok {
				return nil
			}
			lst := v.([]orders.Order)
			if len(lst) == 0 {
				continue
			}
			ord := lst[len(lst)-1]
			if ord.TargetID != a.cfg.AgentID {
				continue
			}
			a.execute(ctx, ord)
		}
	}
}

func (a *Agent) execute(ctx context.Context, ord orders.Order) {
	log.Printf("[kafka-agent] received order action=%s epoch=%d payload=%v", ord.Action, ord.Epoch, ord.Payload)

	ack := orders.Ack{TargetID: a.cfg.AgentID, Action: ord.Action, Epoch: ord.Epoch, FinishedAt: time.Now()}
	var err error
	switch ord.Action {
	case orders.ActionStart:
		bsAny, _ := ord.Payload["bootstrapServers"].([]any)
		bss := make([]string, 0, len(bsAny))
		for _, x := range bsAny {
			if s, ok := x.(string); ok && s != "" {
				bss = append(bss, s)
			}
		}
		err = a.startKafka(ctx, bss)
	case orders.ActionStop:
		err = a.stopKafka()
	case orders.ActionAddVoter:
		bs, _ := ord.Payload["bootstrapServer"].(string)
		vid, _ := ord.Payload["voterId"].(string)
		ep, _ := ord.Payload["voterEndpoint"].(string)
		err = a.addVoter(ctx, bs, vid, ep)
	case orders.ActionRemoveVoter:
		bs, _ := ord.Payload["bootstrapServer"].(string)
		vid, _ := ord.Payload["voterId"].(string)
		err = a.removeVoter(ctx, bs, vid)
	case orders.ActionReassignPartitions:
		bs, _ := ord.Payload["bootstrapServer"].(string)
		err = a.reassignPartitions(ctx, bs)
	}
	if err != nil {
		ack.Ok = false
		ack.Message = err.Error()
		log.Printf("[kafka-agent] action=%s epoch=%d failed: %v", ord.Action, ord.Epoch, err)
	} else {
		ack.Ok = true
		log.Printf("[kafka-agent] action=%s epoch=%d ok", ord.Action, ord.Epoch)
	}
	_ = a.kv.PutJSON(ctx, a.cfg.AckKey, &ack)
}

func (a *Agent) startKafka(ctx context.Context, bootstrapControllers []string) error {
	propsPath := filepath.Join(a.cfg.WorkDir, "server.properties")
	if err := os.MkdirAll(a.cfg.WorkDir, 0o755); err != nil {
		return err
	}
	props := a.renderProperties(bootstrapControllers)
	if err := os.WriteFile(propsPath, []byte(props), 0o644); err != nil {
		return err
	}

	if a.cfg.ClusterID != "" {
		fmtCmd := filepath.Join(a.cfg.KafkaBinDir, "kafka-storage.bat")
		args := []string{"format", "--ignore-formatted", "-t", a.cfg.ClusterID, "-c", propsPath, "--no-initial-controllers"}
		log.Printf("[kafka-agent] exec %s %s", fmtCmd, strings.Join(args, " "))
		cmd := exec.CommandContext(ctx, fmtCmd, args...)
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		_ = cmd.Run()
	}

	startCmd := filepath.Join(a.cfg.KafkaBinDir, "kafka-server-start.bat")
	log.Printf("[kafka-agent] start %s %s", startCmd, propsPath)
	cmd := exec.CommandContext(ctx, startCmd, propsPath)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		return err
	}
	a.proc = cmd.Process
	log.Printf("[kafka-agent] started kafka pid=%d", cmd.Process.Pid)
	return nil
}

func (a *Agent) stopKafka() error {
	if a.proc == nil {
		return nil
	}
	pid := a.proc.Pid
	_ = exec.Command("taskkill", "/PID", strconv.Itoa(pid), "/T").Run()
	time.Sleep(2 * time.Second)
	_ = exec.Command("taskkill", "/F", "/PID", strconv.Itoa(pid), "/T").Run()
	a.proc = nil
	return nil
}

func (a *Agent) renderProperties(bootstrapControllers []string) string {
	bs := strings.Join(bootstrapControllers, ",")
	if bs == "" && a.cfg.ControllerAddr != "" {
		bs = a.cfg.ControllerAddr
	}
	nodeID := a.cfg.NodeID
	if nodeID == "" {
		nodeID = "1"
	}
	return fmt.Sprintf(`node.id=%s
process.roles=broker,controller

listeners=PLAINTEXT://%s,CONTROLLER://%s
advertised.listeners=PLAINTEXT://%s
inter.broker.listener.name=PLAINTEXT
controller.listener.names=CONTROLLER
listener.security.protocol.map=PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT

controller.quorum.bootstrap.servers=%s

log.dirs=%s
metadata.log.dir=%s
`, nodeID, a.cfg.BrokerAddr, a.cfg.ControllerAddr, a.cfg.BrokerAddr, bs, a.cfg.LogDir, a.cfg.MetaLogDir)
}

func waitTCP(addr string, timeout time.Duration) error {
	dl := time.Now().Add(timeout)
	for time.Now().Before(dl) {
		c, err := net.DialTimeout("tcp", addr, 2*time.Second)
		if err == nil {
			_ = c.Close()
			return nil
		}
		time.Sleep(1 * time.Second)
	}
	return fmt.Errorf("tcp timeout: %s", addr)
}

func (a *Agent) waitQuorumReady(ctx context.Context, bootstrapServer string, timeout time.Duration) error {
	tool := filepath.Join(a.cfg.KafkaBinDir, "kafka-metadata-quorum.bat")
	dl := time.Now().Add(timeout)
	attempt := 0
	for time.Now().Before(dl) {
		args := []string{"--bootstrap-server", bootstrapServer, "describe", "--status"}
		log.Printf("[kafka-agent] exec %s %s (attempt=%d)", tool, strings.Join(args, " "), attempt)
		attempt++
		cmd := exec.CommandContext(ctx, tool, args...)
		out, err := cmd.CombinedOutput()
		if err == nil {
			return nil
		}
		low := strings.ToLower(string(out))
		if strings.Contains(low, "not init") {
			return fmt.Errorf("metadata quorum unavailable (bootstrap=%s): cluster not initialized", bootstrapServer)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
	return fmt.Errorf("quorum not ready via %s", bootstrapServer)
}

func (a *Agent) addVoter(ctx context.Context, bootstrapServer, voterID, voterEndpoint string) error {
	if err := waitTCP(voterEndpoint, 90*time.Second); err != nil {
		return err
	}
	if err := a.waitQuorumReady(ctx, bootstrapServer, 90*time.Second); err != nil {
		return err
	}
	tool := filepath.Join(a.cfg.KafkaBinDir, "kafka-metadata-quorum.bat")
	args := []string{"--bootstrap-server", bootstrapServer, "add-voter", "--voter-id", voterID, "--voter-endpoint", voterEndpoint}
	log.Printf("[kafka-agent] exec %s %s", tool, strings.Join(args, " "))
	cmd := exec.CommandContext(ctx, tool, args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

func (a *Agent) removeVoter(ctx context.Context, bootstrapServer, voterID string) error {
	tool := filepath.Join(a.cfg.KafkaBinDir, "kafka-metadata-quorum.bat")
	args := []string{"--bootstrap-server", bootstrapServer, "remove-voter", "--voter-id", voterID}
	log.Printf("[kafka-agent] exec %s %s", tool, strings.Join(args, " "))
	cmd := exec.CommandContext(ctx, tool, args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

func (a *Agent) reassignPartitions(ctx context.Context, bootstrapServer string) error {
	a.setProgress(map[string]any{"op": "reassign_partitions", "phase": "start", "done": false, "updatedAt": time.Now().Format(time.RFC3339)})
	defer a.clearProgress()
	if err := a.waitQuorumReady(ctx, bootstrapServer, 90*time.Second); err != nil {
		return err
	}
	topicsTool := filepath.Join(a.cfg.KafkaBinDir, "kafka-topics.bat")
	reassignTool := filepath.Join(a.cfg.KafkaBinDir, "kafka-reassign-partitions.bat")

	a.setProgress(map[string]any{"op": "reassign_partitions", "phase": "list_topics", "done": false, "updatedAt": time.Now().Format(time.RFC3339)})
	listArgs := []string{"--bootstrap-server", bootstrapServer, "--list"}
	log.Printf("[kafka-agent] exec %s %s", topicsTool, strings.Join(listArgs, " "))
	out, err := exec.CommandContext(ctx, topicsTool, listArgs...).Output()
	if err != nil {
		return err
	}
	topics := []string{}
	for _, ln := range strings.Split(string(out), "\n") {
		t := strings.TrimSpace(ln)
		if t != "" {
			topics = append(topics, t)
		}
	}
	if len(topics) == 0 {
		return nil
	}

	tj := "{\"version\":1,\"topics\":["
	for i, t := range topics {
		if i > 0 {
			tj += ","
		}
		tj += fmt.Sprintf("{\"topic\":%q}", t)
	}
	tj += "]}"
	_ = os.MkdirAll(a.cfg.WorkDir, 0o755)
	topicsPath := filepath.Join(a.cfg.WorkDir, "replctl-topics.json")
	_ = os.WriteFile(topicsPath, []byte(tj), 0o644)

	brokerList := a.cfg.NodeID
	if brokerList == "" {
		brokerList = "1,2,3"
	}
	a.setProgress(map[string]any{"op": "reassign_partitions", "phase": "generate_plan", "done": false, "topics": len(topics), "updatedAt": time.Now().Format(time.RFC3339)})
	genArgs := []string{"--bootstrap-server", bootstrapServer, "--broker-list", brokerList, "--topics-to-move-json-file", topicsPath, "--generate"}
	log.Printf("[kafka-agent] exec %s %s", reassignTool, strings.Join(genArgs, " "))
	genCmd := exec.CommandContext(ctx, reassignTool, genArgs...)
	genOut, genErr := genCmd.CombinedOutput()
	if genErr != nil {
		return fmt.Errorf("generate failed: %v\n%s", genErr, string(genOut))
	}
	plan, err := extractJSONBlock(string(genOut))
	if err != nil {
		return err
	}
	planPath := filepath.Join(a.cfg.WorkDir, "replctl-reassign-plan.json")
	_ = os.WriteFile(planPath, []byte(plan), 0o644)

	a.setProgress(map[string]any{"op": "reassign_partitions", "phase": "execute", "done": false, "updatedAt": time.Now().Format(time.RFC3339)})
	execArgs := []string{"--bootstrap-server", bootstrapServer, "--reassignment-json-file", planPath, "--execute"}
	log.Printf("[kafka-agent] exec %s %s", reassignTool, strings.Join(execArgs, " "))
	execCmd := exec.CommandContext(ctx, reassignTool, execArgs...)
	execCmd.Stdout = os.Stdout
	execCmd.Stderr = os.Stderr
	if err := execCmd.Run(); err != nil {
		return err
	}
	a.setProgress(map[string]any{"op": "reassign_partitions", "phase": "verify", "done": false, "updatedAt": time.Now().Format(time.RFC3339)})
	deadline := time.Now().Add(10 * time.Minute)
	for time.Now().Before(deadline) {
		verArgs := []string{"--bootstrap-server", bootstrapServer, "--reassignment-json-file", planPath, "--verify"}
		log.Printf("[kafka-agent] exec %s %s", reassignTool, strings.Join(verArgs, " "))
		verCmd := exec.CommandContext(ctx, reassignTool, verArgs...)
		out, _ := verCmd.CombinedOutput()
		msg := strings.TrimSpace(string(out))
		if len(msg) > 280 {
			msg = msg[:280]
		}
		a.setProgress(map[string]any{"op": "reassign_partitions", "phase": "verify", "done": false, "last": msg, "updatedAt": time.Now().Format(time.RFC3339)})
		low := strings.ToLower(string(out))
		if strings.Contains(low, "is complete") || (strings.Contains(low, "completed") && !strings.Contains(low, "in progress")) {
			a.setProgress(map[string]any{"op": "reassign_partitions", "phase": "done", "done": true, "updatedAt": time.Now().Format(time.RFC3339)})
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(5 * time.Second):
		}
	}
	return nil
}

func extractJSONBlock(s string) (string, error) {
	marker := "Proposed partition reassignment configuration"
	idx := strings.Index(s, marker)
	if idx < 0 {
		idx = 0
	}
	start := strings.Index(s[idx:], "{")
	if start < 0 {
		return "", fmt.Errorf("no json start")
	}
	start = idx + start
	end := strings.LastIndex(s, "}")
	if end < 0 || end <= start {
		return "", fmt.Errorf("no json end")
	}
	return strings.TrimSpace(s[start : end+1]), nil
}

func (a *Agent) kafkaHeartbeat(ctx context.Context) {
	t := time.NewTicker(5 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if a.reg == nil || a.cfg.Service.CheckID == "" {
				continue
			}
			role := a.detectKafkaRole(ctx)
			if role == "" {
				_ = a.reg.SetTTL(ctx, a.cfg.Service.CheckID, servicereg.StatusWarning, a.buildServiceNote("startup"))
			} else {
				_ = a.reg.SetTTL(ctx, a.cfg.Service.CheckID, servicereg.StatusPassing, a.buildServiceNote(role))
			}
		}
	}
}

func (a *Agent) detectKafkaRole(ctx context.Context) string {
	bs := a.cfg.ControllerAddr
	if bs == "" {
		return ""
	}
	if err := a.waitQuorumReady(ctx, bs, 10*time.Second); err != nil {
		return ""
	}
	tool := filepath.Join(a.cfg.KafkaBinDir, "kafka-metadata-quorum.bat")
	cmd := exec.CommandContext(ctx, tool, "--bootstrap-server", bs, "describe", "--status")
	out, err := cmd.CombinedOutput()
	if err != nil {
		return "running"
	}
	s := string(out)
	leader := ""
	for _, line := range strings.Split(s, "\n") {
		if strings.Contains(line, "LeaderId") || strings.Contains(line, "Leader ID") {
			digits := ""
			for _, ch := range line {
				if ch >= '0' && ch <= '9' {
					digits += string(ch)
				}
			}
			if digits != "" {
				leader = digits
				break
			}
		}
	}
	if leader != "" && a.cfg.NodeID != "" {
		if leader == a.cfg.NodeID {
			return "controller-leader"
		}
		return "controller-follower"
	}
	return "running"
}

func (a *Agent) setProgress(m map[string]any) {
	a.opMu.Lock()
	a.op = m
	a.opMu.Unlock()
}
func (a *Agent) clearProgress() {
	a.opMu.Lock()
	a.op = nil
	a.opMu.Unlock()
}
func (a *Agent) getProgress() map[string]any {
	a.opMu.Lock()
	defer a.opMu.Unlock()
	if a.op == nil {
		return nil
	}
	out := map[string]any{}
	for k, v := range a.op {
		out[k] = v
	}
	return out
}

func (a *Agent) buildServiceNote(role string) string {
	payload := map[string]any{
		"kafka": map[string]any{
			"role": role,
		},
	}
	if p := a.getProgress(); p != nil {
		payload["kafka"].(map[string]any)["progress"] = p
	}
	b, err := json.Marshal(payload)
	if err != nil {
		return role
	}
	return string(b)
}
