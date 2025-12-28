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
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/umitbozkurt/consul-replctl/internal/orders"
	"github.com/umitbozkurt/consul-replctl/internal/servicereg"
	"github.com/umitbozkurt/consul-replctl/internal/store"
	"github.com/umitbozkurt/consul-replctl/internal/types"
)

type Config struct {
	AgentID   string
	OrdersKey string
	AckKey    string
	HealthKey string

	KafkaBinDir string
	WorkDir     string
	LogDir      string
	MetaLogDir  string

	BrokerAddr     string
	ControllerAddr string

	ClusterID string
	NodeID    string
	StorageID string

	Service servicereg.Registration
}

type Agent struct {
	cfg  Config
	kv   store.KV
	reg  servicereg.Registry
	proc *os.Process
	// procLog holds the log file handle for the running Kafka process.
	procLog *os.File

	opMu sync.Mutex
	op   map[string]any
}

func New(cfg Config, kv store.KV, reg servicereg.Registry) *Agent {
	return &Agent{cfg: cfg, kv: kv, reg: reg}
}

func (a *Agent) Run(ctx context.Context) error {
	log.Printf("[kafka-agent] starting agent_id=%s orders_key=%s ack_key=%s bin_dir=%s work_dir=%s broker_addr=%s controller_addr=%s",
		a.cfg.AgentID, a.cfg.OrdersKey, a.cfg.AckKey, a.cfg.KafkaBinDir, a.cfg.WorkDir, a.cfg.BrokerAddr, a.cfg.ControllerAddr)

	startHeartbeat := a.cfg.HealthKey != "" || (a.reg != nil && a.cfg.Service.ID != "")
	if a.reg != nil && a.cfg.Service.ID != "" {
		_ = a.reg.Register(ctx, a.cfg.Service)
		defer a.reg.Deregister(context.Background(), a.cfg.Service.ID)
		_ = a.reg.SetTTL(ctx, a.cfg.Service.CheckID, servicereg.StatusWarning, a.buildServiceNote("startup", "", nil, 0))
	}
	if a.cfg.HealthKey != "" {
		startNote := a.buildServiceNote("startup", "", nil, 0)
		a.publishHealth(ctx, false, startNote, startNote)
	}
	if startHeartbeat {
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
		initAny, _ := ord.Payload["initialControllers"].([]any)
		inits := make([]string, 0, len(initAny))
		for _, x := range initAny {
			if s, ok := x.(string); ok && s != "" {
				inits = append(inits, s)
			}
		}
		standalone, _ := ord.Payload["standalone"].(bool)
		err = a.startKafka(ctx, bss, inits, standalone)
	case orders.ActionStop:
		err = a.stopKafka()
	case orders.ActionAddVoter:
		bs, _ := ord.Payload["bootstrapServer"].(string)
		vid, _ := ord.Payload["voterId"].(string)
		ep, _ := ord.Payload["voterEndpoint"].(string)
		err = a.addVoter(ctx, bs, vid, ep)
	case orders.ActionRemoveVoter:
		bs, _ := ord.Payload["bootstrapServer"].(string)
		cid, _ := ord.Payload["controller-id"].(string)
		cdid, _ := ord.Payload["controller-directory-id"].(string)
		err = a.removeVoter(ctx, bs, cid, cdid)
	case orders.ActionRemoveController:
		bs, _ := ord.Payload["bootstrapServer"].(string)
		cid, _ := ord.Payload["controllerId"].(string)
		dirID, _ := ord.Payload["controllerDirectoryId"].(string)
		err = a.removeController(ctx, bs, cid, dirID)
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

func (a *Agent) startKafka(ctx context.Context, bootstrapControllers []string, initialControllers []string, standalone bool) error {
	nodeID := a.nodeID()
	addrs := a.normalizeBootstrapControllers(bootstrapControllers)
	if len(addrs) == 0 && a.cfg.ControllerAddr != "" {
		addrs = []string{a.cfg.ControllerAddr}
	}
	bootstrap := strings.Join(addrs, ",")
	propsPath := filepath.Join(a.cfg.WorkDir, fmt.Sprintf("server-%s.properties", nodeID))
	if err := os.MkdirAll(a.cfg.WorkDir, 0o755); err != nil {
		return err
	}
	if standalone {
		bs := []string{}
		for _, addr := range strings.Split(bootstrap, ",") {
			if addr != a.cfg.ControllerAddr {
				bs = append(bs, addr)
			}
		}
		bootstrap = strings.Join(bs, ",")
	}
	props := a.renderProperties(bootstrap)
	if err := os.WriteFile(propsPath, []byte(props), 0o644); err != nil {
		return err
	}

	if a.cfg.ClusterID != "" {
		fmtCmd := filepath.Join(a.cfg.KafkaBinDir, "kafka-storage.bat")
		args := []string{"format", "--ignore-formatted", "-t", a.cfg.ClusterID, "-c", propsPath}
		if standalone {
			args = append(args, "--standalone")
		} else {
			if bootstrap == "" {
				return fmt.Errorf("controller.quorum.bootstrap.servers required for kafka-storage format")
			}
			initialControllersStr := strings.Join(initialControllers, ",")
			if initialControllersStr == "" {
				return fmt.Errorf("initial controllers required for kafka-storage format")
			}
			args = append(args, "--initial-controllers", initialControllersStr)
		}
		log.Printf("[kafka-agent] exec %s %s", fmtCmd, strings.Join(args, " "))
		cmd := exec.CommandContext(ctx, fmtCmd, args...)
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		_ = cmd.Run()
	}

	startCmd := filepath.Join(a.cfg.KafkaBinDir, "kafka-server-start.bat")
	logPath := filepath.Join(a.cfg.WorkDir, fmt.Sprintf("kafka-server-%s.log", nodeID))
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	log.Printf("[kafka-agent] start %s %s (log=%s)", startCmd, propsPath, logPath)
	cmd := exec.CommandContext(ctx, startCmd, propsPath)
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	if err := cmd.Start(); err != nil {
		_ = logFile.Close()
		return err
	}
	a.proc = cmd.Process
	a.procLog = logFile
	log.Printf("[kafka-agent] started kafka pid=%d", cmd.Process.Pid)
	runNote := a.buildServiceNote("running", "", nil, 0)
	a.publishHealth(ctx, true, runNote, runNote)
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
	if a.procLog != nil {
		_ = a.procLog.Close()
		a.procLog = nil
	}
	a.publishHealth(context.Background(), false, "stopped", "stopped")
	return nil
}

func (a *Agent) renderProperties(bootstrap string) string {
	bs := bootstrap
	if bs == "" && a.cfg.ControllerAddr != "" {
		bs = a.cfg.ControllerAddr
	}
	nodeID := a.nodeID()
	logDir := escapeWindowsPath(a.cfg.LogDir)
	metaLogDir := escapeWindowsPath(a.cfg.MetaLogDir)
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
`, nodeID, a.cfg.BrokerAddr, a.cfg.ControllerAddr, a.cfg.BrokerAddr, bs, logDir, metaLogDir)
}

func (a *Agent) nodeID() string {
	if a.cfg.NodeID == "" {
		return "1"
	}
	return a.cfg.NodeID
}

func escapeWindowsPath(p string) string {
	return strings.ReplaceAll(p, `\`, `\\`)
}

func (a *Agent) normalizeBootstrapControllers(bootstrapControllers []string) []string {
	addrs := make([]string, 0, len(bootstrapControllers))
	for _, addr := range bootstrapControllers {
		addr = strings.TrimSpace(addr)
		if addr != "" {
			addrs = append(addrs, addr)
		}
	}
	return addrs
}

func (a *Agent) buildInitialControllers(bootstrapAddrs []string, provided []string) (string, error) {
	storageID := strings.TrimSpace(a.cfg.StorageID)
	nodeID := strings.TrimSpace(a.cfg.NodeID)
	broker := trimHostPort(a.cfg.BrokerAddr)
	controller := trimHostPort(a.cfg.ControllerAddr)

	source := a.normalizeBootstrapControllers(provided)
	if len(source) == 0 {
		source = a.normalizeBootstrapControllers(bootstrapAddrs)
	}
	if len(source) == 0 {
		return "", nil
	}

	entries := make([]string, 0, len(source))
	for i, raw := range source {
		raw = strings.TrimSpace(raw)
		if raw == "" {
			continue
		}
		id := ""
		right := raw
		if strings.Contains(raw, "@") {
			parts := strings.SplitN(raw, "@", 2)
			if parts[0] != "" {
				id = strings.TrimSpace(parts[0])
			}
			right = parts[1]
		}

		// Ensure host:port:storage suffix.
		if strings.Count(right, ":") < 2 {
			if storageID == "" {
				return "", fmt.Errorf("storage id required for initial controller %q", raw)
			}
			right = fmt.Sprintf("%s:%s", right, storageID)
		}

		if id == "" {
			base := trimHostPort(right)
			switch {
			case nodeID != "" && (strings.EqualFold(base, controller) || strings.EqualFold(base, broker)):
				id = nodeID
			default:
				id = strconv.Itoa(i + 1)
			}
		}

		entries = append(entries, fmt.Sprintf("%s@%s", id, right))
	}
	if len(entries) == 0 {
		return "", fmt.Errorf("no usable initial controllers")
	}
	return strings.Join(entries, ","), nil
}

func trimHostPort(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return ""
	}
	parts := strings.Split(s, ":")
	if len(parts) >= 2 {
		return strings.Join(parts[:2], ":")
	}
	return s
}

func ensureStorageSuffix(endpoint, storageID string) string {
	if strings.Count(endpoint, ":") < 2 {
		return fmt.Sprintf("%s:%s", endpoint, storageID)
	}
	return endpoint
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
	controller := fmt.Sprintf("%s@%s", voterID, voterEndpoint)
	args := []string{"--bootstrap-controller", bootstrapServer, "add-controller", "--controller", controller}
	log.Printf("[kafka-agent] exec %s %s", tool, strings.Join(args, " "))
	cmd := exec.CommandContext(ctx, tool, args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

func (a *Agent) removeVoter(ctx context.Context, bootstrapServer, cid string, cdid string) error {
	tool := filepath.Join(a.cfg.KafkaBinDir, "kafka-metadata-quorum.bat")
	args := []string{"--bootstrap-controller", bootstrapServer, "remove-controller", "--controller-id", cid, "--controller-directory-id", cdid}
	log.Printf("[kafka-agent] exec %s %s", tool, strings.Join(args, " "))
	cmd := exec.CommandContext(ctx, tool, args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

func (a *Agent) removeController(ctx context.Context, bootstrapController, controllerID, controllerDirID string) error {
	tool := filepath.Join(a.cfg.KafkaBinDir, "kafka-metadata-quorum.bat")
	args := []string{"--bootstrap-controller", bootstrapController, "remove-controller", "--controller-id", controllerID}
	if controllerDirID != "" {
		args = append(args, "--controller-directory-id", controllerDirID)
	}
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
			info := a.detectKafkaInfo(ctx)
			role := info.role
			noteRole := role
			if noteRole == "" {
				noteRole = "startup"
			}
			note := a.buildServiceNote(noteRole, info.leaderID, info.lags, info.maxLag)

			if a.reg != nil && a.cfg.Service.CheckID != "" {
				status := servicereg.StatusWarning
				if role != "" {
					status = servicereg.StatusPassing
				}
				_ = a.reg.SetTTL(ctx, a.cfg.Service.CheckID, status, note)
			}
			healthy := role != ""
			a.publishHealth(ctx, healthy, note, note)
		}
	}
}

type kafkaInfo struct {
	role     string
	leaderID string
	lags     map[string]int64
	maxLag   int64
}

func (a *Agent) detectKafkaInfo(ctx context.Context) kafkaInfo {
	bs := a.cfg.ControllerAddr
	if bs == "" {
		return kafkaInfo{}
	}
	if err := a.waitQuorumReady(ctx, bs, 10*time.Second); err != nil {
		return kafkaInfo{}
	}
	tool := filepath.Join(a.cfg.KafkaBinDir, "kafka-metadata-quorum.bat")
	statusCmd := exec.CommandContext(ctx, tool, "--bootstrap-controller", bs, "describe", "--status")
	statusOut, statusErr := statusCmd.CombinedOutput()

	replicationCmd := exec.CommandContext(ctx, tool, "--bootstrap-controller", bs, "describe", "--replication")
	replicationOut, _ := replicationCmd.CombinedOutput()

	leader := extractFirstDigits(string(statusOut))
	role := ""
	if statusErr == nil {
		role = "running"
		if leader != "" && a.cfg.NodeID != "" {
			if leader == a.cfg.NodeID {
				role = "controller-leader"
			} else {
				role = "controller-follower"
			}
		}
	}
	lags, maxLag := extractReplicationLag(string(replicationOut))
	return kafkaInfo{role: role, leaderID: leader, lags: lags, maxLag: maxLag}
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

func (a *Agent) buildServiceNote(role string, leader string, lags map[string]int64, maxLag int64) string {
	payload := map[string]any{
		"kafka": map[string]any{
			"role": role,
		},
	}
	if leader != "" {
		payload["kafka"].(map[string]any)["leaderId"] = leader
	}
	if len(lags) > 0 {
		payload["kafka"].(map[string]any)["lag"] = map[string]any{
			"max":        maxLag,
			"byFollower": lags,
		}
	} else if maxLag > 0 {
		payload["kafka"].(map[string]any)["lag"] = map[string]any{"max": maxLag}
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

func (a *Agent) publishHealth(ctx context.Context, healthy bool, reason string, note string) {
	if a.cfg.HealthKey == "" {
		return
	}
	meta := map[string]string{}
	if a.cfg.Service.ID != "" {
		meta["serviceId"] = a.cfg.Service.ID
	}
	if a.cfg.Service.CheckID != "" {
		meta["checkId"] = a.cfg.Service.CheckID
	}
	if a.cfg.Service.Address != "" {
		meta["address"] = a.cfg.Service.Address
	}
	if a.cfg.Service.Port > 0 {
		meta["port"] = strconv.Itoa(a.cfg.Service.Port)
	}
	if len(a.cfg.Service.Tags) > 0 {
		meta["tags"] = strings.Join(a.cfg.Service.Tags, ",")
	}
	h := types.HealthStatus{
		ID:          a.cfg.AgentID,
		Healthy:     healthy,
		Reason:      reason,
		Note:        note,
		ServiceMeta: meta,
		UpdatedAt:   time.Now(),
	}
	if err := a.kv.PutJSON(ctx, a.cfg.HealthKey, &h); err != nil {
		log.Printf("[kafka-agent] health publish error: %v", err)
	}
}

func extractFirstDigits(s string) string {
	for _, line := range strings.Split(s, "\n") {
		if strings.Contains(line, "LeaderId") || strings.Contains(line, "Leader ID") {
			digits := ""
			for _, ch := range line {
				if ch >= '0' && ch <= '9' {
					digits += string(ch)
				}
			}
			if digits != "" {
				return digits
			}
		}
	}
	return ""
}

func extractReplicationLag(s string) (map[string]int64, int64) {
	lags := map[string]int64{}
	var maxLag int64
	// Example row:
	// NodeId  DirectoryId             LogEndOffset    Lag     LastFetchTimestamp      LastCaughtUpTimestamp   Status
	// 1       279THXvBR4WGfBj_y1nstQ  67              0       1766926184999           1766926184999           Leader
	reRow := regexp.MustCompile(`^\s*([0-9]+)\s+[A-Za-z0-9_-]+\s+[0-9]+\s+([0-9]+)`)
	for _, line := range strings.Split(s, "\n") {
		if m := reRow.FindStringSubmatch(line); len(m) == 3 {
			id := m[1]
			lagVal := m[2]
			if v, err := strconv.ParseInt(lagVal, 10, 64); err == nil {
				lags[id] = v
				if v > maxLag {
					maxLag = v
				}
			}
		}
	}
	return lags, maxLag
}
