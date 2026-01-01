package kafka

import (
	"bufio"
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
	SpecKey   string

	KafkaBinDir string
	WorkDir     string
	LogDir      string
	MetaLogDir  string

	BrokerAddr     string
	ControllerAddr string

	ClusterID string
	NodeID    string
	StorageID string

	Service   servicereg.Registration
	PropsPath string
}

type Agent struct {
	cfg Config
	kv  store.KV
	reg servicereg.Registry
	// procCmd holds the running Kafka command (for Wait supervision).
	procCmd *exec.Cmd
	proc    *os.Process
	// procLog holds the log file handle for the running Kafka process.
	procLog *os.File

	procMu           sync.Mutex
	procExpectedExit bool

	baseService   servicereg.Registration
	activeService servicereg.Registration
	activeSlot    int
	svcMu         sync.Mutex

	opMu sync.Mutex
	op   map[string]any
}

func New(cfg Config, kv store.KV, reg servicereg.Registry) *Agent {
	return &Agent{cfg: cfg, kv: kv, reg: reg, baseService: cfg.Service}
}

func (a *Agent) Run(ctx context.Context) error {
	log.Printf("[kafka-agent] starting agent_id=%s orders_key=%s ack_key=%s bin_dir=%s work_dir=%s broker_addr=%s controller_addr=%s",
		a.cfg.AgentID, a.cfg.OrdersKey, a.cfg.AckKey, a.cfg.KafkaBinDir, a.cfg.WorkDir, a.cfg.BrokerAddr, a.cfg.ControllerAddr)

	startHeartbeat := a.cfg.HealthKey != "" || (a.reg != nil && (a.baseService.ID != "" || a.baseService.Name != ""))
	if a.cfg.HealthKey != "" {
		startNote := a.buildServiceNote("startup", "", nil, 0)
		a.publishHealth(ctx, false, startNote, startNote)
	}
	if startHeartbeat {
		go a.kafkaHeartbeat(ctx)
	}

	specKey := a.cfg.SpecKey
	if specKey == "" {
		specKey = "spec/kafka"
	}
	specCh := (<-chan any)(nil)
	if specKey != "" {
		specCh = a.kv.WatchPrefixJSON(ctx, specKey, func() any { return &[]types.ReplicaSpec{} })
	}
	ch := a.kv.WatchPrefixJSON(ctx, a.cfg.OrdersKey, func() any { return &[]orders.Order{} })
	defer a.stopServiceRegistration()
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
		case v, ok := <-specCh:
			if !ok {
				a.stopServiceRegistration()
				return nil
			}
			specs := v.([]types.ReplicaSpec)
			var spec types.ReplicaSpec
			if len(specs) > 0 {
				spec = specs[len(specs)-1]
			}
			a.updateServiceRegistration(ctx, spec)
		}
	}
}

func (a *Agent) updateServiceRegistration(ctx context.Context, spec types.ReplicaSpec) {
	if a.reg == nil {
		return
	}
	if a.baseService.ID == "" && a.baseService.Name == "" {
		return
	}
	member := false
	slot := 0
	for i, id := range spec.Members {
		if id == a.cfg.AgentID {
			member = true
			slot = i + 1
			break
		}
	}
	a.svcMu.Lock()
	activeID := a.activeService.ID
	activeSlot := a.activeSlot
	a.svcMu.Unlock()

	if member {
		newReg := a.baseService
		baseName := newReg.Name
		if baseName == "" {
			baseName = "kafka"
		}
		newReg.Name = baseName
		slotID := fmt.Sprintf("kafka-%d", slot)
		newReg.ID = slotID
		newReg.CheckID = fmt.Sprintf("check:%s", slotID)
		if activeID == newReg.ID && activeSlot == slot {
			return
		}
		newReg.TTL = "15s"
		a.stopServiceRegistration()
		if err := a.reg.Register(ctx, newReg); err != nil {
			log.Printf("[kafka-agent] service register failed: %v", err)
			return
		}
		a.svcMu.Lock()
		a.activeService = newReg
		a.activeSlot = slot
		a.svcMu.Unlock()
		note := a.buildServiceNote("startup", "", nil, 0)
		_ = a.reg.SetTTL(ctx, newReg.CheckID, servicereg.StatusWarning, note)
		log.Printf("[kafka-agent] service registered (member of spec slot=%d) id=%s", slot, newReg.ID)
		return
	}

	a.stopServiceRegistration()
}

func (a *Agent) stopServiceRegistration() {
	a.svcMu.Lock()
	active := a.activeService
	a.activeService = servicereg.Registration{}
	a.activeSlot = 0
	a.svcMu.Unlock()

	if active.ID != "" && a.reg != nil {
		_ = a.reg.Deregister(context.Background(), active.ID)
	}
}

func (a *Agent) getActiveService() servicereg.Registration {
	a.svcMu.Lock()
	defer a.svcMu.Unlock()
	return a.activeService
}

func (a *Agent) execute(ctx context.Context, ord orders.Order) {
	log.Printf("[kafka-agent] received order action=%s epoch=%d payload=%v", ord.Action, ord.Epoch, ord.Payload)

	ack := orders.Ack{TargetID: a.cfg.AgentID, Action: ord.Action, Epoch: ord.Epoch, FinishedAt: time.Now()}
	var err error
	switch ord.Action {
	case orders.ActionStart:
		bsAny, _ := ord.Payload["bootstrap-controllers"].([]any)
		bsc := make([]string, 0, len(bsAny))
		for _, x := range bsAny {
			if s, ok := x.(string); ok && s != "" {
				bsc = append(bsc, s)
			}
		}
		mode, _ := ord.Payload["mode"].(string)
		err = a.startKafka(ctx, bsc, mode)
	case orders.ActionStop:
		err = a.stopKafka()
	case orders.ActionAddController:
		bsAny, _ := ord.Payload["bootstrap-controllers"].([]any)
		bsc := make([]string, 0, len(bsAny))
		for _, x := range bsAny {
			if s, ok := x.(string); ok && s != "" {
				bsc = append(bsc, s)
			}
		}
		err = a.addController(ctx, strings.Join(bsc, ","))
	case orders.ActionRemoveController:
		bs, _ := ord.Payload["bootstrap-controller"].(string)
		cid, _ := ord.Payload["controller-id"].(string)
		dirID, _ := ord.Payload["controller-directory-id"].(string)
		err = a.removeController(ctx, bs, cid, dirID)
	case orders.ActionReassignPartitions:
		brokerList, _ := ord.Payload["broker-list"].(string)
		bssAny, _ := ord.Payload["bootstrap-servers"].([]any)
		bscAny, _ := ord.Payload["bootstrap-controllers"].([]any)
		bss := make([]string, 0, len(bssAny))
		bsc := make([]string, 0, len(bscAny))
		for _, x := range bssAny {
			if s, ok := x.(string); ok && s != "" {
				bss = append(bss, s)
			}
		}
		for _, x := range bscAny {
			if s, ok := x.(string); ok && s != "" {
				bsc = append(bsc, s)
			}
		}
		log.Printf("[kafka-agent] reassign partitions ord:%v bootstrap servers: %+v bootstrap controllers: %+v", ord, bss, bsc)
		err = a.reassignPartitions(ctx, strings.Join(bss, ","), strings.Join(bsc, ","), brokerList)
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

func (a *Agent) startKafka(ctx context.Context, bootstrapControllers []string, mode string) error {
	nodeID := a.nodeID()
	addrs := a.normalizeBootstrapControllers(bootstrapControllers)
	if len(addrs) == 0 && a.cfg.ControllerAddr != "" {
		addrs = []string{a.cfg.ControllerAddr}
	}
	bootstrap := strings.Join(addrs, ",")
	propsPath := filepath.Join(a.cfg.WorkDir, fmt.Sprintf("server-%s.properties", nodeID))
	a.cfg.PropsPath = propsPath
	if err := os.MkdirAll(a.cfg.WorkDir, 0o755); err != nil {
		return err
	}
	props := a.renderProperties(bootstrap)
	if err := os.WriteFile(propsPath, []byte(props), 0o644); err != nil {
		return err
	}

	if a.cfg.ClusterID != "" {
		fmtCmd := filepath.Join(a.cfg.KafkaBinDir, "kafka-storage.bat")
		args := []string{"format", "-t", a.cfg.ClusterID, "-c", propsPath}
		switch mode {
		case "standalone":
			args = append(args, "--standalone")
		case "no-initial-controllers":
			args = append(args, "--no-initial-controllers")
		default:
		}
		log.Printf("[kafka-agent] exec %s %s", fmtCmd, strings.Join(args, " "))
		cmd := exec.CommandContext(ctx, fmtCmd, args...)
		cmd.Env = a.kafkaEnv()
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
	cmd.Env = a.kafkaEnv()
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	if err := cmd.Start(); err != nil {
		_ = logFile.Close()
		return err
	}
	a.procMu.Lock()
	a.procCmd = cmd
	a.proc = cmd.Process
	a.procLog = logFile
	a.procExpectedExit = false
	a.procMu.Unlock()
	log.Printf("[kafka-agent] started kafka pid=%d", cmd.Process.Pid)
	go a.watchKafkaProcess(ctx, cmd)
	runNote := a.buildServiceNote("running", "", nil, 0)
	a.publishHealth(ctx, true, runNote, runNote)
	return nil
}

func (a *Agent) stopKafka() error {
	a.procMu.Lock()
	cmd := a.procCmd
	if cmd == nil || cmd.Process == nil {
		a.procMu.Unlock()
		return nil
	}
	a.procExpectedExit = true
	pid := cmd.Process.Pid
	a.procMu.Unlock()
	_ = exec.Command("taskkill", "/PID", strconv.Itoa(pid), "/T").Run()
	time.Sleep(2 * time.Second)
	_ = exec.Command("taskkill", "/F", "/PID", strconv.Itoa(pid), "/T").Run()
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
# Replication defaults
default.replication.factor=3
min.insync.replicas=2

# (optional but recommended)
unclean.leader.election.enable=false
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

func (a *Agent) kafkaEnv() []string {
	env := os.Environ()
	if a.cfg.KafkaBinDir == "" {
		return env
	}
	logCfg := filepath.Join(a.cfg.KafkaBinDir, "..", "..", "config", "log4j2.yaml")
	// log.Printf("[kafka-agent] setting KAFKA_LOG4J_OPTS to use log4j2 config: %s", logCfg)
	return setEnvVar(env, "KAFKA_LOG4J_OPTS", fmt.Sprintf("-Dlog4j2.configurationFile=%s", logCfg))
}

func setEnvVar(env []string, key, value string) []string {
	prefix := key + "="
	replaced := false
	for i, kv := range env {
		if k, _, ok := strings.Cut(kv, "="); ok && strings.EqualFold(k, key) {
			env[i] = prefix + value
			replaced = true
		}
	}
	if !replaced {
		env = append(env, prefix+value)
	}
	return env
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
		args := []string{"--bootstrap-controller", bootstrapServer, "describe", "--status"}
		log.Printf("[kafka-agent] exec %s %s (attempt=%d)", tool, strings.Join(args, " "), attempt)
		attempt++
		cmd := exec.CommandContext(ctx, tool, args...)
		cmd.Env = a.kafkaEnv()
		out, err := cmd.Output()
		if err == nil {
			return nil
		}
		log.Printf("[kafka-agent] quorum status output:\n%s", string(out))
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

func (a *Agent) waitGetObserver(ctx context.Context, bootstrapController string, timeout time.Duration) error {
	nodeID := strings.TrimSpace(a.nodeID())
	if nodeID == "" {
		return fmt.Errorf("node id required for observer wait")
	}
	tool := filepath.Join(a.cfg.KafkaBinDir, "kafka-metadata-quorum.bat")
	dl := time.Now().Add(timeout)
	attempt := 0
	for time.Now().Before(dl) {
		args := []string{"--bootstrap-controller", bootstrapController, "describe", "--status"}
		log.Printf("[kafka-agent] exec %s %s (observer attempt=%d)", tool, strings.Join(args, " "), attempt)
		attempt++
		cmd := exec.CommandContext(ctx, tool, args...)
		cmd.Env = a.kafkaEnv()
		out, err := cmd.Output()
		if observerListed(string(out), nodeID) {
			return nil
		}
		if err != nil {
			log.Printf("[kafka-agent] observer check error: %v", err)
		}
		log.Printf("[kafka-agent] observer check output:\n%s", string(out))
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
	return fmt.Errorf("observer state not reached for node %s via %s", nodeID, bootstrapController)
}

func observerListed(status string, nodeID string) bool {
	if nodeID == "" {
		return false
	}
	reCurrent := regexp.MustCompile(`(?mi)currentobservers:\s*(\[[^\r\n]*\])`)
	reInSync := regexp.MustCompile(`(?mi)in-sync\s+observers:\s*(\[[^\r\n]*\])`)
	for _, re := range []*regexp.Regexp{reCurrent, reInSync} {
		m := re.FindStringSubmatch(status)
		if len(m) == 2 {
			for _, id := range parseObserverList(m[1]) {
				if id == nodeID {
					return true
				}
			}
		}
	}
	log.Printf("[kafka-agent] observer %s not found in status", nodeID)
	return false
}

func parseObserverList(list string) []string {
	reID := regexp.MustCompile(`(?i)"id"\s*:\s*([0-9]+)`)
	out := []string{}
	for _, m := range reID.FindAllStringSubmatch(list, -1) {
		if len(m) == 2 {
			id := strings.TrimSpace(m[1])
			if id != "" {
				out = append(out, id)
			}
		}
	}
	if len(out) > 0 {
		return out
	}
	parts := strings.Split(list, ",")
	for _, raw := range parts {
		id := strings.TrimSpace(raw)
		if id == "" {
			continue
		}
		if idx := strings.IndexAny(id, "@ \t("); idx >= 0 {
			id = id[:idx]
		}
		if strings.Contains(id, "=") {
			id = id[strings.LastIndex(id, "=")+1:]
		}
		id = strings.Trim(id, "{}")
		id = strings.TrimSpace(id)
		if id != "" {
			out = append(out, id)
		}
	}
	return out
}

func (a *Agent) addController(ctx context.Context, bootstrapServer string) error {
	ctrlEndpoint := strings.TrimSpace(a.cfg.ControllerAddr)
	if ctrlEndpoint == "" {
		return fmt.Errorf("controller address required for add-controller")
	}
	bootstrapServer = strings.TrimSpace(bootstrapServer)
	if bootstrapServer == "" {
		return fmt.Errorf("bootstrap controller required for add-controller")
	}
	log.Printf("[kafka-agent] adding controller %s via bootstrap %s", ctrlEndpoint, bootstrapServer)
	if err := waitTCP(trimHostPort(ctrlEndpoint), 90*time.Second); err != nil {
		return err
	}
	log.Printf("[kafka-agent] controller endpoint %s reachable", ctrlEndpoint)
	if err := a.waitQuorumReady(ctx, bootstrapServer, 90*time.Second); err != nil {
		return err
	}
	log.Printf("[kafka-agent] quorum ready via %s", bootstrapServer)
	if err := a.waitGetObserver(ctx, bootstrapServer, 90*time.Second); err != nil {
		return err
	}
	controllerEndpoint := trimHostPort(ctrlEndpoint)
	if a.cfg.StorageID != "" {
		controllerEndpoint = ensureStorageSuffix(controllerEndpoint, a.cfg.StorageID)
	}
	tool := filepath.Join(a.cfg.KafkaBinDir, "kafka-metadata-quorum.bat")
	args := []string{"--command-config", a.cfg.PropsPath, "--bootstrap-controller", bootstrapServer, "add-controller"}
	attempt := 0
	for {
		log.Printf("[kafka-agent] exec %s %s (attempt=%d)", tool, strings.Join(args, " "), attempt)
		cmd := exec.CommandContext(ctx, tool, args...)
		cmd.Env = a.kafkaEnv()
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		if err := cmd.Run(); err != nil {
			log.Printf("[kafka-agent] add-controller error: %v", err)
			attempt++
			if attempt < 3 {
				time.Sleep(3 * time.Second)
				continue
			}
			return err
		}
		log.Printf("[kafka-agent] controller exited %d added successfully", cmd.ProcessState.ExitCode())
		break
	}
	return nil
}

func (a *Agent) removeVoter(ctx context.Context, bootstrapServer, cid string, cdid string) error {
	tool := filepath.Join(a.cfg.KafkaBinDir, "kafka-metadata-quorum.bat")
	args := []string{"--bootstrap-controller", bootstrapServer, "remove-controller", "--controller-id", cid, "--controller-directory-id", cdid}
	log.Printf("[kafka-agent] exec %s %s", tool, strings.Join(args, " "))
	cmd := exec.CommandContext(ctx, tool, args...)
	cmd.Env = a.kafkaEnv()
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

func (a *Agent) removeController(ctx context.Context, bootstrapController, controllerID, controllerDirID string) error {
	if controllerID == "" {
		return fmt.Errorf("controller id required for remove-controller")
	}
	replication, err := a.metaDecribeReplication(ctx, bootstrapController)
	log.Printf("[kafka-agent] remove controller replication state: %+v err:%v", replication, err)
	var directoryId string
	for _, ctrl := range replication.Rows {
		log.Printf("[kafka-agent] checking controller id %d vs %s", ctrl.NodeID, controllerID)
		if controllerID == fmt.Sprint(ctrl.NodeID) {
			directoryId = ctrl.DirectoryID
			break
		}
	}
	if directoryId == "" {
		return fmt.Errorf("controller id %s not found in replication state", controllerID)
	}

	tool := filepath.Join(a.cfg.KafkaBinDir, "kafka-metadata-quorum.bat")
	args := []string{"--bootstrap-controller", bootstrapController, "remove-controller", "--controller-id", controllerID, "--controller-directory-id", directoryId}
	log.Printf("[kafka-agent] exec %s %s", tool, strings.Join(args, " "))
	cmd := exec.CommandContext(ctx, tool, args...)
	cmd.Env = a.kafkaEnv()
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

func (a *Agent) reassignPartitions(ctx context.Context, bootstrapServer string, bootstrapControllers string, brokerList string) error {
	a.setProgress(map[string]any{"op": "reassign_partitions", "phase": "start", "done": false, "updatedAt": time.Now().Format(time.RFC3339)})
	defer a.clearProgress()
	if err := a.waitQuorumReady(ctx, bootstrapControllers, 90*time.Second); err != nil {
		return err
	}
	log.Printf("[kafka-agent] starting partition reassignment via %s", bootstrapServer)
	topicsTool := filepath.Join(a.cfg.KafkaBinDir, "kafka-topics.bat")
	reassignTool := filepath.Join(a.cfg.KafkaBinDir, "kafka-reassign-partitions.bat")

	a.setProgress(map[string]any{"op": "reassign_partitions", "phase": "list_topics", "done": false, "updatedAt": time.Now().Format(time.RFC3339)})
	listArgs := []string{"--bootstrap-server", bootstrapServer, "--list"}
	log.Printf("[kafka-agent] exec %s %s", topicsTool, strings.Join(listArgs, " "))
	listCmd := exec.CommandContext(ctx, topicsTool, listArgs...)
	listCmd.Env = a.kafkaEnv()
	out, err := listCmd.Output()
	if err != nil {
		return err
	}
	log.Printf("[kafka-agent] topics:\n%s", string(out))
	topics := []string{}
	for _, ln := range strings.Split(string(out), "\n") {
		t := strings.TrimSpace(ln)
		if t != "" {
			topics = append(topics, t)
		}
	}
	if len(topics) == 0 {
		log.Printf("[kafka-agent] no topics to reassign")
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

	a.setProgress(map[string]any{"op": "reassign_partitions", "phase": "generate_plan", "done": false, "topics": len(topics), "updatedAt": time.Now().Format(time.RFC3339)})
	genArgs := []string{"--bootstrap-server", bootstrapServer, "--broker-list", brokerList, "--topics-to-move-json-file", topicsPath, "--generate"}
	log.Printf("[kafka-agent] exec %s %s", reassignTool, strings.Join(genArgs, " "))
	genCmd := exec.CommandContext(ctx, reassignTool, genArgs...)
	genCmd.Env = a.kafkaEnv()
	genOut, genErr := genCmd.Output()
	if genErr != nil {
		return fmt.Errorf("generate failed: %v\n%s", genErr, string(genOut))
	}
	log.Printf("[kafka-agent] generated reassignment plan:\n%s", string(genOut))
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
	execCmd.Env = a.kafkaEnv()
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
		verCmd.Env = a.kafkaEnv()
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
			log.Printf("[kafka-agent] heartbeat role=%s leader=%s maxLag=%d, note=%s", role, info.leaderID, info.maxLag, note)
			svc := a.getActiveService()
			if a.reg != nil && svc.CheckID != "" {
				status := servicereg.StatusWarning
				if role != "" {
					status = servicereg.StatusPassing
				}
				_ = a.reg.SetTTL(ctx, svc.CheckID, status, note)
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

type MetaReplication struct {
	Raw  string
	Rows []MetaReplicationRow
	Lags map[string]int64
	// MaxLag is the largest Lag value seen in Rows (or extracted from Raw).
	MaxLag int64
}

type MetaReplicationRow struct {
	NodeID                int64
	DirectoryID           string
	LogEndOffset          int64
	Lag                   int64
	LastFetchTimestamp    int64
	LastCaughtUpTimestamp int64
	Status                string
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
	statusCmd.Env = a.kafkaEnv()
	statusOut, statusErr := statusCmd.CombinedOutput()

	replication, _ := a.metaDecribeReplication(ctx, bs)

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
	return kafkaInfo{role: role, leaderID: leader, lags: replication.Lags, maxLag: replication.MaxLag}
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
	svc := a.getActiveService()
	meta := map[string]string{}
	if svc.ID != "" {
		meta["serviceId"] = svc.ID
	}
	if svc.CheckID != "" {
		meta["checkId"] = svc.CheckID
	}
	if svc.Address != "" {
		meta["address"] = svc.Address
	}
	if svc.Port > 0 {
		meta["port"] = strconv.Itoa(svc.Port)
	}
	if len(svc.Tags) > 0 {
		meta["tags"] = strings.Join(svc.Tags, ",")
	}
	if a.reg != nil && svc.CheckID != "" {
		status := servicereg.StatusPassing
		if !healthy {
			status = servicereg.StatusCritical
		}
		_ = a.reg.SetTTL(ctx, svc.CheckID, status, note)
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

func (a *Agent) watchKafkaProcess(ctx context.Context, cmd *exec.Cmd) {
	err := cmd.Wait()
	a.procMu.Lock()
	expected := a.procExpectedExit
	if a.procCmd == cmd {
		a.procCmd = nil
		a.proc = nil
	}
	if a.procLog != nil {
		_ = a.procLog.Close()
		a.procLog = nil
	}
	a.procMu.Unlock()
	if ctx.Err() != nil || expected {
		log.Printf("[kafka-agent] kafka process exited (expected): %v", err)
		return
	}
	reason := "kafka process exited"
	if err != nil {
		reason = fmt.Sprintf("kafka process exited: %v", err)
	}
	log.Printf("[kafka-agent] %s", reason)
	note := a.buildServiceNote("stopped", "", nil, 0)
	a.publishHealth(context.Background(), false, reason, note)
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

func (a *Agent) metaDecribeReplication(ctx context.Context, bootstrapController string) (MetaReplication, error) {
	res := MetaReplication{
		Lags: map[string]int64{},
	}
	bs := strings.TrimSpace(bootstrapController)
	if bs == "" {
		bs = strings.TrimSpace(a.cfg.ControllerAddr)
	}
	if bs == "" {
		return res, fmt.Errorf("bootstrap controller required for replication describe")
	}
	tool := filepath.Join(a.cfg.KafkaBinDir, "kafka-metadata-quorum.bat")
	cmd := exec.CommandContext(ctx, tool, "--bootstrap-controller", bs, "describe", "--replication")
	cmd.Env = a.kafkaEnv()
	out, err := cmd.CombinedOutput()
	res.Raw = string(out)
	res.Rows, err = parseMetaReplicationRows(res.Raw)
	if err != nil {
		return res, fmt.Errorf("parse replication rows: %w", err)
	}
	res.Lags, res.MaxLag = extractReplicationLag(res.Raw)
	if err != nil {
		return res, fmt.Errorf("describe replication via %s: %w", bs, err)
	}
	log.Printf("[kafka-agent] replication describe output:\n%s", string(out))
	return res, nil
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

func parseMetaReplicationRows(s string) ([]MetaReplicationRow, error) {
	var result []MetaReplicationRow

	scanner := bufio.NewScanner(strings.NewReader(s))

	headerSeen := false

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())

		// Skip empty lines
		if line == "" {
			continue
		}

		// Skip log noise (log4j, ERROR, timestamps, etc.)
		if strings.Contains(line, "ERROR") ||
			strings.HasPrefix(line, "202") {
			continue
		}

		// Detect header
		if strings.HasPrefix(line, "NodeId") {
			headerSeen = true
			continue
		}

		if !headerSeen {
			continue
		}

		// Split by whitespace (Kafka output is column-aligned)
		fields := strings.Fields(line)
		if len(fields) < 7 {
			return nil, fmt.Errorf("invalid replication line: %q", line)
		}

		nodeID, err := strconv.Atoi(fields[0])
		if err != nil {
			return nil, fmt.Errorf("invalid NodeId in line %q: %w", line, err)
		}

		logEndOffset, err := strconv.ParseInt(fields[2], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid LogEndOffset in line %q: %w", line, err)
		}

		lag, err := strconv.ParseInt(fields[3], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid Lag in line %q: %w", line, err)
		}

		lastFetchTs, err := strconv.ParseInt(fields[4], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid LastFetchTimestamp in line %q: %w", line, err)
		}

		lastCaughtUpTs, err := strconv.ParseInt(fields[5], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid LastCaughtUpTimestamp in line %q: %w", line, err)
		}

		result = append(result, MetaReplicationRow{
			NodeID:                int64(nodeID),
			DirectoryID:           fields[1],
			LogEndOffset:          logEndOffset,
			Lag:                   lag,
			LastFetchTimestamp:    lastFetchTs,
			LastCaughtUpTimestamp: lastCaughtUpTs,
			Status:                fields[6],
		})
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	if !headerSeen {
		return nil, fmt.Errorf("replication header not found in output")
	}

	return result, nil
}
