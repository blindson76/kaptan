package main

import (
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"maps"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"
	"unsafe"

	"github.com/beevik/ntp"
	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	"github.com/qmuntal/stateless"
	"golang.org/x/sys/windows"
	yaml "gopkg.in/yaml.v3"
)

type NodeInfo struct {
	ID     int
	NodeID string
	Raft   string
	HTTP   string
}

var allNodes map[int]NodeInfo

type Peer struct {
	ID       string `yaml:"id"`
	RaftAddr string `yaml:"raft_addr"`
	HttpAddr string `yaml:"http_addr"`
}

type Config struct {
	Peers             []Peer   `yaml:"peers"`
	ExtSources        []string `yaml:"ext_sources"`
	BootstrapExpected int      `yaml:"bootstrap_expected"`
	BootstrapDelay    int      `yaml:"bootstrap_delay"`
	StoragePath       string   `yaml:"storage_path"`
	CallbackCommand   string   `yaml:"callback_command"`
}

// Windows SYSTEMTIME yapısı
type systemTime struct {
	wYear         uint16
	wMonth        uint16
	wDayOfWeek    uint16
	wDay          uint16
	wHour         uint16
	wMinute       uint16
	wSecond       uint16
	wMilliseconds uint16
}

var (
	modkernel32       = syscall.NewLazyDLL("kernel32.dll")
	procSetSystemTime = modkernel32.NewProc("SetSystemTime")
)

type CommandType string

const (
	CmdReport   CommandType = "report"
	CmdDecision CommandType = "decision"
	CmdOrder    CommandType = "order"
)

type TimeModeType string

const (
	TimeAuto               TimeModeType = "auto"
	TimeManualTimeModeType              = "manual"
	TimeHandover           TimeModeType = "handover"
)

type OrderStatusType string

const (
	OrderApplied OrderStatusType = "applied"
	OrderPending OrderStatusType = "pending"
)

type Command struct {
	Type     CommandType `json:"type"`
	Report   *Report     `json:"report,omitempty"`
	Decision *Decision   `json:"decision,omitempty"`
	Order    *Order      `json:"order,omitempty"`
}

type Report struct {
	NodeID         string            `json:"node_id"`
	NodeNo         int               `json:"node_no"`
	Status         string            `json:"status"`
	Timestamp      int64             `json:"timestamp"`
	Meta           map[string]string `json:"meta,omitempty"`
	TimeMode       TimeModeType      `json:"time_mode"`
	OrderIndex     int               `json:"order_index"`
	ManulTimeDelta int64             `json:"manual_time_delta,omitempty"`
	OrderStatus    OrderStatusType   `json:"order_status"`
}

type Decision struct {
	ID        string            `json:"id"`
	Reason    string            `json:"reason"`
	CreatedAt int64             `json:"created_at"`
	Reports   map[string]Report `json:"reports"`
	Orders    map[string]Order  `json:"orders"`
	Target    string            `json:"target"`
}

type Order struct {
	NodeID  string `json:"node_id"`
	Action  string `json:"action"`
	Payload string `json:"payload,omitempty"`
}

type ClusterState struct {
	Reports  map[string]Report `json:"reports"`
	Decision *Decision         `json:"decision,omitempty"`
}

func NewClusterState() ClusterState {
	return ClusterState{
		Reports: map[string]Report{},
	}
}

type SimpleFSM struct {
	mu    sync.RWMutex
	state ClusterState
}

func NewSimpleFSM() *SimpleFSM {
	return &SimpleFSM{
		state: NewClusterState(),
	}
}

func (f *SimpleFSM) Apply(l *raft.Log) interface{} {
	var cmd Command
	if err := json.Unmarshal(l.Data, &cmd); err != nil {
		return err
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	switch cmd.Type {
	case CmdReport:
		if cmd.Report == nil {
			return errors.New("empty report")
		}

		// Idempotent: aynı node tekrar report gönderirse overwrite edilir.
		f.state.Reports[cmd.Report.NodeID] = *cmd.Report

		fmt.Printf("[FSM] report committed: node=%s report_count=%d\n",
			cmd.Report.NodeID,
			len(f.state.Reports),
		)

		return nil

	case CmdDecision:
		if cmd.Decision == nil {
			return errors.New("empty decision")
		}

		// Idempotent: karar bir kere commit edilir.
		if f.state.Decision != nil {
			fmt.Printf("[FSM] decision already exists, ignored: %s\n", f.state.Decision.ID)
			return nil
		}

		f.state.Decision = cmd.Decision

		fmt.Printf("[FSM] decision committed: id=%s order_count=%d\n",
			cmd.Decision.ID,
			len(cmd.Decision.Orders),
		)

		return nil

	case CmdOrder:
		return nil

	default:
		return fmt.Errorf("unknown command type: %s", cmd.Type)
	}
}

func (f *SimpleFSM) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	data, err := json.Marshal(f.state)
	if err != nil {
		return nil, err
	}

	return &SimpleSnapshot{data: data}, nil
}

func (f *SimpleFSM) Restore(rc io.ReadCloser) error {
	defer rc.Close()

	var s ClusterState
	if err := json.NewDecoder(rc).Decode(&s); err != nil {
		return err
	}

	if s.Reports == nil {
		s.Reports = map[string]Report{}
	}

	f.mu.Lock()
	f.state = s
	f.mu.Unlock()

	return nil
}

func (f *SimpleFSM) GetState() ClusterState {
	f.mu.RLock()
	defer f.mu.RUnlock()

	b, _ := json.Marshal(f.state)

	var cp ClusterState
	_ = json.Unmarshal(b, &cp)

	if cp.Reports == nil {
		cp.Reports = map[string]Report{}
	}

	return cp
}

type SimpleSnapshot struct {
	data []byte
}

func (s *SimpleSnapshot) Persist(sink raft.SnapshotSink) error {
	if len(s.data) > 0 {
		if _, err := sink.Write(s.data); err != nil {
			_ = sink.Cancel()
			return err
		}
	}

	return sink.Close()
}

func (s *SimpleSnapshot) Release() {}

type LocalState string
type Trigger string

const (
	WorkerInitial       LocalState = "initial"
	WorkerCollectReport LocalState = "collect_report"
	WorkerWorking       LocalState = "working"

	LeaderWaitReports LocalState = "wait_reports"
	LeaderDecision    LocalState = "decision"
	LeaderMonitoring  LocalState = "monitoring"

	TriggerJoinedCluster     Trigger = "joined_cluster"
	TriggerSelfReport        Trigger = "self_report"
	TriggerReportCountEnough Trigger = "report_count_enough"
	TriggerDecisionPublished Trigger = "decision_published"
	TriggerOrder             Trigger = "order"
)

type App struct {
	nodeIdx int
	nodeID  string
	baseDir string

	raft *raft.Raft
	fsm  *SimpleFSM

	workerSM *stateless.StateMachine
	leaderSM *stateless.StateMachine

	leaderNotifyCh chan bool

	detectedMu    sync.Mutex
	detectedNodes map[string]NodeInfo

	appliedMu       sync.Mutex
	appliedDecision map[string]bool

	cfg *Config
}

func NewApp(
	nodeIdx int,
	baseDir string,
	r *raft.Raft,
	fsm *SimpleFSM,
	leaderNotifyCh chan bool,
	cfg *Config,
) *App {
	app := &App{
		nodeIdx:         nodeIdx,
		nodeID:          fmt.Sprintf("node-%d", nodeIdx),
		baseDir:         baseDir,
		raft:            r,
		fsm:             fsm,
		leaderNotifyCh:  leaderNotifyCh,
		detectedNodes:   map[string]NodeInfo{},
		appliedDecision: map[string]bool{},
		cfg:             cfg,
	}

	app.configureWorkerSM()
	app.configureLeaderSM()

	return app
}

func (a *App) configureWorkerSM() {
	sm := stateless.NewStateMachine(WorkerInitial)

	sm.Configure(WorkerInitial).
		Permit(TriggerJoinedCluster, WorkerCollectReport)

	sm.Configure(WorkerCollectReport).
		OnEntry(func(_ context.Context, _ ...any) error {
			fmt.Printf("[%s][worker-sm] collect_report -> publish_report\n", a.nodeID)
			go a.publishSelfReportUntilSuccess()
			return nil
		}).
		Permit(TriggerOrder, WorkerWorking)

	sm.Configure(WorkerWorking).
		OnEntry(func(_ context.Context, _ ...any) error {
			fmt.Printf("[%s][worker-sm] working\n", a.nodeID)
			return nil
		}).
		Ignore(TriggerOrder)

	a.workerSM = sm
}

func (a *App) configureLeaderSM() {
	sm := stateless.NewStateMachine(LeaderWaitReports)

	sm.Configure(LeaderWaitReports).
		Ignore(TriggerSelfReport).
		Permit(TriggerReportCountEnough, LeaderDecision)

	sm.Configure(LeaderDecision).
		OnEntry(func(_ context.Context, _ ...any) error {
			fmt.Printf("[%s][leader-sm] decision\n", a.nodeID)
			return nil
		}).
		Permit(TriggerDecisionPublished, LeaderMonitoring)

	sm.Configure(LeaderMonitoring).
		OnEntry(func(_ context.Context, _ ...any) error {
			fmt.Printf("[%s][leader-sm] monitoring\n", a.nodeID)
			return nil
		}).
		Ignore(TriggerSelfReport).
		Ignore(TriggerReportCountEnough).
		Ignore(TriggerDecisionPublished)

	a.leaderSM = sm
}

func (a *App) fireWorker(t Trigger) {
	if err := a.workerSM.Fire(t); err != nil {
		fmt.Printf("[%s][worker-sm] trigger ignored: %s err=%v\n", a.nodeID, t, err)
	}
}

func (a *App) fireLeader(t Trigger) {
	if err := a.leaderSM.Fire(t); err != nil {
		fmt.Printf("[%s][leader-sm] trigger ignored: %s err=%v\n", a.nodeID, t, err)
	}
}

func (a *App) applyCommand(cmd Command) error {
	data, err := json.Marshal(cmd)
	if err != nil {
		return err
	}

	future := a.raft.Apply(data, 5*time.Second)
	if err := future.Error(); err != nil {
		return err
	}

	if resp := future.Response(); resp != nil {
		if err, ok := resp.(error); ok {
			return err
		}
	}

	return nil
}

func (a *App) publishSelfReportUntilSuccess() {
	for {
		err := a.publishSelfReport()
		if err == nil {
			fmt.Printf("[%s] self_report published\n", a.nodeID)
			return
		}

		fmt.Printf("[%s] self_report failed: %v\n", a.nodeID, err)
		time.Sleep(1 * time.Second)
	}
}

func (a *App) publishSelfReport() error {

	report := Report{
		NodeID:    a.nodeID,
		NodeNo:    a.nodeIdx,
		Status:    "ready",
		Timestamp: time.Now().Unix(),
		Meta: map[string]string{
			"raft": allNodes[a.nodeIdx].Raft,
			"http": allNodes[a.nodeIdx].HTTP,
		},
	}
	file, err := os.Open(a.cfg.StoragePath)
	if err != nil {
		fmt.Printf("No existing configuration found\n")
	}
	defer file.Close()
	var extCfg Report
	decoder := json.NewDecoder(file)
	if err := decoder.Decode(&extCfg); err == nil {
		report.ManulTimeDelta = extCfg.ManulTimeDelta
		report.OrderIndex = extCfg.OrderIndex
		report.OrderStatus = extCfg.OrderStatus
		report.TimeMode = extCfg.TimeMode
	} else {
		fmt.Printf("Failed when parsing existing configuration\n")
	}

	if sources, err := availableNTPSources(a.cfg.ExtSources); err != nil {
		fmt.Printf("Error while checkking ext sources\n")
	} else if len(sources) > 0 {
		report.Meta["sources"] = strings.Join(sources, ",")
	}

	if a.raft.State() == raft.Leader {
		return a.applyCommand(Command{
			Type:   CmdReport,
			Report: &report,
		})
	}

	leaderHTTP := a.leaderHTTP()
	if leaderHTTP == "" {
		return errors.New("leader unknown")
	}

	body, _ := json.Marshal(report)

	resp, err := http.Post("http://"+leaderHTTP+"/report", "application/json", bytes.NewReader(body))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode/100 != 2 {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("leader response: %s %s", resp.Status, string(b))
	}

	return nil
}

func (a *App) leaderHTTP() string {
	leaderRaft := string(a.raft.Leader())
	if leaderRaft == "" {
		return ""
	}

	for _, n := range allNodes {
		if n.Raft == leaderRaft {
			return n.HTTP
		}
	}

	return ""
}

func (a *App) leaderDecisionLoop() {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for range ticker.C {
		if a.raft.State() != raft.Leader {
			continue
		}

		s := a.fsm.GetState()

		if s.Decision != nil {
			a.fireLeader(TriggerDecisionPublished)
			continue
		}

		if len(s.Reports) < a.cfg.BootstrapExpected {
			continue
		}

		a.fireLeader(TriggerReportCountEnough)

		decision := buildDecision(s.Reports)

		fmt.Printf("[%s][leader] publishing decision: %s\n", a.nodeID, decision.ID)

		err := a.applyCommand(Command{
			Type:     CmdDecision,
			Decision: &decision,
		})
		if err != nil {
			fmt.Printf("[%s][leader] decision apply failed: %v\n", a.nodeID, err)
			continue
		}

		a.fireLeader(TriggerDecisionPublished)
	}
}

func buildDecision(reports map[string]Report) Decision {

	reportSlice := slices.Collect(maps.Values(reports))

	slices.SortFunc(reportSlice, func(a, b Report) int {
		return -cmp.Compare(a.OrderIndex, b.OrderIndex)
	})
	recent := reportSlice[0]

	orders := map[string]Order{}

	// Static registry kullanıldığı için tüm 6 node için order üret.
	// Böylece 4/5/6 sonradan gelse bile decision içinde kendi order'ını bulur.
	for _, node := range orderedAllNodes() {
		orders[node.NodeID] = Order{
			NodeID:  node.NodeID,
			Action:  "start_work",
			Payload: "cluster_ready",
		}
	}

	reportsCopy := map[string]Report{}
	for k, v := range reports {
		reportsCopy[k] = v
	}

	return Decision{
		ID:        fmt.Sprintf("decision-%d", time.Now().UnixNano()),
		Reason:    "enough reports received",
		CreatedAt: time.Now().Unix(),
		Reports:   reportsCopy,
		Orders:    orders,
		Target:    recent.NodeID,
	}
}

func (a *App) workerOrderLoop() {
	fmt.Printf("WorkerLoop started\n")
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for range ticker.C {
		s := a.fsm.GetState()
		if s.Decision == nil {
			continue
		}

		order, ok := s.Decision.Orders[a.nodeID]
		if !ok {
			continue
		}

		if a.isDecisionApplied(s.Decision.ID) {
			continue
		}
		fmt.Printf("Order:%+v\n", order)

		if err := a.applyOrderIdempotent(s.Decision.ID, order); err != nil {
			fmt.Printf("[%s][order] apply failed: decision=%s err=%v\n",
				a.nodeID,
				s.Decision.ID,
				err,
			)
			continue
		}

		a.markDecisionApplied(s.Decision.ID)
		a.fireWorker(TriggerOrder)
	}
}

func (a *App) isDecisionApplied(decisionID string) bool {
	a.appliedMu.Lock()
	defer a.appliedMu.Unlock()

	if a.appliedDecision[decisionID] {
		return true
	}

	marker := a.orderMarkerPath(decisionID)
	if _, err := os.Stat(marker); err == nil {
		a.appliedDecision[decisionID] = true
		return true
	}

	return false
}

func (a *App) markDecisionApplied(decisionID string) {
	a.appliedMu.Lock()
	defer a.appliedMu.Unlock()

	a.appliedDecision[decisionID] = true
}

func (a *App) orderMarkerPath(decisionID string) string {
	return filepath.Join(a.baseDir, "applied_orders", decisionID+".done")
}

func (a *App) applyOrderIdempotent(decisionID string, order Order) error {
	marker := a.orderMarkerPath(decisionID)

	if _, err := os.Stat(marker); err == nil {
		fmt.Printf("[%s][order] already applied: decision=%s\n", a.nodeID, decisionID)
		return nil
	}

	if err := os.MkdirAll(filepath.Dir(marker), 0755); err != nil {
		return err
	}

	fmt.Printf("[%s][order] apply: decision=%s action=%s payload=%s\n",
		a.nodeID,
		decisionID,
		order.Action,
		order.Payload,
	)

	switch order.Action {
	case "start_work":
		// Gerçek iş burada başlatılır.
		// Örnek:
		// exec.Command("cmd", "/c", "start_worker.bat").Run()

	default:
		return fmt.Errorf("unknown order action: %s", order.Action)
	}

	return os.WriteFile(marker, []byte(time.Now().Format(time.RFC3339Nano)), 0644)
}

func (a *App) StartHTTP() {
	mux := http.NewServeMux()

	mux.HandleFunc("/report", a.handleReport)
	mux.HandleFunc("/state", a.handleState)
	mux.HandleFunc("/order", a.handleOrder)

	addr := allNodes[a.nodeIdx].HTTP

	go func() {
		fmt.Printf("[%s][http] listening: %s\n", a.nodeID, addr)
		if err := http.ListenAndServe(addr, mux); err != nil {
			log.Fatal(err)
		}
	}()
}

func (a *App) handleReport(w http.ResponseWriter, r *http.Request) {
	if a.raft.State() != raft.Leader {
		leaderHTTP := a.leaderHTTP()
		if leaderHTTP == "" {
			http.Error(w, "leader unknown", http.StatusServiceUnavailable)
			return
		}

		http.Redirect(w, r, "http://"+leaderHTTP+"/report", http.StatusTemporaryRedirect)
		return
	}

	var report Report
	if err := json.NewDecoder(r.Body).Decode(&report); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	err := a.applyCommand(Command{
		Type:   CmdReport,
		Report: &report,
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	a.fireLeader(TriggerSelfReport)

	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

func (a *App) handleState(w http.ResponseWriter, r *http.Request) {
	resp := map[string]any{
		"node_id":     a.nodeID,
		"raft_state":  a.raft.State().String(),
		"leader_raft": string(a.raft.Leader()),
		"leader_http": a.leaderHTTP(),
		"cluster":     a.fsm.GetState(),
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}
func (a *App) handleOrder(w http.ResponseWriter, r *http.Request) {
	if a.raft.State() != raft.Leader {
		leaderHTTP := a.leaderHTTP()
		if leaderHTTP == "" {
			http.Error(w, "leader unknown", http.StatusServiceUnavailable)
			return
		}

		http.Redirect(w, r, "http://"+leaderHTTP+"/report", http.StatusTemporaryRedirect)
		return
	}
	if r.Method != http.MethodPost {
		http.Error(w, "invalid method", http.StatusBadRequest)
		return
	}

	var decision Decision
	if err := json.NewDecoder(r.Body).Decode(&decision); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	fmt.Printf("[leader] applying order: %+v\n", decision)
	err := a.applyCommand(Command{
		Type:     CmdDecision,
		Decision: &decision,
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}
func (a *App) rememberDetectedNode(node NodeInfo) {
	a.detectedMu.Lock()
	defer a.detectedMu.Unlock()

	a.detectedNodes[node.NodeID] = node
}

func (a *App) voterReconcileLoop() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		a.reconcileVotersOnce()
	}
}

func (a *App) reconcileVotersOnce() {
	if a.raft.State() != raft.Leader {
		return
	}

	a.detectedMu.Lock()
	nodes := make([]NodeInfo, 0, len(a.detectedNodes))
	for _, n := range a.detectedNodes {
		nodes = append(nodes, n)
	}
	a.detectedMu.Unlock()

	sort.Slice(nodes, func(i, j int) bool {
		return nodes[i].ID < nodes[j].ID
	})

	cfgFuture := a.raft.GetConfiguration()
	if err := cfgFuture.Error(); err != nil {
		fmt.Printf("[%s][reconcile] get configuration failed: %v\n", a.nodeID, err)
		return
	}

	existing := map[raft.ServerID]bool{}
	for _, s := range cfgFuture.Configuration().Servers {
		existing[s.ID] = true
	}

	for _, node := range nodes {
		id := raft.ServerID(node.NodeID)

		if existing[id] {
			continue
		}

		fmt.Printf("[%s][reconcile] AddVoter: %s %s\n",
			a.nodeID,
			node.NodeID,
			node.Raft,
		)

		f := a.raft.AddVoter(id, raft.ServerAddress(node.Raft), 0, 5*time.Second)
		if err := f.Error(); err != nil {
			fmt.Printf("[%s][reconcile] AddVoter failed: node=%s err=%v\n",
				a.nodeID,
				node.NodeID,
				err,
			)
			continue
		}

		fmt.Printf("[%s][reconcile] AddVoter OK: %s\n", a.nodeID, node.NodeID)

		existing[id] = true
	}
}

func (a *App) leaderChangeLoop() {
	for isLeader := range a.leaderNotifyCh {
		if isLeader {
			fmt.Printf("[%s][raft] became leader\n", a.nodeID)

			// Yeni leader olur olmaz eksik voter'ları tamamlamayı dene.
			go a.reconcileVotersOnce()

			// Bu node'un report'u yoksa tekrar report göndermeyi dene.
			go a.publishSelfReportUntilSuccess()
		} else {
			fmt.Printf("[%s][raft] lost leadership / follower\n", a.nodeID)
		}
	}
}

func MonitorCluster(nodes map[int]NodeInfo, BootstrapExpect int) (<-chan []NodeInfo, <-chan NodeInfo) {
	bootstrapChan := make(chan []NodeInfo, 1)
	newNodeChan := make(chan NodeInfo, 64)

	go func() {
		seen := map[int]bool{}
		bootstrapSent := false

		for {
			ready := scanReadyNodes(nodes)

			for _, node := range ready {
				if !seen[node.ID] {
					seen[node.ID] = true

					if bootstrapSent {
						newNodeChan <- node
					}
				}
			}

			if !bootstrapSent && len(ready) >= BootstrapExpect {
				bootstrapSent = true

				bootstrapNodes := make([]NodeInfo, BootstrapExpect)
				copy(bootstrapNodes, ready[:BootstrapExpect])

				bootstrapChan <- bootstrapNodes

				for i := BootstrapExpect; i < len(ready); i++ {
					newNodeChan <- ready[i]
				}
			}

			time.Sleep(500 * time.Millisecond)
		}
	}()

	return bootstrapChan, newNodeChan
}

func scanReadyNodes(nodes map[int]NodeInfo) []NodeInfo {
	ordered := orderedNodesFromMap(nodes)

	var ready []NodeInfo

	for _, node := range ordered {
		if tcpReady(node.Raft, 250*time.Millisecond) {
			ready = append(ready, node)
		}
	}

	return ready
}

func orderedAllNodes() []NodeInfo {
	return orderedNodesFromMap(allNodes)
}

func orderedNodesFromMap(nodes map[int]NodeInfo) []NodeInfo {
	ids := make([]int, 0, len(nodes))
	for id := range nodes {
		ids = append(ids, id)
	}

	sort.Ints(ids)

	result := make([]NodeInfo, 0, len(ids))
	for _, id := range ids {
		result = append(result, nodes[id])
	}

	return result
}

func tcpReady(addr string, timeout time.Duration) bool {
	conn, err := net.DialTimeout("tcp", addr, timeout)
	if err != nil {
		return false
	}

	_ = conn.Close()
	return true
}
func loadConfig(configPath string) (*Config, error) {
	// 2. Dosya içeriğini byte dizisi olarak oku
	yamlFile, err := os.ReadFile(configPath)
	if err != nil {
		log.Fatalf("Dosya okuma hatası (%s): %v", configPath, err)
	}

	var config Config

	// 3. Dosyadan okunan veriyi struct içine aktar
	err = yaml.Unmarshal(yamlFile, &config)
	if err != nil {
		return nil, err
	}
	return &config, nil
}
func isNTPAvailable(addr string) (bool, error) {
	options := ntp.QueryOptions{
		Timeout: 2 * time.Second,
		Port:    123,
	}
	if response, err := ntp.QueryWithOptions(addr, options); err != nil {
		return false, err
	} else if response.Leap == ntp.LeapNoWarning {
		return true, nil
	}
	return false, nil
}

func availableNTPSources(servers []string) ([]string, error) {
	var wg sync.WaitGroup
	var availableServers []string
	wg.Add(len(servers))
	for _, server := range servers {
		go func() {
			available, err := isNTPAvailable(server)
			if err == nil && available {
				availableServers = append(availableServers, server)
			}
			wg.Done()
		}()
	}
	wg.Wait()
	return availableServers, nil
}

// enableSystemTimePrivilege işlem token'ında SeSystemtimePrivilege yetkisini açar.
func enableSystemTimePrivilege() error {
	// 1. Mevcut işlemin (process) token'ını aç
	var token windows.Token
	currentProcess := windows.CurrentProcess()
	err := windows.OpenProcessToken(currentProcess, windows.TOKEN_ADJUST_PRIVILEGES|windows.TOKEN_QUERY, &token)
	if err != nil {
		return fmt.Errorf("process token açılamadı: %v", err)
	}
	defer token.Close()

	// 2. SeSystemtimePrivilege için LUID (Locally Unique Identifier) değerini bul
	var luid windows.LUID
	privName := windows.StringToUTF16Ptr("SeSystemtimePrivilege")
	err = windows.LookupPrivilegeValue(nil, privName, &luid)
	if err != nil {
		return fmt.Errorf("privilege LUID bulunamadı: %v", err)
	}

	// 3. TokenPrivileges yapısını hazırla ve yetkiyi aktif (ENABLED) et
	tokenPrivileges := windows.Tokenprivileges{
		PrivilegeCount: 1,
		Privileges: [1]windows.LUIDAndAttributes{
			{
				Luid:       luid,
				Attributes: windows.SE_PRIVILEGE_ENABLED,
			},
		},
	}

	// 4. Token ayrıcalıklarını güncelle
	err = windows.AdjustTokenPrivileges(token, false, &tokenPrivileges, 0, nil, nil)
	if err != nil {
		return fmt.Errorf("token ayrıcalığı güncellenemedi: %v", err)
	}

	return nil
}

// AdjustSystemTimeWithPrivilege önce gerekli izni alır, ardından zamanı günceller.
func AdjustSystemTimeWithPrivilege(delta time.Duration) error {
	// Gerekli Windows token ayrıcalığını al
	err := enableSystemTimePrivilege()
	if err != nil {
		return fmt.Errorf("yetki yükseltme hatası: %v", err)
	}

	// Zaman hesaplama (UTC)
	newTime := time.Now().UTC().Add(delta)
	st := systemTime{
		wYear:         uint16(newTime.Year()),
		wMonth:        uint16(newTime.Month()),
		wDayOfWeek:    uint16(newTime.Weekday()),
		wDay:          uint16(newTime.Day()),
		wHour:         uint16(newTime.Hour()),
		wMinute:       uint16(newTime.Minute()),
		wSecond:       uint16(newTime.Second()),
		wMilliseconds: uint16(newTime.Nanosecond() / int(time.Millisecond)),
	}

	// Windows API çağrısı
	r1, _, errCall := procSetSystemTime.Call(uintptr(unsafe.Pointer(&st)))
	if r1 == 0 {
		return fmt.Errorf("SetSystemTime başarısız oldu: %v", errCall)
	}

	return nil
}
func main() {

	configPath := flag.String("config", "config.yaml", "Konfigürasyon dosyasının yolu")
	nodeName := flag.String("node-name", "", "Node name")

	flag.Parse()

	appCfg, err := loadConfig(*configPath)
	if err != nil {
		log.Panicf("Loading config error:%v", err)
	}
	allNodes = make(map[int]NodeInfo)
	nodeIdx := -1
	for i, peer := range appCfg.Peers {
		allNodes[i] = NodeInfo{
			ID:     i,
			NodeID: peer.ID,
			Raft:   peer.RaftAddr,
			HTTP:   peer.HttpAddr,
		}
		if *nodeName == peer.ID {
			nodeIdx = i
		}
	}
	if nodeIdx < 0 {
		log.Panicf("Couldnt found %s in peers", *nodeName)
	}

	info, ok := allNodes[nodeIdx]
	if !ok {
		log.Panicf("Couldnt found %s in peers", *nodeName)
	}

	nodeID := info.NodeID
	bindAddr := info.Raft

	_ = stateless.NewStateMachineWithExternalStorage(
		func(ctx context.Context) (stateless.State, error) {
			return nil, nil
		},
		func(ctx context.Context, s stateless.State) error {
			return nil
		},
		stateless.FiringImmediate,
	)

	baseDir := filepath.Join(".", "raft_data", nodeID)

	// Senin şartın: her açılış fresh store.
	_ = os.RemoveAll(baseDir)
	_ = os.MkdirAll(baseDir, 0755)

	fmt.Printf("[%s] Fresh Store ile başlatıldı. Raft=%s HTTP=%s\n",
		nodeID,
		info.Raft,
		info.HTTP,
	)

	leaderNotifyCh := make(chan bool, 16)

	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(nodeID)
	config.LogLevel = "none"
	config.Logger = hclog.NewNullLogger()
	config.NotifyCh = leaderNotifyCh

	advertiseAddr, err := net.ResolveTCPAddr("tcp", bindAddr)
	if err != nil {
		panic(err)
	}

	transport, err := raft.NewTCPTransport(bindAddr, advertiseAddr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		panic(err)
	}

	logStore := raft.NewInmemStore()
	stableStore := raft.NewInmemStore()
	snapshotStore := raft.NewInmemSnapshotStore()

	fsm := NewSimpleFSM()

	r, err := raft.NewRaft(config, fsm, logStore, stableStore, snapshotStore, transport)
	if err != nil {
		panic(err)
	}

	app := NewApp(nodeIdx, baseDir, r, fsm, leaderNotifyCh, appCfg)

	app.StartHTTP()

	go app.leaderChangeLoop()
	go app.leaderDecisionLoop()
	go app.workerOrderLoop()
	go app.voterReconcileLoop()

	bootstrapChan, newNodeChan := MonitorCluster(allNodes, appCfg.BootstrapExpected)

	go func() {
		for node := range newNodeChan {
			app.rememberDetectedNode(node)
			fmt.Printf("[%s][monitor] detected node: %s raft=%s\n",
				nodeID,
				node.NodeID,
				node.Raft,
			)
		}
	}()

	fmt.Printf("[Sistem] İlk %d node'un hazır olması bekleniyor...\n", appCfg.BootstrapExpected)

	readyNodes := <-bootstrapChan

	fmt.Printf("\n[Bootstrap] %d node hazır. Ek %d saniye bekleniyor...\n", len(readyNodes), appCfg.BootstrapDelay)

	// Bootstrap öncesi 3 saniye daha bekle.
	// Bu sürede gelen node'lar da bootstrap config'e dahil edilir.
	bootstrapSet := map[string]NodeInfo{}
	for _, node := range readyNodes {
		bootstrapSet[node.NodeID] = node
		app.rememberDetectedNode(node)
	}

	deadline := time.After(time.Duration(appCfg.BootstrapDelay) * time.Second)

bootstrapCollectLoop:
	for {
		select {
		case <-deadline:
			break bootstrapCollectLoop

		case node := <-newNodeChan:
			fmt.Printf("[Bootstrap] node bootstrap listesine eklendi: %s\n", node.NodeID)
			bootstrapSet[node.NodeID] = node
			app.rememberDetectedNode(node)
		}
	}

	finalBootstrapNodes := make([]NodeInfo, 0, len(bootstrapSet))
	for _, node := range bootstrapSet {
		finalBootstrapNodes = append(finalBootstrapNodes, node)
	}

	sort.Slice(finalBootstrapNodes, func(i, j int) bool {
		return finalBootstrapNodes[i].ID < finalBootstrapNodes[j].ID
	})

	fmt.Printf("[Bootstrap] Cluster başlatılıyor. Voter sayısı=%d\n", len(finalBootstrapNodes))

	for _, node := range finalBootstrapNodes {
		fmt.Printf(" - %s raft=%s http=%s\n", node.NodeID, node.Raft, node.HTTP)
	}

	var servers []raft.Server
	for _, node := range finalBootstrapNodes {
		servers = append(servers, raft.Server{
			Suffrage: raft.Voter,
			ID:       raft.ServerID(node.NodeID),
			Address:  raft.ServerAddress(node.Raft),
		})
	}

	cfg := raft.Configuration{Servers: servers}

	fmt.Printf("[%s] Bootstrapping\n", nodeID)

	future := r.BootstrapCluster(cfg)
	if err := future.Error(); err != nil {
		fmt.Printf("[%s] Bootstrap durumu: %v\n", nodeID, err)
	} else {
		fmt.Printf("[%s] Bootstrap done\n", nodeID)
	}

	fmt.Println("[Sistem] Worker state başlatılıyor...")

	// Leader seçimi ve cluster commit için kısa bekleme.
	time.Sleep(2 * time.Second)

	app.fireWorker(TriggerJoinedCluster)

	select {}
}
