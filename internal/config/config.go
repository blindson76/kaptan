package config

import (
	"errors"
	"fmt"
	"os"
	"regexp"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Log struct {
		Level string `yaml:"level"`
	} `yaml:"log"`

	Consul ConsulConfig `yaml:"consul"`
	Tasks  TasksConfig  `yaml:"tasks"`
}

type ConsulConfig struct {
	Address    string `yaml:"address"`
	Datacenter string `yaml:"datacenter"`
	Token      string `yaml:"token"`
	Prefix     string `yaml:"prefix"`
}

type TasksConfig struct {
	ServicesController ServicesControllerConfig `yaml:"services_controller"`
	ServicesAgent      ServicesAgentConfig      `yaml:"services_agent"`
	MongoController    MongoControllerConfig    `yaml:"mongo_controller"`
	MongoWorker        MongoWorkerConfig        `yaml:"mongo_worker"`
	MongoAgent         MongoAgentConfig         `yaml:"mongo_agent"`
	KafkaController    KafkaControllerConfig    `yaml:"kafka_controller"`
	KafkaWorker        KafkaWorkerConfig        `yaml:"kafka_worker"`
	KafkaAgent         KafkaAgentConfig         `yaml:"kafka_agent"`
}

type MongoControllerConfig struct {
	Enabled                   bool          `yaml:"enabled"`
	InstanceNumber            int           `yaml:"instance_number"`
	ControllerID              string        `yaml:"controller_id"`
	LockKey                   string        `yaml:"lock_key"`
	StateKey                  string        `yaml:"state_key"`
	CandidatesPrefix          string        `yaml:"candidates_prefix"`
	HealthPrefix              string        `yaml:"health_prefix"`
	SpecKey                   string        `yaml:"spec_key"`
	ReplicaSetID              string        `yaml:"replica_set_id"`
	ElectionInterval          time.Duration `yaml:"election_interval"`
	InitialSettleDuration     time.Duration `yaml:"initial_settle_duration"`
	AllowDegradedSingleMember bool          `yaml:"allow_degraded_single_member"`
}

type KafkaControllerConfig struct {
	Enabled                   bool          `yaml:"enabled"`
	InstanceNumber            int           `yaml:"instance_number"`
	ControllerID              string        `yaml:"controller_id"`
	LockKey                   string        `yaml:"lock_key"`
	StateKey                  string        `yaml:"state_key"`
	CandidatesPrefix          string        `yaml:"candidates_prefix"`
	HealthPrefix              string        `yaml:"health_prefix"`
	SpecKey                   string        `yaml:"spec_key"`
	ReplicaSetID              string        `yaml:"replica_set_id"`
	ElectionInterval          time.Duration `yaml:"election_interval"`
	InitialSettleDuration     time.Duration `yaml:"initial_settle_duration"`
	AllowDegradedSingleMember bool          `yaml:"allow_degraded_single_member"`
}

type MongoWorkerConfig struct {
	Enabled   bool   `yaml:"enabled"`
	WorkerID  string `yaml:"worker_id"`
	ReportKey string `yaml:"report_key"`

	MongodPath string `yaml:"mongod_path"`
	DBPath     string `yaml:"dbpath"`
	TempPort   int    `yaml:"temp_port"`

	AdminUser string `yaml:"admin_user"`
	AdminPass string `yaml:"admin_pass"`
}

type KafkaWorkerConfig struct {
	Enabled   bool     `yaml:"enabled"`
	WorkerID  string   `yaml:"worker_id"`
	ReportKey string   `yaml:"report_key"`
	MetaDirs  []string `yaml:"meta_dirs"`

	BrokerAddr     string `yaml:"broker_addr"`
	ControllerAddr string `yaml:"controller_addr"`
}

func Load(path string) (*Config, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	expanded, err := ExpandEnvLike(string(b))
	if err != nil {
		return nil, err
	}
	var cfg Config
	if err := yaml.Unmarshal([]byte(expanded), &cfg); err != nil {
		return nil, err
	}
	if cfg.Consul.Prefix == "" {
		cfg.Consul.Prefix = "replctl/v1"
	}
	if err := validate(&cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}

func validate(c *Config) error {
	if c.Consul.Address == "" {
		return errors.New("consul.address is required")
	}
	if c.Tasks.MongoController.Enabled {
		if c.Tasks.MongoController.InstanceNumber <= 0 {
			return errors.New("tasks.mongo_controller.instance_number must be > 0 when enabled")
		}
	}
	if c.Tasks.KafkaController.Enabled {
		if c.Tasks.KafkaController.InstanceNumber <= 0 {
			return errors.New("tasks.kafka_controller.instance_number must be > 0 when enabled")
		}
	}
	return nil
}

// ExpandEnvLike expands ${env.VAR} (and ${env:VAR}) inside config.
// - Unknown vars become empty string (same as many CI templaters).
// - It does NOT interpret backslashes, so Windows paths remain stable.
func ExpandEnvLike(in string) (string, error) {
	// ${env.VAR} or ${env:VAR}
	re := regexp.MustCompile(`\$\{env[.:]([A-Za-z_][A-Za-z0-9_]*)\}`)
	out := re.ReplaceAllStringFunc(in, func(m string) string {
		sub := re.FindStringSubmatch(m)
		if len(sub) != 2 {
			return m
		}
		return os.Getenv(sub[1])
	})
	// Also allow ${VAR} as fallback
	re2 := regexp.MustCompile(`\$\{([A-Za-z_][A-Za-z0-9_]*)\}`)
	out = re2.ReplaceAllStringFunc(out, func(m string) string {
		sub := re2.FindStringSubmatch(m)
		if len(sub) != 2 {
			return m
		}
		return os.Getenv(sub[1])
	})
	if strings.Contains(out, "${env.") || strings.Contains(out, "${env:") {
		return "", fmt.Errorf("unexpanded env placeholders remain in config (check syntax)")
	}
	return out, nil
}

type ServiceConfig struct {
	Enabled bool     `yaml:"enabled"`
	Address string   `yaml:"address"`
	Tags    []string `yaml:"tags"`
	TTL     string   `yaml:"ttl"`
}

type MongoAgentConfig struct {
	Enabled   bool   `yaml:"enabled"`
	AgentID   string `yaml:"agent_id"`
	OrdersKey string `yaml:"orders_key"`
	AckKey    string `yaml:"ack_key"`

	MongodPath  string `yaml:"mongod_path"`
	MongoshPath string `yaml:"mongosh_path"`
	DBPath      string `yaml:"dbpath"`
	BindIP      string `yaml:"bind_ip"`
	Port        int    `yaml:"port"`
	ReplSetName string `yaml:"replset_name"`
	LogPath     string `yaml:"log_path"`

	Service ServiceConfig `yaml:"service"`
}

type KafkaAgentConfig struct {
	Enabled   bool   `yaml:"enabled"`
	AgentID   string `yaml:"agent_id"`
	OrdersKey string `yaml:"orders_key"`
	AckKey    string `yaml:"ack_key"`

	KafkaBinDir    string `yaml:"kafka_bin_dir"`
	WorkDir        string `yaml:"work_dir"`
	LogDir         string `yaml:"log_dir"`
	MetaLogDir     string `yaml:"meta_log_dir"`
	BrokerAddr     string `yaml:"broker_addr"`
	ControllerAddr string `yaml:"controller_addr"`
	ClusterID      string `yaml:"cluster_id"`
	NodeID         string `yaml:"node_id"`

	Service ServiceConfig `yaml:"service"`
}

type ServiceInstanceRole string

const (
	ServiceRoleMaster ServiceInstanceRole = "master"
	ServiceRoleSlave  ServiceInstanceRole = "slave"
)

type ServiceStartSpec struct {
	Cmd     string   `yaml:"cmd"`
	Args    []string `yaml:"args"`
	WorkDir string   `yaml:"work_dir"`
}

type ServiceDef struct {
	Name      string   `yaml:"name"`
	Instances int      `yaml:"instances"` // should be 2
	Tags      []string `yaml:"tags"`
	TTL       string   `yaml:"ttl"`

	Start ServiceStartSpec `yaml:"start"`
}

type ServicesControllerConfig struct {
	Enabled        bool   `yaml:"enabled"`
	ControllerID   string `yaml:"controller_id"`
	InstanceNumber int    `yaml:"instance_number"`

	LockKey  string `yaml:"lock_key"`
	StateKey string `yaml:"state_key"`

	// Candidate sources used to spread across nodes
	KafkaCandidatesPrefix string `yaml:"kafka_candidates_prefix"`
	MongoCandidatesPrefix string `yaml:"mongo_candidates_prefix"`

	// Dependencies: wait until these services have >= MinPassing passing checks
	WaitFor    []string `yaml:"wait_for"`
	MinPassing int      `yaml:"min_passing"`

	Services []ServiceDef `yaml:"services"`

	ReconcileInterval time.Duration `yaml:"reconcile_interval"`
}

type ServicesAgentConfig struct {
	Enabled bool   `yaml:"enabled"`
	AgentID string `yaml:"agent_id"`

	// Watch this prefix for orders (the agent filters targetId==AgentID)
	OrdersPrefix string `yaml:"orders_prefix"`
	AckPrefix    string `yaml:"ack_prefix"`

	ServiceAddress string `yaml:"service_address"` // advertise address for service registration
}
