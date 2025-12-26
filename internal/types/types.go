package types

import "time"

type CandidateKind string

const (
	CandidateMongo CandidateKind = "mongo"
	CandidateKafka CandidateKind = "kafka"
)

type CandidateReport struct {
	ID   string        `json:"id"`
	Kind CandidateKind `json:"kind"`
	Host string        `json:"host"`
	Addr string        `json:"addr,omitempty"`
	Meta map[string]string `json:"meta,omitempty"`

	Eligible bool   `json:"eligible"`
	Reason   string `json:"reason,omitempty"`

	UpdatedAt time.Time `json:"updatedAt"`

	// Mongo hints (offline probe may fill these)
	MongoMemberID          *int      `json:"mongoMemberId,omitempty"`
	LastSeenReplicaSetID   string    `json:"lastSeenReplicaSetId,omitempty"`   // replSet name/id
	LastSeenReplicaSetUUID string    `json:"lastSeenReplicaSetUUID,omitempty"` // replSet UUID
	LastTerm               int64     `json:"lastTerm,omitempty"`
	LastOplogTs            time.Time `json:"lastOplogTs,omitempty"`

	// Kafka hints (offline probe may fill these)
	KafkaClusterID        string   `json:"kafkaClusterId,omitempty"` // from meta.properties cluster.id
	KafkaNodeID           string   `json:"kafkaNodeId,omitempty"`    // from meta.properties node.id
	KafkaBrokerAddr       string   `json:"kafkaBrokerAddr,omitempty"`
	KafkaControllerAddr   string   `json:"kafkaControllerAddr,omitempty"`
	KafkaMode             string   `json:"kafkaMode,omitempty"`             // "combined"
	KafkaDynamicVoter     bool     `json:"kafkaDynamicVoter,omitempty"`     // true for KRaft dynamic quorum
	KafkaBootstrapServers []string `json:"kafkaBootstrapServers,omitempty"` // controller.quorum.bootstrap.servers
}

type HealthStatus struct {
	ID          string            `json:"id"`
	Kind        CandidateKind     `json:"kind"`
	Healthy     bool              `json:"healthy"`
	Reason      string            `json:"reason,omitempty"`
	Note        string            `json:"note,omitempty"`
	ServiceMeta map[string]string `json:"serviceMeta,omitempty"`
	UpdatedAt   time.Time         `json:"updatedAt"`
}

type ReplicaSpec struct {
	Kind    CandidateKind `json:"kind"`
	Members []string      `json:"members"`

	// Monotonic version used as an "epoch" for orders.
	Version int64 `json:"version"`

	UpdatedAt time.Time `json:"updatedAt"`

	// Mongo
	MongoReplicaSetID   string          `json:"mongoReplicaSetId,omitempty"`   // replSet name
	MongoReplicaSetUUID string          `json:"mongoReplicaSetUUID,omitempty"` // replSet UUID (best-effort)
	MongoWipeMembers    map[string]bool `json:"mongoWipeMembers,omitempty"`    // per-member wipe request

	// Kafka
	KafkaMode             string   `json:"kafkaMode,omitempty"`         // "combined"
	KafkaDynamicVoter     bool     `json:"kafkaDynamicVoter,omitempty"` // true
	KafkaBootstrapServers []string `json:"kafkaBootstrapServers,omitempty"`
	KafkaClusterID        string   `json:"kafkaClusterId,omitempty"` // cluster.id for kafka-storage format
}

func (h HealthStatus) IsHealthy() bool  { return h.Healthy }
func (c CandidateReport) GetId() string { return c.ID }
func (c CandidateReport) Less(other CandidateReport) bool {
	// newest first
	return c.UpdatedAt.After(other.UpdatedAt)
}
func (h HealthStatus) GetId() string { return h.ID }
func (h HealthStatus) Less(other HealthStatus) bool {
	return h.UpdatedAt.After(other.UpdatedAt)
}
func (s ReplicaSpec) GetMembers() []string { return s.Members }
