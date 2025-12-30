package types

import "time"

type CandidateKind string

const (
	CandidateMongo CandidateKind = "mongo"
	CandidateKafka CandidateKind = "kafka"
)

type CandidateReport struct {
	ID   string            `json:"id"`
	Kind CandidateKind     `json:"kind"`
	Host string            `json:"host"`
	Addr string            `json:"addr,omitempty"` // optional (mongo/kafka listen addr)
	Meta map[string]string `json:"meta,omitempty"`

	MongoMemberID *int `json:"mongoMemberId,omitempty"`

	// Offline probe summary
	LastSeenReplicaSetID   string    `json:"lastSeenReplicaSetId,omitempty"`
	LastSeenReplicaSetUUID string    `json:"lastSeenReplicaSetUuid,omitempty"`
	LastTerm               int64     `json:"lastTerm,omitempty"`
	LastOplogTs            time.Time `json:"lastOplogTs,omitempty"`

	KafkaClusterID      string `json:"kafkaClusterId,omitempty"`
	KafkaNodeID         string `json:"kafkaNodeId,omitempty"`
	KafkaBrokerAddr     string `json:"kafkaBrokerAddr,omitempty"`
	KafkaControllerAddr string `json:"kafkaControllerAddr,omitempty"`
	KafkaStorageID      string `json:"kafkaStorageId,omitempty"`

	Eligible bool   `json:"eligible"`
	Reason   string `json:"reason,omitempty"`

	UpdatedAt time.Time `json:"updatedAt"`
}

type HealthStatus struct {
	ID          string            `json:"id"`
	Healthy     bool              `json:"healthy"`
	Reason      string            `json:"reason,omitempty"`
	Note        string            `json:"note,omitempty"`
	ServiceMeta map[string]string `json:"serviceMeta,omitempty"`
	UpdatedAt   time.Time         `json:"updatedAt"`
}

type ReplicaSpec struct {
	Kind      CandidateKind `json:"kind"`
	Members   []string      `json:"members"` // candidate IDs
	UpdatedAt time.Time     `json:"updatedAt"`
	Version   int64         `json:"version"`

	// Mongo-specific hints
	MongoReplicaSetID   string          `json:"mongoReplicaSetId,omitempty"`
	MongoReplicaSetUUID string          `json:"mongoReplicaSetUuid,omitempty"`
	MongoWipeMembers    map[string]bool `json:"mongoWipeMembers,omitempty"`

	// Kafka-specific hints (used by provider/job templates)
	KafkaMode                 string   `json:"kafkaMode,omitempty"` // "combined"
	KafkaDynamicVoter         bool     `json:"kafkaDynamicVoter,omitempty"`
	KafkaBootstrapControllers []string `json:"kafkaBootstrapControllers,omitempty"` // controller.quorum.bootstrap.controllers
	KafkaBootstrapServers     []string `json:"kafkaBootstrapServers,omitempty"`     // controller.quorum.bootstrap.servers
	// Directory IDs per controller for remove-controller operations.
	KafkaControllerDirectoryIDs map[string]string `json:"kafkaControllerDirectoryIds,omitempty"`
	KafkaMemberIDs              map[string]string `json:"kafkaMemberIds,omitempty"`
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
