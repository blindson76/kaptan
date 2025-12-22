package orders

import "time"

type Kind string
const (
    KindMongo Kind = "mongo"
    KindKafka Kind = "kafka"
    KindService Kind = "service"
)

type Action string
const (
    ActionWipe Action = "wipe"
    ActionStart Action = "start"
    ActionStop Action = "stop"
    ActionInit Action = "init"
    ActionReconfigure Action = "reconfigure"

    ActionAddVoter Action = "add_voter"
    ActionRemoveVoter Action = "remove_voter"
    ActionReassignPartitions Action = "reassign_partitions"
)

type Order struct {
    Kind Kind `json:"kind"`
    TargetID string `json:"targetId"`
    Action Action `json:"action"`
    Payload map[string]any `json:"payload,omitempty"`
    IssuedAt time.Time `json:"issuedAt"`
    Epoch int64 `json:"epoch"`
}

type Ack struct {
    TargetID string `json:"targetId"`
    Action Action `json:"action"`
    Epoch int64 `json:"epoch"`
    Ok bool `json:"ok"`
    Message string `json:"message,omitempty"`
    FinishedAt time.Time `json:"finishedAt"`
}
