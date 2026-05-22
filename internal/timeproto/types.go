package timeproto

import "time"

const (
	ModeAuto     = "auto"
	ModeHandover = "handover"
	ModeManual   = "manual"
)

type OrderPayload struct {
	OrderNo         int64    `json:"orderNo"`
	Mode            string   `json:"mode"`
	MasterAgentID   string   `json:"masterAgentId,omitempty"`
	ManualTime      string   `json:"manualTime,omitempty"`
	ExternalServers []string `json:"externalServers,omitempty"`
	Peers           []string `json:"peers,omitempty"`
	OrphanStratum   int      `json:"orphanStratum,omitempty"`
	GeneratedAt     string   `json:"generatedAt,omitempty"`
}

type LastOrderSnapshot struct {
	OrderNo    int64  `json:"orderNo"`
	Mode       string `json:"mode"`
	ManualTime string `json:"manualTime,omitempty"`
}

type AgentReport struct {
	AgentID                 string            `json:"agentId"`
	Mode                    string            `json:"mode"`
	LastOrder               LastOrderSnapshot `json:"lastOrder"`
	ExternalSourceReachable bool              `json:"externalSourceReachable"`
	MasterCandidateScore    int64             `json:"masterCandidateScore,omitempty"`
	UpdatedAt               time.Time         `json:"updatedAt"`
	Note                    string            `json:"note,omitempty"`
}

type OperatorRequest struct {
	Mode       string    `json:"mode"`
	ManualTime string    `json:"manualTime,omitempty"`
	UpdatedAt  time.Time `json:"updatedAt"`
}

type ControllerState struct {
	Mode              string    `json:"mode"`
	MasterAgentID     string    `json:"masterAgentId,omitempty"`
	LastOrderNo       int64     `json:"lastOrderNo"`
	LastDecisionHash  string    `json:"lastDecisionHash,omitempty"`
	LastManualTime    string    `json:"lastManualTime,omitempty"`
	ExternalDownSince time.Time `json:"externalDownSince,omitempty"`
	ExternalUpSince   time.Time `json:"externalUpSince,omitempty"`
	UpdatedAt         time.Time `json:"updatedAt"`
}
