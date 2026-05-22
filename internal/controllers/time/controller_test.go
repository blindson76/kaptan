package timectl

import (
	"testing"
	"time"

	"github.com/umitbozkurt/consul-replctl/internal/timeproto"
)

func TestNewestOrderModeSelectsHighestOrder(t *testing.T) {
	mode, manual := newestOrderMode([]timeproto.AgentReport{
		{
			AgentID:   "a",
			UpdatedAt: time.Unix(100, 0),
			LastOrder: timeproto.LastOrderSnapshot{OrderNo: 2, Mode: timeproto.ModeAuto},
		},
		{
			AgentID:   "b",
			UpdatedAt: time.Unix(50, 0),
			LastOrder: timeproto.LastOrderSnapshot{OrderNo: 3, Mode: timeproto.ModeManual, ManualTime: "2026-01-01T00:00:00Z"},
		},
	}, timeproto.ModeAuto)
	if mode != timeproto.ModeManual {
		t.Fatalf("expected manual mode, got %s", mode)
	}
	if manual != "2026-01-01T00:00:00Z" {
		t.Fatalf("unexpected manual time: %s", manual)
	}
}

func TestSelectMasterChoosesHighestScore(t *testing.T) {
	master := selectMaster([]timeproto.AgentReport{
		{AgentID: "n1", MasterCandidateScore: 1, UpdatedAt: time.Unix(100, 0)},
		{AgentID: "n2", MasterCandidateScore: 5, UpdatedAt: time.Unix(10, 0)},
		{AgentID: "n3", MasterCandidateScore: 5, UpdatedAt: time.Unix(11, 0)},
	})
	if master != "n3" {
		t.Fatalf("expected n3 as master, got %s", master)
	}
}
