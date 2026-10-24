package mongo

import (
	"testing"
	"time"

	"github.com/umitbozkurt/consul-replctl/internal/types"
)

func TestIsMongoStuck(t *testing.T) {
	if !isMongoStuck(`{"mongo":{"state":3,"stateStr":"RECOVERING","stuck":true,"stuckFor":"1m0s"}}`) {
		t.Fatal("expected stuck")
	}
	if isMongoStuck(`{"mongo":{"state":3,"stateStr":"RECOVERING"}}`) || isMongoStuck("") || isMongoStuck("x") {
		t.Fatal("unexpected stuck")
	}
	if s, str, ok := parseMongoState(`{"mongo":{"state":3,"stateStr":"RECOVERING","stuck":true}}`); !ok || s != 3 || str != "RECOVERING" {
		t.Fatalf("parse: %d %s %v", s, str, ok)
	}
}

func TestOplogStaleOrderingAndWipe(t *testing.T) {
	now := time.Now()
	c := New(Config{}, nil, nil, nil)
	c.candidates = []types.CandidateReport{
		{ID: "old", Eligible: true, LastOplogTs: now.Add(-5 * time.Hour), FirstOplogTs: now.Add(-9 * time.Hour)},
		{ID: "new", Eligible: true, LastOplogTs: now, FirstOplogTs: now.Add(-2 * time.Hour)},
		{ID: "mid", Eligible: true, LastOplogTs: now.Add(-90 * time.Minute), FirstOplogTs: now.Add(-3 * time.Hour)},
		{ID: "empty", Eligible: true},
	}
	el := c.eligibleCandidates()
	if el[0].ID != "new" || el[1].ID != "mid" || el[len(el)-1].ID != "old" {
		t.Fatalf("order: %v", el)
	}
	ws := c.oplogWindowStart(el)
	if !ws.Equal(now.Add(-2 * time.Hour)) {
		t.Fatalf("window start %v", ws)
	}
	if c.shouldWipe(el[1], "", ws) {
		t.Fatal("mid is inside the window, no wipe")
	}
	if c.shouldWipe(types.CandidateReport{ID: "empty"}, "", ws) {
		t.Fatal("empty oplog is not stale")
	}
	if !c.shouldWipe(types.CandidateReport{ID: "old", LastOplogTs: now.Add(-5 * time.Hour)}, "", ws) {
		t.Fatal("old should be wiped")
	}
}

func TestStuckMemberScheduledForWipe(t *testing.T) {
	c := New(Config{}, nil, nil, nil)
	c.health = []types.HealthStatus{{ID: "a", Reason: `{"mongo":{"state":3,"stuck":true}}`}}
	c.markStuckForWipe("a")
	if !c.shouldWipe(types.CandidateReport{ID: "a"}, "", time.Time{}) {
		t.Fatal("expected wipe")
	}
	if c.shouldWipe(types.CandidateReport{ID: "a"}, "", time.Time{}) {
		t.Fatal("wipe flag should be consumed")
	}
}
