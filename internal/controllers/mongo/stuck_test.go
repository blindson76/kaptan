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
	c := New(Config{MaxOplogLag: time.Hour}, nil, nil, nil)
	c.candidates = []types.CandidateReport{
		{ID: "old", Eligible: true, LastOplogTs: now.Add(-5 * time.Hour)},
		{ID: "new", Eligible: true, LastOplogTs: now},
		{ID: "mid", Eligible: true, LastOplogTs: now.Add(-10 * time.Minute)},
	}
	el := c.eligibleCandidates()
	if el[0].ID != "new" || el[1].ID != "mid" || el[2].ID != "old" {
		t.Fatalf("order: %v", el)
	}
	newest := c.newestOplog(el)
	if c.shouldWipe(el[1], "", newest) {
		t.Fatal("mid should not be wiped")
	}
	if !c.shouldWipe(el[2], "", newest) {
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
