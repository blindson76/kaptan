package mongo

import (
	"strings"
	"testing"
	"time"
)

func TestStuckTrackerResets(t *testing.T) {
	var tr stuckTracker
	t0 := time.Unix(1000, 0)
	if d := tr.observe(3, true, t0); d != 0 {
		t.Fatalf("got %v", d)
	}
	if d := tr.observe(3, true, t0.Add(70*time.Second)); !isStuck(d, 60*time.Second) {
		t.Fatalf("expected stuck, elapsed %v", d)
	}
	if d := tr.observe(5, true, t0.Add(80*time.Second)); d != 80*time.Second-0 && d != 80*time.Second {
		t.Fatalf("STARTUP2 should continue timer, got %v", d)
	}
	if d := tr.observe(2, true, t0.Add(90*time.Second)); d != 0 || isStuck(d, time.Second) {
		t.Fatalf("expected reset, got %v", d)
	}
	if d := tr.observe(3, true, t0.Add(100*time.Second)); d != 0 {
		t.Fatalf("timer should restart, got %v", d)
	}
	tr.observe(3, true, t0.Add(110*time.Second))
	if d := tr.observe(3, false, t0.Add(120*time.Second)); d != 0 {
		t.Fatalf("failed probe resets, got %v", d)
	}
}

func TestRemediationEscalationAndBackoff(t *testing.T) {
	var r remediation
	th := time.Minute
	now := time.Unix(0, 0)
	if a := r.next(false, th, now); a != actionNone {
		t.Fatal("not stuck")
	}
	if a := r.next(true, th, now); a != actionRecheck {
		t.Fatalf("got %v", a)
	}
	if a := r.next(true, th, now.Add(5*time.Second)); a != actionRestart {
		t.Fatalf("got %v", a)
	}
	if a := r.next(true, th, now.Add(10*time.Second)); a != actionNone {
		t.Fatalf("backoff expected, got %v", a)
	}
	if a := r.next(true, th, now.Add(70*time.Second)); a != actionRestart {
		t.Fatalf("got %v", a)
	}
	if a := r.next(true, th, now.Add(130*time.Second)); a != actionNone {
		t.Fatalf("doubled backoff expected, got %v", a)
	}
	r.next(false, th, now)
	if a := r.next(true, th, now.Add(200*time.Second)); a != actionRecheck {
		t.Fatalf("reset expected, got %v", a)
	}
}

func TestWithStuck(t *testing.T) {
	a := &Agent{}
	note := a.buildServiceNote(3, "RECOVERING", "", "", nil, "", nil)
	out := withStuck(note, 75*time.Second)
	if !strings.Contains(out, `"stuck":true`) || !strings.Contains(out, `"stuckFor":"1m15s"`) {
		t.Fatalf("unexpected note %s", out)
	}
}
