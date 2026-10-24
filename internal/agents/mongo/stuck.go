package mongo

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"go.mongodb.org/mongo-driver/bson"
)

const (
	defaultRecoveringTimeout = 60 * time.Second
	maxRestartBackoff        = 10 * time.Minute
)

// isTransitionalState reports whether a mongo member state is one that must
// be left within a bounded time (STARTUP, STARTUP2, RECOVERING).
func isTransitionalState(state int) bool {
	return state == 0 || state == 3 || state == 5
}

// stuckTracker tracks how long a member has continuously been in a
// transitional state (STARTUP/STARTUP2/RECOVERING).
type stuckTracker struct {
	since time.Time
}

// observe records the current state and returns how long the member has been
// continuously transitional. The timer is reset when the state is left or the
// probe fails.
func (t *stuckTracker) observe(state int, ok bool, now time.Time) time.Duration {
	if !ok || !isTransitionalState(state) {
		t.since = time.Time{}
		return 0
	}
	if t.since.IsZero() {
		t.since = now
	}
	return now.Sub(t.since)
}

// isStuck reports whether the elapsed time exceeds the threshold.
func isStuck(elapsed, threshold time.Duration) bool {
	return threshold > 0 && elapsed >= threshold
}

// remediation decides the escalation step for a stuck member.
type remediation struct {
	resyncTried bool
	restarts    int
	lastRestart time.Time
}

type remediationAction int

const (
	actionNone remediationAction = iota
	actionRecheck
	actionRestart
)

// restartBackoff returns the minimum wait between restarts (doubles each time, capped).
func restartBackoff(base time.Duration, restarts int) time.Duration {
	d := base
	for i := 1; i < restarts && d < maxRestartBackoff; i++ {
		d *= 2
	}
	if d > maxRestartBackoff {
		d = maxRestartBackoff
	}
	return d
}

// next returns the action to take given the stuck state.
func (r *remediation) next(stuck bool, threshold time.Duration, now time.Time) remediationAction {
	if !stuck {
		r.resyncTried = false
		return actionNone
	}
	if !r.resyncTried {
		r.resyncTried = true
		return actionRecheck
	}
	if r.restarts > 0 && now.Sub(r.lastRestart) < restartBackoff(threshold, r.restarts) {
		return actionNone
	}
	r.restarts++
	r.lastRestart = now
	return actionRestart
}

// recheckSyncSource clears maintenance mode and re-evaluates the sync source
// so the member can pick a new one, without touching the replica set config.
func (a *Agent) recheckSyncSource(ctx context.Context) error {
	cli, err := a.mongoClient(ctx)
	if err != nil {
		return err
	}
	defer cli.Disconnect(context.Background())
	cmdCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	admin := cli.Database("admin")
	_ = admin.RunCommand(cmdCtx, bson.D{{Key: "replSetMaintenance", Value: false}}).Err()
	if err := admin.RunCommand(cmdCtx, bson.D{{Key: "replSetSyncFrom", Value: ""}}).Err(); err != nil {
		return fmt.Errorf("replSetSyncFrom reset: %w", err)
	}
	return nil
}

// restartMongod stops the local mongod and starts it again.
func (a *Agent) restartMongod(ctx context.Context) error {
	if err := a.shutdown(ctx); err != nil {
		return err
	}
	deadline := time.Now().Add(30 * time.Second)
	for {
		a.mongodMu.Lock()
		running := a.mongodCmd != nil
		a.mongodMu.Unlock()
		if !running {
			break
		}
		if time.Now().After(deadline) || ctx.Err() != nil {
			return fmt.Errorf("mongod did not stop in time")
		}
		time.Sleep(300 * time.Millisecond)
	}
	return a.startMongod(ctx, a.cfg.ReplSetName)
}

func (a *Agent) remediateStuck(ctx context.Context, act remediationAction, elapsed time.Duration) {
	switch act {
	case actionRecheck:
		log.Printf("%s", redf("[mongo-agent] member stuck for %s, re-checking sync source", elapsed.Round(time.Second)))
		if err := a.recheckSyncSource(ctx); err != nil {
			log.Printf("[mongo-agent] sync source recheck failed: %v", err)
		}
	case actionRestart:
		log.Printf("%s", redf("[mongo-agent] member still stuck after %s, restarting mongod", elapsed.Round(time.Second)))
		runCtx := a.runCtx
		if runCtx == nil {
			runCtx = ctx
		}
		go func() {
			defer a.restarting.Store(false)
			if err := a.restartMongod(runCtx); err != nil {
				log.Printf("%s", redf("[mongo-agent] mongod restart failed: %v", err))
			}
		}()
	}
}

// withStuck adds stuck markers to a JSON service note.
func withStuck(note string, elapsed time.Duration) string {
	var m map[string]any
	if err := json.Unmarshal([]byte(note), &m); err != nil {
		return note
	}
	mm, ok := m["mongo"].(map[string]any)
	if !ok {
		return note
	}
	mm["stuck"] = true
	mm["stuckFor"] = elapsed.Round(time.Second).String()
	b, err := json.Marshal(m)
	if err != nil {
		return note
	}
	return string(b)
}
