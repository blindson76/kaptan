package common

import (
    "context"
    "log"
    "time"

    "github.com/qmuntal/stateless"
    "github.com/umitbozkurt/consul-replctl/internal/fsm"
)

// RunLeaderLoop tries to acquire lock; while leader, runs runActive(ctx).
// If leadership lost (ctx cancel or runActive returns), it retries.
func RunLeaderLoop(ctx context.Context, locker interface {
    Acquire(context.Context, string, string) (func() error, error)
}, lockKey string, owner string, runActive func(context.Context) error) error {

    for {
        select {
        case <-ctx.Done():
            return ctx.Err()
        default:
        }

        release, err := locker.Acquire(ctx, lockKey, owner)
        if err != nil {
            log.Printf("leader lock acquire failed: %v", err)
            time.Sleep(2 * time.Second)
            continue
        }
        log.Printf("became leader for lock=%s owner=%s", lockKey, owner)

        runCtx, cancel := context.WithCancel(ctx)
        err = runActive(runCtx)
        cancel()

        _ = release()
        log.Printf("released leader lock=%s owner=%s err=%v", lockKey, owner, err)

        // backoff to prevent hot loop
        time.Sleep(1 * time.Second)
    }
}

// BuildExternalStorage wires stateless external storage to a persisted state.
// The stateless library expects callbacks of GetState and SetState.
func BuildExternalStorage(ctx context.Context, p *fsm.PersistedState) (get func(context.Context) string, set func(context.Context, string)) {
    // Load once at start.
    if _, err := p.Load(ctx); err != nil {
        log.Printf("state load error: %v", err)
    }
    get = func(context.Context) string { return p.Get() }
    set = func(c context.Context, s string) {
        if err := p.Save(c, s); err != nil {
            log.Printf("state save error: %v", err)
        }
    }
    return
}

// Helper to create machine with external storage.
func NewMachine(ctx context.Context, p *fsm.PersistedState) *stateless.StateMachine {
    get, set := BuildExternalStorage(ctx, p)
    return stateless.NewStateMachineWithExternalStorage(get, set)
}
