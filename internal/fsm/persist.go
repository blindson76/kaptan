package fsm

import (
    "context"
    "sync"
)

// StateStore is the minimal interface needed by state persistence.
type StateStore interface {
    PutJSON(ctx context.Context, key string, v any) error
    GetJSON(ctx context.Context, key string, out any) (bool, error)
}

type PersistedState struct {
    mu   sync.Mutex
    key  string
    st   StateStore
    cur  string
}

// NewPersistedState keeps state in memory but also persists each transition to the store key.
func NewPersistedState(st StateStore, key string, defaultState string) *PersistedState {
    return &PersistedState{st: st, key: key, cur: defaultState}
}

func (p *PersistedState) Load(ctx context.Context) (string, error) {
    p.mu.Lock()
    defer p.mu.Unlock()

    var saved struct {
        State string `json:"state"`
    }
    ok, err := p.st.GetJSON(ctx, p.key, &saved)
    if err != nil {
        return "", err
    }
    if ok && saved.State != "" {
        p.cur = saved.State
    }
    return p.cur, nil
}

func (p *PersistedState) Save(ctx context.Context, state string) error {
    p.mu.Lock()
    defer p.mu.Unlock()
    p.cur = state
    return p.st.PutJSON(ctx, p.key, map[string]string{"state": state})
}

func (p *PersistedState) Get() string {
    p.mu.Lock()
    defer p.mu.Unlock()
    return p.cur
}
