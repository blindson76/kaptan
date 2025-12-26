package consul

import (
    "context"
    "errors"
    "fmt"
    "time"

    capi "github.com/hashicorp/consul/api"
)

type Lock struct {
    c      *capi.Client
    prefix string
}

func (s *Store) Locker() *Lock {
    return &Lock{c: s.c, prefix: s.prefix}
}

func (l *Lock) key(k string) string {
    if k == "" {
        return l.prefix
    }
    return fmt.Sprintf("%s/%s", l.prefix, k)
}

// Acquire uses a Consul session + KV Acquire (ephemeral lock).
// It automatically renews the session until ctx is cancelled or release() is called.
func (l *Lock) Acquire(ctx context.Context, key string, owner string) (func() error, error) {
    sess, _, err := l.c.Session().Create(&capi.SessionEntry{
        Name:      owner,
        Behavior:  capi.SessionBehaviorDelete,
        TTL:       "10s",
        LockDelay: 0,
    }, nil)
    if err != nil {
        return nil, err
    }

    stopRenew := make(chan struct{})
    go func() {
        ticker := time.NewTicker(3 * time.Second)
        defer ticker.Stop()
        for {
            select {
            case <-ticker.C:
                _, _, _ = l.c.Session().Renew(sess, nil)
            case <-stopRenew:
                return
            case <-ctx.Done():
                return
            }
        }
    }()

    kv := l.c.KV()
    p := &capi.KVPair{Key: l.key(key), Value: []byte(owner), Session: sess}

    for {
        select {
        case <-ctx.Done():
            close(stopRenew)
            _, _ = l.c.Session().Destroy(sess, nil)
            return nil, ctx.Err()
        default:
        }
        ok, _, err := kv.Acquire(p, nil)
        if err != nil {
            time.Sleep(1 * time.Second)
            continue
        }
        if ok {
            break
        }
        time.Sleep(700 * time.Millisecond)
    }

    released := false
    release := func() error {
        if released {
            return nil
        }
        released = true
        close(stopRenew)
        // Release key then destroy session
        _, _, _ = kv.Release(p, nil)
        _, _ = l.c.Session().Destroy(sess, nil)
        return nil
    }
    return release, nil
}

var ErrNotLeader = errors.New("not leader")
