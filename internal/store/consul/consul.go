package consul

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"path"
	"reflect"
	"sort"
	"time"

	capi "github.com/hashicorp/consul/api"
)

type Store struct {
	c      *capi.Client
	prefix string
	node   string
}

func New(address, datacenter, token, prefix, node string) (*Store, error) {
	cfg := capi.DefaultConfig()
	cfg.Address = address
	if datacenter != "" {
		cfg.Datacenter = datacenter
	}
	if token != "" {
		cfg.Token = token
	}
	cli, err := capi.NewClient(cfg)
	if err != nil {
		return nil, err
	}
	if prefix == "" {
		prefix = "replctl/v1"
	}
	return &Store{c: cli, prefix: prefix, node: node}, nil
}

func (s *Store) key(k string) string {
	if k == "" {
		return s.prefix
	}
	return path.Join(s.prefix, k)
}

func (s *Store) PutJSON(ctx context.Context, key string, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	p := &capi.KVPair{Key: s.key(key), Value: b}
	_, err = s.c.KV().Put(p, nil)
	return err
}

func (s *Store) PutJSONEphemeral(ctx context.Context, key string, owner string, v any) error {
	if owner == "" {
		owner = key
	}
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}

	// Avoid default serfHealth check; TTL renewals provide liveness.
	sessEntry := &capi.SessionEntry{
		Name:      owner,
		Behavior:  capi.SessionBehaviorDelete,
		TTL:       "10s",
		LockDelay: 0,
	}
	if s.node != "" {
		sessEntry.Node = s.node
	}
	sess, _, err := s.c.Session().CreateNoChecks(sessEntry, nil)
	if err != nil {
		return err
	}

	kv := s.c.KV()
	p := &capi.KVPair{Key: s.key(key), Value: b, Session: sess}

	// Acquire the key so it is tied to the session (behavior=delete).
	for {
		if ctx.Err() != nil {
			_, _ = s.c.Session().Destroy(sess, nil)
			return ctx.Err()
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

	// Renew the session while ctx is alive; if the process dies, the session expires and Consul deletes the key.
	go func() {
		ticker := time.NewTicker(3 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				_, _, _ = s.c.Session().Renew(sess, nil)
			case <-ctx.Done():
				_, _ = s.c.Session().Destroy(sess, nil)
				return
			}
		}
	}()

	return nil
}

func (s *Store) GetJSON(ctx context.Context, key string, out any) (bool, error) {
	p, _, err := s.c.KV().Get(s.key(key), nil)
	if err != nil {
		return false, err
	}
	if p == nil {
		return false, nil
	}
	if out == nil {
		return true, nil
	}
	return true, json.Unmarshal(p.Value, out)
}

func (s *Store) Delete(ctx context.Context, key string) error {
	_, err := s.c.KV().Delete(s.key(key), nil)
	return err
}

func (s *Store) ListJSON(ctx context.Context, prefix string, outSlicePtr any) error {
	pairs, _, err := s.c.KV().List(s.key(prefix), nil)
	if err != nil {
		return err
	}
	rv := reflect.ValueOf(outSlicePtr)
	if rv.Kind() != reflect.Pointer || rv.Elem().Kind() != reflect.Slice {
		return errors.New("outSlicePtr must be pointer to slice")
	}
	slice := rv.Elem()
	slice.SetLen(0)

	// stable order
	sort.Slice(pairs, func(i, j int) bool { return pairs[i].Key < pairs[j].Key })

	elemType := slice.Type().Elem()
	for _, p := range pairs {
		if p == nil || len(p.Value) == 0 {
			continue
		}
		elem := reflect.New(elemType)
		if err := json.Unmarshal(p.Value, elem.Interface()); err != nil {
			return fmt.Errorf("unmarshal %s: %w", p.Key, err)
		}
		slice.Set(reflect.Append(slice, elem.Elem()))
	}
	return nil
}

func (s *Store) WatchPrefixJSON(ctx context.Context, prefix string, outSlicePtrFactory func() any) <-chan any {
	out := make(chan any, 1)
	go func() {
		defer close(out)
		var lastIndex uint64
		var lastHash string

		for {
			select {
			case <-ctx.Done():
				return
			default:
			}

			pairs, meta, err := s.c.KV().List(s.key(prefix), &capi.QueryOptions{
				WaitIndex: lastIndex,
				WaitTime:  5 * time.Second,
			})
			if err != nil {
				time.Sleep(1 * time.Second)
				continue
			}
			if meta != nil {
				lastIndex = meta.LastIndex
			}

			// Hash values + keys to reduce duplicate bursts (e.g., session renew side-effects).
			h := sha256.New()
			for _, p := range pairs {
				if p == nil {
					continue
				}
				h.Write([]byte(p.Key))
				h.Write([]byte{0})
				h.Write(p.Value)
				h.Write([]byte{0})
			}
			sum := hex.EncodeToString(h.Sum(nil))
			if sum == lastHash {
				continue
			}
			lastHash = sum

			outSlicePtr := outSlicePtrFactory()
			// Reuse ListJSON logic by unmarshalling here
			// (but we already have pairs)
			rv := reflect.ValueOf(outSlicePtr)
			if rv.Kind() != reflect.Pointer || rv.Elem().Kind() != reflect.Slice {
				continue
			}
			slice := rv.Elem()
			slice.SetLen(0)
			elemType := slice.Type().Elem()
			sort.Slice(pairs, func(i, j int) bool { return pairs[i].Key < pairs[j].Key })
			for _, p := range pairs {
				if p == nil || len(p.Value) == 0 {
					continue
				}
				elem := reflect.New(elemType)
				_ = json.Unmarshal(p.Value, elem.Interface())
				slice.Set(reflect.Append(slice, elem.Elem()))
			}

			select {
			case out <- rv.Elem().Interface():
			default:
				// drop if consumer is slow; next event will include latest snapshot
			}
		}
	}()
	return out
}
