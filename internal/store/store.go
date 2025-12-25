package store

import "context"

// Generic KV store + watch + lock with minimal surface.
// Only Consul implementation is provided in this repo.

type KV interface {
    PutJSON(ctx context.Context, key string, v any) error

    // PutJSONEphemeral writes the value under key and ties it to a session (behavior=delete).
    // When the caller process dies (session isn't renewed), Consul automatically deletes the key.
    // Implementations should keep renewing the session until ctx is cancelled.
    PutJSONEphemeral(ctx context.Context, key string, owner string, v any) error
    GetJSON(ctx context.Context, key string, out any) (bool, error)

    Delete(ctx context.Context, key string) error

    // ListJSON reads all keys under prefix (recursively) and unmarshals each into outSlicePtr (must be *[]T)
    ListJSON(ctx context.Context, prefix string, outSlicePtr any) error

    // WatchPrefixJSON emits the decoded list whenever something under prefix changes.
    // Implementations should avoid duplicate bursts by using ModifyIndex / hashing.
    WatchPrefixJSON(ctx context.Context, prefix string, outSlicePtrFactory func() any) <-chan any
}

type Locker interface {
    // Acquire blocks until lock is acquired or ctx cancelled. Returns a release func.
    Acquire(ctx context.Context, key string, owner string) (release func() error, err error)
}
