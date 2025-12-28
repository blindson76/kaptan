package runtime

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	capi "github.com/hashicorp/consul/api"
)

const logPrefix = "[runtime]"

func WithSignals(parent context.Context) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(parent)
	ch := make(chan os.Signal, 2)
	signal.Notify(ch, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-ch
		log.Printf("%s signal received, shutting down", logPrefix)
		cancel()
		// second signal -> hard exit
		select {
		case <-ch:
			os.Exit(2)
		case <-time.After(2 * time.Second):
		}
	}()
	return ctx, cancel
}

// WaitConsulReady polls the Consul leader endpoint until a leader is reported or the timeout/context expires.
func WaitConsulReady(ctx context.Context, c *capi.Client, timeout time.Duration) error {
	backoff := 1 * time.Second
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		leader, err := c.Status().Leader()
		if err == nil && leader != "" {
			// KV round-trip
			key := "healthchecks/consulready"
			_, err = c.KV().Put(&capi.KVPair{Key: key, Value: []byte("ok")}, nil)
			if err == nil {
				_, _, err = c.KV().Get(key, nil)
			}
			if _, err := c.Agent().Self(); err != nil {
				log.Printf("%s consul agent self check failed: %v", logPrefix, err)
			} else {
				return nil
			}
		}

		time.Sleep(backoff)
		if backoff < 2*time.Second {
			backoff *= 2
		}
	}
}
