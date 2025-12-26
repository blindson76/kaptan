package runtime

import (
	"context"
	"fmt"
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
func WaitConsulReady(ctx context.Context, cli *capi.Client, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if leader, err := cli.Status().Leader(); err == nil && leader != "" {
			time.Sleep(5 * time.Second)
			return nil
		} else {
			lastErr = err
		}
		time.Sleep(1 * time.Second)
	}
	if lastErr != nil {
		return fmt.Errorf("consul not ready: %w", lastErr)
	}
	return fmt.Errorf("consul not ready: timeout after %s", timeout)
}
