package runtime

import (
    "context"
    "log"
    "os"
    "os/signal"
    "syscall"
    "time"
)

func WithSignals(parent context.Context) (context.Context, context.CancelFunc) {
    ctx, cancel := context.WithCancel(parent)
    ch := make(chan os.Signal, 2)
    signal.Notify(ch, os.Interrupt, syscall.SIGTERM)
    go func() {
        <-ch
        log.Printf("signal received, shutting down")
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
