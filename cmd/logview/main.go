package main

import (
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
)

const defaultAddr = "127.0.0.1:6644"

func main() {
	addr := flag.String("addr", defaultAddr, "udp listen address")
	flag.Parse()

	conn, err := net.ListenPacket("udp", *addr)
	if err != nil {
		log.Fatalf("listen udp %s: %v", *addr, err)
	}
	defer conn.Close()

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigs
		_ = conn.Close()
	}()

	fmt.Fprintf(os.Stderr, "listening on udp %s\n", *addr)

	buf := make([]byte, 64*1024)
	for {
		n, _, err := conn.ReadFrom(buf)
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return
			}
			log.Printf("read error: %v", err)
			continue
		}
		if n == 0 {
			continue
		}
		if _, err := os.Stdout.Write(buf[:n]); err != nil {
			log.Printf("write error: %v", err)
			continue
		}
		if buf[n-1] != '\n' {
			_, _ = os.Stdout.Write([]byte("\n"))
		}
	}
}
