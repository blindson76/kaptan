package logging

import (
	"io"
	"log"
	"net"
	"os"
	"sync"
	"time"
)

const udpAddr = "127.0.0.1:6644"

func Setup(nodeID string) {
	if nodeID == "" {
		nodeID = "unknown"
	}
	udpWriter := newUDPWriter(udpAddr, nodeID)
	log.SetOutput(io.MultiWriter(os.Stderr, udpWriter))
}

type udpWriter struct {
	mu     sync.Mutex
	addr   *net.UDPAddr
	conn   *net.UDPConn
	nodeID string
}

func newUDPWriter(addr, nodeID string) *udpWriter {
	udpAddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return &udpWriter{nodeID: nodeID}
	}
	return &udpWriter{addr: udpAddr, nodeID: nodeID}
}

func (w *udpWriter) Write(p []byte) (int, error) {
	payload := p
	if w.nodeID != "" {
		prefix := "[node=" + w.nodeID + "] "
		payload = make([]byte, 0, len(prefix)+len(p))
		payload = append(payload, prefix...)
		payload = append(payload, p...)
	}
	if w.addr == nil {
		return len(p), nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.conn == nil {
		conn, err := net.DialUDP("udp", nil, w.addr)
		if err != nil {
			return len(p), nil
		}
		w.conn = conn
	}
	_ = w.conn.SetWriteDeadline(time.Now().Add(200 * time.Millisecond))
	_, _ = w.conn.Write(payload)
	return len(p), nil
}