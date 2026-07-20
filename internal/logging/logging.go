package logging

import (
	"io"
	"log"
	"net"
	"os"
	"sync"
	"time"
)

// DefaultAddr is used whenever no remote log collector address is
// configured. It only reaches a collector (see cmd/logview) running on
// the same host; set logging.address in config.yaml to ship logs to an
// actual remote collector.
const DefaultAddr = "127.0.0.1:6644"

// Setup wires the standard logger to also forward every line to a remote
// UDP log collector (see cmd/logview), tagged with nodeID. addr may be
// empty, in which case DefaultAddr is used.
func Setup(nodeID, addr string) {
	log.SetOutput(io.MultiWriter(os.Stderr, NewWriter(nodeID, addr, "")))
}

// NewWriter returns an io.Writer that ships every Write to a remote UDP
// log collector, tagged with nodeID and an optional free-form tag (e.g.
// "service=my-service-a role=master stream=stdout"). It is safe to use
// as an exec.Cmd's Stdout/Stderr so that a managed service's own output
// is shipped to the same remote log sink as the daemon's logs.
func NewWriter(nodeID, addr, tag string) io.Writer {
	if nodeID == "" {
		nodeID = "unknown"
	}
	if addr == "" {
		addr = DefaultAddr
	}
	return newUDPWriter(addr, nodeID, tag)
}

type udpWriter struct {
	mu     sync.Mutex
	addr   *net.UDPAddr
	conn   *net.UDPConn
	prefix string
}

func newUDPWriter(addr, nodeID, tag string) *udpWriter {
	prefix := "[node=" + nodeID + "]"
	if tag != "" {
		prefix += "[" + tag + "]"
	}
	prefix += " "
	udpAddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return &udpWriter{prefix: prefix}
	}
	return &udpWriter{addr: udpAddr, prefix: prefix}
}

func (w *udpWriter) Write(p []byte) (int, error) {
	payload := make([]byte, 0, len(w.prefix)+len(p))
	payload = append(payload, w.prefix...)
	payload = append(payload, p...)
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