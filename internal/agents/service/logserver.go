package service

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

const defaultTailLines = 200

type logServer struct {
	logDir string
}

func newLogServer(logDir string) *logServer {
	return &logServer{logDir: logDir}
}

func (s *logServer) start(ctx context.Context, addr string) {
	mux := http.NewServeMux()
	mux.HandleFunc("/logs", s.handleList)
	mux.HandleFunc("/logs/", s.handleView)
	srv := &http.Server{Addr: addr, Handler: mux}
	go func() {
		<-ctx.Done()
		shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutCtx)
	}()
	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("[service-agent] log server error: %v", err)
		}
	}()
}

// logFileEntry describes a single service log file.
type logFileEntry struct {
	Service    string    `json:"service"`
	File       string    `json:"file"`
	Size       int64     `json:"size"`
	ModifiedAt time.Time `json:"modified_at"`
}

// handleList returns a JSON list of all available service log files.
func (s *logServer) handleList(w http.ResponseWriter, _ *http.Request) {
	var entries []logFileEntry
	_ = filepath.Walk(s.logDir, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return nil
		}
		if !strings.HasSuffix(info.Name(), ".log") {
			return nil
		}
		rel, _ := filepath.Rel(s.logDir, path)
		entries = append(entries, logFileEntry{
			Service:    strings.TrimSuffix(info.Name(), ".log"),
			File:       rel,
			Size:       info.Size(),
			ModifiedAt: info.ModTime(),
		})
		return nil
	})
	if entries == nil {
		entries = []logFileEntry{}
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(entries)
}

// handleView serves the log file for a service.
// URL: GET /logs/{service}[?lines=N][&follow=true]
// - lines: number of tail lines to return (default 200)
// - follow: if "true", streams new content as it arrives (like tail -f)
func (s *logServer) handleView(w http.ResponseWriter, r *http.Request) {
	name := strings.TrimPrefix(r.URL.Path, "/logs/")
	// Guard against path traversal
	name = filepath.Base(name)
	if name == "" || name == "." {
		http.Error(w, "service name required", http.StatusBadRequest)
		return
	}

	logPath := s.findLogFile(name)
	if logPath == "" {
		http.Error(w, "log file not found", http.StatusNotFound)
		return
	}

	follow := r.URL.Query().Get("follow") == "true"
	lines := defaultTailLines
	if lStr := r.URL.Query().Get("lines"); lStr != "" {
		if n, err := strconv.Atoi(lStr); err == nil && n > 0 {
			lines = n
		}
	}

	if follow {
		s.streamLog(w, r, logPath)
	} else {
		s.tailLog(w, logPath, lines)
	}
}

// findLogFile looks for a log file whose base name equals name or starts with name+"-".
// It searches all subdirectories of logDir and returns the first match.
func (s *logServer) findLogFile(name string) string {
	var found string
	_ = filepath.Walk(s.logDir, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() || found != "" {
			return nil
		}
		base := strings.TrimSuffix(info.Name(), ".log")
		if base == name || strings.HasPrefix(base, name+"-") {
			found = path
		}
		return nil
	})
	return found
}

// tailLog writes the last n lines of the log file to w.
func (s *logServer) tailLog(w http.ResponseWriter, path string, lines int) {
	f, err := os.Open(path)
	if err != nil {
		http.Error(w, fmt.Sprintf("open log: %v", err), http.StatusInternalServerError)
		return
	}
	defer f.Close()

	content, err := tailFile(f, lines)
	if err != nil {
		http.Error(w, fmt.Sprintf("read log: %v", err), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	_, _ = w.Write(content)
}

// streamLog seeks to the end of the file and streams new content to w as it is written.
// Exits when the request context is cancelled (client disconnects).
func (s *logServer) streamLog(w http.ResponseWriter, r *http.Request, path string) {
	f, err := os.Open(path)
	if err != nil {
		http.Error(w, fmt.Sprintf("open log: %v", err), http.StatusInternalServerError)
		return
	}
	defer f.Close()

	// Seek to the end so only new content is streamed.
	if _, err := f.Seek(0, io.SeekEnd); err != nil {
		http.Error(w, fmt.Sprintf("seek log: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(http.StatusOK)

	flusher, canFlush := w.(http.Flusher)
	buf := make([]byte, 4096)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-r.Context().Done():
			return
		case <-ticker.C:
			for {
				n, readErr := f.Read(buf)
				if n > 0 {
					if _, writeErr := w.Write(buf[:n]); writeErr != nil {
						return
					}
					if canFlush {
						flusher.Flush()
					}
				}
				if readErr != nil {
					break
				}
			}
		}
	}
}

// tailFile reads up to the last n newline-delimited lines from f.
func tailFile(f *os.File, n int) ([]byte, error) {
	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	size := info.Size()
	if size == 0 {
		return nil, nil
	}

	// Read at most 4 MiB from the end to find the last n lines.
	const maxRead = 4 * 1024 * 1024
	readSize := int64(maxRead)
	if readSize > size {
		readSize = size
	}
	if _, err := f.Seek(-readSize, io.SeekEnd); err != nil {
		return nil, err
	}
	data := make([]byte, readSize)
	nr, err := io.ReadFull(f, data)
	if err != nil && err != io.ErrUnexpectedEOF {
		return nil, err
	}
	data = data[:nr]

	// Walk backwards to find the start of the n-th line from the end.
	// Skip a trailing newline so it is not counted as an extra empty line.
	pos := len(data)
	if pos > 0 && data[pos-1] == '\n' {
		pos--
	}
	count := 0
	for pos > 0 {
		pos--
		if data[pos] == '\n' {
			count++
			if count >= n {
				pos++ // include the character after this newline
				return data[pos:], nil
			}
		}
	}
	return data, nil
}
