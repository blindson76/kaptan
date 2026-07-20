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
		// Use the full relative path (without extension) as the service identifier
		// so that files from different agent nodes remain distinct.
		entries = append(entries, logFileEntry{
			Service:    strings.TrimSuffix(filepath.ToSlash(rel), ".log"),
			File:       filepath.ToSlash(rel),
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
//   - service: relative path (without .log) as returned by GET /logs, e.g. "node-1/svc-master"
//     or just the service base name "svc" (returns the most recently modified match)
//   - lines: number of tail lines to return (default 200)
//   - follow: if "true", streams new content as it arrives (like tail -f)
func (s *logServer) handleView(w http.ResponseWriter, r *http.Request) {
	name := strings.TrimPrefix(r.URL.Path, "/logs/")
	if name == "" {
		http.Error(w, "service name required", http.StatusBadRequest)
		return
	}

	logPath, ok := s.resolveLogPath(name)
	if !ok {
		http.Error(w, "invalid path", http.StatusBadRequest)
		return
	}
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

// resolveLogPath maps a client-supplied name to an absolute path within logDir.
// It returns ("", false) when the name contains a path-traversal attempt.
// It returns ("", true) when no matching file is found.
// The name may be:
//   - a full relative path without extension, e.g. "node-1/svc-master"
//   - a bare service base name, e.g. "svc" (matches the most recently modified file
//     whose base name equals "svc" or starts with "svc-")
func (s *logServer) resolveLogPath(name string) (string, bool) {
	absLogDir, err := filepath.Abs(s.logDir)
	if err != nil {
		return "", false
	}
	// Normalise the separator so that URL slashes work on all platforms.
	name = filepath.FromSlash(name)

	// Attempt exact match: treat name as a relative path to a .log file.
	candidate := filepath.Clean(filepath.Join(absLogDir, name+".log"))
	if !strings.HasPrefix(candidate, absLogDir+string(filepath.Separator)) {
		return "", false // path traversal attempt
	}
	if _, statErr := os.Stat(candidate); statErr == nil {
		return candidate, true
	}

	// Fall back: search by base name only (ignore any directory prefix the
	// client supplied and match against the file's own base name).
	base := filepath.Base(name)
	found := s.findLogFileByBase(base)
	return found, true
}

// findLogFileByBase looks for a log file whose base name (without the .log
// suffix) equals baseName or starts with baseName+"-".  When multiple files
// match (e.g. svc-master.log and svc-slave.log for baseName "svc"), the most
// recently modified one is returned so that the active instance is preferred.
func (s *logServer) findLogFileByBase(baseName string) string {
	var (
		found    string
		foundMod time.Time
	)
	_ = filepath.Walk(s.logDir, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return nil
		}
		base := strings.TrimSuffix(info.Name(), ".log")
		if base != baseName && !strings.HasPrefix(base, baseName+"-") {
			return nil
		}
		// Prefer exact match; among equal specificity prefer most-recently modified.
		if found == "" || info.ModTime().After(foundMod) {
			found = path
			foundMod = info.ModTime()
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
	// If the last n lines exceed 4 MiB the output will be truncated to whatever
	// content fits within that window; the 4 MiB cap keeps memory bounded for
	// very large or noisy log files.
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
