package service

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLogServerList_Empty(t *testing.T) {
	dir := t.TempDir()
	srv := newLogServer(dir)

	req := httptest.NewRequest(http.MethodGet, "/logs", nil)
	rr := httptest.NewRecorder()
	srv.handleList(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}
	var entries []logFileEntry
	if err := json.Unmarshal(rr.Body.Bytes(), &entries); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("expected empty list, got %v", entries)
	}
}

func TestLogServerList_WithFiles(t *testing.T) {
	dir := t.TempDir()
	// Create two log files in a subdirectory (matching how the agent writes them)
	subDir := filepath.Join(dir, "node-1")
	if err := os.MkdirAll(subDir, 0o755); err != nil {
		t.Fatal(err)
	}
	writeFile(t, filepath.Join(subDir, "svc-master.log"), "line1\nline2\n")
	writeFile(t, filepath.Join(subDir, "svc-slave.log"), "line3\n")
	writeFile(t, filepath.Join(subDir, "not-a-log.txt"), "ignore me")

	srv := newLogServer(dir)
	req := httptest.NewRequest(http.MethodGet, "/logs", nil)
	rr := httptest.NewRecorder()
	srv.handleList(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}
	var entries []logFileEntry
	if err := json.Unmarshal(rr.Body.Bytes(), &entries); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("expected 2 entries, got %d: %v", len(entries), entries)
	}
	for _, e := range entries {
		if !strings.HasSuffix(e.File, ".log") {
			t.Fatalf("unexpected non-log entry: %v", e)
		}
	}
}

func TestLogServerView_NotFound(t *testing.T) {
	dir := t.TempDir()
	srv := newLogServer(dir)

	req := httptest.NewRequest(http.MethodGet, "/logs/nonexistent", nil)
	rr := httptest.NewRecorder()
	srv.handleView(rr, req)

	if rr.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", rr.Code)
	}
}

func TestLogServerView_MissingName(t *testing.T) {
	dir := t.TempDir()
	srv := newLogServer(dir)

	req := httptest.NewRequest(http.MethodGet, "/logs/", nil)
	rr := httptest.NewRecorder()
	srv.handleView(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rr.Code)
	}
}

func TestLogServerView_TailDefault(t *testing.T) {
	dir := t.TempDir()
	subDir := filepath.Join(dir, "node-1")
	if err := os.MkdirAll(subDir, 0o755); err != nil {
		t.Fatal(err)
	}
	writeFile(t, filepath.Join(subDir, "mysvc-master.log"), "alpha\nbeta\ngamma\n")

	srv := newLogServer(dir)
	req := httptest.NewRequest(http.MethodGet, "/logs/mysvc", nil)
	rr := httptest.NewRecorder()
	srv.handleView(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}
	body := rr.Body.String()
	for _, line := range []string{"alpha", "beta", "gamma"} {
		if !strings.Contains(body, line) {
			t.Errorf("expected %q in body, got: %q", line, body)
		}
	}
}

func TestLogServerView_TailLines(t *testing.T) {
	dir := t.TempDir()
	// Build a file with 10 lines
	var sb strings.Builder
	for i := 1; i <= 10; i++ {
		fmt.Fprintf(&sb, "line%d\n", i)
	}
	writeFile(t, filepath.Join(dir, "svc.log"), sb.String())

	srv := newLogServer(dir)
	req := httptest.NewRequest(http.MethodGet, "/logs/svc?lines=3", nil)
	rr := httptest.NewRecorder()
	srv.handleView(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}
	body := rr.Body.String()
	// Should contain last 3 lines
	for _, line := range []string{"line8", "line9", "line10"} {
		if !strings.Contains(body, line) {
			t.Errorf("expected %q in tail output, got: %q", line, body)
		}
	}
	// Should NOT contain earlier lines (use newline terminator to avoid matching "line10" as "line1")
	if strings.Contains(body, "line1\n") || strings.Contains(body, "line2\n") {
		t.Errorf("did not expect early lines in tail output, got: %q", body)
	}
}

func TestLogServerView_PathTraversal(t *testing.T) {
	dir := t.TempDir()
	// Create a log file outside the logDir to ensure we cannot access it
	outside := t.TempDir()
	writeFile(t, filepath.Join(outside, "secret.log"), "secret content")

	srv := newLogServer(dir)
	req := httptest.NewRequest(http.MethodGet, "/logs/../../secret", nil)
	rr := httptest.NewRecorder()
	srv.handleView(rr, req)

	// Should either 404 (file not in logDir) or 400 (bad name)
	if rr.Code == http.StatusOK {
		if strings.Contains(rr.Body.String(), "secret content") {
			t.Fatal("path traversal succeeded — security issue")
		}
	}
}

func TestTailFile(t *testing.T) {
	f, err := os.CreateTemp("", "tail-*.txt")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(f.Name())
	defer f.Close()

	lines := []string{"one", "two", "three", "four", "five"}
	for _, l := range lines {
		fmt.Fprintln(f, l)
	}
	// Reopen for reading
	f2, err := os.Open(f.Name())
	if err != nil {
		t.Fatal(err)
	}
	defer f2.Close()

	got, err := tailFile(f2, 3)
	if err != nil {
		t.Fatalf("tailFile: %v", err)
	}
	result := string(got)
	for _, l := range []string{"three", "four", "five"} {
		if !strings.Contains(result, l) {
			t.Errorf("expected %q in tail, got %q", l, result)
		}
	}
	if strings.Contains(result, "one") || strings.Contains(result, "two") {
		t.Errorf("unexpected lines in tail: %q", result)
	}
}

func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("writeFile %s: %v", path, err)
	}
}
