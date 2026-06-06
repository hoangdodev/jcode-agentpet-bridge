package agentpet

import (
	"bufio"
	"context"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// fakeServer accepts one connection on a temp unix socket and pushes each
// newline JSON it receives into ch.
func fakeServer(t *testing.T, path string, ch chan<- Event) net.Listener {
	t.Helper()
	ln, err := net.Listen("unix", path)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				r := bufio.NewReader(c)
				for {
					line, err := r.ReadBytes('\n')
					if err != nil {
						return
					}
					var e Event
					if jsonErr := json.Unmarshal(line, &e); jsonErr == nil {
						ch <- e
					}
				}
			}(c)
		}
	}()
	return ln
}

// shortSocketPath returns a sub-104-char socket path (macOS sun_path limit;
// t.TempDir() lives under /var/folders/... which often exceeds 104 chars).
func shortSocketPath(t *testing.T, name string) string {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", "jb")
	if err != nil {
		t.Fatalf("mkdtemp: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return filepath.Join(dir, name)
}

func TestSendOverSocket(t *testing.T) {
	sockPath := shortSocketPath(t, "ap.sock")
	queueDir := t.TempDir()

	ch := make(chan Event, 1)
	ln := fakeServer(t, sockPath, ch)
	defer ln.Close()

	c, err := New(sockPath, queueDir)
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	via, err := c.Send(context.Background(), Event{
		SessionID: "s1", AgentKind: "cli", EventName: "working",
		Project: "/tmp", Timestamp: 123.456,
	})
	if err != nil {
		t.Fatalf("send: %v", err)
	}
	if !via {
		t.Fatalf("expected delivered via socket")
	}
	select {
	case got := <-ch:
		if got.SessionID != "s1" || got.EventName != "working" || got.AgentKind != "cli" {
			t.Fatalf("payload mismatch: %+v", got)
		}
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for server to receive event")
	}
	// queue must remain empty
	entries, _ := os.ReadDir(queueDir)
	if len(entries) != 0 {
		t.Fatalf("queue should be empty, got %d files", len(entries))
	}
}

func TestSendFallsBackToQueueWhenNoServer(t *testing.T) {
	sockPath := shortSocketPath(t, "nope.sock")
	_ = os.Remove(sockPath) // ensure not present
	queueDir := t.TempDir()

	c, err := New(sockPath, queueDir)
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	via, err := c.Send(context.Background(), Event{
		SessionID: "s1", AgentKind: "cli", EventName: "registered",
		Timestamp: 1,
	})
	if err != nil {
		t.Fatalf("send: %v", err)
	}
	if via {
		t.Fatalf("expected queue fallback, got socket success")
	}
	entries, err := os.ReadDir(queueDir)
	if err != nil || len(entries) != 1 {
		t.Fatalf("queue should have 1 file, got %d (%v)", len(entries), err)
	}
	if !strings.HasSuffix(entries[0].Name(), ".json") {
		t.Fatalf("unexpected queue file name: %s", entries[0].Name())
	}
	body, _ := os.ReadFile(filepath.Join(queueDir, entries[0].Name()))
	if !strings.Contains(string(body), `"sessionId":"s1"`) {
		t.Fatalf("queue file body unexpected: %s", body)
	}
}

func TestEncodeLineEndsWithNewline(t *testing.T) {
	line, err := encodeLine(Event{
		SessionID: "x", AgentKind: "cli", EventName: "done", Timestamp: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if line[len(line)-1] != '\n' {
		t.Fatalf("missing trailing newline: %q", line)
	}
}

func TestPingReportsUnreachable(t *testing.T) {
	c, _ := New("/nonexistent/agentpet.sock", "/tmp")
	if err := c.Ping(); err == nil {
		t.Fatal("expected error when socket missing")
	}
}

// Regression: queue file/dir must be owner-only. Payloads can embed user
// session metadata (FriendlyName, prompt slice via Detail) so 0644/0755
// would expose them to other local users.
func TestQueueFileAndDirArePrivate(t *testing.T) {
	sockPath := shortSocketPath(t, "nope.sock")
	_ = os.Remove(sockPath)
	queueDir := filepath.Join(t.TempDir(), "queue") // not yet created

	c, err := New(sockPath, queueDir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Send(context.Background(), Event{
		SessionID: "s", EventName: "done", Timestamp: 1,
	}); err != nil {
		t.Fatal(err)
	}

	di, err := os.Stat(queueDir)
	if err != nil {
		t.Fatal(err)
	}
	if perm := di.Mode().Perm(); perm != 0o700 {
		t.Fatalf("queue dir perm = %o, want 0o700", perm)
	}

	entries, _ := os.ReadDir(queueDir)
	if len(entries) != 1 {
		t.Fatalf("expected 1 queue file, got %d", len(entries))
	}
	fi, err := os.Stat(filepath.Join(queueDir, entries[0].Name()))
	if err != nil {
		t.Fatal(err)
	}
	if perm := fi.Mode().Perm(); perm != 0o600 {
		t.Fatalf("queue file perm = %o, want 0o600", perm)
	}
}
