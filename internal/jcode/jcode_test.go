package jcode

import (
	"context"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"testing"
)

// shortSocketPath returns a sub-104-char path under /tmp suitable for unix
// sockets on macOS (sun_path is fixed at 104 bytes there; the default
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

// fakeDebugServer simulates jcode's debug socket. It expects exactly one
// `debug_command:sessions` request and replies with an ack + debug_response
// whose `output` is the JSON-encoded sessions list passed in.
func fakeDebugServer(t *testing.T, path string, sessions []Session) net.Listener {
	t.Helper()
	ln, err := net.Listen("unix", path)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go func() {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		defer c.Close()
		dec := json.NewDecoder(c)
		var req debugRequest
		if err := dec.Decode(&req); err != nil {
			t.Errorf("decode: %v", err)
			return
		}
		// ack
		ack := map[string]any{"type": "ack", "id": req.ID}
		ackBytes, _ := json.Marshal(ack)
		_, _ = c.Write(append(ackBytes, '\n'))
		// debug_response with sessions as `output` string
		outBytes, _ := json.Marshal(sessions)
		resp := debugResponse{
			Type: "debug_response", ID: req.ID, OK: true,
			Output: string(outBytes),
		}
		respBytes, _ := json.Marshal(resp)
		_, _ = c.Write(append(respBytes, '\n'))
	}()
	return ln
}

func TestQuerySessions(t *testing.T) {
	sock := shortSocketPath(t, "d.sock")
	want := []Session{
		{SessionID: "a", Status: "running", IsProcessing: true, WorkingDir: "/x"},
		{SessionID: "b", Status: "ready", IsProcessing: false, WorkingDir: "/y"},
	}
	ln := fakeDebugServer(t, sock, want)
	defer ln.Close()

	got, err := QuerySessions(context.Background(), sock)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(got) != 2 || got[0].SessionID != "a" || got[1].SessionID != "b" {
		t.Fatalf("unexpected: %+v", got)
	}
	if !got[0].IsProcessing || got[1].IsProcessing {
		t.Fatalf("is_processing mismatch: %+v", got)
	}
}

func TestQuerySessionsDropsEmptyIDs(t *testing.T) {
	sock := shortSocketPath(t, "d.sock")
	in := []Session{
		{SessionID: "", Status: "ready"},
		{SessionID: "ok", Status: "ready"},
	}
	ln := fakeDebugServer(t, sock, in)
	defer ln.Close()

	got, err := QuerySessions(context.Background(), sock)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].SessionID != "ok" {
		t.Fatalf("got %+v", got)
	}
}

func TestQuerySessionsDialError(t *testing.T) {
	_, err := QuerySessions(context.Background(), "/nonexistent/sock")
	if err == nil {
		t.Fatal("expected dial error")
	}
}

func TestDiscoverDebugSockets(t *testing.T) {
	dir, err := os.MkdirTemp("/tmp", "jb")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	good := filepath.Join(dir, "g.sock")
	missing := filepath.Join(dir, "m.sock")

	// create a unix socket so DiscoverDebugSockets sees it via os.Stat
	ln, err := net.Listen("unix", good)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()

	sf := map[string]map[string]any{
		"s1": {"socket": "/x", "debug_socket": good, "pid": 1},
		"s2": {"socket": "/x", "debug_socket": missing, "pid": 2},
	}
	b, _ := json.Marshal(sf)
	path := filepath.Join(dir, "servers.json")
	if err := os.WriteFile(path, b, 0o644); err != nil {
		t.Fatal(err)
	}

	got, err := DiscoverDebugSockets(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0] != good {
		t.Fatalf("expected [%s] got %v", good, got)
	}
}

func TestDiscoverDebugSocketsMissingFileIsNotError(t *testing.T) {
	got, err := DiscoverDebugSockets(filepath.Join(t.TempDir(), "nope.json"))
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Fatalf("expected empty got %v", got)
	}
}
