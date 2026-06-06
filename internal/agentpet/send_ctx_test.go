package agentpet

import (
	"context"
	"os"
	"testing"
	"time"
)

// Send with an already-cancelled context must skip the socket dial entirely
// and queue the event instead. Verifies the ctx.Err() short-circuit.
func TestSendCancelledCtxSkipsSocket(t *testing.T) {
	sockPath := shortSocketPath(t, "ap.sock")
	queueDir := t.TempDir()

	// Start a server that WOULD accept, but we'll never reach it.
	ch := make(chan Event, 1)
	ln := fakeServer(t, sockPath, ch)
	defer ln.Close()

	c, err := New(sockPath, queueDir)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already done

	start := time.Now()
	via, err := c.Send(ctx, Event{SessionID: "s", AgentKind: "cli", EventName: "done"})
	dur := time.Since(start)
	if err != nil {
		t.Fatalf("send: %v", err)
	}
	if via {
		t.Fatal("expected queue fallback when ctx done")
	}
	if dur > 100*time.Millisecond {
		t.Fatalf("cancelled ctx took %v, expected < 100ms", dur)
	}
	entries, _ := os.ReadDir(queueDir)
	if len(entries) != 1 {
		t.Fatalf("expected 1 queue file, got %d", len(entries))
	}
	select {
	case got := <-ch:
		t.Fatalf("server should not receive when ctx done, got %+v", got)
	case <-time.After(50 * time.Millisecond):
	}
}

// Fast dial error (ENOENT on missing dir) must still queue the event and
// honour ctx deadline rather than blocking on the 500ms dialer timeout.
func TestSendFastDialErrorStillQueues(t *testing.T) {
	queueDir := t.TempDir()
	c, err := New("/no/such/dir/ap.sock", queueDir)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	start := time.Now()
	via, err := c.Send(ctx, Event{SessionID: "s", EventName: "done"})
	dur := time.Since(start)
	if err != nil {
		t.Fatalf("send: %v", err)
	}
	if via {
		t.Fatal("expected queue fallback")
	}
	if dur > 250*time.Millisecond {
		t.Fatalf("dial took %v, expected fast ENOENT", dur)
	}
	entries, _ := os.ReadDir(queueDir)
	if len(entries) != 1 {
		t.Fatalf("expected queued event, got %d files", len(entries))
	}
}
