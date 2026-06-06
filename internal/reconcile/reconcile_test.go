package reconcile

import (
	"slices"
	"testing"

	"github.com/hoangdodev/jcode-agentpet-bridge/internal/jcode"
)

func sess(id, status string, busy bool) jcode.Session {
	return jcode.Session{
		SessionID: id, Status: status, IsProcessing: busy,
		WorkingDir: "/tmp",
	}
}

// step is a test-only single-source wrapper around StepSources. Production
// always polls multiple sockets so the convenience is not part of the public
// API.
func step(r *Reconciler, sessions []jcode.Session) []Emit {
	return r.StepSources([]Source{{Sock: "single", Sessions: sessions}})
}

func eventNames(es []Emit) []string {
	out := make([]string, len(es))
	for i, e := range es {
		out[i] = e.Event
	}
	return out
}

func TestDeriveEventWorkingWhenProcessing(t *testing.T) {
	if got := DeriveEvent(sess("s", "ready", true)); got != EventWorking {
		t.Fatalf("want working got %s", got)
	}
}

func TestDeriveEventWorkingWhenRunningStatus(t *testing.T) {
	if got := DeriveEvent(sess("s", "running", false)); got != EventWorking {
		t.Fatalf("want working got %s", got)
	}
}

func TestDeriveEventDoneWhenReadyIdle(t *testing.T) {
	if got := DeriveEvent(sess("s", "ready", false)); got != EventDone {
		t.Fatalf("want done got %s", got)
	}
}

func TestNewSessionEmitsRegisteredThenCurrent(t *testing.T) {
	r := New()
	got := eventNames(step(r, []jcode.Session{sess("a", "running", true)}))
	want := []string{"registered", "working"}
	if !equal(got, want) {
		t.Fatalf("got %v want %v", got, want)
	}
}

func TestNewIdleSessionEmitsRegisteredAndDone(t *testing.T) {
	r := New()
	got := eventNames(step(r, []jcode.Session{sess("a", "ready", false)}))
	want := []string{"registered", "done"}
	if !equal(got, want) {
		t.Fatalf("got %v want %v", got, want)
	}
}

func TestNoChangeEmitsNothing(t *testing.T) {
	r := New()
	step(r, []jcode.Session{sess("a", "running", true)})
	got := step(r, []jcode.Session{sess("a", "running", true)})
	if len(got) != 0 {
		t.Fatalf("want 0 emits got %v", got)
	}
}

func TestTransitionWorkingToDoneEmitsOnce(t *testing.T) {
	r := New()
	step(r, []jcode.Session{sess("a", "running", true)})
	got := eventNames(step(r, []jcode.Session{sess("a", "ready", false)}))
	if !equal(got, []string{"done"}) {
		t.Fatalf("first transition got %v", got)
	}
	got2 := step(r, []jcode.Session{sess("a", "ready", false)})
	if len(got2) != 0 {
		t.Fatalf("idempotent failed: %v", got2)
	}
}

func TestDisappearedSessionEmitsFinalDone(t *testing.T) {
	r := New()
	step(r, []jcode.Session{sess("a", "running", true)})
	got := eventNames(step(r, nil))
	if !equal(got, []string{"done"}) {
		t.Fatalf("got %v", got)
	}
	if len(r.Tracked()) != 0 {
		t.Fatalf("session not cleared: %v", r.Tracked())
	}
}

func TestDisappearedAlreadyDoneDoesNotDoubleEmit(t *testing.T) {
	r := New()
	step(r, []jcode.Session{sess("a", "ready", false)}) // registered + done
	got := step(r, nil)
	if len(got) != 0 {
		t.Fatalf("expected no double emit, got %v", got)
	}
}

func TestMultipleSessionsTrackedIndependently(t *testing.T) {
	r := New()
	step(r, []jcode.Session{
		sess("A", "running", true),
		sess("B", "ready", false),
	})
	emits := step(r, []jcode.Session{
		sess("A", "ready", false),
		sess("B", "running", true),
	})
	got := map[string]string{}
	for _, e := range emits {
		got[e.Session.SessionID] = e.Event
	}
	want := map[string]string{"A": "done", "B": "working"}
	if len(got) != len(want) || got["A"] != want["A"] || got["B"] != want["B"] {
		t.Fatalf("got %v want %v", got, want)
	}
}

func TestTracked(t *testing.T) {
	r := New()
	step(r, []jcode.Session{sess("a", "ready", false)})
	if got := r.Tracked(); len(got) != 1 || got[0] != "a" {
		t.Fatalf("expected [a] got %v", got)
	}
	step(r, nil) // session vanishes -> final done, untracked
	if got := r.Tracked(); len(got) != 0 {
		t.Fatalf("expected empty got %v", got)
	}
}

func TestVanishedThenReappearReannouncesRegistered(t *testing.T) {
	// Edge case: a session disappears (final done emitted) then a new TUI with
	// the same id reconnects. We treat it as fresh: registered again.
	r := New()
	step(r, []jcode.Session{sess("a", "running", true)})
	step(r, nil) // gone -> final done
	got := eventNames(step(r, []jcode.Session{sess("a", "running", true)}))
	if !equal(got, []string{"registered", "working"}) {
		t.Fatalf("reappear got %v", got)
	}
}

func equal(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	// Compare in order, since the API guarantees per-session ordering for the
	// same session. For cross-session ordering we sort to keep tests robust.
	aa := slices.Clone(a)
	bb := slices.Clone(b)
	if len(aa) > 0 && aa[0] != bb[0] {
		slices.Sort(aa)
		slices.Sort(bb)
	}
	return slices.Equal(aa, bb)
}
