package reconcile

import (
	"testing"

	"github.com/hoangdodev/jcode-agentpet-bridge/internal/jcode"
)

// Bug #1: when a polling source fails transiently, sessions it owned should
// NOT be considered vanished. The caller signals failure by OMITTING that
// source from the StepSources call.
func TestPartialSocketFailureDoesNotVanish(t *testing.T) {
	r := New()
	srcA, srcB := "/tmp/jcode-a.sock", "/tmp/jcode-b.sock"

	r.StepSources([]Source{
		{Sock: srcA, Sessions: []jcode.Session{
			{SessionID: "A1", Status: "running", IsProcessing: true, WorkingDir: "/a"},
		}},
		{Sock: srcB, Sessions: []jcode.Session{
			{SessionID: "B1", Status: "running", IsProcessing: true, WorkingDir: "/b"},
		}},
	})

	// Tick 2: only srcA polled successfully; srcB is omitted (transient fail).
	emits := r.StepSources([]Source{
		{Sock: srcA, Sessions: []jcode.Session{
			{SessionID: "A1", Status: "running", IsProcessing: true, WorkingDir: "/a"},
		}},
	})
	for _, e := range emits {
		if e.Session.SessionID == "B1" {
			t.Fatalf("B1 should NOT vanish on transient srcB failure, got %q",
				e.Event)
		}
	}

	// Tick 3: srcB recovers, B1 still there. Should be silent.
	emits = r.StepSources([]Source{
		{Sock: srcA, Sessions: []jcode.Session{
			{SessionID: "A1", Status: "running", IsProcessing: true, WorkingDir: "/a"},
		}},
		{Sock: srcB, Sessions: []jcode.Session{
			{SessionID: "B1", Status: "running", IsProcessing: true, WorkingDir: "/b"},
		}},
	})
	for _, e := range emits {
		if e.Session.SessionID == "B1" {
			t.Fatalf("B1 should be silent on recovery, got %q", e.Event)
		}
	}

	// Tick 4: srcB recovered AND reports B1 truly gone. Vanish allowed.
	emits = r.StepSources([]Source{
		{Sock: srcA, Sessions: []jcode.Session{
			{SessionID: "A1", Status: "running", IsProcessing: true, WorkingDir: "/a"},
		}},
		{Sock: srcB, Sessions: nil},
	})
	var got Emit
	for _, e := range emits {
		if e.Session.SessionID == "B1" {
			got = e
		}
	}
	if got.Event != EventDone {
		t.Fatalf("expected B1 done on real vanish, got %+v", got)
	}
	if got.Session.WorkingDir != "/b" {
		t.Fatalf("bug #2: final done lost WorkingDir, got %q",
			got.Session.WorkingDir)
	}
}

// Bug #2: final done emitted for a vanished session must carry the last-seen
// WorkingDir + FriendlyName so AgentPet knows which pet to mark idle.
func TestVanishedDonePreservesSessionFields(t *testing.T) {
	r := New()
	src := "/tmp/jcode-x.sock"
	full := jcode.Session{
		SessionID:    "X",
		Status:       "running",
		IsProcessing: true,
		WorkingDir:   "/Users/me/Projects/foo",
		FriendlyName: "foo-session",
		Model:        "claude-sonnet",
	}

	r.StepSources([]Source{{Sock: src, Sessions: []jcode.Session{full}}})

	emits := r.StepSources([]Source{{Sock: src, Sessions: nil}})
	if len(emits) != 1 || emits[0].Event != EventDone {
		t.Fatalf("expected single done, got %+v", emits)
	}
	got := emits[0].Session
	if got.WorkingDir != full.WorkingDir {
		t.Fatalf("WorkingDir lost: %q", got.WorkingDir)
	}
	if got.FriendlyName != full.FriendlyName {
		t.Fatalf("FriendlyName lost: %q", got.FriendlyName)
	}
}

// Snapshot must return the last-seen row for tracked sessions so shutdown
// flush can populate the event with WorkingDir.
func TestSnapshotReturnsLastSeen(t *testing.T) {
	r := New()
	r.StepSources([]Source{{Sock: "single", Sessions: []jcode.Session{
		{SessionID: "X", WorkingDir: "/p1", IsProcessing: true},
	}}})
	r.StepSources([]Source{{Sock: "single", Sessions: []jcode.Session{
		{SessionID: "X", WorkingDir: "/p2", IsProcessing: true},
	}}})
	if got := r.Snapshot("X").WorkingDir; got != "/p2" {
		t.Fatalf("expected /p2 got %q", got)
	}
	if got := r.Snapshot("missing").SessionID; got != "" {
		t.Fatalf("unknown sid should return zero value, got %+v",
			r.Snapshot("missing"))
	}
}

// Bug #5: same session id appearing in multiple sources within one tick must
// not produce contradictory or duplicate events. Dedup picks the first
// occurrence.
func TestDuplicateSessionAcrossSourcesDedup(t *testing.T) {
	r := New()
	srcA, srcB := "/tmp/jcode-a.sock", "/tmp/jcode-b.sock"
	emits := r.StepSources([]Source{
		{Sock: srcA, Sessions: []jcode.Session{
			{SessionID: "Dup", Status: "running", IsProcessing: true},
		}},
		{Sock: srcB, Sessions: []jcode.Session{
			{SessionID: "Dup", Status: "ready", IsProcessing: false},
		}},
	})
	got := eventNames(emits)
	// Allowed: registered + working (from srcA which won the dedup).
	if !equal(got, []string{"registered", "working"}) {
		t.Fatalf("expected registered+working from first source, got %v", got)
	}
}
