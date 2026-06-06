// Package reconcile diffs a snapshot of jcode sessions against the last known
// per-session event and produces the list of state changes to forward to
// AgentPet.
package reconcile

import (
	"sort"

	"github.com/hoangdodev/jcode-agentpet-bridge/internal/jcode"
)

// Event names AgentPet understands when --agent cli is used. They match
// AgentPetCore.AgentState raw values, so StateMapper short-circuits in the
// daemon.
const (
	EventRegistered = "registered"
	EventWorking    = "working"
	EventDone       = "done"
)

// Emit is one outbound forwarding decision.
type Emit struct {
	Event   string
	Session jcode.Session
}

// Source is one debug socket's tick result: the socket address it came from
// and the sessions it reported. Callers pass ONE Source per socket they
// successfully polled this tick; sockets that failed must be omitted, not
// included with an empty Sessions slice (otherwise the reconciler would treat
// the socket's previous sessions as vanished).
type Source struct {
	Sock     string
	Sessions []jcode.Session
}

// Reconciler keeps per-session last-emitted state, last-seen snapshot, and the
// owning source socket so transient socket failures do not look like vanished
// sessions.
//
// A Reconciler is NOT safe for concurrent use. The bridge's main loop owns
// exactly one and calls StepSources / Tracked / Snapshot / Forget serially
// from a single goroutine. Callers that need concurrent access must wrap it
// with their own synchronisation.
type Reconciler struct {
	last  map[string]string        // sid -> last emitted event
	known map[string]struct{}      // sid presence
	src   map[string]string        // sid -> owning source sock (for vanish gating)
	snap  map[string]jcode.Session // sid -> last-seen session (for vanish payload)
}

// New returns a fresh reconciler.
func New() *Reconciler {
	return &Reconciler{
		last:  make(map[string]string),
		known: make(map[string]struct{}),
		src:   make(map[string]string),
		snap:  make(map[string]jcode.Session),
	}
}

// DeriveEvent maps a session row to its current normalised state.
func DeriveEvent(s jcode.Session) string {
	if s.IsProcessing || s.Status == "running" {
		return EventWorking
	}
	return EventDone
}

// StepSources compares the new per-source snapshot against the cache and
// returns the events that must be forwarded. Sessions belonging to sources NOT
// passed in this call are NOT considered vanished (their source is assumed to
// be transiently unreachable). It mutates the reconciler's cache.
func (r *Reconciler) StepSources(sources []Source) []Emit {
	healthy := make(map[string]struct{}, len(sources))
	for _, src := range sources {
		healthy[src.Sock] = struct{}{}
	}

	seen := make(map[string]struct{})
	out := make([]Emit, 0, 4)

	for _, src := range sources {
		for _, s := range src.Sessions {
			if s.SessionID == "" {
				continue
			}
			// Defensive dedup: if the same id appears more than once in this
			// tick (e.g. from two sockets), keep the first and skip the rest.
			if _, dup := seen[s.SessionID]; dup {
				continue
			}
			seen[s.SessionID] = struct{}{}

			current := DeriveEvent(s)
			r.src[s.SessionID] = src.Sock
			r.snap[s.SessionID] = s

			if _, isKnown := r.known[s.SessionID]; !isKnown {
				out = append(out, Emit{Event: EventRegistered, Session: s})
				r.known[s.SessionID] = struct{}{}
				r.last[s.SessionID] = EventRegistered
				if current != EventRegistered {
					out = append(out, Emit{Event: current, Session: s})
					r.last[s.SessionID] = current
				}
				continue
			}
			if r.last[s.SessionID] != current {
				out = append(out, Emit{Event: current, Session: s})
				r.last[s.SessionID] = current
			}
		}
	}

	// Vanished sessions get a final done unless they were already done. Only
	// vanish sessions whose owning source was actually polled this tick;
	// sessions from non-healthy sources stay tracked.
	for sid := range r.known {
		if _, alive := seen[sid]; alive {
			continue
		}
		owner, hasOwner := r.src[sid]
		if hasOwner {
			if _, ok := healthy[owner]; !ok {
				// Owning source was not polled this tick -> keep tracked.
				continue
			}
		}
		if r.last[sid] != EventDone {
			snap := r.snap[sid]
			snap.SessionID = sid
			snap.Status = "closed"
			out = append(out, Emit{Event: EventDone, Session: snap})
		}
		delete(r.known, sid)
		delete(r.last, sid)
		delete(r.src, sid)
		delete(r.snap, sid)
	}

	return out
}

// Tracked returns the session IDs the reconciler currently knows about, sorted
// for deterministic logging. Useful for graceful shutdown to flush final `done`
// for everything.
func (r *Reconciler) Tracked() []string {
	out := make([]string, 0, len(r.known))
	for sid := range r.known {
		out = append(out, sid)
	}
	sort.Strings(out)
	return out
}

// Snapshot returns the last-seen Session row for sid, or zero if unknown.
// Useful for shutdown flush so the final done carries WorkingDir / metadata.
func (r *Reconciler) Snapshot(sid string) jcode.Session {
	return r.snap[sid]
}
