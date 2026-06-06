// Package agentpet talks to the AgentPet menu bar daemon over its Unix domain
// socket at ~/.agentpet/agentpet.sock, falling back to a queue directory when
// the daemon is offline (matching AgentPet's own EventSender semantics so no
// event is lost across restarts).
package agentpet

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/google/uuid"
)

// Event mirrors AgentPet's AgentEvent JSON shape.
type Event struct {
	SessionID string  `json:"sessionId"`
	AgentKind string  `json:"agentKind"`
	EventName string  `json:"eventName"`
	Project   string  `json:"project,omitempty"`
	Message   string  `json:"message,omitempty"`
	Timestamp float64 `json:"timestamp"`
}

// Client sends events to AgentPet. Safe for concurrent use: each Send dials a
// fresh connection, and the queue fallback uses tmp+rename with a uuid suffix
// so concurrent writers cannot collide.
type Client struct {
	socketPath string
	queueDir   string
	dialer     net.Dialer
}

// New returns a Client. paths default to ~/.agentpet/agentpet.sock and
// ~/.agentpet/queue when empty.
func New(socketPath, queueDir string) (*Client, error) {
	if socketPath == "" || queueDir == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return nil, fmt.Errorf("home dir: %w", err)
		}
		if socketPath == "" {
			socketPath = filepath.Join(home, ".agentpet", "agentpet.sock")
		}
		if queueDir == "" {
			queueDir = filepath.Join(home, ".agentpet", "queue")
		}
	}
	return &Client{
		socketPath: socketPath,
		queueDir:   queueDir,
		dialer:     net.Dialer{Timeout: 500 * time.Millisecond},
	}, nil
}

// Send delivers an event over the socket, returning whether it went out over
// the socket (false means it was queued instead). The dial honours ctx via
// DialContext, and the write deadline is clamped to the earlier of 500ms or
// ctx.Deadline(). If ctx is already done before the socket write, the event
// is queued without a dial attempt.
//
// Safe for concurrent use: each call dials a fresh connection, and the queue
// fallback uses tmp+rename with a uuid suffix so concurrent writers cannot
// collide.
func (c *Client) Send(ctx context.Context, evt Event) (deliveredViaSocket bool, err error) {
	if evt.Timestamp == 0 {
		evt.Timestamp = float64(time.Now().UnixNano()) / 1e9
	}
	line, err := encodeLine(evt)
	if err != nil {
		return false, fmt.Errorf("encode: %w", err)
	}
	if ctx.Err() == nil {
		if writeErr := c.writeSocket(ctx, line); writeErr == nil {
			return true, nil
		}
	}
	// queue fallback is fast (local file rename) so we always attempt it even
	// when ctx is done; losing events at shutdown is worse than a few extra ms.
	if qErr := c.writeQueue(line); qErr != nil {
		return false, fmt.Errorf("socket and queue both failed: %w", qErr)
	}
	return false, nil
}

func encodeLine(evt Event) ([]byte, error) {
	b, err := json.Marshal(evt)
	if err != nil {
		return nil, err
	}
	return append(b, '\n'), nil
}

func (c *Client) writeSocket(ctx context.Context, line []byte) error {
	conn, err := c.dialer.DialContext(ctx, "unix", c.socketPath)
	if err != nil {
		return err
	}
	defer conn.Close()
	// Clamp write deadline to min(500ms, ctx.Deadline()).
	deadline := time.Now().Add(500 * time.Millisecond)
	if cd, ok := ctx.Deadline(); ok && cd.Before(deadline) {
		deadline = cd
	}
	_ = conn.SetWriteDeadline(deadline)
	if _, err := conn.Write(line); err != nil {
		return err
	}
	return nil
}

func (c *Client) writeQueue(line []byte) error {
	// 0o700 dir + 0o600 file: queue payloads embed the user's session metadata
	// (FriendlyName, raw prompt slice via Detail) so we keep them owner-only,
	// matching the threat model of jcode's debug socket (also 0o600).
	if err := os.MkdirAll(c.queueDir, 0o700); err != nil {
		return err
	}
	// Nanoseconds + uuid avoid collisions on burst writes and give the
	// AgentPet daemon a strict total order when it drains the queue.
	name := fmt.Sprintf("%d-%s.json", time.Now().UnixNano(), uuid.NewString())
	full := filepath.Join(c.queueDir, name)
	tmp := full + ".tmp"
	if err := os.WriteFile(tmp, line, 0o600); err != nil {
		return err
	}
	if err := os.Rename(tmp, full); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return nil
}

// Ping tries to connect without writing, to detect daemon health at boot.
// A non-nil return means the AgentPet socket is currently unreachable; the
// caller should treat events as queued until the daemon comes up.
func (c *Client) Ping() error {
	conn, err := c.dialer.Dial("unix", c.socketPath)
	if err != nil {
		return fmt.Errorf("agentpet socket unreachable: %w", err)
	}
	_ = conn.Close()
	return nil
}
