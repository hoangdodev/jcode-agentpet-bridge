// Package jcode reads jcode server metadata and queries the debug socket for
// session snapshots used by the bridge.
package jcode

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sort"
	"time"
)

// Session is a row from `debug_command:sessions`. Only fields the bridge
// consumes are kept; unknown fields are tolerated by the JSON decoder.
type Session struct {
	SessionID    string `json:"session_id"`
	Status       string `json:"status"`
	IsProcessing bool   `json:"is_processing"`
	WorkingDir   string `json:"working_dir"`
	FriendlyName string `json:"friendly_name"`
	Model        string `json:"model"`
	Detail       string `json:"detail"`
}

// ServersFile mirrors ~/.jcode/servers.json (only fields we use).
type ServersFile map[string]struct {
	Socket      string `json:"socket"`
	DebugSocket string `json:"debug_socket"`
	PID         int    `json:"pid"`
}

// DiscoverDebugSockets returns the debug_socket paths that actually exist on
// disk. Missing or unreadable servers.json yields an empty list, not an error,
// so the daemon can keep polling.
func DiscoverDebugSockets(serversJSONPath string) ([]string, error) {
	b, err := os.ReadFile(serversJSONPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("read %s: %w", serversJSONPath, err)
	}
	var sf ServersFile
	if err := json.Unmarshal(b, &sf); err != nil {
		return nil, fmt.Errorf("parse %s: %w", serversJSONPath, err)
	}
	var out []string
	for _, s := range sf {
		if s.DebugSocket == "" {
			continue
		}
		if _, err := os.Stat(s.DebugSocket); err == nil {
			out = append(out, s.DebugSocket)
		}
	}
	// Sort so callers see deterministic log lines across ticks (map iteration
	// order is randomised by the runtime).
	sort.Strings(out)
	return out, nil
}

// DefaultServersPath returns ~/.jcode/servers.json.
func DefaultServersPath() (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(home, ".jcode", "servers.json"), nil
}

// debugRequest is the JSON sent over the debug socket.
type debugRequest struct {
	Type    string `json:"type"`
	ID      int    `json:"id"`
	Command string `json:"command"`
}

// debugResponse covers both the `ack` and the `debug_response` envelopes; we
// only act on the latter when ok=true.
type debugResponse struct {
	Type    string `json:"type"`
	ID      int    `json:"id"`
	OK      bool   `json:"ok"`
	Output  string `json:"output"`
	Message string `json:"message"`
}

// QuerySessions sends `debug_command:sessions` and returns the parsed list.
// Times out via ctx + a hard write/read deadline.
func QuerySessions(ctx context.Context, debugSocket string) ([]Session, error) {
	d := net.Dialer{Timeout: 2 * time.Second}
	conn, err := d.DialContext(ctx, "unix", debugSocket)
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", debugSocket, err)
	}
	defer conn.Close()

	deadline := time.Now().Add(2 * time.Second)
	if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
		deadline = d
	}
	_ = conn.SetDeadline(deadline)

	req := debugRequest{Type: "debug_command", ID: 1, Command: "sessions"}
	reqBytes, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	if _, err := conn.Write(append(reqBytes, '\n')); err != nil {
		return nil, fmt.Errorf("write debug_command: %w", err)
	}

	dec := json.NewDecoder(conn)
	for {
		var resp debugResponse
		if err := dec.Decode(&resp); err != nil {
			return nil, fmt.Errorf("decode response: %w", err)
		}
		switch resp.Type {
		case "ack":
			continue
		case "error":
			return nil, fmt.Errorf("debug_command error: %s", resp.Message)
		case "debug_response":
			if !resp.OK {
				return nil, fmt.Errorf("debug_command not ok: %s", resp.Message)
			}
			var sessions []Session
			if err := json.Unmarshal([]byte(resp.Output), &sessions); err != nil {
				return nil, fmt.Errorf("parse sessions output: %w", err)
			}
			// Drop sessions with empty IDs (defensive).
			out := sessions[:0]
			for _, s := range sessions {
				if s.SessionID != "" {
					out = append(out, s)
				}
			}
			return out, nil
		default:
			// Unknown frame, keep reading.
		}
	}
}
