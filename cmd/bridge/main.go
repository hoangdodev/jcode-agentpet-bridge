// Command bridge polls every jcode debug socket every interval and forwards
// session state transitions to AgentPet.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"slices"
	"strings"
	"sync"
	"syscall"
	"time"
	"unicode/utf8"

	"github.com/hoangdodev/jcode-agentpet-bridge/internal/agentpet"
	"github.com/hoangdodev/jcode-agentpet-bridge/internal/jcode"
	"github.com/hoangdodev/jcode-agentpet-bridge/internal/reconcile"
)

// Version is the build-time SHA, set via -ldflags. Defaults to "dev" so
// `go run` still works.
var Version = "dev"

type config struct {
	serversJSON string
	socketPath  string
	queueDir    string
	interval    time.Duration
	dryRun      bool
	once        bool
	verbose     bool
	showVersion bool
	pollTimeout time.Duration
}

func parseFlags(args []string) (*config, error) {
	fs := flag.NewFlagSet("bridge", flag.ContinueOnError)
	defaultServers, _ := jcode.DefaultServersPath()

	c := &config{}
	fs.StringVar(&c.serversJSON, "servers-json", defaultServers, "path to ~/.jcode/servers.json")
	fs.StringVar(&c.socketPath, "agentpet-socket", "", "AgentPet socket path (default ~/.agentpet/agentpet.sock)")
	fs.StringVar(&c.queueDir, "agentpet-queue", "", "AgentPet queue dir (default ~/.agentpet/queue)")
	fs.DurationVar(&c.interval, "interval", time.Second, "poll interval")
	fs.DurationVar(&c.pollTimeout, "poll-timeout", 2*time.Second, "per-poll deadline")
	fs.BoolVar(&c.dryRun, "dry-run", false, "log decisions without sending events")
	fs.BoolVar(&c.once, "once", false, "poll one tick and exit")
	fs.BoolVar(&c.verbose, "v", false, "verbose logging (debug level)")
	fs.BoolVar(&c.showVersion, "version", false, "print version and exit")

	if err := fs.Parse(args); err != nil {
		return nil, err
	}
	return c, nil
}

func newLogger(verbose bool) *slog.Logger {
	level := slog.LevelInfo
	if verbose {
		level = slog.LevelDebug
	}
	h := slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level})
	return slog.New(h)
}

func run(ctx context.Context, c *config, logger *slog.Logger) error {
	client, err := agentpet.New(c.socketPath, c.queueDir)
	if err != nil {
		return fmt.Errorf("agentpet client: %w", err)
	}
	if err := client.Ping(); err != nil {
		logger.Warn("agentpet socket not reachable at startup; events will queue",
			"err", err)
	}

	r := reconcile.New()
	ticker := time.NewTicker(c.interval)
	defer ticker.Stop()

	tick := func() {
		socks, err := jcode.DiscoverDebugSockets(c.serversJSON)
		if err != nil {
			logger.Warn("discover servers failed", "err", err)
			return // keep polling
		}
		if len(socks) == 0 {
			logger.Debug("no jcode debug sockets visible (set display.debug_socket = true)")
			return
		}
		sources := pollAll(ctx, socks, c.pollTimeout, logger)
		emits := r.StepSources(sources)
		for _, e := range emits {
			if c.dryRun {
				logger.Info("DRY",
					"event", e.Event,
					"session", e.Session.SessionID,
					"wd", e.Session.WorkingDir)
				continue
			}
			evt := agentpet.Event{
				SessionID: "jcode:" + e.Session.SessionID,
				AgentKind: "cli",
				EventName: e.Event,
				Project:   e.Session.WorkingDir,
				Message:   composeMessage(e.Session),
			}
			via, err := client.Send(ctx, evt)
			if err != nil {
				logger.Warn("send failed",
					"event", e.Event,
					"session", e.Session.SessionID,
					"err", err)
				continue
			}
			logger.Info("emit",
				"event", e.Event,
				"session", e.Session.SessionID,
				"wd", e.Session.WorkingDir,
				"via", deliveryLabel(via))
		}
	}

	tick()
	if c.once {
		return nil
	}

	for {
		select {
		case <-ctx.Done():
			flushFinalDone(client, r, c.dryRun, logger)
			return nil
		case <-ticker.C:
			tick()
		}
	}
}

func composeMessage(s jcode.Session) string {
	bits := make([]string, 0, 3)
	if s.FriendlyName != "" {
		bits = append(bits, s.FriendlyName)
	}
	if s.Model != "" {
		bits = append(bits, s.Model)
	}
	if s.Detail != "" {
		bits = append(bits, truncateRunes(s.Detail, 80))
	}
	return strings.Join(bits, " | ")
}

// truncateRunes returns s truncated to at most maxRunes runes. Byte slicing
// would cut multi-byte runes and inject U+FFFD into the JSON payload (which
// shows as mojibake in AgentPet). 80 runes still fits in any sane label.
func truncateRunes(s string, maxRunes int) string {
	if utf8.RuneCountInString(s) <= maxRunes {
		return s
	}
	r := []rune(s)
	return string(r[:maxRunes])
}

func deliveryLabel(viaSocket bool) string {
	if viaSocket {
		return "socket"
	}
	return "queue"
}

// pollAll queries every debug socket concurrently and returns one Source per
// SUCCESSFULLY-polled socket. Sockets whose query failed are omitted so the
// reconciler does not treat their sessions as vanished.
func pollAll(ctx context.Context, socks []string, perPoll time.Duration, logger *slog.Logger) []reconcile.Source {
	type result struct {
		sock string
		s    []jcode.Session
		err  error
	}
	out := make(chan result, len(socks))
	var wg sync.WaitGroup
	for _, sock := range socks {
		wg.Add(1)
		go func(sock string) {
			defer wg.Done()
			pctx, cancel := context.WithTimeout(ctx, perPoll)
			defer cancel()
			s, err := jcode.QuerySessions(pctx, sock)
			out <- result{sock: sock, s: s, err: err}
		}(sock)
	}
	wg.Wait()
	close(out)

	sources := make([]reconcile.Source, 0, len(socks))
	failed := 0
	for r := range out {
		if r.err != nil {
			logger.Debug("query failed", "sock", r.sock, "err", r.err)
			failed++
			continue
		}
		sources = append(sources, reconcile.Source{Sock: r.sock, Sessions: r.s})
	}
	if failed == len(socks) && len(socks) > 0 {
		logger.Warn("all debug sockets failed this tick", "count", failed)
	}
	// Deterministic order so log lines line up across ticks.
	slices.SortFunc(sources, func(a, b reconcile.Source) int {
		return strings.Compare(a.Sock, b.Sock)
	})
	for i := range sources {
		slices.SortFunc(sources[i].Sessions, func(a, b jcode.Session) int {
			return strings.Compare(a.SessionID, b.SessionID)
		})
	}
	return sources
}

// flushFinalDone emits a single `done` for every still-tracked session so the
// pet does not stay "working" forever after we exit. The session snapshot is
// reused so the event carries the last-seen WorkingDir / FriendlyName.
//
// The whole flush is bounded by flushTimeout (default 2s, well under
// launchd's 20s default ExitTimeOut, see launchd.plist(5)) so a wedged
// AgentPet socket can never delay shutdown past the launchd budget; any
// unsent events fall through to the queue directory.
func flushFinalDone(client *agentpet.Client, r *reconcile.Reconciler, dryRun bool, logger *slog.Logger) {
	tracked := r.Tracked()
	if len(tracked) == 0 {
		return
	}
	logger.Info("shutdown flush", "sessions", len(tracked))
	const flushTimeout = 2 * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), flushTimeout)
	defer cancel()
	for _, sid := range tracked {
		if dryRun {
			logger.Info("DRY shutdown",
				"event", reconcile.EventDone,
				"session", sid)
			continue
		}
		snap := r.Snapshot(sid)
		_, _ = client.Send(ctx, agentpet.Event{
			SessionID: "jcode:" + sid,
			AgentKind: "cli",
			EventName: reconcile.EventDone,
			Project:   snap.WorkingDir,
			Message:   "bridge shutdown",
		})
	}
}

func main() {
	c, err := parseFlags(os.Args[1:])
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			os.Exit(0)
		}
		fmt.Fprintln(os.Stderr, "flag:", err)
		os.Exit(2)
	}
	if c.showVersion {
		fmt.Println(Version)
		return
	}
	logger := newLogger(c.verbose)
	logger.Info("bridge starting",
		"version", Version,
		"servers_json", c.serversJSON,
		"interval", c.interval)

	ctx, cancel := signal.NotifyContext(context.Background(),
		os.Interrupt, syscall.SIGTERM)
	defer cancel()

	if err := run(ctx, c, logger); err != nil {
		logger.Error("bridge exiting on error", "err", err)
		os.Exit(1)
	}
}
