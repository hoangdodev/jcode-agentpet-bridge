# jcode -> AgentPet bridge

[`jcode`](https://github.com/1jehuang/jcode) (the OSS Rust CLI coding agent) doesn't fire Claude Code hooks, so [`AgentPet`](https://github.com/ntd4996/agentpet) (macOS menu-bar pet that reacts to AI agent state) never lights up for jcode sessions out of the box. This is a tiny userspace daemon that polls every running jcode server's debug socket and forwards normalised state transitions straight into the AgentPet daemon socket.

```
jcode TUI ──> *-debug.sock ──poll 1Hz──> bridge ──unix socket──> ~/.agentpet/agentpet.sock ──> AgentPet menu bar
                                              └─fallback──> ~/.agentpet/queue/*.json (when daemon is down)
```

Single static Go binary, ~3 MB, ~6 MB RSS, zero runtime deps.

## States emitted

Mapped from the `debug_command:sessions` snapshot:

| jcode session                                     | event sent       |
|---------------------------------------------------|------------------|
| first seen                                        | `registered`     |
| `status=running` OR `is_processing=true`          | `working`        |
| `status=ready` AND `is_processing=false`          | `done`           |
| session disappears (TUI closed)                   | `done` (final)   |

Events are diffed against an in-memory cache so the same state is never sent twice in a row. `AgentKind.cli` + `StateMapper.short-circuit` means the raw values pass straight through.

## Requirements

1. `~/.jcode/config.toml` has `display.debug_socket = true` (otherwise the sibling `*-debug.sock` is not created and `sessions` is rejected).
2. AgentPet.app installed and running (or its socket directory present; events queue otherwise).
3. Go 1.22+ to build (only needed once).

## Use

```sh
make build              # produces bin/jcode-agentpet-bridge (~3 MB, static)
make test               # go test ./... -race
make dry                # one-shot, log decisions, send nothing
make run                # foreground daemon, slog text -> stderr
```

Useful flags:

```
-interval 1s            poll interval (default 1s)
-poll-timeout 2s        per-socket query deadline
-dry-run                log what would be sent, send nothing
-once                   tick once and exit
-v                      slog debug level
-version                print build SHA and exit
```

## Launch at login (launchd)

```sh
make install-launchd    # builds, templates HOME_DIR, bootstraps LaunchAgent
make reload             # rebuild + re-bootstrap (use after `git pull`)
make logs               # tail -F /tmp/jcode-agentpet-bridge.log
make uninstall-launchd
```

## Architecture

```
cmd/bridge/main.go             slog, signal.NotifyContext, ticker, SIGTERM flush
internal/jcode/                discover servers.json + query debug socket
internal/agentpet/             direct unix-socket client + queue fallback
internal/reconcile/            session diff -> emit list (pure, 12 unit tests)
launchd/                       LaunchAgent template (HOME_DIR substituted by Makefile)
```

## Design notes

- **Why polling, not subscribe?** jcode's main socket exposes `{"type":"subscribe"}` but it only acks; broadcasts are gated behind self-dev / TUI client flow. The debug socket's `sessions` command gives us everything we need (`session_id`, `status`, `is_processing`, `working_dir`, `friendly_name`, `model`, `detail`) in a single round-trip. 1 Hz is plenty for an ambient pet and costs ~free.

- **Why speak the AgentPet socket directly?** AgentPet's `EventSender` writes newline-delimited JSON (`{sessionId, agentKind, eventName, project, timestamp}`) to `~/.agentpet/agentpet.sock`. The bridge writes the exact same frame, skipping any fork-exec per event.

- **Multiple jcode servers**: bridge reads `~/.jcode/servers.json` every tick and queries every `debug_socket` declared in parallel (one goroutine per server), so multiple `jcode serve` instances all light up the same pet without one slow socket blocking the others.

- **AgentPet daemon offline**: `EventSender` in AgentPet already falls back to `~/.agentpet/queue/*.json` and drains on next launch. The bridge replicates that fallback (atomic `.tmp` -> rename) so no event is lost across either side's restart.

- **Graceful shutdown**: on SIGTERM/SIGINT, every still-tracked session gets a final `done` so the pet doesn't stay "working" forever after the bridge exits.

## Known limitations

- No `waiting` state. jcode doesn't surface "needs permission" in the `sessions` snapshot, so we can't distinguish blocked-on-user from working. Could be added later by tapping `ambient permissions list`.
- Up to ~1 s lag on a transition. Acceptable for an ambient menu-bar pet.
- Requires `display.debug_socket = true`. This also exposes raw TUI state to any local process on the same Mac; same trust boundary as the main jcode socket.

## License

MIT. See [LICENSE](./LICENSE).
