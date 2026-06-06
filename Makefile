.PHONY: build test vet fmt run dry install-launchd uninstall-launchd reload status logs clean

BIN       := bin/jcode-agentpet-bridge
PKG       := ./cmd/bridge
SHA       := $(shell git rev-parse --short HEAD 2>/dev/null || echo dev)
LDFLAGS   := -s -w -X main.Version=$(SHA)
PLIST_SRC := launchd/io.local.jcode-agentpet-bridge.plist
PLIST_DST := $(HOME)/Library/LaunchAgents/io.local.jcode-agentpet-bridge.plist
LABEL     := io.local.jcode-agentpet-bridge

build:
	@mkdir -p bin
	CGO_ENABLED=0 go build -trimpath -ldflags "$(LDFLAGS)" -o $(BIN) $(PKG)
	@ls -lh $(BIN)

test:
	go test ./... -race -count=1

vet:
	go vet ./...
	@diff=$$(gofmt -l .); if [ -n "$$diff" ]; then echo "gofmt diff in:"; echo "$$diff"; exit 1; fi

fmt:
	gofmt -w .

run: build
	./$(BIN) -v

dry: build
	./$(BIN) --dry-run --once -v

install-launchd: build uninstall-launchd
	@mkdir -p $(HOME)/Library/LaunchAgents
	sed 's|BIN_PATH|$(abspath $(BIN))|g' $(PLIST_SRC) > $(PLIST_DST)
	launchctl bootstrap gui/$$(id -u) $(PLIST_DST)
	@echo "Installed. Tail /tmp/jcode-agentpet-bridge.log to verify."

uninstall-launchd:
	-launchctl bootout gui/$$(id -u)/$(LABEL) 2>/dev/null
	-rm -f $(PLIST_DST)

reload: uninstall-launchd install-launchd

status:
	@launchctl print gui/$$(id -u)/$(LABEL) 2>/dev/null | head -40 || echo "not loaded"

logs:
	@touch /tmp/jcode-agentpet-bridge.log
	tail -F /tmp/jcode-agentpet-bridge.log

clean:
	rm -rf bin
