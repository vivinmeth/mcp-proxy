# mcp-proxy
Kind: code · Shape: single · Stack: go · Git mode: trunk

Our fork (`vivinmeth/mcp-proxy`, branch `master`) of tbxark/mcp-proxy: a Go MCP proxy that puts many
downstream MCP servers behind one HTTP entrypoint, each at its own route
(`<baseURL path>/<serverName>/`) over SSE or streamable HTTP. It runs as fleet's MCP gateway on alien
(`mcp.alien.vin`), which holds every upstream login and key. Forked 2026-10-23 at tbxark v1.1.0 for
credit tracking, pooling, affinity and muxing.

## Map

One `main` package, flat layout. New code goes in the root file for its concern; extend it rather
than add a parallel one. File-by-file detail and the test layout: [docs/DEVELOPMENT.md](docs/DEVELOPMENT.md).

| Path | What |
|---|---|
| `main.go` | flags; dispatch to server, `-authorize`, or `-auth-status`/`-doctor` |
| `config.go` | the V2 config schema, validation, loading (file or URL, env expansion) |
| `http.go` | the mux, per-server middleware (recover, logging, bearer auth), startup and shutdown |
| `client.go` | downstream clients (`stdio`, `sse`, `streamable-http`), keepalive, reconnect |
| `progress.go` | progress-token relay |
| `oauth.go`, `oauth_store.go` | downstream OAuth; tokens under `~/.config/mcp-proxy/oauth/` |
| `doctor.go` | `-auth-status` and `-doctor` |
| `*_test.go` | unit tests, next to the code |
| `test/e2e/` | end-to-end tests against the compiled binary (skipped under `-short`) |
| `testdata/stdio-server/` | the downstream MCP fixture |
| `docs/` | `CONFIGURATION.md`, `USAGE.md`, `DEPLOYMENT.md`, `DEVELOPMENT.md`; `index.html` is the Claude-config converter on GitHub Pages |

## Done

- `make format` (go fix/fmt/vet, all tests, `go mod tidy`, golangci-lint fmt and run, nilaway; needs
  `golangci-lint` and `nilaway`). At least `go vet ./...` and `go test ./...` before a change is done.
- While iterating: `go test -short ./...` (unit only), `go test -run TestName .` (one test).
- `make build` puts the binary in `./build/`; `./build/mcp-proxy -check-config --config <file>`
  checks a config.
- A change that ships: build the image, deploy it to the gateway, and call a server through
  `mcp.alien.vin` (Tools).
- Changing flags or config fields: update `docs/CONFIGURATION.md` / `docs/USAGE.md` /
  `docs/DEPLOYMENT.md`.

## Project rules

- **Fork.** `upstream` is tbxark/mcp-proxy: never push to it or open a PR there; our fixes stay in
  the fork. Keep changes small so upstream syncs stay easy.
- **Stay on mcp-go v1.1.0:** v1.3.0 breaks fallback to downstreams that only speak 2025-11-25.

Behaviours to preserve:

- HTTP clients force `Accept-Encoding: identity` (`mcpHTTPHeaders` in `client.go`): some MCP servers
  otherwise send gzip that mcp-go's JSON decoder can't read.
- `/_healthz` and `/_readyz` are unauthenticated. `/_healthz` is liveness. `/_readyz` is 503 until
  every client mounts, and 503 again (`status: degraded`) once a client that had connected fails
  `pingFailureThreshold` probes in a row (one miss is not enough: a busy single-threaded downstream
  just doesn't answer while it works). Clients that never connected are excluded, so one broken
  server doesn't take the proxy out of rotation; if *nothing* mounted, it is 503 `status: unavailable`.
- A failed downstream connection is logged but non-fatal unless that server sets `panicIfInvalid`.
  This covers client *creation* too: stdio spawns its subprocess in `newMCPClient`, so a missing
  command must not abort startup (`clientStartupError` in `http.go` is the single policy point).
- `autoReconnect` (per server, default off) has two timings: the startup goroutine in `http.go`
  retries an unreachable backend every `reconnectInterval` (default 15s) and mounts the route only
  once connected; a connection that drops after being healthy is rebuilt by the ping task after
  `pingFailureThreshold` failed probes, paced by `pingInterval`. `panicIfInvalid` is checked first,
  so retrying never overrides it.
- `Client.Close` swallows `*exec.ExitError`: a stdio server that exits non-zero when stdin closes is
  not a shutdown failure.
- `shutdown` sets `shuttingDown` before walking the clients map, and a startup goroutine that
  finishes after that closes its own client. `newMCPClient` takes no context and has already spawned
  the subprocess, so `cancel()` alone would leak it.
- OAuth dynamic client registration (empty `clientId`) is reloaded from disk on daemon start;
  without it, token refresh silently fails.
- Forwarded tool calls, prompt gets and resource reads drop the client's `io.modelcontextprotocol/*`
  `_meta` keys (`forwardableMeta`): they describe the client↔proxy session, the proxy↔downstream one
  may be older, and a downstream that checks the body against `MCP-Protocol-Version` rejects the
  mismatch. Vendor `_meta` keys pass through.
- A forwarded `progressToken` is replaced with a proxy-unique one (`progressRelay`), and the
  downstream's `notifications/progress` is rewritten back to the originating session: every client
  of a route shares one downstream connection, so client tokens can collide. Stdio clients are never
  `Start`ed through mcp-go's `Client`, so their transport gets the handler directly.
- The e2e tests run the real binary and can't patch package variables: timings they need are config
  fields (`mcpProxy.startupGracePeriod`, `options.pingInterval`).

## Tools

- **Ship to the gateway** (on alien, from this clone): `docker build -t fleet/mcp-proxy:<short-sha> .`
  (no registry, never pull), set the tag in fleet's `fleet-engine/src/mcp/docker-compose.yml`, then
  `fleet mcp deploy`. Gateway details: fleet's `docs/harnesses.md` (MCP).
- **Don't run `make buildImage`:** it pushes to tbxark's ghcr image.
- CI from upstream: `.github/workflows/goreleaser.yml` (on tags) and `docker.yml` (manual, ghcr).
- Skill: `go`.
