# Development

Reference for working on the proxy's code. The rules every change must keep are in
[AGENTS.md](../AGENTS.md).

## Request flow

MCP client → HTTP mux (`http.go`) → per-server middleware chain (recover, optional logging, optional
bearer-token auth) → `server.MCPServer` (mcp-go) → proxied downstream client (`client.go`).

## Files

- `main.go`: flag parsing and dispatch to one of three modes: normal server, `-authorize`
  (interactive OAuth), or `-auth-status`/`-doctor` (credential diagnostics).
- `config.go`: the only (V2) config schema, validation, and loading via `confstore` (local file or
  HTTP(S) URL, with env-var expansion). Per-server `Options` inherit from `mcpProxy.options` defaults
  during `load()`. The V1 schema is gone; `FullConfig` still carries the old `server`/`clients` keys
  as `json.RawMessage` only so a V1 file fails with a migration hint instead of "mcpProxy is
  required".
- `client.go`: wraps mcp-go clients for the three transports (`stdio`, `sse`, `streamable-http`). On
  startup each client connects, initializes, and copies the downstream server's tools, prompts,
  resources and resource templates onto the local `MCPServer` (with pagination and tool allow/block
  filtering). Every client gets a keepalive probe loop (`options.pingInterval`, default 30s) that
  drives the health state behind `/_readyz`: a real `tools/list` request on modern protocol
  connections (2026-07-28 removed ping), ping on legacy ones. `Client.connect` builds the transport,
  swaps it in under `mu`, and closes the previous one; tool, prompt and resource handlers resolve the
  live transport through `getClient()` at call time, so reconnecting is transparent to in-flight
  requests. `Start` gets the long-lived context (mcp-go's SSE transport ties its event stream to it,
  and stdio stores it for request handling), so only `Initialize` is bounded by `connectTimeout`.
- `progress.go`: `progressRelay`, which swaps a forwarded request's `progressToken` for a
  proxy-unique one and routes the downstream's progress back.
- `oauth.go` / `oauth_store.go`: OAuth client support for downstream servers that need interactive
  auth (for example Notion). Tokens and dynamically registered client credentials persist as JSON
  under the user config dir (`~/.config/mcp-proxy/oauth/` on Linux) via `FileTokenStore`, so a
  one-off `-authorize` run shares state with the daemon. `oauthAwareError` adds re-auth hints to
  connection failures.
- `doctor.go`: backs `-auth-status` (local only: config and cached token expiry) and `-doctor` (also
  connects to each remote server; may refresh tokens but never opens a browser).

## Tests

```
<file>_test.go        unit tests, in the root package next to the code they cover
test/e2e/             end-to-end tests (package e2e), skipped under -short
testdata/stdio-server downstream MCP server fixture
```

Go requires a package's white-box tests to sit in its own directory, so the unit tests stay in the
root. The end-to-end tests drive the **compiled binary** as a subprocess instead of calling into the
package, which is why they can live under `test/e2e/` and run with `t.Parallel`.

- `test/e2e/helpers_test.go`: builds both binaries once, boots the proxy (`startProxy` waits for
  `/_readyz`; `launchProxy` doesn't, for the cases that never become ready), and talks MCP to it.
- `test/e2e/stdio_test.go`: a stdio subprocess proxied over both proxy transports: tool, prompt and
  resource proxying, tool filtering, `env` reaching the subprocess, auth, readiness after a killed
  subprocess, graceful shutdown.
- `test/e2e/remote_test.go`: the outbound half, against an in-process `httptest` MCP server: the
  config's `headers` and `Accept-Encoding: identity` reaching the downstream, a slow server not
  blocking readiness, a vanished one degrading it, and an all-unreachable config reporting
  `unavailable`.
- `test/e2e/cli_test.go`: `-version`, `-check-config`, `-auth-status`, and the `panicIfInvalid`
  startup policy.
- `testdata/stdio-server/`: the fixture (tools, prompts, resources, a template, a failing tool, a
  tool the filter must block, a `pid` tool, and a 2-per-page pagination limit). It sits under
  `testdata` so `go build ./...` and the linters skip it; `go run ./testdata/stdio-server` works for
  manual poking.

Because the e2e tests run the real binary, they cannot patch package variables: the timings they
depend on are config fields (`mcpProxy.startupGracePeriod`, `options.pingInterval`), which the test
configs set to sub-second values.

One trap: mcp-go's client *and* `client.go`'s own loops both follow list cursors, so a pagination
regression only shows up if both break.
