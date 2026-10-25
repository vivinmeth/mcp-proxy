package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/http/httptest"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mark3labs/mcp-go/client/transport"
	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
)

func TestMCPHTTPClientRequestsUncompressedResponses(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get(acceptEncodingHeader); got != "identity" {
			http.Error(w, "expected Accept-Encoding: identity, got "+got, http.StatusBadRequest)
			return
		}
		if got := r.Header.Get("X-Test"); got != "value" {
			http.Error(w, "expected X-Test header, got "+got, http.StatusBadRequest)
			return
		}

		var request struct {
			ID     json.RawMessage `json:"id"`
			Method string          `json:"method"`
		}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if request.Method != string(mcp.MethodInitialize) {
			w.WriteHeader(http.StatusAccepted)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"jsonrpc": "2.0",
			"id":      request.ID,
			"result": map[string]any{
				"protocolVersion": mcp.LATEST_PROTOCOL_VERSION,
				"capabilities":    map[string]any{},
				"serverInfo": map[string]string{
					"name":    "test",
					"version": "test",
				},
			},
		})
	}))
	defer server.Close()

	configuredHeaders := map[string]string{
		"accept-encoding": "gzip",
		"X-Test":          "value",
	}
	mcpClient, err := newMCPClient("test", &MCPClientConfigV2{
		TransportType: MCPClientTypeStreamable,
		URL:           server.URL,
		Headers:       configuredHeaders,
	})
	if err != nil {
		t.Fatalf("newMCPClient: %v", err)
	}
	defer func() { _ = mcpClient.Close() }()

	ctx := t.Context()
	if err := mcpClient.client.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	request := mcp.InitializeRequest{}
	request.Params.ClientInfo = mcp.Implementation{Name: "test", Version: "test"}
	if _, err := mcpClient.client.Initialize(ctx, request); err != nil {
		t.Fatalf("Initialize: %v", err)
	}

	if got := configuredHeaders["accept-encoding"]; got != "gzip" {
		t.Fatalf("configured headers were mutated: got %q", got)
	}
}

// Regression for https://github.com/tbxark/mcp-proxy/issues/66 — JetBrains MCP
// clients reject null resource lists; empty collections must serialize as [].
func TestMCPServerListResourcesReturnsEmptyArrayNotNull(t *testing.T) {
	t.Parallel()

	server, err := newMCPServer("test", &MCPProxyConfigV2{
		Type:    MCPServerTypeStreamable,
		Version: "test",
		BaseURL: "http://localhost:9090",
	}, &MCPClientConfigV2{
		Options: &OptionsV2{},
	})
	if err != nil {
		t.Fatalf("newMCPServer: %v", err)
	}

	assertEmptyJSONArray := func(t *testing.T, raw []byte, field, want string) {
		t.Helper()

		var decoded struct {
			Error *struct {
				Code    int    `json:"code"`
				Message string `json:"message"`
			} `json:"error"`
			Result map[string]json.RawMessage `json:"result"`
		}
		if err := json.Unmarshal(raw, &decoded); err != nil {
			t.Fatalf("unmarshal response: %v\npayload: %s", err, string(raw))
		}
		if decoded.Error != nil {
			t.Fatalf("unexpected JSON-RPC error (code %d): %s\nfull response: %s",
				decoded.Error.Code, decoded.Error.Message, string(raw))
		}

		got, ok := decoded.Result[field]
		if !ok {
			t.Fatalf("response missing result.%s\nfull response: %s", field, string(raw))
		}
		if string(got) != want {
			t.Fatalf("expected result.%s to be %s, got %s\nfull response: %s",
				field, want, string(got), string(raw))
		}
	}

	t.Run("resources/list", func(t *testing.T) {
		resp := server.mcpServer.HandleMessage(context.Background(), []byte(`{
			"jsonrpc": "2.0",
			"id": 1,
			"method": "resources/list",
			"params": {}
		}`))

		raw, err := json.Marshal(resp)
		if err != nil {
			t.Fatalf("marshal response: %v", err)
		}
		assertEmptyJSONArray(t, raw, "resources", "[]")
	})

	t.Run("resources/templates/list", func(t *testing.T) {
		resp := server.mcpServer.HandleMessage(context.Background(), []byte(`{
			"jsonrpc": "2.0",
			"id": 2,
			"method": "resources/templates/list",
			"params": {}
		}`))

		raw, err := json.Marshal(resp)
		if err != nil {
			t.Fatalf("marshal response: %v", err)
		}
		assertEmptyJSONArray(t, raw, "resourceTemplates", "[]")
	})
}

// A downstream that answers "method not found" is alive and simply does not
// implement ping; only a broken connection may mark a client unhealthy.
func TestIsTransportFailure(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "transport closed", err: transport.NewError(errors.New("transport closed")), want: true},
		{name: "wrapped transport error", err: fmt.Errorf("ping: %w", transport.NewError(errors.New("eof"))), want: true},
		{name: "ping unsupported", err: mcp.ErrMethodNotFound, want: false},
		{name: "plain error", err: errors.New("boom"), want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isTransportFailure(tt.err); got != tt.want {
				t.Errorf("isTransportFailure(%v) = %v, want %v", tt.err, got, tt.want)
			}
		})
	}
}

// rawDownstream is a hand-written streamable-http MCP endpoint, so a test can
// control exactly when tools/list answers (or does not). The real mcp-go server
// always answers, which is why the listing-hang case cannot use it.
type rawDownstream struct {
	url            string
	mu             sync.Mutex
	tools          []string
	hangToolsList  bool
	toolsListCalls int

	// Optional descriptor/metadata controls. Zero values preserve the original
	// empty-catalog behaviour.
	toolExecution     map[string]any   // attached to every listed tool when set
	toolsRaw          json.RawMessage  // verbatim tools array; overrides tools when set
	resourceTemplates []map[string]any // raw templates; []any{} when nil
	prompts           []string         // prompt names; none when nil
	resources         []string         // resource URIs; none when nil
	hangMethods       map[string]bool  // methods that accept and never answer

	server *httptest.Server
}

func newRawDownstream(t *testing.T) *rawDownstream {
	t.Helper()

	d := &rawDownstream{}
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusAccepted)
			return
		}
		body, _ := io.ReadAll(r.Body)
		_ = r.Body.Close()

		var req struct {
			ID     json.RawMessage `json:"id"`
			Method string          `json:"method"`
		}
		if err := json.Unmarshal(body, &req); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if req.ID == nil {
			// A notification (e.g. notifications/initialized): accept it.
			w.WriteHeader(http.StatusAccepted)
			return
		}

		if d.shouldHang(req.Method) {
			<-r.Context().Done()
			return
		}

		writeResult := func(result any) {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req.ID,
				"result":  result,
			})
		}

		switch req.Method {
		case "initialize":
			writeResult(map[string]any{
				"protocolVersion": mcp.LATEST_PROTOCOL_VERSION,
				"capabilities":    map[string]any{},
				"serverInfo":      map[string]string{"name": "downstream", "version": "1"},
			})
		case "tools/list":
			d.mu.Lock()
			d.toolsListCalls++
			hang := d.hangToolsList
			names := slices.Clone(d.tools)
			execution := d.toolExecution
			rawTools := slices.Clone(d.toolsRaw)
			d.mu.Unlock()
			if hang {
				<-r.Context().Done()
				return
			}
			// A test that cares about key order needs verbatim JSON: encoding a
			// map would sort the property names and erase the thing under test.
			if rawTools != nil {
				writeResult(map[string]any{"tools": rawTools})
				return
			}
			tools := make([]map[string]any, 0, len(names))
			for _, name := range names {
				tool := map[string]any{
					"name":        name,
					"inputSchema": map[string]any{"type": "object"},
				}
				if execution != nil {
					tool["execution"] = execution
				}
				tools = append(tools, tool)
			}
			writeResult(map[string]any{"tools": tools})
		case "prompts/list":
			d.mu.Lock()
			names := slices.Clone(d.prompts)
			d.mu.Unlock()
			prompts := make([]map[string]any, 0, len(names))
			for _, name := range names {
				prompts = append(prompts, map[string]any{"name": name})
			}
			writeResult(map[string]any{"prompts": prompts})
		case "resources/list":
			d.mu.Lock()
			uris := slices.Clone(d.resources)
			d.mu.Unlock()
			resources := make([]map[string]any, 0, len(uris))
			for _, uri := range uris {
				resources = append(resources, map[string]any{"uri": uri, "name": uri})
			}
			writeResult(map[string]any{"resources": resources})
		case "resources/templates/list":
			d.mu.Lock()
			templates := d.resourceTemplates
			d.mu.Unlock()
			if templates == nil {
				templates = []map[string]any{}
			}
			writeResult(map[string]any{"resourceTemplates": templates})
		default:
			writeResult(map[string]any{})
		}
	})
	s := httptest.NewServer(handler)
	t.Cleanup(s.Close)
	d.url = s.URL
	d.server = s
	return d
}

func (d *rawDownstream) shouldHang(method string) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.hangMethods[method]
}

func (d *rawDownstream) setTools(names ...string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.tools = slices.Clone(names)
}

// setRawTools replaces the tools/list payload with raw JSON, so a test can
// control key order (and anything else an encoded map would normalise).
func (d *rawDownstream) setRawTools(raw string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.toolsRaw = json.RawMessage(raw)
}

func (d *rawDownstream) setPrompts(names ...string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.prompts = slices.Clone(names)
}

func (d *rawDownstream) setResources(uris ...string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.resources = slices.Clone(uris)
}

func (d *rawDownstream) setToolExecution(execution map[string]any) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.toolExecution = execution
}

func (d *rawDownstream) setResourceTemplates(templates ...map[string]any) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.resourceTemplates = slices.Clone(templates)
}

func (d *rawDownstream) hangMethodsNamed(methods ...string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.hangMethods = make(map[string]bool, len(methods))
	for _, method := range methods {
		d.hangMethods[method] = true
	}
}

func (d *rawDownstream) hangToolsListCalls(hang bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.hangToolsList = hang
}

// close shuts the fake downstream down, so a test can observe how a transport
// failure is reported (the URL in the error includes any configured query).
func (d *rawDownstream) close() {
	d.mu.Lock()
	server := d.server
	d.mu.Unlock()
	if server != nil {
		server.Close()
	}
}

// serverToolNames reads back the tools registered on the proxy-side server.
func serverToolNames(t *testing.T, mcpServer *server.MCPServer) []string {
	t.Helper()

	resp := mcpServer.HandleMessage(context.Background(), []byte(`{
		"jsonrpc": "2.0", "id": 1, "method": "tools/list", "params": {}
	}`))
	raw, err := json.Marshal(resp)
	if err != nil {
		t.Fatalf("marshal tools/list response: %v", err)
	}
	var decoded struct {
		Result struct {
			Tools []struct {
				Name string `json:"name"`
			} `json:"tools"`
		} `json:"result"`
	}
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatalf("unmarshal tools/list response: %v", err)
	}
	names := make([]string, 0, len(decoded.Result.Tools))
	for _, tool := range decoded.Result.Tools {
		names = append(names, tool.Name)
	}
	slices.Sort(names)
	return names
}

// serverToolInputSchema reads back the raw inputSchema the proxy-side server
// serves for a tool. It stays raw because a decoded map would lose the property
// key order this is meant to inspect.
func serverToolInputSchema(t *testing.T, mcpServer *server.MCPServer, toolName string) string {
	t.Helper()

	resp := mcpServer.HandleMessage(context.Background(), []byte(`{
		"jsonrpc": "2.0", "id": 1, "method": "tools/list", "params": {}
	}`))
	raw, err := json.Marshal(resp)
	if err != nil {
		t.Fatalf("marshal tools/list response: %v", err)
	}
	var decoded struct {
		Result struct {
			Tools []struct {
				Name        string          `json:"name"`
				InputSchema json.RawMessage `json:"inputSchema"`
			} `json:"tools"`
		} `json:"result"`
	}
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatalf("unmarshal tools/list response: %v", err)
	}
	for _, tool := range decoded.Result.Tools {
		if tool.Name == toolName {
			return string(tool.InputSchema)
		}
	}
	t.Fatalf("tool %q not registered on proxy server", toolName)
	return ""
}

func newProxyServerForTest(t *testing.T) *server.MCPServer {
	t.Helper()

	proxyServer, err := newMCPServer("test", &MCPProxyConfigV2{
		Type:    MCPServerTypeStreamable,
		Version: "1",
		BaseURL: "http://localhost:9090",
	}, &MCPClientConfigV2{Options: &OptionsV2{}})
	if err != nil {
		t.Fatalf("newMCPServer: %v", err)
	}
	return proxyServer.mcpServer
}

// Regression for the reconnect path re-registering a shrunken catalog: AddTool
// is an upsert, so without replace semantics a tool the downstream dropped stays
// exposed. The registration must replace the whole set instead.
func TestCatalogRegistrationDropsStaleToolsOnReRegister(t *testing.T) {
	t.Parallel()

	downstream := newRawDownstream(t)
	downstream.setTools("alpha", "beta")

	mcpClient, err := newMCPClient("test", &MCPClientConfigV2{
		TransportType: MCPClientTypeStreamable,
		URL:           downstream.url,
		Options:       &OptionsV2{},
	})
	if err != nil {
		t.Fatalf("newMCPClient: %v", err)
	}
	defer func() { _ = mcpClient.Close() }()

	proxyServer := newProxyServerForTest(t)
	ctx := t.Context()
	if err := mcpClient.addToMCPServer(ctx, mcp.Implementation{Name: "test"}, proxyServer); err != nil {
		t.Fatalf("addToMCPServer: %v", err)
	}
	if got := serverToolNames(t, proxyServer); !slices.Equal(got, []string{"alpha", "beta"}) {
		t.Fatalf("initial tools = %v, want [alpha beta]", got)
	}

	// The downstream shrank; a reconnect re-runs registration. The dropped tool
	// must not survive.
	downstream.setTools("alpha")
	if err := mcpClient.addToolsToServer(ctx, proxyServer); err != nil {
		t.Fatalf("re-register tools: %v", err)
	}
	if got := serverToolNames(t, proxyServer); !slices.Equal(got, []string{"alpha"}) {
		t.Errorf("tools after re-registration = %v, want [alpha]", got)
	}
}

// The proxy re-serves the downstream's tool descriptors, so the inputSchema it
// publishes must survive the decode/re-encode round trip. mcp-go v1.1.0 began
// recording property key order on decode; before that the schema was rebuilt
// from a map and the downstream's order was replaced with sorted order.
func TestCatalogRegistrationPreservesInputSchemaPropertyOrder(t *testing.T) {
	t.Parallel()

	downstream := newRawDownstream(t)
	// Deliberately not alphabetical: sorted order would be alpha, zeta.
	downstream.setRawTools(`[{"name":"ordered","inputSchema":{"type":"object","properties":{"zeta":{"type":"string"},"alpha":{"type":"number"}},"required":["zeta"]}}]`)

	mcpClient, err := newMCPClient("test", &MCPClientConfigV2{
		TransportType: MCPClientTypeStreamable,
		URL:           downstream.url,
		Options:       &OptionsV2{},
	})
	if err != nil {
		t.Fatalf("newMCPClient: %v", err)
	}
	defer func() { _ = mcpClient.Close() }()

	proxyServer := newProxyServerForTest(t)
	if err := mcpClient.addToMCPServer(t.Context(), mcp.Implementation{Name: "test"}, proxyServer); err != nil {
		t.Fatalf("addToMCPServer: %v", err)
	}

	schema := serverToolInputSchema(t, proxyServer, "ordered")
	zeta := strings.Index(schema, `"zeta"`)
	alpha := strings.Index(schema, `"alpha"`)
	if zeta == -1 || alpha == -1 {
		t.Fatalf("inputSchema lost a property: %s", schema)
	}
	if zeta > alpha {
		t.Errorf("inputSchema property order was normalised: %s", schema)
	}
}

// Regression for the listing half of connect being unbounded: a downstream that
// completes initialize and then never answers tools/list would otherwise wedge
// the startup goroutine (and the retry loop) forever. catalogTimeout must turn
// that into a bounded error.
func TestCatalogTimeoutBoundsHungToolsList(t *testing.T) {
	// Not parallel: it shortens the package-level catalogTimeout.
	old := catalogTimeout
	catalogTimeout = 300 * time.Millisecond
	t.Cleanup(func() { catalogTimeout = old })

	downstream := newRawDownstream(t)
	downstream.setTools("alpha")
	downstream.hangToolsListCalls(true)

	mcpClient, err := newMCPClient("test", &MCPClientConfigV2{
		TransportType: MCPClientTypeStreamable,
		URL:           downstream.url,
	})
	if err != nil {
		t.Fatalf("newMCPClient: %v", err)
	}
	defer func() { _ = mcpClient.Close() }()

	proxyServer := newProxyServerForTest(t)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	start := time.Now()
	err = mcpClient.addToMCPServer(ctx, mcp.Implementation{Name: "test"}, proxyServer)
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("addToMCPServer succeeded despite a tools/list that never answers")
	}
	if elapsed > 3*time.Second {
		t.Errorf("addToMCPServer took %v, want it bounded by catalogTimeout", elapsed)
	}
}

// Regression for a Client built without Options: newMCPClient permits a nil
// Options, and the keepalive task used to dereference it directly, panicking as
// soon as such a client connected. The option accessors are nil-safe, so a
// successful connection must stay up.
func TestNilOptionsClientDoesNotPanicOnConnect(t *testing.T) {
	t.Parallel()

	downstream := newRawDownstream(t)
	downstream.setTools("alpha")

	mcpClient, err := newMCPClient("test", &MCPClientConfigV2{
		TransportType: MCPClientTypeStreamable,
		URL:           downstream.url,
	})
	if err != nil {
		t.Fatalf("newMCPClient: %v", err)
	}
	defer func() { _ = mcpClient.Close() }()

	proxyServer := newProxyServerForTest(t)
	ctx := t.Context()
	if err := mcpClient.addToMCPServer(ctx, mcp.Implementation{Name: "test"}, proxyServer); err != nil {
		t.Fatalf("addToMCPServer with nil Options: %v", err)
	}

	// Let the keepalive task run a tick; a nil dereference there crashes the
	// process rather than this goroutine.
	time.Sleep(100 * time.Millisecond)
}

func TestForwardableMetaStripsProtocolEnvelope(t *testing.T) {
	t.Parallel()

	envelope := map[string]any{
		mcp.MetaKeyProtocolVersion:     "2026-07-28",
		mcp.MetaKeyClientInfo:          map[string]any{"name": "claude-code", "version": "2.1.310"},
		mcp.MetaKeyClientCapabilities:  map[string]any{},
		mcp.MetaKeyLogLevel:            "info",
		"io.modelcontextprotocol/next": true,
	}

	tests := []struct {
		name string
		in   *mcp.Meta
		want *mcp.Meta
	}{
		{name: "nil", in: nil, want: nil},
		{
			name: "only envelope keys is dropped entirely",
			in:   &mcp.Meta{AdditionalFields: maps.Clone(envelope)},
			want: nil,
		},
		{
			name: "progressToken and vendor keys survive",
			in: &mcp.Meta{ProgressToken: "tok-1", AdditionalFields: func() map[string]any {
				fields := maps.Clone(envelope)
				fields["com.example/trace"] = "abc"
				// Only the exact reserved prefix is protocol-owned.
				fields["io.modelcontextprotocol.example/x"] = 1
				return fields
			}()},
			want: &mcp.Meta{ProgressToken: "tok-1", AdditionalFields: map[string]any{
				"com.example/trace":                 "abc",
				"io.modelcontextprotocol.example/x": 1,
			}},
		},
		{
			name: "progressToken alone",
			in:   &mcp.Meta{ProgressToken: 7, AdditionalFields: maps.Clone(envelope)},
			want: &mcp.Meta{ProgressToken: 7},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var before map[string]any
			if tt.in != nil {
				before = maps.Clone(tt.in.AdditionalFields)
			}
			got := forwardableMeta(tt.in)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("forwardableMeta() = %#v, want %#v", got, tt.want)
			}
			// The inbound request is shared with the route server; it must not be
			// mutated in place.
			if tt.in != nil && !reflect.DeepEqual(tt.in.AdditionalFields, before) {
				t.Errorf("forwardableMeta mutated its input: %#v", tt.in.AdditionalFields)
			}
		})
	}
}

// The handler-level check: a tool call, prompt get and resource read carrying a
// 2026-07-28 envelope reach a legacy downstream without it, and with the other
// _meta keys intact.
func TestForwardedRequestsDropProtocolEnvelope(t *testing.T) {
	t.Parallel()

	var (
		mu    sync.Mutex
		metas = map[string]map[string]any{}
	)
	record := func(method string) func(ctx context.Context, id any, message any) {
		return func(ctx context.Context, id any, message any) {
			encoded, _ := json.Marshal(message)
			var decoded struct {
				Params struct {
					Meta map[string]any `json:"_meta"`
				} `json:"params"`
			}
			_ = json.Unmarshal(encoded, &decoded)
			mu.Lock()
			metas[method] = decoded.Params.Meta
			mu.Unlock()
		}
	}
	hooks := &server.Hooks{}
	hooks.AddBeforeCallTool(func(ctx context.Context, id any, message *mcp.CallToolRequest) {
		record("tools/call")(ctx, id, message)
	})
	hooks.AddBeforeGetPrompt(func(ctx context.Context, id any, message *mcp.GetPromptRequest) {
		record("prompts/get")(ctx, id, message)
	})
	hooks.AddBeforeReadResource(func(ctx context.Context, id any, message *mcp.ReadResourceRequest) {
		record("resources/read")(ctx, id, message)
	})

	downstream := server.NewMCPServer("downstream", "1.0.0", server.WithHooks(hooks),
		server.WithToolCapabilities(true), server.WithPromptCapabilities(true), server.WithResourceCapabilities(true, true))
	downstream.AddTool(mcp.NewTool("echo"), func(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		return mcp.NewToolResultText("ok"), nil
	})
	downstream.AddPrompt(mcp.NewPrompt("greet"), func(ctx context.Context, request mcp.GetPromptRequest) (*mcp.GetPromptResult, error) {
		return mcp.NewGetPromptResult("greet", []mcp.PromptMessage{mcp.NewPromptMessage(mcp.RoleUser, mcp.NewTextContent("hi"))}), nil
	})
	downstream.AddResource(mcp.NewResource("test://r", "r"), func(ctx context.Context, request mcp.ReadResourceRequest) ([]mcp.ResourceContents, error) {
		return []mcp.ResourceContents{mcp.TextResourceContents{URI: "test://r", Text: "r"}}, nil
	})
	// Pin the downstream to an older revision, like the hosted servers that
	// reject a body envelope disagreeing with the MCP-Protocol-Version header.
	httpServer := httptest.NewServer(server.NewStreamableHTTPServer(downstream,
		server.WithStateLess(true), server.WithStreamableHTTPProtocolVersions(mcp.LATEST_LEGACY_PROTOCOL_VERSION)))
	t.Cleanup(httpServer.Close)

	c, err := newMCPClient("legacy", &MCPClientConfigV2{
		TransportType: MCPClientTypeStreamable,
		URL:           httpServer.URL,
		Options:       &OptionsV2{},
	})
	if err != nil {
		t.Fatalf("newMCPClient: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	route := server.NewMCPServer("legacy", "1.0.0", server.WithResourceCapabilities(true, true))
	if err := c.connect(t.Context(), mcp.Implementation{Name: "proxy", Version: "1"}, route); err != nil {
		t.Fatalf("connect: %v", err)
	}
	if v := c.getClient().ProtocolVersion(); mcp.IsModernProtocol(v) {
		t.Fatalf("downstream negotiated %q, want a legacy revision for this test", v)
	}

	inbound := func() *mcp.Meta {
		return &mcp.Meta{ProgressToken: "tok", AdditionalFields: map[string]any{
			mcp.MetaKeyProtocolVersion:    "2026-07-28",
			mcp.MetaKeyClientInfo:         map[string]any{"name": "claude-code", "version": "2.1.310"},
			mcp.MetaKeyClientCapabilities: map[string]any{},
			"com.example/trace":           "abc",
		}}
	}

	toolRequest := mcp.CallToolRequest{}
	toolRequest.Params.Name = "echo"
	toolRequest.Params.Meta = inbound()
	if _, err := c.callTool(t.Context(), toolRequest); err != nil {
		t.Fatalf("callTool: %v", err)
	}
	if _, ok := toolRequest.Params.Meta.AdditionalFields[mcp.MetaKeyProtocolVersion]; !ok {
		t.Error("callTool mutated the caller's _meta")
	}

	promptRequest := mcp.GetPromptRequest{}
	promptRequest.Params.Name = "greet"
	promptRequest.Params.Meta = inbound()
	prompt, ok := route.ListPrompts()["greet"]
	if !ok {
		t.Fatal("prompt greet was not registered on the route")
	}
	if _, err := prompt.Handler(t.Context(), promptRequest); err != nil {
		t.Fatalf("get prompt: %v", err)
	}

	resourceRequest := mcp.ReadResourceRequest{}
	resourceRequest.Params.URI = "test://r"
	resourceRequest.Params.Meta = inbound()
	if _, err := c.readResource(t.Context(), resourceRequest); err != nil {
		t.Fatalf("readResource: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	for _, method := range []string{"tools/call", "prompts/get", "resources/read"} {
		meta, ok := metas[method]
		if !ok {
			t.Errorf("%s never reached the downstream", method)
			continue
		}
		for key := range meta {
			if strings.HasPrefix(key, protocolMetaPrefix) {
				t.Errorf("%s forwarded envelope key %q", method, key)
			}
		}
		// The progress token is kept, but as the proxy's own (see progressRelay).
		token, _ := meta["progressToken"].(string)
		if !strings.HasPrefix(token, progressTokenPrefix) || meta["com.example/trace"] != "abc" {
			t.Errorf("%s _meta = %v, want a progressToken and the vendor key kept", method, meta)
		}
	}
}
