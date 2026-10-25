package e2e

import (
	"context"
	"fmt"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/mark3labs/mcp-go/client"
	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
)

// These tests cover the downstream's notifications/progress reaching the
// client whose request it reports on. Every client of a route shares the
// proxy's one connection to the downstream, so the proxy has to tell callers
// apart even when they chose the same progress token.

// progressLog collects the progress notifications one client connection saw.
type progressLog struct {
	mu   sync.Mutex
	seen []map[string]any
}

func collectProgress(mcpClient *client.Client) *progressLog {
	log := &progressLog{}
	mcpClient.OnNotification(func(n mcp.JSONRPCNotification) {
		if n.Method != string(mcp.MethodNotificationProgress) {
			return
		}
		log.mu.Lock()
		log.seen = append(log.seen, n.Params.AdditionalFields)
		log.mu.Unlock()
	})
	return log
}

// take returns what has been collected so far and resets the log.
func (l *progressLog) take() []map[string]any {
	l.mu.Lock()
	defer l.mu.Unlock()
	seen := l.seen
	l.seen = nil
	return seen
}

// callToolWithProgress calls a tool with a progress token (none if token is
// nil) and returns its text result.
func callToolWithProgress(t *testing.T, mcpClient *client.Client, name string, args map[string]any, token mcp.ProgressToken) string {
	t.Helper()

	request := mcp.CallToolRequest{}
	request.Params.Name = name
	request.Params.Arguments = args
	if token != nil {
		request.Params.Meta = &mcp.Meta{ProgressToken: token}
	}
	result, err := mcpClient.CallTool(testContext(t), request)
	if err != nil {
		t.Fatalf("call %s: %v", name, err)
	}
	return resultText(t, result)
}

// checkProgress asserts seen is steps notifications, in order, all carrying
// the caller's own token.
func checkProgress(t *testing.T, seen []map[string]any, token mcp.ProgressToken, steps int) {
	t.Helper()

	if len(seen) != steps {
		t.Fatalf("got %d progress notifications, want %d: %v", len(seen), steps, seen)
	}
	for i, params := range seen {
		if fmt.Sprint(params["progressToken"]) != fmt.Sprint(token) {
			t.Errorf("progress[%d] token = %v, want the caller's %v", i, params["progressToken"], token)
		}
		if fmt.Sprint(params["progress"]) != fmt.Sprint(i+1) {
			t.Errorf("progress[%d] = %v, want %d", i, params["progress"], i+1)
		}
	}
}

// startProgressDownstream serves a remote MCP server whose "slow" tool reports
// "steps" progress notifications, tagged with the "who" argument, before it
// answers.
func startProgressDownstream(t *testing.T, streamableOptions ...server.StreamableHTTPOption) string {
	t.Helper()

	mcpServer := server.NewMCPServer("downstream", "1.0.0", server.WithToolCapabilities(true))
	mcpServer.AddTool(
		mcp.NewTool("slow", mcp.WithNumber("steps", mcp.Required()), mcp.WithString("who")),
		func(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			steps := request.GetInt("steps", 0)
			who := request.GetString("who", "")
			if meta := request.Params.Meta; meta != nil && meta.ProgressToken != nil {
				for i := 1; i <= steps; i++ {
					time.Sleep(20 * time.Millisecond)
					_ = server.ServerFromContext(ctx).SendNotificationToClient(ctx, string(mcp.MethodNotificationProgress), map[string]any{
						"progressToken": meta.ProgressToken,
						"progress":      i,
						"total":         steps,
						"message":       who,
					})
				}
				time.Sleep(20 * time.Millisecond)
			}
			return mcp.NewToolResultText("done:" + who), nil
		},
	)
	streamableOptions = append([]server.StreamableHTTPOption{server.WithStateLess(true)}, streamableOptions...)
	downstream := httptest.NewServer(server.NewStreamableHTTPServer(mcpServer, streamableOptions...))
	t.Cleanup(downstream.Close)
	return downstream.URL + "/mcp"
}

func TestProgressReachesOnlyItsCaller(t *testing.T) {
	skipShort(t)
	t.Parallel()

	configPath, addr := remoteConfig(t, "streamable-http", startProgressDownstream(t))
	proxy := startProxy(t, configPath, addr)

	// Two clients, one modern and one 2025-era, call at the same time with the
	// same token; the proxy forwards both over its single downstream session.
	callers := map[string]*client.Client{
		"modern": proxy.connectAs(t, "streamable-http", "remote", mcp.LATEST_PROTOCOL_VERSION),
		"legacy": proxy.connectAs(t, "streamable-http", "remote", mcp.LATEST_LEGACY_PROTOCOL_VERSION),
	}
	logs := map[string]*progressLog{}
	for who, mcpClient := range callers {
		logs[who] = collectProgress(mcpClient)
	}

	var wg sync.WaitGroup
	for who, mcpClient := range callers {
		wg.Go(func() {
			if got := callToolWithProgress(t, mcpClient, "slow", map[string]any{"steps": 4, "who": who}, 1); got != "done:"+who {
				t.Errorf("%s: slow = %q", who, got)
			}
		})
	}
	wg.Wait()

	for who, log := range logs {
		t.Run(who, func(t *testing.T) {
			seen := log.take()
			checkProgress(t, seen, 1, 4)
			for _, params := range seen {
				if params["message"] != who {
					t.Errorf("caller %s got progress for %v", who, params["message"])
				}
			}
		})
	}

	// A request without a token asked for no progress.
	t.Run("no token", func(t *testing.T) {
		mcpClient := callers["modern"]
		if got := callToolWithProgress(t, mcpClient, "slow", map[string]any{"steps": 2, "who": "silent"}, nil); got != "done:silent" {
			t.Errorf("slow = %q", got)
		}
		if seen := logs["modern"].take(); len(seen) != 0 {
			t.Errorf("caller without a token got progress: %v", seen)
		}
	})
}

// The 2026-07-28 client and the proxy's 2025-11-25 downstream session are
// negotiated separately; progress has to cross between the two.
func TestProgressFromLegacyDownstreamToModernClient(t *testing.T) {
	skipShort(t)
	t.Parallel()

	downstreamURL := startProgressDownstream(t,
		server.WithStreamableHTTPProtocolVersions(mcp.LATEST_LEGACY_PROTOCOL_VERSION))
	configPath, addr := remoteConfig(t, "streamable-http", downstreamURL)
	proxy := startProxy(t, configPath, addr)

	mcpClient := proxy.connect(t, "streamable-http", "remote")
	if v := mcpClient.ProtocolVersion(); !mcp.IsModernProtocol(v) {
		t.Fatalf("client negotiated %q with the proxy, want a 2026-07-28-era revision", v)
	}
	log := collectProgress(mcpClient)
	if got := callToolWithProgress(t, mcpClient, "slow", map[string]any{"steps": 3, "who": "modern"}, "tok-a"); got != "done:modern" {
		t.Errorf("slow = %q", got)
	}
	checkProgress(t, log.take(), "tok-a", 3)
}
