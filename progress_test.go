package main

import (
	"context"
	"fmt"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
)

// recordingSession is the client side of a proxied request: whatever the route
// sends to it lands on its notification channel.
type recordingSession struct {
	id            string
	notifications chan mcp.JSONRPCNotification
}

func newRecordingSession(id string) *recordingSession {
	return &recordingSession{id: id, notifications: make(chan mcp.JSONRPCNotification, 100)}
}

func (s *recordingSession) Initialize()       {}
func (s *recordingSession) Initialized() bool { return true }
func (s *recordingSession) SessionID() string { return s.id }
func (s *recordingSession) NotificationChannel() chan<- mcp.JSONRPCNotification {
	return s.notifications
}

// received drains what has been sent to the session so far.
func (s *recordingSession) received() []mcp.JSONRPCNotification {
	var got []mcp.JSONRPCNotification
	for {
		select {
		case n := <-s.notifications:
			got = append(got, n)
		default:
			return got
		}
	}
}

const (
	progressSteps = 3
	progressPause = 20 * time.Millisecond
)

// progressDownstream serves a tool, a prompt and a resource that each report
// progressSteps progress notifications against the request's own token before
// answering. The progress message names the caller, so a test can tell whose
// request a notification belongs to.
func progressDownstream(t *testing.T, protocolVersions ...string) string {
	t.Helper()

	downstream := server.NewMCPServer("downstream", "1.0.0",
		server.WithToolCapabilities(true), server.WithPromptCapabilities(true), server.WithResourceCapabilities(true, true))
	report := func(ctx context.Context, meta *mcp.Meta, who string) {
		if meta == nil || meta.ProgressToken == nil {
			return
		}
		// The pauses make this a slow call, and are not only for realism: mcp-go
		// (v1.1.0) can drop a notification queued in the same instant as the
		// response, when its stream forwarder and its final drain race for it.
		for i := 1; i <= progressSteps; i++ {
			time.Sleep(progressPause)
			_ = server.ServerFromContext(ctx).SendNotificationToClient(ctx, string(mcp.MethodNotificationProgress), map[string]any{
				"progressToken": meta.ProgressToken,
				"progress":      i,
				"total":         progressSteps,
				"message":       who,
			})
		}
		time.Sleep(progressPause)
	}
	downstream.AddTool(mcp.NewTool("slow", mcp.WithString("who")), func(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		who := request.GetString("who", "")
		report(ctx, request.Params.Meta, who)
		return mcp.NewToolResultText("done:" + who), nil
	})
	downstream.AddPrompt(mcp.NewPrompt("slow"), func(ctx context.Context, request mcp.GetPromptRequest) (*mcp.GetPromptResult, error) {
		report(ctx, request.Params.Meta, "prompt")
		return mcp.NewGetPromptResult("slow", []mcp.PromptMessage{mcp.NewPromptMessage(mcp.RoleUser, mcp.NewTextContent("hi"))}), nil
	})
	downstream.AddResource(mcp.NewResource("test://slow", "slow"), func(ctx context.Context, request mcp.ReadResourceRequest) ([]mcp.ResourceContents, error) {
		report(ctx, request.Params.Meta, "resource")
		return []mcp.ResourceContents{mcp.TextResourceContents{URI: "test://slow", Text: "slow"}}, nil
	})

	options := []server.StreamableHTTPOption{server.WithStateLess(true)}
	if len(protocolVersions) > 0 {
		options = append(options, server.WithStreamableHTTPProtocolVersions(protocolVersions...))
	}
	httpServer := httptest.NewServer(server.NewStreamableHTTPServer(downstream, options...))
	t.Cleanup(httpServer.Close)
	return httpServer.URL
}

// connectProgressRoute connects a proxy client to url and returns the route
// server its catalog was registered on.
func connectProgressRoute(t *testing.T, url string) *server.MCPServer {
	t.Helper()

	c, err := newMCPClient("progress", &MCPClientConfigV2{
		TransportType: MCPClientTypeStreamable,
		URL:           url,
		Options:       &OptionsV2{},
	})
	if err != nil {
		t.Fatalf("newMCPClient: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	route := server.NewMCPServer("progress", "1.0.0",
		server.WithToolCapabilities(true), server.WithPromptCapabilities(true), server.WithResourceCapabilities(true, true))
	if err := c.connect(t.Context(), mcp.Implementation{Name: "proxy", Version: "1"}, route); err != nil {
		t.Fatalf("connect: %v", err)
	}
	return route
}

// handle sends one JSON-RPC request through the route on behalf of session and
// fails the test if it does not succeed.
func handle(t *testing.T, route *server.MCPServer, session *recordingSession, body string) {
	t.Helper()

	resp := route.HandleMessage(route.WithContext(context.Background(), session), []byte(body))
	if _, ok := resp.(mcp.JSONRPCResponse); !ok {
		t.Errorf("%s: response = %#v, want a result", body, resp)
	}
}

// progressOf returns the progress notifications among got, failing on any
// other method.
func progressOf(t *testing.T, got []mcp.JSONRPCNotification) []map[string]any {
	t.Helper()

	var params []map[string]any
	for _, n := range got {
		if n.Method != string(mcp.MethodNotificationProgress) {
			t.Errorf("unexpected notification %q", n.Method)
			continue
		}
		params = append(params, n.Params.AdditionalFields)
	}
	return params
}

func TestProgressIsRelayedToTheCallingRequest(t *testing.T) {
	t.Parallel()

	for name, versions := range map[string][]string{
		"modern downstream": nil,
		"legacy downstream": {mcp.LATEST_LEGACY_PROTOCOL_VERSION},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			route := connectProgressRoute(t, progressDownstream(t, versions...))

			// Two callers that picked the same token, at the same time, over
			// the one downstream connection the route has.
			callers := []*recordingSession{newRecordingSession("a"), newRecordingSession("b")}
			var wg sync.WaitGroup
			for _, session := range callers {
				wg.Go(func() {
					handle(t, route, session, fmt.Sprintf(`{"jsonrpc":"2.0","id":1,"method":"tools/call",
						"params":{"name":"slow","arguments":{"who":%q},"_meta":{"progressToken":1}}}`, session.id))
				})
			}
			wg.Wait()

			for _, session := range callers {
				progress := progressOf(t, session.received())
				if len(progress) != progressSteps {
					t.Errorf("caller %s got %d progress notifications, want %d", session.id, len(progress), progressSteps)
				}
				for i, p := range progress {
					if p["message"] != session.id {
						t.Errorf("caller %s got progress meant for %v", session.id, p["message"])
					}
					// The caller's own token comes back, not the proxy's.
					if fmt.Sprint(p["progressToken"]) != "1" {
						t.Errorf("caller %s progress token = %v, want 1", session.id, p["progressToken"])
					}
					if fmt.Sprint(p["progress"]) != fmt.Sprint(i+1) {
						t.Errorf("caller %s progress[%d] = %v, want %d", session.id, i, p["progress"], i+1)
					}
				}
			}
		})
	}
}

func TestProgressIsRelayedForPromptsAndResources(t *testing.T) {
	t.Parallel()

	route := connectProgressRoute(t, progressDownstream(t))
	for method, body := range map[string]string{
		"prompts/get":    `{"jsonrpc":"2.0","id":1,"method":"prompts/get","params":{"name":"slow","_meta":{"progressToken":"p"}}}`,
		"resources/read": `{"jsonrpc":"2.0","id":1,"method":"resources/read","params":{"uri":"test://slow","_meta":{"progressToken":"p"}}}`,
	} {
		session := newRecordingSession(method)
		handle(t, route, session, body)
		progress := progressOf(t, session.received())
		if len(progress) != progressSteps {
			t.Errorf("%s relayed %d progress notifications, want %d", method, len(progress), progressSteps)
		}
		for _, p := range progress {
			if p["progressToken"] != "p" {
				t.Errorf("%s progress token = %v, want p", method, p["progressToken"])
			}
		}
	}
}

// A request without a progress token asked for no progress, and the downstream
// is not given a token to report against.
func TestNoProgressWithoutToken(t *testing.T) {
	t.Parallel()

	route := connectProgressRoute(t, progressDownstream(t))
	session := newRecordingSession("silent")
	handle(t, route, session, `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"slow","arguments":{"who":"silent"}}}`)
	if got := session.received(); len(got) != 0 {
		t.Errorf("caller without a progress token got %d notifications: %v", len(got), got)
	}
}

func TestProgressRelayTrack(t *testing.T) {
	t.Parallel()

	var relay progressRelay
	if meta, release := relay.track(t.Context(), nil); meta != nil {
		t.Errorf("track(nil) = %v, want nil", meta)
	} else {
		release()
	}

	untokened := &mcp.Meta{AdditionalFields: map[string]any{"k": "v"}}
	if meta, _ := relay.track(t.Context(), untokened); meta != untokened {
		t.Errorf("track without a token rewrote the meta: %v", meta)
	}

	inbound := &mcp.Meta{ProgressToken: 7, AdditionalFields: map[string]any{"k": "v"}}
	first, releaseFirst := relay.track(t.Context(), inbound)
	second, releaseSecond := relay.track(t.Context(), inbound)
	if first == nil || second == nil {
		t.Fatal("track dropped a meta that has a progress token")
	}
	if inbound.ProgressToken != 7 {
		t.Errorf("track mutated the caller's meta: %v", inbound.ProgressToken)
	}
	firstToken, _ := first.ProgressToken.(string)
	if !strings.HasPrefix(firstToken, progressTokenPrefix) || first.ProgressToken == second.ProgressToken {
		t.Errorf("forwarded tokens %v and %v, want two distinct proxy tokens", first.ProgressToken, second.ProgressToken)
	}
	if first.AdditionalFields["k"] != "v" {
		t.Errorf("track dropped other _meta keys: %v", first.AdditionalFields)
	}

	releaseFirst()
	releaseSecond()
	if len(relay.routes) != 0 {
		t.Errorf("%d routes left after release, want 0", len(relay.routes))
	}
}
