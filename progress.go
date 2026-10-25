package main

import (
	"context"
	"log/slog"
	"strconv"
	"sync"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
)

// progressTokenPrefix marks the progress tokens the proxy sends downstream in
// place of the ones its clients chose.
const progressTokenPrefix = "mcp-proxy-progress-"

// progressRelay carries a downstream's notifications/progress back to the
// client request that asked for it.
//
// mcp-go's client hands every notification to one connection-wide handler,
// without saying which request's stream it arrived on, so the progress token is
// the only thing that ties a notification to its caller. Tokens are chosen by
// clients, though, and every client of a route shares one downstream
// connection: two callers that both pick token 1 would be indistinguishable.
// Each forwarded request therefore gets a token of the proxy's own, unique on
// that connection, and the notification is rewritten back to the caller's
// token on the way out.
//
// A route lives only as long as the forwarded request: progress that arrives
// after it returned (or was cancelled) is dropped.
type progressRelay struct {
	mu     sync.Mutex
	next   uint64
	routes map[string]progressRoute
}

// progressRoute is where one in-flight request's progress goes: the inbound
// request context, which carries the client session and the request's stream,
// and the token that client chose.
type progressRoute struct {
	ctx   context.Context
	token mcp.ProgressToken
}

// track replaces meta's progress token with one unique to this downstream
// connection and remembers where its notifications go. The returned release
// must be called once the forwarded request is done. A request without a
// progress token is passed through untouched, so nothing is relayed for it.
func (r *progressRelay) track(ctx context.Context, meta *mcp.Meta) (*mcp.Meta, func()) {
	if meta == nil || meta.ProgressToken == nil {
		return meta, func() {}
	}

	r.mu.Lock()
	r.next++
	token := progressTokenPrefix + strconv.FormatUint(r.next, 10)
	if r.routes == nil {
		r.routes = make(map[string]progressRoute)
	}
	r.routes[token] = progressRoute{ctx: ctx, token: meta.ProgressToken}
	r.mu.Unlock()

	forwarded := *meta
	forwarded.ProgressToken = token
	return &forwarded, func() {
		r.mu.Lock()
		delete(r.routes, token)
		r.mu.Unlock()
	}
}

// relay is registered as a notification handler on every downstream client. It
// runs on the transport's reader goroutine, before the response that follows
// the notification is delivered, so a relayed notification is queued ahead of
// the final result. Sending never blocks: mcp-go drops a notification when the
// session's queue is full rather than wait.
func (r *progressRelay) relay(notification mcp.JSONRPCNotification) {
	if notification.Method != string(mcp.MethodNotificationProgress) {
		return
	}
	token, ok := notification.Params.AdditionalFields["progressToken"].(string)
	if !ok {
		return
	}

	r.mu.Lock()
	route, ok := r.routes[token]
	r.mu.Unlock()
	if !ok || route.ctx.Err() != nil {
		return
	}
	mcpServer := server.ServerFromContext(route.ctx)
	if mcpServer == nil {
		return
	}

	// Copy rather than rewrite in place: other handlers see the same map.
	params := make(map[string]any, len(notification.Params.AdditionalFields))
	for key, value := range notification.Params.AdditionalFields {
		params[key] = value
	}
	params["progressToken"] = route.token
	if err := mcpServer.SendNotificationToClient(route.ctx, notification.Method, params); err != nil {
		slog.Debug("Failed to relay progress notification", "err", err)
	}
}
