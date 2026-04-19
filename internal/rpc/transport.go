package rpc

import (
	"crypto/tls"
	"net"
	"net/http"
	"time"
)

// SharedTransport is the process-wide HTTP transport used by every RPC and
// REST client we construct. Sharing one Transport across all endpoints has
// two concrete wins:
//
//   - TCP connection pool reuse across reconstructions. Each RPCClient /
//     RESTClient used to build its own http.Transport, so two clients
//     pointing at the same host couldn't share idle keep-alive connections.
//     With a shared Transport they do.
//   - TLS session resumption cache lives across clients. When a connection
//     drops and we dial again (same host, possibly a different Client
//     wrapping the same Transport), the cache lets us skip the TLS
//     handshake (~1-2 round trips + some crypto) and resume. On
//     high-churn connection patterns against a TLS-terminating proxy this
//     can shave real wall-clock off every reconnect.
//
// Tuning choices, and what they buy us:
//
//   - MaxIdleConnsPerHost = 256. Go's default is TWO. With 96 fetch
//     workers all hitting one host, the default forces a fresh TCP/TLS
//     handshake for 94 of every 96 requests — one of the biggest silent
//     drags on throughput. 256 means we hold onto every idle keep-alive
//     up to the per-host cap and reuse freely.
//   - MaxIdleConns = 1024 across all hosts. Ceiling for the whole pool.
//   - MaxConnsPerHost = 0 (unlimited). We cap via the fetch-worker count
//     at the call-site layer, so there's no reason to gate at the TCP
//     level.
//   - ForceAttemptHTTP2 = true. When the upstream terminator supports
//     HTTP/2, a single multiplexed connection carries many concurrent
//     requests — collapsing our pool requirements and removing
//     head-of-line blocking per connection.
//   - ClientSessionCache = tls.NewLRUClientSessionCache(512). TLS
//     session tickets / session IDs are cached for resumption; on
//     reconnect we skip the full handshake.
//   - DialContext with 5s connect + 30s keep-alive probe. Long-lived
//     TCP keep-alive probes keep the kernel's view of connection health
//     accurate so `Dial` doesn't occasionally hand us a broken socket.
//   - ResponseHeaderTimeout = 60s. Tendermint JSON-RPC batch responses
//     can take this long on cold archive queries; tighter and we'd
//     abort legitimate slow reads.
var SharedTransport = func() *http.Transport {
	return &http.Transport{
		DialContext: (&net.Dialer{
			Timeout:   5 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		MaxIdleConns:          1024,
		MaxIdleConnsPerHost:   256,
		MaxConnsPerHost:       0,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   5 * time.Second,
		ResponseHeaderTimeout: 120 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		ForceAttemptHTTP2:     true,
		TLSClientConfig: &tls.Config{
			ClientSessionCache: tls.NewLRUClientSessionCache(512),
			MinVersion:         tls.VersionTLS12,
		},
	}
}()

// newHTTPClient returns an http.Client that wraps the shared Transport
// with a per-call timeout. Every RPC/REST endpoint gets its own Client
// wrapper (because Timeout is a Client-level field, not Transport), but
// they all share connection pools + TLS session cache via SharedTransport.
func newHTTPClient(timeout time.Duration) *http.Client {
	return &http.Client{
		Transport: SharedTransport,
		Timeout:   timeout,
	}
}
