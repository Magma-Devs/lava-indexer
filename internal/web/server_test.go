package web

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestParseProxyUpstream(t *testing.T) {
	cases := []struct {
		raw     string
		wantErr bool
	}{
		// A hostname is accepted without resolving it: at boot the graphql
		// container may not exist yet. .invalid never resolves (RFC 2606).
		{"http://graphql.invalid:5000", false},
		{"http://127.0.0.1:5000", false},
		{"http://10.0.0.5:5000/graphql", false},
		{"ftp://graphql:5000", true},
		{"http://", true},
		{"http://8.8.8.8:5000", true},
		{"http://169.254.169.254/latest/meta-data", true},
	}
	for _, c := range cases {
		_, err := parseProxyUpstream(c.raw)
		if (err != nil) != c.wantErr {
			t.Errorf("parseProxyUpstream(%q) error = %v, want error %v", c.raw, err, c.wantErr)
		}
	}
}

func TestPrivateDialContextRefusesPublicAddresses(t *testing.T) {
	dial := privateDialContext(&net.Dialer{Timeout: time.Second})
	for _, addr := range []string{"8.8.8.8:80", "169.254.169.254:80"} {
		conn, err := dial(context.Background(), "tcp", addr)
		if err == nil {
			conn.Close()
			t.Fatalf("dial %s succeeded, want refusal", addr)
		}
		if !strings.Contains(err.Error(), "refusing to proxy") {
			t.Errorf("dial %s error = %v, want a refusal", addr, err)
		}
	}
}

func graphqlStatus(t *testing.T, mux http.Handler) (int, string) {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/graphql", strings.NewReader(`{"query":"{ __typename }"}`))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	body, _ := io.ReadAll(rec.Body)
	return rec.Code, string(body)
}

func TestGraphQLProxyRegisteredWhenUpstreamDoesNotResolve(t *testing.T) {
	s := &Server{GraphQLEnabled: true, GraphQLUpstream: "http://graphql.invalid:5000"}
	code, _ := graphqlStatus(t, s.Mux())
	if code != http.StatusBadGateway {
		t.Fatalf("/graphql status = %d, want %d (proxy registered, upstream not up yet)", code, http.StatusBadGateway)
	}
}

func TestGraphQLProxyRecoversWhenUpstreamStarts(t *testing.T) {
	// Reserve a loopback port, then free it so the upstream is down at first.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close()

	s := &Server{GraphQLEnabled: true, GraphQLUpstream: "http://" + addr}
	mux := s.Mux()

	if code, _ := graphqlStatus(t, mux); code != http.StatusBadGateway {
		t.Fatalf("before upstream starts: /graphql status = %d, want %d", code, http.StatusBadGateway)
	}

	ln, err = net.Listen("tcp", addr)
	if err != nil {
		t.Skipf("could not re-listen on %s: %v", addr, err)
	}
	upstream := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"data":{"__typename":"Query"}}`)
	})}
	go func() { _ = upstream.Serve(ln) }()
	defer upstream.Close()

	code, body := graphqlStatus(t, mux)
	if code != http.StatusOK || !strings.Contains(body, `"__typename":"Query"`) {
		t.Fatalf("after upstream starts: /graphql = %d %q, want 200 from the upstream", code, body)
	}
}
