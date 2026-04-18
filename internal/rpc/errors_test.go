package rpc

import (
	"errors"
	"fmt"
	"testing"
)

func TestNoEndpointCoversError_IsSentinel(t *testing.T) {
	err := &NoEndpointCoversError{MinHeight: 100, MaxHeight: 200}
	if !errors.Is(err, ErrNoEndpointCovers) {
		t.Fatalf("errors.Is should match the sentinel on the struct directly")
	}
	wrapped := fmt.Errorf("fetch: %w", err)
	if !errors.Is(wrapped, ErrNoEndpointCovers) {
		t.Fatalf("errors.Is should match through a %%w wrap")
	}
	if errors.Is(err, ErrHeightPruned) {
		t.Fatalf("NoEndpointCoversError must not match ErrHeightPruned")
	}
}

func TestHeightPrunedError_IsSentinel(t *testing.T) {
	err := &HeightPrunedError{Height: 42, URL: "http://x", Status: 404}
	if !errors.Is(err, ErrHeightPruned) {
		t.Fatalf("errors.Is should match the sentinel on the struct")
	}
	wrapped := fmt.Errorf("header h=42: %w", err)
	if !errors.Is(wrapped, ErrHeightPruned) {
		t.Fatalf("errors.Is should match through a %%w wrap")
	}
	if errors.Is(err, ErrNoEndpointCovers) {
		t.Fatalf("HeightPrunedError must not match ErrNoEndpointCovers")
	}
}

func TestHTTPStatusError_Message(t *testing.T) {
	got := (&HTTPStatusError{Status: 404, Body: "not found"}).Error()
	want := "http 404: not found"
	if got != want {
		t.Fatalf("HTTPStatusError.Error() = %q, want %q", got, want)
	}
	got = (&HTTPStatusError{Status: 500}).Error()
	if got != "http 500" {
		t.Fatalf("empty-body form: got %q", got)
	}
}
