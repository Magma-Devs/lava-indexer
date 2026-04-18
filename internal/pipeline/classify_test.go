package pipeline

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/magma-devs/lava-indexer/internal/rpc"
)

func TestIsPermanentFetchError(t *testing.T) {
	cancelledCtx, cancel := context.WithCancel(context.Background())
	cancel()

	tests := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "nil is not permanent",
			err:  nil,
			want: false,
		},
		{
			name: "no-endpoint-covers direct is permanent",
			err:  &rpc.NoEndpointCoversError{MinHeight: 1, MaxHeight: 10},
			want: true,
		},
		{
			name: "no-endpoint-covers wrapped is permanent",
			err:  fmt.Errorf("fetch: %w", &rpc.NoEndpointCoversError{MinHeight: 1, MaxHeight: 10}),
			want: true,
		},
		{
			name: "height-pruned is permanent",
			err:  &rpc.HeightPrunedError{Height: 42, URL: "http://x", Status: 404},
			want: true,
		},
		{
			name: "rate-limited is transient",
			err:  &rpc.ErrRateLimited{RetryAfter: time.Second, Status: 429},
			want: false,
		},
		{
			name: "context cancel is transient (parent owns the decision)",
			err:  cancelledCtx.Err(),
			want: false,
		},
		{
			name: "deadline exceeded is transient",
			err:  context.DeadlineExceeded,
			want: false,
		},
		{
			name: "generic http 500 is transient",
			err:  &rpc.HTTPStatusError{Status: 500},
			want: false,
		},
		{
			name: "plain errors.New is transient",
			err:  errors.New("socket died"),
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isPermanentFetchError(tt.err); got != tt.want {
				t.Fatalf("isPermanentFetchError(%v) = %v, want %v", tt.err, got, tt.want)
			}
		})
	}
}
