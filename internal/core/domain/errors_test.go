package domain

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"syscall"
	"testing"
)

func TestIsUnavailable(t *testing.T) {
	refused := &net.OpError{Op: "dial", Net: "tcp", Err: os.NewSyscallError("connect", syscall.ECONNREFUSED)}

	for _, tc := range []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"plain", errors.New("boom"), false},
		{"marked", Unavailable(errors.New("503")), true},
		{"marked and wrapped", fmt.Errorf("sink: %w", Unavailable(errors.New("503"))), true},
		{"circuit open", fmt.Errorf("%w: open", ErrCircuitOpen), true},
		{"attempt timeout", fmt.Errorf("wait: %w", context.DeadlineExceeded), true},
		{"connection refused", fmt.Errorf("add documents: %w", refused), true},
		{"bare errno", syscall.ECONNRESET, true},
		{"dropped connection", fmt.Errorf("read: %w", io.ErrUnexpectedEOF), true},
		{"dns", &net.DNSError{Err: "no such host", Name: "meilisearch"}, true},
		{"permanent", Permanent(errors.New("bad document")), false},
		{"permanent wins", Permanent(Unavailable(errors.New("x"))), false},
		{"canceled", context.Canceled, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := IsUnavailable(tc.err); got != tc.want {
				t.Errorf("IsUnavailable(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}

	if Unavailable(nil) != nil {
		t.Error("Unavailable(nil) must stay nil")
	}
}
