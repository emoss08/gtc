package domain

import (
	"context"
	"errors"
	"io"
	"net"
	"syscall"
)

var (
	ErrSinkNotFound       = errors.New("sink not found")
	ErrSinkAlreadyExists  = errors.New("sink already registered")
	ErrSinkInitFailed     = errors.New("sink initialization failed")
	ErrSinkProcessFailed  = errors.New("sink processing failed")
	ErrSinkShutdownFailed = errors.New("sink shutdown failed")
	ErrWALConnectionLost  = errors.New("WAL connection lost")
	ErrInvalidEvent       = errors.New("invalid CDC event")
	ErrCircuitOpen        = errors.New("circuit breaker is open")
	ErrShuttingDown       = errors.New("service is shutting down")
)

// A sink failure is one of three kinds, and the pipeline treats each one
// differently:
//
//   - Permanent: the destination rejected this event (bad data). Retrying the
//     same event cannot succeed, so the dead-letter queue parks it at once.
//   - Unavailable: the destination is down, unreachable, overloaded or
//     refusing every write. It says nothing about the event, so nothing is
//     parked: the pipeline holds its position and retries until the sink is
//     back.
//   - Anything else: a transient failure while the sink is otherwise healthy.
//     It is retried, and only after CDC_DLQ_THRESHOLD failure cycles of the
//     same event does the dead-letter queue isolate and park it.

// PermanentError marks a failure caused by the event data rather than the
// destination: retrying the same event cannot succeed.
type PermanentError struct {
	Err error
}

func (e *PermanentError) Error() string { return e.Err.Error() }
func (e *PermanentError) Unwrap() error { return e.Err }

// Permanent wraps err as a PermanentError. A nil err stays nil.
func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return &PermanentError{Err: err}
}

// IsPermanent reports whether err, or any error it wraps, is permanent.
func IsPermanent(err error) bool {
	var p *PermanentError
	return errors.As(err, &p)
}

// UnavailableError marks a failure caused by the destination rather than the
// event: the sink cannot take writes right now (connection refused, 5xx,
// throttling, credentials refused, a failing health check).
type UnavailableError struct {
	Err error
}

func (e *UnavailableError) Error() string { return e.Err.Error() }
func (e *UnavailableError) Unwrap() error { return e.Err }

// Unavailable wraps err as an UnavailableError. A nil err stays nil.
func Unavailable(err error) error {
	if err == nil {
		return nil
	}
	return &UnavailableError{Err: err}
}

// IsUnavailable reports whether err says the sink, not the event, is at
// fault: an error a sink marked Unavailable, an open circuit, an attempt that
// timed out, or a transport failure (refused, reset or dropped connection,
// DNS, network timeout). A permanent error is never unavailable.
func IsUnavailable(err error) bool {
	if err == nil || IsPermanent(err) {
		return false
	}
	var u *UnavailableError
	if errors.As(err, &u) {
		return true
	}
	if errors.Is(err, ErrCircuitOpen) || errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var netErr net.Error
	if errors.As(err, &netErr) {
		return true
	}
	return errors.Is(err, syscall.ECONNREFUSED) ||
		errors.Is(err, syscall.ECONNRESET) ||
		errors.Is(err, syscall.ECONNABORTED) ||
		errors.Is(err, syscall.EPIPE) ||
		errors.Is(err, io.EOF) ||
		errors.Is(err, io.ErrUnexpectedEOF)
}
