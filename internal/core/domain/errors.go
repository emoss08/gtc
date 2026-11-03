package domain

import "errors"

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
