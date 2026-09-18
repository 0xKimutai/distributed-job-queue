package backoff

import (
	"math/rand/v2"
	"time"
)

const (
	DefaultBase    = 1 * time.Second
	DefaultMaxDelay = 10 * time.Minute
)

// Calculate returns a jittered exponential backoff duration for the given
// attempt number (0-indexed: first retry is attempt 0).
//
// Formula: delay = random(0, base * 2^attempt), capped at maxDelay.
//
// Full jitter is used rather than pure exponential backoff because it
// desynchronizes retries across many workers — preventing retry storms
// where all workers hammer a recovering service at the same instant.
//
// Reference: https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/
func Calculate(attempt int, base, maxDelay time.Duration) time.Duration {
	if attempt < 0 {
		attempt = 0
	}

	// Cap the exponent to prevent int64 overflow on large attempt counts.
	// 2^62 overflows int64; capping at 30 gives a max window of ~12 days
	// which is far beyond any sane maxDelay.
	exp := attempt
	if exp > 30 {
		exp = 30
	}

	// base * 2^exp is the upper bound of the jitter window.
	window := time.Duration(int64(base) * (1 << exp))
	if window > maxDelay || window <= 0 {
		window = maxDelay
	}

	// rand.N returns a random duration in [0, window).
	// Each worker gets a different random value, spreading retries over time.
	return rand.N(window)
}

// ErrPermanent wraps an error to signal that this failure should NOT be
// retried — it is a permanent failure (e.g. unknown task, invalid payload).
// Workers check for this type and skip straight to the dead letter queue.
type ErrPermanent struct {
	Cause error
}

func (e *ErrPermanent) Error() string {
	return e.Cause.Error()
}

func (e *ErrPermanent) Unwrap() error {
	return e.Cause
}

// Permanent wraps an error as permanent. Use this in job handlers to signal
// that retrying is pointless.
//
// Example:
//
//	if payload.UserID == "" {
//	    return backoff.Permanent(fmt.Errorf("missing user_id in payload"))
//	}
func Permanent(err error) error {
	return &ErrPermanent{Cause: err}
}

// IsPermanent reports whether err is a permanent failure.
func IsPermanent(err error) bool {
	var p *ErrPermanent
	// errors.As unwraps the chain, so wrapped permanent errors are detected too.
	return isType(err, &p)
}

// isType is a helper because errors.As needs an addressable target.
func isType(err error, target **ErrPermanent) bool {
	for err != nil {
		if e, ok := err.(*ErrPermanent); ok {
			*target = e
			return true
		}
		// Unwrap one level.
		type unwrapper interface{ Unwrap() error }
		if u, ok := err.(unwrapper); ok {
			err = u.Unwrap()
		} else {
			break
		}
	}
	return false
}
