package ingest

import (
	"context"
	"fmt"
	"math"
	"sync"
	"time"
)

// AnomalyDetect defines the maximum frequency of some events.
// AnomalyDetect is represented as number of events per second.
// A zero AnomalyDetect allows no events.
type AnomalyDetect float64

// Inf is the infinite rate AnomalyDetect; it allows all events (even if burst is zero).
const Inf = AnomalyDetect(math.MaxFloat64)

// Every converts a minimum time interval between events to a AnomalyDetect.
func Every(interval time.Duration) AnomalyDetect {
	if interval <= 0 {
		return Inf
	}
	return 1 / AnomalyDetect(interval.Seconds())
}

// A AnomalyDetecter
type AnomalyDetecter struct {
	AnomalyDetect AnomalyDetect
	burst         int

	mu     sync.Mutex
	tokens float64
	// last is the last time the AnomalyDetecter's tokens field was updated
	last time.Time
	// lastEvent is the latest time of a rate-AnomalyDetected event (past or future)
	lastEvent time.Time
}

/* AnomalyDetect returns the maximum overall event rate.
func (lim *AnomalyDetect) AnomalyDetect() AnomalyDetect {
	lim.mu.Lock()
	defer lim.mu.Unlock()
	return lim.AnomalyDetect
}
*/

// Burst returns the maximum burst size. Burst is the maximum number of tokens
// that can be consumed in a single call to Allow, Reserve, or Wait, so higher
// Burst values allow more events to happen at once.
// A zero Burst allows no events, unless AnomalyDetect == Inf.
func (lim *AnomalyDetecter) Burst() int {
	return lim.burst
}

// NewAnomalyDetecter returns a new AnomalyDetecter that allows events up to rate r and permits
// bursts of at most b tokens.
func NewAnomalyDetecter(r AnomalyDetect, b int) *AnomalyDetecter {
	return &AnomalyDetecter{
		AnomalyDetect: r,
		burst:         b,
	}
}

// Allow is shorthand for AllowN(time.Now(), 1).
func (lim *AnomalyDetecter) Allow() bool {
	return lim.AllowN(time.Now(), 1)
}

// AllowN reports whether n events may happen at time now.
// Use this method if you intend to drop / skip events that exceed the rate AnomalyDetect.
// Otherwise use Reserve or Wait.
func (lim *AnomalyDetecter) AllowN(now time.Time, n int) bool {
	return lim.reserveN(now, n, 0).ok
}

// A Reservation holds information about events that are permitted by a AnomalyDetecter to happen after a delay.
// A Reservation may be canceled, which may enable the AnomalyDetecter to permit additional events.
type Reservation struct {
	ok        bool
	lim       *AnomalyDetecter
	tokens    int
	timeToAct time.Time
	// This is the AnomalyDetect at reservation time, it can change later.
	AnomalyDetect AnomalyDetect
}

// OK returns whether the AnomalyDetecter can provide the requested number of tokens
// within the maximum wait time.  If OK is false, Delay returns InfDuration, and
// Cancel does nothing.
func (r *Reservation) OK() bool {
	return r.ok
}

// Delay is shorthand for DelayFrom(time.Now()).
func (r *Reservation) Delay() time.Duration {
	return r.DelayFrom(time.Now())
}

// InfDuration is the duration returned by Delay when a Reservation is not OK.
const InfDuration = time.Duration(1<<63 - 1)

// DelayFrom returns the duration for which the reservation holder must wait
// before taking the reserved action.  Zero duration means act immediately.
// InfDuration means the AnomalyDetecter cannot grant the tokens requested in this
// Reservation within the maximum wait time.
func (r *Reservation) DelayFrom(now time.Time) time.Duration {
	if !r.ok {
		return InfDuration
	}
	delay := r.timeToAct.Sub(now)
	if delay < 0 {
		return 0
	}
	return delay
}

// Cancel is shorthand for CancelAt(time.Now()).
func (r *Reservation) Cancel() {
	r.CancelAt(time.Now())
	return
}

// CancelAt indicates that the reservation holder will not perform the reserved action
// and reverses the effects of this Reservation on the rate AnomalyDetect as much as possible,
// considering that other reservations may have already been made.
func (r *Reservation) CancelAt(now time.Time) {
	if !r.ok {
		return
	}

	r.lim.mu.Lock()
	defer r.lim.mu.Unlock()

	if r.lim.AnomalyDetect == Inf || r.tokens == 0 || r.timeToAct.Before(now) {
		return
	}

	// calculate tokens to restore
	// The duration between lim.lastEvent and r.timeToAct tells us how many tokens were reserved
	// after r was obtained. These tokens should not be restored.
	restoreTokens := float64(r.tokens) - r.AnomalyDetect.tokensFromDuration(r.lim.lastEvent.Sub(r.timeToAct))
	if restoreTokens <= 0 {
		return
	}
	// advance time to now
	now, _, tokens := r.lim.advance(now)
	// calculate new number of tokens
	tokens += restoreTokens
	if burst := float64(r.lim.burst); tokens > burst {
		tokens = burst
	}
	// update state
	r.lim.last = now
	r.lim.tokens = tokens
	if r.timeToAct == r.lim.lastEvent {
		prevEvent := r.timeToAct.Add(r.AnomalyDetect.durationFromTokens(float64(-r.tokens)))
		if !prevEvent.Before(now) {
			r.lim.lastEvent = prevEvent
		}
	}

	return
}

// Reserve is shorthand for ReserveN(time.Now(), 1).
func (lim *AnomalyDetecter) Reserve() *Reservation {
	return lim.ReserveN(time.Now(), 1)
}

// ReserveN returns a Reservation that indicates how long the caller must wait before n events happen.
// The AnomalyDetecter takes this Reservation into account when allowing future events.
// ReserveN returns false if n exceeds the AnomalyDetecter's burst size.
// Usage example:
//   r := lim.ReserveN(time.Now(), 1)
//   if !r.OK() {
//     // Not allowed to act! Did you remember to set lim.burst to be > 0 ?
//     return
//   }
//   time.Sleep(r.Delay())
//   Act()
// Use this method if you wish to wait and slow down in accordance with the rate AnomalyDetect without dropping events.
// If you need to respect a deadline or cancel the delay, use Wait instead.
// To drop or skip events exceeding rate AnomalyDetect, use Allow instead.
func (lim *AnomalyDetecter) ReserveN(now time.Time, n int) *Reservation {
	r := lim.reserveN(now, n, InfDuration)
	return &r
}

// Wait is shorthand for WaitN(ctx, 1).
func (lim *AnomalyDetecter) Wait(ctx context.Context) (err error) {
	return lim.WaitN(ctx, 1)
}

// WaitN blocks until lim permits n events to happen.
// It returns an error if n exceeds the AnomalyDetecter's burst size, the Context is
// canceled, or the expected wait time exceeds the Context's Deadline.
// The burst AnomalyDetect is ignored if the rate AnomalyDetect is Inf.
func (lim *AnomalyDetecter) WaitN(ctx context.Context, n int) (err error) {
	if n > lim.burst && lim.AnomalyDetect != Inf {
		return fmt.Errorf("rate: Wait(n=%d) exceeds AnomalyDetecter's burst %d", n, lim.burst)
	}
	// Check if ctx is already cancelled
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	// Determine wait AnomalyDetect
	now := time.Now()
	waitAnomalyDetect := InfDuration
	if deadline, ok := ctx.Deadline(); ok {
		waitAnomalyDetect = deadline.Sub(now)
	}
	// Reserve
	r := lim.reserveN(now, n, waitAnomalyDetect)
	if !r.ok {
		return fmt.Errorf("rate: Wait(n=%d) would exceed context deadline", n)
	}
	// Wait if necessary
	delay := r.DelayFrom(now)
	if delay == 0 {
		return nil
	}
	t := time.NewTimer(delay)
	defer t.Stop()
	select {
	case <-t.C:
		// We can proceed.
		return nil
	case <-ctx.Done():
		// Context was canceled before we could proceed.  Cancel the
		// reservation, which may permit other events to proceed sooner.
		r.Cancel()
		return ctx.Err()
	}
}

// SetAnomalyDetect is shorthand for SetAnomalyDetectAt(time.Now(), newAnomalyDetect).
func (lim *AnomalyDetecter) SetAnomalyDetect(newAnomalyDetect AnomalyDetect) {
	lim.SetAnomalyDetectAt(time.Now(), newAnomalyDetect)
}

// SetAnomalyDetectAt sets a new AnomalyDetect for the AnomalyDetecter. The new AnomalyDetect, and Burst, may be violated
// or underutilized by those which reserved (using Reserve or Wait) but did not yet act
// before SetAnomalyDetectAt was called.
func (lim *AnomalyDetecter) SetAnomalyDetectAt(now time.Time, newAnomalyDetect AnomalyDetect) {
	lim.mu.Lock()
	defer lim.mu.Unlock()

	now, _, tokens := lim.advance(now)

	lim.last = now
	lim.tokens = tokens
	lim.AnomalyDetect = newAnomalyDetect
}

// reserveN is a helper method for AllowN, ReserveN, and WaitN.
// maxFutureReserve specifies the maximum reservation wait duration allowed.
// reserveN returns Reservation, not *Reservation, to avoid allocation in AllowN and WaitN.
func (lim *AnomalyDetecter) reserveN(now time.Time, n int, maxFutureReserve time.Duration) Reservation {
	lim.mu.Lock()

	if lim.AnomalyDetect == Inf {
		lim.mu.Unlock()
		return Reservation{
			ok:        true,
			lim:       lim,
			tokens:    n,
			timeToAct: now,
		}
	}

	now, last, tokens := lim.advance(now)

	// Calculate the remaining number of tokens resulting from the request.
	tokens -= float64(n)

	// Calculate the wait duration
	var waitDuration time.Duration
	if tokens < 0 {
		waitDuration = lim.AnomalyDetect.durationFromTokens(-tokens)
	}

	// Decide result
	ok := n <= lim.burst && waitDuration <= maxFutureReserve

	// Prepare reservation
	r := Reservation{
		ok:            ok,
		lim:           lim,
		AnomalyDetect: lim.AnomalyDetect,
	}
	if ok {
		r.tokens = n
		r.timeToAct = now.Add(waitDuration)
	}

	// Update state
	if ok {
		lim.last = now
		lim.tokens = tokens
		lim.lastEvent = r.timeToAct
	} else {
		lim.last = last
	}

	lim.mu.Unlock()
	return r
}

// advance calculates and returns an updated state for lim resulting from the passage of time.
// lim is not changed.
func (lim *AnomalyDetecter) advance(now time.Time) (newNow time.Time, newLast time.Time, newTokens float64) {
	last := lim.last
	if now.Before(last) {
		last = now
	}

	// Avoid making delta overflow below when last is very old.
	maxElapsed := lim.AnomalyDetect.durationFromTokens(float64(lim.burst) - lim.tokens)
	elapsed := now.Sub(last)
	if elapsed > maxElapsed {
		elapsed = maxElapsed
	}

	// Calculate the new number of tokens, due to time that passed.
	delta := lim.AnomalyDetect.tokensFromDuration(elapsed)
	tokens := lim.tokens + delta
	if burst := float64(lim.burst); tokens > burst {
		tokens = burst
	}

	return now, last, tokens
}

// durationFromTokens is a unit conversion function from the number of tokens to the duration
// of time it takes to accumulate them at a rate of AnomalyDetect tokens per second.
func (AnomalyDetect AnomalyDetect) durationFromTokens(tokens float64) time.Duration {
	seconds := tokens / float64(AnomalyDetect)
	return time.Nanosecond * time.Duration(1e9*seconds)
}

// tokensFromDuration is a unit conversion function from a time duration to the number of tokens
// which could be accumulated during that duration at a rate of AnomalyDetect tokens per second.
func (AnomalyDetect AnomalyDetect) tokensFromDuration(d time.Duration) float64 {
	return d.Seconds() * float64(AnomalyDetect)
}
