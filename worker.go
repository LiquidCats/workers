package workers

import (
	"context"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"
)

// Default values
const (
	DefaultMinWorkerCount   = 3
	DefaultMaxWorkerCount   = 5
	DefaultPollingFrequency = 300 * time.Millisecond
)

type Runner[T any] struct {
	min    uint8
	max    uint32
	h      Handler[T]
	dataCh chan T
	f      time.Duration

	activeWorkers atomic.Uint32
	busyWorkers   atomic.Uint32
	stopping      atomic.Bool

	deflateCh chan struct{}
}

func NewRunner[T any](h Handler[T], opts ...Opt[T]) (*Runner[T], chan<- T, error) {
	dataCh := make(chan T, DefaultMaxWorkerCount)
	r := &Runner[T]{
		h:      h,
		dataCh: dataCh,

		min: DefaultMinWorkerCount,
		max: DefaultMaxWorkerCount,
		f:   DefaultPollingFrequency,

		deflateCh: make(chan struct{}, DefaultMaxWorkerCount),
	}

	for _, opt := range opts {
		opt(r)
	}

	if r.min == 0 {
		return nil, nil, ErrInvalidMinWorkerCount
	}
	if r.max == 0 {
		return nil, nil, ErrInvalidMaxWorkerCount
	}
	if uint32(r.min) > r.max {
		return nil, nil, ErrInvalidMinMaxWorkerCount
	}
	if r.f == 0 {
		return nil, nil, ErrInvalidPollingFrequency
	}

	return r, r.dataCh, nil
}

func (r *Runner[T]) Run(ctx context.Context) error {
	ticker := time.NewTicker(r.f)
	defer ticker.Stop()
	defer close(r.deflateCh)

	g, ctx := errgroup.WithContext(ctx)

	// Spawn minimum workers upfront
	for i := 0; i < int(r.min); i++ {
		r.spawn(ctx, g)
	}

	g.Go(func() error {
		for {
			select {
			case <-ctx.Done():
				r.stopping.Store(true)
				return ctx.Err()
			case <-ticker.C:
				if r.stopping.Load() {
					return nil
				}

				active := r.activeWorkers.Load()
				busy := r.busyWorkers.Load()

				if active == 0 {
					continue
				}

				// Scale up if >= 70% busy and below max
				busyRatio := float64(busy) / float64(active)
				if active > 0 && busyRatio >= 0.7 && active < r.max {
					r.spawn(ctx, g)
					continue
				}

				// Scale down if too many idle workers and above min
				if active > uint32(r.min) && busyRatio < 0.3 {
					excess := active - uint32(r.min)
					toRemove := min(excess, active/2) //nolint:mnd
				L:
					for range toRemove {
						select {
						case r.deflateCh <- struct{}{}:
						case <-ctx.Done():
							return nil
						default:
							break L
						}
					}
				}
			}
		}
	})

	return g.Wait()
}

func (r *Runner[T]) spawn(ctx context.Context, g *errgroup.Group) {
	if r.stopping.Load() {
		return
	}

	// Atomic compare-and-swap to prevent race condition
	for {
		current := r.activeWorkers.Load()
		if current >= r.max {
			return
		}
		// Atomically increment only if value hasn't changed
		if r.activeWorkers.CompareAndSwap(current, current+1) {
			break
		}
		// If CAS failed, retry (another goroutine modified it)
	}

	g.Go(func() error {
		defer r.activeWorkers.Add(^uint32(0))

		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-r.deflateCh:
				// Scale down
				return nil
			case d, ok := <-r.dataCh:
				if !ok {
					r.stopping.Store(true)
					return nil
				}
				if r.stopping.Load() {
					return nil
				}

				r.busyWorkers.Add(1)

				r.h.Handle(ctx, d)

				r.busyWorkers.Add(^uint32(0))
			}
		}
	})
}
