package workers

import (
	"context"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"
)

const (
	DefaultMinWorkerCount   int32         = 3
	DefaultMaxWorkerCount   int32         = 5
	DefaultPollingFrequency time.Duration = 300 * time.Millisecond
	DefaultMaxLoad          float64       = 0.7
	DefaultMinLoad          float64       = 0.3
	DefaultScalingCooldown  time.Duration = 5 * time.Second
)

type config[T any] struct {
	pollingFreq time.Duration

	min int32
	max int32

	maxLoad float64
	minLoad float64

	scalingCooldown time.Duration
}

type Pool[T any] struct {
	cfg config[T]

	handler HandleFunc[T]

	dataCh    chan T
	deflateCh chan struct{}

	activeWorkers   atomic.Int32
	busyWorkers     atomic.Int32
	lastScalingTime atomic.Int64
	stopping        atomic.Bool
	closed          atomic.Bool
}

type HandleFunc[T any] func(ctx context.Context, v T) error

func New[T any](h HandleFunc[T], opts ...Opt[T]) (*Pool[T], error) {
	cfg := config[T]{
		pollingFreq:     DefaultPollingFrequency,
		min:             DefaultMinWorkerCount,
		max:             DefaultMaxWorkerCount,
		maxLoad:         DefaultMaxLoad,
		minLoad:         DefaultMinLoad,
		scalingCooldown: DefaultScalingCooldown,
	}
	for _, opt := range opts {
		opt(&cfg)
	}

	if cfg.min == 0 {
		return nil, ErrInvalidMinWorkerCount
	}
	if cfg.max == 0 {
		return nil, ErrInvalidMaxWorkerCount
	}
	if cfg.min > cfg.max {
		return nil, ErrInvalidMinMaxWorkerCount
	}
	if cfg.pollingFreq == 0 {
		return nil, ErrInvalidPollingFrequency
	}
	if cfg.maxLoad > 1 || cfg.maxLoad <= 0 {
		return nil, ErrInvalidMaxLoad
	}
	if cfg.minLoad < 0 || cfg.minLoad >= cfg.maxLoad {
		return nil, ErrInvalidMinLoad
	}

	p := &Pool[T]{
		cfg:       cfg,
		handler:   h,
		dataCh:    make(chan T, cfg.max),
		deflateCh: make(chan struct{}, cfg.max),
	}

	return p, nil
}

func (p *Pool[T]) Submit(ctx context.Context, data T) error {
	if p.closed.Load() {
		return ErrPoolClosed
	}

	select {
	case p.dataCh <- data:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (p *Pool[T]) Close() error {
	if p.closed.CompareAndSwap(false, true) {
		close(p.dataCh)
	}
	return nil
}

func (p *Pool[T]) Start(ctx context.Context) error {
	if p.closed.Load() {
		return ErrPoolClosed
	}

	group, ctx := errgroup.WithContext(ctx)

	p.autoscaler(ctx, group)

	for range p.cfg.min {
		p.spawn(ctx, group)
	}

	return group.Wait()
}

// ActiveWorkers returns the current number of active workers.
func (p *Pool[T]) ActiveWorkers() int32 {
	return p.activeWorkers.Load()
}

// BusyWorkers returns the current number of busy workers.
func (p *Pool[T]) BusyWorkers() int32 {
	return p.busyWorkers.Load()
}

// QueueSize returns the current number of pending items in the queue.
func (p *Pool[T]) QueueSize() int {
	return len(p.dataCh)
}

func (p *Pool[T]) autoscaler(ctx context.Context, g *errgroup.Group) {
	g.Go(func() error {
		ticker := time.NewTicker(p.cfg.pollingFreq)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				active := p.activeWorkers.Load()
				busy := p.busyWorkers.Load()

				if active == 0 {
					continue
				}

				now := time.Now().UnixNano()
				lastScaling := p.lastScalingTime.Load()
				cooldownNanos := p.cfg.scalingCooldown.Nanoseconds()

				if now-lastScaling < cooldownNanos {
					continue
				}

				busyRatio := float64(busy) / float64(active)

				if busyRatio >= p.cfg.maxLoad && active < p.cfg.max {
					p.spawn(ctx, g)
					p.lastScalingTime.Store(now)
					continue
				}

				if active > p.cfg.min && busyRatio < p.cfg.minLoad {
					excess := active - p.cfg.min
					toRemove := min(excess, max(1, active/2)) //nolint:mnd
					for range toRemove {
						select {
						case p.deflateCh <- struct{}{}:
						default:
						}
					}
					p.lastScalingTime.Store(now)
				}
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	})
}

func (r *Pool[T]) spawn(ctx context.Context, g *errgroup.Group) {
	if r.stopping.Load() {
		return
	}

	g.Go(func() error {
		// Atomic compare-and-swap to prevent race condition
		var workerStarted bool
		for {
			current := r.activeWorkers.Load()
			if current >= r.cfg.max {
				return nil
			}
			if r.activeWorkers.CompareAndSwap(current, current+1) {
				workerStarted = true
				break
			}
		}

		if workerStarted {
			defer r.activeWorkers.Add(-1)
		}

		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-r.deflateCh:
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
				err := r.handler(ctx, d)
				r.busyWorkers.Add(-1)

				if err != nil {
					return err
				}
			}
		}
	})
}
