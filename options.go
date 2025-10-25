package workers

import "time"

type Opt[T any] func(r *Runner[T])

func WithMinWorkerCount[T any](min uint8) Opt[T] {
	return func(r *Runner[T]) {
		r.min = min
	}
}

func WithMaxWorkerCount[T any](max uint32) Opt[T] {
	return func(r *Runner[T]) {
		r.max = max
		r.deflateCh = make(chan struct{}, max)
		r.dataCh = make(chan T)
	}
}

func WithPollingFrequency[T any](f time.Duration) Opt[T] {
	return func(r *Runner[T]) {
		r.f = f
	}
}
