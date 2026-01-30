package workers

import "time"

type Opt[T any] func(r *config[T])

func WithMinWorkerCount[T any](min int32) Opt[T] {
	return func(cfg *config[T]) {
		cfg.min = min
	}
}

func WithMaxWorkerCount[T any](max int32) Opt[T] {
	return func(cfg *config[T]) {
		cfg.max = max
	}
}

func WithPollingFrequency[T any](f time.Duration) Opt[T] {
	return func(cfg *config[T]) {
		cfg.pollingFreq = f
	}
}

func WithMaxLoad[T any](maxLoad float64) Opt[T] {
	return func(cfg *config[T]) {
		cfg.maxLoad = maxLoad
	}
}

func WithMinLoad[T any](minLoad float64) Opt[T] {
	return func(cfg *config[T]) {
		cfg.minLoad = minLoad
	}
}

func WithScalingCooldown[T any](cooldown time.Duration) Opt[T] {
	return func(cfg *config[T]) {
		cfg.scalingCooldown = cooldown
	}
}
