package workers

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// setupBenchPool creates a pool and ensures workers are ready before benchmarking
func setupBenchPool(b *testing.B, handler HandleFunc[int], opts ...Opt[int]) (*Pool[int], context.Context, context.CancelFunc) {
	b.Helper()

	pool, err := New(handler, opts...)
	require.NoError(b, err)

	ctx, cancel := context.WithCancel(context.Background())
	go pool.Start(ctx)

	// Wait for workers to be active
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if pool.ActiveWorkers() > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	return pool, ctx, cancel
}

// BenchmarkPool_Submit measures raw submit latency with no-op handler
func BenchmarkPool_Submit(b *testing.B) {
	handler := func(ctx context.Context, v int) error {
		return nil
	}

	pool, ctx, cancel := setupBenchPool(b, handler,
		WithMinWorkerCount[int](8),
		WithMaxWorkerCount[int](8),
	)
	defer cancel()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			pool.Submit(ctx, 1)
		}
	})
}

// BenchmarkPool_SubmitSequential measures sequential submit performance
func BenchmarkPool_SubmitSequential(b *testing.B) {
	handler := func(ctx context.Context, v int) error {
		return nil
	}

	pool, ctx, cancel := setupBenchPool(b, handler,
		WithMinWorkerCount[int](8),
		WithMaxWorkerCount[int](8),
	)
	defer cancel()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pool.Submit(ctx, i)
	}
}

// BenchmarkPool_Throughput measures actual processing throughput
func BenchmarkPool_Throughput(b *testing.B) {
	var processed atomic.Int64
	done := make(chan struct{})

	handler := func(ctx context.Context, v int) error {
		processed.Add(1)
		return nil
	}

	pool, ctx, cancel := setupBenchPool(b, handler,
		WithMinWorkerCount[int](8),
		WithMaxWorkerCount[int](8),
	)
	defer cancel()

	b.ResetTimer()
	start := time.Now()

	// Submit all work
	for i := 0; i < b.N; i++ {
		pool.Submit(ctx, i)
	}

	// Wait for processing to complete
	go func() {
		for processed.Load() < int64(b.N) {
			time.Sleep(1 * time.Millisecond)
		}
		close(done)
	}()

	<-done
	elapsed := time.Since(start)

	b.StopTimer()
	b.ReportMetric(float64(b.N)/elapsed.Seconds(), "ops/sec")
}

// BenchmarkPool_CPUBoundWork measures performance with CPU-bound work
func BenchmarkPool_CPUBoundWork(b *testing.B) {
	handler := func(ctx context.Context, v int) error {
		// Simulate CPU work
		sum := 0
		for i := 0; i < 1000; i++ {
			sum += i
		}
		return nil
	}

	pool, ctx, cancel := setupBenchPool(b, handler,
		WithMinWorkerCount[int](8),
		WithMaxWorkerCount[int](8),
	)
	defer cancel()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pool.Submit(ctx, i)
	}
}

// BenchmarkPool_ConcurrentSubmit measures concurrent submit performance
func BenchmarkPool_ConcurrentSubmit(b *testing.B) {
	handler := func(ctx context.Context, v int) error {
		return nil
	}

	pool, ctx, cancel := setupBenchPool(b, handler,
		WithMinWorkerCount[int](8),
		WithMaxWorkerCount[int](8),
	)
	defer cancel()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			pool.Submit(ctx, 1)
		}
	})
}

// BenchmarkPool_MetricsAccess measures metrics method performance
func BenchmarkPool_MetricsAccess(b *testing.B) {
	handler := func(ctx context.Context, v int) error {
		time.Sleep(1 * time.Millisecond)
		return nil
	}

	pool, ctx, cancel := setupBenchPool(b, handler,
		WithMinWorkerCount[int](5),
		WithMaxWorkerCount[int](5),
	)
	defer cancel()

	// Keep pool busy
	go func() {
		for i := 0; i < 1000; i++ {
			pool.Submit(ctx, i)
		}
	}()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pool.ActiveWorkers()
		pool.BusyWorkers()
		pool.QueueSize()
	}
}

// BenchmarkPool_MemoryAllocation measures allocations per submit
func BenchmarkPool_MemoryAllocation(b *testing.B) {
	handler := func(ctx context.Context, v int) error {
		return nil
	}

	pool, ctx, cancel := setupBenchPool(b, handler,
		WithMinWorkerCount[int](8),
		WithMaxWorkerCount[int](8),
	)
	defer cancel()

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		pool.Submit(ctx, i)
	}
}

// BenchmarkPool_Scaling measures autoscaling overhead
func BenchmarkPool_Scaling(b *testing.B) {
	configs := []struct {
		name string
		min  int32
		max  int32
	}{
		{"fixed_8", 8, 8},
		{"scale_2_to_16", 2, 16},
		{"scale_1_to_32", 1, 32},
	}

	for _, cfg := range configs {
		b.Run(cfg.name, func(b *testing.B) {
			handler := func(ctx context.Context, v int) error {
				return nil
			}

			pool, ctx, cancel := setupBenchPool(b, handler,
				WithMinWorkerCount[int](cfg.min),
				WithMaxWorkerCount[int](cfg.max),
				WithPollingFrequency[int](50*time.Millisecond),
				WithScalingCooldown[int](10*time.Millisecond),
			)
			defer cancel()

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				pool.Submit(ctx, i)
			}
		})
	}
}

// BenchmarkPool_Creation measures pool creation time
func BenchmarkPool_Creation(b *testing.B) {
	handler := func(ctx context.Context, v int) error {
		return nil
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pool, _ := New(handler,
			WithMinWorkerCount[int](5),
			WithMaxWorkerCount[int](10),
		)
		_ = pool
	}
}

// BenchmarkPool_StartStop measures pool lifecycle overhead
func BenchmarkPool_StartStop(b *testing.B) {
	handler := func(ctx context.Context, v int) error {
		return nil
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pool, _ := New(handler,
			WithMinWorkerCount[int](2),
			WithMaxWorkerCount[int](2),
		)

		ctx, cancel := context.WithCancel(context.Background())
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			pool.Start(ctx)
		}()

		time.Sleep(10 * time.Millisecond)
		cancel()
		wg.Wait()
	}
}

// BenchmarkPool_SubmitWithBackpressure measures submit latency when queue is nearly full
func BenchmarkPool_SubmitWithBackpressure(b *testing.B) {
	handler := func(ctx context.Context, v int) error {
		// Slow processing to create backpressure
		time.Sleep(100 * time.Microsecond)
		return nil
	}

	pool, err := New(handler,
		WithMinWorkerCount[int](2),
		WithMaxWorkerCount[int](2),
	)
	require.NoError(b, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go pool.Start(ctx)
	time.Sleep(50 * time.Millisecond)

	// Pre-fill queue to create backpressure
	for i := 0; i < 10; i++ {
		pool.Submit(ctx, i)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pool.Submit(ctx, i)
	}
}

// BenchmarkPool_ComparePoolSizes compares different pool configurations
func BenchmarkPool_ComparePoolSizes(b *testing.B) {
	configs := []struct {
		name string
		size int32
	}{
		{"workers_1", 1},
		{"workers_2", 2},
		{"workers_4", 4},
		{"workers_8", 8},
		{"workers_16", 16},
		{"workers_32", 32},
	}

	for _, cfg := range configs {
		b.Run(cfg.name, func(b *testing.B) {
			handler := func(ctx context.Context, v int) error {
				// Simulate minimal work
				_ = v * 2
				return nil
			}

			pool, ctx, cancel := setupBenchPool(b, handler,
				WithMinWorkerCount[int](cfg.size),
				WithMaxWorkerCount[int](cfg.size),
			)
			defer cancel()

			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					pool.Submit(ctx, 1)
				}
			})
		})
	}
}

// BenchmarkPool_ErrorHandling measures error path performance
func BenchmarkPool_ErrorHandling(b *testing.B) {
	var errCount atomic.Int64

	handler := func(ctx context.Context, v int) error {
		if v%100 == 0 {
			errCount.Add(1)
			// Don't return actual error as it kills workers
			// Just simulate error handling overhead
		}
		return nil
	}

	pool, ctx, cancel := setupBenchPool(b, handler,
		WithMinWorkerCount[int](8),
		WithMaxWorkerCount[int](8),
	)
	defer cancel()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pool.Submit(ctx, i)
	}

	b.ReportMetric(float64(errCount.Load()), "errors")
}

// BenchmarkPool_ChannelVsPool compares raw channel with pool
func BenchmarkPool_ChannelVsPool(b *testing.B) {
	b.Run("raw_channel", func(b *testing.B) {
		dataCh := make(chan int, 100)
		done := make(chan struct{})

		// Start worker
		go func() {
			for range dataCh {
				// Process
			}
			close(done)
		}()

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			dataCh <- i
		}
		b.StopTimer()

		close(dataCh)
		<-done
	})

	b.Run("worker_pool", func(b *testing.B) {
		handler := func(ctx context.Context, v int) error {
			return nil
		}

		pool, ctx, cancel := setupBenchPool(b, handler,
			WithMinWorkerCount[int](1),
			WithMaxWorkerCount[int](1),
		)
		defer cancel()

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			pool.Submit(ctx, i)
		}
	})
}

// BenchmarkPool_ContextCancellation measures context handling overhead
func BenchmarkPool_ContextCancellation(b *testing.B) {
	handler := func(ctx context.Context, v int) error {
		return nil
	}

	pool, _, cancel := setupBenchPool(b, handler,
		WithMinWorkerCount[int](8),
		WithMaxWorkerCount[int](8),
	)
	defer cancel()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ctx, cancel := context.WithCancel(context.Background())
		pool.Submit(ctx, i)
		cancel()
	}
}
