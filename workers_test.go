package workers_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LiquidCats/workers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockHandler is a test implementation of Handler interface
type mockHandler struct {
	handleFunc func(ctx context.Context, data int)
	callCount  atomic.Int32
	mu         sync.Mutex
	calls      []int
}

func (m *mockHandler) Handle(ctx context.Context, data int) {
	m.callCount.Add(1)
	m.mu.Lock()
	m.calls = append(m.calls, data)
	m.mu.Unlock()

	if m.handleFunc != nil {
		m.handleFunc(ctx, data)
	}
}

func (m *mockHandler) GetCallCount() int32 {
	return m.callCount.Load()
}

func (m *mockHandler) GetCalls() []int {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make([]int, len(m.calls))
	copy(result, m.calls)
	return result
}

func TestNewRunner(t *testing.T) {
	t.Run("success with defaults", func(t *testing.T) {
		handler := &mockHandler{}
		runner, dataCh, err := workers.NewRunner[int](handler)

		require.NoError(t, err)
		assert.NotNil(t, runner)
		assert.NotNil(t, dataCh)
	})

	t.Run("success with custom options", func(t *testing.T) {
		handler := &mockHandler{}
		runner, dataCh, err := workers.NewRunner[int](
			handler,
			workers.WithMinWorkerCount[int](2),
			workers.WithMaxWorkerCount[int](10),
			workers.WithPollingFrequency[int](100*time.Millisecond),
		)

		require.NoError(t, err)
		assert.NotNil(t, runner)
		assert.NotNil(t, dataCh)
	})

	t.Run("error when min worker count is zero", func(t *testing.T) {
		handler := &mockHandler{}
		runner, dataCh, err := workers.NewRunner[int](
			handler,
			workers.WithMinWorkerCount[int](0),
		)

		assert.Error(t, err)
		assert.ErrorIs(t, err, workers.ErrInvalidMinWorkerCount)
		assert.Nil(t, runner)
		assert.Nil(t, dataCh)
	})

	t.Run("error when max worker count is zero", func(t *testing.T) {
		handler := &mockHandler{}
		runner, dataCh, err := workers.NewRunner[int](
			handler,
			workers.WithMaxWorkerCount[int](0),
		)

		assert.Error(t, err)
		assert.ErrorIs(t, err, workers.ErrInvalidMaxWorkerCount)
		assert.Nil(t, runner)
		assert.Nil(t, dataCh)
	})

	t.Run("error when min is greater than max", func(t *testing.T) {
		handler := &mockHandler{}
		runner, dataCh, err := workers.NewRunner[int](
			handler,
			workers.WithMinWorkerCount[int](10),
			workers.WithMaxWorkerCount[int](5),
		)

		assert.Error(t, err)
		assert.ErrorIs(t, err, workers.ErrInvalidMinMaxWorkerCount)
		assert.Nil(t, runner)
		assert.Nil(t, dataCh)
	})

	t.Run("error when polling frequency is zero", func(t *testing.T) {
		handler := &mockHandler{}
		runner, dataCh, err := workers.NewRunner[int](
			handler,
			workers.WithPollingFrequency[int](0),
		)

		assert.Error(t, err)
		assert.ErrorIs(t, err, workers.ErrInvalidPollingFrequency)
		assert.Nil(t, runner)
		assert.Nil(t, dataCh)
	})
}

func TestRunner_Run_BasicExecution(t *testing.T) {
	t.Run("processes data successfully", func(t *testing.T) {
		handler := &mockHandler{}
		runner, dataCh, err := workers.NewRunner[int](handler)
		require.NoError(t, err)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		errCh := make(chan error, 1)
		go func() {
			errCh <- runner.Run(ctx)
		}()

		// Send some data
		for i := 0; i < 5; i++ {
			dataCh <- i
		}

		// Wait for processing
		time.Sleep(100 * time.Millisecond)

		// Cancel and wait for completion
		cancel()
		err = <-errCh

		assert.Error(t, err)
		assert.ErrorIs(t, err, context.Canceled)
		assert.Equal(t, int32(5), handler.GetCallCount())
	})

	t.Run("stops when context is cancelled", func(t *testing.T) {
		handler := &mockHandler{
			handleFunc: func(ctx context.Context, data int) {
				time.Sleep(10 * time.Millisecond)
			},
		}
		runner, dataCh, err := workers.NewRunner[int](handler)
		require.NoError(t, err)

		ctx, cancel := context.WithCancel(context.Background())

		errCh := make(chan error, 1)
		go func() {
			errCh <- runner.Run(ctx)
		}()

		// Send some data
		dataCh <- 1

		// Cancel immediately
		cancel()
		err = <-errCh

		assert.Error(t, err)
		assert.ErrorIs(t, err, context.Canceled)
	})

	t.Run("stops when data channel is closed", func(t *testing.T) {
		handler := &mockHandler{}
		runner, dataCh, err := workers.NewRunner[int](handler)
		require.NoError(t, err)

		ctx := context.Background()

		errCh := make(chan error, 1)
		go func() {
			errCh <- runner.Run(ctx)
		}()

		// Send data and close channel
		dataCh <- 1
		dataCh <- 2
		close(dataCh)

		// Wait for completion
		err = <-errCh

		assert.NoError(t, err)
		assert.GreaterOrEqual(t, handler.GetCallCount(), int32(2))
	})
}

func TestRunner_Run_WorkerScaling(t *testing.T) {
	t.Run("scales up when busy", func(t *testing.T) {
		processingCh := make(chan struct{})
		releaseCh := make(chan struct{})

		handler := &mockHandler{
			handleFunc: func(ctx context.Context, data int) {
				processingCh <- struct{}{}
				<-releaseCh
			},
		}

		runner, dataCh, err := workers.NewRunner[int](
			handler,
			workers.WithMinWorkerCount[int](2),
			workers.WithMaxWorkerCount[int](5),
			workers.WithPollingFrequency[int](50*time.Millisecond),
		)
		require.NoError(t, err)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		go func() {
			_ = runner.Run(ctx)
		}()

		// Fill up workers to trigger scaling
		for i := 0; i < 4; i++ {
			dataCh <- i
		}

		// Wait for workers to start processing
		for i := 0; i < 2; i++ {
			<-processingCh
		}

		// Wait for scaling logic to run
		time.Sleep(150 * time.Millisecond)

		// Release workers
		close(releaseCh)
		time.Sleep(50 * time.Millisecond)

		cancel()

		assert.GreaterOrEqual(t, handler.GetCallCount(), int32(2))
	})

	t.Run("scales down when idle", func(t *testing.T) {
		handler := &mockHandler{
			handleFunc: func(ctx context.Context, data int) {
				time.Sleep(10 * time.Millisecond)
			},
		}

		runner, dataCh, err := workers.NewRunner[int](
			handler,
			workers.WithMinWorkerCount[int](2),
			workers.WithMaxWorkerCount[int](5),
			workers.WithPollingFrequency[int](50*time.Millisecond),
		)
		require.NoError(t, err)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		go func() {
			_ = runner.Run(ctx)
		}()

		// Send initial burst
		for i := 0; i < 10; i++ {
			dataCh <- i
		}

		// Wait for processing and scaling
		time.Sleep(200 * time.Millisecond)

		// Stop and verify
		cancel()
		time.Sleep(50 * time.Millisecond)

		assert.GreaterOrEqual(t, handler.GetCallCount(), int32(10))
	})
}

func TestRunner_Run_ConcurrentDataProcessing(t *testing.T) {
	t.Run("handles concurrent data correctly", func(t *testing.T) {
		handler := &mockHandler{}
		runner, dataCh, err := workers.NewRunner[int](
			handler,
			workers.WithMinWorkerCount[int](3),
			workers.WithMaxWorkerCount[int](10),
		)
		require.NoError(t, err)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		go func() {
			_ = runner.Run(ctx)
		}()

		// Send data concurrently
		const numItems = 50
		var wg sync.WaitGroup
		wg.Add(numItems)

		for i := 0; i < numItems; i++ {
			go func(val int) {
				defer wg.Done()
				dataCh <- val
			}(i)
		}

		wg.Wait()
		time.Sleep(200 * time.Millisecond)

		cancel()
		time.Sleep(50 * time.Millisecond)

		assert.Equal(t, int32(numItems), handler.GetCallCount())
	})
}

func TestRunner_Run_WithDifferentTypes(t *testing.T) {
	t.Run("works with string type", func(t *testing.T) {
		type stringHandler struct {
			calls []string
			mu    sync.Mutex
		}

		handler := &stringHandler{}
		runner, dataCh, err := workers.NewRunner[string](
			handlerFunc[string](func(ctx context.Context, data string) {
				handler.mu.Lock()
				handler.calls = append(handler.calls, data)
				handler.mu.Unlock()
			}),
		)
		require.NoError(t, err)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		go func() {
			_ = runner.Run(ctx)
		}()

		dataCh <- "hello"
		dataCh <- "world"

		time.Sleep(100 * time.Millisecond)
		cancel()

		handler.mu.Lock()
		assert.Len(t, handler.calls, 2)
		handler.mu.Unlock()
	})
}

// handlerFunc is a helper to create a Handler from a function
type handlerFunc[T any] func(ctx context.Context, data T)

func (f handlerFunc[T]) Handle(ctx context.Context, data T) {
	f(ctx, data)
}

// Benchmarks

func BenchmarkRunner_SingleWorker(b *testing.B) {
	handler := &mockHandler{}
	runner, dataCh, err := workers.NewRunner[int](
		handler,
		workers.WithMinWorkerCount[int](1),
		workers.WithMaxWorkerCount[int](1),
	)
	require.NoError(b, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		_ = runner.Run(ctx)
	}()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dataCh <- i
	}
	b.StopTimer()

	// Wait for processing
	time.Sleep(100 * time.Millisecond)
}

func BenchmarkRunner_MinimalWorkers(b *testing.B) {
	handler := &mockHandler{}
	runner, dataCh, err := workers.NewRunner[int](
		handler,
		workers.WithMinWorkerCount[int](3),
		workers.WithMaxWorkerCount[int](3),
	)
	require.NoError(b, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		_ = runner.Run(ctx)
	}()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dataCh <- i
	}
	b.StopTimer()

	// Wait for processing
	time.Sleep(100 * time.Millisecond)
}

func BenchmarkRunner_ManyWorkers(b *testing.B) {
	handler := &mockHandler{}
	runner, dataCh, err := workers.NewRunner[int](
		handler,
		workers.WithMinWorkerCount[int](10),
		workers.WithMaxWorkerCount[int](20),
	)
	require.NoError(b, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		_ = runner.Run(ctx)
	}()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dataCh <- i
	}
	b.StopTimer()

	// Wait for processing
	time.Sleep(100 * time.Millisecond)
}

func BenchmarkRunner_WithScaling(b *testing.B) {
	handler := &mockHandler{}
	runner, dataCh, err := workers.NewRunner[int](
		handler,
		workers.WithMinWorkerCount[int](2),
		workers.WithMaxWorkerCount[int](50),
		workers.WithPollingFrequency[int](10*time.Millisecond),
	)
	require.NoError(b, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		_ = runner.Run(ctx)
	}()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dataCh <- i
	}
	b.StopTimer()

	// Wait for processing
	time.Sleep(100 * time.Millisecond)
}

func BenchmarkRunner_WithSlowHandler(b *testing.B) {
	handler := &mockHandler{
		handleFunc: func(ctx context.Context, data int) {
			time.Sleep(1 * time.Millisecond)
		},
	}
	runner, dataCh, err := workers.NewRunner[int](
		handler,
		workers.WithMinWorkerCount[int](5),
		workers.WithMaxWorkerCount[int](10),
	)
	require.NoError(b, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		_ = runner.Run(ctx)
	}()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dataCh <- i
	}
	b.StopTimer()

	// Wait for processing
	time.Sleep(time.Duration(b.N+100) * time.Millisecond)
}

func BenchmarkRunner_Parallel(b *testing.B) {
	handler := &mockHandler{}
	runner, dataCh, err := workers.NewRunner[int](
		handler,
		workers.WithMinWorkerCount[int](5),
		workers.WithMaxWorkerCount[int](20),
	)
	require.NoError(b, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		_ = runner.Run(ctx)
	}()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			dataCh <- i
			i++
		}
	})
	b.StopTimer()

	// Wait for processing
	time.Sleep(100 * time.Millisecond)
}

func BenchmarkRunner_HighContention(b *testing.B) {
	handler := &mockHandler{
		handleFunc: func(ctx context.Context, data int) {
			// Simulate some work
			time.Sleep(100 * time.Microsecond)
		},
	}
	runner, dataCh, err := workers.NewRunner[int](
		handler,
		workers.WithMinWorkerCount[int](10),
		workers.WithMaxWorkerCount[int](100),
		workers.WithPollingFrequency[int](5*time.Millisecond),
	)
	require.NoError(b, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		_ = runner.Run(ctx)
	}()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			dataCh <- i
			i++
		}
	})
	b.StopTimer()

	// Wait for processing
	time.Sleep(200 * time.Millisecond)
}

func BenchmarkRunner_DifferentDataTypes(b *testing.B) {
	b.Run("int", func(b *testing.B) {
		handler := &mockHandler{}
		runner, dataCh, err := workers.NewRunner[int](handler)
		require.NoError(b, err)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		go func() {
			_ = runner.Run(ctx)
		}()

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			dataCh <- i
		}
		b.StopTimer()
		time.Sleep(50 * time.Millisecond)
	})

	b.Run("string", func(b *testing.B) {
		handler := handlerFunc[string](func(ctx context.Context, data string) {})
		runner, dataCh, err := workers.NewRunner[string](handler)
		require.NoError(b, err)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		go func() {
			_ = runner.Run(ctx)
		}()

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			dataCh <- "test"
		}
		b.StopTimer()
		time.Sleep(50 * time.Millisecond)
	})

	b.Run("struct", func(b *testing.B) {
		type payload struct {
			ID    int
			Value string
		}
		handler := handlerFunc[payload](func(ctx context.Context, data payload) {})
		runner, dataCh, err := workers.NewRunner[payload](handler)
		require.NoError(b, err)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		go func() {
			_ = runner.Run(ctx)
		}()

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			dataCh <- payload{ID: i, Value: "test"}
		}
		b.StopTimer()
		time.Sleep(50 * time.Millisecond)
	})
}

func BenchmarkNewRunner(b *testing.B) {
	handler := &mockHandler{}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, _, _ = workers.NewRunner[int](handler)
	}
}

func BenchmarkNewRunner_WithOptions(b *testing.B) {
	handler := &mockHandler{}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, _, _ = workers.NewRunner[int](
			handler,
			workers.WithMinWorkerCount[int](5),
			workers.WithMaxWorkerCount[int](10),
			workers.WithPollingFrequency[int](100*time.Millisecond),
		)
	}
}

func BenchmarkRunner_ScalingBehavior(b *testing.B) {
	b.Run("scale_up", func(b *testing.B) {
		handler := &mockHandler{
			handleFunc: func(ctx context.Context, data int) {
				time.Sleep(50 * time.Millisecond)
			},
		}
		runner, dataCh, err := workers.NewRunner[int](
			handler,
			workers.WithMinWorkerCount[int](2),
			workers.WithMaxWorkerCount[int](20),
			workers.WithPollingFrequency[int](10*time.Millisecond),
		)
		require.NoError(b, err)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		go func() {
			_ = runner.Run(ctx)
		}()

		b.ResetTimer()
		// Burst of work to trigger scaling
		for i := 0; i < b.N; i++ {
			select {
			case dataCh <- i:
			case <-time.After(100 * time.Millisecond):
				b.Fatal("timeout sending data")
			}
		}
		b.StopTimer()
		time.Sleep(200 * time.Millisecond)
	})

	b.Run("scale_down", func(b *testing.B) {
		handler := &mockHandler{}
		runner, dataCh, err := workers.NewRunner[int](
			handler,
			workers.WithMinWorkerCount[int](2),
			workers.WithMaxWorkerCount[int](20),
			workers.WithPollingFrequency[int](10*time.Millisecond),
		)
		require.NoError(b, err)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		go func() {
			_ = runner.Run(ctx)
		}()

		// Pre-fill to scale up
		for i := 0; i < 20; i++ {
			dataCh <- i
		}
		time.Sleep(100 * time.Millisecond)

		b.ResetTimer()
		// Minimal work to trigger scaling down
		for i := 0; i < b.N; i++ {
			dataCh <- i
			time.Sleep(50 * time.Millisecond)
		}
		b.StopTimer()
	})
}

func BenchmarkRunner_MemoryAllocation(b *testing.B) {
	handler := &mockHandler{}
	runner, dataCh, err := workers.NewRunner[int](
		handler,
		workers.WithMinWorkerCount[int](5),
		workers.WithMaxWorkerCount[int](10),
	)
	require.NoError(b, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		_ = runner.Run(ctx)
	}()

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		dataCh <- i
	}
	b.StopTimer()
	time.Sleep(100 * time.Millisecond)
}
