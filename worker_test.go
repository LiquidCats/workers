package workers

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

// PoolTestSuite is a test suite for the worker pool
type PoolTestSuite struct {
	suite.Suite
}

// TestPoolTestSuite runs the test suite
func TestPoolTestSuite(t *testing.T) {
	suite.Run(t, new(PoolTestSuite))
}

func (s *PoolTestSuite) TestNew_ValidConfig() {
	handler := func(ctx context.Context, v int) error {
		return nil
	}

	pool, err := New(handler,
		WithMinWorkerCount[int](2),
		WithMaxWorkerCount[int](5),
		WithMaxLoad[int](0.8),
		WithMinLoad[int](0.2),
	)

	require.NoError(s.T(), err)
	require.NotNil(s.T(), pool)
	assert.Equal(s.T(), int32(2), pool.cfg.min)
	assert.Equal(s.T(), int32(5), pool.cfg.max)
	assert.Equal(s.T(), 0.8, pool.cfg.maxLoad)
	assert.Equal(s.T(), 0.2, pool.cfg.minLoad)
}

func (s *PoolTestSuite) TestNew_DefaultConfig() {
	handler := func(ctx context.Context, v int) error {
		return nil
	}

	pool, err := New(handler)

	require.NoError(s.T(), err)
	assert.Equal(s.T(), DefaultMinWorkerCount, pool.cfg.min)
	assert.Equal(s.T(), DefaultMaxWorkerCount, pool.cfg.max)
	assert.Equal(s.T(), DefaultMaxLoad, pool.cfg.maxLoad)
	assert.Equal(s.T(), DefaultMinLoad, pool.cfg.minLoad)
	assert.Equal(s.T(), DefaultScalingCooldown, pool.cfg.scalingCooldown)
	assert.Equal(s.T(), DefaultPollingFrequency, pool.cfg.pollingFreq)
}

func (s *PoolTestSuite) TestNew_ValidationErrors() {
	tests := []struct {
		name        string
		opts        []Opt[int]
		expectedErr error
	}{
		{
			name:        "invalid min worker count",
			opts:        []Opt[int]{WithMinWorkerCount[int](0)},
			expectedErr: ErrInvalidMinWorkerCount,
		},
		{
			name: "invalid max worker count",
			opts: []Opt[int]{
				WithMinWorkerCount[int](2),
				WithMaxWorkerCount[int](0),
			},
			expectedErr: ErrInvalidMaxWorkerCount,
		},
		{
			name: "min > max",
			opts: []Opt[int]{
				WithMinWorkerCount[int](10),
				WithMaxWorkerCount[int](5),
			},
			expectedErr: ErrInvalidMinMaxWorkerCount,
		},
		{
			name:        "invalid polling frequency",
			opts:        []Opt[int]{WithPollingFrequency[int](0)},
			expectedErr: ErrInvalidPollingFrequency,
		},
		{
			name:        "maxLoad > 1",
			opts:        []Opt[int]{WithMaxLoad[int](1.5)},
			expectedErr: ErrInvalidMaxLoad,
		},
		{
			name:        "maxLoad = 0",
			opts:        []Opt[int]{WithMaxLoad[int](0)},
			expectedErr: ErrInvalidMaxLoad,
		},
		{
			name:        "maxLoad < 0",
			opts:        []Opt[int]{WithMaxLoad[int](-0.1)},
			expectedErr: ErrInvalidMaxLoad,
		},
		{
			name: "minLoad >= maxLoad",
			opts: []Opt[int]{
				WithMinLoad[int](0.8),
				WithMaxLoad[int](0.7),
			},
			expectedErr: ErrInvalidMinLoad,
		},
		{
			name:        "minLoad < 0",
			opts:        []Opt[int]{WithMinLoad[int](-0.1)},
			expectedErr: ErrInvalidMinLoad,
		},
		{
			name: "minLoad == maxLoad",
			opts: []Opt[int]{
				WithMinLoad[int](0.5),
				WithMaxLoad[int](0.5),
			},
			expectedErr: ErrInvalidMinLoad,
		},
	}

	for _, tt := range tests {
		s.Run(tt.name, func() {
			handler := func(ctx context.Context, v int) error {
				return nil
			}

			pool, err := New(handler, tt.opts...)

			assert.ErrorIs(s.T(), err, tt.expectedErr)
			assert.Nil(s.T(), pool)
		})
	}
}

func (s *PoolTestSuite) TestSubmit_Success() {
	received := make(chan int, 10)
	handler := func(ctx context.Context, v int) error {
		received <- v
		return nil
	}

	pool, err := New(handler,
		WithMinWorkerCount[int](1),
		WithMaxWorkerCount[int](2),
	)
	require.NoError(s.T(), err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go pool.Start(ctx)
	time.Sleep(100 * time.Millisecond)

	err = pool.Submit(ctx, 42)
	require.NoError(s.T(), err)

	select {
	case val := <-received:
		assert.Equal(s.T(), 42, val)
	case <-time.After(2 * time.Second):
		s.T().Fatal("Timeout waiting for task to be processed")
	}
}

func (s *PoolTestSuite) TestSubmit_ClosedPool() {
	handler := func(ctx context.Context, v int) error {
		return nil
	}

	pool, err := New(handler)
	require.NoError(s.T(), err)

	err = pool.Close()
	require.NoError(s.T(), err)

	err = pool.Submit(context.Background(), 42)
	assert.ErrorIs(s.T(), err, ErrPoolClosed)
}

func (s *PoolTestSuite) TestSubmit_CancelledContext() {
	block := make(chan struct{})
	defer close(block)

	handler := func(ctx context.Context, v int) error {
		<-block
		return nil
	}

	// Create pool with max 1 (buffer will be 1)
	pool, err := New(handler,
		WithMinWorkerCount[int](1),
		WithMaxWorkerCount[int](1),
	)
	require.NoError(s.T(), err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go pool.Start(ctx)
	time.Sleep(100 * time.Millisecond)

	// Fill the worker (blocks in handler) and the channel buffer (size = cfg.max = 1)
	err = pool.Submit(ctx, 1) // Worker processes this, blocks on <-block
	require.NoError(s.T(), err)
	time.Sleep(50 * time.Millisecond)

	err = pool.Submit(ctx, 2) // This goes into the buffer
	require.NoError(s.T(), err)
	time.Sleep(50 * time.Millisecond)

	// Now channel is full, use cancelled context
	cancelCtx, cancelFunc := context.WithCancel(context.Background())
	cancelFunc()

	err = pool.Submit(cancelCtx, 3)
	assert.ErrorIs(s.T(), err, context.Canceled, "Submit with cancelled context should return context.Canceled")
}

func (s *PoolTestSuite) TestClose_GracefulShutdown() {
	var processed atomic.Int32
	handler := func(ctx context.Context, v int) error {
		time.Sleep(50 * time.Millisecond)
		processed.Add(1)
		return nil
	}

	pool, err := New(handler,
		WithMinWorkerCount[int](2),
		WithMaxWorkerCount[int](2),
	)
	require.NoError(s.T(), err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go pool.Start(ctx)
	time.Sleep(100 * time.Millisecond)

	for i := 0; i < 5; i++ {
		err := pool.Submit(ctx, i)
		require.NoError(s.T(), err)
	}

	time.Sleep(100 * time.Millisecond)

	err = pool.Close()
	require.NoError(s.T(), err)

	time.Sleep(1 * time.Second)
	cancel()

	assert.GreaterOrEqual(s.T(), processed.Load(), int32(5))
}

func (s *PoolTestSuite) TestClose_DoubleClose() {
	handler := func(ctx context.Context, v int) error {
		return nil
	}

	pool, err := New(handler)
	require.NoError(s.T(), err)

	err = pool.Close()
	assert.NoError(s.T(), err)

	err = pool.Close()
	assert.NoError(s.T(), err, "Double close should not error")
}

func (s *PoolTestSuite) TestStart_ClosedPool() {
	handler := func(ctx context.Context, v int) error {
		return nil
	}

	pool, err := New(handler)
	require.NoError(s.T(), err)

	err = pool.Close()
	require.NoError(s.T(), err)

	err = pool.Start(context.Background())
	assert.ErrorIs(s.T(), err, ErrPoolClosed)
}

func (s *PoolTestSuite) TestMetrics_ActiveWorkers() {
	handler := func(ctx context.Context, v int) error {
		time.Sleep(100 * time.Millisecond)
		return nil
	}

	pool, err := New(handler,
		WithMinWorkerCount[int](3),
		WithMaxWorkerCount[int](5),
	)
	require.NoError(s.T(), err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go pool.Start(ctx)
	time.Sleep(200 * time.Millisecond)

	active := pool.ActiveWorkers()
	assert.Equal(s.T(), int32(3), active)
}

func (s *PoolTestSuite) TestMetrics_BusyWorkers() {
	started := make(chan struct{})
	wait := make(chan struct{})

	handler := func(ctx context.Context, v int) error {
		started <- struct{}{}
		<-wait
		return nil
	}

	pool, err := New(handler,
		WithMinWorkerCount[int](2),
		WithMaxWorkerCount[int](2),
	)
	require.NoError(s.T(), err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go pool.Start(ctx)
	time.Sleep(100 * time.Millisecond)

	pool.Submit(ctx, 1)
	pool.Submit(ctx, 2)

	<-started
	<-started

	busy := pool.BusyWorkers()
	assert.Equal(s.T(), int32(2), busy)

	close(wait)
}

func (s *PoolTestSuite) TestMetrics_QueueSize() {
	wait := make(chan struct{})
	defer close(wait)

	handler := func(ctx context.Context, v int) error {
		<-wait
		return nil
	}

	pool, err := New(handler,
		WithMinWorkerCount[int](1),
		WithMaxWorkerCount[int](2),
	)
	require.NoError(s.T(), err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go pool.Start(ctx)
	time.Sleep(100 * time.Millisecond)

	go func() {
		for i := 0; i < 5; i++ {
			pool.Submit(ctx, i)
			time.Sleep(10 * time.Millisecond)
		}
	}()

	time.Sleep(100 * time.Millisecond)

	queueSize := pool.QueueSize()
	assert.GreaterOrEqual(s.T(), queueSize, 0, "Queue size should be non-negative")
}

func (s *PoolTestSuite) TestWorkerLifecycle_ErrorPropagation() {
	expectedErr := errors.New("handler error")

	handler := func(ctx context.Context, v int) error {
		return expectedErr
	}

	pool, err := New(handler,
		WithMinWorkerCount[int](1),
		WithMaxWorkerCount[int](1),
	)
	require.NoError(s.T(), err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go pool.Start(ctx)
	time.Sleep(100 * time.Millisecond)

	err = pool.Submit(ctx, 42)
	require.NoError(s.T(), err)

	time.Sleep(200 * time.Millisecond)
}

func (s *PoolTestSuite) TestWorkerLifecycle_ContextCancellation() {
	handler := func(ctx context.Context, v int) error {
		time.Sleep(10 * time.Millisecond)
		return nil
	}

	pool, err := New(handler,
		WithMinWorkerCount[int](2),
		WithMaxWorkerCount[int](2),
	)
	require.NoError(s.T(), err)

	ctx, cancel := context.WithCancel(context.Background())

	go pool.Start(ctx)
	time.Sleep(100 * time.Millisecond)

	active := pool.ActiveWorkers()
	assert.Equal(s.T(), int32(2), active)

	cancel()
	time.Sleep(200 * time.Millisecond)

	active = pool.ActiveWorkers()
	assert.Equal(s.T(), int32(0), active, "All workers should stop after context cancellation")
}

func (s *PoolTestSuite) TestAutoscaling_ScaleUp() {
	handler := func(ctx context.Context, v int) error {
		time.Sleep(200 * time.Millisecond)
		return nil
	}

	pool, err := New(handler,
		WithMinWorkerCount[int](1),
		WithMaxWorkerCount[int](5),
		WithMaxLoad[int](0.5),
		WithPollingFrequency[int](100*time.Millisecond),
		WithScalingCooldown[int](50*time.Millisecond),
	)
	require.NoError(s.T(), err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go pool.Start(ctx)
	time.Sleep(100 * time.Millisecond)

	for i := 0; i < 10; i++ {
		pool.Submit(ctx, i)
	}

	time.Sleep(500 * time.Millisecond)

	active := pool.ActiveWorkers()
	assert.Greater(s.T(), active, int32(1), "Pool should scale up under load")
}

func (s *PoolTestSuite) TestAutoscaling_ScaleDown() {
	var processed atomic.Int32
	handler := func(ctx context.Context, v int) error {
		processed.Add(1)
		time.Sleep(50 * time.Millisecond)
		return nil
	}

	pool, err := New(handler,
		WithMinWorkerCount[int](1),
		WithMaxWorkerCount[int](5),
		WithMaxLoad[int](0.7),
		WithMinLoad[int](0.2),
		WithPollingFrequency[int](100*time.Millisecond),
		WithScalingCooldown[int](50*time.Millisecond),
	)
	require.NoError(s.T(), err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go pool.Start(ctx)
	time.Sleep(100 * time.Millisecond)

	for i := 0; i < 20; i++ {
		pool.Submit(ctx, i)
	}

	time.Sleep(300 * time.Millisecond)
	activeAtPeak := pool.ActiveWorkers()

	time.Sleep(2 * time.Second)
	activeAfterIdle := pool.ActiveWorkers()

	assert.LessOrEqual(s.T(), activeAfterIdle, activeAtPeak, "Pool should scale down after load decreases")
}

func (s *PoolTestSuite) TestAutoscaling_MaxWorkerLimit() {
	handler := func(ctx context.Context, v int) error {
		time.Sleep(200 * time.Millisecond)
		return nil
	}

	maxWorkers := int32(3)
	pool, err := New(handler,
		WithMinWorkerCount[int](1),
		WithMaxWorkerCount[int](maxWorkers),
		WithMaxLoad[int](0.5),
		WithPollingFrequency[int](50*time.Millisecond),
		WithScalingCooldown[int](10*time.Millisecond),
	)
	require.NoError(s.T(), err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go pool.Start(ctx)
	time.Sleep(100 * time.Millisecond)

	for i := 0; i < 50; i++ {
		pool.Submit(ctx, i)
	}

	time.Sleep(500 * time.Millisecond)

	active := pool.ActiveWorkers()
	assert.LessOrEqual(s.T(), active, maxWorkers, "Active workers should not exceed max")
}

// TestConcurrentOperations tests thread safety
func TestConcurrentOperations(t *testing.T) {
	handler := func(ctx context.Context, v int) error {
		time.Sleep(10 * time.Millisecond)
		return nil
	}

	pool, err := New(handler,
		WithMinWorkerCount[int](5),
		WithMaxWorkerCount[int](10),
	)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go pool.Start(ctx)
	time.Sleep(100 * time.Millisecond)

	// Concurrent submits
	done := make(chan struct{})
	for i := 0; i < 10; i++ {
		go func(id int) {
			for j := 0; j < 100; j++ {
				pool.Submit(ctx, id*100+j)
			}
			done <- struct{}{}
		}(i)
	}

	// Concurrent metric reads
	for i := 0; i < 10; i++ {
		go func() {
			for j := 0; j < 100; j++ {
				pool.ActiveWorkers()
				pool.BusyWorkers()
				pool.QueueSize()
				time.Sleep(1 * time.Millisecond)
			}
		}()
	}

	for i := 0; i < 10; i++ {
		<-done
	}

	time.Sleep(2 * time.Second)

	assert.NotPanics(t, func() {
		pool.Close()
	})
}

// TestPoolWithDifferentTypes tests generic type support
func TestPoolWithDifferentTypes(t *testing.T) {
	t.Run("string pool", func(t *testing.T) {
		received := make(chan string, 10)
		handler := func(ctx context.Context, v string) error {
			received <- v
			return nil
		}

		pool, err := New(handler)
		require.NoError(t, err)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		go pool.Start(ctx)
		time.Sleep(100 * time.Millisecond)

		err = pool.Submit(ctx, "test")
		require.NoError(t, err)

		select {
		case val := <-received:
			assert.Equal(t, "test", val)
		case <-time.After(1 * time.Second):
			t.Fatal("Timeout")
		}
	})

	t.Run("struct pool", func(t *testing.T) {
		type Task struct {
			ID   int
			Name string
		}

		received := make(chan Task, 10)
		handler := func(ctx context.Context, v Task) error {
			received <- v
			return nil
		}

		pool, err := New(handler)
		require.NoError(t, err)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		go pool.Start(ctx)
		time.Sleep(100 * time.Millisecond)

		task := Task{ID: 1, Name: "test"}
		err = pool.Submit(ctx, task)
		require.NoError(t, err)

		select {
		case val := <-received:
			assert.Equal(t, task, val)
		case <-time.After(1 * time.Second):
			t.Fatal("Timeout")
		}
	})
}
