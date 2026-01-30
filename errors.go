package workers

import "errors"

var (
	ErrInvalidMinWorkerCount    = errors.New("invalid min worker count")
	ErrInvalidMaxWorkerCount    = errors.New("invalid max worker count")
	ErrInvalidMinMaxWorkerCount = errors.New("max worker count must be greater than min worker count")
	ErrInvalidPollingFrequency  = errors.New("invalid polling frequency")
	ErrInvalidMaxLoad           = errors.New("max load must be between 0 and 1")
	ErrInvalidMinLoad           = errors.New("min load must be less than max load and >= 0")

	ErrPoolClosed = errors.New("pool is closed")
)
