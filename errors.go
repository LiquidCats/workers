package workers

import "errors"

var (
	ErrInvalidMinWorkerCount    = errors.New("invalid min worker count")
	ErrInvalidMaxWorkerCount    = errors.New("invalid max worker count")
	ErrInvalidMinMaxWorkerCount = errors.New("max worker count must be greater than min worker count")
	ErrInvalidPollingFrequency  = errors.New("invalid polling frequency")
)
