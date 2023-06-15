package tq

import (
	"context"
)

// ProcessTask should return nil if the processing of a task is successful.
//
// If ProcessTask returns a non-nil error or panics, the task
// will be retried if retry-count is remaining,
// otherwise the task will be moved to the dead-letter queue.
type Handler interface {
	ProcessTask(context.Context, *Task) Result
}

// The HandlerFunc type is an adapter to allow the use of ordinary functions as a Handler.
// If f is a function with the appropriate signature, HandlerFunc(f) is a Handler that calls f.
type HandlerFunc func(context.Context, *Task) Result

func (fn HandlerFunc) ProcessTask(ctx context.Context, task *Task) Result {
	return fn(ctx, task)
}
