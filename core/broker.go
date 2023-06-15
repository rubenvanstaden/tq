package tq

import (
	"context"
)

type Broker interface {

	// Add new messages to stream.
	Enqueue(ctx context.Context, message *Task) error

	// Pull new messages from stream.
	Messages(ctx context.Context, name string) ([]*Task, error)
}
