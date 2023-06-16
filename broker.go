package tq

import (
	"context"
)

type Broker interface {

	// Add new messages to stream.
	Enqueue(ctx context.Context, msg *Task) error

	// Pull new messages from stream.
	Dequeue(ctx context.Context, name string) ([]*Task, error)

	// Ackowledge a task was completed by a worker.
	Ack(ctx context.Context, msgId string) error
}
