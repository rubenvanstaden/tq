package tq

import (
	"context"
	"fmt"
	"sync"
)

type WorkerPool struct {

	// Pull tasks from distributed stream.
	broker Broker

	// Number of workers to spin up.
	count int

	// Pull tasks from broker into a local channel.
	taskStream chan Task

	// Push results onto distributed stream.
	resultStream chan Result

	// Multiplex a set of task handlers on startup.
	handler *ServeMux

	// Done channel
	done chan struct{}
}

func New(count int) WorkerPool {
	return WorkerPool{
		count:        count,
		taskStream:   make(chan Task),
		resultStream: make(chan Result),
		handler:      NewServeMux(),
		done:         make(chan struct{}),
	}
}

func (s *WorkerPool) Serve(ctx context.Context) {

	// Pull events from broker into local channels.
	//var wgc sync.WaitGroup
	//s.consume(ctx, &wgc)

	// Offload consumed events to workers.
	var wgp sync.WaitGroup
	s.process(ctx, &wgp)
}

func (s *WorkerPool) consume(ctx context.Context, wg *sync.WaitGroup) error {
	return nil
}

func (s *WorkerPool) process(ctx context.Context, wg *sync.WaitGroup) {
	for i := 0; i < s.count; i++ {
		wg.Add(1)
		go worker(ctx, wg, s.handler, s.taskStream, s.resultStream)
	}
}

func (s *WorkerPool) Register(pattern string, handler func(context.Context, *Task) Result) {
	s.handler.Register(pattern, handler)
}

// Ensure confinement by keeping the concurrent scope small.
func worker(ctx context.Context, wg *sync.WaitGroup, handler *ServeMux, jobs <-chan Task, results chan<- Result) {
	defer wg.Done()
	for {
		select {
		case task, ok := <-jobs:
			if !ok {
				return
			}
			// fan-in job execution multiplexing results into the results channel
			results <- handler.ProcessTask(ctx, &task)
		case <-ctx.Done():
			fmt.Printf("cancelled worker. Error detail: %v\n", ctx.Err())
			results <- Result{
				Error: ctx.Err(),
			}
			return
		}
	}
}
