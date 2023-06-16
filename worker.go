package tq

import (
	"context"
	"fmt"
	"log"
	"sync"
)

type WorkerPool struct {

	// Pull tasks from distributed stream.
	broker Broker

	// Number of workers to spin up.
	count int

	// Pull tasks from broker into a local channel.
	taskStream chan *Task

	// Push results onto distributed stream.
	resultStream chan Result

	// Multiplex a set of task handlers on startup.
	handler *ServeMux

	// Done channel
	done chan struct{}
}

func NewWorkerPool(broker Broker, count int) WorkerPool {
	return WorkerPool{
		broker:       broker,
		count:        count,
		taskStream:   make(chan *Task),
		resultStream: make(chan Result),
		handler:      NewServeMux(),
		done:         make(chan struct{}),
	}
}

func (s *WorkerPool) Serve(ctx context.Context) {

    log.Println("Serving workers")

	var wg sync.WaitGroup

	// Pull events from broker into local channels.
    wg.Add(1)
    go s.read(ctx, "default", &wg)

	// Offload consumed tasks to workers.
	s.process(ctx, &wg)

    // Push resutls to result stream.
    wg.Add(1)
    go s.results(ctx, "result", &wg)

    wg.Wait()
}

func (s *WorkerPool) results(ctx context.Context, name string, wg *sync.WaitGroup) {

	log.Printf("Publishing results {stream: %s}", name)

	defer wg.Done()

	for {
		select {
		case <-ctx.Done():
			return 
        case r := <-s.resultStream:

            log.Printf("pushing results: %v", r)

             // Ack stream not to send task to other workers.
             //err := s.broker.Ack(ctx, r.Id)
             //if err != nil {
             //    log.Fatalln("unable to ack task: %w", err)
             //}

		}
	}
}

func (s *WorkerPool) read(ctx context.Context, name string, wg *sync.WaitGroup) {

	log.Printf("Consuming messages {stream: %s}", name)

	defer wg.Done()

	for {
		select {
		case <-ctx.Done():
			return 
		default:
			msgs, err := s.broker.Dequeue(ctx, name)
			if err != nil {
                log.Fatalln(err)
			}
			for _, msg := range msgs {
				s.taskStream <- msg
			}
		}
	}
}

func (s *WorkerPool) process(ctx context.Context, wg *sync.WaitGroup) {
	for i := 0; i < s.count; i++ {
		wg.Add(1)
		go worker(ctx, wg, s.handler, s.taskStream, s.resultStream)
	}
}

func (s *WorkerPool) Register(key string, handler func(context.Context, *Task) Result) {
	s.handler.Register(key, handler)
}

// Ensure confinement by keeping the concurrent scope small.
func worker(ctx context.Context, wg *sync.WaitGroup, handler *ServeMux, jobs <-chan *Task, results chan<- Result) {

	defer wg.Done()

	for {
		select {
		case task, ok := <-jobs:
			if !ok {
				return
			}
			// fan-in job execution multiplexing results into the results channel
			results <- handler.ProcessTask(ctx, task)
		case <-ctx.Done():
			fmt.Printf("cancelled worker. Error detail: %v\n", ctx.Err())
			results <- Result{
				Error: ctx.Err(),
			}
			return
		}
	}
}
