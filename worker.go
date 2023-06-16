package tq

import (
	"context"
	"fmt"
	"log"
	"sync"
)

type WorkerPool struct {

	// Number of workers to spin up.
	count int

	// Pull tasks from distributed stream.
	tasks TaskQueue

	// Push results onto distributed stream.
	results TaskQueue

	// Multiplex a set of task handlers on startup.
	handler *ServeMux

	// Done channel
	done chan struct{}
}

func NewWorkerPool(tasks TaskQueue, results TaskQueue, count int) WorkerPool {
	return WorkerPool{
		count:        count,
		tasks:   tasks,
		results: results,
		handler:      NewServeMux(),
		done:         make(chan struct{}),
	}
}

func (s *WorkerPool) Serve(ctx context.Context) {

	log.Println("Serving workers")

	var wg sync.WaitGroup

	s.process(ctx, &wg)

	wg.Wait()
}

func (s *WorkerPool) process(ctx context.Context, wg *sync.WaitGroup) {

	// Pull events from broker into local channels.
    wg.Add(1)
    taskStream := make(chan *Task)
    go func() {
        defer wg.Done()
        for {
            select {
            case <-ctx.Done():
                return
            default:
                msgs, err := s.tasks.Dequeue(ctx)
                if err != nil {
                    log.Fatalln(err)
                }
                for _, msg := range msgs {
                    taskStream <- msg
                }
            }
        }
    }()

    // Push results from channel onto distributed stream.
    wg.Add(1)
    resultStream := make(chan Result)
    go func() {
        defer wg.Done()
        for {
            select {
            case <-ctx.Done():
                return
            case r := <-resultStream:

                log.Printf("pushing results: %v", r)

                // Ack stream not to send task to other workers.
                //err := s.broker.Ack(ctx, r.Id)
                //if err != nil {
                //    log.Fatalln("unable to ack task: %w", err)
                //}
            }
        }
    }()

	for i := 0; i < s.count; i++ {
		wg.Add(1)
		go worker(ctx, wg, s.handler, taskStream, resultStream)
	}
}

func (s *WorkerPool) Register(key string, handler func(context.Context, *Task) Result) {
	s.handler.Register(key, handler)
}

// Ensure confinement by keeping the concurrent scope small.
func worker(ctx context.Context, wg *sync.WaitGroup, handler *ServeMux, tasks <-chan *Task, results chan<- Result) {

	defer wg.Done()

	for {
		select {
		case task, ok := <-tasks:
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
