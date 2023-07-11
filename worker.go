package tq

import (
	"context"
	"fmt"
	"log"
	"sync"
)

type pool struct {

	// Number of workers to spin up.
	count int

	// Pull tasks from distributed stream.
	tasks Stream

	// Push results onto distributed stream.
	results Stream

	// Multiplex a set of task handlers on startup.
	handler *serveMux
}

func NewWorkerPool(tasks, results Stream, count int) pool {
	return pool{
		count:   count,
		tasks:   tasks,
		results: results,
		handler: NewServeMux(),
	}
}

func (s *pool) Register(key string, handler func(context.Context, *Task) *Result) {
	s.handler.Register(key, handler)
}

func (s *pool) Serve(ctx context.Context) {

	log.Println("Serving workers")

	var wg sync.WaitGroup

	s.process(ctx, &wg)

	wg.Wait()
}

// TODO: Implement confinement by defining a smaller lexical scope.
func (s *pool) process(ctx context.Context, wg *sync.WaitGroup) {

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
				for _, t := range msgs {
					taskStream <- t
				}
			}
		}
	}()

	// Push results from channel onto distributed stream.
	wg.Add(1)
	resultStream := make(chan *Task)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case t := <-resultStream:

				// Enqueue the process task onto the result stream.
				err := s.results.Enqueue(ctx, t)
				if err != nil {
					log.Fatalf(": %v", err)
				}

				// Send ack to task stream that process results are on the result stream.
				//err := s.tasks.Ack(ctx, r.Id)
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

// Ensure confinement by keeping the concurrent scope small.
func worker(ctx context.Context, wg *sync.WaitGroup, handler *serveMux, tasks <-chan *Task, results chan<- *Task) {

	defer wg.Done()

	for {
		select {
		case task, ok := <-tasks:
			if !ok {
				return
			}
			// Fan-In job execution multiplexing results into the results channel.
            task.Result = *handler.ProcessTask(ctx, task)
			results <- task
		case <-ctx.Done():
			fmt.Printf("cancelled worker. Error detail: %v\n", ctx.Err())
			return
		}
	}
}
