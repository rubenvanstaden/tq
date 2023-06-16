package main

import (
	"context"
	"log"
	"os"
	"os/signal"

	"github.com/rubenvanstaden/tq/example/task"
	"github.com/rubenvanstaden/tq/redis"
)

const BROKER_URL = "127.0.0.1:6379"

func main() {

	ctx, cancel := context.WithCancel(context.Background())
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() { <-c; cancel() }()

	// Push task to worker pool for execution.

	taskQueue := redis.NewTaskQueue(BROKER_URL, "default")

	tk := task.TaskUploadArtifacts(0, "hello friend")

	err := taskQueue.Enqueue(ctx, tk)
	if err != nil {
		log.Fatalf(": %v", err)
	}

	// Pull processed task results.

	resultQueue := redis.NewResultQueue(BROKER_URL, "results")
	for {
		select {
		case <-ctx.Done():
			return
		default:
			msgs, err := resultQueue.Dequeue(ctx)
			if err != nil {
				log.Fatalln(err)
			}
			for _, msg := range msgs {
				log.Println("YES!")
				log.Println(msg)
			}
		}
	}
}
