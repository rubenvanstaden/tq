package main

import (
	"context"
	"os"
	"os/signal"

	"github.com/rubenvanstaden/tq"
	"github.com/rubenvanstaden/tq/example/task"
	"github.com/rubenvanstaden/tq/redis"
)

const BROKER_URL = "127.0.0.1:6379"

func main() {

	ctx, cancel := context.WithCancel(context.Background())
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() { <-c; cancel() }()

	taskQueue := redis.NewTaskQueue(BROKER_URL, "default")
	resultQueue := redis.NewResultQueue(BROKER_URL, "results")

	wp := tq.NewWorkerPool(taskQueue, resultQueue, 1)

	wp.Register("upload", task.HandlerUploadArtifacts)

	wp.Serve(ctx)
}
