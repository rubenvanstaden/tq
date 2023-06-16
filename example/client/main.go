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

	broker := redis.New(BROKER_URL)

	tk := task.TaskUploadArtifacts(0, "hello friend")

	err := broker.Enqueue(ctx, tk)
	if err != nil {
		log.Fatalf(": %v", err)
	}

	for {
		select {
		case <-ctx.Done():
            return
		default:
            msgs, err := broker.Dequeue(ctx, "result")
			if err != nil {
                log.Fatalln(err)
			}
			for _, msg := range msgs {
                log.Println(msg)
			}
		}
	}
}
