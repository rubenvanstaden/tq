package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"os/signal"

	"github.com/rubenvanstaden/tq"
	"github.com/rubenvanstaden/tq/redis"
)

const BROKER_URL = "127.0.0.1:6379"

func main() {

	ctx, cancel := context.WithCancel(context.Background())
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() { <-c; cancel() }()

    // 1. Encode the event payload into a tq.Task object.

	payload := ArtifactPayload{
        Id: 0,
        Data: "artifacts",
    }

	bytes, err := json.Marshal(payload)
	if err != nil {
		panic(err)
	}

    t0 := &tq.Task{
		Key:          "upload",
		Payload:      bytes,
		RetryCount:   1,
		RetryTimeout: 3,
	}

    t1 := &tq.Task{
		Key:          "download",
		Payload:      bytes,
		RetryCount:   1,
		RetryTimeout: 3,
	}

	// 2. Push task to worker pool for execution.

	ts := redis.NewStream(BROKER_URL, "default")

	err = ts.Enqueue(ctx, t0)
	if err != nil {
		log.Fatalf(": %v", err)
	}

	err = ts.Enqueue(ctx, t1)
	if err != nil {
		log.Fatalf(": %v", err)
	}

	// 3. Pull processed task results.

	rs := redis.NewStream(BROKER_URL, "results")
	for {
		select {
		case <-ctx.Done():
			return
		default:
			msgs, err := rs.Dequeue(ctx)
			if err != nil {
				log.Fatalln(err)
			}
			for _, msg := range msgs {
				log.Println("[+] Results from server")
				log.Println(msg.Result)
			}
		}
	}
}
