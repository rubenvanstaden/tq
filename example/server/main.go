package main

import (
	"context"
	"encoding/json"
	"fmt"
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

    ts := redis.NewStream(BROKER_URL, "default")
    rs := redis.NewStream(BROKER_URL, "results")

	wp := tq.NewWorkerPool(ts, rs, 1)

	wp.Register("upload", HandlerUploadArtifacts)
	wp.Register("download", HandlerDownloadArtifacts)

	wp.Serve(ctx)
}

func HandlerUploadArtifacts(ctx context.Context, t *tq.Task) *tq.Result {

	var p ArtifactPayload

	err := json.Unmarshal(t.Payload, &p)
    if err != nil {
		return &tq.Result{
            Value: "",
			Error: err.Error(),
		}
	}

	fmt.Println("[*] Uploading artifacts...")

	return &tq.Result{
        Value: "result:" + p.Data,
        Error: "",
	}
}

func HandlerDownloadArtifacts(ctx context.Context, t *tq.Task) *tq.Result {

	var p ArtifactPayload

	err := json.Unmarshal(t.Payload, &p)
    if err != nil {
		return &tq.Result{
            Value: "",
			Error: err.Error(),
		}
	}

	fmt.Println("[*] Downloading artifacts...")

	return &tq.Result{
        Value: "result:" + p.Data,
        Error: "",
	}
}
