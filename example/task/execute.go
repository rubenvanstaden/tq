package task

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/rubenvanstaden/tq"
)

type ArtifactPayload struct {
	Id   int
	Data string
}

func TaskUploadArtifacts(id int, data string) *tq.Task {

	payload := ArtifactPayload{Id: id, Data: data}

	bytes, err := json.Marshal(payload)
	if err != nil {
		panic(err)
	}

	return &tq.Task{
		Key:          "upload",
		Payload:      bytes,
		RetryCount:   1,
		RetryTimeout: 3,
	}
}

func HandlerUploadArtifacts(ctx context.Context, t *tq.Task) *tq.Result {

	var p ArtifactPayload
	if err := json.Unmarshal(t.Payload, &p); err != nil {
		return &tq.Result{
			Error: err.Error(),
		}
	}

	fmt.Printf("[*] Upload job artifacts (job-id: %d, storage-id: %s)\n", p.Id, p.Data)

	return &tq.Result{
		Value: "hello there!",
	}
}
