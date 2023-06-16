package redis

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	redis "github.com/redis/go-redis/v9"

	"github.com/rubenvanstaden/tq"
)

const (
	STREAM_NAME   = "default"
	STREAM_SIZE   = 10
	GROUP_NAME    = "workers"
	WORKER_NAME   = "worker"
	BLOCK_TIMEOUT = 1 * time.Second
)

type broker struct {
	client *redis.Client
	tq.Broker
}

func New(url string) tq.Broker {

	options := &redis.Options{
		Addr: url,
	}

	client := redis.NewClient(options)

	_, err := client.Ping(context.Background()).Result()
	if err != nil {
		log.Fatal("Unable to connect to Redis", err)
	}

    // Create stream in redis DB.
    ctx := context.Background()
	err = client.XGroupCreateMkStream(ctx, STREAM_NAME, GROUP_NAME, "0").Err()
	if err != nil {
		if !strings.Contains(fmt.Sprint(err), "BUSYGROUP") {
			fmt.Printf("Error on create Consumer Group: %v ...\n", GROUP_NAME)
            log.Fatalln(err)
		}
	}

    // Create stream in redis DB.
	err = client.XGroupCreateMkStream(ctx, "result", GROUP_NAME, "0").Err()
	if err != nil {
		if !strings.Contains(fmt.Sprint(err), "BUSYGROUP") {
			fmt.Printf("Error on create Consumer Group: %v ...\n", GROUP_NAME)
            log.Fatalln(err)
		}
	}

	return &broker{
		client: client,
	}
}

func (s *broker) Enqueue(ctx context.Context, msg *tq.Task) error {

	op := "broker.Enqueue"

	args := &redis.XAddArgs{
		Stream: STREAM_NAME,
		Values: msg.Encode(),
		MaxLen: STREAM_SIZE,
	}

	id, err := s.client.XAdd(ctx, args).Result()
	if err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}

	msg.Id = id

	return nil
}

func (s *broker) Dequeue(ctx context.Context, name string) ([]*tq.Task, error) {

	op := "broker.Messages"

	val, err := s.client.XRange(ctx, name, "-", "+").Result()
	if err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}

    var msgs []*tq.Task

	if len(val) != 0 {

        args := &redis.XReadGroupArgs{
            Streams:  []string{name, ">"},
            Group:    GROUP_NAME,
            Consumer: WORKER_NAME,
            Count:    STREAM_SIZE,
            Block:    BLOCK_TIMEOUT,
        }

        tasks, err := s.client.XReadGroup(ctx, args).Result()
        if err != nil && err != redis.Nil {
            return nil, fmt.Errorf("could not dequeue task: %w", err)
        }

        if len(tasks) != 0 {
            for _, task := range tasks[0].Messages {
                msg := tq.Decode(task.ID, task.Values)
                msgs = append(msgs, msg)
            }
        }
    }

	return msgs, nil
}

func (s *broker) Ack(ctx context.Context, msgId string) error {

 	op := "broker.Ack"

    if msgId == "" {
        log.Fatalf("message id not set")
    }
 
    err := s.client.XAck(ctx, STREAM_NAME, GROUP_NAME, msgId).Err()
 
 	if errors.Is(err, redis.Nil) {
 		return fmt.Errorf("%s: %s", op, "not found")
 	}
 
 	if err != nil {
 		return fmt.Errorf("%s: %w", op, err)
 	}

	return nil
}
