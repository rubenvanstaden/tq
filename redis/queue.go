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

type taskQueue struct {
	client       *redis.Client
	streamName   string
	groupName    string
	workerName   string
	streamSize   int
	blockTimeout time.Duration

	tq.TaskQueue
}

func NewTaskQueue(url, name string) tq.TaskQueue {

	options := &redis.Options{
		Addr: url,
	}

	client := redis.NewClient(options)

	_, err := client.Ping(context.Background()).Result()
	if err != nil {
		log.Fatal("Unable to connect to Redis", err)
	}

	tq := &taskQueue{
		client:       client,
		streamName:   name,
		groupName:    "tq:workers",
		workerName:   "tq:tasks",
		streamSize:   15,
		blockTimeout: 1 * time.Second,
	}

	// Create stream in redis DB.
	ctx := context.Background()
	err = client.XGroupCreateMkStream(ctx, tq.streamName, tq.groupName, "0").Err()
	if err != nil {
		if !strings.Contains(fmt.Sprint(err), "BUSYGROUP") {
			fmt.Printf("Error on create Consumer Group: %v ...\n", tq.groupName)
			log.Fatalln(err)
		}
	}

	return tq
}

func (s *taskQueue) Enqueue(ctx context.Context, msg *tq.Task) error {

	op := "broker.Enqueue"

	args := &redis.XAddArgs{
		Stream: s.streamName,
		Values: msg.Encode(),
		MaxLen: int64(s.streamSize),
	}

	id, err := s.client.XAdd(ctx, args).Result()
	if err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}

	msg.Id = id

	return nil
}

func (s *taskQueue) Dequeue(ctx context.Context) ([]*tq.Task, error) {

	op := "broker.Messages"

	val, err := s.client.XRange(ctx, s.streamName, "-", "+").Result()
	if err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}

	var msgs []*tq.Task

	if len(val) != 0 {

		args := &redis.XReadGroupArgs{
			Streams:  []string{s.streamName, ">"},
			Group:    s.groupName,
			Consumer: s.workerName,
			Count:    int64(s.streamSize),
			Block:    s.blockTimeout,
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

func (s *taskQueue) Ack(ctx context.Context, msgId string) error {

	op := "broker.Ack"

	if msgId == "" {
		log.Fatalf("message id not set")
	}

	err := s.client.XAck(ctx, s.streamName, s.groupName, msgId).Err()

	if errors.Is(err, redis.Nil) {
		return fmt.Errorf("%s: %s", op, "not found")
	}

	if err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}

	return nil
}
