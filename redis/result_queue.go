package redis

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	redis "github.com/redis/go-redis/v9"

	"github.com/rubenvanstaden/tq"
)

type resultQueue struct {
	client       *redis.Client
	streamName   string
	groupName    string
	workerName   string
	streamSize   int
	blockTimeout time.Duration

	tq.ResultQueue
}

func NewResultQueue(url, name string) tq.ResultQueue {

	options := &redis.Options{
		Addr: url,
	}

	client := redis.NewClient(options)

	_, err := client.Ping(context.Background()).Result()
	if err != nil {
		log.Fatal("Unable to connect to Redis", err)
	}

	q := &resultQueue{
		client:       client,
		streamName:   name,
		groupName:    "tq:workers",
		workerName:   "tq:tasks",
		streamSize:   15,
		blockTimeout: 1 * time.Second,
	}

	// Create stream in redis DB.
	ctx := context.Background()
	err = client.XGroupCreateMkStream(ctx, q.streamName, q.groupName, "0").Err()
	if err != nil {
		if !strings.Contains(fmt.Sprint(err), "BUSYGROUP") {
			fmt.Printf("Error on create Consumer Group: %v ...\n", q.groupName)
			log.Fatalln(err)
		}
	}

	return q
}

func (s *resultQueue) Enqueue(ctx context.Context, msg *tq.Result) error {

	op := "redis.Enqueue"

	args := &redis.XAddArgs{
		Stream: s.streamName,
		Values: msg.Encode(),
		MaxLen: int64(s.streamSize),
	}

	_, err := s.client.XAdd(ctx, args).Result()
	if err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}

	return nil
}

func (s *resultQueue) Dequeue(ctx context.Context) ([]*tq.Result, error) {

	op := "redis.Messages"

	val, err := s.client.XRange(ctx, s.streamName, "-", "+").Result()
	if err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}

	var msgs []*tq.Result

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
				msg := tq.DecodeResult(task.Values)
				msgs = append(msgs, msg)
			}
		}
	}

	return msgs, nil
}
