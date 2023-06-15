package redis

import (
	"context"
	"log"

	redis "github.com/redis/go-redis/v9"

	"github.com/rubenvanstaden/tq"
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

	return &broker{
		client: client,
	}
}

func (s *broker) Enqueue(ctx context.Context, msg *tq.Task) error {
	return nil
}

func (s *broker) Messages(ctx context.Context, name string) ([]*tq.Task, error) {
	return nil, nil
}
