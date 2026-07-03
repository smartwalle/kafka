package main

import (
	"context"
	"errors"
	"log"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"

	"github.com/smartwalle/kafka/examples"

	"github.com/IBM/sarama"

	"github.com/smartwalle/kafka"
)

func main() {
	var config = kafka.NewConsumerConfig()
	config.Brokers = examples.Brokers
	config.GroupID = examples.GroupID
	config.Topics = []string{examples.Topic}
	config.MaxConcurrentPartitions = 0

	config.SaramaConfig.Consumer.Offsets.Initial = sarama.OffsetOldest
	config.SaramaConfig.Consumer.Return.Errors = true
	config.SaramaConfig.Consumer.Offsets.AutoCommit.Enable = true

	var consumer = kafka.NewConsumer(config)
	var consumed atomic.Uint64
	consumer.OnMessage(func(ctx context.Context, committer kafka.Committer, msg *kafka.Message) {
		if err := processMessage(ctx, msg); err != nil {
			log.Printf("process message failed: topic=%s partition=%d offset=%d err=%v", msg.Topic, msg.Partition, msg.Offset, err)
			return
		}

		committer.MarkMessage(msg, "")

		if n := consumed.Add(1); n%1000 == 0 {
			log.Printf("consumed=%d latest topic=%s partition=%d offset=%d n=%d", n, msg.Topic, msg.Partition, msg.Offset, n)
		}
	})

	consumer.OnError(func(_ context.Context, err error) {
		var messageErr *kafka.MessageError
		if errors.As(err, &messageErr) {
			log.Printf("message error: %v", messageErr)
			return
		}
		log.Printf("consumer error: %v", err)
	})

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := consumer.Start(ctx); err != nil {
		log.Fatalf("start consumer failed: %v", err)
	}

	<-ctx.Done()
	if err := consumer.Stop(context.Background()); err != nil {
		log.Printf("stop consumer failed: %v", err)
	}
}

func processMessage(ctx context.Context, _ *kafka.Message) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	return nil
}
