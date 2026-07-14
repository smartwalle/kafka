package main

import (
	"context"
	"crypto/tls"
	"errors"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/smartwalle/kafka/examples"
	"github.com/xdg-go/scram"

	"github.com/IBM/sarama"

	"github.com/smartwalle/kafka"
)

func main() {
	certPool, err := kafka.NewCertPool(examples.CAFile)
	if err != nil {
		log.Fatalln(err)
	}

	var config = kafka.NewConsumerConfig()
	config.Brokers = examples.Brokers
	config.GroupID = examples.GroupID
	config.Topics = []string{examples.Topic}

	config.SaramaConfig.Version = sarama.V2_8_0_0
	config.SaramaConfig.Net.DialTimeout = 10 * time.Second
	config.SaramaConfig.Net.TLS.Enable = true
	config.SaramaConfig.Net.TLS.Config = &tls.Config{RootCAs: certPool}
	config.SaramaConfig.Net.SASL.Enable = true
	config.SaramaConfig.Net.SASL.User = examples.User
	config.SaramaConfig.Net.SASL.Password = examples.Password
	config.SaramaConfig.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA512
	config.SaramaConfig.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient {
		return kafka.NewSCRAMClient(scram.SHA512)
	}

	config.SaramaConfig.Consumer.Offsets.Initial = sarama.OffsetOldest
	config.SaramaConfig.Consumer.Return.Errors = true
	config.SaramaConfig.Consumer.Offsets.AutoCommit.Enable = true

	var consumer = kafka.NewConsumer(config)

	consumer.OnMessage(func(ctx context.Context, committer kafka.Committer, msg *kafka.Message) {
		log.Printf("latest topic=%s partition=%d offset=%d message=%s \n", msg.Topic, msg.Partition, msg.Offset, string(msg.Value))
		committer.MarkMessage(msg, "")
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

	if err = consumer.Start(ctx); err != nil {
		log.Fatalf("start consumer failed: %v", err)
	}

	<-ctx.Done()

	if err = consumer.Stop(context.Background()); err != nil {
		log.Printf("stop consumer failed: %v", err)
	}
}
