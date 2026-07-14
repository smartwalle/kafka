package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/IBM/sarama"
	"github.com/smartwalle/kafka"
	"github.com/smartwalle/kafka/examples"
	"github.com/xdg-go/scram"
)

func main() {
	certPool, err := kafka.NewCertPool(examples.CAFile)
	if err != nil {
		log.Fatalln(err)
	}

	var config = kafka.NewConfig()
	config.Version = sarama.V2_8_0_0
	config.Net.DialTimeout = 10 * time.Second
	config.Net.TLS.Enable = true
	config.Net.TLS.Config = &tls.Config{RootCAs: certPool}
	config.Net.SASL.Enable = true
	config.Net.SASL.User = examples.User
	config.Net.SASL.Password = examples.Password
	config.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA512
	config.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient {
		return kafka.NewSCRAMClient(scram.SHA512)
	}
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 3
	config.Producer.Return.Successes = true

	producer, err := sarama.NewSyncProducer(examples.Brokers, config)
	if err != nil {
		log.Fatalf("create producer failed: %v", err)
	}
	defer func() {
		if nErr := producer.Close(); nErr != nil {
			log.Printf("close producer failed: %v", nErr)
		}
	}()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()

	var idx = 0
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			idx += 1
			value := fmt.Sprintf("message %d", idx)
			partition, offset, err := producer.SendMessage(&sarama.ProducerMessage{
				Topic: examples.Topic,
				Value: sarama.StringEncoder(value),
			})
			if err != nil {
				log.Printf("produce failed: %v", err)
				continue
			}
			log.Printf("produced topic=%s partition=%d offset=%d value=%s",
				examples.Topic, partition, offset, value)
		}
	}
}
