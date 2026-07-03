package main

import (
	"fmt"
	"log"
	"time"

	"github.com/smartwalle/kafka"
	"github.com/smartwalle/kafka/examples"

	"github.com/IBM/sarama"
)

func main() {
	saramaConfig := kafka.NewConfig()
	saramaConfig.Version = sarama.V2_8_0_0
	saramaConfig.Producer.RequiredAcks = sarama.WaitForAll
	saramaConfig.Producer.Retry.Max = 3
	saramaConfig.Producer.Partitioner = sarama.NewManualPartitioner

	producer, err := sarama.NewAsyncProducer(examples.Brokers, saramaConfig)
	if err != nil {
		log.Fatalf("create producer failed: %v", err)
	}

	for i := 0; i < 100000; i++ {
		if err = produceOnce(producer, i, examples.Topic); err != nil {
			log.Fatalf("produce message failed: %v", err)
		}
	}

	producer.Close()
}

func produceOnce(producer sarama.AsyncProducer, i int, topic string) error {
	now := time.Now()
	msg := &sarama.ProducerMessage{
		Partition: int32(i % 5),
		Topic:     topic,
		Key:       sarama.StringEncoder("example"),
		Value:     sarama.StringEncoder(fmt.Sprintf("hello kafka %d-%s", i, now.Format(time.RFC3339))),
	}

	producer.Input() <- msg
	return nil
}
