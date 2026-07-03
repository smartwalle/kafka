package main

import (
	"log"

	"github.com/smartwalle/kafka/examples"

	"github.com/IBM/sarama"
)

func main() {
	saramaConfig := sarama.NewConfig()
	saramaConfig.Version = sarama.V2_8_0_0

	admin, err := sarama.NewClusterAdmin(examples.Brokers, saramaConfig)
	if err != nil {
		log.Fatalf("create cluster admin failed: %v", err)
	}

	//deleteTopic(admin, examples.Topic)
	createTopic(admin, examples.Topic)

	admin.Close()
}

func createTopic(admin sarama.ClusterAdmin, topic string) {
	if err := admin.CreateTopic(topic, &sarama.TopicDetail{NumPartitions: 6, ReplicationFactor: 1}, false); err != nil {
		log.Println("创建 Topic 异常:", err)
		return
	}
	log.Println("创建 Topic 成功:", topic)
}

func deleteTopic(admin sarama.ClusterAdmin, topic string) {
	if err := admin.DeleteTopic(topic); err != nil {
		log.Println("删除 Topic 异常:", err)
		return
	}
	log.Println("删除 Topic 成功:", topic)
}
