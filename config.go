package kafka

import (
	"time"

	"github.com/IBM/sarama"
)

type SaramaConfig = sarama.Config

func NewConfig() *SaramaConfig {
	return sarama.NewConfig()
}

// ConsumerConfig 描述一个基于 Sarama consumer group 的消费者配置。
type ConsumerConfig struct {
	// SaramaConfig 会传给 sarama.NewConsumerGroup。
	//
	// 为 nil 时使用 sarama.NewConfig()。Consumer 不会覆盖调用方传入的 Sarama 配置；
	// 如需通过 OnError 接收 Sarama consumer group 异步错误，应设置
	// SaramaConfig.Consumer.Return.Errors = true。是否启用 Sarama 自动提交也完全由
	// 调用方配置决定。
	SaramaConfig *SaramaConfig

	// Brokers 是 Kafka bootstrap broker 地址列表。
	Brokers []string

	// GroupID 是 Kafka consumer group id。
	GroupID string

	// Topics 是当前 consumer group member 订阅的 topic 列表。
	Topics []string

	// MessageTimeout 为传给 OnMessage 的 context 设置可选的单条消息超时时间。
	//
	// 0 表示不设置单条消息超时。该超时是协作式的：handler 必须主动监听
	// ctx.Done()；Go 无法安全地强制中断正在运行的业务代码。
	MessageTimeout time.Duration

	// MaxConcurrentPartitions 限制当前 Consumer 同时处理消息的 partition 数量。
	//
	// 0 表示不限制。该限制只影响业务处理并发，不影响 Kafka consumer group
	// 分配给当前 Consumer 的 partition 数量。同一 partition 仍按 Kafka 消息顺序
	// 逐条调用 OnMessage，不会改成分区内并发。
	MaxConcurrentPartitions int
}

func NewConsumerConfig() ConsumerConfig {
	var config = ConsumerConfig{}
	config.SaramaConfig = NewConfig()
	return config
}

func normalizeConfig(config ConsumerConfig) ConsumerConfig {
	if config.SaramaConfig == nil {
		config.SaramaConfig = NewConfig()
	}
	if config.MaxConcurrentPartitions < 0 {
		config.MaxConcurrentPartitions = 0
	}
	return config
}
