package kafka

import (
	"context"
	"errors"
	"fmt"

	"github.com/IBM/sarama"
)

// State 表示 Consumer 的生命周期状态。
type State int32

const (
	// StateIdle 表示 Consumer 已创建但尚未启动。
	StateIdle State = iota

	// StateRunning 表示 Consumer 正在拉取、分发和处理消息。
	StateRunning

	// StateClosed 表示 Consumer 已经停止。Closed 后不支持复用。
	StateClosed
)

var (
	// ErrConsumerHandlerRequired 表示 Start 前没有注册消息处理函数。
	ErrConsumerHandlerRequired = errors.New("consumer handler required")

	// ErrConsumerRunning 表示 Consumer 已经启动，不能重复 Start。
	ErrConsumerRunning = errors.New("consumer is running")

	// ErrConsumerClosed 表示 Consumer 已经关闭。当前实现关闭后不支持复用。
	ErrConsumerClosed = errors.New("consumer closed")

	// ErrConsumerBrokersRequired 表示没有配置 Kafka broker 地址。
	ErrConsumerBrokersRequired = errors.New("consumer brokers required")

	// ErrConsumerGroupRequired 表示没有配置 group id，或 ConsumerGroupFactory 返回了 nil。
	ErrConsumerGroupRequired = errors.New("consumer group required")

	// ErrConsumerTopicsRequired 表示没有配置订阅 topic。
	ErrConsumerTopicsRequired = errors.New("consumer topics required")
)

// Message 是 Sarama ConsumerMessage 的别名，保留原始 Kafka 消息字段。
type Message = sarama.ConsumerMessage

// MessageHandler 业务消息处理函数。
//
// Consumer 不会自动标记或提交 offset。业务处理成功后，如需确认消费进度，
// 应通过 committer.MarkMessage 和 committer.Commit 手动控制。handler 可能被不同
// partition 并发调用，最大并发数可通过 ConsumerConfig.MaxConcurrentPartitions 限制；handler
// 必须保证线程安全和幂等。handler panic 会被 recover，并通过 OnError 包装为
// MessageError 上报。
type MessageHandler func(context.Context, Committer, *Message)

// ErrorHandler 错误通知回调。
//
// ErrorHandler 会接收 Consume 启动/运行错误、Sarama 异步错误以及 handler panic。
// 如果未开启 SaramaConfig.Consumer.Return.Errors，Sarama 异步错误不会进入 OnError。
type ErrorHandler func(context.Context, error)

// Committer 暴露业务手动确认消费进度需要的最小能力。
type Committer interface {
	// MarkMessage 标记消息已消费。该方法只标记 offset，不保证立即提交到 Kafka。
	MarkMessage(msg *Message, metadata string)

	// Commit 同步提交当前已标记的 offset。
	Commit()
}

// MessageError 和具体消息相关的错误。
type MessageError struct {
	Message *Message
	Err     error
}

func (e *MessageError) Error() string {
	if e.Message == nil {
		return fmt.Sprintf("message: %v", e.Err)
	}
	return fmt.Sprintf("message topic=%s partition=%d offset=%d: %v", e.Message.Topic, e.Message.Partition, e.Message.Offset, e.Err)
}

func (e *MessageError) Unwrap() error {
	return e.Err
}
