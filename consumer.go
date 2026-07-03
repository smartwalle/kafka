package kafka

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/IBM/sarama"
)

// ConsumerGroupFactory 用于自定义 Sarama ConsumerGroup 的创建方式。
//
// 默认使用 sarama.NewConsumerGroup。调用方可以通过 UseConsumerGroupFactory 改为
// sarama.NewConsumerGroupFromClient 或其他封装后的初始化逻辑。factory 必须返回非 nil
// 的 sarama.ConsumerGroup。
type ConsumerGroupFactory func(config ConsumerConfig) (sarama.ConsumerGroup, error)

// Consumer 是面向生产环境的 Sarama consumer group 封装。
//
// Consumer 持有一个 Sarama ConsumerGroup。Kafka 正常 rebalance 后，它会持续创建新的
// session 并继续消费，直到 Stop 被调用；如果 Consume 返回非关闭类错误，Consumer 会
// 通过 OnError 上报并停止消费循环。Consumer 关闭后不支持复用；需要重新消费时应创建
// 新的 Consumer。
type Consumer struct {
	config ConsumerConfig

	// state、rootCtx、rootCancel、consumerGroup 组成 Consumer 的生命周期状态。
	// 这些字段只在 Start、Stop、waitShutdown 这类生命周期方法中修改，访问时需要持有 mu。
	state         State
	rootCtx       context.Context
	rootCancel    context.CancelFunc
	consumerGroup sarama.ConsumerGroup

	// mu 保护生命周期状态；wg 等待消费循环和错误循环退出；done 用于对外通知关闭完成。
	mu       sync.Mutex
	wg       sync.WaitGroup
	done     chan struct{}
	doneOnce sync.Once

	// 回调使用 atomic.Value 保存，允许在消费过程中读取最新的 handler。
	// handler 本身可能被多个分区并发调用，线程安全由调用方保证。
	messageHandler atomic.Value
	errorHandler   atomic.Value

	// partitionSlots 用于限制正在执行 OnMessage 的 partition 数量；nil 表示不限制。
	partitionSlots chan struct{}

	// consumerGroupFactory 默认创建独立的 Sarama ConsumerGroup，调用方可按需复用 sarama.Client。
	consumerGroupFactory ConsumerGroupFactory
}

// NewConsumer 创建一个 Consumer。
//
// 调用 Start 前必须先注册 OnMessage。
func NewConsumer(config ConsumerConfig) *Consumer {
	config = normalizeConfig(config)
	var partitionSlots chan struct{}
	if config.MaxConcurrentPartitions > 0 {
		partitionSlots = make(chan struct{}, config.MaxConcurrentPartitions)
	}
	return &Consumer{
		config:               config,
		state:                StateIdle,
		done:                 make(chan struct{}),
		partitionSlots:       partitionSlots,
		consumerGroupFactory: defaultConsumerGroupFactory,
	}
}

func defaultConsumerGroupFactory(config ConsumerConfig) (sarama.ConsumerGroup, error) {
	return sarama.NewConsumerGroup(config.Brokers, config.GroupID, config.SaramaConfig)
}

// UseConsumerGroupFactory 设置 ConsumerGroup 创建函数。
//
// 传入 nil 会恢复为默认的 sarama.NewConsumerGroup。该方法应在 Start 前调用；
// Start 后调用不会替换已经创建的 ConsumerGroup。
func (c *Consumer) UseConsumerGroupFactory(fn ConsumerGroupFactory) {
	if fn == nil {
		fn = defaultConsumerGroupFactory
	}
	c.consumerGroupFactory = fn
}

// OnMessage 注册消息处理回调。
//
// 不同分配到的 claim 可能并发调用 handler。handler 必须保证线程安全和幂等；处理失败、
// 进程崩溃、未提交 offset 或 rebalance 时序变化都可能导致 Kafka 重投消息。配置
// MaxConcurrentPartitions 后，Consumer 会限制同时处理消息的 partition 数量。
func (c *Consumer) OnMessage(handler MessageHandler) {
	if handler == nil {
		return
	}
	c.messageHandler.Store(handler)
}

// OnError 注册错误回调。
//
// OnError 仅用于观测错误，应尽快返回。OnError 自身的 panic 会被 recover。
func (c *Consumer) OnError(handler ErrorHandler) {
	if handler == nil {
		return
	}
	c.errorHandler.Store(handler)
}

// Start 创建 Sarama consumer group，并启动后台消费循环。
//
// ctx 不能为 nil。Start 会校验配置和 OnMessage，并且不支持重复启动或关闭后复用。
func (c *Consumer) Start(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := c.validate(); err != nil {
		return err
	}
	if c.messageHandler.Load() == nil {
		return ErrConsumerHandlerRequired
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	// Consumer 的 Sarama ConsumerGroup 只创建一次。关闭后的 Consumer 不复用，
	// 避免旧 session、错误通道和关闭状态被再次使用。
	switch c.state {
	case StateRunning:
		return ErrConsumerRunning
	case StateClosed:
		return ErrConsumerClosed
	default:
	}

	group, err := c.consumerGroupFactory(c.config)
	if err != nil {
		return err
	}
	if group == nil {
		return ErrConsumerGroupRequired
	}

	c.consumerGroup = group
	c.rootCtx, c.rootCancel = context.WithCancel(ctx)
	c.state = StateRunning

	// consumeLoop 负责 ConsumerGroup.Consume，errorLoop 负责读取 Sarama 异步错误。
	// waitShutdown 不计入 wg，避免它等待自己；它会在两个后台循环退出后统一收尾。
	c.wg.Add(2)
	go c.consumeLoop()
	go c.errorLoop()
	go c.waitShutdown()

	return nil
}

// Stop 停止拉取新消息，并等待 Consumer 关闭完成。
//
// Stop 会取消传给 handler 的 context，并关闭 Sarama consumer group。正在运行的
// handler 必须主动响应 context 取消。ctx 当前仅保留兼容方法签名，Stop 会忽略 ctx
// 的取消或超时，并始终等待 ConsumerGroup 关闭完成。
func (c *Consumer) Stop(_ context.Context) (err error) {
	c.mu.Lock()
	// 先快照当前 cancel 和 ConsumerGroup，再释放锁执行关闭，避免 Close 阻塞生命周期锁。
	// 重复调用 Stop 时，cancel 和 ConsumerGroup.Close 应保持幂等。
	var cancel = c.rootCancel
	var consumerGroup = c.consumerGroup

	switch c.state {
	case StateIdle:
		// 未启动时直接进入关闭态，并通知等待 Stop 的调用方返回。
		c.state = StateClosed
		c.finish()
	case StateClosed:
	case StateRunning:
		c.state = StateClosed
	}
	c.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	if consumerGroup != nil {
		err = consumerGroup.Close()
	}

	<-c.done
	return err
}

func (c *Consumer) validate() error {
	// 这里仅校验 wrapper 依赖的必要配置和 Sarama 自身配置。
	// 是否开启自动提交、错误返回等行为由调用方配置 SaramaConfig 决定。
	if len(c.config.Brokers) == 0 {
		return ErrConsumerBrokersRequired
	}
	if c.config.GroupID == "" {
		return ErrConsumerGroupRequired
	}
	if len(c.config.Topics) == 0 {
		return ErrConsumerTopicsRequired
	}
	return c.config.SaramaConfig.Validate()
}

func (c *Consumer) consumeLoop() {
	defer func() {
		c.wg.Done()
	}()

	var handler = &consumerGroupHandler{consumer: c}
	for {
		if c.rootCtx.Err() != nil {
			return
		}

		// Sarama 的 Consume 对应一个 consumer group session。
		// 正常 rebalance 时 Consume 可能返回 nil，此时需要继续进入下一轮，获取新的分区分配。
		var err = c.consumerGroup.Consume(c.rootCtx, c.config.Topics, handler)
		if err != nil {
			if c.rootCtx.Err() != nil || errors.Is(err, sarama.ErrClosedConsumerGroup) {
				return
			}
			// 非关闭类错误通常表示本轮消费已经不可继续，例如 coordinator/session 异常。
			// 这里上报错误并取消根 context，让 Consumer 进入关闭流程，由调用方决定是否重建。
			c.handleError(c.rootCtx, err)
			if c.rootCancel != nil {
				c.rootCancel()
			}
			return
		}
	}
}

func (c *Consumer) errorLoop() {
	defer func() {
		c.wg.Done()
	}()

	for {
		select {
		case <-c.rootCtx.Done():
			return
		case err, ok := <-c.consumerGroup.Errors():
			// 只有 SaramaConfig.Consumer.Return.Errors=true 时，Sarama 才会把异步错误写入该通道。
			// 通道关闭表示 ConsumerGroup 已经关闭或错误循环不再可用。
			if !ok {
				return
			}
			if err != nil {
				c.handleError(c.rootCtx, err)
			}
		}
	}
}

func (c *Consumer) waitShutdown() {
	// 等待 consumeLoop 和 errorLoop 全部退出后，再统一把状态收敛到 Closed。
	// Stop 会先调用 ConsumerGroup.Close；这里再次 Close 是兜底，Sarama Close 可重复调用。
	c.wg.Wait()

	c.mu.Lock()
	if c.state != StateClosed {
		c.state = StateClosed
	}
	var consumerGroup = c.consumerGroup
	c.mu.Unlock()

	if consumerGroup != nil {
		_ = consumerGroup.Close()
	}
	c.finish()
}

func (c *Consumer) finish() {
	// done 可能由 Stop(StateIdle) 或 waitShutdown 触发，用 Once 保证只关闭一次。
	c.doneOnce.Do(func() {
		close(c.done)
	})
}

func (c *Consumer) handleError(ctx context.Context, err error) {
	var handler, _ = c.errorHandler.Load().(ErrorHandler)
	if handler == nil {
		return
	}

	defer func() {
		// OnError 只用于观测，回调 panic 不应导致消费协程崩溃。
		_ = recover()
	}()
	handler(ctx, err)
}

func (c *Consumer) handleMessage(session sarama.ConsumerGroupSession, msg *sarama.ConsumerMessage) {
	var handler, _ = c.messageHandler.Load().(MessageHandler)
	if handler == nil {
		return
	}

	var ctx = session.Context()
	var cancel = func() {}
	if c.config.MessageTimeout > 0 {
		// MessageTimeout 只限制单条消息 handler 的 context 生命周期。
		// 如果 handler 不响应 context，Stop 仍会等待 handler 自行返回。
		ctx, cancel = context.WithTimeout(ctx, c.config.MessageTimeout)
	}
	defer cancel()

	defer func() {
		if x := recover(); x != nil {
			// handler panic 会被转换为 MessageError 上报；offset 不会自动提交。
			c.handleError(session.Context(), &MessageError{Message: msg, Err: fmt.Errorf("message handler panic: %v", x)})
		}
	}()

	// 不在 wrapper 内自动 MarkMessage/Commit，避免把“回调已调用”等同于“业务已处理成功”。
	// 调用方需要在 handler 内根据业务结果自行决定提交时机。
	handler(ctx, session, msg)
}

func (c *Consumer) acquirePartitionSlot(ctx context.Context) bool {
	if ctx.Err() != nil {
		return false
	}
	if c.partitionSlots == nil {
		return true
	}
	select {
	case c.partitionSlots <- struct{}{}:
		if ctx.Err() != nil {
			c.releasePartitionSlot()
			return false
		}
		return true
	case <-ctx.Done():
		return false
	}
}

func (c *Consumer) releasePartitionSlot() {
	if c.partitionSlots == nil {
		return
	}
	<-c.partitionSlots
}

type consumerGroupHandler struct {
	consumer *Consumer
}

func (*consumerGroupHandler) Setup(sarama.ConsumerGroupSession) error {
	// Sarama 在新 session 建立后调用。当前 wrapper 不暴露 session 生命周期钩子，
	// 需要初始化外部资源时应在 Start 前完成，避免和 rebalance 生命周期耦合。
	return nil
}

func (*consumerGroupHandler) Cleanup(sarama.ConsumerGroupSession) error {
	// Sarama 在 session 结束前调用；这里不自动提交，也不做额外清理。
	// 未提交的 offset 会按 Kafka 语义在后续 session 中重新投递。
	return nil
}

func (h *consumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	// Sarama 会为分配到的 claim 启动并发消费；不同分区之间不会因为本循环互相串行阻塞。
	// 同一分区内仍按 claim.Messages() 的顺序逐条调用 handler。
	for {
		select {
		case <-session.Context().Done():
			return nil
		case msg, ok := <-claim.Messages():
			if !ok {
				return nil
			}
			if !h.consumer.acquirePartitionSlot(session.Context()) {
				return nil
			}
			// MaxConcurrentPartitions 只限制同时处理消息的 partition 数量，
			// 不改变同一分区的顺序处理。
			func() {
				defer h.consumer.releasePartitionSlot()
				h.consumer.handleMessage(session, msg)
			}()
		}
	}
}
