package kafka

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/IBM/sarama"
)

func TestConsumerLifecycleErrors(t *testing.T) {
	consumer := newTestConsumer(newFakeConsumerGroup(nil))
	if err := consumer.Start(context.Background()); !errors.Is(err, ErrConsumerHandlerRequired) {
		t.Fatalf("Start() without handler error = %v, want %v", err, ErrConsumerHandlerRequired)
	}

	consumer.OnMessage(func(context.Context, Committer, *sarama.ConsumerMessage) {})
	if err := consumer.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	if err := consumer.Start(context.Background()); !errors.Is(err, ErrConsumerRunning) {
		t.Fatalf("Start() while running error = %v, want %v", err, ErrConsumerRunning)
	}
	if err := consumer.Stop(context.Background()); err != nil {
		t.Fatalf("Stop() error = %v", err)
	}
	if err := consumer.Start(context.Background()); !errors.Is(err, ErrConsumerClosed) {
		t.Fatalf("Start() after Stop error = %v, want %v", err, ErrConsumerClosed)
	}
}

func TestConsumerStartRejectsNilConsumerGroup(t *testing.T) {
	consumer := NewConsumer(ConsumerConfig{
		Brokers: []string{"127.0.0.1:9092"},
		GroupID: "group-a",
		Topics:  []string{"topic-a"},
	})
	consumer.UseConsumerGroupFactory(func(ConsumerConfig) (sarama.ConsumerGroup, error) {
		return nil, nil
	})
	consumer.OnMessage(func(context.Context, Committer, *sarama.ConsumerMessage) {})

	if err := consumer.Start(context.Background()); !errors.Is(err, ErrConsumerGroupRequired) {
		t.Fatalf("Start() error = %v, want %v", err, ErrConsumerGroupRequired)
	}
}

func TestConsumerNormalizesNegativeMaxConcurrentPartitions(t *testing.T) {
	consumer := NewConsumer(ConsumerConfig{
		Brokers:                 []string{"127.0.0.1:9092"},
		GroupID:                 "group-a",
		Topics:                  []string{"topic-a"},
		MaxConcurrentPartitions: -1,
	})

	if consumer.config.MaxConcurrentPartitions != 0 {
		t.Fatalf("MaxConcurrentPartitions = %d, want 0", consumer.config.MaxConcurrentPartitions)
	}
	if consumer.partitionSlots != nil {
		t.Fatal("partitionSlots is not nil, want nil")
	}
}

func TestConsumerDoesNotAutoMarkSuccessfulMessages(t *testing.T) {
	msg := &sarama.ConsumerMessage{Topic: "topic-a", Partition: 2, Offset: 7}
	group := newFakeConsumerGroup([]*sarama.ConsumerMessage{msg})
	consumer := newTestConsumer(group)

	handled := make(chan struct{}, 1)
	consumer.OnMessage(func(context.Context, Committer, *sarama.ConsumerMessage) {
		handled <- struct{}{}
	})

	if err := consumer.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	waitSignals(t, handled, 1)
	if err := consumer.Stop(context.Background()); err != nil {
		t.Fatalf("Stop() error = %v", err)
	}

	if got := group.markedMessages(); got != 0 {
		t.Fatalf("marked messages = %d, want 0", got)
	}
}

func TestConsumerAllowsManualMarkAndCommit(t *testing.T) {
	msg := &sarama.ConsumerMessage{Topic: "topic-a", Partition: 2, Offset: 7}
	group := newFakeConsumerGroup([]*sarama.ConsumerMessage{msg})
	consumer := newTestConsumer(group)

	handled := make(chan struct{}, 1)
	consumer.OnMessage(func(_ context.Context, committer Committer, msg *sarama.ConsumerMessage) {
		committer.MarkMessage(msg, "")
		committer.Commit()
		handled <- struct{}{}
	})

	if err := consumer.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	waitSignals(t, handled, 1)
	if err := consumer.Stop(context.Background()); err != nil {
		t.Fatalf("Stop() error = %v", err)
	}

	if got := group.markedMessages(); got != 1 {
		t.Fatalf("marked messages = %d, want 1", got)
	}
	if got := group.commits(); got != 1 {
		t.Fatalf("commits = %d, want 1", got)
	}
}

func TestConsumerNormalizesNilSaramaConfig(t *testing.T) {
	consumer := NewConsumer(ConsumerConfig{
		Brokers: []string{"127.0.0.1:9092"},
		GroupID: "group-a",
		Topics:  []string{"topic-a"},
	})

	if consumer.config.SaramaConfig == nil {
		t.Fatal("SaramaConfig is nil, want default config")
	}
}

func TestConsumerUsesProvidedSaramaConfig(t *testing.T) {
	saramaConfig := sarama.NewConfig()
	saramaConfig.Consumer.Return.Errors = false
	saramaConfig.Consumer.Offsets.AutoCommit.Enable = true

	consumer := NewConsumer(ConsumerConfig{
		SaramaConfig: saramaConfig,
		Brokers:      []string{"127.0.0.1:9092"},
		GroupID:      "group-a",
		Topics:       []string{"topic-a"},
	})

	if consumer.config.SaramaConfig != saramaConfig {
		t.Fatal("SaramaConfig was not preserved")
	}
}

func TestConsumerHandlerPanicReportsMessageError(t *testing.T) {
	msg := &sarama.ConsumerMessage{Topic: "topic-a", Partition: 2, Offset: 7}
	group := newFakeConsumerGroup([]*sarama.ConsumerMessage{msg})
	consumer := newTestConsumer(group)

	errorsSeen := make(chan error, 1)
	consumer.OnMessage(func(context.Context, Committer, *sarama.ConsumerMessage) {
		panic("boom")
	})
	consumer.OnError(func(_ context.Context, err error) {
		errorsSeen <- err
	})

	if err := consumer.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	err := waitAnyError(t, errorsSeen)
	if stopErr := consumer.Stop(context.Background()); stopErr != nil {
		t.Fatalf("Stop() error = %v", stopErr)
	}

	var messageErr *MessageError
	if !errors.As(err, &messageErr) {
		t.Fatalf("OnError() error = %T, want *MessageError", err)
	}
	if got := group.markedMessages(); got != 0 {
		t.Fatalf("marked messages = %d, want 0", got)
	}
}

func TestConsumerForwardsConsumerGroupErrors(t *testing.T) {
	group := newFakeConsumerGroup(nil)
	consumer := newTestConsumer(group)

	groupErr := errors.New("broker disconnected")
	errorsSeen := make(chan error, 1)
	consumer.OnMessage(func(context.Context, Committer, *sarama.ConsumerMessage) {})
	consumer.OnError(func(_ context.Context, err error) {
		errorsSeen <- err
	})

	if err := consumer.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	group.sendError(groupErr)
	err := waitAnyError(t, errorsSeen)
	if stopErr := consumer.Stop(context.Background()); stopErr != nil {
		t.Fatalf("Stop() error = %v", stopErr)
	}
	if !errors.Is(err, groupErr) {
		t.Fatalf("OnError() error = %v, want %v", err, groupErr)
	}
}

func TestConsumerStopsAfterConsumeError(t *testing.T) {
	consumeErr := errors.New("consume failed")
	group := newFakeConsumerGroup(nil)
	group.consumeErr = consumeErr
	consumer := newTestConsumer(group)

	errorsSeen := make(chan error, 1)
	consumer.OnMessage(func(context.Context, Committer, *sarama.ConsumerMessage) {})
	consumer.OnError(func(_ context.Context, err error) {
		errorsSeen <- err
	})

	if err := consumer.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	err := waitAnyError(t, errorsSeen)
	if !errors.Is(err, consumeErr) {
		t.Fatalf("OnError() error = %v, want %v", err, consumeErr)
	}

	waitDone(t, consumer.done)
	if got := group.consumeCalls(); got != 1 {
		t.Fatalf("Consume() calls = %d, want 1", got)
	}
	if err := consumer.Stop(context.Background()); err != nil {
		t.Fatalf("Stop() error = %v", err)
	}
}

func TestConsumerLimitsMaxConcurrentPartitions(t *testing.T) {
	const (
		maxConcurrentPartitions = 2
		total                   = 3
	)

	consumer := NewConsumer(ConsumerConfig{
		Brokers:                 []string{"127.0.0.1:9092"},
		GroupID:                 "group-a",
		Topics:                  []string{"topic-a"},
		MaxConcurrentPartitions: maxConcurrentPartitions,
	})

	entered := make(chan struct{}, total)
	release := make(chan struct{})
	var mu sync.Mutex
	var active int
	var maxActive int
	consumer.OnMessage(func(ctx context.Context, _ Committer, _ *sarama.ConsumerMessage) {
		mu.Lock()
		active++
		if active > maxActive {
			maxActive = active
		}
		mu.Unlock()

		entered <- struct{}{}
		select {
		case <-release:
		case <-ctx.Done():
		}

		mu.Lock()
		active--
		mu.Unlock()
	})

	ctx, cancel := context.WithCancel(context.Background())
	session := &fakeSession{ctx: ctx}
	handler := &consumerGroupHandler{consumer: consumer}

	ready := make(chan struct{}, total)
	errs := make(chan error, total)
	var wg sync.WaitGroup
	for i := 0; i < total; i++ {
		claim := &fakeClaim{messages: make(chan *sarama.ConsumerMessage, 1)}
		claim.messages <- &sarama.ConsumerMessage{Topic: "topic-a", Partition: int32(i), Offset: int64(i)}
		close(claim.messages)

		wg.Add(1)
		go func() {
			defer wg.Done()
			ready <- struct{}{}
			errs <- handler.ConsumeClaim(session, claim)
		}()
	}

	waitSignals(t, ready, total)
	waitSignals(t, entered, maxConcurrentPartitions)
	select {
	case <-entered:
		cancel()
		close(release)
		wg.Wait()
		t.Fatal("handler exceeded MaxConcurrentPartitions before a slot was released")
	case <-time.After(50 * time.Millisecond):
	}

	release <- struct{}{}
	waitSignals(t, entered, 1)

	close(release)
	wg.Wait()
	cancel()
	close(errs)

	for err := range errs {
		if err != nil {
			t.Fatalf("ConsumeClaim() error = %v", err)
		}
	}

	mu.Lock()
	got := maxActive
	mu.Unlock()
	if got > maxConcurrentPartitions {
		t.Fatalf("max active partitions = %d, want <= %d", got, maxConcurrentPartitions)
	}
}

func TestConsumerUsesMessageTimeout(t *testing.T) {
	group := newFakeConsumerGroup([]*sarama.ConsumerMessage{{Topic: "topic-a", Partition: 2, Offset: 7}})
	consumer := newTestConsumer(group)
	consumer.config.MessageTimeout = time.Millisecond

	timedOut := make(chan struct{}, 1)
	consumer.OnMessage(func(ctx context.Context, _ Committer, _ *sarama.ConsumerMessage) {
		<-ctx.Done()
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			timedOut <- struct{}{}
		}
	})

	if err := consumer.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	waitSignals(t, timedOut, 1)
	if err := consumer.Stop(context.Background()); err != nil {
		t.Fatalf("Stop() error = %v", err)
	}
}

func TestConsumerStopIgnoresContextAndWaitsForClose(t *testing.T) {
	group := newFakeConsumerGroup(nil)
	group.blockClose()
	consumer := newTestConsumer(group)
	consumer.OnMessage(func(context.Context, Committer, *sarama.ConsumerMessage) {})

	if err := consumer.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()

	stopped := make(chan error, 1)
	go func() {
		stopped <- consumer.Stop(ctx)
	}()

	<-ctx.Done()
	select {
	case err := <-stopped:
		t.Fatalf("Stop() returned before ConsumerGroup.Close() finished: %v", err)
	default:
	}

	group.releaseClose()
	if err := <-stopped; err != nil {
		t.Fatalf("Stop() error = %v", err)
	}
}

func newTestConsumer(group *fakeConsumerGroup) *Consumer {
	consumer := NewConsumer(ConsumerConfig{
		Brokers: []string{"127.0.0.1:9092"},
		GroupID: "group-a",
		Topics:  []string{"topic-a"},
	})
	consumer.UseConsumerGroupFactory(func(ConsumerConfig) (sarama.ConsumerGroup, error) {
		return group, nil
	})
	return consumer
}

func waitSignals(t *testing.T, signals <-chan struct{}, want int) {
	t.Helper()

	timeout := time.After(time.Second)
	for i := 0; i < want; i++ {
		select {
		case <-signals:
		case <-timeout:
			t.Fatalf("received %d signals, want %d", i, want)
		}
	}
}

func waitAnyError(t *testing.T, errorsSeen <-chan error) error {
	t.Helper()

	select {
	case err := <-errorsSeen:
		return err
	case <-time.After(time.Second):
		t.Fatal("timeout waiting error")
		return nil
	}
}

func waitDone(t *testing.T, done <-chan struct{}) {
	t.Helper()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("timeout waiting done")
	}
}

type fakeConsumerGroup struct {
	messages   []*sarama.ConsumerMessage
	errors     chan error
	closed     chan struct{}
	consumeErr error

	closeOnce  sync.Once
	closeBlock chan struct{}

	sessionMu sync.Mutex
	session   *fakeSession

	consumeMu sync.Mutex
	consumes  int
}

func newFakeConsumerGroup(messages []*sarama.ConsumerMessage) *fakeConsumerGroup {
	return &fakeConsumerGroup{
		messages: messages,
		errors:   make(chan error, 8),
		closed:   make(chan struct{}),
	}
}

func (g *fakeConsumerGroup) Consume(ctx context.Context, _ []string, handler sarama.ConsumerGroupHandler) error {
	g.consumeMu.Lock()
	g.consumes++
	g.consumeMu.Unlock()

	if g.consumeErr != nil {
		return g.consumeErr
	}

	session := &fakeSession{ctx: ctx}
	g.sessionMu.Lock()
	g.session = session
	g.sessionMu.Unlock()

	if err := handler.Setup(session); err != nil {
		return err
	}
	claim := &fakeClaim{messages: make(chan *sarama.ConsumerMessage, len(g.messages))}
	for _, msg := range g.messages {
		claim.messages <- msg
	}
	close(claim.messages)
	if err := handler.ConsumeClaim(session, claim); err != nil {
		return err
	}
	if err := handler.Cleanup(session); err != nil {
		return err
	}

	select {
	case <-ctx.Done():
		return sarama.ErrClosedConsumerGroup
	case <-g.closed:
		return sarama.ErrClosedConsumerGroup
	}
}

func (g *fakeConsumerGroup) Errors() <-chan error {
	return g.errors
}

func (g *fakeConsumerGroup) Close() error {
	g.closeOnce.Do(func() {
		if g.closeBlock != nil {
			<-g.closeBlock
		}
		close(g.closed)
		close(g.errors)
	})
	return nil
}

func (g *fakeConsumerGroup) blockClose() {
	g.closeBlock = make(chan struct{})
}

func (g *fakeConsumerGroup) releaseClose() {
	close(g.closeBlock)
}

func (*fakeConsumerGroup) Pause(map[string][]int32) {}

func (*fakeConsumerGroup) Resume(map[string][]int32) {}

func (*fakeConsumerGroup) PauseAll() {}

func (*fakeConsumerGroup) ResumeAll() {}

func (g *fakeConsumerGroup) sendError(err error) {
	g.errors <- err
}

func (g *fakeConsumerGroup) markedMessages() int {
	g.sessionMu.Lock()
	defer g.sessionMu.Unlock()
	if g.session == nil {
		return 0
	}
	return g.session.markedMessages()
}

func (g *fakeConsumerGroup) commits() int {
	g.sessionMu.Lock()
	defer g.sessionMu.Unlock()
	if g.session == nil {
		return 0
	}
	return g.session.commitCount()
}

func (g *fakeConsumerGroup) consumeCalls() int {
	g.consumeMu.Lock()
	defer g.consumeMu.Unlock()
	return g.consumes
}

type fakeSession struct {
	ctx context.Context

	mu      sync.Mutex
	marked  int
	commits int
}

func (*fakeSession) Claims() map[string][]int32 {
	return map[string][]int32{"topic-a": {0}}
}

func (*fakeSession) MemberID() string {
	return "member-a"
}

func (*fakeSession) GenerationID() int32 {
	return 1
}

func (*fakeSession) MarkOffset(string, int32, int64, string) {}

func (s *fakeSession) Commit() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.commits++
}

func (*fakeSession) ResetOffset(string, int32, int64, string) {}

func (s *fakeSession) MarkMessage(*sarama.ConsumerMessage, string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.marked++
}

func (s *fakeSession) Context() context.Context {
	return s.ctx
}

func (s *fakeSession) markedMessages() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.marked
}

func (s *fakeSession) commitCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.commits
}

type fakeClaim struct {
	messages chan *sarama.ConsumerMessage
}

func (*fakeClaim) Topic() string {
	return "topic-a"
}

func (*fakeClaim) Partition() int32 {
	return 0
}

func (*fakeClaim) InitialOffset() int64 {
	return 0
}

func (*fakeClaim) HighWaterMarkOffset() int64 {
	return 0
}

func (c *fakeClaim) Messages() <-chan *sarama.ConsumerMessage {
	return c.messages
}
