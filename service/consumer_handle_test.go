package service

import (
	"context"
	"errors"
	"testing"

	ioc "github.com/hecc-blot/core/contract/ioc"
	"github.com/hecc-blot/core/contract/log"
	logMocks "github.com/hecc-blot/core/mocks/log"
	iocSvc "github.com/hecc-blot/core/service/ioc"
	mqContract "github.com/hecc-blot/mq/contract"
	"github.com/hecc-blot/mq/mocks"

	"github.com/stretchr/testify/assert"
)

// testConsume 测试用消费者。字段顺序刻意固化「注入字段必须在最前面」这一约定：
// LogSvc 之后紧跟一个没有 inject tag 的具名字段 marker，注入在此处停止，
// 因此声明在 marker 之后的 Dep 不会被注入。
type testConsume struct {
	LogSvc log.ILog `inject:""`

	marker string

	Dep log.ILog `inject:""`

	called []*mqContract.Message
}

func (c *testConsume) Consume(_ context.Context, msg *mqContract.Message) error {
	c.called = append(c.called, msg)
	return nil
}

// newTestContainer 返回已注册 log.ILog 的容器。
// ioc.Inject 对未注册的类型会 panic，而 testConsume 声明了 `inject:""` 的日志字段。
func newTestContainer() ioc.IContainer {
	container := iocSvc.New()
	container.Set(new(log.ILog), &logMocks.MockLog{})
	return container
}

func TestConsumerHandleRegisterAndStart(t *testing.T) {
	consumer := &mocks.MockConsumer{}
	handle := NewConsumerHandle(consumer, newTestContainer(), nil)

	handle.
		Register("order-created", "order-service", &testConsume{}).
		Register("order-paid", "order-service", &testConsume{}, mqContract.WithConcurrency(4))

	assert.NoError(t, handle.Start(context.Background()))

	assert.Len(t, consumer.Subscriptions, 2)
	assert.Equal(t, "order-created", consumer.Subscriptions[0].Topic)
	assert.Equal(t, "order-service", consumer.Subscriptions[0].Group)
	assert.NotNil(t, consumer.Subscriptions[0].Handler)

	assert.Equal(t, "order-paid", consumer.Subscriptions[1].Topic)
	assert.Equal(t, "order-service", consumer.Subscriptions[1].Group)

	// opts 原样透传：第一条未传选项，第二条 WithConcurrency(4) 原样到达后端
	assert.Empty(t, consumer.Subscriptions[0].Opts)
	assert.Len(t, consumer.Subscriptions[1].Opts, 1)
	assert.Equal(t, 4, mqContract.ApplySubscribeOptions(consumer.Subscriptions[1].Opts...).Concurrency)
}

func TestConsumerHandleHandlerForwardsToConsume(t *testing.T) {
	consumer := &mocks.MockConsumer{}
	handle := NewConsumerHandle(consumer, newTestContainer(), nil)

	instance := &testConsume{}
	handle.Register("order-created", "order-service", instance)
	assert.NoError(t, handle.Start(context.Background()))

	msg := &mqContract.Message{Topic: "order-created", Body: []byte(`{"id":1}`)}
	assert.NoError(t, consumer.Subscriptions[0].Handler(context.Background(), msg))

	assert.Len(t, instance.called, 1)
	assert.Same(t, msg, instance.called[0])
}

// TestConsumerHandleInjectDependencies 同时验证两条规则：
// 注入字段在最前面才会被注入；遇到无 inject tag 的具名字段即停止（其后的字段保持零值）。
func TestConsumerHandleInjectDependencies(t *testing.T) {
	container := newTestContainer()

	handle := NewConsumerHandle(&mocks.MockConsumer{}, container, nil)
	instance := &testConsume{}
	handle.Register("order-created", "order-service", instance)

	assert.NotNil(t, instance.LogSvc, "声明在无 tag 字段之前的注入字段应被注入")
	assert.Nil(t, instance.Dep, "声明在无 tag 字段之后的字段不应被注入")
}

func TestConsumerHandleRegisterPanics(t *testing.T) {
	newHandle := func() mqContract.IConsumerHandle {
		return NewConsumerHandle(&mocks.MockConsumer{}, newTestContainer(), nil)
	}

	t.Run("topic 为空", func(t *testing.T) {
		assert.Panics(t, func() { newHandle().Register("", "g", &testConsume{}) })
	})

	t.Run("group 为空", func(t *testing.T) {
		assert.Panics(t, func() { newHandle().Register("t", "", &testConsume{}) })
	})

	t.Run("consumer 为 nil", func(t *testing.T) {
		assert.Panics(t, func() { newHandle().Register("t", "g", nil) })
	})

	t.Run("重复注册同一 topic+group", func(t *testing.T) {
		handle := newHandle()
		handle.Register("t", "g", &testConsume{})
		assert.Panics(t, func() { handle.Register("t", "g", &testConsume{}) })
	})

	t.Run("Start 之后再注册", func(t *testing.T) {
		handle := newHandle()
		handle.Register("t", "g", &testConsume{})
		assert.NoError(t, handle.Start(context.Background()))
		assert.Panics(t, func() { handle.Register("t2", "g", &testConsume{}) })
	})
}

func TestNewConsumerHandleNilPanics(t *testing.T) {
	assert.Panics(t, func() { NewConsumerHandle(nil, newTestContainer(), nil) })
	assert.Panics(t, func() { NewConsumerHandle(&mocks.MockConsumer{}, nil, nil) })
}

// TestConsumerHandleStartFailureCloses 订阅失败时应关闭底层 consumer 并返回错误，
// 且注册器此后不可再用（IConsumer.Close 是整体关闭，无法只回退已成功的订阅）。
func TestConsumerHandleStartFailureCloses(t *testing.T) {
	closed := 0
	consumer := &mocks.MockConsumer{
		SubscribeFn: func(context.Context, string, string, mqContract.Handler, ...mqContract.SubscribeOption) error {
			return errors.New("boom")
		},
		CloseFn: func() error { closed++; return nil },
	}

	handle := NewConsumerHandle(consumer, newTestContainer(), nil)
	handle.Register("order-created", "order-service", &testConsume{})

	err := handle.Start(context.Background())
	assert.Error(t, err)
	assert.ErrorContains(t, err, "order-created")
	assert.Equal(t, 1, closed, "订阅失败应关闭底层 consumer")

	assert.Error(t, handle.Start(context.Background()), "失败后注册器不可再启动")
}

func TestConsumerHandleStartTwice(t *testing.T) {
	handle := NewConsumerHandle(&mocks.MockConsumer{}, newTestContainer(), nil)
	handle.Register("order-created", "order-service", &testConsume{})

	assert.NoError(t, handle.Start(context.Background()))
	assert.Error(t, handle.Start(context.Background()))
}

func TestConsumerHandleShutdownIdempotent(t *testing.T) {
	closed := 0
	consumer := &mocks.MockConsumer{CloseFn: func() error { closed++; return nil }}

	handle := NewConsumerHandle(consumer, newTestContainer(), nil)
	handle.Register("order-created", "order-service", &testConsume{})
	assert.NoError(t, handle.Start(context.Background()))

	assert.NoError(t, handle.Shutdown())
	assert.NoError(t, handle.Shutdown())
	assert.Equal(t, 1, closed, "Shutdown 幂等：重复调用只关闭一次")
}

// TestConsumerHandleStartLogs 注册了 logger 时应逐个输出启动日志。
func TestConsumerHandleStartLogs(t *testing.T) {
	logSvc := &logMocks.MockLog{}
	handle := NewConsumerHandle(&mocks.MockConsumer{}, newTestContainer(), logSvc)

	handle.Register("order-created", "order-service", &testConsume{})
	assert.NoError(t, handle.Start(context.Background()))

	assert.Len(t, logSvc.InfoMsgs, 1)
}
