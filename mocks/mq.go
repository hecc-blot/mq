package mocks

import (
	"context"
	"time"

	mqContract "github.com/hecc-blot/mq/contract"
)

// MockProducer 是 IProducer / IDelayedProducer 接口的 mock 实现。
// 通过 SendFn / SendDelayFn 定制行为，未设置时记录到 Sent 并返回 nil。
type MockProducer struct {
	SendFn      func(ctx context.Context, msg *mqContract.Message) error
	SendDelayFn func(ctx context.Context, msg *mqContract.Message, at time.Time) error
	CloseFn     func() error

	// Sent 记录经 Send / SendDelay 发出的消息，便于断言。
	Sent []*mqContract.Message
}

func (m *MockProducer) Send(ctx context.Context, msg *mqContract.Message) error {
	if m.SendFn != nil {
		return m.SendFn(ctx, msg)
	}
	m.Sent = append(m.Sent, msg)
	return nil
}

func (m *MockProducer) SendDelay(ctx context.Context, msg *mqContract.Message, at time.Time) error {
	if m.SendDelayFn != nil {
		return m.SendDelayFn(ctx, msg, at)
	}
	m.Sent = append(m.Sent, msg)
	return nil
}

func (m *MockProducer) Close() error {
	if m.CloseFn != nil {
		return m.CloseFn()
	}
	return nil
}

// Subscription 记录一次订阅，供测试断言。
type Subscription struct {
	Topic   string
	Group   string
	Handler mqContract.Handler
}

// MockConsumer 是 IConsumer / IOrderedConsumer 接口的 mock 实现。
// 通过 SubscribeFn / SubscribeOrderedFn 定制行为，未设置时记录到 Subscriptions。
type MockConsumer struct {
	SubscribeFn        func(ctx context.Context, topic, group string, handler mqContract.Handler, opts ...mqContract.SubscribeOption) error
	SubscribeOrderedFn func(ctx context.Context, topic, group string, handler mqContract.Handler) error
	CloseFn            func() error

	// Subscriptions 记录订阅，便于断言。
	Subscriptions []Subscription
}

func (m *MockConsumer) Subscribe(ctx context.Context, topic, group string, handler mqContract.Handler, opts ...mqContract.SubscribeOption) error {
	if m.SubscribeFn != nil {
		return m.SubscribeFn(ctx, topic, group, handler, opts...)
	}
	m.Subscriptions = append(m.Subscriptions, Subscription{Topic: topic, Group: group, Handler: handler})
	return nil
}

func (m *MockConsumer) SubscribeOrdered(ctx context.Context, topic, group string, handler mqContract.Handler) error {
	if m.SubscribeOrderedFn != nil {
		return m.SubscribeOrderedFn(ctx, topic, group, handler)
	}
	m.Subscriptions = append(m.Subscriptions, Subscription{Topic: topic, Group: group, Handler: handler})
	return nil
}

func (m *MockConsumer) Close() error {
	if m.CloseFn != nil {
		return m.CloseFn()
	}
	return nil
}

// MockMqFactory 是 IMqFactory 接口的 mock 实现，未定制时返回默认 mock。
type MockMqFactory struct {
	ProducerFn func() mqContract.IProducer
	ConsumerFn func() mqContract.IConsumer
}

func (m *MockMqFactory) Producer() mqContract.IProducer {
	if m.ProducerFn != nil {
		return m.ProducerFn()
	}
	return &MockProducer{}
}

func (m *MockMqFactory) Consumer() mqContract.IConsumer {
	if m.ConsumerFn != nil {
		return m.ConsumerFn()
	}
	return &MockConsumer{}
}

// 编译期断言：确保 mock 完整实现各接口。
var (
	_ mqContract.IProducer        = (*MockProducer)(nil)
	_ mqContract.IDelayedProducer = (*MockProducer)(nil)
	_ mqContract.IConsumer        = (*MockConsumer)(nil)
	_ mqContract.IOrderedConsumer = (*MockConsumer)(nil)
	_ mqContract.IMqFactory       = (*MockMqFactory)(nil)
)
